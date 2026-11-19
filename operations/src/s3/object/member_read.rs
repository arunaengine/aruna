//! Decides where a GET or HEAD of a distributed bucket's key is answered: a current version at
//! the range's coordinator or a copy, a pinned one at a holder of its records (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::holder_choice::{HolderCandidate, HolderChoiceError, HolderChoiceOperation, pick};
use aruna_core::jobs::{ByteStream, JobEffect, JobEvent};
use aruna_core::member_read::{
    MemberAnswer, MemberBody, MemberFetch, MemberMethod, MemberSend, TrailerValue,
};
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutboundEffect, OutboundEvent, xml_field};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::delegation::{
    COPY_READ_HEADER, DELEGATION_HEADER, Payload, SignedDelegation, TRAILER_BYTES, body_digest,
    bound_header, request_digest,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, ReadRoute, RouteError};
use aruna_core::structs::storage::node_info::NodeUrls;
use bytes::Bytes;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use ulid::Ulid;

use crate::distribution::delegate::{DelegationInput, unsigned_delegation};
use crate::distribution::guard::GuardError;
use crate::distribution::resolve::key_holders;
use crate::distribution::routing::{copy_readers, read_coordinator, route_read};
use crate::s3::object::member_rest::{RestAnswer, RestAsk, RestCallOperation};
use crate::s3::object::range_list::DelegatedCaller;

/// How long a delegation lets a caller without a credential act.
const ANONYMOUS_SECS: u64 = 60;
const ACCEPT_ANY: &str = "*/*";
const NOT_FOUND: u16 = 404;
const CONFLICT: u16 = 409;
const FAILED_HERE: u16 = 500;
const UNREACHED: u16 = 503;
/// How long a member may take to start the answer to a read.
const ANSWER_LIMIT: Duration = Duration::from_secs(30);
/// Bytes of a member's answer read as text at most.
const MAX_TEXT_BYTES: u64 = 16 * 1024 * 1024;

/// Characters a key keeps unescaped in a forwarded path; `/` separates its segments.
const KEY_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

pub(crate) type Header = (String, String);

/// Where a read of a key is answered.
#[derive(Debug, PartialEq)]
pub enum MemberRead {
    /// This node answers; `Some` names the range whose copy here stands in for a coordinator
    /// that cannot answer, so the answer may be behind.
    Local(Option<Ulid>),
    /// Another member answered; `stale` names the range when a copy did.
    Remote {
        body: MemberBody,
        stale: Option<Ulid>,
    },
}

/// The read a member receives: `version` pins it, and `query` and `headers` travel with it.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberKey {
    pub bucket: String,
    pub key: String,
    pub version: Option<Ulid>,
    pub head: bool,
    pub query: Option<String>,
    /// Lowercase names; a later value of a name replaces an earlier one.
    pub headers: Vec<Header>,
}

impl MemberKey {
    fn plain(bucket: &str, key: &str, version: Option<Ulid>, head: bool) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            head,
            query: version.map(|version| format!("versionId={version}")),
            headers: Vec::new(),
        }
    }

    pub fn get(bucket: &str, key: &str, version: Option<Ulid>) -> Self {
        Self::plain(bucket, key, version, false)
    }

    pub fn head(bucket: &str, key: &str) -> Self {
        Self::plain(bucket, key, None, true)
    }

    /// The request a member receives for this read.
    fn request(&self) -> MemberRequest {
        let (method, operation) = match self.head {
            true => (MemberMethod::Head, "HeadObject"),
            false => (MemberMethod::Get, "GetObject"),
        };
        MemberRequest {
            method,
            operation,
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            query: self.query.clone(),
            headers: self.headers.clone(),
            body: MemberPayload::Empty,
        }
    }
}

/// The client's trailing checksum a forwarded body carries in its signed trailer.
#[derive(Debug, PartialEq)]
pub struct TrailerChecksum {
    pub algorithm: String,
    pub value: TrailerValue,
}

/// The body of a forwarded request and how its delegation binds it.
#[derive(Debug, PartialEq)]
pub enum MemberPayload {
    Empty,
    /// Streams with a trailer the issuer signs over its length and digest.
    Stream(ByteStream, Option<TrailerChecksum>),
    /// A small body known in advance, bound by its digest.
    Known(Bytes),
}

/// One forwarded S3 request: what the member receives besides the delegation.
#[derive(Debug, PartialEq)]
pub struct MemberRequest {
    pub method: MemberMethod,
    pub operation: &'static str,
    pub bucket: String,
    pub key: String,
    pub query: Option<String>,
    pub headers: Vec<Header>,
    pub body: MemberPayload,
}

/// Why no member's bytes came back.
#[derive(Debug, PartialEq)]
pub enum MemberRefusal {
    /// The member answered with this refusal.
    Refused {
        node_id: NodeId,
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// The member has no published endpoint, cannot be reached or started no answer in time.
    Unreachable {
        set_id: Ulid,
        range_id: Ulid,
        node_id: NodeId,
        late: bool,
    },
    /// No holder of the range answered a REST read.
    NoHolder { set_id: Ulid, range_id: Ulid },
    /// The member's answer broke off or could not be read.
    Broken(String),
    /// The key could not be routed.
    Route(GuardError),
    /// This node could not send the request.
    Failed(String),
}

impl MemberRefusal {
    /// The HTTP status this refusal stands for.
    pub fn status(&self) -> u16 {
        match self {
            Self::Refused { status, .. } => *status,
            Self::Unreachable { .. } | Self::NoHolder { .. } | Self::Broken(_) => UNREACHED,
            Self::Failed(_) => FAILED_HERE,
            Self::Route(
                GuardError::NotCoordinator { .. }
                | GuardError::RecordMissing(_)
                | GuardError::Unreadable(_)
                | GuardError::Route(
                    RouteError::Pending(_) | RouteError::Moving(_) | RouteError::StaleRange(_),
                ),
            ) => UNREACHED,
            Self::Route(
                GuardError::Route(
                    RouteError::Deleting(_) | RouteError::NotMember(_) | RouteError::NoRange(_),
                )
                | GuardError::Unbound(_),
            ) => CONFLICT,
            Self::Route(GuardError::Conversion(_)) => FAILED_HERE,
        }
    }

    /// Whether another member may answer in its place.
    pub fn unreachable(&self) -> bool {
        self.status() >= FAILED_HERE
    }
}

impl From<IoError> for MemberRefusal {
    fn from(error: IoError) -> Self {
        Self::Failed(error.to_string())
    }
}

/// A failed read; `stale` names the range when a standing-in copy refused it.
#[derive(Debug, PartialEq)]
pub struct MemberFailure {
    pub refusal: MemberRefusal,
    pub stale: Option<Ulid>,
}

impl From<IoError> for MemberFailure {
    fn from(error: IoError) -> Self {
        MemberRefusal::from(error).into()
    }
}

impl From<MemberRefusal> for MemberFailure {
    fn from(refusal: MemberRefusal) -> Self {
        Self {
            refusal,
            stale: None,
        }
    }
}

/// The range and member one request goes to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberTarget {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub node_id: NodeId,
}

#[derive(Debug, PartialEq)]
enum CallStep {
    Urls,
    Sign { base: String, headers: Vec<Header> },
    Answer,
    Avoid(MemberRefusal),
}

type CallTurn = Result<Next<MemberBody>, MemberRefusal>;

/// One S3 request at one member under a delegation bound to exactly this request. A read is
/// limited in how long the member may take and skips a failing member for a while.
#[derive(Debug, PartialEq)]
pub struct MemberCallOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: DelegatedCaller,
    target: MemberTarget,
    request: MemberRequest,
    read: bool,
    now_ms: u64,
    step: CallStep,
}

impl MemberCallOperation {
    fn new(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        target: MemberTarget,
        request: MemberRequest,
        read: bool,
    ) -> Self {
        Self {
            ids,
            caller,
            target,
            request,
            read,
            now_ms: 0,
            step: CallStep::Urls,
        }
    }

    /// A write or other request that goes to the key's coordinator and waits for its answer.
    pub fn write(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        target: MemberTarget,
        request: MemberRequest,
    ) -> Self {
        Self::new(ids, caller, target, request, false)
    }

    /// A read at a chosen member.
    pub fn read(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        target: MemberTarget,
        request: MemberRequest,
    ) -> Self {
        Self::new(ids, caller, target, request, true)
    }

    fn unreached(&self, late: bool) -> MemberRefusal {
        let target = &self.target;
        MemberRefusal::Unreachable {
            set_id: target.set_id,
            range_id: target.range_id,
            node_id: target.node_id,
            late,
        }
    }

    fn failed(&mut self, refusal: MemberRefusal) -> CallTurn {
        if !self.read || !refusal.unreachable() {
            return Err(refusal);
        }
        let avoid = NetEffect::AvoidHolder {
            node_id: self.target.node_id,
            now_ms: self.now_ms,
        };
        self.step = CallStep::Avoid(refusal);
        Ok(Next::Call(Effect::Net(avoid).into()))
    }

    /// How the delegation binds the body, and how much longer the body travels than it is.
    fn payload(&self) -> (Payload, i64) {
        match &self.request.body {
            MemberPayload::Empty => (Payload::Empty, 0),
            MemberPayload::Stream(_, checksum) => {
                let checksum = checksum.as_ref().map(|checksum| checksum.algorithm.clone());
                (Payload::Framed { checksum }, TRAILER_BYTES as i64)
            }
            MemberPayload::Known(bytes) => (Payload::Digest(body_digest(bytes)), 0),
        }
    }

    /// The delegation for exactly this request and the headers it binds.
    fn sign(&mut self, base: String) -> CallTurn {
        let Some(ids) = self.ids else {
            let refusal = MemberRefusal::Failed("this node has no signing key".to_string());
            return self.failed(refusal);
        };
        let (payload, extra) = self.payload();
        let request = &self.request;
        let mut headers = BTreeMap::new();
        for (name, value) in &request.headers {
            // A framed body is longer by its trailer.
            let value = match (name.as_str(), value.parse::<i64>()) {
                ("content-length", Ok(length)) => (length + extra).to_string(),
                _ => value.clone(),
            };
            headers.insert(name.clone(), value);
        }
        // Set here so the delegation binds it; a forwarded read never asks for a folder index.
        (headers.entry("accept".to_string())).or_insert_with(|| ACCEPT_ANY.to_string());
        let bound: Vec<_> = (headers.iter())
            .filter(|(name, _)| bound_header(name))
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let query = request.query.as_deref();
        let method = request.method.as_str();
        let digest = request_digest(method, &request.bucket, &request.key, query, &bound);
        let now_secs = self.now_ms / 1000;
        let caller = &self.caller;
        // The Everyone principal has no credential, so only a read's short window bounds it.
        let credential_expiry = match self.read && caller.user_id.is_nil() {
            true => now_secs.saturating_add(ANONYMOUS_SECS),
            false => caller.credential_expiry,
        };
        let input = DelegationInput {
            set_id: self.target.set_id,
            range_id: self.target.range_id,
            target: self.target.node_id,
            user_id: caller.user_id,
            group_id: caller.group_id,
            credential_id: caller.credential_id.clone(),
            restrictions: caller.restrictions.clone(),
            credential_expiry,
            operation: request.operation.to_string(),
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            copy_source: (request.headers.iter())
                .find(|(name, _)| name == "x-amz-copy-source")
                .map(|(_, value)| value.clone()),
            request_digest: digest,
            payload,
        };
        let delegation = unsigned_delegation(ids, input, now_secs);
        let headers = headers.into_iter().collect();
        self.step = CallStep::Sign { base, headers };
        let sign = NetEffect::DelegationSign(Box::new(delegation));
        Ok(Next::Call(Effect::Net(sign).into()))
    }

    fn send(&mut self, base: &str, mut headers: Vec<Header>, signed: SignedDelegation) -> CallTurn {
        let delegation = match signed.encode() {
            Ok(delegation) => delegation,
            Err(error) => return self.failed(MemberRefusal::Failed(error.to_string())),
        };
        headers.push((DELEGATION_HEADER.to_string(), delegation));
        let request = &mut self.request;
        let path = utf8_percent_encode(&request.key, KEY_SET);
        let mut url = format!("{}/{}/{path}", base.trim_end_matches('/'), request.bucket);
        if let Some(query) = &request.query {
            url = format!("{url}?{query}");
        }
        let body = match std::mem::replace(&mut request.body, MemberPayload::Empty) {
            MemberPayload::Empty => MemberSend::Empty,
            MemberPayload::Known(bytes) => MemberSend::Bytes(bytes),
            MemberPayload::Stream(stream, checksum) => MemberSend::Framed {
                stream,
                nonce: signed.delegation.nonce,
                checksum: checksum.map(|checksum| checksum.value),
            },
        };
        let fetch = MemberFetch {
            method: request.method,
            url,
            headers,
            body,
            answer_limit: self.read.then_some(ANSWER_LIMIT),
            any_status: false,
        };
        self.step = CallStep::Answer;
        let fetch = OutboundEffect::MemberFetch(Box::new(fetch));
        Ok(Next::Call(Effect::Outbound(fetch).into()))
    }
}

impl Operation for MemberCallOperation {
    type Output = MemberBody;
    type Error = MemberRefusal;

    fn first(&mut self) -> CallTurn {
        let urls = NodeUrlsOperation::new(self.target.node_id);
        Ok(Next::Call(IoCall::child(urls)))
    }

    fn next(&mut self, result: IoResult) -> CallTurn {
        match std::mem::replace(&mut self.step, CallStep::Answer) {
            CallStep::Urls => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                match urls.ok().flatten().and_then(|urls| urls.s3) {
                    Some(base) => self.sign(base),
                    None => self.failed(self.unreached(false)),
                }
            }
            CallStep::Sign { base, headers } => match result.event()? {
                Event::Net(NetEvent::DelegationSign(Some(signed))) => {
                    self.send(&base, headers, *signed)
                }
                Event::Net(NetEvent::DelegationSign(None)) => {
                    let refusal = MemberRefusal::Failed("this node did not sign".to_string());
                    self.failed(refusal)
                }
                other => Err(unexpected("Event::Net(NetEvent::DelegationSign)", &other)),
            },
            CallStep::Answer => match result.event()? {
                Event::Outbound(OutboundEvent::MemberAnswered(answer)) => match *answer {
                    MemberAnswer::Body(body) => Ok(Next::Done(body)),
                    MemberAnswer::Refused { status, body } => self.failed(MemberRefusal::Refused {
                        node_id: self.target.node_id,
                        status,
                        code: xml_field(&body, "Code"),
                        message: xml_field(&body, "Message").unwrap_or_default(),
                    }),
                    MemberAnswer::Silent => self.failed(self.unreached(false)),
                    MemberAnswer::Late => self.failed(self.unreached(true)),
                },
                other => Err(unexpected("OutboundEvent::MemberAnswered", &other)),
            },
            CallStep::Avoid(refusal) => {
                result.event()?;
                Err(refusal)
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[derive(Debug, PartialEq)]
enum TextStep {
    Call,
    Collect,
}

/// The text of a member's answer: of a call this operation makes, or of a body already here.
/// A broken or oversized answer is a refusal, never an empty text.
#[derive(Debug, PartialEq)]
pub struct MemberTextOperation {
    call: Option<MemberCallOperation>,
    body: Option<MemberBody>,
    step: TextStep,
}

impl MemberTextOperation {
    pub fn of_call(call: MemberCallOperation) -> Self {
        Self {
            call: Some(call),
            body: None,
            step: TextStep::Call,
        }
    }

    pub fn of_body(body: MemberBody) -> Self {
        Self {
            call: None,
            body: Some(body),
            step: TextStep::Collect,
        }
    }

    fn collect(&mut self, body: MemberBody) -> Result<Next<String>, MemberRefusal> {
        self.step = TextStep::Collect;
        let collect = JobEffect::CollectUpTo {
            stream: body.stream,
            limit: MAX_TEXT_BYTES,
        };
        Ok(Next::Call(Effect::Job(collect).into()))
    }
}

impl Operation for MemberTextOperation {
    type Output = String;
    type Error = MemberRefusal;

    fn first(&mut self) -> Result<Next<String>, MemberRefusal> {
        if let Some(call) = self.call.take() {
            return Ok(Next::Call(IoCall::child(call)));
        }
        let body = self.body.take().ok_or(IoError::Unfinished)?;
        self.collect(body)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, MemberRefusal> {
        if self.step == TextStep::Call {
            let body = result.child::<MemberBody, MemberRefusal>()??;
            return self.collect(body);
        }
        let collected = match result.event()? {
            Event::Job(JobEvent::CollectedUpTo(collected)) => collected,
            other => return Err(unexpected("JobEvent::CollectedUpTo", &other)),
        };
        let broken = |reason: String| MemberRefusal::Broken(reason);
        let bytes = collected
            .map_err(|error| broken(format!("A member's answer broke off: {error}")))?
            .ok_or_else(|| broken("A member's answer is too long".to_string()))?;
        String::from_utf8(bytes)
            .map(Next::Done)
            .map_err(|_| broken("A member answered with invalid text".to_string()))
    }
}

fn unexpected(expected: &'static str, got: &Event) -> MemberRefusal {
    IoError::Unexpected {
        expected,
        got: got.label(),
    }
    .into()
}

/// Who asks for the read, which decides how far it may travel.
#[derive(Debug, PartialEq)]
enum ReadOrigin {
    Caller,
    /// A member sent the read here, so it is never sent on.
    Delegated {
        copy_read: bool,
    },
    /// This node missed the key and asks the coordinator; a delegated read stops here.
    AfterMiss {
        delegated: bool,
    },
}

/// How the read proves its caller and reaches a member.
#[derive(Debug, PartialEq)]
enum ReadAccess {
    /// The member's S3 endpoint under a delegation.
    Delegated(DelegatedCaller),
    /// The member's REST route with the caller's bearer token.
    Rest(RestAsk),
}

#[derive(Debug, PartialEq)]
enum ReadStep {
    Coordinator,
    Route,
    Holders,
    Choice,
    /// The chosen holder; on a miss at a replica the coordinator is asked.
    Holder,
    Coordinating,
    Missed,
    Readers(MemberRefusal),
    Copy(MemberRefusal),
    RestHolders,
    RestAnswer,
}

type ReadTurn = Result<Next<MemberRead>, MemberFailure>;

/// Finds where `object` is answered and reads it there for a caller this node verified. Over S3
/// only a current version may come from a copy, and only while its coordinator cannot answer;
/// over REST the holders are asked in turn, the coordinator last.
#[derive(Debug, PartialEq)]
pub struct MemberReadOperation {
    ids: Option<(NodeId, RealmId)>,
    access: ReadAccess,
    binding: DistributionBinding,
    object: MemberKey,
    origin: ReadOrigin,
    seed: u128,
    now_ms: u64,
    range_id: Option<Ulid>,
    coordinator: Option<NodeId>,
    holder: Option<NodeId>,
    copies: VecDeque<NodeId>,
    step: ReadStep,
}

impl MemberReadOperation {
    fn with_access(
        ids: Option<(NodeId, RealmId)>,
        access: ReadAccess,
        binding: DistributionBinding,
        object: MemberKey,
    ) -> Self {
        Self {
            ids,
            access,
            binding,
            object,
            origin: ReadOrigin::Caller,
            seed: rand::random(),
            now_ms: 0,
            range_id: None,
            coordinator: None,
            holder: None,
            copies: VecDeque::new(),
            step: ReadStep::Coordinator,
        }
    }

    pub fn new(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        binding: DistributionBinding,
        object: MemberKey,
    ) -> Self {
        Self::with_access(ids, ReadAccess::Delegated(caller), binding, object)
    }

    /// A read through the REST route `ask` names, for the key `key`.
    pub fn rest(binding: DistributionBinding, key: &str, ask: RestAsk) -> Self {
        let object = MemberKey::get("", key, None);
        Self::with_access(None, ReadAccess::Rest(ask), binding, object)
    }

    /// For a read another member delegated here: answered here or refused. A current one is
    /// refused unless this node coordinates the range or `copy_read` asks this copy.
    pub fn delegated(mut self, copy_read: bool) -> Self {
        self.origin = ReadOrigin::Delegated { copy_read };
        self
    }

    /// For a key this node missed, since a copy may lag behind the coordinator. `Local(None)`
    /// leaves the miss as it is, `Local(Some(range))` keeps it while the coordinator is silent.
    pub fn after_miss(mut self, delegated: bool) -> Self {
        self.origin = ReadOrigin::AfterMiss { delegated };
        self
    }

    fn pinned(&self) -> bool {
        self.object.version.is_some()
    }

    fn is_rest(&self) -> bool {
        matches!(self.access, ReadAccess::Rest(_))
    }

    fn fetch(&self, node_id: NodeId, range_id: Ulid, copy_read: bool) -> Result<IoCall, IoError> {
        let ReadAccess::Delegated(caller) = &self.access else {
            return Err(IoError::Unfinished);
        };
        let target = MemberTarget {
            set_id: self.binding.set_id,
            range_id,
            node_id,
        };
        let mut request = self.object.request();
        if copy_read {
            (request.headers).push((COPY_READ_HEADER.to_string(), "1".to_string()));
        }
        let call = MemberCallOperation::read(self.ids, caller.clone(), target, request);
        Ok(IoCall::child(call))
    }

    fn ask_holder(&mut self, node_id: NodeId) -> ReadTurn {
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.holder = Some(node_id);
        self.step = ReadStep::Holder;
        Ok(Next::Call(self.fetch(node_id, range_id, false)?))
    }

    fn ask_coordinator(&self) -> Result<IoCall, IoError> {
        let coordinator = self.coordinator.ok_or(IoError::Unfinished)?;
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.fetch(coordinator, range_id, false)
    }

    fn remote(body: MemberBody, stale: Option<Ulid>) -> ReadTurn {
        Ok(Next::Done(MemberRead::Remote { body, stale }))
    }

    /// Asks the next REST holder; a range none answers fails the read.
    fn next_rest(&mut self) -> ReadTurn {
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        let Some(node_id) = self.copies.pop_front() else {
            let set_id = self.binding.set_id;
            return Err(MemberRefusal::NoHolder { set_id, range_id }.into());
        };
        let ReadAccess::Rest(ask) = &self.access else {
            return Err(IoError::Unfinished.into());
        };
        let call = RestCallOperation::relay(node_id, self.binding.set_id, ask);
        self.holder = Some(node_id);
        self.step = ReadStep::RestAnswer;
        Ok(Next::Call(IoCall::child(call)))
    }

    fn rest_answered(&mut self, answer: RestAnswer) -> ReadTurn {
        let more = !self.copies.is_empty();
        match answer {
            RestAnswer::Silent => self.next_rest(),
            // A copy's miss goes on; the coordinator, asked last, decides.
            RestAnswer::Body(body) if body.status == NOT_FOUND && more => self.next_rest(),
            RestAnswer::Body(body) => Self::remote(body, None),
            RestAnswer::Refused { status, .. } if status == NOT_FOUND && more => self.next_rest(),
            RestAnswer::Refused { status, body } => {
                let node_id = self.holder.ok_or(IoError::Unfinished)?;
                let refusal = MemberRefusal::Refused {
                    node_id,
                    status,
                    code: None,
                    message: rest_message(&body),
                };
                Err(refusal.into())
            }
        }
    }

    fn coordinator_known(&mut self, coordinator: Option<(Ulid, NodeId)>) -> ReadTurn {
        let Some((range_id, node_id)) = coordinator else {
            return Ok(Next::Done(MemberRead::Local(None)));
        };
        self.range_id = Some(range_id);
        self.coordinator = Some(node_id);
        match self.origin {
            _ if self.is_rest() => {
                self.copies = VecDeque::from([node_id]);
                self.next_rest()
            }
            ReadOrigin::Caller => self.ask_holder(node_id),
            ReadOrigin::Delegated { .. } => {
                let error = GuardError::NotCoordinator {
                    set_id: self.binding.set_id,
                    range_id,
                    node_id,
                };
                Err(MemberRefusal::Route(error).into())
            }
            ReadOrigin::AfterMiss { delegated: true } => Ok(Next::Done(MemberRead::Local(None))),
            ReadOrigin::AfterMiss { delegated: false } => {
                self.step = ReadStep::Missed;
                Ok(Next::Call(self.ask_coordinator()?))
            }
        }
    }

    /// While the coordinator cannot answer a caller, a current version comes from a copy.
    fn failed(&mut self, refusal: MemberRefusal) -> ReadTurn {
        let origin_caller = self.origin == ReadOrigin::Caller;
        if self.is_rest() || self.pinned() || !origin_caller || !refusal.unreachable() {
            return Err(refusal.into());
        }
        let readers = copy_readers(&self.binding, &self.object.key);
        self.step = ReadStep::Readers(refusal);
        Ok(Next::Call(IoCall::child(readers)))
    }

    fn next_copy(&mut self, first_refusal: MemberRefusal) -> ReadTurn {
        let Some(node_id) = self.copies.pop_front() else {
            return Err(first_refusal.into());
        };
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.step = ReadStep::Copy(first_refusal);
        Ok(Next::Call(self.fetch(node_id, range_id, true)?))
    }
}

/// The message of a REST refusal.
fn rest_message(body: &str) -> String {
    (serde_json::from_str::<serde_json::Value>(body).ok())
        .and_then(|value| value.get("error")?.as_str().map(str::to_string))
        .unwrap_or_default()
}

impl Operation for MemberReadOperation {
    type Output = MemberRead;
    type Error = MemberFailure;

    fn first(&mut self) -> ReadTurn {
        let (binding, key) = (&self.binding, &self.object.key);
        let pinned = self.object.version.is_some();
        let by_route = pinned || self.is_rest();
        match self.origin {
            ReadOrigin::Delegated { copy_read } if pinned || copy_read => {
                Ok(Next::Done(MemberRead::Local(None)))
            }
            ReadOrigin::Caller if by_route => {
                self.step = ReadStep::Route;
                Ok(Next::Call(IoCall::child(route_read(binding, key, false))))
            }
            _ => {
                self.step = ReadStep::Coordinator;
                Ok(Next::Call(IoCall::child(read_coordinator(binding, key))))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match std::mem::replace(&mut self.step, ReadStep::Holder) {
            ReadStep::Coordinator => match result.child::<Option<(Ulid, NodeId)>, GuardError>()? {
                Ok(coordinator) => self.coordinator_known(coordinator),
                Err(error) => self.failed(MemberRefusal::Route(error)),
            },
            ReadStep::Route => match result.child::<ReadRoute, GuardError>()? {
                Ok(ReadRoute::Holder { range_id, node_id }) => {
                    self.range_id = Some(range_id);
                    self.coordinator = Some(node_id);
                    let holders = key_holders(self.binding.set_id, &self.object.key);
                    self.step = match self.is_rest() {
                        true => ReadStep::RestHolders,
                        false => ReadStep::Holders,
                    };
                    Ok(Next::Call(IoCall::child(holders)))
                }
                Ok(ReadRoute::Local { .. }) => Ok(Next::Done(MemberRead::Local(None))),
                Err(error) => self.failed(MemberRefusal::Route(error)),
            },
            ReadStep::RestHolders => {
                let holders = result.child::<Vec<NodeId>, IoError>()?.unwrap_or_default();
                let coordinator = self.coordinator.ok_or(IoError::Unfinished)?;
                let ReadAccess::Rest(ask) = &self.access else {
                    return Err(IoError::Unfinished.into());
                };
                let local = ask.local;
                self.copies = (holders.into_iter())
                    .filter(|holder| *holder != local && *holder != coordinator)
                    .chain([coordinator])
                    .collect();
                self.next_rest()
            }
            ReadStep::RestAnswer => {
                let answer = result.child::<RestAnswer, IoError>()??;
                self.rest_answered(answer)
            }
            ReadStep::Holders => {
                let holders = result.child::<Vec<NodeId>, IoError>()?.unwrap_or_default();
                let choice = HolderChoiceOperation::new(&holders, self.now_ms);
                self.step = ReadStep::Choice;
                Ok(Next::Call(IoCall::child(choice)))
            }
            ReadStep::Choice => {
                let choice = result.child::<Vec<HolderCandidate>, HolderChoiceError>()?;
                let coordinator = self.coordinator.ok_or(IoError::Unfinished)?;
                let chosen = pick(choice.unwrap_or_default(), self.seed);
                self.ask_holder(chosen.map_or(coordinator, |chosen| chosen.node_id))
            }
            ReadStep::Holder => match result.child::<MemberBody, MemberRefusal>()? {
                Ok(body) => Self::remote(body, None),
                // A replica may lag behind the coordinator, which is asked once.
                Err(refusal)
                    if refusal.status() == NOT_FOUND && self.holder != self.coordinator =>
                {
                    self.step = ReadStep::Coordinating;
                    Ok(Next::Call(self.ask_coordinator()?))
                }
                Err(refusal) => self.failed(refusal),
            },
            ReadStep::Coordinating => match result.child::<MemberBody, MemberRefusal>()? {
                Ok(body) => Self::remote(body, None),
                Err(refusal) => self.failed(refusal),
            },
            ReadStep::Missed => match result.child::<MemberBody, MemberRefusal>()? {
                Ok(body) => Self::remote(body, None),
                Err(refusal) if refusal.unreachable() => {
                    Ok(Next::Done(MemberRead::Local(self.range_id)))
                }
                Err(refusal) => Err(refusal.into()),
            },
            ReadStep::Readers(refusal) => {
                let readers = result.child::<(Ulid, bool, Vec<NodeId>), GuardError>()?;
                let (range_id, here, copies) = readers.map_err(MemberRefusal::Route)?;
                if here {
                    return Ok(Next::Done(MemberRead::Local(Some(range_id))));
                }
                self.range_id = Some(range_id);
                self.copies = copies.into();
                self.next_copy(refusal)
            }
            ReadStep::Copy(first_refusal) => match result.child::<MemberBody, MemberRefusal>()? {
                Ok(body) => Self::remote(body, self.range_id),
                Err(failed) if failed.unreachable() => self.next_copy(first_refusal),
                Err(refusal) => Err(MemberFailure {
                    refusal,
                    stale: self.range_id,
                }),
            },
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
#[path = "member_read_tests.rs"]
mod tests;
