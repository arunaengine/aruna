//! Decides where a GET or HEAD of a distributed bucket's key is answered: a current version at
//! the range's coordinator or a copy, a pinned one at a holder of its records (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::holder_choice::{HolderCandidate, HolderChoiceError, HolderChoiceOperation, pick};
use aruna_core::member_read::{MemberAnswer, MemberBody, MemberFetch};
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutboundEffect, OutboundEvent};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::delegation::{
    COPY_READ_HEADER, DELEGATION_HEADER, Payload, SignedDelegation, request_digest,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, ReadRoute, RouteError};
use aruna_core::structs::storage::node_info::NodeUrls;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use ulid::Ulid;

use crate::distribution::delegate::{DelegationInput, unsigned_delegation};
use crate::distribution::guard::GuardError;
use crate::distribution::resolve::key_holders;
use crate::distribution::routing::{copy_readers, read_coordinator, route_read};
use crate::s3::object::range_list::DelegatedCaller;

/// How long a delegation lets a caller without a credential act.
const ANONYMOUS_SECS: u64 = 60;
const ACCEPT_ANY: &str = "*/*";
const NOT_FOUND: u16 = 404;
const CONFLICT: u16 = 409;
const FAILED_HERE: u16 = 500;
const UNREACHED: u16 = 503;

/// Characters a key keeps unescaped in a forwarded path; `/` separates its segments.
const KEY_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

type Header = (String, String);

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

    fn operation(&self) -> &'static str {
        match self.head {
            true => "HeadObject",
            false => "GetObject",
        }
    }
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
    /// The key could not be routed.
    Route(GuardError),
    /// This node could not send the read.
    Failed(String),
}

impl MemberRefusal {
    /// The HTTP status this refusal stands for.
    pub fn status(&self) -> u16 {
        match self {
            Self::Refused { status, .. } => *status,
            Self::Unreachable { .. } => UNREACHED,
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
    fn unreachable(&self) -> bool {
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

/// The range and member one read goes to.
#[derive(Clone, Debug, PartialEq)]
struct MemberTarget {
    set_id: Ulid,
    range_id: Ulid,
    node_id: NodeId,
}

#[derive(Debug, PartialEq)]
enum FetchStep {
    Urls,
    Sign { base: String, headers: Vec<Header> },
    Answer,
    Avoid(MemberRefusal),
}

type FetchTurn = Result<Next<MemberBody>, MemberRefusal>;

/// One read at one member under a delegation bound to exactly this request. A member that fails
/// is skipped in holder choices for a while.
#[derive(Debug, PartialEq)]
struct MemberFetchOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: DelegatedCaller,
    target: MemberTarget,
    object: MemberKey,
    /// Asks a copy to answer while the range's coordinator cannot.
    copy_read: bool,
    now_ms: u64,
    step: FetchStep,
}

impl MemberFetchOperation {
    fn unreached(&self, late: bool) -> MemberRefusal {
        let target = &self.target;
        MemberRefusal::Unreachable {
            set_id: target.set_id,
            range_id: target.range_id,
            node_id: target.node_id,
            late,
        }
    }

    fn failed(&mut self, refusal: MemberRefusal) -> FetchTurn {
        if !refusal.unreachable() {
            return Err(refusal);
        }
        let avoid = NetEffect::AvoidHolder {
            node_id: self.target.node_id,
            now_ms: self.now_ms,
        };
        self.step = FetchStep::Avoid(refusal);
        Ok(Next::Call(Effect::Net(avoid).into()))
    }

    /// The delegation for exactly this read and the headers it binds.
    fn sign(&mut self, base: String) -> FetchTurn {
        let Some(ids) = self.ids else {
            let refusal = MemberRefusal::Failed("this node has no signing key".to_string());
            return self.failed(refusal);
        };
        let object = &self.object;
        let mut headers: BTreeMap<String, String> = object.headers.iter().cloned().collect();
        (headers.entry("accept".to_string())).or_insert_with(|| ACCEPT_ANY.to_string());
        if self.copy_read {
            headers.insert(COPY_READ_HEADER.to_string(), "1".to_string());
        }
        let bound: Vec<_> = (headers.iter())
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let method = if object.head { "HEAD" } else { "GET" };
        let query = object.query.as_deref();
        let digest = request_digest(method, &object.bucket, &object.key, query, &bound);
        let now_secs = self.now_ms / 1000;
        let caller = &self.caller;
        let credential_expiry = match caller.user_id.is_nil() {
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
            operation: object.operation().to_string(),
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            copy_source: None,
            request_digest: digest,
            payload: Payload::Empty,
        };
        let delegation = unsigned_delegation(ids, input, now_secs);
        let headers = headers.into_iter().collect();
        self.step = FetchStep::Sign { base, headers };
        let sign = NetEffect::DelegationSign(Box::new(delegation));
        Ok(Next::Call(Effect::Net(sign).into()))
    }

    fn send(
        &mut self,
        base: &str,
        mut headers: Vec<Header>,
        signed: SignedDelegation,
    ) -> FetchTurn {
        let delegation = match signed.encode() {
            Ok(delegation) => delegation,
            Err(error) => return self.failed(MemberRefusal::Failed(error.to_string())),
        };
        headers.push((DELEGATION_HEADER.to_string(), delegation));
        let object = &self.object;
        let path = utf8_percent_encode(&object.key, KEY_SET);
        let mut url = format!("{}/{}/{path}", base.trim_end_matches('/'), object.bucket);
        if let Some(query) = &object.query {
            url = format!("{url}?{query}");
        }
        let head = object.head;
        self.step = FetchStep::Answer;
        let fetch = MemberFetch { head, url, headers };
        let fetch = OutboundEffect::MemberFetch(Box::new(fetch));
        Ok(Next::Call(Effect::Outbound(fetch).into()))
    }
}

impl Operation for MemberFetchOperation {
    type Output = MemberBody;
    type Error = MemberRefusal;

    fn first(&mut self) -> FetchTurn {
        let urls = NodeUrlsOperation::new(self.target.node_id);
        Ok(Next::Call(IoCall::child(urls)))
    }

    fn next(&mut self, result: IoResult) -> FetchTurn {
        match std::mem::replace(&mut self.step, FetchStep::Answer) {
            FetchStep::Urls => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                match urls.ok().flatten().and_then(|urls| urls.s3) {
                    Some(base) => self.sign(base),
                    None => self.failed(self.unreached(false)),
                }
            }
            FetchStep::Sign { base, headers } => match result.event()? {
                Event::Net(NetEvent::DelegationSign(Some(signed))) => {
                    self.send(&base, headers, *signed)
                }
                Event::Net(NetEvent::DelegationSign(None)) => {
                    let refusal = MemberRefusal::Failed("this node did not sign".to_string());
                    self.failed(refusal)
                }
                other => Err(unexpected("Event::Net(NetEvent::DelegationSign)", &other)),
            },
            FetchStep::Answer => match result.event()? {
                Event::Outbound(OutboundEvent::MemberAnswered(answer)) => match *answer {
                    MemberAnswer::Body(body) => Ok(Next::Done(body)),
                    MemberAnswer::Refused {
                        status,
                        code,
                        message,
                    } => self.failed(MemberRefusal::Refused {
                        node_id: self.target.node_id,
                        status,
                        code,
                        message,
                    }),
                    MemberAnswer::Silent => self.failed(self.unreached(false)),
                    MemberAnswer::Late => self.failed(self.unreached(true)),
                },
                other => Err(unexpected("OutboundEvent::MemberAnswered", &other)),
            },
            FetchStep::Avoid(refusal) => {
                result.event()?;
                Err(refusal)
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
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
}

type ReadTurn = Result<Next<MemberRead>, MemberFailure>;

/// Finds where `object` is answered and reads it there for a caller this node verified. Only a
/// current version may come from a copy, and only while its coordinator cannot answer.
#[derive(Debug, PartialEq)]
pub struct MemberReadOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: DelegatedCaller,
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
    pub fn new(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        binding: DistributionBinding,
        object: MemberKey,
    ) -> Self {
        Self {
            ids,
            caller,
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

    fn fetch(&self, node_id: NodeId, range_id: Ulid, copy_read: bool) -> IoCall {
        IoCall::child(MemberFetchOperation {
            ids: self.ids,
            caller: self.caller.clone(),
            target: MemberTarget {
                set_id: self.binding.set_id,
                range_id,
                node_id,
            },
            object: self.object.clone(),
            copy_read,
            now_ms: self.now_ms,
            step: FetchStep::Urls,
        })
    }

    fn ask_holder(&mut self, node_id: NodeId) -> ReadTurn {
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.holder = Some(node_id);
        self.step = ReadStep::Holder;
        Ok(Next::Call(self.fetch(node_id, range_id, false)))
    }

    fn ask_coordinator(&self) -> Result<IoCall, IoError> {
        let coordinator = self.coordinator.ok_or(IoError::Unfinished)?;
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        Ok(self.fetch(coordinator, range_id, false))
    }

    fn remote(body: MemberBody, stale: Option<Ulid>) -> ReadTurn {
        Ok(Next::Done(MemberRead::Remote { body, stale }))
    }

    fn coordinator_known(&mut self, coordinator: Option<(Ulid, NodeId)>) -> ReadTurn {
        let Some((range_id, node_id)) = coordinator else {
            return Ok(Next::Done(MemberRead::Local(None)));
        };
        self.range_id = Some(range_id);
        self.coordinator = Some(node_id);
        match self.origin {
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
        if self.pinned() || !origin_caller || !refusal.unreachable() {
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
        Ok(Next::Call(self.fetch(node_id, range_id, true)))
    }
}

impl Operation for MemberReadOperation {
    type Output = MemberRead;
    type Error = MemberFailure;

    fn first(&mut self) -> ReadTurn {
        let (binding, key) = (&self.binding, &self.object.key);
        let pinned = self.object.version.is_some();
        match self.origin {
            ReadOrigin::Delegated { copy_read } if pinned || copy_read => {
                Ok(Next::Done(MemberRead::Local(None)))
            }
            ReadOrigin::Caller if pinned => {
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
                    self.step = ReadStep::Holders;
                    Ok(Next::Call(IoCall::child(holders)))
                }
                Ok(ReadRoute::Local { .. }) => Ok(Next::Done(MemberRead::Local(None))),
                Err(error) => self.failed(MemberRefusal::Route(error)),
            },
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
            ReadStep::Readers(refusal) => {
                let readers = result.child::<(Ulid, bool, Vec<NodeId>), GuardError>()?;
                let (range_id, here, copies) =
                    readers.map_err(|error| guard_refusal(&error).entry_status())?;
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
                Err(failed) => Err(failed.entry_status()),
            },
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::distribution::DistributionRevision;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn range() -> Ulid {
        Ulid::from_bytes([5; 16])
    }

    fn caller() -> MemberCaller {
        MemberCaller {
            auth: AuthContext::anonymous(realm()),
            group_id: Ulid::from_bytes([2; 16]),
            credential_expiry: 0,
        }
    }

    fn object(version: Option<Ulid>) -> MemberKey {
        MemberKey {
            bucket: "spire".to_string(),
            key: "a b".to_string(),
            version,
        }
    }

    fn read(version: Option<Ulid>) -> MemberReadOperation {
        let binding = DistributionBinding {
            set_id: Ulid::from_bytes([3; 16]),
            member_id: Ulid::from_bytes([4; 16]),
            incarnation: 0,
            revision: DistributionRevision::default(),
        };
        let ids = Some((node(1), realm()));
        MemberReadOperation::new(ids, caller(), binding, object(version))
    }

    fn child<T, E>(answer: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(answer))
    }

    fn body(version: &str) -> MemberBody {
        let chunks = futures_util::stream::empty::<Result<bytes::Bytes, std::io::Error>>();
        MemberBody {
            version: Some(version.to_string()),
            stream: ByteStream::new(chunks),
        }
    }

    fn refused(status: u16, code: Option<&str>) -> Result<MemberBody, MemberRefusal> {
        let code = code.map(str::to_string);
        Err(MemberRefusal { status, code })
    }

    fn coordinated(read: &mut MemberReadOperation) {
        read.first().unwrap();
        let coordinator = Some((range(), node(2)));
        read.next(child::<_, GuardError>(Ok(coordinator))).unwrap();
    }

    #[test]
    fn own_range_local() {
        let mut read = read(None);
        read.first().unwrap();
        let answer = read.next(child::<Option<(Ulid, NodeId)>, GuardError>(Ok(None)));
        assert_eq!(answer, Ok(Next::Done(MemberRead::Local(None))));
    }

    #[test]
    fn refusal_sets_status() {
        let mut read = read(None);
        coordinated(&mut read);
        let answer = read.next(child(refused(403, Some("AccessDenied"))));
        assert_eq!(answer, Err(EntryStatus::Denied));
    }

    #[test]
    fn missing_version_unavailable() {
        let mut read = read(None);
        coordinated(&mut read);
        let answer = read.next(child::<_, MemberRefusal>(Ok(body("not a ulid"))));
        assert_eq!(answer, Err(EntryStatus::Unavailable));
    }

    #[test]
    fn copy_answers_stale() {
        let version = Ulid::from_bytes([9; 16]);
        let mut read = read(None);
        coordinated(&mut read);
        let readers = read.next(child(refused(503, None))).unwrap();
        assert!(matches!(readers, Next::Call(IoCall::Child(_))));
        let copies = (range(), false, vec![node(3)]);
        read.next(child::<_, GuardError>(Ok(copies))).unwrap();
        let answer = read.next(child::<_, MemberRefusal>(Ok(body(&version.to_string()))));
        let Ok(Next::Done(MemberRead::Remote {
            version: found,
            stale,
            ..
        })) = answer
        else {
            panic!("the copy answers");
        };
        assert_eq!((found, stale), (version, Some(range())));
    }

    #[test]
    fn own_copy_serves() {
        let mut read = read(None);
        coordinated(&mut read);
        read.next(child(refused(503, None))).unwrap();
        let readers = (range(), true, Vec::<NodeId>::new());
        let answer = read.next(child::<_, GuardError>(Ok(readers)));
        assert_eq!(answer, Ok(Next::Done(MemberRead::Local(Some(range())))));
    }

    #[test]
    fn silent_copies_fail() {
        let mut read = read(None);
        coordinated(&mut read);
        read.next(child(refused(503, Some("AccessDenied"))))
            .unwrap();
        let readers = (range(), false, vec![node(3)]);
        read.next(child::<_, GuardError>(Ok(readers))).unwrap();
        let answer = read.next(child(refused(503, None)));
        assert_eq!(answer, Err(EntryStatus::Denied));
    }

    #[test]
    fn replica_miss_retried() {
        let mut read = read(Some(Ulid::from_bytes([9; 16])));
        read.first().unwrap();
        let route = ReadRoute::Holder {
            range_id: range(),
            node_id: node(2),
        };
        read.next(child::<_, GuardError>(Ok(route))).unwrap();
        read.next(child::<_, IoError>(Ok(vec![node(3)]))).unwrap();
        let candidate = HolderCandidate {
            node_id: node(3),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            free_slots: 1,
            bandwidth: 1,
        };
        let chosen = child::<_, HolderChoiceError>(Ok(vec![candidate]));
        read.next(chosen).unwrap();
        assert_eq!(read.holder, Some(node(3)));
        read.next(child(refused(404, Some("NoSuchKey")))).unwrap();
        assert_eq!(read.step, ReadStep::Coordinating);
        let answer = read.next(child(refused(404, Some("NoSuchKey"))));
        assert_eq!(answer, Err(EntryStatus::Missing));
    }

    fn get(copy_read: bool) -> MemberGetOperation {
        MemberGetOperation {
            ids: Some((node(1), realm())),
            caller: caller(),
            target: MemberTarget {
                set_id: Ulid::from_bytes([3; 16]),
                range_id: range(),
                node_id: node(2),
            },
            object: object(Some(Ulid::from_bytes([9; 16]))),
            copy_read,
            now_ms: 10_000,
            step: GetStep::Urls,
        }
    }

    fn urls(s3: Option<&str>) -> IoResult {
        let urls = NodeUrls {
            api: None,
            s3: s3.map(str::to_string),
        };
        child::<_, NodeInfoError>(Ok(Some(urls)))
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    #[test]
    fn get_binds_request() {
        let mut get = get(true);
        get.first().unwrap();
        let Ok(Next::Call(IoCall::Effect(effect))) = get.next(urls(Some("http://s3/"))) else {
            panic!("the delegation is signed");
        };
        let Effect::Net(NetEffect::DelegationSign(delegation)) = *effect else {
            panic!("the delegation is signed");
        };
        assert_eq!(delegation.operation, "GetObject");
        assert_eq!(delegation.target, node(2));
        // A caller without a credential is bounded by a short window.
        assert_eq!(delegation.credential_expiry, 10 + ANONYMOUS_SECS);
        let version = Ulid::from_bytes([9; 16]);
        let query = format!("versionId={version}");
        let bound = [("accept", ACCEPT_ANY), (COPY_READ_HEADER, "1")];
        let digest = request_digest("GET", "spire", "a b", Some(&query), &bound);
        assert_eq!(delegation.request_digest, digest);
        let signed = SignedDelegation {
            signature: iroh::SecretKey::from_bytes(&[1; 32]).sign(b"delegation"),
            delegation: *delegation,
        };
        let header = signed.encode().unwrap();
        let signed = Event::Net(NetEvent::DelegationSign(Some(Box::new(signed))));
        let Ok(Next::Call(IoCall::Effect(effect))) = get.next(event(signed)) else {
            panic!("the read is sent");
        };
        let Effect::Blob(BlobEffect::MemberGet(request)) = *effect else {
            panic!("the read is sent");
        };
        assert_eq!(request.url, format!("http://s3/spire/a%20b?{query}"));
        let expected = [
            ("accept", ACCEPT_ANY),
            (COPY_READ_HEADER, "1"),
            (DELEGATION_HEADER, header.as_str()),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        assert_eq!(request.headers, expected);
    }

    #[test]
    fn silent_member_avoided() {
        let mut get = get(false);
        get.step = GetStep::Answer;
        let silent = Box::new(MemberAnswer::Silent);
        let silent = Event::Blob(BlobEvent::MemberAnswered(silent));
        let avoid = NetEffect::AvoidHolder {
            node_id: node(2),
            now_ms: 10_000,
        };
        let expected = IoCall::from(Effect::Net(avoid));
        assert_eq!(get.next(event(silent)), Ok(Next::Call(expected)));
        let answer = get.next(event(Event::Net(NetEvent::HolderAvoided)));
        assert_eq!(answer, Err(MemberRefusal::new(UNREACHED)));
    }

    #[test]
    fn refusal_keeps_holder() {
        let mut get = get(false);
        get.step = GetStep::Answer;
        let code = Some("AccessDenied".to_string());
        let status = 403;
        let refused = Box::new(MemberAnswer::Refused {
            status,
            code: code.clone(),
        });
        let answer = get.next(event(Event::Blob(BlobEvent::MemberAnswered(refused))));
        assert_eq!(answer, Err(MemberRefusal { status, code }));
    }

    #[test]
    fn unpublished_member_avoided() {
        let mut get = get(false);
        get.first().unwrap();
        assert!(matches!(
            get.next(urls(None)),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        assert!(matches!(get.step, GetStep::Avoid(_)));
    }
}
