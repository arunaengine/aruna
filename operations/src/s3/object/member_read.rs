//! Reads a key of a distributed bucket where an S3 GET of the caller would be answered: a current
//! version at the range's coordinator or a copy, a pinned one at a holder of its records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, NetEffect};
use aruna_core::events::{BlobEvent, Event, NetEvent};
use aruna_core::holder_choice::{HolderCandidate, HolderChoiceError, HolderChoiceOperation, pick};
use aruna_core::jobs::ByteStream;
use aruna_core::member_read::{MemberAnswer, MemberGet};
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::delegation::{
    COPY_READ_HEADER, DELEGATION_HEADER, Delegation, Payload, SignedDelegation, request_digest,
};
use aruna_core::structs::storage::distribution::{DistributionBinding, ReadRoute, RouteError};
use aruna_core::structs::storage::node_info::NodeUrls;
use aruna_core::types::GroupId;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use ulid::Ulid;

use crate::distribution::delegate::{DelegationInput, unsigned_delegation};
use crate::distribution::guard::GuardError;
use crate::distribution::resolve::key_holders;
use crate::distribution::routing::{copy_readers, read_coordinator, route_read};
use crate::s3::object::archive::EntryStatus;

/// How long a delegation lets a caller without a credential act.
const ANONYMOUS_SECS: u64 = 60;
const ACCEPT_ANY: &str = "*/*";
const NOT_FOUND: u16 = 404;
/// The status of a member that gave no answer, as of a coordinator out of reach.
const UNREACHED: u16 = 503;
const FAILED_HERE: u16 = 500;

/// Characters a key keeps unescaped in a forwarded path; `/` separates its segments.
const KEY_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// Where a member of a distributed bucket reads a key for the caller.
#[derive(Debug, PartialEq)]
pub enum MemberRead {
    /// This node reads its own records; `Some` names a range its copy serves while the
    /// coordinator cannot, so the version may be behind.
    Local(Option<Ulid>),
    /// The member that serves the key answered with this version and its bytes.
    Remote {
        version: Ulid,
        blob: ByteStream,
        stale: Option<Ulid>,
    },
}

/// The caller this node verified and sends other members under a delegation.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberCaller {
    pub auth: AuthContext,
    pub group_id: GroupId,
    /// Unix seconds at which the caller's credential expires.
    pub credential_expiry: u64,
}

/// The key and, when pinned, the version a read asks for.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberKey {
    pub bucket: String,
    pub key: String,
    pub version: Option<Ulid>,
}

/// Why a member gave no bytes, as the status and S3 code of its answer.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberRefusal {
    status: u16,
    code: Option<String>,
}

impl MemberRefusal {
    fn new(status: u16) -> Self {
        Self { status, code: None }
    }

    fn unreachable(&self) -> bool {
        self.status >= FAILED_HERE
    }

    fn entry_status(&self) -> EntryStatus {
        match self.code.as_deref() {
            Some("NoSuchKey" | "NoSuchVersion" | "MethodNotAllowed") => EntryStatus::Missing,
            Some("AccessDenied") => EntryStatus::Denied,
            _ => EntryStatus::Unavailable,
        }
    }
}

impl From<IoError> for MemberRefusal {
    fn from(_: IoError) -> Self {
        Self::new(FAILED_HERE)
    }
}

/// A routing failure as a refusal; only a retryable one lets a copy answer instead.
fn guard_refusal(error: &GuardError) -> MemberRefusal {
    MemberRefusal::new(match error {
        GuardError::NotCoordinator { .. }
        | GuardError::RecordMissing(_)
        | GuardError::Unreadable(_)
        | GuardError::Route(
            RouteError::Pending(_) | RouteError::Moving(_) | RouteError::StaleRange(_),
        ) => UNREACHED,
        GuardError::Route(
            RouteError::Deleting(_) | RouteError::NotMember(_) | RouteError::NoRange(_),
        )
        | GuardError::Unbound(_) => 409,
        GuardError::Conversion(_) => FAILED_HERE,
    })
}

/// The range and member one read goes to.
#[derive(Clone, Debug, PartialEq)]
struct MemberTarget {
    set_id: Ulid,
    range_id: Ulid,
    node_id: NodeId,
}

/// What a member answered: its bytes and the version it names for them.
#[derive(Debug, PartialEq)]
struct MemberBody {
    version: Option<String>,
    stream: ByteStream,
}

#[derive(Debug, PartialEq)]
enum GetStep {
    Urls,
    Sign { base: String, headers: Vec<Header> },
    Answer,
    Avoid(MemberRefusal),
}

type Header = (String, String);
type GetTurn = Result<Next<MemberBody>, MemberRefusal>;

/// One GET at one member under a delegation bound to exactly this request. A member that fails
/// is skipped in holder choices for a while.
#[derive(Debug, PartialEq)]
struct MemberGetOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: MemberCaller,
    target: MemberTarget,
    object: MemberKey,
    /// Asks a copy to answer while the range's coordinator cannot.
    copy_read: bool,
    now_ms: u64,
    step: GetStep,
}

impl MemberGetOperation {
    fn query(&self) -> Option<String> {
        (self.object.version).map(|version| format!("versionId={version}"))
    }

    fn failed(&mut self, refusal: MemberRefusal) -> GetTurn {
        if !refusal.unreachable() {
            return Err(refusal);
        }
        let avoid = NetEffect::AvoidHolder {
            node_id: self.target.node_id,
            now_ms: self.now_ms,
        };
        self.step = GetStep::Avoid(refusal);
        Ok(Next::Call(Effect::Net(avoid).into()))
    }

    /// The delegation for exactly this GET and the headers it binds.
    fn sign(&mut self, base: String) -> GetTurn {
        let Some(ids) = self.ids else {
            return self.failed(MemberRefusal::new(FAILED_HERE));
        };
        let mut headers = vec![("accept".to_string(), ACCEPT_ANY.to_string())];
        if self.copy_read {
            headers.push((COPY_READ_HEADER.to_string(), "1".to_string()));
        }
        let bound: Vec<_> = (headers.iter())
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let object = &self.object;
        let query = self.query();
        let digest = request_digest("GET", &object.bucket, &object.key, query.as_deref(), &bound);
        let now_secs = self.now_ms / 1000;
        let auth = &self.caller.auth;
        let credential_expiry = match auth.user_id.is_nil() {
            true => now_secs.saturating_add(ANONYMOUS_SECS),
            false => self.caller.credential_expiry,
        };
        let input = DelegationInput {
            set_id: self.target.set_id,
            range_id: self.target.range_id,
            target: self.target.node_id,
            user_id: auth.user_id,
            group_id: self.caller.group_id,
            credential_id: String::new(),
            restrictions: auth.path_restrictions.clone(),
            credential_expiry,
            operation: "GetObject".to_string(),
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            copy_source: None,
            request_digest: digest,
            payload: Payload::Empty,
        };
        let delegation: Delegation = unsigned_delegation(ids, input, now_secs);
        self.step = GetStep::Sign { base, headers };
        let sign = NetEffect::DelegationSign(Box::new(delegation));
        Ok(Next::Call(Effect::Net(sign).into()))
    }

    fn send(&mut self, base: &str, mut headers: Vec<Header>, signed: SignedDelegation) -> GetTurn {
        let Ok(delegation) = signed.encode() else {
            return self.failed(MemberRefusal::new(FAILED_HERE));
        };
        headers.push((DELEGATION_HEADER.to_string(), delegation));
        let object = &self.object;
        let path = utf8_percent_encode(&object.key, KEY_SET);
        let mut url = format!("{}/{}/{path}", base.trim_end_matches('/'), object.bucket);
        if let Some(query) = self.query() {
            url = format!("{url}?{query}");
        }
        self.step = GetStep::Answer;
        let get = BlobEffect::MemberGet(Box::new(MemberGet { url, headers }));
        Ok(Next::Call(Effect::Blob(get).into()))
    }
}

impl Operation for MemberGetOperation {
    type Output = MemberBody;
    type Error = MemberRefusal;

    fn first(&mut self) -> GetTurn {
        let urls = NodeUrlsOperation::new(self.target.node_id);
        Ok(Next::Call(IoCall::child(urls)))
    }

    fn next(&mut self, result: IoResult) -> GetTurn {
        match std::mem::replace(&mut self.step, GetStep::Answer) {
            GetStep::Urls => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                match urls.ok().flatten().and_then(|urls| urls.s3) {
                    Some(base) => self.sign(base),
                    None => self.failed(MemberRefusal::new(UNREACHED)),
                }
            }
            GetStep::Sign { base, headers } => match result.event()? {
                Event::Net(NetEvent::DelegationSign(Some(signed))) => {
                    self.send(&base, headers, *signed)
                }
                Event::Net(NetEvent::DelegationSign(None)) => {
                    self.failed(MemberRefusal::new(FAILED_HERE))
                }
                other => Err(unexpected("Event::Net(NetEvent::DelegationSign)", &other)),
            },
            GetStep::Answer => match result.event()? {
                Event::Blob(BlobEvent::MemberAnswered(answer)) => match *answer {
                    MemberAnswer::Body { version, stream } => {
                        Ok(Next::Done(MemberBody { version, stream }))
                    }
                    MemberAnswer::Refused { status, code } => {
                        self.failed(MemberRefusal { status, code })
                    }
                    MemberAnswer::Silent => self.failed(MemberRefusal::new(UNREACHED)),
                },
                Event::Blob(BlobEvent::Error(_)) => self.failed(MemberRefusal::new(FAILED_HERE)),
                other => Err(unexpected("Event::Blob(BlobEvent::MemberAnswered)", &other)),
            },
            GetStep::Avoid(refusal) => {
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

#[derive(Debug, PartialEq)]
enum ReadStep {
    Coordinator,
    Route,
    Holders,
    Choice,
    /// The chosen holder; on a miss at a replica the coordinator is asked.
    Holder,
    Coordinating,
    Readers(MemberRefusal),
    Copy(MemberRefusal),
}

type ReadTurn = Result<Next<MemberRead>, EntryStatus>;

/// Reads `key` for a caller this node verified. A member's refusal ends the read with the entry
/// status it stands for; only a current version may come from a copy.
#[derive(Debug, PartialEq)]
pub struct MemberReadOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: MemberCaller,
    binding: DistributionBinding,
    object: MemberKey,
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
        caller: MemberCaller,
        binding: DistributionBinding,
        object: MemberKey,
    ) -> Self {
        Self {
            ids,
            caller,
            binding,
            object,
            seed: rand::random(),
            now_ms: 0,
            range_id: None,
            coordinator: None,
            holder: None,
            copies: VecDeque::new(),
            step: ReadStep::Coordinator,
        }
    }

    fn pinned(&self) -> bool {
        self.object.version.is_some()
    }

    fn get(&self, node_id: NodeId, range_id: Ulid, copy_read: bool) -> IoCall {
        IoCall::child(MemberGetOperation {
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
            step: GetStep::Urls,
        })
    }

    fn ask_holder(&mut self, node_id: NodeId) -> ReadTurn {
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.holder = Some(node_id);
        self.step = ReadStep::Holder;
        Ok(Next::Call(self.get(node_id, range_id, false)))
    }

    fn remote(body: MemberBody, stale: Option<Ulid>) -> ReadTurn {
        let version = (body.version.as_deref())
            .and_then(|version| Ulid::from_string(version).ok())
            .ok_or(EntryStatus::Unavailable)?;
        Ok(Next::Done(MemberRead::Remote {
            version,
            blob: body.stream,
            stale,
        }))
    }

    /// While the coordinator cannot answer, a current version comes from a copy.
    fn failed(&mut self, refusal: MemberRefusal) -> ReadTurn {
        if self.pinned() || !refusal.unreachable() {
            return Err(refusal.entry_status());
        }
        let readers = copy_readers(&self.binding, &self.object.key);
        self.step = ReadStep::Readers(refusal);
        Ok(Next::Call(IoCall::child(readers)))
    }

    fn next_copy(&mut self, first_refusal: MemberRefusal) -> ReadTurn {
        let Some(node_id) = self.copies.pop_front() else {
            return Err(first_refusal.entry_status());
        };
        let range_id = self.range_id.ok_or(IoError::Unfinished)?;
        self.step = ReadStep::Copy(first_refusal);
        Ok(Next::Call(self.get(node_id, range_id, true)))
    }
}

impl Operation for MemberReadOperation {
    type Output = MemberRead;
    type Error = EntryStatus;

    fn first(&mut self) -> ReadTurn {
        let (binding, key) = (&self.binding, &self.object.key);
        if self.pinned() {
            self.step = ReadStep::Route;
            return Ok(Next::Call(IoCall::child(route_read(binding, key, false))));
        }
        self.step = ReadStep::Coordinator;
        Ok(Next::Call(IoCall::child(read_coordinator(binding, key))))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match std::mem::replace(&mut self.step, ReadStep::Holder) {
            ReadStep::Coordinator => match result.child::<Option<(Ulid, NodeId)>, GuardError>()? {
                Ok(None) => Ok(Next::Done(MemberRead::Local(None))),
                Ok(Some((range_id, node_id))) => {
                    self.range_id = Some(range_id);
                    self.coordinator = Some(node_id);
                    self.ask_holder(node_id)
                }
                Err(error) => self.failed(guard_refusal(&error)),
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
                Err(error) => self.failed(guard_refusal(&error)),
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
                Err(refusal) if refusal.status == NOT_FOUND && self.holder != self.coordinator => {
                    let coordinator = self.coordinator.ok_or(IoError::Unfinished)?;
                    let range_id = self.range_id.ok_or(IoError::Unfinished)?;
                    self.step = ReadStep::Coordinating;
                    Ok(Next::Call(self.get(coordinator, range_id, false)))
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
