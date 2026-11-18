//! The decisions behind a run's host calls: what an intent found says, how a mutating call's end
//! is recorded, and the private state and object calls made only of plugin rows and reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{CallIntent, StateRefusal, StateRemoval, StateWrite};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_plugins::broker::{ErrorKind, HostError, Intent, ObjectInfo, ObjectPage};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::check::{Authorize, CallError, matching};
use super::intents::{Begun, begin, intent_row_key, settled_row, unknown_row};
use super::marks::is_marked;
use super::objects::LocalObject;
use super::rows::{StoreError, delete_rows, mapped_child, write_rows};
use super::state::{PutError, put_state, remove_state};
use super::unavailable;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::list::{ListBucketInput, ListBucketOperation, ListContinuationToken};

/// Entries of one object listing page.
pub const OBJECT_PAGE: usize = 100;

/// A host call that ended without a reply; `changed` says the run called differently than an
/// earlier attempt, so the run fails as changed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}", .error.message)]
pub struct CallFailure {
    pub error: HostError,
    pub changed: bool,
}

impl From<HostError> for CallFailure {
    fn from(error: HostError) -> Self {
        Self {
            error,
            changed: false,
        }
    }
}

impl From<CallError> for CallFailure {
    fn from(error: CallError) -> Self {
        error.0.into()
    }
}

impl From<IoError> for CallFailure {
    fn from(error: IoError) -> Self {
        unavailable(error.to_string()).into()
    }
}

impl From<StoreError> for CallFailure {
    fn from(error: StoreError) -> Self {
        unavailable(error.0).into()
    }
}

/// The core intent of a guest call.
pub fn call_intent(intent: &Intent) -> CallIntent {
    CallIntent {
        ordinal: intent.ordinal,
        digest: intent.digest,
    }
}

/// The earlier reply of a call, `None` when its effect may be dispatched now, or why not.
pub fn begun_reply(begun: Begun) -> Result<Option<String>, CallFailure> {
    match begun {
        Begun::Fresh => Ok(None),
        Begun::Settled(reply) => Ok(Some(reply)),
        Begun::Changed => Err(CallFailure {
            error: HostError::new(
                ErrorKind::Conflict,
                "the call differs from an earlier attempt",
            ),
            changed: true,
        }),
        Begun::Unknown(reason) => Err(HostError::new(ErrorKind::UnknownOutcome, reason).into()),
    }
}

/// How a mutating call ended without a reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallEnd {
    /// It had no effect.
    None(HostError),
    /// It may have had its effect.
    Maybe(String),
}

#[derive(Debug, PartialEq)]
enum RecordPhase {
    Settling(String),
    Clearing(HostError),
    MarkingUnknown(String),
}

/// Records a fresh effect's outcome in its intent: a reply settles it, a refusal that had no
/// effect clears it, and an error that may have committed leaves it unknown.
#[derive(Debug, PartialEq)]
pub struct RecordCall {
    run_id: Ulid,
    intent: CallIntent,
    outcome: Option<Result<String, CallEnd>>,
    phase: Option<RecordPhase>,
}

impl RecordCall {
    pub fn new(run_id: Ulid, intent: CallIntent, outcome: Result<String, CallEnd>) -> Self {
        Self {
            run_id,
            intent,
            outcome: Some(outcome),
            phase: None,
        }
    }

    fn mark_unknown(&mut self, reason: String) -> Result<Next<String>, CallFailure> {
        match unknown_row(self.run_id, &self.intent, &reason) {
            Ok(row) => {
                self.phase = Some(RecordPhase::MarkingUnknown(reason));
                Ok(Next::Call(IoCall::child(write_rows(vec![row]))))
            }
            // A failure leaves the intent pending, which a later attempt reads as unknown too.
            Err(_) => Err(unknown_outcome(reason)),
        }
    }
}

fn unknown_outcome(reason: String) -> CallFailure {
    HostError::new(ErrorKind::UnknownOutcome, reason).into()
}

impl Operation for RecordCall {
    type Output = String;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<String>, CallFailure> {
        match self.outcome.take().ok_or(IoError::Unfinished)? {
            Ok(reply) => match settled_row(self.run_id, &self.intent, reply.clone()) {
                Ok(row) => {
                    self.phase = Some(RecordPhase::Settling(reply));
                    Ok(Next::Call(IoCall::child(write_rows(vec![row]))))
                }
                Err(error) => self.mark_unknown(error.0),
            },
            Err(CallEnd::None(error)) => {
                self.phase = Some(RecordPhase::Clearing(error));
                let key = intent_row_key(self.run_id, &self.intent);
                Ok(Next::Call(IoCall::child(delete_rows(vec![key]))))
            }
            Err(CallEnd::Maybe(reason)) => self.mark_unknown(reason),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CallFailure> {
        let written = result.child::<(), StoreError>()?;
        match self.phase.take().ok_or(IoError::Unfinished)? {
            RecordPhase::Settling(reply) => match written {
                Ok(()) => Ok(Next::Done(reply)),
                Err(error) => self.mark_unknown(error.0),
            },
            RecordPhase::Clearing(error) => {
                written?;
                Err(error.into())
            }
            RecordPhase::MarkingUnknown(reason) => Err(unknown_outcome(reason)),
        }
    }
}

/// The change of a private state value a call asks for.
#[derive(Debug, PartialEq)]
pub enum StateChange {
    Put(StateWrite),
    Remove(StateRemoval),
}

impl StateChange {
    fn intent(&self) -> &CallIntent {
        match self {
            Self::Put(put) => &put.intent,
            Self::Remove(remove) => &remove.intent,
        }
    }
}

#[derive(Debug, PartialEq)]
enum StatePhase {
    Beginning,
    Writing,
    Recording,
}

/// One state write or removal through the run's call history; answers the new version, or 0 for
/// a removal. A refusal clears its intent, and a write that may have happened leaves it unknown.
#[derive(Debug, PartialEq)]
pub struct StateCall {
    run_id: Ulid,
    install_id: Ulid,
    intent: CallIntent,
    removal: bool,
    change: Option<StateChange>,
    phase: StatePhase,
}

impl StateCall {
    pub fn new((run_id, install_id): (Ulid, Ulid), change: StateChange) -> Self {
        Self {
            run_id,
            install_id,
            intent: change.intent().clone(),
            removal: matches!(change, StateChange::Remove(_)),
            change: Some(change),
            phase: StatePhase::Beginning,
        }
    }

    fn record(&mut self, end: CallEnd) -> Next<u64> {
        self.phase = StatePhase::Recording;
        let record = RecordCall::new(self.run_id, self.intent.clone(), Err(end));
        Next::Call(IoCall::child(record))
    }

    fn written(&mut self, written: Result<u64, PutError>) -> Next<u64> {
        match written {
            Ok(version) => Next::Done(version),
            Err(PutError::Refused(message, refusal)) => {
                let kind = match refusal {
                    StateRefusal::Limit => ErrorKind::Limit,
                    StateRefusal::Conflict => ErrorKind::Conflict,
                };
                self.record(CallEnd::None(HostError::new(kind, message)))
            }
            Err(PutError::Store(error)) => self.record(CallEnd::Maybe(error.0)),
        }
    }
}

impl Operation for StateCall {
    type Output = u64;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<u64>, CallFailure> {
        let begun = begin(self.run_id, self.intent.clone());
        Ok(Next::Call(IoCall::child(begun)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, CallFailure> {
        match self.phase {
            StatePhase::Beginning => {
                let begun = result.child::<Begun, StoreError>()??;
                if let Some(reply) = begun_reply(begun)? {
                    return Ok(Next::Done(match self.removal {
                        true => 0,
                        false => (reply.parse())
                            .map_err(|_| unavailable("a stored reply is not a version"))?,
                    }));
                }
                self.phase = StatePhase::Writing;
                let ids = (self.run_id, self.install_id);
                Ok(Next::Call(
                    match self.change.take().ok_or(IoError::Unfinished)? {
                        StateChange::Put(put) => IoCall::child(put_state(ids.0, ids.1, put)),
                        StateChange::Remove(remove) => {
                            IoCall::child(remove_state(ids.0, ids.1, remove))
                        }
                    },
                ))
            }
            StatePhase::Writing => Ok(match self.removal {
                true => {
                    let removed = result.child::<(), PutError>()?;
                    self.written(removed.map(|()| 0))
                }
                false => {
                    let written = result.child::<u64, PutError>()?;
                    self.written(written)
                }
            }),
            StatePhase::Recording => {
                result.child::<String, CallFailure>()??;
                Err(IoError::Unfinished.into())
            }
        }
    }
}

/// The version id a plugin gave as text.
pub fn version_id(text: &str) -> Result<Ulid, HostError> {
    let invalid = || HostError::new(ErrorKind::Invalid, format!("`{text}` is not a version id"));
    Ulid::from_string(text).map_err(|_| invalid())
}

/// The call that asks whether `bucket/key` at `version` is marked; a version that is no id is not.
fn marked_call(bucket: &str, key: &str, version: &str) -> Result<Option<IoCall>, StoreError> {
    let Ok(version_id) = Ulid::from_string(version) else {
        return Ok(None);
    };
    Ok(Some(IoCall::child(is_marked((bucket, key, version_id))?)))
}

#[derive(Debug, PartialEq)]
enum InfoPhase {
    Locating,
    Head(String, String),
    Marks(Box<ObjectInfo>),
}

/// What a head of an object says, without the rest of its record.
#[derive(Debug, PartialEq, Eq)]
struct HeadFacts {
    size: u64,
    version: String,
}

/// Size, version and mark of one object the call check admitted.
#[derive(Debug, PartialEq)]
pub struct ObjectInfoCall {
    locate: Option<LocalObject>,
    path: String,
    version: Option<Ulid>,
    phase: InfoPhase,
}

impl ObjectInfoCall {
    pub fn new(
        place: (RealmId, NodeId),
        install: &PluginInstall,
        path: &str,
        version: Option<&str>,
    ) -> Result<Self, HostError> {
        Ok(Self {
            locate: Some(LocalObject::new(place, install, path)),
            path: path.to_string(),
            version: version.map(version_id).transpose()?,
            phase: InfoPhase::Locating,
        })
    }
}

impl Operation for ObjectInfoCall {
    type Output = ObjectInfo;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<ObjectInfo>, CallFailure> {
        let locate = self.locate.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(locate)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectInfo>, CallFailure> {
        match std::mem::replace(&mut self.phase, InfoPhase::Locating) {
            InfoPhase::Locating => {
                let (bucket, key) = result.child::<(String, String), CallError>()??;
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: self.version,
                });
                self.phase = InfoPhase::Head(bucket, key);
                // A given version is checked as the current one: the same grant, its own mark.
                Ok(Next::Call(mapped_child(head, |head| {
                    head.map(|head| HeadFacts {
                        size: head.location.map_or(0, |location| location.blob_size),
                        version: (head.resolved_version_id.map(|id| id.to_string()))
                            .unwrap_or_default(),
                    })
                    .map_err(drop)
                })))
            }
            InfoPhase::Head(bucket, key) => {
                let head = result.child::<HeadFacts, ()>()?.map_err(|()| {
                    HostError::new(ErrorKind::NotFound, format!("no object at {}", self.path))
                })?;
                let call = marked_call(&bucket, &key, &head.version)?;
                let info = ObjectInfo {
                    path: format!("{bucket}/{key}"),
                    size: head.size,
                    version: head.version,
                    marked: false,
                };
                match call {
                    Some(call) => {
                        self.phase = InfoPhase::Marks(Box::new(info));
                        Ok(Next::Call(call))
                    }
                    None => Ok(Next::Done(info)),
                }
            }
            InfoPhase::Marks(mut info) => {
                info.marked = result.child::<bool, StoreError>()??;
                Ok(Next::Done(*info))
            }
        }
    }
}

/// One listed object: key, size and version.
type ListedObject = (String, u64, String);

/// What a listing says, reduced to what the call answers.
#[derive(Debug, PartialEq, Eq)]
struct Listed {
    objects: Vec<ListedObject>,
    cursor: Option<String>,
}

#[derive(Debug, PartialEq)]
enum VisiblePhase {
    Authorizing(ListedObject),
    Marking(Box<ObjectInfo>),
}

/// The listed objects of a local bucket the account may read, each named by the permission path
/// of its own bucket and key, never by the text of the listing prefix. Only an unavailable check
/// fails; a target the grants or the authorization refuse is left out.
#[derive(Debug, PartialEq)]
pub struct VisibleObjects {
    place: (RealmId, NodeId),
    install: PluginInstall,
    bucket: String,
    listed: VecDeque<ListedObject>,
    visible: Vec<ObjectInfo>,
    phase: Option<VisiblePhase>,
}

impl VisibleObjects {
    pub fn new(
        place: (RealmId, NodeId),
        install: &PluginInstall,
        bucket: &str,
        listed: Vec<ListedObject>,
    ) -> Self {
        Self {
            place,
            install: install.clone(),
            bucket: bucket.to_string(),
            listed: listed.into(),
            visible: Vec::new(),
            phase: None,
        }
    }

    /// The authorization of the next object the grants cover, or the finished list.
    fn advance(&mut self) -> Next<Vec<ObjectInfo>> {
        let read = Permission::ObjectsRead;
        while let Some((key, size, version)) = self.listed.pop_front() {
            let (realm, node) = self.place;
            let group = self.install.group_id;
            let path = object_permission_path(realm, group, node, &self.bucket, &key);
            let Ok(patterns) = matching(&self.install, read, &path) else {
                continue;
            };
            let check = Authorize::call(&self.install, read, &path, &patterns);
            self.phase = Some(VisiblePhase::Authorizing((key, size, version)));
            return Next::Call(IoCall::child(check));
        }
        Next::Done(std::mem::take(&mut self.visible))
    }
}

impl Operation for VisibleObjects {
    type Output = Vec<ObjectInfo>;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, CallFailure> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallFailure> {
        match self.phase.take().ok_or(IoError::Unfinished)? {
            VisiblePhase::Authorizing((key, size, version)) => {
                match result.child::<(), CallError>()? {
                    Ok(()) => {}
                    Err(CallError(error)) if error.kind == ErrorKind::Unavailable => {
                        return Err(error.into());
                    }
                    Err(_) => return Ok(self.advance()),
                }
                let call = marked_call(&self.bucket, &key, &version)?;
                let info = ObjectInfo {
                    path: format!("{}/{key}", self.bucket),
                    size,
                    version,
                    marked: false,
                };
                match call {
                    Some(call) => {
                        self.phase = Some(VisiblePhase::Marking(Box::new(info)));
                        Ok(Next::Call(call))
                    }
                    None => {
                        self.visible.push(info);
                        Ok(self.advance())
                    }
                }
            }
            VisiblePhase::Marking(mut info) => {
                info.marked = result.child::<bool, StoreError>()??;
                self.visible.push(*info);
                Ok(self.advance())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ListPhase {
    Locating,
    Listing(String),
    Filtering(Option<String>),
}

/// One page of a local bucket's objects the account may read.
#[derive(Debug, PartialEq)]
pub struct ObjectListCall {
    place: (RealmId, NodeId),
    install: PluginInstall,
    locate: Option<LocalObject>,
    token: Option<ListContinuationToken>,
    phase: ListPhase,
}

impl ObjectListCall {
    pub fn new(
        place: (RealmId, NodeId),
        install: &PluginInstall,
        prefix_path: &str,
        cursor: Option<&str>,
    ) -> Result<Self, HostError> {
        let invalid = || HostError::new(ErrorKind::Invalid, "bad cursor");
        let token = cursor
            .map(|cursor| {
                let bytes = hex::decode(cursor).map_err(|_| invalid())?;
                ListContinuationToken::from_bytes(&bytes).map_err(|_| invalid())
            })
            .transpose()?;
        Ok(Self {
            place,
            install: install.clone(),
            locate: Some(LocalObject::new(place, install, prefix_path)),
            token,
            phase: ListPhase::Locating,
        })
    }
}

impl Operation for ObjectListCall {
    type Output = ObjectPage;
    type Error = CallFailure;

    fn first(&mut self) -> Result<Next<ObjectPage>, CallFailure> {
        let locate = self.locate.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(locate)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectPage>, CallFailure> {
        match std::mem::replace(&mut self.phase, ListPhase::Locating) {
            ListPhase::Locating => {
                let (bucket, key) = result.child::<(String, String), CallError>()??;
                let list = ListBucketOperation::new(ListBucketInput {
                    bucket: bucket.clone(),
                    group_id: self.install.group_id,
                    continuation_token: self.token.take(),
                    max_keys: Some(OBJECT_PAGE),
                    prefix: (!key.is_empty()).then_some(key),
                    delimiter: None,
                    start_after: None,
                });
                self.phase = ListPhase::Listing(bucket);
                Ok(Next::Call(mapped_child(list, |listed| {
                    let listed = listed.map_err(|error| error.to_string())?;
                    let cursor = (listed.continuation_token)
                        .map(|token| token.to_bytes().map(hex::encode))
                        .transpose()
                        .map_err(|error| error.to_string())?;
                    let objects = (listed.objects.into_iter())
                        .map(|object| {
                            let size = object.location.map_or(0, |location| location.blob_size);
                            (object.head.key, size, object.version_id.to_string())
                        })
                        .collect();
                    Ok::<_, String>(Listed { objects, cursor })
                })))
            }
            ListPhase::Listing(bucket) => {
                let listed = result.child::<Listed, String>()?.map_err(unavailable)?;
                self.phase = ListPhase::Filtering(listed.cursor);
                let visible =
                    VisibleObjects::new(self.place, &self.install, &bucket, listed.objects);
                Ok(Next::Call(IoCall::child(visible)))
            }
            ListPhase::Filtering(cursor) => {
                let objects = result.child::<Vec<ObjectInfo>, CallFailure>()??;
                Ok(Next::Done(ObjectPage { objects, cursor }))
            }
        }
    }
}

#[cfg(test)]
#[path = "host_calls_tests.rs"]
mod tests;
