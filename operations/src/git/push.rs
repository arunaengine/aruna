//! Accepts a validated push: stores its pack and publishes its ref updates before refs move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
pub use aruna_core::git::PushRequest;
use aruna_core::git::{
    GitChange, GitRecord, LfsObject, PendingMerge, RefUpdate, StoredObject, ZERO_OID, decode_push,
    valid_path, valid_ref,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use ulid::Ulid;

use super::access::{Repository, RepositoryOperation};
use super::calls::node_of;
use super::objects::MarkImportedOperation;
use super::publish::PublishRecordOperation;
use super::state::{Ancestry, GitState, reduce};
use super::{GitError, objects, records};
use crate::driver::DriverContext;

type Document = MetadataRegistryRecord;

const MAX_ITEMS: usize = 100_000;

/// The request and pack of a push body.
pub fn decode(body: Bytes) -> Result<(PushRequest, Bytes), GitError> {
    decode_push(body).ok_or(GitError::Invalid)
}

fn objects_in(pack: &[u8]) -> u32 {
    pack.get(8..12)
        .and_then(|count| <[u8; 4]>::try_from(count).ok())
        .map_or(0, u32::from_be_bytes)
}

/// What the pending call of accepting a push answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptStep {
    Open,
    Scan,
    Copy,
    Publish,
}

/// Runs inside the receive hook of a push that already holds the repository, so it must not
/// project. Locked paths of other users and unknown LFS objects refuse the push.
pub fn accept(
    context: &DriverContext,
    auth: &AuthContext,
    (id, key): (Ulid, &str),
    request: PushRequest,
    pack: Bytes,
) -> AcceptPushOperation {
    let node = node_of(context);
    AcceptPushOperation {
        node,
        auth: auth.clone(),
        id,
        key: key.to_string(),
        request,
        pack: Some(pack),
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        step: AcceptStep::Open,
        document: None,
        state: None,
        oids: VecDeque::new(),
        asked: None,
        stored: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub struct AcceptPushOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    id: Ulid,
    key: String,
    request: PushRequest,
    pack: Option<Bytes>,
    open: Option<RepositoryOperation>,
    step: AcceptStep,
    document: Option<Document>,
    state: Option<GitState>,
    oids: VecDeque<String>,
    /// Whether the object whose copy is being read has a valid id.
    asked: Option<bool>,
    stored: Vec<StoredObject>,
}

type AcceptTurn = Result<Next<GitRecord>, GitError>;

impl AcceptPushOperation {
    fn opened(&mut self, document: Document) -> AcceptTurn {
        if !super::push_key_valid(self.id, &self.key) {
            return Err(GitError::NotHook);
        }
        let request = &self.request;
        if request.refs.is_empty()
            || request.lfs.len() + request.paths.len() > MAX_ITEMS
            || !(request.refs.iter()).all(|update| valid_ref(&update.name, false))
            || !request.paths.iter().all(|path| valid_path(path))
        {
            return Err(GitError::Invalid);
        }
        self.document = Some(document);
        self.step = AcceptStep::Scan;
        Ok(Next::Call(IoCall::child(records::scan(self.id))))
    }

    fn scanned(&mut self, records: &[GitRecord]) -> AcceptTurn {
        let (mut state, _) = reduce(records, &Ancestry::new());
        // The refs this node served the pushing client; a replay without ancestry answers can
        // disagree with them about fast-forwarded branches.
        if let Some(served) = super::project::served(self.id) {
            state.refs = served;
        }
        for update in &self.request.refs {
            if update.name == "refs/heads/main" && update.new == ZERO_OID {
                return Err(GitError::Refused(
                    "the main branch cannot be deleted".into(),
                ));
            }
            let current = state.refs.get(&update.name);
            if current.map_or(ZERO_OID, String::as_str) != update.old {
                return Err(GitError::Stale);
            }
        }
        unlocked(&state, &self.auth, &self.request.paths)?;
        self.oids = self.request.lfs.iter().cloned().collect();
        self.state = Some(state);
        self.next_object()
    }

    fn keep(&mut self, location: Option<StoredObject>, valid: bool) -> Result<(), GitError> {
        self.stored
            .push(location.filter(|_| valid).ok_or(GitError::Invalid)?);
        Ok(())
    }

    /// A known object keeps its original location, so access is decided for the original.
    fn next_object(&mut self) -> AcceptTurn {
        while let Some(oid) = self.oids.pop_front() {
            let valid = LfsObject {
                oid: oid.clone(),
                size: 0,
            }
            .valid();
            let state = self.state.as_ref().ok_or(IoError::Unfinished)?;
            if let Some(original) = state.lfs.get(&oid).cloned() {
                self.keep(Some(original), valid)?;
                continue;
            }
            let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
            self.asked = Some(valid);
            self.step = AcceptStep::Copy;
            return Ok(Next::Call(IoCall::child(objects::copy(document, &oid))));
        }
        self.publish()
    }

    /// Pushed metadata follows the recorded push; the hook already checked that it merges.
    fn publish(&mut self) -> AcceptTurn {
        let refs = std::mem::take(&mut self.request.refs);
        let main =
            (refs.iter()).find(|update| update.name == "refs/heads/main" && update.new != ZERO_OID);
        let merge = main.map(|update| PendingMerge {
            user_id: self.auth.user_id,
            old: update.old.clone(),
            new: update.new.clone(),
        });
        let pack = self.pack.take().ok_or(IoError::Unfinished)?;
        let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
        let written = (pack, std::mem::take(&mut self.stored));
        let made = (Vec::new(), merge);
        let publish = record_publish(self.node, &self.auth, document, refs, written, made)?;
        self.step = AcceptStep::Publish;
        Ok(Next::Call(IoCall::child(publish)))
    }
}

impl Operation for AcceptPushOperation {
    type Output = GitRecord;
    type Error = GitError;

    fn first(&mut self) -> AcceptTurn {
        let open = self.open.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> AcceptTurn {
        match self.step {
            AcceptStep::Open => self.opened(result.child::<Repository, GitError>()??.0),
            AcceptStep::Scan => {
                let records = result.child::<Vec<GitRecord>, GitError>()??;
                self.scanned(&records)
            }
            AcceptStep::Copy => {
                let copy = result.child::<Option<StoredObject>, GitError>()??;
                let valid = self.asked.take().ok_or(IoError::Unfinished)?;
                self.keep(copy, valid)?;
                self.next_object()
            }
            AcceptStep::Publish => Ok(Next::Done(result.child::<GitRecord, GitError>()??)),
        }
    }
}

/// Refuses changes to files another user locked.
pub(super) fn unlocked(
    state: &GitState,
    auth: &AuthContext,
    paths: &[String],
) -> Result<(), GitError> {
    let locked =
        |path: &&String| (state.locks.get(*path)).is_some_and(|lock| lock.user_id != auth.user_id);
    match paths.iter().find(locked) {
        Some(path) => Err(GitError::Locked(path.clone())),
        None => Ok(()),
    }
}

/// The record of a non-empty pack and its ref updates. `made` lists commits this node created
/// itself; a pending merge is written in the same step.
pub(super) fn record_publish(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    refs: Vec<RefUpdate>,
    (pack, lfs): (Bytes, Vec<StoredObject>),
    (made, merge): (Vec<String>, Option<PendingMerge>),
) -> Result<PublishRecordOperation, GitError> {
    let pack = (objects_in(&pack) != 0).then_some(pack);
    let described = pack.as_ref().map(objects::describe_pack).transpose()?;
    let change = GitChange::Objects {
        pack: described,
        refs,
        lfs,
        revision: None,
        digest: None,
        made,
    };
    let extra = match &merge {
        Some(merge) => vec![super::pending::entry(document.document_id, merge)?],
        None => Vec::new(),
    };
    let extras = (pack, extra);
    Ok(PublishRecordOperation::new(
        node,
        document,
        auth.user_id,
        change,
        extras,
    ))
}

/// Like [`record_publish`] for a pack this node made itself, outside a Git transfer: its cache
/// holds the objects already. A push must not use it, since the transfer holds the repository.
pub(super) fn record_made(
    context: &DriverContext,
    auth: &AuthContext,
    document: &Document,
    refs: Vec<RefUpdate>,
    written: (Bytes, Vec<StoredObject>),
    made: (Vec<String>, Option<PendingMerge>),
) -> RecordMadeOperation {
    let node = node_of(context);
    RecordMadeOperation {
        publish: Some(record_publish(node, auth, document, refs, written, made)),
        actor: auth.user_id,
        record: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RecordMadeOperation {
    publish: Option<Result<PublishRecordOperation, GitError>>,
    actor: aruna_core::UserId,
    record: Option<GitRecord>,
}

impl Operation for RecordMadeOperation {
    type Output = GitRecord;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<GitRecord>, GitError> {
        let publish = self.publish.take().ok_or(IoError::Unfinished)??;
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GitRecord>, GitError> {
        if let Some(record) = self.record.take() {
            result.child::<(), IoError>()?.ok();
            return Ok(Next::Done(record));
        }
        let record = result.child::<GitRecord, GitError>()??;
        let GitChange::Objects {
            pack: Some(pack), ..
        } = &record.change
        else {
            return Ok(Next::Done(record));
        };
        let mark: MarkImportedOperation =
            objects::mark_imported((record.document_id, self.actor), pack);
        self.record = Some(record);
        Ok(Next::Call(IoCall::child(mark)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{auth, child, dataset, node};
    use super::*;
    use aruna_core::git::encode_push;

    #[test]
    fn body_roundtrip() {
        let request = PushRequest {
            refs: vec![RefUpdate {
                name: "refs/heads/main".into(),
                old: "0".repeat(40),
                new: "a".repeat(40),
            }],
            lfs: vec!["b".repeat(64)],
            paths: vec!["isa.investigation.xlsx".into()],
        };
        let body = encode_push(&request, b"PACK\0\0\0\x02\0\0\0\x03rest").expect("encodes");
        let (decoded, pack) = decode(Bytes::from(body)).expect("decodes");
        assert_eq!(decoded, request);
        assert_eq!(objects_in(&pack), 3);
        assert!(decode(Bytes::from_static(&[0, 0, 1, 0, b'{'])).is_err());
    }

    fn update(old: &str) -> RefUpdate {
        RefUpdate {
            name: "refs/heads/draft".into(),
            old: old.into(),
            new: "a".repeat(40),
        }
    }

    fn pushing(refs: Vec<RefUpdate>, lfs: Vec<String>) -> AcceptPushOperation {
        AcceptPushOperation {
            node: Some(node(1)),
            auth: auth(),
            id: dataset().document_id,
            key: "key".to_string(),
            request: PushRequest {
                refs,
                lfs,
                paths: Vec::new(),
            },
            pack: Some(Bytes::new()),
            open: None,
            step: AcceptStep::Scan,
            document: Some(dataset()),
            state: None,
            oids: VecDeque::new(),
            asked: None,
            stored: Vec::new(),
        }
    }

    // A branch that moved since the client looked refuses the push.
    #[test]
    fn moved_branch_stale() {
        let mut accept = pushing(vec![update(&"b".repeat(40))], Vec::new());
        let answer = accept.next(child(Ok::<_, GitError>(Vec::<GitRecord>::new())));
        assert_eq!(answer, Err(GitError::Stale));
    }

    // An LFS object this node never received refuses the push.
    #[test]
    fn unknown_object_invalid() {
        let oid = "c".repeat(64);
        let mut accept = pushing(vec![update(ZERO_OID)], vec![oid]);
        let scanned = child(Ok::<_, GitError>(Vec::<GitRecord>::new()));
        assert!(matches!(accept.next(scanned), Ok(Next::Call(_))));
        assert_eq!(accept.step, AcceptStep::Copy);
        let missing = child(Ok::<Option<StoredObject>, GitError>(None));
        assert_eq!(accept.next(missing), Err(GitError::Invalid));
    }

    // An empty pack is not stored, so the record names none.
    #[test]
    fn empty_pack_dropped() {
        let refs = vec![update(ZERO_OID)];
        let written = (Bytes::new(), Vec::new());
        let publish = record_publish(
            Some(node(1)),
            &auth(),
            &dataset(),
            refs,
            written,
            (Vec::new(), None),
        );
        assert!(publish.is_ok());
    }
}
