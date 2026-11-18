//! Accepts a validated push: stores its pack and publishes its ref updates before refs move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
pub use aruna_core::git::PushRequest;
use aruna_core::git::{
    GitChange, GitRecord, LfsObject, PendingMerge, PushProof, RefUpdate, Refs, StoredObject,
    ZERO_OID, decode_push, valid_path, valid_ref,
};
use aruna_core::git_state::{Ancestry, GitState};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use ulid::Ulid;

use super::access::{Repository, RepositoryOperation};
use super::calls::{
    push_key_answer, push_key_call, sequence_answer, sequence_call, served_answer, served_call,
};
use super::publish::PublishRecordOperation;
use super::state::reduce;
use super::{GitError, objects, records};

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
    Key,
    Scan,
    Served,
    Copy,
    Sequence,
    Publish,
}

/// Runs inside the receive hook of a push that already holds the repository, so it must not
/// project. Locked paths of other users and unknown LFS objects refuse the push.
pub fn accept(
    node: Option<NodeId>,
    auth: &AuthContext,
    (id, key): (Ulid, &str),
    request: PushRequest,
    pack: Bytes,
) -> AcceptPushOperation {
    AcceptPushOperation {
        node,
        auth: auth.clone(),
        id,
        proof: PushProof(key.to_string()),
        request,
        pack: Some(pack),
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        step: AcceptStep::Open,
        document: None,
        state: None,
        oids: VecDeque::new(),
        asked: None,
        stored: Vec::new(),
        merge: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct AcceptPushOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    id: Ulid,
    proof: PushProof,
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
    merge: Option<PendingMerge>,
}

type AcceptTurn = Result<Next<GitRecord>, GitError>;

impl AcceptPushOperation {
    fn key_checked(&mut self, valid: bool) -> AcceptTurn {
        if !valid {
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
        self.step = AcceptStep::Scan;
        Ok(Next::Call(IoCall::child(records::scan(self.id))))
    }

    /// The refs this node served the pushing client replace the reduced ones: a replay without
    /// ancestry answers can disagree with them about fast-forwarded branches.
    fn check_refs(&mut self, served: Option<Refs>) -> AcceptTurn {
        let mut state = self.state.take().ok_or(IoError::Unfinished)?;
        if let Some(served) = served {
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
        self.order_merge()
    }

    /// Pushed metadata follows the recorded push; the hook already checked that it merges.
    fn order_merge(&mut self) -> AcceptTurn {
        let refs = &self.request.refs;
        let main =
            (refs.iter()).find(|update| update.name == "refs/heads/main" && update.new != ZERO_OID);
        self.merge = main.map(|update| PendingMerge {
            user_id: self.auth.user_id,
            old: update.old.clone(),
            new: update.new.clone(),
        });
        if self.merge.is_none() {
            return self.publish(None);
        }
        self.step = AcceptStep::Sequence;
        Ok(Next::Call(sequence_call()))
    }

    fn publish(&mut self, sequence: Option<Ulid>) -> AcceptTurn {
        let refs = std::mem::take(&mut self.request.refs);
        let merge = self.merge.take().zip(sequence);
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
            AcceptStep::Open => {
                self.document = Some(result.child::<Repository, GitError>()??.0);
                self.step = AcceptStep::Key;
                Ok(Next::Call(push_key_call(self.id, &self.proof)))
            }
            AcceptStep::Key => self.key_checked(push_key_answer(result)?),
            AcceptStep::Scan => {
                let records = result.child::<Vec<GitRecord>, GitError>()??;
                self.state = Some(reduce(&records, &Ancestry::new()).0);
                self.step = AcceptStep::Served;
                Ok(Next::Call(served_call(self.id)))
            }
            AcceptStep::Served => self.check_refs(served_answer(result)?),
            AcceptStep::Copy => {
                let copy = result.child::<Option<StoredObject>, GitError>()??;
                let valid = self.asked.take().ok_or(IoError::Unfinished)?;
                self.keep(copy, valid)?;
                self.next_object()
            }
            AcceptStep::Sequence => self.publish(Some(sequence_answer(result)?)),
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
/// itself; a pending merge is written in the same step, in its place of the given sequence.
pub(super) fn record_publish(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    refs: Vec<RefUpdate>,
    (pack, lfs): (Bytes, Vec<StoredObject>),
    (made, merge): (Vec<String>, Option<(PendingMerge, Ulid)>),
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
        Some((merge, sequence)) => {
            vec![super::pending::entry(
                document.document_id,
                *sequence,
                merge,
            )?]
        }
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

/// What the pending call of recording a made pack answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MadeStep {
    Sequence,
    Publish,
    Mark,
}

/// Like [`record_publish`] for a pack this node made itself, outside a Git transfer: its cache
/// holds the objects already. A push must not use it, since the transfer holds the repository.
pub(super) fn record_made(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    refs: Vec<RefUpdate>,
    written: (Bytes, Vec<StoredObject>),
    (made, merge): (Vec<String>, Option<PendingMerge>),
) -> RecordMadeOperation {
    RecordMadeOperation {
        node,
        auth: auth.clone(),
        document: document.clone(),
        refs,
        written: Some(written),
        made,
        merge,
        step: MadeStep::Sequence,
        record: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RecordMadeOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    document: Document,
    refs: Vec<RefUpdate>,
    written: Option<(Bytes, Vec<StoredObject>)>,
    made: Vec<String>,
    merge: Option<PendingMerge>,
    step: MadeStep,
    record: Option<GitRecord>,
}

impl RecordMadeOperation {
    fn publish(&mut self, sequence: Option<Ulid>) -> Result<Next<GitRecord>, GitError> {
        let merge = self.merge.take().zip(sequence);
        let publish = record_publish(
            self.node,
            &self.auth,
            &self.document,
            std::mem::take(&mut self.refs),
            self.written.take().ok_or(IoError::Unfinished)?,
            (std::mem::take(&mut self.made), merge),
        )?;
        self.step = MadeStep::Publish;
        Ok(Next::Call(IoCall::child(publish)))
    }
}

impl Operation for RecordMadeOperation {
    type Output = GitRecord;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<GitRecord>, GitError> {
        match self.merge {
            Some(_) => Ok(Next::Call(sequence_call())),
            None => self.publish(None),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GitRecord>, GitError> {
        match self.step {
            MadeStep::Sequence => self.publish(Some(sequence_answer(result)?)),
            MadeStep::Publish => {
                let record = result.child::<GitRecord, GitError>()??;
                let GitChange::Objects {
                    pack: Some(pack), ..
                } = &record.change
                else {
                    return Ok(Next::Done(record));
                };
                let mark = objects::mark_imported((record.document_id, self.auth.user_id), pack);
                self.record = Some(record);
                self.step = MadeStep::Mark;
                Ok(Next::Call(IoCall::child(mark)))
            }
            MadeStep::Mark => {
                result.child::<(), IoError>()?.ok();
                Ok(Next::Done(self.record.take().ok_or(IoError::Unfinished)?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{answered, asks_child, asks_store, auth, child, dataset, node};
    use super::*;
    use aruna_core::git::{GitAnswer, encode_push};

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
            proof: PushProof("key".to_string()),
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
            merge: None,
        }
    }

    fn scanned() -> IoResult {
        child(Ok::<_, GitError>(Vec::<GitRecord>::new()))
    }

    fn no_served() -> IoResult {
        answered(GitAnswer::ServedRefs(None))
    }

    // Only the push whose key the store handed out may record itself.
    #[test]
    fn foreign_key_refused() {
        let mut accept = pushing(vec![update(ZERO_OID)], Vec::new());
        accept.step = AcceptStep::Key;
        let refused = answered(GitAnswer::PushKeyValid(false));
        assert_eq!(accept.next(refused), Err(GitError::NotHook));
    }

    // The refs this node served decide, not a replay of the records.
    #[test]
    fn served_refs_decide() {
        let mut accept = pushing(vec![update(&"b".repeat(40))], Vec::new());
        accept.next(scanned()).unwrap();
        let name = "refs/heads/draft".to_string();
        let served = Refs::from([(name, "b".repeat(40))]);
        let answer = accept.next(answered(GitAnswer::ServedRefs(Some(served))));
        assert!(asks_child(&answer));
        assert_eq!(accept.step, AcceptStep::Publish);
    }

    // A push to main takes its place in the merge order before it is recorded.
    #[test]
    fn main_push_ordered() {
        let main = RefUpdate {
            name: "refs/heads/main".into(),
            old: ZERO_OID.into(),
            new: "a".repeat(40),
        };
        let mut accept = pushing(vec![main], Vec::new());
        accept.next(scanned()).unwrap();
        assert!(asks_store(&accept.next(no_served())));
        assert_eq!(accept.step, AcceptStep::Sequence);
        let sequence = answered(GitAnswer::Sequence(Ulid::from(1)));
        assert!(asks_child(&accept.next(sequence)));
    }

    // A made pack without a merge needs no place in the merge order.
    #[test]
    fn plain_record_direct() {
        let written = (Bytes::new(), Vec::new());
        let mut made = record_made(
            Some(node(1)),
            &auth(),
            &dataset(),
            vec![update(ZERO_OID)],
            written,
            (Vec::new(), None),
        );
        assert!(asks_child(&made.first()));
        assert_eq!(made.step, MadeStep::Publish);
    }

    // A branch that moved since the client looked refuses the push.
    #[test]
    fn moved_branch_stale() {
        let mut accept = pushing(vec![update(&"b".repeat(40))], Vec::new());
        assert!(asks_store(&accept.next(scanned())));
        assert_eq!(accept.next(no_served()), Err(GitError::Stale));
    }

    // An LFS object this node never received refuses the push.
    #[test]
    fn unknown_object_invalid() {
        let oid = "c".repeat(64);
        let mut accept = pushing(vec![update(ZERO_OID)], vec![oid]);
        accept.next(scanned()).unwrap();
        assert!(asks_child(&accept.next(no_served())));
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
