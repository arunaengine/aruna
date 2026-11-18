//! Removes Git packs and LFS copies stored as objects before packs moved into Fjall.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageReadError;
use aruna_core::git::{
    GitChange, GitPack, GitPackRecord, GitRecord, LOCAL_OBJECTS, StoredObject, git_pack_key,
};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{NodeRealm, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use tracing::{info, warn};
use ulid::Ulid;

use super::objects::DescribeObjectOperation;
use super::state::reduce;
use super::{GitError, download, lfs, objects, records};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::s3::object::delete::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use aruna_core::git_state::Ancestry;

type Document = MetadataRegistryRecord;
type ObjectRows = Vec<(Vec<u8>, StoredObject)>;
/// Why an object must stay, or `None` when it may go.
type Keep = Option<&'static str>;

/// The read that says whether the exact version still exists on this node.
fn stored_call(node: Option<NodeId>, object: &StoredObject) -> Option<DescribeObjectOperation> {
    let found = (object.bucket.as_str(), object.group_id?);
    Some(objects::described(
        node,
        found,
        &object.key,
        Some(object.version_id),
    ))
}

fn stored_answer(result: IoResult) -> Result<bool, GitError> {
    let found = result.child::<Option<StoredObject>, GitError>()?;
    Ok(found.is_ok_and(|object| object.is_some()))
}

/// Deletes each old pack or copy once its bytes are safe: a pack or pack copy after its bytes
/// are in Fjall, an LFS copy while its original stays readable or once nothing names it, and
/// everything of a deleted dataset. Kept objects are logged with the reason and retried later.
pub fn remove_objects(ids: NodeRealm) -> RemoveObjectsOperation {
    RemoveObjectsOperation { ids, rows: None }
}

#[derive(Debug, PartialEq)]
pub struct RemoveObjectsOperation {
    ids: NodeRealm,
    rows: Option<VecDeque<(Vec<u8>, StoredObject)>>,
}

impl RemoveObjectsOperation {
    fn next_row(&mut self) -> Result<Next<()>, GitError> {
        let (node_id, realm_id) = self.ids.ok_or(IoError::Unfinished)?;
        let rows = self.rows.as_mut().ok_or(IoError::Unfinished)?;
        while let Some((key, object)) = rows.pop_front() {
            if !object.key.starts_with("git-packs/") && !object.key.starts_with("git-copies/") {
                continue;
            }
            let Some(id) = key.get(..16).and_then(|id| <[u8; 16]>::try_from(id).ok()) else {
                continue;
            };
            let remove = RemoveObjectOperation {
                node: Some(node_id),
                realm_id,
                id: Ulid::from_bytes(id),
                object,
                key,
                step: RemoveStep::Document,
            };
            return Ok(Next::Call(IoCall::child(remove)));
        }
        Ok(Next::Done(()))
    }
}

impl Operation for RemoveObjectsOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        if self.ids.is_none() {
            return Ok(Next::Done(()));
        }
        let rows = records::prefixed::<StoredObject>(LOCAL_OBJECTS, Vec::new());
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if self.rows.is_some() {
            result.child::<(), GitError>()??;
            return self.next_row();
        }
        match result.child::<ObjectRows, GitError>()? {
            Ok(rows) => {
                self.rows = Some(rows.into());
                self.next_row()
            }
            Err(error) => {
                warn!(%error, "Local Git objects unreadable");
                Ok(Next::Done(()))
            }
        }
    }
}

/// What the pending call of removing one object answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveStep {
    Document,
    Settled,
    Stored,
    Delete,
    Index,
}

/// Removes one object when it is safe; every outcome is logged and none stops the pass.
#[derive(Debug, PartialEq)]
struct RemoveObjectOperation {
    node: Option<NodeId>,
    realm_id: RealmId,
    id: Ulid,
    object: StoredObject,
    key: Vec<u8>,
    step: RemoveStep,
}

type RemoveTurn = Result<Next<()>, GitError>;

impl RemoveObjectOperation {
    fn failed(&self, error: impl std::fmt::Display) -> RemoveTurn {
        let (document_id, key) = (self.id, &self.object.key);
        warn!(%document_id, %key, %error, "Old Git object not deleted");
        Ok(Next::Done(()))
    }

    fn check_stored(&mut self) -> RemoveTurn {
        let Some(read) = stored_call(self.node, &self.object) else {
            return self.failed(GitError::Invalid);
        };
        self.step = RemoveStep::Stored;
        Ok(Next::Call(IoCall::child(read)))
    }

    /// A version that is already gone only loses its index row.
    fn remove_row(&mut self) -> RemoveTurn {
        self.step = RemoveStep::Index;
        let key = std::mem::take(&mut self.key);
        Ok(Next::Call(IoCall::child(records::remove(
            LOCAL_OBJECTS,
            key,
        ))))
    }
}

impl Operation for RemoveObjectOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> RemoveTurn {
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> RemoveTurn {
        let (document_id, key) = (self.id, self.object.key.clone());
        match self.step {
            RemoveStep::Document => match result.child::<Option<Document>, StorageReadError>()? {
                // No dataset is left whose history could need the object.
                Ok(None) => self.check_stored(),
                Ok(Some(document)) => {
                    self.step = RemoveStep::Settled;
                    let settle = settled(self.node, &document, &self.object);
                    Ok(Next::Call(IoCall::child(settle)))
                }
                Err(_) => Ok(Next::Done(())),
            },
            RemoveStep::Settled => match result.child::<Keep, GitError>()? {
                Ok(None) => self.check_stored(),
                Ok(Some(reason)) => {
                    info!(%document_id, %key, reason, "Old Git object kept");
                    Ok(Next::Done(()))
                }
                Err(error) => {
                    warn!(%document_id, %key, %error, "Old Git object kept");
                    Ok(Next::Done(()))
                }
            },
            RemoveStep::Stored => {
                if !stored_answer(result)? {
                    return self.remove_row();
                }
                let group_id = self.object.group_id.ok_or(GitError::Invalid)?;
                let object = &self.object;
                let delete = DeleteObjectOperation::new(DeleteObjectInput {
                    bucket: object.bucket.clone(),
                    key: object.key.clone(),
                    version_id: Some(object.version_id),
                    group_id,
                    realm_id: self.realm_id,
                    node_id: object.node_id,
                    deleted_by: UserId::nil(self.realm_id),
                });
                self.step = RemoveStep::Delete;
                Ok(Next::Call(IoCall::child(delete)))
            }
            RemoveStep::Delete => match result.child::<DeleteObjectResult, DeleteObjectError>()? {
                Ok(_) => self.remove_row(),
                Err(_) => self.failed(GitError::Unavailable),
            },
            RemoveStep::Index => match result.child::<(), GitError>()? {
                Ok(()) => {
                    info!(%document_id, %key, "Deleted old Git object");
                    Ok(Next::Done(()))
                }
                Err(error) => self.failed(error),
            },
        }
    }
}

/// What the pending call of judging one object answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettleStep {
    History,
    Moved,
    Stored,
    Original,
}

/// Whether the object may go, or the reason it must stay.
fn settled(node: Option<NodeId>, document: &Document, object: &StoredObject) -> SettledOperation {
    SettledOperation {
        node,
        document: document.clone(),
        object: object.clone(),
        step: SettleStep::History,
        failure: None,
    }
}

#[derive(Debug, PartialEq)]
struct SettledOperation {
    node: Option<NodeId>,
    document: Document,
    object: StoredObject,
    step: SettleStep,
    failure: Option<GitError>,
}

type SettleTurn = Result<Next<Keep>, GitError>;

impl SettledOperation {
    /// Whether an LFS copy is not the last one: its original is still readable, or no record
    /// names the content at all.
    fn check_original(&mut self, history: &[GitRecord]) -> SettleTurn {
        let lfs_state = reduce(history, &Ancestry::new()).0.lfs;
        let Some(original) = lfs_state.get(&self.object.sha256) else {
            return Ok(Next::Done(None));
        };
        if original.key == self.object.key && original.node_id == self.object.node_id {
            return Ok(Next::Done(Some("the copy is the recorded original")));
        }
        // The pushing user reads the original, as a download by that user would.
        let pusher = (history.iter())
            .find(|record| match &record.change {
                GitChange::Objects { lfs, .. } => lfs.contains(original),
                GitChange::Checkpoint(checkpoint) => checkpoint.lfs.contains(original),
                _ => false,
            })
            .map_or(UserId::nil(self.document.realm_id), |record| record.user_id);
        let auth = super::project::author(pusher);
        self.step = SettleStep::Original;
        let read = lfs::readable(self.node, &auth, original);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for SettledOperation {
    type Output = Keep;
    type Error = GitError;

    fn first(&mut self) -> SettleTurn {
        let scan = records::scan(self.document.document_id);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> SettleTurn {
        match self.step {
            SettleStep::History => {
                let history = result.child::<Vec<GitRecord>, GitError>()??;
                let pack = GitPack {
                    sha256: self.object.sha256.clone(),
                    size: self.object.size,
                };
                let named = |record: &GitRecord| match &record.change {
                    GitChange::Objects {
                        pack: Some(own), ..
                    } => *own == pack,
                    GitChange::Checkpoint(checkpoint) => checkpoint.packs.contains(&pack),
                    _ => false,
                };
                // Copies of remote packs were stored under git-copies/ too.
                if !self.object.key.starts_with("git-packs/") && !history.iter().any(named) {
                    return self.check_original(&history);
                }
                let digest = pack.digest().ok_or(GitError::Invalid)?;
                self.step = SettleStep::Moved;
                let moved = MovedOperation {
                    node: self.node,
                    document: self.document.clone(),
                    pack,
                    digest,
                    history,
                    loaded: false,
                };
                Ok(Next::Call(IoCall::child(moved)))
            }
            SettleStep::Moved => {
                let Err(error) = result.child::<(), GitError>()? else {
                    return Ok(Next::Done(None));
                };
                self.failure = Some(error);
                let Some(read) = stored_call(self.node, &self.object) else {
                    return self.lost();
                };
                self.step = SettleStep::Stored;
                Ok(Next::Call(IoCall::child(read)))
            }
            SettleStep::Stored => match stored_answer(result)? {
                true => Err(self.failure.take().ok_or(IoError::Unfinished)?),
                false => self.lost(),
            },
            SettleStep::Original => Ok(Next::Done(match result.child::<bool, IoError>()?? {
                true => None,
                false => Some("the original is unreadable, so this copy may be the last one"),
            })),
        }
    }
}

impl SettledOperation {
    /// Neither the old version nor Fjall has the bytes, so the row points at nothing.
    fn lost(&mut self) -> SettleTurn {
        let error = self.failure.take().ok_or(IoError::Unfinished)?;
        warn!(document_id = %self.document.document_id, key = %self.object.key, %error,
            "Old Git pack is lost; its index row is dropped");
        Ok(Next::Done(None))
    }
}

/// Whether the pack's bytes are in Fjall, moved there now, or named by no record.
#[derive(Debug, PartialEq)]
struct MovedOperation {
    node: Option<NodeId>,
    document: Document,
    pack: GitPack,
    digest: [u8; 32],
    history: Vec<GitRecord>,
    loaded: bool,
}

impl Operation for MovedOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        let key = git_pack_key(self.document.document_id, &self.digest).to_vec();
        let load = records::load::<GitPackRecord>(GIT_PACK_KEYSPACE, key);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if self.loaded {
            result.child::<Bytes, GitError>()??;
            return Ok(Next::Done(()));
        }
        self.loaded = true;
        if result
            .child::<Option<GitPackRecord>, GitError>()??
            .is_some()
        {
            return Ok(Next::Done(()));
        }
        let pack = &self.pack;
        let owner = self.history.iter().find(|record| {
            matches!(&record.change, GitChange::Objects { pack: Some(own), .. } if own == pack)
        });
        match owner {
            Some(owner) => {
                let read = download::pack_bytes(self.node, &self.document, pack, owner);
                Ok(Next::Call(IoCall::child(read)))
            }
            None => Ok(Next::Done(())),
        }
    }
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
