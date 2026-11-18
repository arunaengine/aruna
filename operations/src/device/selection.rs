//! Selects and deselects the documents the owner keeps available offline on this device.
//! A selected document gets a local replica; a deselected one is read through the realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::refresh::{RefreshOutcome, RefreshReplicaOperation, refusal_reason};
use super::replica::{
    DeselectDocumentOperation, ReplicaOrigin, ReplicaRecord, ReplicaScanError, SelectionError,
    TrackCreatedOperation, cache_budget,
};
use super::replica_store::{cache_usage, delete_replica, read_replica, store_replica};
use crate::distribution::here::NodeRealm;
use crate::driver::DriverContext;

fn refresh_call(ids: NodeRealm, document_id: Ulid) -> IoCall {
    IoCall::child(RefreshReplicaOperation::new(ids, document_id))
}

fn replica_of(result: IoResult) -> Result<Option<ReplicaRecord>, IoError> {
    result.child::<Option<ReplicaRecord>, IoError>()?
}

fn written(result: IoResult) -> Result<bool, IoError> {
    Ok(result.child::<(), IoError>()?.is_ok())
}

/// What the pending call of a selection answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectStep {
    Read,
    Mark,
    Refreshed,
    Reserve,
    Filled,
    Final,
    Undo,
}

/// Keeps one document available offline. A first selection needs the realm once for the graph;
/// selecting again is idempotent and only refreshes what is already there.
#[derive(Debug, PartialEq)]
pub struct SelectDocumentOperation {
    ids: NodeRealm,
    document_id: Ulid,
    step: SelectStep,
    failure: Option<SelectionError>,
}

/// The selection of one document; see [`SelectDocumentOperation`].
pub fn select_document(context: &DriverContext, document_id: Ulid) -> SelectDocumentOperation {
    SelectDocumentOperation {
        ids: context.node_and_realm(),
        document_id,
        step: SelectStep::Read,
        failure: None,
    }
}

type Selecting = Result<Next<ReplicaRecord>, SelectionError>;

impl SelectDocumentOperation {
    fn refresh(&mut self, then: SelectStep) -> Selecting {
        self.step = then;
        Ok(Next::Call(refresh_call(self.ids, self.document_id)))
    }

    fn read_final(&mut self) -> Selecting {
        self.step = SelectStep::Final;
        Ok(Next::Call(IoCall::child(read_replica(self.document_id))))
    }

    /// The row goes in first because the refresh fills it from the holder's answer; a refresh
    /// that brings nothing leaves no half-selected document.
    fn reserve(&mut self) -> Selecting {
        let provisional = ReplicaRecord::new(
            self.document_id,
            GroupId::nil(),
            String::new(),
            ReplicaOrigin::Realm,
        );
        self.step = SelectStep::Reserve;
        Ok(Next::Call(IoCall::child(store_replica(&provisional))))
    }

    fn filled(&mut self, outcome: RefreshOutcome) -> Selecting {
        let failure = match outcome {
            RefreshOutcome::Installed => return self.read_final(),
            RefreshOutcome::Refused(SyncRefusal::NotFound) => SelectionError::NotFound,
            RefreshOutcome::Refused(refusal) => SelectionError::Refused(refusal_reason(&refusal)),
            RefreshOutcome::Unreachable => SelectionError::Unavailable,
        };
        self.failure = Some(failure);
        self.step = SelectStep::Undo;
        Ok(Next::Call(IoCall::child(delete_replica(self.document_id))))
    }
}

impl Operation for SelectDocumentOperation {
    type Output = ReplicaRecord;
    type Error = SelectionError;

    fn first(&mut self) -> Selecting {
        Ok(Next::Call(IoCall::child(read_replica(self.document_id))))
    }

    fn next(&mut self, result: IoResult) -> Selecting {
        match self.step {
            SelectStep::Read => match replica_of(result)? {
                Some(mut replica) if !replica.selected => {
                    replica.selected = true;
                    self.step = SelectStep::Mark;
                    Ok(Next::Call(IoCall::child(store_replica(&replica))))
                }
                Some(_) => self.refresh(SelectStep::Refreshed),
                None => self.reserve(),
            },
            SelectStep::Mark => {
                written(result)?;
                self.refresh(SelectStep::Refreshed)
            }
            SelectStep::Refreshed => {
                result.child::<RefreshOutcome, IoError>()??;
                self.read_final()
            }
            SelectStep::Reserve => match written(result)? {
                true => self.refresh(SelectStep::Filled),
                false => Err(SelectionError::Unavailable),
            },
            SelectStep::Filled => self.filled(result.child::<RefreshOutcome, IoError>()??),
            SelectStep::Final => {
                let replica = replica_of(result)?.ok_or(SelectionError::Unavailable)?;
                Ok(Next::Done(replica))
            }
            SelectStep::Undo => {
                written(result)?;
                Err(self.failure.take().ok_or(IoError::Unfinished)?)
            }
        }
    }
}

/// Stops keeping one document offline; see [`DeselectDocumentOperation`].
pub fn deselect_document(document_id: Ulid) -> DeselectDocumentOperation {
    DeselectDocumentOperation::new(document_id)
}

/// What the pending call of keeping a read document answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeepStep {
    Read,
    Stale,
    Usage,
    Reserve,
    Fill,
    Undo,
}

/// Keeps a document the owner just read online as an unselected replica, so it stays readable
/// offline. A stale copy is refreshed; a new one is kept only while the cache budget allows.
#[derive(Debug, PartialEq)]
pub struct KeepReadOperation {
    ids: NodeRealm,
    record: MetadataRegistryRecord,
    budget: u64,
    step: KeepStep,
}

/// The keeping of one document just read; see [`KeepReadOperation`].
pub fn keep_read(context: &DriverContext, record: &MetadataRegistryRecord) -> KeepReadOperation {
    KeepReadOperation {
        ids: context.node_and_realm(),
        record: record.clone(),
        budget: cache_budget(),
        step: KeepStep::Read,
    }
}

impl KeepReadOperation {
    fn refresh(&mut self, then: KeepStep) -> Result<Next<()>, IoError> {
        self.step = then;
        Ok(Next::Call(refresh_call(self.ids, self.record.document_id)))
    }

    fn read(&mut self, replica: Option<ReplicaRecord>) -> Result<Next<()>, IoError> {
        let Some(replica) = replica else {
            // A graph cannot be removed locally yet, so a full cache takes no new copy.
            self.step = KeepStep::Usage;
            return Ok(Next::Call(IoCall::child(cache_usage())));
        };
        // A pinned or edited copy follows the refresh beat instead.
        let current = replica.record.as_ref();
        if replica.selected
            || replica.pending_edits > 0
            || current.is_some_and(|held| held.last_event_id == self.record.last_event_id)
        {
            return Ok(Next::Done(()));
        }
        self.refresh(KeepStep::Stale)
    }

    fn reserve(&mut self) -> Result<Next<()>, IoError> {
        let mut provisional = ReplicaRecord::new(
            self.record.document_id,
            self.record.group_id,
            self.record.document_path.clone(),
            ReplicaOrigin::Realm,
        );
        provisional.selected = false;
        self.step = KeepStep::Reserve;
        Ok(Next::Call(IoCall::child(store_replica(&provisional))))
    }
}

impl Operation for KeepReadOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let read = read_replica(self.record.document_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match self.step {
            KeepStep::Read => self.read(replica_of(result)?),
            KeepStep::Stale => {
                result.child::<RefreshOutcome, IoError>()??;
                Ok(Next::Done(()))
            }
            KeepStep::Usage => match result.child::<u64, ReplicaScanError>()? {
                Ok(used) if used < self.budget => self.reserve(),
                _ => Ok(Next::Done(())),
            },
            KeepStep::Reserve => match written(result)? {
                true => self.refresh(KeepStep::Fill),
                false => Ok(Next::Done(())),
            },
            KeepStep::Fill => {
                if result.child::<RefreshOutcome, IoError>()?? == RefreshOutcome::Installed {
                    return Ok(Next::Done(()));
                }
                self.step = KeepStep::Undo;
                let delete = delete_replica(self.record.document_id);
                Ok(Next::Call(IoCall::child(delete)))
            }
            KeepStep::Undo => {
                written(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Selects a document this device created, once the realm has accepted it. The drain runs this,
/// so a draft the owner authored offline stays readable and editable on the device it was
/// written on.
pub(crate) fn track_created(
    document_id: Ulid,
    group_id: GroupId,
    document_path: String,
) -> TrackCreatedOperation {
    TrackCreatedOperation::new(document_id, group_id, document_path)
}

#[cfg(test)]
#[path = "selection_tests.rs"]
mod tests;
