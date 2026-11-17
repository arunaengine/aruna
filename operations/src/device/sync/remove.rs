//! Moves one synced file into its folder's trash and drops its base row, with the audit row
//! committed in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, LocalFileEffect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, LocalFileEvent, StorageEvent};
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{ActionOutcome, SyncActionRecord, SyncedFolder, WriteGuard};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use super::actions::ActionError;
use super::repository::{action_entry, base_key};

#[derive(Clone, Copy, Debug, PartialEq)]
enum RemoveState {
    Init,
    MoveFile,
    StartTransaction,
    WriteRows { txn_id: TxnId },
    DeleteBase { txn_id: TxnId },
    Commit,
    Finish,
    Error,
}

/// Moves one file into the folder's trash and drops its base row, with the
/// audit row committed alongside. The file is never unlinked.
#[derive(Debug, PartialEq)]
pub struct RemoveEntryOperation {
    folder: SyncedFolder,
    relative: String,
    guard: WriteGuard,
    record: SyncActionRecord,
    state: RemoveState,
    rows: Vec<(String, Key, Value)>,
    /// A refused move keeps the entry exactly as it was: only the audit row is
    /// written, never the base-row deletion.
    refused: bool,
    output: Option<Result<SyncActionRecord, ActionError>>,
}

impl RemoveEntryOperation {
    pub fn new(
        folder: SyncedFolder,
        relative: String,
        guard: WriteGuard,
        record: SyncActionRecord,
    ) -> Self {
        Self {
            folder,
            relative,
            guard,
            record,
            state: RemoveState::Init,
            rows: Vec::new(),
            refused: false,
            output: None,
        }
    }
}

impl Operation for RemoveEntryOperation {
    type Output = SyncActionRecord;
    type Error = ActionError;

    fn start(&mut self) -> Effects {
        self.state = RemoveState::MoveFile;
        smallvec![Effect::LocalFile(LocalFileEffect::MoveAside {
            root: self.folder.root.clone(),
            relative: self.relative.clone(),
            guard: self.guard.clone(),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return remove_failed(self, ActionError::Storage(error));
        }
        match (self.state, event) {
            (RemoveState::MoveFile, Event::LocalFile(event)) => {
                let refused = matches!(event, LocalFileEvent::Refused { .. });
                self.record = match event {
                    // The trash path is the evidence the bytes still exist.
                    LocalFileEvent::Moved { to } => SyncActionRecord {
                        trashed_to: Some(to),
                        outcome: ActionOutcome::Applied,
                        ..self.record.clone()
                    },
                    LocalFileEvent::Refused { reason } => SyncActionRecord {
                        outcome: match reason {
                            aruna_core::events::LocalFileRefusal::Drifted => ActionOutcome::Stale,
                            other => ActionOutcome::Failed {
                                reason: format!("{other:?}"),
                            },
                        },
                        ..self.record.clone()
                    },
                    other => {
                        return remove_failed(
                            self,
                            ActionError::Storage(StorageError::WriteError(format!("{other:?}"))),
                        );
                    }
                };
                let Ok(row) = action_entry(&self.record) else {
                    return remove_failed(self, ActionError::Unavailable);
                };
                self.rows = vec![row];
                self.refused = refused;
                self.state = RemoveState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            (
                RemoveState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.state = RemoveState::WriteRows { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.rows),
                    txn_id: Some(txn_id),
                })]
            }
            (
                RemoveState::WriteRows { txn_id },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                if self.refused {
                    self.state = RemoveState::Commit;
                    return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
                }
                self.state = RemoveState::DeleteBase { txn_id };
                smallvec![Effect::Storage(StorageEffect::Delete {
                    key_space: SYNC_BASE_KEYSPACE.to_string(),
                    key: base_key(self.folder.folder_id, &self.relative),
                    txn_id: Some(txn_id),
                })]
            }
            (
                RemoveState::DeleteBase { txn_id },
                Event::Storage(StorageEvent::DeleteResult { .. }),
            ) => {
                self.state = RemoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (RemoveState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = RemoveState::Finish;
                self.output = Some(Ok(self.record.clone()));
                smallvec![]
            }
            (_, _) => remove_failed(self, ActionError::Unavailable),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoveState::Finish | RemoveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ActionError::Unavailable)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveState::WriteRows { txn_id } | RemoveState::DeleteBase { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn remove_failed(operation: &mut RemoveEntryOperation, error: ActionError) -> Effects {
    let cleanup = operation.abort();
    operation.state = RemoveState::Error;
    operation.output = Some(Err(error));
    cleanup
}
