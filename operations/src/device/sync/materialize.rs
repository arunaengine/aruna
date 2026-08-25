//! Writes one remote version into a synced folder and records what happened.
//!
//! The guard travels with the write: the adapter refuses the rename when the
//! file no longer carries the bytes the decision was taken on, and the refusal
//! becomes a pending entry the owner resolves explicitly. When the write is an
//! explicit owner action, its audit row is committed in the same transaction as
//! the base row that records it.

use std::sync::Arc;

use aruna_core::effects::{Effect, LocalFileEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, LocalFileEvent, LocalFileRefusal, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ActionOutcome, AuthContext, EntrySide, EntryState, PendingMark, ReplaceReason,
    SyncActionRecord, SyncBase, SyncedBytes, SyncedFolder, VersionedObjectArn, WriteGuard,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_core::util::unix_timestamp_millis;
use bytes::Bytes;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};

use super::reconcile::Download;
use super::repository::{action_entry, base_entry};

/// What one materialization did to the folder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaterializeOutcome {
    Written,
    /// The bytes landed beside the file under this relative path.
    Copied {
        relative: String,
    },
    /// The local bytes were preserved; the entry stays pending for the owner.
    Refused {
        reason: LocalFileRefusal,
    },
}

pub struct MaterializeInput {
    pub folder: SyncedFolder,
    pub relative: String,
    pub remote_version: Ulid,
    pub guard: WriteGuard,
    pub conflicted: bool,
    /// Fingerprint of the local file the decision was taken against, so a
    /// pending entry is marked against the state the owner will be shown.
    pub local_fingerprint: Option<String>,
    /// The sides the reconciler last observed, carried through so the reported
    /// entry keeps naming both of them after the write.
    pub local: Option<EntrySide>,
    pub remote: Option<EntrySide>,
    /// The synced bytes the entry already had. A refused write must not lose
    /// them, and a successful one replaces them.
    pub synced: Option<SyncedBytes>,
    /// Audit row of the explicit action this write serves, committed with the
    /// base row. Absent for the automatic sync.
    pub audit: Option<SyncActionRecord>,
    pub blob: BackendStream<Result<Bytes, StreamError>>,
}

impl std::fmt::Debug for MaterializeInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MaterializeInput")
            .field("relative", &self.relative)
            .field("remote_version", &self.remote_version)
            .field("conflicted", &self.conflicted)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("writing the file failed: {0}")]
    Write(String),
    #[error("materializing the entry did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}")]
    UnexpectedEvent { state: String },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MaterializeState {
    Init,
    WriteFile,
    StartTransaction,
    WriteRows { txn_id: TxnId },
    Commit,
    Finish,
    Error,
}

#[derive(Debug)]
pub struct MaterializeEntryOperation {
    input: MaterializeInput,
    state: MaterializeState,
    rows: Vec<(String, Key, Value)>,
    outcome: Option<MaterializeOutcome>,
    output: Option<Result<MaterializeOutcome, MaterializeError>>,
}

impl PartialEq for MaterializeEntryOperation {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
            && self.input.relative == other.input.relative
            && self.input.remote_version == other.input.remote_version
    }
}

impl MaterializeEntryOperation {
    pub fn new(input: MaterializeInput) -> Self {
        Self {
            input,
            state: MaterializeState::Init,
            rows: Vec::new(),
            outcome: None,
            output: None,
        }
    }

    /// The base row one outcome leaves behind. Nothing here is ever invented:
    /// synced bytes appear only when this write produced them.
    fn base_row(
        &self,
        outcome: &MaterializeOutcome,
        written: Option<(String, [u8; 32], u64)>,
    ) -> SyncBase {
        let now = unix_timestamp_millis();
        let mark = PendingMark {
            fingerprint: self.input.local_fingerprint.clone(),
            remote_version: Some(self.input.remote_version),
        };
        let kept = SyncBase {
            synced: self.input.synced.clone(),
            local_version_id: None,
            synced_at_ms: now,
            entry: EntryState::InSync,
            pending_at: None,
            local: self.input.local.clone(),
            remote: self.input.remote.clone(),
        };
        match (outcome, written) {
            (MaterializeOutcome::Written, Some((fingerprint, blake3, size))) => SyncBase {
                synced: Some(SyncedBytes {
                    fingerprint: fingerprint.clone(),
                    blake3,
                    size,
                    remote_version_id: self.input.remote_version,
                }),
                local: Some(EntrySide {
                    size,
                    modified_at_ms: None,
                    fingerprint: Some(fingerprint),
                    blake3: Some(blake3),
                    version_id: None,
                }),
                ..kept
            },
            (MaterializeOutcome::Failed { message }, _) => SyncBase {
                entry: EntryState::Error {
                    reason: message.clone(),
                },
                ..kept
            },
            // A conflicted copy over a known base is a conflict; over none it is
            // a replacement the owner still has to authorise.
            (MaterializeOutcome::Copied { relative }, _) => match self.input.synced.is_some() {
                true => SyncBase {
                    entry: EntryState::Conflict {
                        remote_version: self.input.remote_version,
                        conflicted_copy: relative.clone(),
                    },
                    pending_at: Some(mark),
                    ..kept
                },
                false => SyncBase {
                    entry: EntryState::PendingReplace {
                        reason: ReplaceReason::BaseUnknown,
                        remote_version: self.input.remote_version,
                        conflicted_copy: Some(relative.clone()),
                    },
                    pending_at: Some(mark),
                    ..kept
                },
            },
            (_, _) => SyncBase {
                entry: EntryState::PendingReplace {
                    reason: self.refusal_reason(),
                    remote_version: self.input.remote_version,
                    conflicted_copy: None,
                },
                pending_at: Some(mark),
                ..kept
            },
        }
    }

    /// A refused replace is only unknown-base when this entry had no base.
    fn refusal_reason(&self) -> ReplaceReason {
        match self.input.synced.is_some() {
            true => ReplaceReason::LocalModified,
            false => ReplaceReason::BaseUnknown,
        }
    }

    /// The rows one outcome commits: the base row, plus the audit row when the
    /// write serves an explicit owner action.
    fn rows_for(
        &self,
        outcome: &MaterializeOutcome,
        written: Option<(String, [u8; 32], u64)>,
    ) -> Result<Vec<(String, Key, Value)>, ConversionError> {
        let after = written.as_ref().map(|(_, blake3, _)| *blake3);
        let base = self.base_row(outcome, written);
        let mut rows = vec![base_entry(
            self.input.folder.folder_id,
            &self.input.relative,
            &base,
        )?];
        if let Some(audit) = self.input.audit.as_ref() {
            rows.push(action_entry(&SyncActionRecord {
                outcome: match outcome {
                    MaterializeOutcome::Refused { .. } => ActionOutcome::Stale,
                    MaterializeOutcome::Failed { message } => ActionOutcome::Failed {
                        reason: message.clone(),
                    },
                    _ => ActionOutcome::Applied,
                },
                after,
                ..audit.clone()
            })?);
        }
        Ok(rows)
    }
}

impl Operation for MaterializeEntryOperation {
    type Output = MaterializeOutcome;
    type Error = MaterializeError;

    fn start(&mut self) -> Effects {
        let root = self.input.folder.root.clone();
        let relative = self.input.relative.clone();
        let blob = std::mem::replace(
            &mut self.input.blob,
            BackendStream(Box::pin(futures_util::stream::empty())),
        );
        self.state = MaterializeState::WriteFile;
        let effect = match self.input.conflicted {
            true => LocalFileEffect::WriteConflicted {
                root,
                relative,
                at_ms: unix_timestamp_millis(),
                blob,
            },
            false => LocalFileEffect::Write {
                root,
                relative,
                guard: self.input.guard.clone(),
                blob,
            },
        };
        smallvec![Effect::LocalFile(effect)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return fail(self, MaterializeError::Storage(error));
        }
        match (self.state, event) {
            (MaterializeState::WriteFile, Event::LocalFile(event)) => {
                let (outcome, written) = match event {
                    LocalFileEvent::Written {
                        fingerprint,
                        blake3,
                        size,
                    } => (
                        MaterializeOutcome::Written,
                        Some((fingerprint, blake3, size)),
                    ),
                    LocalFileEvent::Copied { relative, .. } => {
                        (MaterializeOutcome::Copied { relative }, None)
                    }
                    LocalFileEvent::Refused { reason } => {
                        (MaterializeOutcome::Refused { reason }, None)
                    }
                    // A failed write is an outcome the owner has to see, not a
                    // silent retry: it is recorded on the entry.
                    LocalFileEvent::Error { message } => {
                        (MaterializeOutcome::Failed { message }, None)
                    }
                    other => return unexpected(self, format!("{other:?}")),
                };
                self.rows = match self.rows_for(&outcome, written) {
                    Ok(rows) => rows,
                    Err(error) => return fail(self, MaterializeError::Conversion(error)),
                };
                self.outcome = Some(outcome);
                self.state = MaterializeState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            (
                MaterializeState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.state = MaterializeState::WriteRows { txn_id };
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.rows),
                    txn_id: Some(txn_id),
                })]
            }
            (
                MaterializeState::WriteRows { txn_id },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                self.state = MaterializeState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                MaterializeState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.state = MaterializeState::Finish;
                self.output = Some(self.outcome.clone().ok_or(MaterializeError::NotFinished));
                smallvec![]
            }
            (state, _) => unexpected(self, format!("{state:?}")),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            MaterializeState::Finish | MaterializeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(MaterializeError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            MaterializeState::WriteRows { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn unexpected(operation: &mut MaterializeEntryOperation, state: String) -> Effects {
    fail(operation, MaterializeError::UnexpectedEvent { state })
}

fn fail(operation: &mut MaterializeEntryOperation, error: MaterializeError) -> Effects {
    let cleanup = operation.abort();
    operation.state = MaterializeState::Error;
    operation.output = Some(Err(error));
    cleanup
}

/// Reads one remote version from the folder's realm node.
pub(crate) async fn fetch_remote(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    relative: &str,
    version: Ulid,
) -> Option<BackendStream<Result<Bytes, StreamError>>> {
    let net_handle = context.net_handle.as_ref()?;
    let realm_id = *net_handle.realm_id();
    let source = VersionedObjectArn::new(
        realm_id,
        folder.remote.node_id,
        folder.remote.bucket.clone(),
        folder.remote.remote_key(relative),
        version,
    )
    .ok()?;
    let request = BaoReadRequest {
        auth_context: AuthContext {
            user_id: folder.created_by,
            realm_id,
            path_restrictions: None,
        },
        realm_id,
        target: BaoReadTarget::ExactVersion(source),
        expected_blake3: None,
        metadata_only: false,
        destination: None,
        known_refs: Vec::new(),
    };
    match managed_read(context, folder.remote.node_id, request).await {
        Ok(BaoReadOutput::Stream { blob, .. }) => Some(blob),
        Ok(BaoReadOutput::Metadata { .. }) => None,
        Err(error) => {
            debug!(relative = %relative, error = %error, "Could not read the remote version");
            None
        }
    }
}

/// Fetches every planned remote version and writes it under its guard. Answers
/// how many entries reached the disk.
pub async fn apply_downloads(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
    downloads: Vec<Download>,
) -> usize {
    let mut applied = 0usize;
    for download in downloads {
        let Some(blob) =
            fetch_remote(context, folder, &download.relative, download.remote_version).await
        else {
            continue;
        };
        let outcome = drive(
            MaterializeEntryOperation::new(MaterializeInput {
                folder: folder.clone(),
                relative: download.relative.clone(),
                remote_version: download.remote_version,
                guard: download.guard,
                conflicted: download.conflicted,
                local_fingerprint: download
                    .local
                    .as_ref()
                    .and_then(|side| side.fingerprint.clone()),
                local: download.local.clone(),
                remote: None,
                synced: download.synced.clone(),
                audit: None,
                blob,
            }),
            context,
        )
        .await;
        match outcome {
            Ok(MaterializeOutcome::Written | MaterializeOutcome::Copied { .. }) => applied += 1,
            Ok(MaterializeOutcome::Refused { reason }) => {
                debug!(relative = %download.relative, reason = ?reason, "Kept the local file");
            }
            Ok(MaterializeOutcome::Failed { message }) => {
                warn!(relative = %download.relative, message = %message, "Could not write the folder entry");
            }
            Err(error) => {
                warn!(relative = %download.relative, error = %error, "Could not write the folder entry");
            }
        }
    }
    applied
}
