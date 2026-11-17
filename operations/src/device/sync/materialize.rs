//! Writes one remote version into a synced folder and records what happened.
//! A refused write keeps the local bytes and leaves the entry pending for the owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, LocalFileEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, LocalFileEvent, LocalFileRefusal};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ActionOutcome, EntrySide, EntryState, PendingMark, ReplaceReason, SyncActionRecord, SyncBase,
    SyncedBytes, SyncedFolder, WriteGuard,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use bytes::Bytes;
use thiserror::Error;
use ulid::Ulid;

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
    /// The write itself failed. Nothing on disk changed and the entry reports
    /// the reason instead of retrying silently forever.
    Failed {
        message: String,
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
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected file answer {0}")]
    UnexpectedEvent(&'static str),
}

/// Writes the file, then commits the base row and the audit row in one transaction.
#[derive(Debug)]
pub struct MaterializeEntryOperation {
    input: MaterializeInput,
    /// The outcome of the file write, once it is known.
    outcome: Option<MaterializeOutcome>,
}

impl PartialEq for MaterializeEntryOperation {
    fn eq(&self, other: &Self) -> bool {
        self.outcome == other.outcome
            && self.input.relative == other.input.relative
            && self.input.remote_version == other.input.remote_version
    }
}

impl MaterializeEntryOperation {
    pub fn new(input: MaterializeInput) -> Self {
        Self {
            input,
            outcome: None,
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

    /// Turns the file answer into the outcome and the rows it commits.
    fn written(&mut self, event: Event) -> Result<IoCall, MaterializeError> {
        let Event::LocalFile(event) = event else {
            return Err(MaterializeError::UnexpectedEvent(event.label()));
        };
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
            LocalFileEvent::Refused { reason } => (MaterializeOutcome::Refused { reason }, None),
            // A failed write is an outcome the owner has to see, not a
            // silent retry: it is recorded on the entry.
            LocalFileEvent::Error { message } => (MaterializeOutcome::Failed { message }, None),
            other => {
                return Err(MaterializeError::UnexpectedEvent(
                    Event::LocalFile(other).label(),
                ));
            }
        };
        let rows = self.rows_for(&outcome, written)?;
        self.outcome = Some(outcome);
        Ok(IoCall::Write(rows))
    }
}

impl Operation for MaterializeEntryOperation {
    type Output = MaterializeOutcome;
    type Error = MaterializeError;

    fn first(&mut self) -> Result<Next<MaterializeOutcome>, MaterializeError> {
        let root = self.input.folder.root.clone();
        let relative = self.input.relative.clone();
        let blob = std::mem::replace(
            &mut self.input.blob,
            BackendStream(Box::pin(futures_util::stream::empty())),
        );
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
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::LocalFile(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MaterializeOutcome>, MaterializeError> {
        match self.outcome.take() {
            None => Ok(Next::Call(self.written(result.event()?)?)),
            Some(outcome) => {
                done(result)?;
                Ok(Next::Done(outcome))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::sync::reconcile::pure_tests::folder;

    fn materialize() -> MaterializeEntryOperation {
        MaterializeEntryOperation::new(MaterializeInput {
            folder: folder(),
            relative: "a.txt".to_string(),
            remote_version: Ulid::from_bytes([9u8; 16]),
            guard: WriteGuard::MustNotExist,
            conflicted: false,
            local_fingerprint: None,
            local: None,
            remote: None,
            synced: None,
            audit: None,
            blob: BackendStream(Box::pin(futures_util::stream::empty())),
        })
    }

    // The file is written first, then its base row is committed in the transaction.
    #[test]
    fn writes_then_commits() {
        let mut materialize = materialize();
        assert!(materialize.transaction());
        assert!(matches!(
            materialize.first(),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let refused = IoResult::Event(Box::new(Event::LocalFile(LocalFileEvent::Refused {
            reason: LocalFileRefusal::Drifted,
        })));
        let Ok(Next::Call(IoCall::Write(rows))) = materialize.next(refused) else {
            panic!("expected the base row");
        };
        assert_eq!(rows.len(), 1);
        let kept = MaterializeOutcome::Refused {
            reason: LocalFileRefusal::Drifted,
        };
        assert_eq!(materialize.next(IoResult::Done), Ok(Next::Done(kept)));
    }

    // A file answer of another kind fails before any row is written.
    #[test]
    fn other_answer_fails() {
        let mut materialize = materialize();
        materialize.first().unwrap();
        let moved = IoResult::Event(Box::new(Event::LocalFile(LocalFileEvent::Moved {
            to: String::new(),
        })));
        assert!(matches!(
            materialize.next(moved),
            Err(MaterializeError::UnexpectedEvent(_))
        ));
    }
}
