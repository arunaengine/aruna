//! Writes one remote version into a synced folder and records what happened.
//! A refused write keeps the local bytes and leaves the entry pending for the owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, LocalFileEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, LocalFileEvent, LocalFileRefusal};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::synced_folder::{
    ActionOutcome, EntrySide, EntryState, PendingMark, ReplaceReason, SyncActionRecord, SyncBase,
    SyncedBytes, SyncedFolder, WriteGuard,
};
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

/// Writes the file outside any transaction, then commits the base row and the audit row in a
/// short write child.
#[derive(Debug)]
pub struct MaterializeEntryOperation {
    input: MaterializeInput,
    /// The outcome of the file write, once it is known.
    outcome: Option<MaterializeOutcome>,
    now_ms: u64,
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
            now_ms: 0,
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
        let now = self.now_ms;
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

    /// Turns the file answer into the outcome and the commit of its rows.
    fn written(&mut self, event: Event) -> Result<CommitMaterialize, MaterializeError> {
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
        let writes = self.rows_for(&outcome, written)?;
        self.outcome = Some(outcome);
        Ok(CommitMaterialize {
            writes,
            deletes: Vec::new(),
        })
    }
}

/// Commits the rows of one finished file change in a short write transaction; a lost conflict
/// reruns it from a clone.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct CommitMaterialize {
    pub(super) writes: Vec<(String, Key, Value)>,
    /// Rows dropped after the writes, in the same transaction.
    pub(super) deletes: Vec<(String, Key)>,
}

impl CommitMaterialize {
    /// Runs this commit as a conflict-retrying child keyed by `seed`.
    pub(super) fn call(self, seed: &str) -> IoCall {
        IoCall::retry(self, seed.as_bytes())
    }
}

impl Operation for CommitMaterialize {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(match self.deletes.is_empty() {
            true => Next::Done(()),
            false => Next::Call(IoCall::Delete(std::mem::take(&mut self.deletes))),
        })
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

impl Operation for MaterializeEntryOperation {
    type Output = MaterializeOutcome;
    type Error = MaterializeError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<MaterializeOutcome>, MaterializeError> {
        let root = self.input.folder.root.clone();
        let relative = self.input.relative.clone();
        let blob = std::mem::replace(&mut self.input.blob, BackendStream::empty());
        let effect = match self.input.conflicted {
            true => LocalFileEffect::WriteConflicted {
                root,
                relative,
                at_ms: self.now_ms,
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
            None => {
                let commit = self.written(result.event()?)?;
                Ok(Next::Call(commit.call(&self.input.relative)))
            }
            Some(outcome) => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(outcome))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::sync::reconcile::pure_tests::folder;
    use aruna_core::calls::ChildOutcome;

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
            blob: BackendStream::empty(),
        })
    }

    fn refused() -> Event {
        Event::LocalFile(LocalFileEvent::Refused {
            reason: LocalFileRefusal::Drifted,
        })
    }

    // The file is written outside any transaction, then a write child commits its base row.
    #[test]
    fn writes_then_commits() {
        let mut materialize = materialize();
        assert_eq!(materialize.transaction(), TransactionMode::None);
        assert!(matches!(
            materialize.first(),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let answer = IoResult::Event(Box::new(refused()));
        assert!(matches!(
            materialize.next(answer),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let committed = IoResult::Child(ChildOutcome::new(Ok::<(), IoError>(())));
        let kept = MaterializeOutcome::Refused {
            reason: LocalFileRefusal::Drifted,
        };
        assert_eq!(materialize.next(committed), Ok(Next::Done(kept)));
    }

    // The commit child writes the base row alone in its own write transaction.
    #[test]
    fn commit_writes_rows() {
        let mut commit = materialize().written(refused()).unwrap();
        assert_eq!(commit.transaction(), TransactionMode::Write);
        assert!(commit.deletes.is_empty());
        let Ok(Next::Call(IoCall::Write(rows))) = commit.first() else {
            panic!("expected the base row");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(commit.next(IoResult::Done), Ok(Next::Done(())));
    }

    // A failed commit fails the materialization instead of reporting the outcome.
    #[test]
    fn commit_failure_fails() {
        let mut materialize = materialize();
        materialize.first().unwrap();
        materialize
            .next(IoResult::Event(Box::new(refused())))
            .unwrap();
        let lost = IoResult::Child(ChildOutcome::new(Err::<(), IoError>(IoError::Conflict)));
        assert_eq!(
            materialize.next(lost),
            Err(MaterializeError::Io(IoError::Conflict))
        );
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
