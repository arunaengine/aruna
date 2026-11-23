//! Moves one synced file into its folder's trash, then drops its base row and writes the audit
//! row in one short transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, LocalFileEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, LocalFileEvent, LocalFileRefusal};
use aruna_core::keyspaces::SYNC_BASE_KEYSPACE;
use aruna_core::operation::{HoldScope, Operation, TransactionMode};
use aruna_core::structs::synced_folder::{
    ActionOutcome, SyncActionRecord, SyncedFolder, WriteGuard,
};

use super::actions::ActionError;
use super::folder_scope;
use super::materialize::CommitMaterialize;
use super::repository::{action_entry, base_key};

#[derive(Clone, Copy, Debug, PartialEq)]
enum RemoveStep {
    MoveFile,
    Commit,
}

/// Moves one file into the folder's trash outside any transaction, then drops its base row
/// with the audit row in a short write child. The file is never unlinked.
#[derive(Debug, PartialEq)]
pub struct RemoveEntryOperation {
    folder: SyncedFolder,
    relative: String,
    guard: WriteGuard,
    record: SyncActionRecord,
    step: RemoveStep,
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
            step: RemoveStep::MoveFile,
        }
    }

    /// Records what the move did and the rows it commits. A refused move keeps the entry exactly
    /// as it was: only the audit row is written, never the base-row deletion.
    fn moved(&mut self, event: Event) -> Result<CommitMaterialize, ActionError> {
        let Event::LocalFile(event) = event else {
            return Err(ActionError::Unavailable);
        };
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
                    LocalFileRefusal::Drifted => ActionOutcome::Stale,
                    other => ActionOutcome::Failed {
                        reason: format!("{other:?}"),
                    },
                },
                ..self.record.clone()
            },
            other => {
                let label = Event::LocalFile(other).label().to_string();
                return Err(ActionError::Storage(StorageError::WriteError(label)));
            }
        };
        let row = action_entry(&self.record).map_err(|_| ActionError::Unavailable)?;
        let base = (
            SYNC_BASE_KEYSPACE.to_string(),
            base_key(self.folder.folder_id, &self.relative),
        );
        self.step = RemoveStep::Commit;
        Ok(CommitMaterialize {
            writes: vec![row],
            deletes: (!refused).then_some(base).into_iter().collect(),
        })
    }
}

impl Operation for RemoveEntryOperation {
    type Output = SyncActionRecord;
    type Error = ActionError;

    fn first(&mut self) -> Result<Next<SyncActionRecord>, ActionError> {
        let effect = LocalFileEffect::MoveAside {
            root: self.folder.root.clone(),
            relative: self.relative.clone(),
            guard: self.guard.clone(),
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::LocalFile(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncActionRecord>, ActionError> {
        match self.step {
            RemoveStep::MoveFile => {
                let commit = self.moved(result.event()?)?;
                Ok(Next::Call(commit.call(&self.relative)))
            }
            RemoveStep::Commit => {
                result.child::<(), IoError>()??;
                Ok(Next::Done(self.record.clone()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }

    /// The folder's pass scope; a child of an action runs under the action's own hold.
    fn hold_scope(&self) -> Option<HoldScope> {
        Some(folder_scope(self.folder.folder_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::sync::reconcile::pure_tests::folder;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::synced_folder::{ActionKind, ActionScope};
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    fn record() -> SyncActionRecord {
        SyncActionRecord {
            action_id: Ulid::from_bytes([5u8; 16]),
            folder_id: folder().folder_id,
            kind: ActionKind::RemoveLocal,
            scope: ActionScope::Entry {
                relative: "a.txt".to_string(),
            },
            actor: UserId::new(Ulid::nil(), RealmId::from_bytes([7u8; 32])),
            at_ms: 1,
            before: None,
            after: None,
            outcome: ActionOutcome::Applied,
            trashed_to: None,
            entries: 0,
        }
    }

    fn remove() -> RemoveEntryOperation {
        RemoveEntryOperation::new(
            folder(),
            "a.txt".to_string(),
            WriteGuard::MustNotExist,
            record(),
        )
    }

    fn answer(event: LocalFileEvent) -> IoResult {
        IoResult::Event(Box::new(Event::LocalFile(event)))
    }

    fn committed() -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<(), IoError>(())))
    }

    // The move runs outside any transaction; a write child adds the audit row and drops the base.
    #[test]
    fn drops_base_row() {
        let mut remove = remove();
        assert_eq!(remove.transaction(), TransactionMode::None);
        let scope = super::super::folder_scope(folder().folder_id);
        assert_eq!(remove.hold_scope(), Some(scope));
        assert!(matches!(remove.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let moved = Event::LocalFile(LocalFileEvent::Moved {
            to: ".trash/a.txt".to_string(),
        });
        let applied = SyncActionRecord {
            trashed_to: Some(".trash/a.txt".to_string()),
            ..record()
        };
        let mut commit = remove.moved(moved).unwrap();
        assert_eq!(commit.transaction(), TransactionMode::Write);
        let audit = IoCall::Write(vec![action_entry(&applied).unwrap()]);
        assert_eq!(commit.first(), Ok(Next::Call(audit)));
        let base = (
            SYNC_BASE_KEYSPACE.to_string(),
            base_key(folder().folder_id, "a.txt"),
        );
        let drop = IoCall::Delete(vec![base]);
        assert_eq!(commit.next(IoResult::Done), Ok(Next::Call(drop)));
        assert_eq!(commit.next(IoResult::Done), Ok(Next::Done(())));
        assert_eq!(remove.next(committed()), Ok(Next::Done(applied)));
    }

    // A refused move keeps the base row and records only the audit row.
    #[test]
    fn refused_keeps_base() {
        let mut remove = remove();
        remove.first().unwrap();
        let refused = answer(LocalFileEvent::Refused {
            reason: LocalFileRefusal::Drifted,
        });
        assert!(matches!(
            remove.next(refused),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Done(stale)) = remove.next(committed()) else {
            panic!("expected the stale record");
        };
        assert_eq!(stale.outcome, ActionOutcome::Stale);
        let mut kept = remove.moved(Event::LocalFile(LocalFileEvent::Refused {
            reason: LocalFileRefusal::Drifted,
        }));
        let commit = kept.as_mut().unwrap();
        assert!(commit.deletes.is_empty());
        assert!(matches!(commit.first(), Ok(Next::Call(IoCall::Write(_)))));
    }
}
