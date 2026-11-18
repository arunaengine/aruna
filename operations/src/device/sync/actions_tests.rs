//! Tests the owner's sync actions call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use byteview::ByteView;

use super::super::reconcile::pure_tests::folder;
use super::*;

fn input(kind: ActionKind) -> ApplyActionInput {
    ApplyActionInput {
        folder_id: folder().folder_id,
        kind,
        scope: ActionScope::Entry {
            relative: "a.txt".to_string(),
        },
        expected: None,
        actor: folder().created_by,
    }
}

fn base(entry: EntryState) -> SyncBase {
    SyncBase {
        synced: None,
        local_version_id: None,
        synced_at_ms: 1,
        entry,
        pending_at: None,
        local: None,
        remote: None,
    }
}

fn entry(realm_id: Option<RealmId>, input: ApplyActionInput) -> EntryActionOperation {
    EntryActionOperation::new(realm_id, folder(), input, "a.txt".to_string())
}

fn row(base: &SyncBase) -> IoResult {
    IoResult::Values(vec![Some(ByteView::from(base.to_bytes().unwrap()))])
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn expected() -> ExpectedEntry {
    ExpectedEntry {
        fingerprint: "4-1".to_string(),
        blake3: [7; 32],
        remote_version: Some(Ulid::from_bytes([3; 16])),
    }
}

fn action(input: ApplyActionInput) -> ApplyActionOperation {
    ApplyActionOperation {
        realm_id: None,
        input,
        step: ActionStep::Folder,
        folder: None,
        cursor: None,
        pending: VecDeque::new(),
        applied: 0,
        stale: 0,
        record: None,
    }
}

// An entry without a readable base row is not found.
#[test]
fn missing_base_refused() {
    let mut action = entry(None, input(ActionKind::Resolve));
    action.first().unwrap();
    let answer = action.next(IoResult::Values(vec![None]));
    assert_eq!(answer, Err(ActionError::NotFound));
}

// A replace needs the hashes the owner saw and a realm to read from.
#[test]
fn replace_needs_expected() {
    let mut missing = entry(None, input(ActionKind::Replace));
    missing.first().unwrap();
    let answer = missing.next(row(&base(EntryState::InSync)));
    assert_eq!(answer, Err(ActionError::ExpectedMissing));
    let given = ApplyActionInput {
        expected: Some(expected()),
        ..input(ActionKind::Replace)
    };
    let mut offline = entry(None, given);
    offline.first().unwrap();
    let answer = offline.next(row(&base(EntryState::InSync)));
    assert_eq!(answer, Err(ActionError::RemoteUnavailable));
}

// Keeping the local bytes writes the rows, then wakes the upload drain.
#[test]
fn keep_arms_uploads() {
    let mut keep = entry(None, input(ActionKind::KeepLocal));
    keep.first().unwrap();
    keep.next(row(&base(EntryState::LocalChanged))).unwrap();
    assert_eq!(keep.step, EntryStep::Kept);
    assert!(matches!(
        keep.next(IoResult::Done),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(keep.step, EntryStep::Armed);
    let failed = child(Err::<(), _>(IoError::Unfinished));
    assert!(matches!(keep.next(failed), Ok(Next::Done(_))));
}

// A refused guarded write keeps the file and is audited as stale.
#[test]
fn refused_write_stale() {
    let given = ApplyActionInput {
        expected: Some(expected()),
        ..input(ActionKind::Replace)
    };
    let mut replace = entry(Some(RealmId::from_bytes([7; 32])), given);
    replace.first().unwrap();
    replace.next(row(&base(EntryState::InSync))).unwrap();
    let blob: Blob = aruna_core::stream::BackendStream(Box::pin(futures_util::stream::empty()));
    replace.next(child(Ok::<_, FetchError>(blob))).unwrap();
    let refused = MaterializeOutcome::Refused {
        reason: aruna_core::events::LocalFileRefusal::Drifted,
    };
    let answer = replace.next(child(Ok::<_, MaterializeError>(refused)));
    let Ok(Next::Done(record)) = answer else {
        panic!("the action ends with its audit row");
    };
    assert_eq!(record.outcome, ActionOutcome::Stale);
}

// A folder being unbound takes no actions.
#[test]
fn deleting_folder_refused() {
    let mut run = action(input(ActionKind::Resolve));
    run.first().unwrap();
    let deleting = SyncedFolder {
        state: FolderState::Deleting,
        ..folder()
    };
    let answer = run.next(child(Ok::<_, FolderError>(deleting)));
    assert_eq!(answer, Err(ActionError::Folder(FolderError::NotFound)));
}

// A bulk action counts entries without observed bytes as stale and audits the total.
#[test]
fn bulk_counts_stale() {
    let bulk = ApplyActionInput {
        scope: ActionScope::AllPending,
        ..input(ActionKind::Replace)
    };
    let mut run = action(bulk);
    run.first().unwrap();
    run.next(child(Ok::<_, FolderError>(folder()))).unwrap();
    assert_eq!(run.step, ActionStep::Listing);
    let pending = EntryState::PendingReplace {
        reason: aruna_core::structs::ReplaceReason::BaseUnknown,
        remote_version: Ulid::from_bytes([3; 16]),
        conflicted_copy: None,
    };
    let page = (vec![("a.txt".to_string(), base(pending))], None::<Key>);
    run.next(child(Ok::<_, FolderError>(page))).unwrap();
    assert_eq!(run.step, ActionStep::Audit);
    let Ok(Next::Done(record)) = run.next(IoResult::Done) else {
        panic!("the bulk action ends with its audit row");
    };
    assert_eq!(record.entries, 0);
    assert!(matches!(record.outcome, ActionOutcome::Failed { .. }));
}
