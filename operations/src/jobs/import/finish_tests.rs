//! Finishes imports through the rewrite, dataset, rollback and cleanup calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{bucket, entry, hidden_location, plan, run};
use super::*;
use crate::jobs::repository::test_support::child;
use crate::jobs::store::JobMutationError;

fn saved() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn updating(step: UpdateStep) -> UpdateDatasetOperation {
    let checkpoint = ImportCheckpoint {
        rewritten_json: Some("{}".to_string()),
        ..Default::default()
    };
    let mut update = UpdateDatasetOperation::new(run(), checkpoint);
    update.base = Some(Ulid::from_parts(1, 1));
    update.step = step;
    update
}

fn cleaning(step: CleanupStep) -> CleanupOperation {
    let mut cleanup = CleanupOperation::new(run(), ImportCheckpoint::default(), None);
    cleanup.step = step;
    cleanup
}

fn rollback(paths: &[&str]) -> RollbackOperation {
    RollbackOperation::new(run(), &plan(paths), paths.len())
}

// A document that is no crate fails before any row is read.
#[test]
fn rewrite_checks_document() {
    let mut rewrite = RewriteCrateOperation::new(run(), ImportCheckpoint::default(), plan(&["a"]));
    assert!(rewrite.first().is_err());
}

// The created dataset ends the phase and records its revision for a pull.
#[test]
fn created_dataset_advances() {
    let mut create = CreateDatasetOperation::new(run(), ImportCheckpoint::default());
    create.step = CreateStep::Emit;
    create.checkpoint.created = true;
    create.checkpoint.phase = ImportPhase::Cleanup;
    let Ok(Next::Done(done)) = create.next(IoResult::Done) else {
        panic!("expected the finished phase");
    };
    assert!(done.created);
}

// Creating needs the rewritten crate.
#[test]
fn create_needs_crate() {
    let mut create = CreateDatasetOperation::new(run(), ImportCheckpoint::default());
    create.step = CreateStep::Federated;
    let answer = child(Ok::<_, crate::federation::import::ImportError>(()));
    assert_eq!(
        create.next(answer),
        Err(ImportFailure::Permanent(
            "rewritten RO-Crate is missing".to_string()
        ))
    );
}

// A dataset still at the pull's base is updated.
#[test]
fn update_at_base() {
    let mut update = updating(UpdateStep::Current);
    let current = ("{}".to_string(), Ulid::from_parts(1, 1));
    let answer = child(Ok::<_, TransferError>(current));
    assert!(matches!(
        update.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(update.step, UpdateStep::Record);
}

// A dataset that moved on with another crate stops the pull.
#[test]
fn moved_dataset_fails() {
    let mut update = updating(UpdateStep::Current);
    let current = ("not json".to_string(), Ulid::from_parts(2, 2));
    let answer = child(Ok::<_, TransferError>(current));
    assert_eq!(
        update.next(answer),
        Err(ImportFailure::Permanent(
            "the dataset changed while the pull ran; pull again".to_string()
        ))
    );
}

// A holder that refuses the base revision reports the conflict in plain words.
#[test]
fn update_conflict_fails() {
    let mut update = updating(UpdateStep::Update);
    let conflict = UpdateDocumentError::RevisionConflict {
        expected: Ulid::from_parts(1, 1),
        current: Ulid::from_parts(2, 2),
    };
    let answer = child(Err::<MetadataRegistryRecord, _>(
        MetadataWriteError::Update(conflict),
    ));
    assert_eq!(
        update.next(answer),
        Err(ImportFailure::Permanent(
            "the dataset changed while the pull updated it; check it and pull again".to_string()
        ))
    );
}

// Nothing attempted means nothing to roll back.
#[test]
fn rollback_without_entries() {
    let mut rollback = rollback(&[]);
    assert_eq!(rollback.first(), Ok(Next::Done((0, Vec::new()))));
}

// Only the entries up to the failed one are rolled back.
#[test]
fn rollback_stops_early() {
    let rollback = RollbackOperation::new(run(), &plan(&["a", "b", "c"]), 1);
    assert_eq!(rollback.attempted, vec![entry("a"), entry("b")]);
}

// A pinned version stays and is reported; the other one is deleted.
#[test]
fn pinned_version_kept() {
    let mut rollback = rollback(&["a", "bb"]);
    rollback.step = RollbackStep::Pins(bucket());
    let pins = child(Ok::<(Vec<bool>, ReleaseGuards), PinnedError>((
        vec![true, false],
        Vec::new(),
    )));
    assert_eq!(
        rollback.next(pins),
        Ok(Next::Call(report_read(&run(), "a")))
    );
    assert_eq!(rollback.removable, vec![entry("bb")]);
    assert!(matches!(
        rollback.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(rollback.kept, vec!["a".to_string()]);
    assert!(matches!(
        rollback.next(saved()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(rollback.step, RollbackStep::Delete);
}

// A version an earlier attempt removed still counts when its row says so.
#[test]
fn missing_version_counts() {
    let mut rollback = rollback(&["a"]);
    rollback.step = RollbackStep::Missing;
    let mut row = entry_report(&entry("a"));
    row.message = Some(REMOVED.to_string());
    let stored = postcard::to_allocvec(&row).unwrap();
    let answer = IoResult::Values(vec![Some(stored.into())]);
    assert_eq!(rollback.next(answer), Ok(Next::Done((1, Vec::new()))));
}

// A delete that failed leaves the rollback unfinished.
#[test]
fn failed_delete_retries() {
    let mut rollback = rollback(&["a"]);
    let outcome = BulkDeleteOutcome {
        key: "a".to_string(),
        requested_version_id: None,
        result: Err(DeleteObjectError::InvalidOperationState),
    };
    rollback.outcomes.push_back((entry("a"), outcome));
    assert!(matches!(
        rollback.next_outcome(),
        Err(ImportFailure::Retryable(_))
    ));
}

// A clean import has nothing to delete and goes straight to settling.
#[test]
fn cleanup_settles_directly() {
    let mut cleanup = CleanupOperation::new(run(), ImportCheckpoint::default(), None);
    assert!(matches!(cleanup.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(cleanup.step, CleanupStep::Settle);
}

// A hidden source is deleted before anything else.
#[test]
fn cleanup_drops_source() {
    let checkpoint = ImportCheckpoint {
        input: Some(super::super::ImportInput {
            location: hidden_location(),
            size: 4,
            blake3: [1; 32],
            upload_id: None,
            eln: false,
        }),
        ..Default::default()
    };
    let mut cleanup = CleanupOperation::new(run(), checkpoint, None);
    assert!(matches!(cleanup.first(), Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(cleanup.step, CleanupStep::Hidden);
}

// A kept version is named in the failure.
#[test]
fn rollback_names_kept() {
    let mut cleanup = cleaning(CleanupStep::Rollback);
    cleanup.checkpoint.failure = Some("broken".to_string());
    let end: RollbackEnd = (1, vec!["a".to_string()]);
    cleanup.next(child(Ok::<_, ImportFailure>(end))).unwrap();
    assert_eq!(cleanup.checkpoint.rolled_back, 1);
    assert_eq!(
        cleanup.checkpoint.failure,
        Some(format!("broken; {PINNED_KEPT}: `a`"))
    );
}

// The settled link ends the import.
#[test]
fn settled_import_done() {
    let mut cleanup = cleaning(CleanupStep::Settle);
    let Ok(Next::Done(done)) = cleanup.next(child(Ok::<_, LinkError>(()))) else {
        panic!("expected the finished import");
    };
    assert_eq!(done.phase, ImportPhase::Done);
}
