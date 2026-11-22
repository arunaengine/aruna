//! Tests removing an unbound folder call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::types::Value;

use super::super::super::reconcile::pure_tests::folder;
use super::*;

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn deleting() -> SyncedFolder {
    SyncedFolder {
        state: FolderState::Deleting,
        ..folder()
    }
}

fn unbinding() -> FinishUnbindOperation {
    FinishUnbindOperation {
        folder_id: folder().folder_id,
        node_id: folder().remote.node_id,
        origin: None,
        step: UnbindStep::Read,
        cleared: 0,
        folder: None,
    }
}

fn page(keys: &[&str]) -> IoResult {
    let rows = (keys.iter())
        .map(|key| (Key::from(key.as_bytes().to_vec()), Value::from(Vec::new())))
        .collect();
    IoResult::Page { rows, next: None }
}

// A folder that is not being deleted is left alone.
#[test]
fn keeps_active_folder() {
    let mut unbind = unbinding();
    unbind.first().unwrap();
    let answer = unbind.next(child(Ok::<_, FolderError>(folder())));
    assert_eq!(answer, Ok(Next::Done(())));
}

// Rows go first, the bucket next, the offer after it and the folder row last.
#[test]
fn cleans_in_order() {
    let mut unbind = unbinding();
    unbind.first().unwrap();
    unbind
        .next(child(Ok::<_, FolderError>(deleting())))
        .unwrap();
    for cleared in 1..=FOLDER_ROWS.len() {
        assert_eq!((unbind.step, unbind.cleared), (UnbindStep::Rows, cleared));
        unbind.next(child(Ok::<_, FolderError>(()))).unwrap();
    }
    assert_eq!(unbind.step, UnbindStep::Bucket);
    unbind.next(child(Ok::<_, FolderError>(()))).unwrap();
    assert_eq!(unbind.step, UnbindStep::Withdraw);
    let gone = OfferedDirectoryError::NotOffered("folder-x".to_string());
    let answer = unbind.next(child(Err::<usize, _>(gone)));
    assert!(matches!(answer, Ok(Next::Call(IoCall::Delete(_)))));
    assert_eq!(unbind.next(IoResult::Done), Ok(Next::Done(())));
}

// A pass stops at its limit, and rows left over fail it.
#[test]
fn clear_stops_limit() {
    let mut clear = ClearRowsOperation::new(SYNC_BASE_KEYSPACE, folder().folder_id, 1);
    clear.first().unwrap();
    let Ok(Next::Call(IoCall::Delete(rows))) = clear.next(page(&["a", "b"])) else {
        panic!("the rows within the limit are deleted");
    };
    assert_eq!(rows.len(), 1);
    assert!(matches!(clear.next(IoResult::Done), Ok(Next::Call(_))));
    assert_eq!(clear.next(page(&["b"])), Err(FolderError::Unavailable));
}

// A folder without rows is done at once.
#[test]
fn clear_ends_empty() {
    let mut clear = ClearRowsOperation::new(SYNC_BASE_KEYSPACE, folder().folder_id, 1);
    clear.first().unwrap();
    assert_eq!(clear.next(page(&[])), Ok(Next::Done(())));
}

// A bucket already gone counts as deleted.
#[test]
fn missing_bucket_done() {
    let ids = (folder().remote.node_id, None);
    let mut delete = DeleteLocalOperation::new(&folder(), ids, 1);
    delete.first().unwrap();
    let answer = delete.next(IoResult::Values(vec![None]));
    assert_eq!(answer, Ok(Next::Done(())));
}

// A version budget that ran out goes straight to the bucket delete.
#[test]
fn spent_budget_deletes() {
    let ids = (folder().remote.node_id, None);
    let mut delete = DeleteLocalOperation::new(&folder(), ids, 0);
    delete.step = BucketStep::Uploads;
    delete.next(child(Ok::<_, FolderError>(()))).unwrap();
    assert_eq!(delete.step, BucketStep::Delete);
    let gone = child(Err::<(), _>(DeleteBucketError::NotFound));
    assert_eq!(delete.next(gone), Ok(Next::Done(())));
}

// No uploads left ends the abort pass.
#[test]
fn aborts_until_empty() {
    let mut abort = AbortUploadsOperation::new("folder-x".to_string(), 1);
    abort.first().unwrap();
    let empty = child(Ok::<_, ListUploadsError>(ListUploadsResult {
        uploads: Vec::new(),
        common_prefixes: Vec::new(),
        is_truncated: false,
        next_key_marker: None,
        next_upload_marker: None,
    }));
    assert_eq!(abort.next(empty), Ok(Next::Done(())));
}
