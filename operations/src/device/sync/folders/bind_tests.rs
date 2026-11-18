//! Tests binding folders call by call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use byteview::ByteView;

use super::super::super::reconcile::pure_tests::folder;
use super::*;

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn binding(create_bucket: bool) -> SyncedFolder {
    SyncedFolder {
        state: FolderState::Binding { create_bucket },
        ..folder()
    }
}

fn read(state: SyncedFolder) -> FinishBindingOperation {
    let mut finish = finish_binding(state.folder_id);
    finish.first().unwrap();
    let _ = finish.next(child(Ok::<_, FolderError>(state)));
    finish
}

fn missing() -> RemoteError {
    RemoteError::Refused(SyncRefusal::NotFound)
}

// A folder inside another would observe the same file twice.
#[test]
fn refuses_nested_roots() {
    assert!(overlaps("/home/ada/data", "/home/ada/data"));
    assert!(overlaps("/home/ada/data", "/home/ada/data/sub"));
    assert!(overlaps("/home/ada/data/sub", "/home/ada/data"));
    assert!(!overlaps("/home/ada/data", "/home/ada/database"));
    assert!(!overlaps("/home/ada/data", "/home/ada/other"));
}

// A second folder on a root already bound is refused before anything is written.
#[test]
fn reserve_checks_roots() {
    let mut reserve = ReserveBindingOperation::new(binding(true));
    reserve.first().unwrap();
    let row = (
        folder_key(Ulid::from_bytes([9; 16])),
        ByteView::from(folder().to_bytes().unwrap()),
    );
    let answer = reserve.next(IoResult::Page {
        rows: vec![row],
        next: None,
    });
    assert_eq!(answer, Err(FolderError::RootOverlaps(folder().root)));
}

// A folder that is no longer binding is left alone.
#[test]
fn finished_binding_kept() {
    let mut finish = finish_binding(folder().folder_id);
    finish.first().unwrap();
    let answer = finish.next(child(Ok::<_, FolderError>(folder())));
    assert_eq!(answer, Ok(Next::Done(folder())));
}

// A new bucket waits until the binding is on disk; a failed sync keeps the binding for later.
#[test]
fn failed_sync_notes() {
    let mut finish = read(binding(true));
    assert_eq!(finish.step, FinishStep::Sync);
    let failed = child(Err::<IoResult, _>(IoError::Unfinished));
    assert!(matches!(finish.next(failed), Ok(Next::Call(_))));
    assert_eq!(finish.step, FinishStep::Note);
    assert_eq!(finish.failure, Some(FolderError::Unavailable));
}

// A remote that refuses the bucket drops the binding and reports the refusal.
#[test]
fn refusal_drops_binding() {
    let mut finish = read(binding(false));
    assert_eq!(finish.step, FinishStep::Remote);
    let refused = child(Err::<SyncVersionPage, _>(missing()));
    assert!(matches!(finish.next(refused), Ok(Next::Call(_))));
    assert_eq!(finish.step, FinishStep::Drop);
    let answer = finish.next(child(Ok::<_, IoError>(())));
    assert!(matches!(
        answer,
        Err(FolderError::RemoteBucketMissing { .. })
    ));
}

// An unreachable node keeps the binding and notes why.
#[test]
fn unreachable_keeps_binding() {
    let mut finish = read(binding(false));
    let lost = RemoteError::Unreachable("gone".to_string());
    finish.next(child(Err::<SyncVersionPage, _>(lost))).unwrap();
    assert_eq!(finish.step, FinishStep::Note);
    let answer = finish.next(child(Ok::<_, FolderError>(binding(false))));
    assert!(matches!(answer, Err(FolderError::RemoteUnreachable { .. })));
}

// A refused binding row is removed only while it is still a binding.
#[test]
fn drops_only_bindings() {
    let mut active = DropBindingOperation::new(folder().folder_id);
    active.first().unwrap();
    let row = ByteView::from(folder().to_bytes().unwrap());
    let answer = active.next(IoResult::Values(vec![Some(row)]));
    assert_eq!(answer, Ok(Next::Done(())));
    let mut held = DropBindingOperation::new(folder().folder_id);
    held.first().unwrap();
    let row = ByteView::from(binding(true).to_bytes().unwrap());
    let answer = held.next(IoResult::Values(vec![Some(row)]));
    assert!(matches!(answer, Ok(Next::Call(IoCall::Delete(_)))));
}
