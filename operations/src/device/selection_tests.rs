//! Tests selecting documents for offline use and keeping documents read online.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;

use super::*;

fn id() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn replica(selected: bool) -> ReplicaRecord {
    let mut replica = ReplicaRecord::new(
        id(),
        Ulid::from_bytes([1; 16]),
        "notes".to_string(),
        ReplicaOrigin::Realm,
    );
    replica.selected = selected;
    replica
}

fn select() -> SelectDocumentOperation {
    SelectDocumentOperation {
        ids: None,
        document_id: id(),
        step: SelectStep::Read,
        failure: None,
    }
}

fn child<T>(value: T) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(Ok::<T, IoError>(value)))
}

fn asks_child<T, E>(next: Result<Next<T>, E>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

// A held but unselected copy is marked, refreshed and read back.
#[test]
fn selects_held_copy() {
    let mut select = select();
    assert!(asks_child(select.first()));
    assert!(asks_child(select.next(child(Some(replica(false))))));
    assert_eq!(select.step, SelectStep::Mark);
    assert!(asks_child(select.next(child(()))));
    assert_eq!(select.step, SelectStep::Refreshed);
    assert!(asks_child(select.next(child(RefreshOutcome::Unreachable))));
    let done = select.next(child(Some(replica(true))));
    assert_eq!(done, Ok(Next::Done(replica(true))));
}

// A first selection that brings nothing leaves no half-selected document.
#[test]
fn undoes_failed_selection() {
    for (outcome, error) in [
        (
            RefreshOutcome::Refused(SyncRefusal::NotFound),
            SelectionError::NotFound,
        ),
        (RefreshOutcome::Unreachable, SelectionError::Unavailable),
    ] {
        let mut select = select();
        select.first().unwrap();
        assert!(asks_child(select.next(child(None::<ReplicaRecord>))));
        assert_eq!(select.step, SelectStep::Reserve);
        assert!(asks_child(select.next(child(()))));
        assert!(asks_child(select.next(child(outcome))));
        assert_eq!(select.step, SelectStep::Undo);
        assert_eq!(select.next(child(())), Err(error));
    }
}

// A slot that cannot be stored ends the selection at once.
#[test]
fn reserve_failure_unavailable() {
    let mut select = select();
    select.first().unwrap();
    select.next(child(None::<ReplicaRecord>)).unwrap();
    let failed = IoResult::Child(ChildOutcome::new(Err::<(), _>(IoError::Unfinished)));
    assert_eq!(select.next(failed), Err(SelectionError::Unavailable));
}
