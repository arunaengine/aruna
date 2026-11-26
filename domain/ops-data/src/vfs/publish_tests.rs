//! A new name's publication binds its handle, reverse row and ancestor witnesses in its own
//! transaction, and a departing stage owner hands its stage only to an allowed scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::handles_tests::{asked, child, context, granted, id, scope, user, vfs};
use super::super::mutate::tests::prepared;
use super::*;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::vfs::{HandleKind, LiveHandle, VfsRecord, decode_handle_id};
use aruna_core::vfs_effects::CloseSummary;

fn owner(access_key: &str, scope: StageScope) -> StageOwner {
    StageOwner {
        user: user(),
        access_key: access_key.to_string(),
        mount_id: id(13),
        scope,
    }
}

fn token() -> StageBase {
    StageBase::Unpublished {
        token: id(6),
        predecessor: None,
    }
}

fn stage() -> NfsStageRecord {
    NfsStageRecord {
        stage_id: id(12),
        handle_id: 4,
        owner: owner("KEY1", StageScope::User),
        key: "p/a".to_string(),
        generation: 2,
        checkpoint: "c-2".to_string(),
        logical_size: 5,
        base: token(),
        source_copies: Vec::new(),
        source_policies: Vec::new(),
        destination_copies: Vec::new(),
        destination_policies: Vec::new(),
        disposition: StageDisposition::Live,
        quota_retry: None,
        foreign_life: None,
        updated_at_ms: 1,
    }
}

fn handle(key: &str, identity: HandleIdentity) -> Value {
    let live = LiveHandle {
        key: key.to_string(),
        identity,
        mode: 0o644,
        attribute_change: 1,
        cookie_id: 9,
        detached: None,
    };
    Value::from(VfsHandleRow::Live(live).to_bytes().unwrap())
}

fn waiting() -> DirectoryWitness {
    DirectoryWitness::Unpublished {
        key: "p/a".to_string(),
        token: id(6),
    }
}

/// A publication of the new name `p/a` whose parent handle 8 rests on it, run up to its
/// attempt registration.
fn publishing() -> VfsPublishOperation {
    let input = PublishInput {
        context: context(),
        instance_id: 1,
        key: "p/a".to_string(),
        handle_id: Some(4),
        base: token(),
        replaces: false,
        stage: Some(stage()),
        logical_size: 5,
        attempt_id: id(22),
        updates: vec![WitnessUpdate {
            key: "p".to_string(),
            handle_id: Some(8),
            witness: waiting(),
        }],
    };
    let mut operation = VfsPublishOperation::new(input, prepared("p/a"));
    operation.seed(5);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    let turn = operation.next(vfs(VfsEvent::GuardAcquired { guard_id: id(10) }));
    // The reverse row and the parent's handle are read with the stage and the file handle.
    let Ok(Next::Call(IoCall::Read(reads))) = turn else {
        panic!("the publication reads its rows");
    };
    assert_eq!(reads.len(), 5);
    let file = HandleIdentity::File(FileIdentity::Unpublished {
        token: id(6),
        predecessor: None,
    });
    let rows = vec![
        None,
        Some(Value::from(stage().to_bytes().unwrap())),
        Some(handle("p/a", file)),
        None,
        Some(handle("p", HandleIdentity::Directory(waiting()))),
    ];
    let turn = operation.next(IoResult::Values(rows));
    assert!(matches!(asked(&turn), VfsEffect::BeginAttempt { .. }));
    operation
}

#[test]
fn new_name_binds() {
    // The handle, its reverse row and the resting witness take the new life with the version.
    let mut operation = publishing();
    let writes = operation.bound_rows(id(31)).unwrap();
    let row = |key: VfsKey| {
        let key = state_key(&key).unwrap();
        let found = writes.iter().find(|(_, row, _)| *row == key);
        found.map(|row| row.2.clone()).unwrap()
    };
    let file = VfsHandleRow::from_bytes(&row(VfsKey::Handle(scope(), 4))).unwrap();
    let committed = HandleIdentity::File(FileIdentity::Committed { life_id: id(31) });
    assert!(matches!(file, VfsHandleRow::Live(live) if live.identity == committed));
    let name = row(VfsKey::Name(scope(), "p/a".to_string()));
    assert_eq!(decode_handle_id(&name).unwrap(), 4);
    let parent = VfsHandleRow::from_bytes(&row(VfsKey::Handle(scope(), 8))).unwrap();
    let advanced = DirectoryWitness::Descendant {
        key: "p/a".to_string(),
        life_id: id(31),
    };
    let expected = HandleIdentity::Directory(advanced.clone());
    assert!(matches!(parent, VfsHandleRow::Live(live) if live.identity == expected));
    // The open registry learns the same witness once the publication commits.
    assert_eq!(operation.input.updates[0].witness, advanced);
    let receipt = operation.receipt(id(22), Some(id(31))).unwrap();
    assert!(matches!(
        receipt.change,
        ReceiptChange::FilePublish { life_id, .. } if life_id == id(31)
    ));
}

#[test]
fn new_name_publishes() {
    // The bound rows ride the version's own transaction as one put.
    let mut operation = publishing();
    let turn = operation.next(vfs(VfsEvent::Done));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.version_id, Some(id(22)));
}

fn summary(remaining_writers: usize) -> CloseSummary {
    CloseSummary {
        instance_id: 1,
        key: "p/a".to_string(),
        kind: HandleKind::File,
        remaining_opens: 1,
        remaining_writers,
        delete_pending: false,
        dirty: true,
        detached: None,
        disposition_owner: None,
        stage_id: Some(id(12)),
        generation: 2,
        logical_size: 5,
        handle_id: Some(4),
        identity: HandleIdentity::File(FileIdentity::Unpublished {
            token: id(6),
            predecessor: None,
        }),
    }
}

fn departing(next: StageOwner) -> VfsCloseOperation {
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage()),
    };
    let mut operation = VfsCloseOperation::new(input).with_successor(next);
    operation.first().unwrap();
    operation
}

#[test]
fn owner_hands_over() {
    // The stage passes to the remaining writer at its generation, then the change is synced.
    let job = |seed| StageScope::Attempt {
        job_id: JobId::from_bytes([seed; 16]),
    };
    let next = owner("KEY2", StageScope::User);
    let moved = handed_over(&stage(), next.clone(), 70).unwrap();
    assert_eq!((moved.owner, moved.generation), (next.clone(), 2));
    assert_eq!(handed_over(&stage(), owner("KEY2", job(5)), 70), None);
    let mut operation = departing(next);
    let turn = operation.next(vfs(VfsEvent::Closed(summary(1))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Closed)));
}

#[test]
fn attempt_cannot_take() {
    // A user's stage never passes to an attempt's scope: it is retained for recovery instead.
    let next = owner(
        "KEY2",
        StageScope::Attempt {
            job_id: JobId::from_bytes([5; 16]),
        },
    );
    let mut operation = departing(next);
    let turn = operation.next(vfs(VfsEvent::Closed(summary(1))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    operation.next(child(())).unwrap();
    let turn = operation.next(IoResult::Done);
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Retained(id(12)))));
}

#[test]
fn reader_close_keeps() {
    // A close by an open that does not own the stage changes nothing.
    let input = CloseInput {
        context: VfsAccessContext {
            access_key: "KEY9".to_string(),
            ..context()
        },
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage()),
    };
    let mut operation =
        VfsCloseOperation::new(input).with_successor(owner("KEY2", StageScope::User));
    operation.first().unwrap();
    let turn = operation.next(vfs(VfsEvent::Closed(summary(1))));
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Closed)));
}

/// Runs one stage move against `row` as it reads and answers the row it writes.
fn moved(mut change: StageMoveOperation, row: &NfsStageRecord) -> (VfsKey, NfsStageRecord) {
    assert!(matches!(change.first(), Ok(Next::Call(IoCall::Read(_)))));
    let read = IoResult::Values(vec![Some(Value::from(row.to_bytes().unwrap()))]);
    let Ok(Next::Call(IoCall::Delete(deletes))) = change.next(read) else {
        panic!("expected the old row's delete");
    };
    let live = state_key(&VfsKey::LiveStage(scope(), 4)).unwrap();
    assert_eq!(deletes, vec![(NFS_STATE_KEYSPACE.to_string(), live)]);
    let Ok(Next::Call(IoCall::Write(writes))) = change.next(IoResult::Done) else {
        panic!("expected the replacement row");
    };
    let (_, key, value) = &writes[0];
    let key = VfsKey::from_bytes(key.as_ref()).unwrap();
    (key, NfsStageRecord::from_bytes(value.as_ref()).unwrap())
}

#[test]
fn quota_backs_off() {
    // A still-full group keeps the checkpoint live with its next retry doubled.
    let mut blocked = stage();
    blocked.quota_retry = Some(next_retry(None, 10));
    let mut operation = QuotaStageOperation::reschedule(scope(), blocked.clone());
    operation.stamp(100);
    let (key, row) = moved(operation.stage_move(), &blocked);
    assert_eq!(key, VfsKey::LiveStage(scope(), 4));
    let retry = row.quota_retry.unwrap();
    assert_eq!(
        (retry.attempts, retry.next_at_ms),
        (2, 100 + 2 * QUOTA_WAIT_MS)
    );
    assert_eq!(
        (row.disposition, row.updated_at_ms),
        (StageDisposition::Live, 100)
    );
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.next(child(())), Ok(Next::Done(())));
}

#[test]
fn quota_retry_retains() {
    // A name that can no longer take the checkpoint keeps it by its stage id.
    let mut blocked = stage();
    blocked.quota_retry = Some(next_retry(None, 10));
    let operation = QuotaStageOperation::retain(scope(), blocked.clone(), DetachReason::LostLife);
    let (key, row) = moved(operation.stage_move(), &blocked);
    assert_eq!(key, VfsKey::RetainedStage(scope(), id(12)));
    assert_eq!(row, retained(&blocked, DetachReason::LostLife, 0));
}

#[test]
fn changed_stage_conflicts() {
    // A row that changed since it was read refuses the move.
    let blocked = stage();
    let operation = QuotaStageOperation::reschedule(scope(), blocked.clone());
    let mut change = operation.stage_move();
    change.first().unwrap();
    let mut newer = blocked;
    newer.generation = 3;
    let read = IoResult::Values(vec![Some(Value::from(newer.to_bytes().unwrap()))]);
    assert_eq!(change.next(read), Err(VfsError::Conflict));
    let mut failing = QuotaStageOperation::reschedule(scope(), stage());
    failing.first().unwrap();
    let refused = IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<(), VfsError>(
        VfsError::Conflict,
    )));
    assert_eq!(failing.next(refused), Err(VfsError::Conflict));
}
