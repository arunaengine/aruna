//! A new name's publication binds its handle, reverse row and ancestor witnesses in its own
//! transaction, and a departing stage owner hands its stage only to an allowed scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::handles_tests::{
    asked, child, context, denied, granted, id, instance, scope, user, vfs,
};
use super::super::mutate::tests::{prepared, put_done, stored};
use super::super::path::marker_key;
use super::super::recovery_tests::{binding, host};
use super::*;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::vfs::{HandleKind, LiveHandle, VfsRecord, decode_handle_id};
use aruna_core::vfs_effects::{CloseSummary, InstanceSummary};

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
    let mut operation = guarded(publish_input());
    let turn = operation.next(vfs(VfsEvent::GuardAcquired { guard_id: id(10) }));
    answer_rows(&mut operation, turn);
    operation
}

/// Runs a publication through admission and the permit up to its guard.
fn guarded(input: PublishInput) -> VfsPublishOperation {
    let mut operation = VfsPublishOperation::new(input, prepared("p/a"));
    operation.seed(5);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation
}

fn publish_input() -> PublishInput {
    PublishInput {
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
        expected_edit: None,
        require_writerless: false,
    }
}

/// Answers the rows a publication reads under its guard, up to its attempt registration.
fn answer_rows(operation: &mut VfsPublishOperation, turn: Result<Next<Published>, VfsError>) {
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
        base_version: None,
        edit_generation: 3,
        successor_owner: None,
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
    // The successor's own credential is admitted to write the name first.
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
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
    assert!(matches!(
        turn,
        Ok(Next::Then {
            output: CloseOutcome::Retained(_),
            ..
        })
    ));
}

#[test]
fn retained_close_detaches() {
    // A synced retention detaches the instance from the checkpoint it moved, with its reason.
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage()),
    };
    let mut operation = VfsCloseOperation::new(input);
    operation.first().unwrap();
    let last = CloseSummary {
        remaining_opens: 0,
        detached: Some(DetachReason::LostLife),
        ..summary(0)
    };
    let turn = operation.next(vfs(VfsEvent::Closed(last)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the retained outcome with the instance release");
    };
    assert_eq!(output, CloseOutcome::Retained(id(12)));
    let retain = VfsEffect::RetainInstance {
        scope: scope(),
        instance_id: 1,
        stage_id: id(12),
        generation: 2,
        reason: DetachReason::LostLife,
    };
    assert_eq!(calls, vec![vfs_call(retain)]);
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
    // With its live instance named, the synced retention detaches that instance too.
    let mut held =
        QuotaStageOperation::retain(scope(), blocked, DetachReason::LostLife).with_instance(1);
    held.first().unwrap();
    assert_eq!(held.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let retain = VfsEffect::RetainInstance {
        scope: scope(),
        instance_id: 1,
        stage_id: id(12),
        generation: 2,
        reason: DetachReason::LostLife,
    };
    let done = Next::Then {
        output: (),
        calls: vec![vfs_call(retain)],
    };
    assert_eq!(held.next(IoResult::Done), Ok(done));
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

/// The guarded instance check of a publication with `input`, answered with `summary`.
fn checked(input: PublishInput, summary: InstanceSummary) -> Result<Next<Published>, VfsError> {
    let mut operation = guarded(input);
    let turn = operation.next(vfs(VfsEvent::GuardAcquired { guard_id: id(10) }));
    assert!(matches!(
        asked(&turn),
        VfsEffect::LookupHandle { handle_id: 4, .. }
    ));
    operation.next(vfs(VfsEvent::Instance(Some(summary))))
}

#[test]
fn stale_edit_refused() {
    // An edit or a size change after the image was read refuses the publication before any put.
    let input = PublishInput {
        expected_edit: Some(2),
        ..publish_input()
    };
    assert_eq!(checked(input, instance()), Err(VfsError::Conflict));
    let input = PublishInput {
        expected_edit: Some(3),
        ..publish_input()
    };
    let resized = InstanceSummary {
        logical_size: 9,
        ..instance()
    };
    assert_eq!(checked(input.clone(), resized), Err(VfsError::Conflict));
    let turn = checked(input, instance());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
}

#[test]
fn returned_writer_refused() {
    // A writerless retry refuses once a writer returned or the stage generation moved on.
    let input = PublishInput {
        require_writerless: true,
        ..publish_input()
    };
    assert_eq!(checked(input.clone(), instance()), Err(VfsError::Conflict));
    let idle = InstanceSummary {
        writer_count: 0,
        stage_id: Some(id(12)),
        generation: 2,
        ..instance()
    };
    let moved = InstanceSummary {
        generation: 3,
        ..idle.clone()
    };
    assert_eq!(checked(input.clone(), moved), Err(VfsError::Conflict));
    let turn = checked(input, idle);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
}

#[test]
fn publish_completes_put() {
    // A settled publication runs the ordinary post-PUT completion after its releases.
    let mut operation = publishing();
    operation.next(vfs(VfsEvent::Done)).unwrap();
    let turn = operation.next(put_done(Ok(stored())));
    assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
    operation.next(IoResult::Done).unwrap();
    let settled = ReceiptOutcome::Applied {
        life_id: Some(id(31)),
    };
    let Ok(Next::Then { calls, .. }) = operation.next(child(settled)) else {
        panic!("the publication settles with its follow-up calls");
    };
    assert_eq!(calls.len(), 3);
    assert!(matches!(calls.last(), Some(IoCall::Child(_))));
}

#[test]
fn successor_from_summary() {
    // Without a prepared successor the earliest remaining writer is chosen and admitted; a
    // denied one leaves the stage retained.
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage()),
    };
    let mut operation = VfsCloseOperation::new(input);
    operation.first().unwrap();
    let leaving = CloseSummary {
        successor_owner: Some(owner("KEY2", StageScope::User)),
        ..summary(1)
    };
    let turn = operation.next(vfs(VfsEvent::Closed(leaving)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(denied());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    operation.next(child(())).unwrap();
    let turn = operation.next(IoResult::Done);
    assert!(matches!(
        turn,
        Ok(Next::Then {
            output: CloseOutcome::Retained(_),
            ..
        })
    ));
}

#[test]
fn refused_publish_errs() {
    // A dirty close with no durable checkpoint reports the refused publication.
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: Some((publish_input(), prepared("p/a"))),
        stage: None,
    };
    let mut operation = VfsCloseOperation::new(input);
    operation.first().unwrap();
    let last = CloseSummary {
        remaining_opens: 0,
        ..summary(0)
    };
    let turn = operation.next(vfs(VfsEvent::Closed(last)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let refused = IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<
        Published,
        VfsError,
    >(VfsError::Denied)));
    assert_eq!(operation.next(refused), Err(VfsError::Denied));
}

#[test]
fn failed_delete_errors() {
    // A final delete refused with no durable checkpoint clears the disposition and is the
    // close's error, so the name never stays delete-pending behind a reported success.
    let mut operation = closing(context(), None);
    let turn = operation.next(vfs(VfsEvent::Closed(pending(Some(smb_owner())))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(refused(VfsError::Conflict));
    assert_eq!(asked(&turn), clear());
    assert_eq!(operation.next(vfs(VfsEvent::Done)), Err(VfsError::Conflict));
}

/// A close of open 22 run up to its `CloseOpen`.
fn closing(context: VfsAccessContext, stage: Option<NfsStageRecord>) -> VfsCloseOperation {
    let input = CloseInput {
        context,
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage,
    };
    let mut operation = VfsCloseOperation::new(input);
    operation.first().unwrap();
    operation
}

/// The last close of a stageless delete-pending instance with `owner` as its disposition
/// owner.
fn pending(owner: Option<StageOwner>) -> CloseSummary {
    CloseSummary {
        remaining_opens: 0,
        delete_pending: true,
        disposition_owner: owner,
        stage_id: None,
        ..summary(0)
    }
}

fn smb_owner() -> StageOwner {
    StageOwner {
        mount_id: Ulid::nil(),
        ..owner("KEY2", StageScope::User)
    }
}

fn refused(error: VfsError) -> IoResult {
    IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<(), VfsError>(
        error,
    )))
}

fn clear() -> VfsEffect {
    VfsEffect::ClearDisposition {
        scope: scope(),
        instance_id: 1,
    }
}

/// The removal the disposition owner runs through `context`.
fn removal(context: VfsAccessContext) -> IoCall {
    let summary = summary(0);
    IoCall::child(VfsRemoveOperation::new(RemoveInput {
        context,
        key: summary.key,
        kind: summary.kind,
        handle_id: summary.handle_id,
        identity: summary.identity,
        instance_id: Some(1),
        attempt_id: id(22),
    }))
}

#[test]
fn smb_owner_deletes() {
    // An SMB owner deletes through its own writable bucket share, never the read-only NFS
    // folder export of the open that closed last.
    let reader = VfsAccessContext {
        access_key: "KEY9".to_string(),
        target: NfsMountTarget::for_folder("bucket", 7, "p").unwrap(),
        read_only: true,
        ..context()
    };
    let mut operation = closing(reader, None);
    let turn = operation.next(vfs(VfsEvent::Closed(pending(Some(smb_owner())))));
    let share = VfsAccessContext {
        access_key: "KEY2".to_string(),
        protocol: VfsProtocol::Smb,
        target: NfsMountTarget::for_bucket("bucket", 7).unwrap(),
        read_only: false,
        ..context()
    };
    assert_eq!(turn, Ok(Next::Call(removal(share))));
}

fn row<T: VfsRecord>(record: &T) -> IoResult {
    IoResult::Values(vec![Some(Value::from(record.to_bytes().unwrap()))])
}

/// A final delete of the NFS owner of mount 13, up to its mount read.
fn nfs_deleting() -> VfsCloseOperation {
    let reader = VfsAccessContext {
        access_key: "KEY9".to_string(),
        read_only: true,
        ..context()
    };
    let mut operation = closing(reader, None);
    let nfs = owner("KEY1", StageScope::User);
    let turn = operation.next(vfs(VfsEvent::Closed(pending(Some(nfs)))));
    let mount = state_read(&[VfsKey::Mount(id(13))]).unwrap();
    assert_eq!(turn, Ok(Next::Call(mount)));
    operation
}

#[test]
fn nfs_owner_rechecked() {
    // An NFS owner deletes through its own live writable binding on a live host; a revoked or
    // read-only binding or a revoked host clears the disposition instead.
    let mut operation = nfs_deleting();
    let turn = operation.next(row(&binding()));
    let read = state_read(&[VfsKey::Host(id(30))]).unwrap();
    assert_eq!(turn, Ok(Next::Call(read)));
    let turn = operation.next(row(&host(None)));
    let export = VfsAccessContext {
        access_key: "KEY1".to_string(),
        protocol: VfsProtocol::Nfs,
        read_only: false,
        ..context()
    };
    assert_eq!(turn, Ok(Next::Call(removal(export))));

    let revoked = NfsMountBinding {
        revoked_at_ms: Some(9),
        ..binding()
    };
    let read_only = NfsMountBinding {
        read_only: true,
        ..binding()
    };
    for binding in [revoked, read_only] {
        let mut operation = nfs_deleting();
        assert_eq!(asked(&operation.next(row(&binding))), clear());
        assert_eq!(operation.next(vfs(VfsEvent::Done)), Err(VfsError::Denied));
    }
    let mut gone = nfs_deleting();
    gone.next(row(&binding())).unwrap();
    assert_eq!(asked(&gone.next(row(&host(Some(9))))), clear());
}

/// The release of instance 1 from the retained checkpoint `id(12)`.
fn retention() -> VfsEffect {
    VfsEffect::RetainInstance {
        scope: scope(),
        instance_id: 1,
        stage_id: id(12),
        generation: 2,
        reason: DetachReason::LostLife,
    }
}

/// Runs a retention's stage move and sync, and returns its outcome and instance release.
fn kept(operation: &mut VfsCloseOperation, turn: CloseTurn) -> (CloseOutcome, Vec<IoCall>) {
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the retained outcome with the instance release");
    };
    (output, calls)
}

#[test]
fn refused_delete_clears() {
    // A definitive refusal without a checkpoint clears the disposition and is the close's
    // error; with one the checkpoint is retained while the delete still blocks new opens.
    let mut bare = closing(context(), None);
    bare.next(vfs(VfsEvent::Closed(pending(Some(smb_owner())))))
        .unwrap();
    assert_eq!(asked(&bare.next(refused(VfsError::Denied))), clear());
    assert_eq!(bare.next(vfs(VfsEvent::Done)), Err(VfsError::Denied));

    let mut staged = closing(context(), Some(stage()));
    let last = CloseSummary {
        stage_id: Some(id(12)),
        ..pending(Some(smb_owner()))
    };
    staged.next(vfs(VfsEvent::Closed(last))).unwrap();
    let turn = staged.next(refused(VfsError::Denied));
    let (output, calls) = kept(&mut staged, turn);
    assert_eq!(output, CloseOutcome::Retained(id(12)));
    assert_eq!(calls, vec![vfs_call(retention())]);
    // A missing disposition owner is a refusal too.
    let mut ownerless = closing(context(), None);
    let turn = ownerless.next(vfs(VfsEvent::Closed(pending(None))));
    assert_eq!(asked(&turn), clear());
}

#[test]
fn transient_delete_retries() {
    // A transient failure keeps the pending delete; a retried close whose open is gone finds
    // the instance by id and finishes the delete.
    let mut first = closing(context(), None);
    first
        .next(vfs(VfsEvent::Closed(pending(Some(smb_owner())))))
        .unwrap();
    let down = VfsError::Unavailable("down".to_string());
    assert_eq!(first.next(refused(down.clone())), Err(down));

    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: None,
    };
    let mut retry = VfsCloseOperation::new(input).with_instance(1);
    assert!(matches!(
        asked(&retry.first()),
        VfsEffect::CloseOpen {
            detach_unstable: false,
            ..
        }
    ));
    let turn = retry.next(vfs(VfsEvent::Failed(VfsFailure::NotFound)));
    let lookup = VfsEffect::LookupById {
        scope: scope(),
        instance_id: 1,
    };
    assert_eq!(asked(&turn), lookup);
    let found = InstanceSummary {
        open_count: 0,
        delete_pending: true,
        disposition_owner: Some(smb_owner()),
        ..instance()
    };
    let turn = retry.next(vfs(VfsEvent::Instance(Some(found))));
    assert_eq!(turn, Ok(Next::Call(settling())));
    let turn = retry.next(child(ReceiptOutcome::RolledBack));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(retry.next(child(())), Ok(Next::Done(CloseOutcome::Deleted)));
}

/// The settlement of the receipt open 22's earlier delete attempt may have left.
fn settling() -> IoCall {
    IoCall::child(VfsReceiptOperation::new(ReceiptInput {
        scope: scope(),
        attempt_id: id(22),
        instance_id: Some(1),
        updates: Vec::new(),
        logical_size: None,
        expected_edit: None,
    }))
}

/// A refused final delete of an instance that names checkpoint `id(12)` with no stage read,
/// up to its answer to the live and retained row read.
fn staged_refusal(live: Option<Value>, held: Option<Value>) -> (VfsCloseOperation, CloseTurn) {
    let mut operation = closing(context(), None);
    let last = CloseSummary {
        stage_id: Some(id(12)),
        ..pending(Some(smb_owner()))
    };
    operation.next(vfs(VfsEvent::Closed(last))).unwrap();
    let turn = operation.next(refused(VfsError::Denied));
    let keys = [
        VfsKey::LiveStage(scope(), 4),
        VfsKey::RetainedStage(scope(), id(12)),
    ];
    assert_eq!(turn, Ok(Next::Call(state_read(&keys).unwrap())));
    let turn = operation.next(IoResult::Values(vec![live, held]));
    (operation, turn)
}

fn encoded(record: &NfsStageRecord) -> Option<Value> {
    Some(Value::from(record.to_bytes().unwrap()))
}

#[test]
fn staged_delete_loads() {
    // A refused delete of an instance whose checkpoint the close did not read loads the live
    // stage row and retains it, never clearing the disposition first.
    let (mut operation, turn) = staged_refusal(encoded(&stage()), None);
    let (output, calls) = kept(&mut operation, turn);
    assert_eq!(output, CloseOutcome::Retained(id(12)));
    assert_eq!(calls, vec![vfs_call(retention())]);

    // A row an earlier attempt already moved to retention only syncs before the release.
    let earlier = retained(&stage(), DetachReason::LostLife, 0);
    let (mut operation, turn) = staged_refusal(None, encoded(&earlier));
    assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the retained outcome with the instance release");
    };
    assert_eq!(output, CloseOutcome::Retained(id(12)));
    assert_eq!(calls, vec![vfs_call(retention())]);

    // Without either row the instance lets go of its checkpoint and the refusal is the error.
    let (mut operation, turn) = staged_refusal(None, None);
    assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
    assert_eq!(asked(&operation.next(IoResult::Done)), retention());
    assert_eq!(operation.next(vfs(VfsEvent::Done)), Err(VfsError::Denied));
}

#[test]
fn discard_reclaims_stage() {
    // A retried unlinked close whose earlier attempt deleted the live row before its sync
    // failed still syncs and reclaims the checkpoint the instance names.
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: None,
    };
    let mut retry = VfsCloseOperation::new(input).with_instance(1);
    retry.first().unwrap();
    retry
        .next(vfs(VfsEvent::Failed(VfsFailure::NotFound)))
        .unwrap();
    let found = InstanceSummary {
        open_count: 0,
        delete_pending: true,
        disposition_owner: Some(smb_owner()),
        detached: Some(DetachReason::Unlinked),
        stage_id: Some(id(12)),
        generation: 1,
        handle_id: Some(9),
        ..instance()
    };
    let turn = retry.next(vfs(VfsEvent::Instance(Some(found))));
    assert_eq!(turn, Ok(Next::Call(settling())));
    let turn = retry.next(child(ReceiptOutcome::RolledBack));
    let read = state_read(&[VfsKey::LiveStage(scope(), 9)]).unwrap();
    assert_eq!(turn, Ok(Next::Call(read)));
    let turn = retry.next(IoResult::Values(vec![None]));
    assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(12),
        generation: 1,
    };
    assert_eq!(asked(&retry.next(IoResult::Done)), remove);
    let turn = retry.next(vfs(VfsEvent::Done));
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Deleted)));
}

#[test]
fn retry_publishes_dirty() {
    // A retried close whose open is gone publishes a writerless dirty instance only when the
    // frontend prepared the publication again.
    let found = InstanceSummary {
        writer_count: 0,
        dirty: true,
        delete_pending: false,
        detached: None,
        ..instance()
    };
    for prepared_again in [true, false] {
        let publish = prepared_again.then(|| (publish_input(), prepared("p/a")));
        let input = CloseInput {
            context: context(),
            scope: scope(),
            open_id: id(22),
            publish,
            stage: None,
        };
        let mut retry = VfsCloseOperation::new(input).with_instance(1);
        retry.first().unwrap();
        retry
            .next(vfs(VfsEvent::Failed(VfsFailure::NotFound)))
            .unwrap();
        let turn = retry.next(vfs(VfsEvent::Instance(Some(found.clone()))));
        if !prepared_again {
            assert_eq!(turn, Err(VfsError::Vfs(VfsFailure::NotFound)));
            continue;
        }
        let publish = VfsPublishOperation::new(publish_input(), prepared("p/a"));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(publish))));
        let published = Published {
            version_id: id(2),
            life_id: None,
        };
        let turn = retry.next(child(published.clone()));
        assert_eq!(turn, Ok(Next::Done(CloseOutcome::Published(published))));
    }
}

#[test]
fn retry_settles_receipt() {
    // A retried close whose earlier removal committed before its sync failed only discards
    // the stage, never removing again or clearing the disposition.
    let input = CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage()),
    };
    let mut retry = VfsCloseOperation::new(input).with_instance(1);
    retry.first().unwrap();
    retry
        .next(vfs(VfsEvent::Failed(VfsFailure::NotFound)))
        .unwrap();
    let found = InstanceSummary {
        open_count: 0,
        delete_pending: true,
        disposition_owner: Some(smb_owner()),
        stage_id: Some(id(12)),
        ..instance()
    };
    let turn = retry.next(vfs(VfsEvent::Instance(Some(found))));
    assert_eq!(turn, Ok(Next::Call(settling())));
    let turn = retry.next(child(ReceiptOutcome::Applied { life_id: None }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(retry.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(12),
        generation: 2,
    };
    assert_eq!(asked(&retry.next(IoResult::Done)), remove);
    let turn = retry.next(vfs(VfsEvent::Done));
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Deleted)));
}

#[test]
fn marker_rewrites_owner() {
    // The frontend's marker template is written as the disposition owner at the closed
    // name's own parent, and a name at the bucket root passes no marker.
    let share = VfsAccessContext {
        access_key: "KEY2".to_string(),
        protocol: VfsProtocol::Smb,
        target: NfsMountTarget::for_bucket("bucket", 7).unwrap(),
        read_only: false,
        ..context()
    };
    let template = || {
        let mut put = prepared("placeholder/");
        put.user_id.user_ulid = id(9);
        put
    };
    let mut expected = prepared(&marker_key("dir"));
    expected.user_id = smb_owner().user;
    let cases = [("dir/a.txt", Some(expected)), ("a.txt", None)];
    for (key, marker) in cases {
        let input = CloseInput {
            context: context(),
            scope: scope(),
            open_id: id(22),
            publish: None,
            stage: None,
        };
        let mut operation = VfsCloseOperation::new(input).with_parent_marker(template());
        operation.first().unwrap();
        let last = CloseSummary {
            key: key.to_string(),
            ..pending(Some(smb_owner()))
        };
        let turn = operation.next(vfs(VfsEvent::Closed(last)));
        let summary = summary(0);
        let mut remove = VfsRemoveOperation::new(RemoveInput {
            context: share.clone(),
            key: key.to_string(),
            kind: summary.kind,
            handle_id: summary.handle_id,
            identity: summary.identity,
            instance_id: Some(1),
            attempt_id: id(22),
        });
        if let Some(put) = marker {
            remove = remove.with_parent_marker(put);
        }
        assert_eq!(turn, Ok(Next::Call(IoCall::child(remove))));
    }
}
