//! Namespace changes in one transaction: guarded writes, unlink and rename plans, publication
//! refusals, close outcomes and stage moves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles_tests::{asked, child, context, granted, head, id, no_open, scope, user, vfs};
use super::mutate::*;
use super::publish::*;
use super::recovery::ReceiptOutcome;
use super::{VfsError, state_key, watch_key};
use crate::s3::object::put::PutObjectError;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::vfs::{
    DetachReason, FileIdentity, HandleIdentity, HandleKind, LiveHandle, NfsStageRecord, QuotaRetry,
    ReceiptChange, StageBase, StageDisposition, StageOwner, StageScope, VfsHandleRow, VfsKey,
    VfsReceipt, VfsRecord, encode_handle_id, encode_token,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{CloseSummary, VfsEffect, VfsEvent};

fn live(key: &str) -> LiveHandle {
    LiveHandle {
        key: key.to_string(),
        identity: HandleIdentity::File(FileIdentity::Unpublished {
            token: id(6),
            predecessor: None,
        }),
        mode: 0o644,
        attribute_change: 1,
        cookie_id: 9,
        detached: None,
    }
}

fn stage(key: &str) -> NfsStageRecord {
    NfsStageRecord {
        stage_id: id(12),
        handle_id: 4,
        owner: StageOwner {
            user: user(),
            access_key: "KEY1".to_string(),
            mount_id: id(13),
            scope: StageScope::User,
        },
        key: key.to_string(),
        generation: 2,
        checkpoint: "c-2".to_string(),
        logical_size: 5,
        base: StageBase::Unpublished {
            token: id(6),
            predecessor: None,
        },
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

#[test]
fn guarded_write_refused() {
    let key = Key::from(b"k".to_vec());
    let plan = MovePlan {
        guards: vec![("nfs".to_string(), key.clone(), None)],
        writes: vec![("nfs".to_string(), key.clone(), Value::from(b"v".to_vec()))],
        deletes: vec![("nfs".to_string(), Key::from(b"old".to_vec()))],
    };
    let mut operation = GuardedWriteOperation::new(plan.clone());
    assert_eq!(operation.transaction(), TransactionMode::Write);
    operation.first().unwrap();
    let taken = IoResult::Values(vec![Some(Value::from(b"x".to_vec()))]);
    assert_eq!(operation.next(taken), Err(VfsError::Conflict));

    // A free guard writes, then deletes, in the same transaction.
    let mut operation = GuardedWriteOperation::new(plan.clone());
    operation.first().unwrap();
    let turn = operation.next(IoResult::Values(vec![None]));
    assert_eq!(turn, Ok(Next::Call(IoCall::Write(plan.writes.clone()))));
    let turn = operation.next(IoResult::Done);
    assert_eq!(turn, Ok(Next::Call(IoCall::Delete(plan.deletes))));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn unlink_keeps_opens() {
    let name = Value::from(encode_handle_id(4).to_vec());
    let (writes, deletes) = unlink_rows(&scope(), 4, &live("p/a"), true, 60, Some(&name)).unwrap();
    // A surviving open detaches with its reason instead of becoming a tombstone.
    let VfsHandleRow::Live(detached) = VfsHandleRow::from_bytes(&writes[0].2).unwrap() else {
        panic!("the handle stays live for its opens");
    };
    assert_eq!(detached.detached, Some(DetachReason::Unlinked));
    assert_eq!(deletes.len(), 1);
    let (writes, _) = unlink_rows(&scope(), 4, &live("p/a"), false, 60, Some(&name)).unwrap();
    let row = VfsHandleRow::from_bytes(&writes[0].2).unwrap();
    assert!(matches!(row, VfsHandleRow::Tombstone { .. }));
}

fn rename(handle: bool) -> RenameInput {
    RenameInput {
        context: context(),
        from: "p/a".to_string(),
        to: "q/b".to_string(),
        kind: HandleKind::File,
        identity: live("p/a").identity,
        handle_id: handle.then_some(4),
        instance_id: Some(1),
        replace: false,
        attempt_id: id(20),
        stage: Some(stage("p/a")),
        source_version: None,
    }
}

#[test]
fn unpublished_move_plan() {
    let token = Value::from(encode_token(id(6)).to_vec());
    let handle = Value::from(VfsHandleRow::Live(live("p/a")).to_bytes().unwrap());
    let name = Value::from(encode_handle_id(4).to_vec());
    let rows = vec![
        None,
        Some(token),
        Some(head(3, None)),
        None,
        None,
        None,
        Some(name),
        None,
        Some(handle),
    ];
    let plan = unpublished_move(&rename(true), &scope(), &rows, id(30), 70).unwrap();
    let state = |key: VfsKey| state_key(&key).unwrap();
    // Every changed row sits in this one plan: tokens, reverse rows, handle, stage, receipt.
    let receipt = state(VfsKey::Receipt(id(20)));
    assert!(
        plan.guards
            .iter()
            .any(|(_, key, value)| *key == receipt && value.is_none())
    );
    let source_watch = watch_key("bucket", "p/a").unwrap();
    assert!(plan.deletes.iter().any(|(_, key)| *key == source_watch));
    assert!(
        plan.deletes
            .iter()
            .any(|(_, key)| *key == state(VfsKey::Name(scope(), "p/a".to_string())))
    );
    let target_watch = watch_key("bucket", "q/b").unwrap();
    let written = |key: &Key| {
        plan.writes
            .iter()
            .find(|(_, row, _)| row == key)
            .map(|row| row.2.clone())
    };
    assert_eq!(
        written(&target_watch),
        Some(Value::from(encode_token(id(30)).to_vec()))
    );
    let moved = written(&state(VfsKey::Handle(scope(), 4))).unwrap();
    let VfsHandleRow::Live(moved) = VfsHandleRow::from_bytes(&moved).unwrap() else {
        panic!("the moved handle stays live");
    };
    assert_eq!(moved.key, "q/b");
    // The destination binds the fresh token over the destination's own marker predecessor.
    let fresh = FileIdentity::Unpublished {
        token: id(30),
        predecessor: Some(id(3)),
    };
    assert_eq!(moved.identity, HandleIdentity::File(fresh));
    let cookie = written(&state(VfsKey::Cookie(scope(), 9))).unwrap();
    let cookie = aruna_core::structs::storage::vfs::CookiePosition::from_bytes(&cookie).unwrap();
    assert_eq!(
        (cookie.directory_key.as_str(), cookie.entry_key.as_str()),
        ("p", "p/a")
    );
    let stage = written(&state(VfsKey::LiveStage(scope(), 4))).unwrap();
    assert_eq!(NfsStageRecord::from_bytes(&stage).unwrap().key, "q/b");
    let receipt = written(&receipt).unwrap();
    let receipt = VfsReceipt::from_bytes(&receipt).unwrap();
    assert!(matches!(
        receipt.change,
        ReceiptChange::UnpublishedRename { token, displaced: None, .. } if token == id(30)
    ));
}

#[test]
fn rename_refusals_first() {
    // A move out of the export and a directory into itself fail before any storage call.
    let mut outside = rename(false);
    outside.context.target =
        aruna_core::structs::storage::vfs::NfsMountTarget::for_folder("bucket", 7, "p").unwrap();
    assert_eq!(
        VfsRenameOperation::new(outside).first(),
        Err(VfsError::CrossExport)
    );
    let mut cycle = rename(false);
    (cycle.kind, cycle.from, cycle.to) =
        (HandleKind::Directory, "p".to_string(), "p/q".to_string());
    assert_eq!(
        VfsRenameOperation::new(cycle).first(),
        Err(VfsError::InvalidName)
    );
}

#[test]
fn committed_move_refused() {
    // A committed source needs a joint put and delete; it never moves as two commits.
    let mut input = rename(false);
    input.identity = HandleIdentity::File(FileIdentity::Committed { life_id: id(4) });
    let mut operation = VfsRenameOperation::new(input);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    let turn = operation.next(granted());
    assert!(matches!(asked(&turn), VfsEffect::AcquirePermit { .. }));
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation.next(no_open()).unwrap();
    operation.next(no_open()).unwrap();
    let rows = vec![
        Some(head(2, Some(4))),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ];
    let turn = operation.next(IoResult::Values(rows));
    assert_eq!(turn, Err(VfsError::Unavailable(JOINT_MOVE.to_string())));
    assert_eq!(operation.abort().len(), 1);
}

#[test]
fn unprepared_replace_refused() {
    // An unpublished file replaces a live destination only by publishing a prepared image.
    let mut input = rename(true);
    input.replace = true;
    let mut operation = VfsRenameOperation::new(input);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation.next(no_open()).unwrap();
    operation.next(no_open()).unwrap();
    let token = Value::from(encode_token(id(6)).to_vec());
    let rows = vec![
        None,
        Some(token),
        Some(head(3, Some(5))),
        None,
        None,
        None,
        None,
        None,
        None,
    ];
    let turn = operation.next(IoResult::Values(rows));
    assert_eq!(turn, Err(VfsError::Unavailable(JOINT_MOVE.to_string())));
}

#[test]
fn unpublished_remove_settles() {
    let input = RemoveInput {
        context: context(),
        key: "p/a".to_string(),
        kind: HandleKind::File,
        handle_id: None,
        identity: live("p/a").identity,
        instance_id: Some(1),
        attempt_id: id(21),
    };
    let mut operation = VfsRemoveOperation::new(input);
    operation.first().unwrap();
    let turn = operation.next(granted());
    assert!(matches!(asked(&turn), VfsEffect::AcquirePermit { .. }));
    let turn = operation.next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }));
    assert!(matches!(asked(&turn), VfsEffect::LookupInstance { .. }));
    let turn = operation.next(no_open());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let token = Value::from(encode_token(id(6)).to_vec());
    let rows = vec![None, None, Some(token)];
    // The token removal, the receipt and any handle rows run as one transaction.
    let turn = operation.next(IoResult::Values(rows));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let settled = ReceiptOutcome::Applied { life_id: None };
    let Ok(Next::Then { calls, .. }) = operation.next(child(settled)) else {
        panic!("expected the outcome with the permit release");
    };
    assert_eq!(calls.len(), 1);
}

#[test]
fn rolled_back_remove() {
    let input = RemoveInput {
        context: context(),
        key: "p/a".to_string(),
        kind: HandleKind::File,
        handle_id: None,
        identity: live("p/a").identity,
        instance_id: None,
        attempt_id: id(21),
    };
    let mut operation = VfsRemoveOperation::new(input);
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation.next(no_open()).unwrap();
    let token = Value::from(encode_token(id(6)).to_vec());
    operation
        .next(IoResult::Values(vec![None, None, Some(token)]))
        .unwrap();
    // An unknown outcome is settled only by the receipt after a sync.
    let unknown = IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<(), VfsError>(
        VfsError::Storage("lost reply".to_string()),
    )));
    assert_eq!(operation.next(unknown), Ok(Next::Call(IoCall::Sync)));
    operation.next(IoResult::Done).unwrap();
    let turn = operation.next(child(ReceiptOutcome::RolledBack));
    assert_eq!(turn, Err(VfsError::Conflict));
}

#[test]
fn put_refusals_classified() {
    assert_eq!(
        refusal(&PutObjectError::HeadChanged),
        Some(VfsError::Conflict)
    );
    assert_eq!(
        refusal(&PutObjectError::FenceTaken),
        Some(VfsError::Conflict)
    );
    let full = PutObjectError::QuotaExceeded { limit: 1, usage: 2 };
    assert_eq!(refusal(&full), Some(VfsError::QuotaExceeded));
    // A storage failure may hide a commit; only the receipt decides.
    let lost = PutObjectError::StorageError(StorageError::ReadError("lost".to_string()));
    assert_eq!(refusal(&lost), None);
}

#[test]
fn transfer_never_shortens() {
    let job = |seed| StageScope::Attempt {
        job_id: JobId::from_bytes([seed; 16]),
    };
    assert!(may_transfer(&StageScope::User, &StageScope::User));
    assert!(may_transfer(&job(5), &job(5)));
    assert!(!may_transfer(&job(5), &job(9)));
    assert!(!may_transfer(&StageScope::User, &job(5)));
    assert!(may_transfer(&job(5), &StageScope::User));
}

#[test]
fn quota_retry_backoff() {
    let first = next_retry(None, 1_000);
    assert_eq!(
        first,
        QuotaRetry {
            attempts: 1,
            next_at_ms: 1_000 + QUOTA_WAIT_MS
        }
    );
    let second = next_retry(Some(first), 2_000);
    assert_eq!(second.next_at_ms, 2_000 + 2 * QUOTA_WAIT_MS);
    let late = next_retry(
        Some(QuotaRetry {
            attempts: 40,
            next_at_ms: 0,
        }),
        0,
    );
    assert_eq!(late.next_at_ms, QUOTA_WAIT_MAX);
}

fn summary(remaining_opens: usize, detached: Option<DetachReason>) -> CloseSummary {
    CloseSummary {
        instance_id: 1,
        key: "p/a".to_string(),
        kind: HandleKind::File,
        remaining_opens,
        remaining_writers: 0,
        delete_pending: false,
        dirty: false,
        detached,
        disposition_owner: None,
        stage_id: Some(id(12)),
        generation: 2,
        logical_size: 5,
        handle_id: Some(4),
        identity: live("p/a").identity,
        base_version: None,
        edit_generation: 1,
        successor_owner: None,
    }
}

fn closing() -> VfsCloseOperation {
    VfsCloseOperation::new(CloseInput {
        context: context(),
        scope: scope(),
        open_id: id(22),
        publish: None,
        stage: Some(stage("p/a")),
    })
}

#[test]
fn close_keeps_opens() {
    let mut operation = closing();
    assert!(matches!(
        asked(&operation.first()),
        VfsEffect::CloseOpen { .. }
    ));
    let turn = operation.next(vfs(VfsEvent::Closed(summary(1, None))));
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Closed)));
}

#[test]
fn unlinked_close_discards() {
    // Only a proven unlink discards the stage row and then its checkpoint file.
    let mut operation = closing();
    operation.first().unwrap();
    let turn = operation.next(vfs(VfsEvent::Closed(summary(
        0,
        Some(DetachReason::Unlinked),
    ))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(child(())), Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(12),
        generation: 2,
    };
    assert_eq!(asked(&turn), remove);
    let turn = operation.next(vfs(VfsEvent::Done));
    assert_eq!(turn, Ok(Next::Done(CloseOutcome::Discarded)));
}

#[test]
fn lost_life_retains() {
    // A stage whose life ended externally is kept by its stage id; its file stays.
    let mut operation = closing();
    operation.first().unwrap();
    let turn = operation.next(vfs(VfsEvent::Closed(summary(
        0,
        Some(DetachReason::LostLife),
    ))));
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
    let kept = retained(&stage("p/a"), DetachReason::LostLife, 9);
    assert_eq!(
        kept.disposition,
        StageDisposition::Retained {
            reason: DetachReason::LostLife
        }
    );
}

#[test]
fn stage_move_checked() {
    let row = Value::from(stage("p/a").to_bytes().unwrap());
    let kept = retained(&stage("p/a"), DetachReason::LostLife, 9);
    let mut operation = StageMoveOperation::new(scope(), stage("p/a"), Some(kept.clone()));
    operation.first().unwrap();
    // The live row goes and the retained row appears under its stage id, in one transaction.
    let turn = operation.next(IoResult::Values(vec![Some(row)]));
    let live_key = state_key(&VfsKey::LiveStage(scope(), 4)).unwrap();
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Delete(rows))) if rows[0].1 == live_key));
    let turn = operation.next(IoResult::Done);
    let kept_key = state_key(&VfsKey::RetainedStage(scope(), id(12))).unwrap();
    assert!(matches!(&turn, Ok(Next::Call(IoCall::Write(rows))) if rows[0].1 == kept_key));
    let mut changed = StageMoveOperation::new(scope(), stage("p/a"), None);
    changed.first().unwrap();
    assert_eq!(
        changed.next(IoResult::Values(vec![None])),
        Err(VfsError::Conflict)
    );
}
