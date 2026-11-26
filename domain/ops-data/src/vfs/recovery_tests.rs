//! Receipt settlement after a sync, cold-start recovery before serving and the bounded
//! tombstone sweep.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::handles_tests::{asked, child, context, denied, granted, head, id, scope, user, vfs};
use super::recovery::*;
use super::{TOMBSTONE_RETENTION_MS, VfsError, state_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::vfs::{
    CookiePosition, EnrolledHostRecord, NfsMountBinding, NfsMountTarget, NfsStageRecord,
    QuotaRetry, RECEIPT_TAG, ReceiptChange, RetireReason, StageBase, StageDisposition, StageOwner,
    StageScope, VfsBootState, VfsHandleRow, VfsKey, VfsReceipt, VfsRecord,
};
use aruna_core::types::{Key, Value};
use aruna_core::vfs_effects::{DiskStage, VfsEffect, VfsEvent};

fn input() -> ReceiptInput {
    ReceiptInput {
        scope: scope(),
        attempt_id: id(20),
        instance_id: Some(1),
        updates: Vec::new(),
        logical_size: Some(5),
        expected_edit: None,
    }
}

fn row<T: VfsRecord>(record: &T) -> Option<Value> {
    Some(Value::from(record.to_bytes().unwrap()))
}

#[test]
fn absent_receipt_rollback() {
    // An absent receipt counts as a rollback only after a successful sync.
    let mut operation = VfsReceiptOperation::new(input());
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Sync)));
    assert!(matches!(
        operation.next(IoResult::Done),
        Ok(Next::Call(IoCall::Read(_)))
    ));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Values(vec![None])) else {
        panic!("expected the rollback with the attempt released");
    };
    assert_eq!(output, ReceiptOutcome::RolledBack);
    assert_eq!(calls.len(), 2);
}

#[test]
fn removal_receipt_detaches() {
    let receipt = VfsReceipt {
        attempt_id: id(20),
        scope: scope(),
        created_at_ms: 1,
        change: ReceiptChange::Remove {
            key: "p/a".to_string(),
            marker_version: None,
            handle_id: Some(4),
        },
    };
    let mut operation = VfsReceiptOperation::new(input());
    operation.first().unwrap();
    operation.next(IoResult::Done).unwrap();
    let turn = operation.next(IoResult::Values(vec![row(&receipt)]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Values(vec![None])) else {
        panic!("expected the settled outcome");
    };
    assert_eq!(output, ReceiptOutcome::Applied { life_id: None });
    // Detach the surviving open, release the attempt, then retire the receipt last.
    assert_eq!(calls.len(), 4);
    let receipt_key = state_key(&VfsKey::Receipt(id(20))).unwrap();
    assert!(matches!(calls.last(), Some(IoCall::Delete(rows)) if rows[0].1 == receipt_key));
}

#[test]
fn publish_receipt_advances() {
    // The finalizer reads the committed life and advances the handle and the ancestor whose
    // witness rested on the unpublished name, in one guarded write.
    let receipt = VfsReceipt {
        attempt_id: id(20),
        scope: scope(),
        created_at_ms: 1,
        change: ReceiptChange::FilePublish {
            key: "p/a".to_string(),
            version_id: id(2),
            life_id: ulid::Ulid::nil(),
            handle_id: Some(4),
            stage_id: None,
        },
    };
    let mut operation = VfsReceiptOperation::new(input());
    operation.first().unwrap();
    operation.next(IoResult::Done).unwrap();
    operation
        .next(IoResult::Values(vec![row(&receipt)]))
        .unwrap();
    let turn = operation.next(IoResult::Values(vec![Some(head(2, Some(8)))]));
    let Ok(Next::Call(IoCall::Read(reads))) = turn else {
        panic!("expected the handle and ancestor rows");
    };
    assert_eq!(reads.len(), 2);
    let name = Value::from(aruna_core::structs::storage::vfs::encode_handle_id(3).to_vec());
    let file = aruna_core::structs::storage::vfs::LiveHandle {
        key: "p/a".to_string(),
        identity: aruna_core::structs::storage::vfs::HandleIdentity::File(
            aruna_core::structs::storage::vfs::FileIdentity::Unpublished {
                token: id(6),
                predecessor: None,
            },
        ),
        mode: 0o644,
        attribute_change: 1,
        cookie_id: 9,
        detached: None,
    };
    let turn = operation.next(IoResult::Values(vec![
        row(&VfsHandleRow::Live(file.clone())),
        Some(name),
    ]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let directory = aruna_core::structs::storage::vfs::LiveHandle {
        key: "p".to_string(),
        identity: aruna_core::structs::storage::vfs::HandleIdentity::Directory(
            aruna_core::structs::storage::vfs::DirectoryWitness::Unpublished {
                key: "p/a".to_string(),
                token: id(6),
            },
        ),
        ..file.clone()
    };
    let rows = vec![
        row(&VfsHandleRow::Live(file)),
        row(&VfsHandleRow::Live(directory)),
    ];
    let turn = operation.next(IoResult::Values(rows));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let Ok(Next::Then { output, calls }) = operation.next(child(())) else {
        panic!("expected the settled outcome");
    };
    assert_eq!(
        output,
        ReceiptOutcome::Applied {
            life_id: Some(id(8))
        }
    );
    let rebind = calls.iter().any(|call| {
        matches!(call, IoCall::Effect(effect)
            if matches!(effect.as_ref(), aruna_core::effects::Effect::Vfs(effect)
                if matches!(effect.as_ref(), VfsEffect::RebindInstance(_))))
    });
    let witnesses = calls.iter().any(|call| {
        matches!(call, IoCall::Effect(effect)
            if matches!(effect.as_ref(), aruna_core::effects::Effect::Vfs(effect)
                if matches!(effect.as_ref(), VfsEffect::UpdateWitnesses { updates, .. }
                    if updates.len() == 1)))
    });
    assert!(rebind && witnesses);
}

fn stage(generation: u64, retry: Option<QuotaRetry>) -> NfsStageRecord {
    NfsStageRecord {
        stage_id: id(12),
        handle_id: 4,
        owner: StageOwner {
            user: user(),
            access_key: "KEY1".to_string(),
            mount_id: id(13),
            scope: StageScope::User,
        },
        key: "p/a".to_string(),
        generation,
        checkpoint: format!("c-{generation}"),
        logical_size: 5,
        base: StageBase::Committed {
            version_id: id(2),
            life_id: id(8),
        },
        source_copies: Vec::new(),
        source_policies: Vec::new(),
        destination_copies: Vec::new(),
        destination_policies: Vec::new(),
        disposition: StageDisposition::Live,
        quota_retry: retry,
        foreign_life: None,
        updated_at_ms: 1,
    }
}

#[test]
fn cold_start_order() {
    let mut operation = ColdStartOperation::new();
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    let boot = VfsBootState {
        boot_generation: 4,
        ..VfsBootState::default()
    };
    // The next boot generation is written and synced before the adapter is configured.
    let Ok(Next::Call(IoCall::Write(writes))) = operation.next(IoResult::Values(vec![row(&boot)]))
    else {
        panic!("expected the boot row");
    };
    assert_eq!(
        VfsBootState::from_bytes(&writes[0].2)
            .unwrap()
            .boot_generation,
        5
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    let configure = VfsEffect::ConfigureBoot {
        boot_generation: 5,
        verifier: boot_verifier(5),
    };
    assert_eq!(asked(&turn), configure);
    let Ok(Next::Call(IoCall::Page { prefix, .. })) = operation.next(vfs(VfsEvent::Done)) else {
        panic!("receipts are settled first");
    };
    assert_eq!(prefix.as_ref(), [RECEIPT_TAG].as_slice());
    let turn = operation.next(IoResult::Page {
        rows: Vec::new(),
        next: None,
    });
    assert_eq!(asked(&turn), VfsEffect::ScanStages);
    // One referenced checkpoint is missing and one file on disk is named by no row.
    let orphan = DiskStage {
        stage_id: id(30),
        generation: 1,
        bytes: 3,
        corrupt: false,
    };
    let turn = operation.next(vfs(VfsEvent::StageInventory {
        files: vec![orphan],
    }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
    let key = Key::from(VfsKey::LiveStage(scope(), 4).to_bytes().unwrap());
    let rows = vec![(key, row(&stage(2, None)).unwrap())];
    let turn = operation.next(IoResult::Page { rows, next: None });
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(30),
        generation: 1,
    };
    assert_eq!(asked(&turn), remove);
    let Ok(Next::Done(report)) = operation.next(vfs(VfsEvent::Done)) else {
        panic!("expected the report");
    };
    assert_eq!(report.corrupt, vec![id(12)]);
    assert_eq!(report.orphans, vec![(id(30), 1)]);
    assert_ne!(boot_verifier(5), boot_verifier(6));
}

#[test]
fn sweep_removes_expired() {
    let now = TOMBSTONE_RETENTION_MS + 100;
    let tombstone = |retired_at_ms| VfsHandleRow::Tombstone {
        reason: RetireReason::Stale,
        retired_at_ms,
    };
    let key = |key: VfsKey| Key::from(key.to_bytes().unwrap());
    let old = key(VfsKey::Handle(scope(), 1));
    let fresh = key(VfsKey::Handle(scope(), 2));
    let cookie = key(VfsKey::Cookie(scope(), 3));
    let position = CookiePosition {
        directory_key: "p".to_string(),
        entry_key: "p/a".to_string(),
        retired_at_ms: 50,
    };
    let due = QuotaRetry {
        attempts: 1,
        next_at_ms: now,
    };
    let rows = vec![
        (old.clone(), row(&tombstone(50)).unwrap()),
        (fresh, row(&tombstone(now)).unwrap()),
        (cookie.clone(), row(&position).unwrap()),
        (
            key(VfsKey::LiveStage(scope(), 4)),
            row(&stage(2, Some(due))).unwrap(),
        ),
    ];
    let mut operation = SweepTombstonesOperation::new(now, None);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let turn = operation.next(IoResult::Page { rows, next: None });
    let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
        panic!("expected the expired rows, got {turn:?}");
    };
    let deleted: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
    assert_eq!(deleted, vec![old, cookie]);
    // The state pass is done, so the token pass starts in the same run.
    assert!(matches!(
        operation.next(IoResult::Done),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let Ok(Next::Done(outcome)) = operation.next(IoResult::Page {
        rows: Vec::new(),
        next: None,
    }) else {
        panic!("expected the sweep outcome");
    };
    assert_eq!(outcome.removed, 2);
    assert_eq!(outcome.due, vec![(scope(), 4)]);
    assert_eq!(outcome.next, None);
}

#[test]
fn sweep_resumes_cursor() {
    // A page that left rows hands back a cursor of its pass.
    let mut operation = SweepTombstonesOperation::new(1, None);
    operation.first().unwrap();
    let last = Key::from(VfsKey::Handle(scope(), 2).to_bytes().unwrap());
    let turn = operation.next(IoResult::Page {
        rows: Vec::new(),
        next: Some(last.clone()),
    });
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    let Ok(Next::Done(outcome)) = operation.next(IoResult::Done) else {
        panic!("expected the outcome");
    };
    let cursor = outcome.next.unwrap();
    assert_eq!(cursor[0], 0);
    let mut resumed = SweepTombstonesOperation::new(1, Some(cursor));
    let Ok(Next::Call(IoCall::Page { after, .. })) = resumed.first() else {
        panic!("expected the resumed page");
    };
    assert_eq!(after, Some(last));
}

#[test]
fn finished_receipts_settle() {
    // A receipt whose owner no longer runs is settled by the sweep.
    let receipt = VfsReceipt {
        attempt_id: id(20),
        scope: scope(),
        created_at_ms: 1,
        change: ReceiptChange::Remove {
            key: "p/a".to_string(),
            marker_version: None,
            handle_id: None,
        },
    };
    let key = Key::from(VfsKey::Receipt(id(20)).to_bytes().unwrap());
    let mut operation = SweepTombstonesOperation::new(1, None);
    operation.first().unwrap();
    let rows = vec![(key, row(&receipt).unwrap())];
    let turn = operation.next(IoResult::Page { rows, next: None });
    assert!(matches!(asked(&turn), VfsEffect::ActiveAttempts { .. }));
    let turn = operation.next(vfs(VfsEvent::Attempts { ids: Vec::new() }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let settled = ReceiptOutcome::Applied { life_id: None };
    let turn = operation.next(child(settled));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));

    // An active owner keeps its receipt.
    let key = Key::from(VfsKey::Receipt(id(20)).to_bytes().unwrap());
    let mut busy = SweepTombstonesOperation::new(1, None);
    busy.first().unwrap();
    busy.next(IoResult::Page {
        rows: vec![(key, row(&receipt).unwrap())],
        next: None,
    })
    .unwrap();
    let turn = busy.next(vfs(VfsEvent::Attempts { ids: vec![id(20)] }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
}

/// A retained copy of [`stage`] by its stage id, last changed at `updated_at_ms`.
fn kept(stage_id: u8, updated_at_ms: u64) -> (Key, Value) {
    let mut record = stage(3, None);
    record.stage_id = id(stage_id);
    record.disposition = StageDisposition::Retained {
        reason: aruna_core::structs::storage::vfs::DetachReason::LostLife,
    };
    record.updated_at_ms = updated_at_ms;
    let key = Key::from(
        VfsKey::RetainedStage(scope(), id(stage_id))
            .to_bytes()
            .unwrap(),
    );
    (key, row(&record).unwrap())
}

#[test]
fn retained_stage_expires() {
    // Custom windows: tombstones after 10 ms, retained checkpoints after 20 ms. A live
    // checkpoint waiting for quota never expires, however old it is.
    let now = 30;
    let key = |key: VfsKey| Key::from(key.to_bytes().unwrap());
    let tombstone = |retired_at_ms| VfsHandleRow::Tombstone {
        reason: RetireReason::Stale,
        retired_at_ms,
    };
    let (old, fresh) = (
        key(VfsKey::Handle(scope(), 1)),
        key(VfsKey::Handle(scope(), 2)),
    );
    let (expired, expired_row) = kept(40, 5);
    let (recent, recent_row) = kept(41, 15);
    let due = QuotaRetry {
        attempts: 3,
        next_at_ms: now,
    };
    let live = key(VfsKey::LiveStage(scope(), 4));
    let rows = vec![
        (old.clone(), row(&tombstone(15)).unwrap()),
        (fresh, row(&tombstone(25)).unwrap()),
        (live, row(&stage(2, Some(due))).unwrap()),
        (expired.clone(), expired_row),
        (recent, recent_row),
    ];
    let mut operation = SweepTombstonesOperation::new(now, None).with_retention(10, 20);
    operation.first().unwrap();
    let turn = operation.next(IoResult::Page { rows, next: None });
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(40),
        generation: 3,
    };
    assert_eq!(asked(&turn), remove);
    let turn = operation.next(vfs(VfsEvent::Done));
    let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
        panic!("expected the expired rows, got {turn:?}");
    };
    let deleted: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
    assert_eq!(deleted, vec![old, expired]);
    operation.next(IoResult::Done).unwrap();
    let Ok(Next::Done(outcome)) = operation.next(IoResult::Page {
        rows: Vec::new(),
        next: None,
    }) else {
        panic!("expected the sweep outcome");
    };
    assert_eq!(outcome.removed, 2);
    assert_eq!(outcome.due, vec![(scope(), 4)]);
}

#[test]
fn failed_removal_keeps() {
    // A checkpoint file the adapter could not remove keeps its row for the next sweep.
    let (expired, expired_row) = kept(40, 1);
    let now = super::STAGE_RETENTION_MS + 1;
    let mut operation = SweepTombstonesOperation::new(now, None);
    operation.first().unwrap();
    let rows = vec![(expired, expired_row)];
    let turn = operation.next(IoResult::Page { rows, next: None });
    assert!(matches!(asked(&turn), VfsEffect::RemoveCheckpoint { .. }));
    let failed = VfsEvent::Failed(aruna_core::vfs_effects::VfsFailure::Unconfigured);
    let turn = operation.next(vfs(failed));
    assert_eq!(turn, Ok(Next::Call(IoCall::Delete(Vec::new()))));
}

/// Runs a cold start whose one live stage row is `record` up to its head read.
fn heading(record: &NfsStageRecord) -> ColdStartOperation {
    let admission = context();
    let mut operation =
        ColdStartOperation::new().with_admission(admission.realm_id, admission.node_id);
    operation.first().unwrap();
    operation.next(IoResult::Values(vec![None])).unwrap();
    operation.next(IoResult::Done).unwrap();
    operation.next(IoResult::Done).unwrap();
    operation.next(vfs(VfsEvent::Done)).unwrap();
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    operation.next(empty).unwrap();
    let file = DiskStage {
        stage_id: record.stage_id,
        generation: record.generation,
        bytes: record.logical_size,
        corrupt: false,
    };
    let files = vec![file];
    operation
        .next(vfs(VfsEvent::StageInventory { files }))
        .unwrap();
    let key = Key::from(VfsKey::LiveStage(scope(), 4).to_bytes().unwrap());
    let rows = vec![(key, row(record).unwrap())];
    let turn = operation.next(IoResult::Page { rows, next: None });
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let bucket = aruna_test_harness::s3::bucket_info(id(3), user());
    let bucket = Value::from(bucket.to_bytes().unwrap());
    let creation = Value::from(7u128.to_be_bytes().to_vec());
    let turn = operation.next(IoResult::Values(vec![Some(bucket), Some(creation)]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    operation
}

/// Answers the retention of the current stage, its scratch charge as a detached retained row
/// and the empty orphan pass with the report.
fn retained_report(operation: &mut ColdStartOperation, turn: Result<Next<ColdStart>, VfsError>) {
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(child(()));
    let VfsEffect::RestoreStage(request) = asked(&turn) else {
        panic!("expected the retained checkpoint to be charged");
    };
    let lost = aruna_core::structs::storage::vfs::DetachReason::LostLife;
    assert_eq!(request.detached, Some(lost));
    let Ok(Next::Done(report)) = operation.next(vfs(VfsEvent::Done)) else {
        panic!("expected the report");
    };
    assert_eq!((report.retained, report.restored), (1, 0));
}

#[test]
fn overwritten_base_retained() {
    // A same-life S3 overwrite leaves the newer head visible; the checkpoint is retained.
    let mut operation = heading(&stage(2, None));
    let turn = operation.next(IoResult::Values(vec![Some(head(9, Some(8))), None]));
    retained_report(&mut operation, turn);
}

fn binding() -> NfsMountBinding {
    NfsMountBinding {
        mount_id: id(13),
        host_id: id(30),
        owner: user(),
        access_key: "KEY1".to_string(),
        group_id: id(3),
        target: NfsMountTarget::for_bucket("bucket", 7).unwrap(),
        read_only: false,
        allowed_networks: Vec::new(),
        display_uid: 1000,
        display_gid: 1000,
        mode: 0o750,
        created_at_ms: 1,
    }
}

fn host(revoked_at_ms: Option<u64>) -> EnrolledHostRecord {
    EnrolledHostRecord {
        host_id: id(30),
        owner: user(),
        fingerprints: vec![[1; 32]],
        allowed_networks: Vec::new(),
        worker_pool: false,
        created_at_ms: 1,
        revoked_at_ms,
    }
}

/// Runs a cold start of a stage on its exact base up to the answer of its host row.
fn hosted(revoked_at_ms: Option<u64>) -> (ColdStartOperation, Result<Next<ColdStart>, VfsError>) {
    let mut operation = heading(&stage(2, None));
    let turn = operation.next(IoResult::Values(vec![Some(head(2, Some(8))), None]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![row(&binding())]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![row(&host(revoked_at_ms))]));
    (operation, turn)
}

#[test]
fn revoked_host_retained() {
    // A writer whose host was revoked does not get its stage back on the name.
    let (mut operation, turn) = hosted(Some(9));
    retained_report(&mut operation, turn);
}

#[test]
fn live_binding_restores() {
    // A stage on its exact base with a live binding restores with that base version once its
    // writer's credential is admitted to write the name again.
    let (mut operation, turn) = hosted(None);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
    let VfsEffect::RestoreStage(request) = asked(&turn) else {
        panic!("expected the restore");
    };
    assert_eq!(request.base_version, Some(id(2)));
    let Ok(Next::Done(report)) = operation.next(vfs(VfsEvent::Done)) else {
        panic!("expected the report");
    };
    assert_eq!((report.retained, report.restored), (0, 1));
}

#[test]
fn denied_writer_retained() {
    // A writer whose credential no longer admits the write keeps its bytes by stage id only.
    let (mut operation, turn) = hosted(None);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(denied());
    retained_report(&mut operation, turn);
}

#[test]
fn pool_host_restores() {
    // A trusted worker pool host serves another owner's mount, so its stage is admitted again;
    // a plain host of another owner retains the stage.
    let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
    let stranger = aruna_core::user::id::UserId::new(id(40), realm);
    let foreign = |worker_pool| EnrolledHostRecord {
        owner: stranger,
        worker_pool,
        ..host(None)
    };
    for (worker_pool, admitted) in [(true, true), (false, false)] {
        let mut operation = heading(&stage(2, None));
        operation
            .next(IoResult::Values(vec![Some(head(2, Some(8))), None]))
            .unwrap();
        operation
            .next(IoResult::Values(vec![row(&binding())]))
            .unwrap();
        let turn = operation.next(IoResult::Values(vec![row(&foreign(worker_pool))]));
        match admitted {
            true => {
                assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
                let turn = operation.next(granted());
                assert!(matches!(asked(&turn), VfsEffect::RestoreStage(_)));
            }
            false => retained_report(&mut operation, turn),
        }
    }
}
