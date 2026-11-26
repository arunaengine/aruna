//! Guarded NFS checkpoints with fresh write admission and owned aborts, and directory opens
//! that page past delete markers to a child the caller may see.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::handles_tests::{
    asked, context, denied, granted, head, id, instance, scope, vfs,
};
use super::super::mutate::tests::dirty_stage;
use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::storage::vfs::VfsRecord;
use aruna_core::types::Value;
use aruna_core::vfs_effects::VfsFailure;

fn release() -> IoCall {
    vfs_call(VfsEffect::ReleaseGuard { guard_id: id(10) })
}

/// A commit of handle 4 run up to its guarded instance lookup.
fn committing() -> VfsCommitOperation {
    let mut operation = VfsCommitOperation::new(CommitInput {
        context: context(),
        instance_id: 1,
        record: dirty_stage(),
        previous: None,
    });
    let turn = operation.first();
    assert!(matches!(asked(&turn), VfsEffect::AcquireGuards { .. }));
    let turn = operation.next(vfs(VfsEvent::GuardAcquired { guard_id: id(10) }));
    assert!(matches!(
        asked(&turn),
        VfsEffect::LookupHandle { handle_id: 4, .. }
    ));
    operation
}

/// A dirty commit run up to its claim, with the live row at generation 5.
fn begun() -> VfsCommitOperation {
    let mut operation = committing();
    operation
        .next(vfs(VfsEvent::Instance(Some(instance()))))
        .unwrap();
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let live = NfsStageRecord {
        generation: 5,
        ..dirty_stage()
    };
    let row = Value::from(live.to_bytes().unwrap());
    // The generation follows the row read under the guard, not the caller's guess.
    let turn = operation.next(IoResult::Values(vec![Some(row)]));
    assert!(matches!(
        asked(&turn),
        VfsEffect::BeginCheckpoint { generation: 6, .. }
    ));
    operation
}

#[test]
fn clean_commit_skips() {
    // A clean instance is still admitted to write at its guarded size, then makes no checkpoint.
    let mut operation = committing();
    let clean = InstanceSummary {
        dirty: false,
        ..instance()
    };
    let turn = operation.next(vfs(VfsEvent::Instance(Some(clean))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
    assert_eq!(asked(&turn), VfsEffect::WriteVerifier);
    let turn = operation.next(vfs(VfsEvent::Verifier { verifier: [7; 8] }));
    let Ok(Next::Then { output, calls }) = turn else {
        panic!("expected the verifier with the guard release");
    };
    assert_eq!((output, calls), ([7; 8], vec![release()]));
}

#[test]
fn commit_needs_put() {
    // A writer that lost write access cannot make its bytes durable, even with read access.
    let mut operation = committing();
    operation
        .next(vfs(VfsEvent::Instance(Some(instance()))))
        .unwrap();
    assert_eq!(operation.next(denied()), Err(VfsError::Denied));
    assert_eq!(operation.abort(), vec![release()]);
}

#[test]
fn lost_claim_untouched() {
    // A request whose claim was refused never drops the generation another request owns.
    let mut operation = begun();
    let conflict = vfs(VfsEvent::Failed(VfsFailure::Conflict));
    assert_eq!(operation.next(conflict), Err(VfsError::Conflict));
    assert_eq!(operation.abort(), vec![release()]);
}

#[test]
fn abort_drops_own() {
    // An accepted claim is aborted as that claim, never removed as a plain file; base ranges
    // to copy need an exact-version read admission first.
    let mut operation = begun();
    let plan = VfsEvent::CheckpointPlan {
        missing_base: vec![ByteRange {
            offset: 5,
            length: 3,
        }],
        logical_size: 8,
    };
    let turn = operation.next(vfs(plan));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let abort = VfsEffect::AbortCheckpoint {
        scope: scope(),
        instance_id: 1,
        stage_id: id(12),
        generation: 6,
    };
    assert_eq!(operation.abort(), vec![vfs_call(abort), release()]);
}

#[test]
fn uncertain_row_read() {
    // A row write with an unknown outcome is read back after a sync before anything is undone.
    let mut operation = begun();
    let plan = VfsEvent::CheckpointPlan {
        missing_base: Vec::new(),
        logical_size: 5,
    };
    let turn = operation.next(vfs(plan));
    assert!(matches!(
        asked(&turn),
        VfsEffect::SealCheckpoint { generation: 6, .. }
    ));
    let sealed = VfsEvent::CheckpointWritten {
        filename: "c-6".to_string(),
        size: 5,
    };
    let turn = operation.next(vfs(sealed));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let lost = IoResult::Child(ChildOutcome::new(Err::<(), VfsError>(VfsError::Storage(
        "lost reply".to_string(),
    ))));
    assert_eq!(operation.next(lost), Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let written = NfsStageRecord {
        generation: 6,
        checkpoint: "c-6".to_string(),
        ..dirty_stage()
    };
    let row = Value::from(written.to_bytes().unwrap());
    let turn = operation.next(IoResult::Values(vec![Some(row)]));
    assert!(matches!(
        asked(&turn),
        VfsEffect::PromoteCheckpoint {
            generation: 6,
            old_generation: Some(5),
            ..
        }
    ));
}

#[test]
fn hidden_child_missing() {
    // An implicit directory pages past a full page of delete markers to its live child, and
    // shows only when the caller may see that child.
    let mut operation = VfsOpenOperation::new(OpenInput {
        context: context(),
        key: "d".to_string(),
        kind: HandleKind::Directory,
        create: false,
        exclusive: false,
        write: false,
        share: ShareMode::default(),
        delete_on_close: false,
        open_id: id(20),
        stable: false,
        mode: 0,
        mount_id: Ulid::nil(),
        stage_scope: StageScope::User,
    });
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    let turn = operation.next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![None, None, None]));
    assert!(matches!(
        turn,
        Ok(Next::Call(IoCall::Page { after: None, .. }))
    ));
    let rows: Vec<_> = (0..STATE_PAGE)
        .map(|index| {
            let key = super::super::head_key("bucket", &format!("d/{index:03}")).unwrap();
            (key, head(2, None))
        })
        .collect();
    let last = rows.last().map(|row| row.0.clone());
    let turn = operation.next(IoResult::Page {
        rows,
        next: last.clone(),
    });
    assert!(matches!(turn, Ok(Next::Call(IoCall::Page { ref after, .. })) if *after == last));
    let live = vec![(
        super::super::head_key("bucket", "d/zz").unwrap(),
        head(6, Some(7)),
    )];
    let turn = operation.next(IoResult::Page {
        rows: live,
        next: None,
    });
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { .. }));
    let staged = VfsEvent::StagedList {
        entries: Vec::new(),
        sequence: 1,
        truncated: false,
    };
    let turn = operation.next(vfs(staged));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(denied()), Err(VfsError::NotFound));
}

#[test]
fn open_binds_handle() {
    // A stable open binds its freshly claimed handle onto the live instance before returning.
    let mut operation = VfsOpenOperation::new(OpenInput {
        context: context(),
        key: "p/new".to_string(),
        kind: HandleKind::File,
        create: true,
        exclusive: false,
        write: true,
        share: ShareMode::default(),
        delete_on_close: false,
        open_id: id(21),
        stable: true,
        mode: 0o600,
        mount_id: id(13),
        stage_scope: StageScope::User,
    });
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation
        .next(IoResult::Values(vec![None, None, None]))
        .unwrap();
    operation
        .next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        })
        .unwrap();
    operation
        .next(vfs(VfsEvent::StagedList {
            entries: Vec::new(),
            sequence: 1,
            truncated: false,
        }))
        .unwrap();
    operation
        .next(IoResult::Child(ChildOutcome::new(Ok::<(), VfsError>(()))))
        .unwrap();
    let opened = VfsEvent::Opened {
        instance_id: 1,
        created: true,
        size: 0,
        change: 3,
    };
    operation.next(vfs(opened)).unwrap();
    let claimed = HandleAllocation {
        handle_id: 44,
        cookie_id: 45,
        created: true,
    };
    let turn = operation.next(IoResult::Child(ChildOutcome::new(Ok::<
        HandleAllocation,
        VfsError,
    >(claimed))));
    assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
    let turn = operation.next(IoResult::Done);
    let VfsEffect::RebindInstance(request) = asked(&turn) else {
        panic!("expected handle rebind before permit release");
    };
    assert_eq!(request.handle_id, Some(44));
    assert_eq!(request.base_version, None);
    let bound = InstanceSummary {
        handle_id: Some(44),
        change: 4,
        ..instance()
    };
    let Ok(Next::Then { output, calls }) = operation.next(vfs(VfsEvent::Instance(Some(bound))))
    else {
        panic!("expected open completion with permit release");
    };
    assert_eq!(output.handle, Some(claimed));
    assert_eq!(output.change, 4);
    assert_eq!(
        calls,
        vec![vfs_call(VfsEffect::ReleasePermit { permit_id: id(9) })]
    );
}

#[test]
fn initial_create_commits() {
    // A brand-new unpublished file without a stage yet still creates its initial checkpoint.
    let mut record = dirty_stage();
    record.base = StageBase::Unpublished {
        token: id(30),
        predecessor: None,
    };
    let mut operation = VfsCommitOperation::new(CommitInput {
        context: context(),
        instance_id: 1,
        record,
        previous: None,
    });
    operation.first().unwrap();
    operation
        .next(vfs(VfsEvent::GuardAcquired { guard_id: id(10) }))
        .unwrap();
    let clean = InstanceSummary {
        dirty: false,
        stage_id: None,
        logical_size: 0,
        identity: HandleIdentity::File(FileIdentity::Unpublished {
            token: id(30),
            predecessor: None,
        }),
        base_version: None,
        ..instance()
    };
    operation
        .next(vfs(VfsEvent::Instance(Some(clean))))
        .unwrap();
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
}

#[test]
fn commit_follows_instance() {
    // A flush and rename through the other protocol before the guard moved the base and the
    // key; the commit copies from the new exact version and admits the new name.
    let mut operation = committing();
    let moved = InstanceSummary {
        key: "p/b".to_string(),
        base_version: Some(id(9)),
        ..instance()
    };
    let turn = operation.next(vfs(VfsEvent::Instance(Some(moved))));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.input.record.key, "p/b");
    let base = StageBase::Committed {
        version_id: id(9),
        life_id: id(4),
    };
    assert_eq!(operation.input.record.base, base);
    let mut detached = committing();
    let gone = InstanceSummary {
        detached: Some(aruna_core::structs::storage::vfs::DetachReason::Unlinked),
        ..instance()
    };
    let turn = detached.next(vfs(VfsEvent::Instance(Some(gone))));
    assert_eq!(turn, Err(VfsError::StaleHandle));
}

/// A commit whose row write lost its reply, up to the settling sync.
fn uncertain() -> VfsCommitOperation {
    let mut operation = begun();
    let plan = VfsEvent::CheckpointPlan {
        missing_base: Vec::new(),
        logical_size: 5,
    };
    operation.next(vfs(plan)).unwrap();
    let sealed = VfsEvent::CheckpointWritten {
        filename: "c-6".to_string(),
        size: 5,
    };
    operation.next(vfs(sealed)).unwrap();
    let lost = IoResult::Child(ChildOutcome::new(Err::<(), VfsError>(VfsError::Storage(
        "lost reply".to_string(),
    ))));
    assert_eq!(operation.next(lost), Ok(Next::Call(IoCall::Sync)));
    operation
}

#[test]
fn uncertain_file_kept() {
    // A sealed file a row may name stays when the sync or the read back fails; only a row
    // read back without it lets the abort drop the claim.
    let mut settle = uncertain();
    assert!(settle.next(IoResult::Woke).is_err());
    assert_eq!(settle.abort(), vec![release()]);
    let mut check = uncertain();
    assert!(matches!(
        check.next(IoResult::Done),
        Ok(Next::Call(IoCall::Read(_)))
    ));
    assert!(check.next(IoResult::Done).is_err());
    assert_eq!(check.abort(), vec![release()]);
    let mut absent = uncertain();
    absent.next(IoResult::Done).unwrap();
    let turn = absent.next(IoResult::Values(vec![None]));
    assert_eq!(turn, Err(VfsError::Storage("lost reply".to_string())));
    let abort = VfsEffect::AbortCheckpoint {
        scope: scope(),
        instance_id: 1,
        stage_id: id(12),
        generation: 6,
    };
    assert_eq!(absent.abort(), vec![vfs_call(abort), release()]);
}

#[test]
fn second_child_opens() {
    // An implicit directory whose first child is hidden still opens through a visible one.
    let mut operation = VfsOpenOperation::new(OpenInput {
        context: context(),
        key: "d".to_string(),
        kind: HandleKind::Directory,
        create: false,
        exclusive: false,
        write: false,
        share: ShareMode::default(),
        delete_on_close: false,
        open_id: id(20),
        stable: false,
        mode: 0,
        mount_id: Ulid::nil(),
        stage_scope: StageScope::User,
    });
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation
        .next(IoResult::Values(vec![None, None, None]))
        .unwrap();
    let rows = vec![
        (
            super::super::head_key("bucket", "d/a").unwrap(),
            head(6, Some(7)),
        ),
        (
            super::super::head_key("bucket", "d/b").unwrap(),
            head(8, Some(9)),
        ),
    ];
    let turn = operation.next(IoResult::Page { rows, next: None });
    assert!(matches!(asked(&turn), VfsEffect::ListStaged { .. }));
    let staged = VfsEvent::StagedList {
        entries: Vec::new(),
        sequence: 1,
        truncated: false,
    };
    let turn = operation.next(vfs(staged));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(denied());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = operation.next(granted());
    let VfsEffect::OpenInstance(request) = asked(&turn) else {
        panic!("expected the open through the second child");
    };
    let witness = DirectoryWitness::Descendant {
        key: "d/b".to_string(),
        life_id: id(9),
    };
    assert_eq!(request.identity, HandleIdentity::Directory(witness));
}

#[test]
fn staged_read_version() {
    // A staged read passes its lock check on and reports the version its base names.
    let mut operation = VfsReadOperation::new(ReadInput {
        context: context(),
        key: "p/a".to_string(),
        instance_id: Some(1),
        version_id: Some(id(2)),
        offset: 0,
        length: 3,
        lock_check: ReadLockCheck::Owner(5),
    });
    operation.first().unwrap();
    let turn = operation.next(granted());
    let VfsEffect::ReadStage { lock_check, .. } = asked(&turn) else {
        panic!("expected the staged read");
    };
    assert_eq!(lock_check, ReadLockCheck::Owner(5));
    let read = VfsEvent::StageRead {
        slices: vec![StageSlice::Written {
            offset: 0,
            data: vec![1, 2, 3],
        }],
        logical_size: 3,
        base_limit: 3,
    };
    let Ok(Next::Done(output)) = operation.next(vfs(read)) else {
        panic!("expected the read");
    };
    assert_eq!(output.version_id, Some(id(2)));
    assert!(output.eof);
}

#[test]
fn delete_close_owner() {
    // A read-only open with delete-on-close records its disposition owner for final delete.
    let mut operation = VfsOpenOperation::new(OpenInput {
        context: context(),
        key: "p/only".to_string(),
        kind: HandleKind::File,
        create: false,
        exclusive: false,
        write: false,
        share: ShareMode {
            delete_access: true,
            ..ShareMode::default()
        },
        delete_on_close: true,
        open_id: id(22),
        stable: false,
        mode: 0,
        mount_id: Ulid::nil(),
        stage_scope: StageScope::User,
    });
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    operation
        .next(vfs(VfsEvent::PermitAcquired { permit_id: id(9) }))
        .unwrap();
    operation
        .next(IoResult::Values(vec![Some(head(2, Some(4))), None, None]))
        .unwrap();
    operation
        .next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        })
        .unwrap();
    operation
        .next(vfs(VfsEvent::StagedList {
            entries: Vec::new(),
            sequence: 1,
            truncated: false,
        }))
        .unwrap();
    let existing = InstanceSummary {
        key: "p/only".to_string(),
        ..instance()
    };
    let turn = operation.next(vfs(VfsEvent::Instance(Some(existing))));
    let VfsEffect::OpenInstance(request) = asked(&turn) else {
        panic!("expected instance open");
    };
    assert!(request.delete_on_close);
    assert_eq!(
        request.owner.map(|owner| owner.access_key),
        Some("KEY1".to_string())
    );
}
