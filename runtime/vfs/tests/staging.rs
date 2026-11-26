//! Sparse stages, checkpoint generations, scratch budgets, cold-start restore and the
//! boot-fenced change counter of the file server adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{
    BucketScope, DetachReason, FileIdentity, HandleIdentity, HeadChangeObserver,
};
use aruna_core::vfs_effects::{
    ByteRange, DiskStage, InstanceSummary, OpenRequest, ReadLockCheck, RebindRequest,
    RestoreRequest, ShareMode, StageSlice, VfsEffect, VfsEvent, VfsFailure, VfsProtocol,
};
use aruna_vfs::{VfsConfig, VfsHandle};
use std::path::Path;
use ulid::Ulid;

const MIB: u64 = 1 << 20;

fn group() -> Ulid {
    Ulid::from_parts(1, 1)
}

fn scope() -> BucketScope {
    BucketScope::new("bucket", 7)
}

fn handle_at(folder: &Path, node_stage_bytes: u64) -> VfsHandle {
    VfsHandle::open(VfsConfig {
        stage_dir: folder.join("stage"),
        node_stage_bytes,
        group_stage_bytes: node_stage_bytes,
    })
    .unwrap()
}

async fn booted(node_stage_bytes: u64) -> (tempfile::TempDir, VfsHandle) {
    let folder = tempfile::tempdir().unwrap();
    let handle = handle_at(folder.path(), node_stage_bytes);
    configure(&handle, 1).await;
    (folder, handle)
}

async fn configure(handle: &VfsHandle, boot_generation: u64) -> VfsEvent {
    let boot = VfsEffect::ConfigureBoot {
        boot_generation,
        verifier: [0, 0, 0, 0, 0, 0, 0, 9],
    };
    handle.execute(boot).await
}

fn unpublished() -> HandleIdentity {
    HandleIdentity::File(FileIdentity::Unpublished {
        token: Ulid::from_parts(4, 4),
        predecessor: None,
    })
}

fn committed() -> HandleIdentity {
    HandleIdentity::File(FileIdentity::Committed {
        life_id: Ulid::from_parts(2, 2),
    })
}

async fn opened(handle: &VfsHandle, key: &str, identity: HandleIdentity, base_size: u64) -> u64 {
    open_request(handle, key, identity, base_size).await.0
}

/// Opens `key` for reading and writing and returns the instance and the open id.
async fn open_request(
    handle: &VfsHandle,
    key: &str,
    identity: HandleIdentity,
    base_size: u64,
) -> (u64, Ulid) {
    let open_id = Ulid::generate();
    let request = OpenRequest {
        scope: scope(),
        key: key.to_string(),
        kind: identity.kind(),
        handle_id: Some(11),
        identity,
        open_id,
        protocol: VfsProtocol::Nfs,
        share: ShareMode {
            read_access: true,
            write_access: true,
            share_read: true,
            share_write: true,
            share_delete: true,
            ..ShareMode::default()
        },
        delete_on_close: false,
        owner: None,
        base_size,
        group_id: group(),
        base_version: None,
    };
    match handle.execute(VfsEffect::OpenInstance(request)).await {
        VfsEvent::Opened { instance_id, .. } => (instance_id, open_id),
        other => panic!("unexpected answer {other:?}"),
    }
}

async fn by_id(handle: &VfsHandle, instance_id: u64) -> Option<InstanceSummary> {
    let lookup = VfsEffect::LookupById {
        scope: scope(),
        instance_id,
    };
    match handle.execute(lookup).await {
        VfsEvent::Instance(summary) => summary,
        other => panic!("unexpected answer {other:?}"),
    }
}

async fn close(handle: &VfsHandle, open_id: Ulid) {
    let close = VfsEffect::CloseOpen {
        scope: scope(),
        open_id,
    };
    let answer = handle.execute(close).await;
    assert!(matches!(answer, VfsEvent::Closed(_)), "{answer:?}");
}

async fn write(handle: &VfsHandle, instance_id: u64, offset: u64, data: &[u8]) -> VfsEvent {
    let effect = VfsEffect::WriteStage {
        scope: scope(),
        instance_id,
        owner_id: 1,
        offset,
        data: data.to_vec(),
        append: false,
        group_id: group(),
    };
    handle.execute(effect).await
}

async fn append(handle: &VfsHandle, instance_id: u64, data: &[u8]) -> VfsEvent {
    let effect = VfsEffect::WriteStage {
        scope: scope(),
        instance_id,
        owner_id: 1,
        offset: 0,
        data: data.to_vec(),
        append: true,
        group_id: group(),
    };
    handle.execute(effect).await
}

async fn truncate(handle: &VfsHandle, instance_id: u64, new_size: u64) -> VfsEvent {
    let effect = VfsEffect::TruncateStage {
        scope: scope(),
        instance_id,
        owner_id: 1,
        new_size,
        group_id: group(),
    };
    handle.execute(effect).await
}

async fn read(handle: &VfsHandle, instance_id: u64, offset: u64, length: u32) -> Vec<StageSlice> {
    let effect = VfsEffect::ReadStage {
        scope: scope(),
        instance_id,
        offset,
        length,
        lock_check: ReadLockCheck::Bypass,
    };
    match handle.execute(effect).await {
        VfsEvent::StageRead { slices, .. } => slices,
        other => panic!("unexpected answer {other:?}"),
    }
}

async fn begin(handle: &VfsHandle, instance_id: u64, stage_id: Ulid, generation: u64) -> VfsEvent {
    let effect = VfsEffect::BeginCheckpoint {
        scope: scope(),
        instance_id,
        stage_id,
        generation,
        group_id: group(),
    };
    handle.execute(effect).await
}

/// Begins, seals and promotes one generation of a stage with no base bytes.
async fn checkpoint(handle: &VfsHandle, instance_id: u64, stage_id: Ulid, generation: u64) {
    let plan = begin(handle, instance_id, stage_id, generation).await;
    assert!(
        matches!(plan, VfsEvent::CheckpointPlan { ref missing_base, .. } if missing_base.is_empty())
    );
    let seal = VfsEffect::SealCheckpoint {
        scope: scope(),
        instance_id,
        stage_id,
        generation,
    };
    let sealed = handle.execute(seal).await;
    assert!(matches!(sealed, VfsEvent::CheckpointWritten { .. }));
    let promote = VfsEffect::PromoteCheckpoint {
        scope: scope(),
        instance_id,
        stage_id,
        generation,
        old_generation: generation.checked_sub(1).filter(|old| *old > 0),
    };
    assert_eq!(handle.execute(promote).await, VfsEvent::Done);
}

async fn read_sealed(handle: &VfsHandle, stage_id: Ulid, generation: u64) -> VfsEvent {
    let effect = VfsEffect::ReadCheckpoint {
        stage_id,
        generation,
        offset: 0,
        length: 64,
    };
    handle.execute(effect).await
}

fn written(offset: u64, data: &[u8]) -> StageSlice {
    StageSlice::Written {
        offset,
        data: data.to_vec(),
    }
}

#[tokio::test]
async fn shrink_extend_zeros() {
    let (_folder, handle) = booted(MIB).await;
    let instance_id = opened(&handle, "a.txt", committed(), 100).await;
    let answer = write(&handle, instance_id, 10, b"abc").await;
    assert!(matches!(
        answer,
        VfsEvent::StageWritten {
            offset: 10,
            written: 3,
            new_size: 100,
            ..
        }
    ));
    truncate(&handle, instance_id, 5).await;
    truncate(&handle, instance_id, 50).await;
    let slices = read(&handle, instance_id, 0, 64).await;
    assert_eq!(
        slices,
        vec![
            StageSlice::Base {
                offset: 0,
                length: 5
            },
            StageSlice::Zero {
                offset: 5,
                length: 45
            },
        ]
    );
    write(&handle, instance_id, 48, b"xy").await;
    let slices = read(&handle, instance_id, 40, 64).await;
    assert_eq!(
        slices,
        vec![
            StageSlice::Zero {
                offset: 40,
                length: 8
            },
            written(48, b"xy"),
        ]
    );
}

#[tokio::test]
async fn append_and_overflow() {
    let (_folder, handle) = booted(MIB).await;
    let instance_id = opened(&handle, "b.txt", unpublished(), 0).await;
    let first = append(&handle, instance_id, b"one").await;
    assert!(matches!(
        first,
        VfsEvent::StageWritten {
            offset: 0,
            new_size: 3,
            ..
        }
    ));
    let second = append(&handle, instance_id, b"two").await;
    assert!(matches!(
        second,
        VfsEvent::StageWritten {
            offset: 3,
            new_size: 6,
            ..
        }
    ));
    assert_eq!(
        read(&handle, instance_id, 0, 64).await,
        vec![written(0, b"onetwo")]
    );
    let wrapped = write(&handle, instance_id, u64::MAX - 1, b"abc").await;
    assert_eq!(wrapped, VfsEvent::Failed(VfsFailure::Overflow));
    let beyond = write(&handle, instance_id, i64::MAX as u64, b"a").await;
    assert_eq!(beyond, VfsEvent::Failed(VfsFailure::Overflow));
    let huge = truncate(&handle, instance_id, u64::MAX).await;
    assert_eq!(huge, VfsEvent::Failed(VfsFailure::Overflow));
}

#[tokio::test]
async fn checkpoint_plan_ranges() {
    let (_folder, handle) = booted(64 * MIB).await;
    let instance_id = opened(&handle, "big.bin", committed(), 20 * MIB).await;
    let data: Vec<u8> = std::iter::repeat_n(7, MIB as usize).collect();
    write(&handle, instance_id, 8 * MIB, &data).await;
    let plan = begin(&handle, instance_id, Ulid::generate(), 1).await;
    assert_eq!(
        plan,
        VfsEvent::CheckpointPlan {
            missing_base: vec![
                ByteRange {
                    offset: 0,
                    length: 8 * MIB
                },
                ByteRange {
                    offset: 9 * MIB,
                    length: 11 * MIB
                },
            ],
            logical_size: 20 * MIB,
        }
    );
}

#[tokio::test]
async fn overwrite_keeps_checkpoint() {
    let (_folder, handle) = booted(MIB).await;
    let stage_id = Ulid::generate();
    let instance_id = opened(&handle, "c.txt", unpublished(), 0).await;
    write(&handle, instance_id, 0, b"hello").await;
    checkpoint(&handle, instance_id, stage_id, 1).await;
    assert_eq!(
        read(&handle, instance_id, 0, 16).await,
        vec![written(0, b"hello")]
    );
    write(&handle, instance_id, 0, b"J").await;
    let first = read_sealed(&handle, stage_id, 1).await;
    assert_eq!(
        first,
        VfsEvent::CheckpointData {
            data: b"hello".to_vec(),
            eof: true
        }
    );
    let slices = read(&handle, instance_id, 0, 16).await;
    assert_eq!(slices, vec![written(0, b"J"), written(1, b"ello")]);
    checkpoint(&handle, instance_id, stage_id, 2).await;
    let gone = read_sealed(&handle, stage_id, 1).await;
    assert_eq!(gone, VfsEvent::Failed(VfsFailure::NotFound));
    let second = read_sealed(&handle, stage_id, 2).await;
    assert_eq!(
        second,
        VfsEvent::CheckpointData {
            data: b"Jello".to_vec(),
            eof: true
        }
    );
    truncate(&handle, instance_id, 2).await;
    truncate(&handle, instance_id, 4).await;
    let slices = read(&handle, instance_id, 0, 16).await;
    assert_eq!(
        slices,
        vec![
            written(0, b"Je"),
            StageSlice::Zero {
                offset: 2,
                length: 2
            }
        ]
    );
}

#[tokio::test]
async fn nospace_keeps_generation() {
    let (_folder, handle) = booted(16).await;
    let stage_id = Ulid::generate();
    let instance_id = opened(&handle, "d.txt", unpublished(), 0).await;
    write(&handle, instance_id, 0, b"abcdef").await;
    checkpoint(&handle, instance_id, stage_id, 1).await;
    let filled = write(&handle, instance_id, 6, b"0123456789").await;
    assert!(matches!(
        filled,
        VfsEvent::StageWritten { new_size: 16, .. }
    ));
    let refused = begin(&handle, instance_id, stage_id, 2).await;
    assert_eq!(refused, VfsEvent::Failed(VfsFailure::NoSpace));
    let refused = write(&handle, instance_id, 16, b"z").await;
    assert_eq!(refused, VfsEvent::Failed(VfsFailure::NoSpace));
    let kept = read_sealed(&handle, stage_id, 1).await;
    assert_eq!(
        kept,
        VfsEvent::CheckpointData {
            data: b"abcdef".to_vec(),
            eof: true
        }
    );
    let slices = read(&handle, instance_id, 0, 32).await;
    assert_eq!(
        slices,
        vec![written(0, b"abcdef"), written(6, b"0123456789")]
    );
}

#[tokio::test]
async fn restore_after_restart() {
    let folder = tempfile::tempdir().unwrap();
    let stage_id = Ulid::generate();
    {
        let handle = handle_at(folder.path(), MIB);
        configure(&handle, 1).await;
        let instance_id = opened(&handle, "e.txt", unpublished(), 0).await;
        write(&handle, instance_id, 0, b"hello").await;
        checkpoint(&handle, instance_id, stage_id, 1).await;
        write(&handle, instance_id, 5, b" lost").await;
    }
    let handle = handle_at(folder.path(), MIB);
    configure(&handle, 2).await;
    let inventory = handle.execute(VfsEffect::ScanStages).await;
    assert_eq!(
        inventory,
        VfsEvent::StageInventory {
            files: vec![DiskStage {
                stage_id,
                generation: 1,
                bytes: 5,
                corrupt: false
            }]
        }
    );
    let fence = handle
        .execute(VfsEffect::FenceVault { scope: scope() })
        .await;
    assert!(matches!(
        fence,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let check = |expected_size| VfsEffect::CheckStage {
        stage_id,
        generation: 1,
        expected_size,
    };
    let valid = handle.execute(check(5)).await;
    assert_eq!(
        valid,
        VfsEvent::StageChecked {
            valid: true,
            size: 5
        }
    );
    let invalid = handle.execute(check(6)).await;
    assert_eq!(
        invalid,
        VfsEvent::StageChecked {
            valid: false,
            size: 5
        }
    );
    let restore = VfsEffect::RestoreStage(RestoreRequest {
        scope: scope(),
        handle_id: 11,
        key: "e.txt".to_string(),
        stage_id,
        generation: 1,
        logical_size: 5,
        group_id: group(),
        identity: unpublished(),
        detached: None,
        owner: None,
        base_version: None,
    });
    let VfsEvent::Opened { instance_id, .. } = handle.execute(restore).await else {
        panic!("the stage was not restored");
    };
    assert_eq!(
        read(&handle, instance_id, 0, 16).await,
        vec![written(0, b"hello")]
    );
}

#[tokio::test]
async fn checkpoint_claims_exclusive() {
    let (_folder, handle) = booted(MIB).await;
    let stage_id = Ulid::generate();
    let first = opened(&handle, "f.txt", unpublished(), 0).await;
    let second = opened(&handle, "g.txt", unpublished(), 0).await;
    write(&handle, first, 0, b"first").await;
    write(&handle, second, 0, b"second").await;
    let plan = begin(&handle, first, stage_id, 1).await;
    assert!(matches!(plan, VfsEvent::CheckpointPlan { .. }));
    let collision = begin(&handle, second, stage_id, 1).await;
    assert_eq!(collision, VfsEvent::Failed(VfsFailure::Conflict));
    let overlapping = begin(&handle, first, stage_id, 2).await;
    assert_eq!(overlapping, VfsEvent::Failed(VfsFailure::Conflict));
    let abort = |instance_id, generation| VfsEffect::AbortCheckpoint {
        scope: scope(),
        instance_id,
        stage_id,
        generation,
    };
    let foreign = handle.execute(abort(second, 1)).await;
    assert_eq!(foreign, VfsEvent::Failed(VfsFailure::NotFound));
    assert_eq!(handle.execute(abort(first, 1)).await, VfsEvent::Done);
    checkpoint(&handle, first, stage_id, 1).await;
    let promoted = handle.execute(abort(first, 1)).await;
    assert_eq!(promoted, VfsEvent::Failed(VfsFailure::NotFound));
    let kept = read_sealed(&handle, stage_id, 1).await;
    assert_eq!(
        kept,
        VfsEvent::CheckpointData {
            data: b"first".to_vec(),
            eof: true
        }
    );
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id,
        generation: 1,
    };
    let in_use = handle.execute(remove).await;
    assert_eq!(in_use, VfsEvent::Failed(VfsFailure::Conflict));
}

#[tokio::test]
async fn rebind_keeps_edits() {
    let (_folder, handle) = booted(MIB).await;
    let instance_id = opened(&handle, "h.txt", committed(), 0).await;
    write(&handle, instance_id, 0, b"abc").await;
    let lookup = VfsEffect::LookupInstance {
        scope: scope(),
        key: "h.txt".to_string(),
    };
    let VfsEvent::Instance(Some(captured)) = handle.execute(lookup).await else {
        panic!("the instance is missing");
    };
    write(&handle, instance_id, 3, b"d").await;
    let version = Ulid::generate();
    let rebind = |expected_edit| {
        VfsEffect::RebindInstance(RebindRequest {
            scope: scope(),
            instance_id,
            key: "h.txt".to_string(),
            handle_id: Some(11),
            identity: committed(),
            base_size: Some(3),
            clear_dirty: true,
            stage_id: None,
            generation: None,
            base_version: Some(version),
            expected_edit: Some(expected_edit),
        })
    };
    let answer = handle.execute(rebind(captured.edit_generation)).await;
    let VfsEvent::Instance(Some(stale)) = answer else {
        panic!("unexpected answer {answer:?}");
    };
    assert!(stale.dirty, "a newer edit must keep the overlay");
    assert_eq!(stale.base_version, Some(version));
    assert_eq!(stale.edit_generation, captured.edit_generation + 1);
    let slices = read(&handle, instance_id, 0, 8).await;
    assert_eq!(slices, vec![written(0, b"abcd")]);
    let answer = handle.execute(rebind(stale.edit_generation)).await;
    let VfsEvent::Instance(Some(current)) = answer else {
        panic!("unexpected answer {answer:?}");
    };
    assert!(!current.dirty);
    assert_eq!(current.edit_generation, stale.edit_generation);
    let base = StageSlice::Base {
        offset: 0,
        length: 3,
    };
    assert_eq!(read(&handle, instance_id, 0, 8).await, vec![base]);
}

#[tokio::test]
async fn sealed_edit_stays() {
    let (_folder, handle) = booted(MIB).await;
    let stage_id = Ulid::generate();
    let instance_id = opened(&handle, "e.txt", unpublished(), 0).await;
    write(&handle, instance_id, 0, b"abc").await;
    let before = by_id(&handle, instance_id).await.unwrap();
    assert_eq!(before.sealed_edit, None);
    checkpoint(&handle, instance_id, stage_id, 1).await;
    let sealed = by_id(&handle, instance_id).await.unwrap();
    assert_eq!(sealed.sealed_edit, Some(sealed.edit_generation));
    write(&handle, instance_id, 3, b"d").await;
    let later = by_id(&handle, instance_id).await.unwrap();
    assert_eq!(later.edit_generation, sealed.edit_generation + 1);
    assert_eq!(later.sealed_edit, sealed.sealed_edit);
}

/// Opens a staged file, checkpoints it once and returns the instance and its open id.
async fn retained_stage(handle: &VfsHandle, stage_id: Ulid) -> (u64, Ulid) {
    let (instance_id, open_id) = open_request(handle, "k.txt", unpublished(), 0).await;
    write(handle, instance_id, 0, b"hello").await;
    checkpoint(handle, instance_id, stage_id, 1).await;
    (instance_id, open_id)
}

fn retain(instance_id: u64, stage_id: Ulid) -> VfsEffect {
    VfsEffect::RetainInstance {
        scope: scope(),
        instance_id,
        stage_id,
        generation: 1,
        reason: DetachReason::Unlinked,
    }
}

fn lookup_key() -> VfsEffect {
    VfsEffect::LookupInstance {
        scope: scope(),
        key: "k.txt".to_string(),
    }
}

#[tokio::test]
async fn retain_reclaims_idle() {
    let (_folder, handle) = booted(MIB).await;
    let stage_id = Ulid::generate();
    let (instance_id, open_id) = retained_stage(&handle, stage_id).await;
    close(&handle, open_id).await;
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id,
        generation: 1,
    };
    let held = handle.execute(remove.clone()).await;
    assert_eq!(held, VfsEvent::Failed(VfsFailure::Conflict));
    let other = Ulid::generate();
    assert_eq!(
        handle.execute(retain(instance_id, other)).await,
        VfsEvent::Done
    );
    assert!(
        by_id(&handle, instance_id).await.is_some(),
        "a stale retain detached it"
    );
    assert_eq!(
        handle.execute(retain(instance_id, stage_id)).await,
        VfsEvent::Done
    );
    assert_eq!(handle.execute(lookup_key()).await, VfsEvent::Instance(None));
    assert_eq!(by_id(&handle, instance_id).await, None);
    assert_eq!(handle.execute(remove).await, VfsEvent::Done);
}

#[tokio::test]
async fn retain_waits_reader() {
    let (_folder, handle) = booted(MIB).await;
    let stage_id = Ulid::generate();
    let (instance_id, writer) = retained_stage(&handle, stage_id).await;
    let (joined, holder) = open_request(&handle, "k.txt", unpublished(), 0).await;
    assert_eq!(joined, instance_id);
    close(&handle, writer).await;
    assert_eq!(
        handle.execute(retain(instance_id, stage_id)).await,
        VfsEvent::Done
    );
    assert_eq!(handle.execute(lookup_key()).await, VfsEvent::Instance(None));
    let summary = by_id(&handle, instance_id).await.unwrap();
    assert_eq!(summary.detached, Some(DetachReason::Unlinked));
    assert_eq!(
        read(&handle, instance_id, 0, 16).await,
        vec![written(0, b"hello")]
    );
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id,
        generation: 1,
    };
    let held = handle.execute(remove.clone()).await;
    assert_eq!(held, VfsEvent::Failed(VfsFailure::Conflict));
    close(&handle, holder).await;
    assert_eq!(by_id(&handle, instance_id).await, None);
    assert_eq!(handle.execute(remove).await, VfsEvent::Done);
}

#[tokio::test]
async fn detached_restore_collects() {
    let folder = tempfile::tempdir().unwrap();
    let stage_id = Ulid::generate();
    {
        let handle = handle_at(folder.path(), MIB);
        configure(&handle, 1).await;
        let instance_id = opened(&handle, "r.txt", unpublished(), 0).await;
        write(&handle, instance_id, 0, b"kept!").await;
        checkpoint(&handle, instance_id, stage_id, 1).await;
    }
    let handle = handle_at(folder.path(), 8);
    configure(&handle, 2).await;
    let restore = VfsEffect::RestoreStage(RestoreRequest {
        scope: scope(),
        handle_id: 11,
        key: "r.txt".to_string(),
        stage_id,
        generation: 1,
        logical_size: 5,
        group_id: group(),
        identity: unpublished(),
        detached: Some(DetachReason::Unlinked),
        owner: None,
        base_version: None,
    });
    assert_eq!(handle.execute(restore).await, VfsEvent::Done);
    let lookup = VfsEffect::LookupHandle {
        scope: scope(),
        handle_id: 11,
    };
    assert_eq!(handle.execute(lookup).await, VfsEvent::Instance(None));
    let other = opened(&handle, "s.txt", unpublished(), 0).await;
    let charged = write(&handle, other, 0, b"fives").await;
    assert_eq!(charged, VfsEvent::Failed(VfsFailure::NoSpace));
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id,
        generation: 1,
    };
    assert_eq!(handle.execute(remove).await, VfsEvent::Done);
    let inventory = handle.execute(VfsEffect::ScanStages).await;
    assert_eq!(inventory, VfsEvent::StageInventory { files: Vec::new() });
    let refunded = write(&handle, other, 0, b"fives").await;
    assert!(matches!(refunded, VfsEvent::StageWritten { .. }));
}

#[tokio::test]
async fn staged_list_pages() {
    let (_folder, handle) = booted(MIB).await;
    for key in ["d/a", "d/b", "d/c", "e/x"] {
        opened(&handle, key, unpublished(), 0).await;
    }
    opened(&handle, "d/z", committed(), 4).await;
    let list = |after: Option<&str>, limit| VfsEffect::ListStaged {
        scope: scope(),
        prefix: "d/".to_string(),
        after: after.map(String::from),
        limit: Some(limit),
    };
    let page = |event: VfsEvent| match event {
        VfsEvent::StagedList {
            entries, truncated, ..
        } => {
            let keys: Vec<String> = entries.into_iter().map(|entry| entry.key).collect();
            (keys, truncated)
        }
        other => panic!("unexpected answer {other:?}"),
    };
    let first = page(handle.execute(list(None, 2)).await);
    assert_eq!(first, (vec!["d/a".to_string(), "d/b".to_string()], true));
    let last = page(handle.execute(list(Some("d/b"), 2)).await);
    assert_eq!(last, (vec!["d/c".to_string()], false));
    let before = page(handle.execute(list(Some("a"), 1)).await);
    assert_eq!(before, (vec!["d/a".to_string()], true));
}

#[tokio::test]
async fn nested_prefix_invalidation() {
    let (_folder, handle) = booted(MIB).await;
    let keys: Vec<String> = ["", "a/", "a/b/", "a/b/c", "x/"].map(String::from).to_vec();
    let sample = || VfsEffect::ReadChange {
        scope: scope(),
        keys: keys.clone(),
    };
    let VfsEvent::ChangeSnapshot { sequence, values } = handle.execute(sample()).await else {
        panic!("no change snapshot");
    };
    assert_eq!(sequence, 0);
    handle.on_head_changed("bucket", "a/b/c");
    let VfsEvent::ChangeSnapshot {
        sequence: after,
        values: changed,
    } = handle.execute(sample()).await
    else {
        panic!("no change snapshot");
    };
    assert_eq!(after, 1);
    assert!((0..4).all(|index| changed[index] > values[index]));
    assert_eq!(changed[4], values[4]);
    let again = handle.execute(sample()).await;
    assert_eq!(
        again,
        VfsEvent::ChangeSnapshot {
            sequence: 1,
            values: changed
        }
    );
}

#[tokio::test]
async fn evicted_key_refreshes() {
    let (_folder, handle) = booted(MIB).await;
    let sample = || VfsEffect::ReadChange {
        scope: scope(),
        keys: vec!["k".to_string()],
    };
    let VfsEvent::ChangeSnapshot { values: before, .. } = handle.execute(sample()).await else {
        panic!("no change snapshot");
    };
    for index in 0..=65_536 {
        handle.on_head_changed("bucket", &format!("n{index}"));
    }
    let VfsEvent::ChangeSnapshot { values: after, .. } = handle.execute(sample()).await else {
        panic!("no change snapshot");
    };
    assert!(after[0] > before[0]);
    let VfsEvent::ChangeSnapshot { values: kept, .. } = handle.execute(sample()).await else {
        panic!("no change snapshot");
    };
    assert_eq!(kept, after);
}

#[tokio::test]
async fn boot_counter_monotonic() {
    let folder = tempfile::tempdir().unwrap();
    let handle = handle_at(folder.path(), MIB);
    let next = || VfsEffect::NextChange {
        scope: scope(),
        key: Some("a".to_string()),
    };
    let unset = handle.execute(next()).await;
    assert_eq!(unset, VfsEvent::Failed(VfsFailure::Unconfigured));
    let value = |event: VfsEvent| match event {
        VfsEvent::ChangeValue { value } => value,
        other => panic!("unexpected answer {other:?}"),
    };
    assert_eq!(configure(&handle, 1).await, VfsEvent::Done);
    let first = value(handle.execute(next()).await);
    assert_eq!(first >> 40, 1);
    let verifier = handle.execute(VfsEffect::WriteVerifier).await;
    let advanced = handle.execute(VfsEffect::AdvanceVerifier).await;
    assert_ne!(verifier, advanced);
    assert_eq!(configure(&handle, 2).await, VfsEvent::Done);
    let second = value(handle.execute(next()).await);
    assert!(second > first);
    assert_eq!(second >> 40, 2);
    let older = configure(&handle, 1).await;
    assert_eq!(older, VfsEvent::Failed(VfsFailure::CounterExhausted));
    let advanced = handle.execute(VfsEffect::AdvanceVerifier).await;
    assert_eq!(configure(&handle, 2).await, VfsEvent::Done);
    assert_eq!(handle.execute(VfsEffect::WriteVerifier).await, advanced);
    assert!(value(handle.execute(next()).await) > second);
    let exhausted = configure(&handle, (1 << 24) - 1).await;
    assert_eq!(exhausted, VfsEvent::Failed(VfsFailure::CounterExhausted));
}
