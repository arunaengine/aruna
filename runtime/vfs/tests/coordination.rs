//! Permits, file guards, pending attempts, share modes, delete-pending, byte-range locks,
//! the vault fence and revocations of the file server adapter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::{
    BucketScope, DirectoryWitness, FileIdentity, HandleIdentity, HandleKind,
};
use aruna_core::vfs_effects::{
    ByteLock, OpenRequest, ReadLockCheck, ShareMode, VfsEffect, VfsEvent, VfsFailure, VfsProtocol,
};
use aruna_vfs::{VfsConfig, VfsHandle, open_owner};
use std::sync::{Arc, Mutex};
use ulid::Ulid;

fn group() -> Ulid {
    Ulid::from_parts(1, 1)
}

async fn booted() -> (tempfile::TempDir, VfsHandle) {
    let folder = tempfile::tempdir().unwrap();
    let handle = VfsHandle::open(VfsConfig {
        stage_dir: folder.path().join("stage"),
        node_stage_bytes: 1 << 20,
        group_stage_bytes: 1 << 20,
    })
    .unwrap();
    let boot = VfsEffect::ConfigureBoot {
        boot_generation: 1,
        verifier: [0; 8],
    };
    assert_eq!(handle.execute(boot).await, VfsEvent::Done);
    (folder, handle)
}

fn scope() -> BucketScope {
    BucketScope::new("bucket", 7)
}

fn committed() -> HandleIdentity {
    HandleIdentity::File(FileIdentity::Committed {
        life_id: Ulid::from_parts(2, 2),
    })
}

fn request(key: &str, identity: HandleIdentity, share: ShareMode) -> OpenRequest {
    OpenRequest {
        scope: scope(),
        key: key.to_string(),
        kind: identity.kind(),
        handle_id: None,
        identity,
        open_id: Ulid::generate(),
        protocol: VfsProtocol::Smb,
        share,
        delete_on_close: false,
        owner: None,
        base_size: 0,
        group_id: group(),
        base_version: None,
    }
}

fn reader() -> ShareMode {
    ShareMode {
        read_access: true,
        share_read: true,
        share_write: true,
        share_delete: true,
        ..ShareMode::default()
    }
}

async fn opened(handle: &VfsHandle, request: OpenRequest) -> u64 {
    match handle.execute(VfsEffect::OpenInstance(request)).await {
        VfsEvent::Opened { instance_id, .. } => instance_id,
        other => panic!("unexpected answer {other:?}"),
    }
}

fn lock(owner: u64, offset: u64, length: u64, protocol: VfsProtocol) -> ByteLock {
    ByteLock {
        owner,
        offset,
        length,
        exclusive: true,
        protocol,
    }
}

async fn guard(handle: &VfsHandle, instances: Vec<u64>) -> VfsEvent {
    let effect = VfsEffect::AcquireGuards {
        scope: scope(),
        instances,
    };
    handle.execute(effect).await
}

async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn permits_stay_fifo() {
    let (_folder, handle) = booted().await;
    let first = handle
        .execute(VfsEffect::AcquirePermit { scope: scope() })
        .await;
    let VfsEvent::PermitAcquired { permit_id } = first else {
        panic!("unexpected answer {first:?}");
    };
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut waiters = Vec::new();
    for position in 0..3 {
        let (handle, order) = (handle.clone(), Arc::clone(&order));
        waiters.push(tokio::spawn(async move {
            let answer = handle
                .execute(VfsEffect::AcquirePermit { scope: scope() })
                .await;
            let VfsEvent::PermitAcquired { permit_id } = answer else {
                panic!("unexpected answer {answer:?}");
            };
            order.lock().unwrap().push(position);
            handle.execute(VfsEffect::ReleasePermit { permit_id }).await
        }));
        settle().await;
    }
    assert!(order.lock().unwrap().is_empty());
    let released = handle.execute(VfsEffect::ReleasePermit { permit_id }).await;
    assert_eq!(released, VfsEvent::Done);
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap(), VfsEvent::Done);
    }
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
}

#[tokio::test]
async fn guards_order_ascending() {
    let (_folder, handle) = booted().await;
    let held = guard(&handle, vec![2, 1, 2]).await;
    let VfsEvent::GuardAcquired { guard_id } = held else {
        panic!("unexpected answer {held:?}");
    };
    let mut callers = Vec::new();
    for instances in [vec![1, 2], vec![2, 1]] {
        let handle = handle.clone();
        callers.push(tokio::spawn(async move {
            let answer = guard(&handle, instances).await;
            let VfsEvent::GuardAcquired { guard_id } = answer else {
                panic!("unexpected answer {answer:?}");
            };
            handle.execute(VfsEffect::ReleaseGuard { guard_id }).await
        }));
    }
    settle().await;
    assert!(callers.iter().all(|caller| !caller.is_finished()));
    handle.release_hold(&aruna_vfs::VfsHold::Guard(guard_id));
    for caller in callers {
        assert_eq!(caller.await.unwrap(), VfsEvent::Done);
    }
    let missing = handle.execute(VfsEffect::ReleaseGuard { guard_id }).await;
    assert_eq!(missing, VfsEvent::Failed(VfsFailure::NotFound));
}

#[tokio::test]
async fn pending_mutation_handoff() {
    let (_folder, handle) = booted().await;
    let attempt_id = Ulid::generate();
    let begin = VfsEffect::BeginAttempt {
        scope: scope(),
        attempt_id,
        instances: vec![7],
    };
    assert_eq!(handle.execute(begin).await, VfsEvent::Done);
    let blocked = guard(&handle, vec![8, 7]).await;
    assert_eq!(blocked, VfsEvent::PendingMutation { attempt_id });
    let active = handle
        .execute(VfsEffect::ActiveAttempts { scope: scope() })
        .await;
    assert_eq!(
        active,
        VfsEvent::Attempts {
            ids: vec![attempt_id]
        }
    );
    let waiter = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.execute(VfsEffect::WaitAttempt { attempt_id }).await })
    };
    settle().await;
    assert!(!waiter.is_finished());
    let finish = VfsEffect::FinishAttempt { attempt_id };
    assert_eq!(handle.execute(finish).await, VfsEvent::Done);
    assert_eq!(waiter.await.unwrap(), VfsEvent::Done);
    let granted = guard(&handle, vec![8, 7]).await;
    assert!(matches!(granted, VfsEvent::GuardAcquired { .. }));
}

#[tokio::test]
async fn share_conflict_symmetric() {
    let (_folder, handle) = booted().await;
    let writer = ShareMode {
        read_access: true,
        write_access: true,
        share_read: true,
        ..ShareMode::default()
    };
    opened(&handle, request("a.txt", committed(), writer)).await;
    let mut nfs = request("a.txt", committed(), reader());
    nfs.protocol = VfsProtocol::Nfs;
    opened(&handle, nfs).await;
    let second_writer = ShareMode {
        write_access: true,
        ..reader()
    };
    let refused = request("a.txt", committed(), second_writer);
    let answer = handle.execute(VfsEffect::OpenInstance(refused)).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::ShareViolation));
    let denies_writers = ShareMode {
        share_write: false,
        ..reader()
    };
    let refused = request("a.txt", committed(), denies_writers);
    let answer = handle.execute(VfsEffect::OpenInstance(refused)).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::ShareViolation));
}

#[tokio::test]
async fn delete_pending_blocks() {
    let (_folder, handle) = booted().await;
    let marker = HandleIdentity::Directory(DirectoryWitness::Marker {
        life_id: Ulid::from_parts(3, 3),
    });
    let folder = request("d/", marker, reader());
    let open_id = folder.open_id;
    let instance_id = opened(&handle, folder).await;
    let pending = |value| VfsEffect::SetDisposition {
        scope: scope(),
        instance_id,
        open_id,
        pending: value,
        owner: None,
    };
    let answer = handle.execute(pending(true)).await;
    let VfsEvent::Instance(Some(summary)) = answer else {
        panic!("unexpected answer {answer:?}");
    };
    assert!(summary.delete_pending);
    assert_eq!(summary.kind, HandleKind::Directory);
    for key in ["d/", "d/x", "d/e/f"] {
        let answer = handle.execute(VfsEffect::OpenInstance(request(key, committed(), reader())));
        assert_eq!(answer.await, VfsEvent::Failed(VfsFailure::DeletePending));
    }
    handle.execute(pending(false)).await;
    opened(&handle, request("d/x", committed(), reader())).await;
}

#[tokio::test]
async fn clear_disposition_unblocks() {
    let (_folder, handle) = booted().await;
    let first = request("a.txt", committed(), reader());
    let open_id = first.open_id;
    let instance_id = opened(&handle, first).await;
    let pending = VfsEffect::SetDisposition {
        scope: scope(),
        instance_id,
        open_id,
        pending: true,
        owner: None,
    };
    assert!(matches!(
        handle.execute(pending).await,
        VfsEvent::Instance(Some(_))
    ));
    let close = VfsEffect::CloseOpen {
        scope: scope(),
        open_id,
        detach_unstable: false,
    };
    let answer = handle.execute(close).await;
    let VfsEvent::Closed(summary) = answer else {
        panic!("unexpected answer {answer:?}");
    };
    assert!(summary.delete_pending && summary.remaining_opens == 0);
    let retry = VfsEffect::OpenInstance(request("a.txt", committed(), reader()));
    let blocked = handle.execute(retry).await;
    assert_eq!(blocked, VfsEvent::Failed(VfsFailure::DeletePending));
    let clear = VfsEffect::ClearDisposition {
        scope: scope(),
        instance_id,
    };
    assert_eq!(handle.execute(clear).await, VfsEvent::Done);
    let lookup = VfsEffect::LookupById {
        scope: scope(),
        instance_id,
    };
    assert_eq!(handle.execute(lookup).await, VfsEvent::Instance(None));
    let reopened = opened(&handle, request("a.txt", committed(), reader())).await;
    assert_ne!(reopened, instance_id);
}

#[tokio::test]
async fn atomic_locks_rollback() {
    let (_folder, handle) = booted().await;
    let instance_id = opened(&handle, request("a.txt", committed(), reader())).await;
    let ranges = |locks, atomic| VfsEffect::LockRanges {
        scope: scope(),
        instance_id,
        locks,
        atomic,
    };
    let test = |probe| VfsEffect::TestLock {
        scope: scope(),
        instance_id,
        lock: probe,
    };
    let held = lock(1, 0, 10, VfsProtocol::Smb);
    assert_eq!(
        handle.execute(ranges(vec![held], true)).await,
        VfsEvent::Done
    );
    let request = vec![
        lock(2, 20, 10, VfsProtocol::Smb),
        lock(2, 5, 10, VfsProtocol::Smb),
    ];
    let answer = handle.execute(ranges(request.clone(), true)).await;
    assert_eq!(answer, VfsEvent::LockConflict { holder: held });
    let probe = lock(3, 20, 1, VfsProtocol::Smb);
    assert_eq!(handle.execute(test(probe)).await, VfsEvent::LockFree);
    handle.execute(ranges(request, false)).await;
    let answer = handle.execute(test(probe)).await;
    assert!(matches!(answer, VfsEvent::LockConflict { .. }));
    let wrapped = lock(4, u64::MAX - 1, 10, VfsProtocol::Smb);
    let answer = handle.execute(ranges(vec![wrapped], true)).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::Overflow));
    let to_end = lock(5, 100, 0, VfsProtocol::Nfs);
    assert_eq!(
        handle.execute(ranges(vec![to_end], true)).await,
        VfsEvent::Done
    );
    let far = lock(6, 1 << 40, 1, VfsProtocol::Nfs);
    let answer = handle.execute(test(far)).await;
    assert_eq!(answer, VfsEvent::LockConflict { holder: to_end });
}

#[tokio::test]
async fn close_releases_own() {
    let (_folder, handle) = booted().await;
    let first = request("a.txt", committed(), reader());
    let (first_open, first_owner) = (first.open_id, open_owner(&first.open_id));
    let instance_id = opened(&handle, first).await;
    opened(&handle, request("a.txt", committed(), reader())).await;
    let own = lock(first_owner, 0, 10, VfsProtocol::Smb);
    let other = lock(9, 50, 10, VfsProtocol::Smb);
    let take = VfsEffect::LockRanges {
        scope: scope(),
        instance_id,
        locks: vec![own, other],
        atomic: true,
    };
    assert_eq!(handle.execute(take).await, VfsEvent::Done);
    let close = VfsEffect::CloseOpen {
        scope: scope(),
        open_id: first_open,
        detach_unstable: false,
    };
    let answer = handle.execute(close).await;
    let VfsEvent::Closed(summary) = answer else {
        panic!("unexpected answer {answer:?}");
    };
    assert_eq!(summary.remaining_opens, 1);
    let probe = |offset| VfsEffect::TestLock {
        scope: scope(),
        instance_id,
        lock: lock(8, offset, 1, VfsProtocol::Smb),
    };
    assert_eq!(handle.execute(probe(0)).await, VfsEvent::LockFree);
    assert_eq!(
        handle.execute(probe(50)).await,
        VfsEvent::LockConflict { holder: other }
    );
}

#[tokio::test]
async fn fence_excludes_stages() {
    let (_folder, handle) = booted().await;
    let instance_id = opened(&handle, request("a.txt", committed(), reader())).await;
    let write = || VfsEffect::WriteStage {
        scope: scope(),
        instance_id,
        owner_id: 1,
        offset: 0,
        data: b"plain".to_vec(),
        append: false,
        group_id: group(),
    };
    let fence = || VfsEffect::FenceVault { scope: scope() };
    let answer = handle.execute(fence()).await;
    let VfsEvent::VaultFenced { fence_id } = answer else {
        panic!("unexpected answer {answer:?}");
    };
    let refused = handle.execute(write()).await;
    assert!(matches!(
        refused,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let release = VfsEffect::ReleaseFence {
        fence_id,
        keep_closed: false,
    };
    assert_eq!(handle.execute(release).await, VfsEvent::Done);
    assert!(matches!(
        handle.execute(write()).await,
        VfsEvent::StageWritten { .. }
    ));
    let refused = handle.execute(fence()).await;
    assert!(matches!(
        refused,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let other = BucketScope::new("other", 1);
    let answer = handle.execute(VfsEffect::FenceVault { scope: other }).await;
    assert!(matches!(answer, VfsEvent::VaultFenced { .. }));
}

#[tokio::test]
async fn fence_waits_guards() {
    let (_folder, handle) = booted().await;
    let instance_id = opened(&handle, request("a.txt", committed(), reader())).await;
    let held = guard(&handle, vec![instance_id]).await;
    let VfsEvent::GuardAcquired { guard_id } = held else {
        panic!("unexpected answer {held:?}");
    };
    let fence = || VfsEffect::FenceVault { scope: scope() };
    let refused = handle.execute(fence()).await;
    assert!(matches!(
        refused,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let release = VfsEffect::ReleaseGuard { guard_id };
    assert_eq!(handle.execute(release).await, VfsEvent::Done);
    let answer = handle.execute(fence()).await;
    assert!(matches!(answer, VfsEvent::VaultFenced { .. }));
}

#[tokio::test]
async fn reads_respect_locks() {
    let (_folder, handle) = booted().await;
    let mut open = request("a.txt", committed(), reader());
    open.base_size = 32;
    let instance_id = opened(&handle, open).await;
    let shared = ByteLock {
        exclusive: false,
        ..lock(2, 20, 10, VfsProtocol::Smb)
    };
    let take = VfsEffect::LockRanges {
        scope: scope(),
        instance_id,
        locks: vec![lock(1, 0, 10, VfsProtocol::Smb), shared],
        atomic: true,
    };
    assert_eq!(handle.execute(take).await, VfsEvent::Done);
    let read = |lock_check, offset| VfsEffect::ReadStage {
        scope: scope(),
        instance_id,
        offset,
        length: 5,
        lock_check,
    };
    for lock_check in [ReadLockCheck::Owner(9), ReadLockCheck::Anonymous] {
        let blocked = handle.execute(read(lock_check, 0)).await;
        assert_eq!(blocked, VfsEvent::Failed(VfsFailure::LockConflict));
    }
    let admitted = [
        (ReadLockCheck::Owner(1), 0),
        (ReadLockCheck::Owner(9), 20),
        (ReadLockCheck::Anonymous, 20),
        (ReadLockCheck::Bypass, 0),
    ];
    for (lock_check, offset) in admitted {
        let answer = handle.execute(read(lock_check, offset)).await;
        assert!(
            matches!(answer, VfsEvent::StageRead { .. }),
            "read {lock_check:?} at {offset} was refused"
        );
    }
}

#[tokio::test]
async fn share_downgrade_admits() {
    let (_folder, handle) = booted().await;
    let writer = ShareMode {
        write_access: true,
        share_write: false,
        ..reader()
    };
    let first = request("a.txt", committed(), writer);
    let open_id = first.open_id;
    opened(&handle, first).await;
    let denies_writers = ShareMode {
        share_write: false,
        ..reader()
    };
    let second = || request("a.txt", committed(), denies_writers);
    let answer = handle.execute(VfsEffect::OpenInstance(second())).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::ShareViolation));
    let downgrade = VfsEffect::UpdateShare {
        scope: scope(),
        open_id,
        share: denies_writers,
    };
    assert_eq!(handle.execute(downgrade).await, VfsEvent::Done);
    opened(&handle, second()).await;
    let upgrade = VfsEffect::UpdateShare {
        scope: scope(),
        open_id,
        share: writer,
    };
    let answer = handle.execute(upgrade).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::ShareViolation));
}

#[tokio::test]
async fn guard_refuses_fenced() {
    let (_folder, handle) = booted().await;
    let instance_id = opened(&handle, request("a.txt", committed(), reader())).await;
    let held = guard(&handle, vec![instance_id]).await;
    let VfsEvent::GuardAcquired { guard_id } = held else {
        panic!("unexpected answer {held:?}");
    };
    let waiter = tokio::spawn({
        let handle = handle.clone();
        async move { guard(&handle, vec![instance_id]).await }
    });
    settle().await;
    let release = VfsEffect::ReleaseGuard { guard_id };
    assert_eq!(handle.execute(release).await, VfsEvent::Done);
    // The fence closes before the waiter runs again, so its granted guards must give way.
    let fence = handle
        .execute(VfsEffect::FenceVault { scope: scope() })
        .await;
    assert!(matches!(fence, VfsEvent::VaultFenced { .. }));
    let late = waiter.await.unwrap();
    assert!(matches!(late, VfsEvent::Failed(VfsFailure::VaultFenced(_))));
    let refused = guard(&handle, vec![instance_id]).await;
    assert!(matches!(
        refused,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
}

/// A named pipe as the volatile file holds the write in its file step until a reader opens it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn admission_survives_cancel() {
    use rustix::fs::{CWD, FileType, Mode};
    let (folder, handle) = booted().await;
    let instance_id = opened(&handle, request("a.txt", committed(), reader())).await;
    let volatile = folder
        .path()
        .join("stage")
        .join(format!("vol-{instance_id}.tmp"));
    let mode = Mode::RUSR | Mode::WUSR;
    rustix::fs::mknodat(CWD, volatile.as_path(), FileType::Fifo, mode, 0).unwrap();
    let mut write = Box::pin(handle.execute(VfsEffect::WriteStage {
        scope: scope(),
        instance_id,
        owner_id: 1,
        offset: 0,
        data: b"plain".to_vec(),
        append: false,
        group_id: group(),
    }));
    std::future::poll_fn(|context| {
        assert!(write.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(write);
    let refused = handle
        .execute(VfsEffect::FenceVault { scope: scope() })
        .await;
    let reason = "a stage write is in flight".to_string();
    assert_eq!(refused, VfsEvent::Failed(VfsFailure::VaultFenced(reason)));
    let drained = tokio::task::spawn_blocking(move || std::fs::read(&volatile));
    drained.await.unwrap().unwrap();
}

#[tokio::test]
async fn fence_kept_closed() {
    let (_folder, handle) = booted().await;
    let answer = handle
        .execute(VfsEffect::FenceVault { scope: scope() })
        .await;
    let VfsEvent::VaultFenced { fence_id } = answer else {
        panic!("unexpected answer {answer:?}");
    };
    let keep = VfsEffect::ReleaseFence {
        fence_id,
        keep_closed: true,
    };
    assert_eq!(handle.execute(keep.clone()).await, VfsEvent::Done);
    let mut writer = request("new.txt", committed(), reader());
    writer.share.write_access = true;
    let answer = handle.execute(VfsEffect::OpenInstance(writer)).await;
    assert!(matches!(
        answer,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let again = handle
        .execute(VfsEffect::FenceVault { scope: scope() })
        .await;
    assert!(matches!(
        again,
        VfsEvent::Failed(VfsFailure::VaultFenced(_))
    ));
    let answer = handle.execute(keep).await;
    assert_eq!(answer, VfsEvent::Failed(VfsFailure::NotFound));
}

#[tokio::test]
async fn revocation_reaches_subscribers() {
    let (_folder, handle) = booted().await;
    let mut revocations = handle.subscribe_revocations();
    let mount_id = Ulid::generate();
    let revoke = VfsEffect::RevokeGrant {
        host_id: None,
        mount_id: Some(mount_id),
        access_key: None,
    };
    assert_eq!(handle.execute(revoke).await, VfsEvent::Done);
    let revocation = revocations.recv().await.unwrap();
    assert_eq!(revocation.mount_id, Some(mount_id));
}

#[tokio::test]
async fn missing_adapter_parts() {
    let (_folder, handle) = booted().await;
    let unknown = handle.execute(VfsEffect::ReleasePermit {
        permit_id: Ulid::generate(),
    });
    assert_eq!(unknown.await, VfsEvent::Failed(VfsFailure::NotFound));
    let lookup = VfsEffect::LookupInstance {
        scope: scope(),
        key: "absent".to_string(),
    };
    assert_eq!(handle.execute(lookup).await, VfsEvent::Instance(None));
}
