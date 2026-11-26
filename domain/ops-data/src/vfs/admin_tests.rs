//! Enrolled hosts, mount bindings and the retained stages a bucket admin recovers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::VfsError;
use super::admin::*;
use super::handles_tests::{asked, context, granted, id, user};
use crate::s3::bucket::key::test_rows::authority_rows;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::vfs::{
    EnrolledHostRecord, NfsMountBinding, NfsMountTarget, NfsStageRecord, StageBase,
    StageDisposition, StageOwner, StageScope, VfsKey, VfsRecord,
};
use aruna_core::types::{Key, Value};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::VfsEffect;

fn stranger() -> UserId {
    UserId::new(id(40), RealmId::from_bytes([1; 32]))
}

fn host(owner: UserId) -> EnrolledHostRecord {
    EnrolledHostRecord {
        host_id: id(30),
        owner,
        fingerprints: vec![[1; 32]],
        allowed_networks: vec!["10.0.0.0/8".to_string()],
        worker_pool: false,
        created_at_ms: 1,
        revoked_at_ms: None,
    }
}

fn row<T: VfsRecord>(record: &T) -> Option<Value> {
    Some(Value::from(record.to_bytes().unwrap()))
}

/// One page of host rows; `more` marks a full page with rows after it.
fn hosts(records: Vec<EnrolledHostRecord>, more: bool) -> IoResult {
    let rows: Vec<(Key, Value)> = records
        .iter()
        .map(|host| {
            let key = Key::from(VfsKey::Host(host.host_id).to_bytes().unwrap());
            (key, row(host).unwrap())
        })
        .collect();
    let next = more.then(|| rows.last().map(|row| row.0.clone())).flatten();
    IoResult::Page { rows, next }
}

#[test]
fn network_notation_checked() {
    for good in ["10.0.0.0/8", "192.168.1.7/32", "fd00::/8", "::1/128"] {
        assert!(valid_network(good), "{good}");
    }
    for bad in [
        "10.0.0.0",
        "10.0.0.0/33",
        "fd00::/129",
        "host/8",
        "10.0.0.0/x",
    ] {
        assert!(!valid_network(bad), "{bad}");
    }
}

#[test]
fn enrollment_needs_possession() {
    let input = |proven| EnrollInput {
        owner: user(),
        fingerprints: vec![[1; 32]],
        proven,
        allowed_networks: vec!["10.0.0.0/8".to_string()],
        worker_pool: false,
    };
    let mut refused = EnrollHostOperation::new(input([2; 32]));
    assert_eq!(refused.first(), Err(VfsError::Denied));
    let mut operation = EnrollHostOperation::new(input([1; 32]));
    operation.seed(7);
    operation.stamp(5);
    assert_eq!(operation.transaction(), TransactionMode::Write);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let turn = operation.next(hosts(Vec::new(), false));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    let turn = operation.next(IoResult::Values(vec![None]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
    let Ok(Next::Done(enrolled)) = operation.next(IoResult::Done) else {
        panic!("expected the enrolled host");
    };
    assert_eq!((enrolled.owner, enrolled.created_at_ms), (user(), 5));
}

#[test]
fn host_owner_only() {
    // Only the verified owner rotates or revokes a host, and revocation ends its grants.
    let mut foreign = HostChangeOperation::new(id(30), stranger(), HostChange::Revoke);
    foreign.first().unwrap();
    let turn = foreign.next(IoResult::Values(vec![row(&host(user()))]));
    assert_eq!(turn, Err(VfsError::Denied));

    let mut operation = HostChangeOperation::new(id(30), user(), HostChange::Revoke);
    operation.stamp(9);
    operation.first().unwrap();
    let turn = operation.next(IoResult::Values(vec![row(&host(user()))]));
    let Ok(Next::Call(IoCall::Write(writes))) = turn else {
        panic!("expected the host row");
    };
    let stored = EnrolledHostRecord::from_bytes(&writes[0].2).unwrap();
    assert_eq!(stored.revoked_at_ms, Some(9));
    let Ok(Next::Then { calls, .. }) = operation.next(IoResult::Done) else {
        panic!("expected the revocation of live grants");
    };
    let revoke = VfsEffect::RevokeGrant {
        host_id: Some(id(30)),
        mount_id: None,
        access_key: None,
    };
    assert_eq!(
        asked(&Ok::<_, VfsError>(Next::<()>::Call(
            calls.into_iter().next().unwrap()
        ))),
        revoke
    );

    // A rotation must prove one of its new keys.
    let rotate = HostChange::Rotate {
        fingerprints: vec![[3; 32]],
        proven: [1; 32],
    };
    let mut unproven = HostChangeOperation::new(id(30), user(), rotate);
    assert_eq!(unproven.first(), Err(VfsError::Denied));
}

fn binding(owner: UserId, mount: u8) -> NfsMountBinding {
    NfsMountBinding {
        mount_id: id(mount),
        host_id: id(30),
        owner,
        access_key: "KEY1".to_string(),
        group_id: id(3),
        target: NfsMountTarget::for_folder("bucket", 7, "team").unwrap(),
        read_only: false,
        allowed_networks: Vec::new(),
        display_uid: 1000,
        display_gid: 1000,
        mode: 0o750,
        created_at_ms: 1,
    }
}

#[test]
fn mount_delete_revokes() {
    let mut foreign = MountRowOperation::delete(id(50), stranger());
    foreign.first().unwrap();
    let turn = foreign.next(IoResult::Values(vec![row(&binding(user(), 50))]));
    assert_eq!(turn, Err(VfsError::NotFound));

    let mut operation = MountRowOperation::delete(id(50), user());
    assert_eq!(operation.transaction(), TransactionMode::Write);
    operation.first().unwrap();
    let turn = operation.next(IoResult::Values(vec![row(&binding(user(), 50))]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the revocation of the mount's state");
    };
    assert_eq!(output.mount_id, id(50));
    assert_eq!(calls.len(), 1);
}

#[test]
fn listing_filters_owner() {
    let key = |mount| Key::from([&[2u8][..], &id(mount).to_bytes()].concat());
    let rows = vec![
        (key(50), row(&binding(user(), 50)).unwrap()),
        (key(51), row(&binding(stranger(), 51)).unwrap()),
        (key(52), row(&binding(user(), 52)).unwrap()),
    ];
    let mut operation = ListMountsOperation::new(user(), None, None, 10);
    operation.first().unwrap();
    let Ok(Next::Done(page)) = operation.next(IoResult::Page { rows, next: None }) else {
        panic!("expected one page");
    };
    let mounts: Vec<_> = page.items.iter().map(|mount| mount.mount_id).collect();
    assert_eq!(mounts, vec![id(50), id(52)]);
    assert_eq!(page.next, None);
}

fn retained(generation: u64) -> NfsStageRecord {
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
        checkpoint: "c".to_string(),
        logical_size: 5,
        base: StageBase::Unpublished {
            token: id(6),
            predecessor: None,
        },
        source_copies: Vec::new(),
        source_policies: Vec::new(),
        destination_copies: Vec::new(),
        destination_policies: Vec::new(),
        disposition: StageDisposition::Retained {
            reason: aruna_core::structs::storage::vfs::DetachReason::LostLife,
        },
        quota_retry: None,
        foreign_life: None,
        updated_at_ms: 1,
    }
}

fn admin() -> StageAdmin {
    StageAdmin {
        context: context(),
        bucket: "bucket".to_string(),
    }
}

/// Answers the admission and the authority rows naming `admins`.
fn admitted(operation: &mut DeleteStageOperation, admins: &[UserId]) -> Result<Next<()>, VfsError> {
    operation.first().unwrap();
    operation.next(granted()).unwrap();
    // The bucket's creator is someone else, so only the listed admins count.
    let info = aruna_test_harness::s3::bucket_info(id(3), stranger());
    let values = authority_rows(&info, None, admins);
    operation.next(IoResult::Values(
        values.into_iter().map(|(_, value)| value).collect(),
    ))
}

#[test]
fn stage_admin_fenced() {
    // A caller outside the bucket admins is refused.
    let mut outsider = DeleteStageOperation::new(admin(), id(12), 2);
    assert_eq!(admitted(&mut outsider, &[]), Err(VfsError::Denied));
    // The generation the admin saw is the explicit conflict fence.
    let mut stale = DeleteStageOperation::new(admin(), id(12), 3);
    assert!(matches!(
        admitted(&mut stale, &[user()]),
        Ok(Next::Call(IoCall::Read(_)))
    ));
    assert_eq!(
        stale.next(IoResult::Values(vec![row(&retained(2))])),
        Err(VfsError::Conflict)
    );
    let mut operation = DeleteStageOperation::new(admin(), id(12), 2);
    admitted(&mut operation, &[user()]).unwrap();
    let turn = operation.next(IoResult::Values(vec![row(&retained(2))]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    let Ok(Next::Then { calls, .. }) = operation.next(IoResult::Done) else {
        panic!("expected the file removal after the commit");
    };
    let remove = VfsEffect::RemoveCheckpoint {
        stage_id: id(12),
        generation: 2,
    };
    assert_eq!(
        asked(&Ok::<_, VfsError>(Next::<()>::Call(
            calls.into_iter().next().unwrap()
        ))),
        remove
    );
}

/// A host of `seed` that allows `network`, revoked when `revoked`.
fn local_host(seed: u8, network: &str, revoked: bool) -> (Key, Value) {
    let mut record = host(user());
    record.host_id = id(seed);
    record.allowed_networks = vec!["10.0.0.0/8".to_string(), network.to_string()];
    record.revoked_at_ms = revoked.then_some(5);
    let key = Key::from(vec![aruna_core::structs::storage::vfs::HOST_TAG, seed]);
    (key, row(&record).unwrap())
}

fn loopback_found(peer: &str, rows: Vec<(Key, Value)>) -> Option<EnrolledHostRecord> {
    let mut operation = LoopbackHostOperation::new(peer.parse().unwrap());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    match operation.next(IoResult::Page { rows, next: None }) {
        Ok(Next::Done(found)) => found,
        other => panic!("expected the lookup outcome, got {other:?}"),
    }
}

#[test]
fn loopback_host_unique() {
    // Only an unrevoked host whose networks hold the peer counts; a mapped address matches too.
    let rows = vec![
        local_host(50, "127.0.0.0/8", true),
        local_host(51, "192.168.0.0/16", false),
        local_host(52, "127.0.0.1/32", false),
    ];
    let found = loopback_found("127.0.0.1", rows.clone()).map(|host| host.host_id);
    assert_eq!(found, Some(id(52)));
    let found = loopback_found("::ffff:127.0.0.1", rows.clone()).map(|host| host.host_id);
    assert_eq!(found, Some(id(52)));
    assert_eq!(loopback_found("127.0.0.2", rows), None);
    let rows = vec![local_host(53, "::1/128", false)];
    assert!(loopback_found("::1", rows).is_some());
}

#[test]
fn ambiguous_loopback_refused() {
    // Two hosts that both hold the peer leave the lookup without a host.
    let rows = vec![
        local_host(52, "127.0.0.1/32", false),
        local_host(53, "127.0.0.0/8", false),
    ];
    assert_eq!(loopback_found("127.0.0.1", rows), None);
}

#[test]
fn remote_peer_skipped() {
    // A peer off the loopback range never reads the host rows.
    let mut operation = LoopbackHostOperation::new("10.1.2.3".parse().unwrap());
    assert_eq!(operation.first(), Ok(Next::Done(None)));
}

fn enrolling(fingerprints: Vec<[u8; 32]>) -> EnrollHostOperation {
    EnrollHostOperation::new(EnrollInput {
        owner: user(),
        fingerprints,
        proven: [1; 32],
        allowed_networks: Vec::new(),
        worker_pool: false,
    })
}

#[test]
fn enrollment_one_key() {
    // An enrollment names exactly the key it proved, nothing more.
    for fingerprints in [vec![], vec![[1; 32], [2; 32]], vec![[1; 32], [1; 32]]] {
        let mut operation = enrolling(fingerprints);
        assert_eq!(operation.first(), Err(VfsError::Denied));
    }
}

#[test]
fn enrollment_unique_key() {
    // A key another live host holds is refused on any page; a revoked holder does not count.
    let mut operation = enrolling(vec![[1; 32]]);
    operation.seed(7);
    operation.first().unwrap();
    let mut revoked = host(stranger());
    revoked.revoked_at_ms = Some(3);
    let mut filler: Vec<_> = (0..super::STATE_PAGE)
        .map(|index| {
            let mut other = host(stranger());
            other.host_id = ulid::Ulid::from_parts(u64::try_from(index).unwrap(), 1);
            other.fingerprints = vec![[9; 32]];
            other
        })
        .collect();
    filler[0] = revoked;
    let turn = operation.next(hosts(filler, true));
    assert!(matches!(
        turn,
        Ok(Next::Call(IoCall::Page { after: Some(_), .. }))
    ));
    let turn = operation.next(hosts(vec![host(stranger())], false));
    assert_eq!(turn, Err(VfsError::Exists));
}

#[test]
fn rotation_proves_new() {
    // A rotation keeps old keys and adds only the proven one, which no other live host holds.
    let rotate = |fingerprints| HostChange::Rotate {
        fingerprints,
        proven: [2; 32],
    };
    let mut extra = HostChangeOperation::new(id(30), user(), rotate(vec![[2; 32], [3; 32]]));
    extra.first().unwrap();
    let turn = extra.next(IoResult::Values(vec![row(&host(user()))]));
    assert_eq!(turn, Err(VfsError::Denied));

    let mut taken = HostChangeOperation::new(id(30), user(), rotate(vec![[1; 32], [2; 32]]));
    taken.first().unwrap();
    let turn = taken.next(IoResult::Values(vec![row(&host(user()))]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
    let mut other = host(stranger());
    other.host_id = id(31);
    other.fingerprints = vec![[2; 32]];
    // The host's own row does not count against its kept key.
    let turn = taken.next(hosts(vec![host(user()), other], false));
    assert_eq!(turn, Err(VfsError::Exists));

    let mut operation = HostChangeOperation::new(id(30), user(), rotate(vec![[1; 32], [2; 32]]));
    operation.first().unwrap();
    operation
        .next(IoResult::Values(vec![row(&host(user()))]))
        .unwrap();
    let turn = operation.next(hosts(vec![host(user())], false));
    let Ok(Next::Call(IoCall::Write(writes))) = turn else {
        panic!("expected the host row");
    };
    let stored = EnrolledHostRecord::from_bytes(&writes[0].2).unwrap();
    assert_eq!(stored.fingerprints, vec![[1; 32], [2; 32]]);
}

/// A bucket mount of the test bucket on host 30, run up to its guarded host read.
fn mounting(pool: bool) -> CreateMountOperation {
    let input = MountInput {
        context: context(),
        caller: user(),
        host_id: id(30),
        group_id: id(3),
        bucket: "bucket".to_string(),
        prefix: None,
        job_id: None,
        read_only: false,
        allowed_networks: Vec::new(),
        display_uid: 1000,
        display_gid: 1000,
        mode: 0o750,
    };
    let mut operation = CreateMountOperation::new(input);
    if pool {
        operation = operation.for_worker_pool();
    }
    operation.first().unwrap();
    let turn = operation.next(granted());
    assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
    operation
}

#[test]
fn worker_pool_mounts() {
    // A worker pool mount needs a live pool host, which the caller does not own; a public
    // mount still needs the caller's own host.
    let pool = EnrolledHostRecord {
        worker_pool: true,
        ..host(stranger())
    };
    let turn = mounting(true).next(IoResult::Values(vec![row(&pool)]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Write(_)))));
    let plain = host(user());
    let turn = mounting(true).next(IoResult::Values(vec![row(&plain)]));
    assert_eq!(turn, Err(VfsError::Denied));
    let revoked = EnrolledHostRecord {
        revoked_at_ms: Some(9),
        ..pool.clone()
    };
    let turn = mounting(true).next(IoResult::Values(vec![row(&revoked)]));
    assert_eq!(turn, Err(VfsError::Denied));
    let turn = mounting(true).next(IoResult::Values(vec![None]));
    assert_eq!(turn, Err(VfsError::NotFound));
    let turn = mounting(false).next(IoResult::Values(vec![row(&pool)]));
    assert_eq!(turn, Err(VfsError::Denied));
}

#[test]
fn revoked_key_unmounts() {
    // Every mount of the revoked key goes in one write, then each one's live state is revoked.
    let key = |mount| Key::from([&[2u8][..], &id(mount).to_bytes()].concat());
    let other = NfsMountBinding {
        access_key: "KEY2".to_string(),
        ..binding(user(), 51)
    };
    let rows = vec![
        (key(50), row(&binding(user(), 50)).unwrap()),
        (key(51), row(&other).unwrap()),
        (key(52), row(&binding(stranger(), 52)).unwrap()),
    ];
    let mut operation = RevokeMountsOperation::new("KEY1".to_string());
    assert_eq!(operation.transaction(), TransactionMode::Write);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let turn = operation.next(IoResult::Page { rows, next: None });
    let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
        panic!("expected the mount deletes");
    };
    let deleted: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
    let state = |mount| super::state_key(&VfsKey::Mount(id(mount))).unwrap();
    assert_eq!(deleted, vec![state(50), state(52)]);
    let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
        panic!("expected the count with the revocations");
    };
    assert_eq!((output, calls.len()), (2, 2));
    let mut none = RevokeMountsOperation::new("KEY9".to_string());
    none.first().unwrap();
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(none.next(empty), Ok(Next::Done(0)));
}
