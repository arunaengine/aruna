//! Tests of the Aruna NFS backend: error mapping, host networks, keys and revocation, folder
//! confinement, refused writes on job views and the replay recheck against live state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::vfs::EnrolledHostRecord;
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::VfsFailure;
use aruna_ops_data::vfs::admin::{EnrollInput, HostChange};
use aruna_ops_data::vfs::{EnrollHostOperation, RevokeHostOperation, RotateHostOperation};
use aruna_runner::driver::drive;
use aruna_runner::testing::memory_context;

use super::admit::host_allows;
use super::exports::{holds, root_key};
use super::*;
use crate::backend::{ReplayAccess, ReplayCheck};
use crate::rpc::RpcPrincipal;
use crate::types::share;

/// The ed25519 base point, a valid node key.
const NODE_KEY: [u8; 32] = [
    0x58, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
    0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
];

fn owner() -> UserId {
    UserId::new(Ulid::from_parts(1, 5), RealmId([1; 32]))
}

fn backend() -> ArunaNfsBackend {
    ArunaNfsBackend::new(ArunaNfsConfig {
        context: Arc::new(memory_context()),
        realm_id: RealmId([1; 32]),
        node_id: NodeId::from_bytes(&NODE_KEY).expect("the base point is a valid key"),
    })
}

fn binding(target: NfsMountTarget) -> NfsMountBinding {
    NfsMountBinding {
        mount_id: Ulid::from_parts(2, 1),
        host_id: Ulid::from_parts(3, 1),
        owner: owner(),
        access_key: "AKVIEW".to_string(),
        group_id: Ulid::from_parts(4, 1),
        target,
        read_only: false,
        allowed_networks: Vec::new(),
        display_uid: 1000,
        display_gid: 1000,
        mode: 0o755,
        created_at_ms: 1,
    }
}

fn host_record(networks: &[&str], revoked: Option<u64>) -> EnrolledHostRecord {
    EnrolledHostRecord {
        host_id: Ulid::from_parts(3, 1),
        owner: owner(),
        fingerprints: vec![[7; 32]],
        allowed_networks: networks.iter().map(|network| network.to_string()).collect(),
        worker_pool: false,
        created_at_ms: 1,
        revoked_at_ms: revoked,
    }
}

/// Enrolls a host of `owner()` with the certificate key `key` and `networks`.
async fn enroll_host(backend: &ArunaNfsBackend, key: u8, networks: &[&str]) -> EnrolledHostRecord {
    let enroll = EnrollHostOperation::new(EnrollInput {
        owner: owner(),
        fingerprints: vec![[key; 32]],
        proven: [key; 32],
        allowed_networks: networks.iter().map(|network| network.to_string()).collect(),
        worker_pool: false,
    });
    drive(enroll, &backend.context)
        .await
        .expect("host enrolled")
}

fn peer(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 1, 2, last))
}

fn principal() -> RpcPrincipal {
    RpcPrincipal::Sys {
        uid: 1000,
        gid: 1000,
    }
}

/// A caller of `binding` after the backend admitted it for this compound.
fn admitted(backend: &ArunaNfsBackend, binding: &NfsMountBinding) -> Caller {
    backend
        .tables
        .lock()
        .mounts
        .insert(binding.mount_id, binding.clone());
    let host = HostIdentity {
        host_id: binding.host_id,
        owner: binding.owner,
        fingerprint: None,
    };
    Caller {
        host,
        peer: peer(1),
        export: Arc::new(MountExport {
            mount_id: binding.mount_id,
            host_id: binding.host_id,
            principal: binding.owner,
            access_key: binding.access_key.clone(),
            incarnation: 9,
            root: aruna_ops_data::vfs::path::JOB_ROOT,
            read_only: true,
            persistent: true,
        }),
    }
}

#[test]
fn vfs_errors_map() {
    let cases = [
        (VfsError::NotFound, NfsStat4::NoEntry),
        (VfsError::Denied, NfsStat4::Access),
        (VfsError::InvalidName, NfsStat4::Invalid),
        (VfsError::IsDirectory, NfsStat4::IsDirectory),
        (VfsError::NotDirectory, NfsStat4::NotDirectory),
        (VfsError::NotEmpty, NfsStat4::NotEmpty),
        (VfsError::Exists, NfsStat4::Exist),
        (VfsError::CrossExport, NfsStat4::Xdev),
        (VfsError::ReadOnly, NfsStat4::ReadOnly),
        (VfsError::Distributed, NfsStat4::Access),
        (VfsError::VaultLocked, NfsStat4::Access),
        (VfsError::KeyRequired, NfsStat4::Access),
        (VfsError::Quarantined, NfsStat4::Access),
        (VfsError::QuotaExceeded, NfsStat4::QuotaExceeded),
        (VfsError::NoSpace, NfsStat4::NoSpace),
        (VfsError::ShareViolation, NfsStat4::ShareDenied),
        (VfsError::LockConflict, NfsStat4::Denied),
        (VfsError::DeletePending, NfsStat4::Access),
        (VfsError::StaleHandle, NfsStat4::Stale),
        (VfsError::ExpiredHandle, NfsStat4::FilehandleExpired),
        (VfsError::BadCookie, NfsStat4::BadCookie),
        (VfsError::Conflict, NfsStat4::Delay),
        (VfsError::Unavailable("busy".into()), NfsStat4::Delay),
        (VfsError::CorruptStage, NfsStat4::Io),
        (VfsError::Storage("disk".into()), NfsStat4::Io),
        (VfsError::Vfs(VfsFailure::NoSpace), NfsStat4::Io),
    ];
    for (error, status) in cases {
        assert_eq!(vfs_status(&error), status, "{error:?}");
    }
}

#[test]
fn view_errors_map() {
    let cases = [
        (ViewError::NotFound, NfsStat4::NoEntry),
        (ViewError::Denied, NfsStat4::Access),
        (ViewError::MissingVersion, NfsStat4::Stale),
        (ViewError::Locked, NfsStat4::Access),
        (ViewError::KeyRequired, NfsStat4::Access),
        (ViewError::Unavailable("down".into()), NfsStat4::Io),
    ];
    for (error, status) in cases {
        assert_eq!(view_status(&error), status, "{error:?}");
    }
}

#[test]
fn networks_admit_peers() {
    let networks = vec!["10.1.0.0/16".to_string(), "fd00::/8".to_string()];
    assert!(network_allows(&networks, peer(3)));
    assert!(!network_allows(
        &networks,
        IpAddr::V4(Ipv4Addr::new(10, 2, 0, 1))
    ));
    let local = IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1));
    assert!(network_allows(&networks, local));
    // An IPv4 peer seen on a dual-stack socket is still checked as IPv4.
    let mapped = IpAddr::V6(Ipv4Addr::new(10, 1, 9, 9).to_ipv6_mapped());
    assert!(network_allows(&networks, mapped));
    assert!(!network_allows(&["not a network".to_string()], peer(3)));
    assert!(!network_allows(&[], peer(3)));
}

#[test]
fn revoked_hosts_refused() {
    assert!(host_allows(&host_record(&["10.1.0.0/16"], None), peer(1)));
    assert!(!host_allows(
        &host_record(&["10.1.0.0/16"], Some(5)),
        peer(1)
    ));
    assert!(!host_allows(
        &host_record(&["192.168.0.0/16"], None),
        peer(1)
    ));
}

#[test]
fn folders_hold_components() {
    let target = NfsMountTarget::for_folder("data", 7, "team").expect("a canonical folder");
    let folder = binding(target);
    assert_eq!(root_key(&folder), "team");
    assert!(holds(&folder, "team"));
    assert!(holds(&folder, "team/a"));
    assert!(holds(&folder, "team/a/b"));
    assert!(!holds(&folder, "team-old/a"));
    assert!(!holds(&folder, "teamwork"));
    assert!(!holds(&folder, "other/team/a"));
    let whole = binding(NfsMountTarget::for_bucket("data", 7).expect("a bucket"));
    assert_eq!(root_key(&whole), "");
    assert!(holds(&whole, "team-old/a"));
}

#[tokio::test]
async fn views_refuse_writes() {
    let backend = backend();
    let view = binding(NfsMountTarget::JobInputView {
        job_id: JobId::from_bytes([8; 16]),
    });
    let caller = admitted(&backend, &view);
    let object = ObjectRef {
        mount_id: view.mount_id,
        object_id: aruna_ops_data::vfs::path::JOB_ROOT,
    };
    let state = IoState::Anonymous;
    let write = backend
        .write(
            &caller,
            object,
            &state,
            0,
            Bytes::from_static(b"x"),
            StableHow::FileSync,
        )
        .await;
    assert_eq!(write, Err(NfsStat4::ReadOnly));
    let resize = AttributeChange {
        size: Some(0),
        ..AttributeChange::default()
    };
    let setattr = backend
        .set_attributes(&caller, object, &state, &resize)
        .await;
    assert_eq!(setattr, Err(NfsStat4::ReadOnly));
    assert_eq!(
        backend.commit(&caller, object, 0, 0).await,
        Err(NfsStat4::ReadOnly)
    );
    let change = AttributeChange::default();
    let writer = ShareRequest {
        owner: OwnerKey {
            client_id: 1,
            owner: Bytes::from_static(b"owner"),
        },
        access: share::WRITE,
        deny: 0,
    };
    let create = backend
        .create_file(
            &caller,
            object,
            "new",
            CreateMode::Guarded,
            &writer,
            &change,
        )
        .await;
    assert_eq!(create, Err(NfsStat4::ReadOnly));
    let mkdir = backend
        .create_directory(&caller, object, "dir", &change)
        .await;
    assert_eq!(mkdir, Err(NfsStat4::ReadOnly));
    assert_eq!(
        backend.remove(&caller, object, "a").await,
        Err(NfsStat4::ReadOnly)
    );
    let rename = backend.rename(&caller, object, "a", object, "b").await;
    assert_eq!(rename, Err(NfsStat4::ReadOnly));
    assert_eq!(
        backend.open(&caller, object, &writer).await,
        Err(NfsStat4::ReadOnly)
    );
}

#[tokio::test]
async fn fingerprint_finds_host() {
    let backend = backend();
    let enroll = EnrollHostOperation::new(EnrollInput {
        owner: owner(),
        fingerprints: vec![[7; 32]],
        proven: [7; 32],
        allowed_networks: vec!["10.1.0.0/16".to_string()],
        worker_pool: false,
    });
    let host = drive(enroll, &backend.context)
        .await
        .expect("host enrolled");
    let found = backend.enrolled_host([7; 32], peer(4)).await;
    assert_eq!(found.map(|found| found.host_id), Some(host.host_id));
    let outside = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    assert_eq!(backend.enrolled_host([7; 32], outside).await, None);
    assert_eq!(backend.enrolled_host([9; 32], peer(4)).await, None);
}

#[tokio::test]
async fn lapsed_replay_refused() {
    let backend = backend();
    let enroll = EnrollHostOperation::new(EnrollInput {
        owner: owner(),
        fingerprints: vec![[7; 32]],
        proven: [7; 32],
        allowed_networks: vec!["10.1.0.0/16".to_string()],
        worker_pool: false,
    });
    let host = drive(enroll, &backend.context)
        .await
        .expect("host enrolled");
    let identity = HostIdentity {
        host_id: host.host_id,
        owner: host.owner,
        fingerprint: Some([7; 32]),
    };
    let grant = |peer, mounts| ReplayGrant {
        host: identity,
        peer,
        principal: principal(),
        mounts,
        checks: Vec::new(),
    };
    assert_eq!(
        backend.recheck_replay(&grant(peer(4), Vec::new())).await,
        Ok(())
    );
    let outside = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    let moved = backend.recheck_replay(&grant(outside, Vec::new())).await;
    assert_eq!(moved, Err(NfsStat4::Access));
    // A deleted binding, or one whose key is gone, never releases its cached reply.
    let deleted = backend
        .recheck_replay(&grant(peer(4), vec![Ulid::from_parts(5, 5)]))
        .await;
    assert_eq!(deleted, Err(NfsStat4::Access));
    let unknown = HostIdentity {
        host_id: Ulid::from_parts(6, 6),
        owner: owner(),
        fingerprint: None,
    };
    let mut lost = grant(peer(4), Vec::new());
    lost.host = unknown;
    assert_eq!(backend.recheck_replay(&lost).await, Err(NfsStat4::Access));
}

#[tokio::test]
async fn loopback_admits_one() {
    let backend = backend();
    let host = enroll_host(&backend, 7, &["127.0.0.1/32", "::1/128", "10.1.0.0/16"]).await;
    let ipv4 = backend.loopback_host(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    assert_eq!(ipv4.map(|found| found.host_id), Some(host.host_id));
    let ipv6 = backend.loopback_host(IpAddr::V6(Ipv6Addr::LOCALHOST)).await;
    assert_eq!(ipv6.map(|found| found.host_id), Some(host.host_id));
    // A peer in the host's networks is still no loopback connection.
    assert_eq!(backend.loopback_host(peer(4)).await, None);
}

#[tokio::test]
async fn loopback_refuses_ambiguity() {
    let backend = backend();
    let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
    assert_eq!(backend.loopback_host(local).await, None);
    enroll_host(&backend, 7, &["127.0.0.1/32"]).await;
    enroll_host(&backend, 8, &["127.0.0.0/8"]).await;
    assert_eq!(backend.loopback_host(local).await, None);
}

#[tokio::test]
async fn revoked_loopback_refused() {
    let backend = backend();
    let host = enroll_host(&backend, 7, &["127.0.0.1/32"]).await;
    let revoke = RevokeHostOperation::new(host.host_id, owner(), HostChange::Revoke);
    drive(revoke, &backend.context).await.expect("host revoked");
    let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
    assert_eq!(backend.loopback_host(local).await, None);
}

// A rotated key ends what its connection may do at once, even with no revocation event: replays
// and mount admission check the proven key against the host record.
#[tokio::test]
async fn rotated_key_refused() {
    let backend = backend();
    let host = enroll_host(&backend, 7, &["10.1.0.0/16"]).await;
    let found = backend.enrolled_host([7; 32], peer(4)).await;
    let found = found.expect("the enrolled key admits");
    assert_eq!(found.fingerprint, Some([7; 32]));
    let grant = ReplayGrant {
        host: found,
        peer: peer(4),
        principal: principal(),
        mounts: Vec::new(),
        checks: Vec::new(),
    };
    assert_eq!(backend.recheck_replay(&grant).await, Ok(()));
    let rotate = HostChange::Rotate {
        fingerprints: vec![[8; 32]],
        proven: [8; 32],
    };
    let rotate = RotateHostOperation::new(host.host_id, owner(), rotate);
    drive(rotate, &backend.context).await.expect("key rotated");
    assert_eq!(backend.recheck_replay(&grant).await, Err(NfsStat4::Access));
    let mount = backend
        .mount_export(&found, peer(4), Ulid::from_parts(5, 5))
        .await;
    assert_eq!(mount, Err(NfsStat4::Access));
}

// A replay is admitted again for each object use: a gone run, a missing bucket, a write on a
// view or a use outside the grant's bindings refuses it.
#[tokio::test]
async fn replay_checks_objects() {
    let backend = backend();
    let host = enroll_host(&backend, 7, &["10.1.0.0/16"]).await;
    let view = binding(NfsMountTarget::JobInputView {
        job_id: JobId::from_bytes([8; 16]),
    });
    let root = ObjectRef {
        mount_id: view.mount_id,
        object_id: aruna_ops_data::vfs::path::JOB_ROOT,
    };
    let check = |access| ReplayCheck {
        object: root,
        access,
    };
    let head = backend
        .recheck_view(&view, &check(ReplayAccess::Head))
        .await;
    assert!(head.is_err(), "a view of a gone run shows nothing");
    let write = backend
        .recheck_view(&view, &check(ReplayAccess::Write))
        .await;
    assert_eq!(write, Err(NfsStat4::ReadOnly));
    let bucket = binding(NfsMountTarget::for_bucket("data", 7).expect("a bucket"));
    let live = ReplayCheck {
        object: ObjectRef {
            mount_id: bucket.mount_id,
            object_id: LIVE_ROOT,
        },
        access: ReplayAccess::List,
    };
    let listed = backend.recheck_live(&bucket, &live).await;
    assert!(listed.is_err(), "a missing bucket lists nothing");
    let grant = ReplayGrant {
        host: HostIdentity {
            host_id: host.host_id,
            owner: host.owner,
            fingerprint: Some([7; 32]),
        },
        peer: peer(4),
        principal: principal(),
        mounts: Vec::new(),
        checks: vec![check(ReplayAccess::Head)],
    };
    assert_eq!(backend.recheck_replay(&grant).await, Err(NfsStat4::Access));
}

// A deleted binding stops serving at once, but the engine's cleanup still releases the opens
// and locks made through it; the binding is forgotten once nothing holds it.
#[tokio::test]
async fn revoked_mount_releases() {
    let backend = backend();
    let view = binding(NfsMountTarget::JobInputView {
        job_id: JobId::from_bytes([8; 16]),
    });
    let caller = admitted(&backend, &view);
    let object = ObjectRef {
        mount_id: view.mount_id,
        object_id: aruna_ops_data::vfs::path::JOB_ROOT,
    };
    let owner = OwnerKey {
        client_id: 1,
        owner: Bytes::from_static(b"owner"),
    };
    let locked = backend
        .lock(&caller, object, &owner, LockKind::Read, LockRange::ALL)
        .await;
    assert_eq!(locked, Ok(()));
    backend.publish_revocation(Revocation::Mount(view.mount_id));
    let shown = backend.attributes(&caller, object).await;
    assert_eq!(shown, Err(NfsStat4::Stale));
    assert_eq!(backend.close(&caller, object, &owner).await, Ok(()));
    assert!(
        backend.tables.lock().closing.contains_key(&view.mount_id),
        "a lock still holds the binding"
    );
    let range = LockRange::ALL;
    let unlocked = backend.unlock(&caller, object, &owner, range).await;
    assert_eq!(unlocked, Ok(false));
    assert!(backend.tables.lock().closing.is_empty());
    let again = backend.unlock(&caller, object, &owner, range).await;
    assert_eq!(again, Err(NfsStat4::Stale));
}
