//! Unit tests for the Aruna SMB backend: error mapping, exact credential spelling, job view and
//! distributed bucket refusal, cursors, revocation delivery and chunked write bodies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionRevision};
use aruna_core::structs::storage::vfs::{FileIdentity, StageScope};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::InstanceSummary;
use aruna_ops_data::vfs::CHUNK_BYTES;
use aruna_ops_data::vfs::handles::{ListNext, ListResume};
use aruna_ops_work::jobs::workflow::input_view::ViewFrom;
use aruna_storage::storage::memory::MemoryStorage;
use futures_util::StreamExt;

use super::*;

/// The Ed25519 base point, a valid node public key.
const NODE_KEY: [u8; 32] = [
    0x58, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
    0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66,
];

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node() -> NodeId {
    NodeId::from_bytes(&NODE_KEY).expect("the base point is a valid key")
}

fn backend(storage: aruna_storage::storage::handle::StorageHandle) -> ArunaSmbBackend {
    let driver = Arc::new(DriverContext::storage_only(storage));
    let config = ArunaSmbConfig {
        realm_id: realm(),
        node_id: node(),
        credential_key: CredentialEncryptionKey::random(),
    };
    ArunaSmbBackend::new(driver, config)
}

fn credential(access_key: &str) -> Credential {
    Credential {
        access_key: access_key.to_string(),
        principal: UserId::new(Ulid::generate(), realm()),
    }
}

async fn write_row(
    storage: &aruna_storage::storage::handle::StorageHandle,
    space: &str,
    key: &[u8],
    value: Vec<u8>,
) {
    storage
        .send_storage_effect(StorageEffect::Write {
            key_space: space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
}

#[test]
fn vfs_errors_map() {
    let cases = [
        (VfsError::NotFound, BackendError::NotFound),
        (VfsError::Denied, BackendError::Denied),
        (VfsError::VaultLocked, BackendError::Denied),
        (VfsError::KeyRequired, BackendError::Denied),
        (VfsError::Quarantined, BackendError::Denied),
        (VfsError::Distributed, BackendError::Denied),
        (VfsError::InvalidName, BackendError::Invalid),
        (VfsError::IsDirectory, BackendError::IsDirectory),
        (VfsError::NotDirectory, BackendError::NotDirectory),
        (VfsError::NotEmpty, BackendError::NotEmpty),
        (VfsError::Exists, BackendError::Exists),
        (VfsError::CrossExport, BackendError::CrossDevice),
        (VfsError::ReadOnly, BackendError::ReadOnly),
        (VfsError::QuotaExceeded, BackendError::DiskFull),
        (VfsError::NoSpace, BackendError::DiskFull),
        (VfsError::ShareViolation, BackendError::Sharing),
        (VfsError::Conflict, BackendError::Sharing),
        (VfsError::LockConflict, BackendError::LockConflict),
        (VfsError::DeletePending, BackendError::DeletePending),
        (VfsError::StaleHandle, BackendError::NotFound),
        (VfsError::ExpiredHandle, BackendError::NotFound),
    ];
    for (error, expected) in cases {
        assert_eq!(vfs_failure(error.clone()), expected, "{error:?}");
    }
    let fault = vfs_failure(VfsError::Storage("disk".into()));
    assert!(matches!(fault, BackendError::Internal(_)));
    assert_eq!(fault.status(), crate::status::INTERNAL_ERROR);
    assert_eq!(
        vfs_failure(VfsError::CrossExport).status(),
        crate::status::NOT_SAME_DEVICE
    );
    assert_eq!(
        vfs_failure(VfsError::ReadOnly).status(),
        crate::status::MEDIA_WRITE_PROTECTED
    );
}

#[test]
fn view_errors_map() {
    assert_eq!(view_failure(ViewError::NotFound), BackendError::NotFound);
    assert_eq!(
        view_failure(ViewError::MissingVersion),
        BackendError::NotFound
    );
    assert_eq!(view_failure(ViewError::Denied), BackendError::Denied);
    assert_eq!(view_failure(ViewError::Locked), BackendError::Denied);
    assert_eq!(view_failure(ViewError::KeyRequired), BackendError::Denied);
    let unavailable = view_failure(ViewError::Unavailable("holder".into()));
    assert!(matches!(unavailable, BackendError::Internal(_)));
}

#[test]
fn lock_owner_folds() {
    let open_id = Ulid::from((7u128 << 64) | 5);
    assert_eq!(files::lock_owner(open_id), 7 ^ 5);
    assert_ne!(
        files::lock_owner(Ulid::generate()),
        files::lock_owner(Ulid::generate())
    );
}

/// An open of `old.txt` at `version_id`, before any change through another protocol.
fn joined_open(version_id: Option<Ulid>) -> FileOpen {
    let user = UserId::new(Ulid::generate(), realm());
    let info = BucketInfo {
        group_id: Ulid::generate(),
        created_at: SystemTime::now(),
        created_by: user,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: None,
    };
    let life_id = Ulid::generate();
    FileOpen {
        access_key: "AKUSER1".into(),
        bucket: "data".into(),
        incarnation: 1,
        key: "old.txt".into(),
        kind: HandleKind::File,
        open_id: Ulid::generate(),
        instance_id: 9,
        identity: HandleIdentity::File(FileIdentity::Committed { life_id }),
        version_id,
        write: true,
        delete_on_close: false,
        owner: StageOwner {
            user,
            access_key: "AKUSER1".into(),
            mount_id: Ulid::nil(),
            scope: StageScope::User,
        },
        info,
        opened_ms: 0,
    }
}

/// The instance of `joined_open` after another protocol renamed it and published `base`.
fn renamed_instance(base: Option<Ulid>, life_id: Ulid) -> InstanceSummary {
    InstanceSummary {
        instance_id: 9,
        key: "dir/new.txt".into(),
        kind: HandleKind::File,
        handle_id: Some(4),
        identity: HandleIdentity::File(FileIdentity::Committed { life_id }),
        logical_size: 12,
        base_limit: 12,
        dirty: false,
        delete_pending: false,
        disposition_owner: None,
        open_count: 1,
        writer_count: 1,
        detached: None,
        stage_id: None,
        generation: 0,
        change: 0,
        base_version: base,
        edit_generation: 0,
        allows_delete: true,
        sealed_edit: None,
        draft: None,
    }
}

#[test]
fn live_open_follows() {
    let (opened, published, life_id) = (Ulid::generate(), Ulid::generate(), Ulid::generate());
    let open = joined_open(Some(opened));
    let unchanged = files::current_open(&open, None);
    assert_eq!(unchanged.key, "old.txt");
    assert_eq!(unchanged.version_id, Some(opened));
    // A rename and publish through NFS move the open to the live name, life and version.
    let live = files::current_open(&open, Some(&renamed_instance(Some(published), life_id)));
    assert_eq!(live.key, "dir/new.txt");
    assert_eq!(live.version_id, Some(published));
    let committed = HandleIdentity::File(FileIdentity::Committed { life_id });
    assert_eq!(live.identity, committed);
    assert_eq!(
        (live.instance_id, live.open_id),
        (open.instance_id, open.open_id)
    );
    // An instance without a published base keeps the version the open joined.
    let kept = files::current_open(&open, Some(&renamed_instance(None, life_id)));
    assert_eq!(kept.version_id, Some(opened));
}

#[test]
fn listing_cursors_resume() {
    let after = names::cursor_of(&ListNext::After("Report.txt".into()));
    let resumed = names::resume_of(after.as_deref());
    assert_eq!(resumed.ok(), Some(ListResume::After("Report.txt".into())));
    assert_eq!(names::cursor_of(&ListNext::End), None);
    assert_eq!(names::resume_of(None).ok(), Some(ListResume::Start));
    // A sealed cursor this process never issued opens nothing.
    assert_eq!(
        names::resume_of(Some("snot-a-position")),
        Err(BackendError::Invalid)
    );
    assert_eq!(names::resume_of(Some("x")), Err(BackendError::Invalid));
}

#[test]
fn view_cursors_resume() {
    let cursors = [
        ViewFrom::After("data/a.csv".into()),
        ViewFrom::Hidden {
            at: "data/b:c".into(),
            pending: None,
        },
        ViewFrom::Hidden {
            at: "data/b".into(),
            pending: Some("data/".into()),
        },
    ];
    for from in cursors {
        let text = jobs::view_cursor(&from);
        assert_eq!(jobs::view_resume(&text), Ok(from));
    }
    assert_eq!(jobs::view_resume("h9:ab"), Err(BackendError::Invalid));
}

#[tokio::test]
async fn exact_key_spelling() {
    let storage = MemoryStorage::open();
    let server = backend(storage.clone());
    let key_id = format!("AK{}", Ulid::generate());
    let access_key = UserAccess::build_access_key(&key_id).expect("an alphanumeric key");
    let mut access = UserAccess {
        access_key: access_key.clone(),
        user_identity: UserId::new(Ulid::generate(), realm()),
        group_id: Ulid::generate(),
        secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
        expiry: SystemTime::now() + Duration::from_secs(3600),
        path_restrictions: None,
        issued_by: *node().as_bytes(),
        revoked_at: None,
    };
    access
        .encrypt_secret(&server.credential_key, "secret", [3; 24])
        .expect("the secret seals");
    let row = access.to_bytes().expect("the access encodes");
    write_row(&storage, USER_ACCESS_KEYSPACE, access_key.as_bytes(), row).await;
    let folded = access_key.to_lowercase();
    assert_ne!(folded, access_key);
    // A case-folded alias names no credential at all.
    let alias = server.lookup_secret(&folded).await;
    assert!(matches!(alias, Ok(None)), "{alias:?}");
    let refused = server.admit(&credential(&folded)).await;
    assert_eq!(refused, Err(BackendError::Revoked));
    // The exact spelling reaches owner admission, which needs the realm this store lacks.
    let exact = server.lookup_secret(&access_key).await;
    assert!(matches!(exact, Err(BackendError::Internal(_))), "{exact:?}");
}

#[tokio::test]
async fn job_view_gated() {
    let server = backend(MemoryStorage::open());
    let job = Ulid::generate();
    let share = ShareTarget::JobView(job);
    let user = server.connect_tree(&credential("AKUSER1"), &share).await;
    assert_eq!(user, Err(BackendError::Denied));
    let other = JobId::from_bytes(Ulid::generate().to_bytes());
    let attempt = credential(&format!("ws{other}"));
    let foreign = server.connect_tree(&attempt, &share).await;
    assert_eq!(foreign, Err(BackendError::Denied));
    let scope = Scope {
        credential: credential("AKUSER1"),
        share,
    };
    let lookup = server.lookup(&scope, "input.csv").await;
    assert_eq!(lookup, Err(BackendError::Denied));
}

#[tokio::test]
async fn distributed_bucket_refused() {
    let storage = MemoryStorage::open();
    let server = backend(storage.clone());
    let info = BucketInfo {
        group_id: Ulid::generate(),
        created_at: SystemTime::now(),
        created_by: UserId::new(Ulid::generate(), realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: Some(DistributionBinding {
            set_id: Ulid::generate(),
            member_id: Ulid::generate(),
            incarnation: 0,
            revision: DistributionRevision::default(),
        }),
    };
    let row = info.to_bytes().expect("the bucket encodes");
    write_row(&storage, S3_BUCKET_KEYSPACE, b"shared", row).await;
    let caller = credential("AKUSER1");
    let shared = ShareTarget::Bucket("shared".into());
    let refused = server.connect_tree(&caller, &shared).await;
    assert_eq!(refused, Err(BackendError::Denied));
    let missing = ShareTarget::Bucket("missing".into());
    let absent = server.connect_tree(&caller, &missing).await;
    assert_eq!(absent, Err(BackendError::NotFound));
}

#[tokio::test]
async fn revocations_reach_subscribers() {
    let server = backend(MemoryStorage::open());
    // Nobody listens yet; the revocation is dropped without failing.
    server.publish_revocation("AKEARLY");
    let mut first = server.revocations();
    let mut second = server.revocation_sender().subscribe();
    server.publish_revocation("AKUSER1");
    let expected = Revocation {
        access_key: "AKUSER1".into(),
    };
    assert_eq!(first.recv().await.ok(), Some(expected.clone()));
    assert_eq!(second.recv().await.ok(), Some(expected));
}

/// Collects every item of a body, reporting failures as their text.
async fn drain(mut body: aruna_core::stream::ByteStream) -> Vec<Result<usize, String>> {
    let mut items = Vec::new();
    while let Some(item) = body.next().await {
        items.push(
            item.map(|chunk| chunk.len())
                .map_err(|error| error.to_string()),
        );
    }
    items
}

#[tokio::test]
async fn empty_body_ends() {
    let calls = Arc::new(Mutex::new(0usize));
    let seen = calls.clone();
    let body = bodies::chunked_body(0, move |_, _| {
        *seen.lock() += 1;
        async { Ok::<Bytes, VfsError>(Bytes::new()) }
    });
    assert!(drain(body).await.is_empty());
    assert_eq!(*calls.lock(), 0);
}

#[tokio::test]
async fn body_crosses_chunks() {
    let chunk = usize::try_from(CHUNK_BYTES).expect("a chunk fits in memory");
    let size = 2 * CHUNK_BYTES + 5;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let body = bodies::chunked_body(size, move |offset, length| {
        seen.lock().push((offset, length));
        let data = vec![(offset / CHUNK_BYTES) as u8; length as usize];
        async move { Ok::<Bytes, VfsError>(Bytes::from(data)) }
    });
    assert_eq!(drain(body).await, vec![Ok(chunk), Ok(chunk), Ok(5)]);
    let full = u32::try_from(CHUNK_BYTES).expect("a chunk length fits");
    let expected = vec![(0, full), (CHUNK_BYTES, full), (2 * CHUNK_BYTES, 5)];
    assert_eq!(*requests.lock(), expected);
}

#[tokio::test]
async fn short_read_fails() {
    let requests = Arc::new(Mutex::new(0usize));
    let seen = requests.clone();
    let body = bodies::chunked_body(CHUNK_BYTES + 1, move |_, length| {
        *seen.lock() += 1;
        let data = vec![0; length as usize - 1];
        async move { Ok::<Bytes, VfsError>(Bytes::from(data)) }
    });
    let conflict = VfsError::Conflict.to_string();
    assert_eq!(drain(body).await, vec![Err(conflict)]);
    // The reader stops at the first failure and reads nothing after it.
    assert_eq!(*requests.lock(), 1);
}

#[tokio::test]
async fn read_error_propagates() {
    let body = bodies::chunked_body(CHUNK_BYTES + 3, move |offset, length| {
        let item = match offset {
            0 => Ok(Bytes::from(vec![1; length as usize])),
            _ => Err(VfsError::NotFound),
        };
        async move { item }
    });
    let chunk = usize::try_from(CHUNK_BYTES).expect("a chunk fits in memory");
    let missing = VfsError::NotFound.to_string();
    assert_eq!(drain(body).await, vec![Ok(chunk), Err(missing)]);
}
