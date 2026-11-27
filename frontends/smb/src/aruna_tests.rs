//! Unit tests for the Aruna SMB backend: error mapping, credential spelling, job view and
//! distributed bucket refusal, cursors, revocations, write bodies and failed close retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USER_ACCESS_KEYSPACE};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionRevision};
use aruna_core::structs::storage::vfs::{
    DetachReason, FileIdentity, NfsStageRecord, QuotaRetry, StageBase, StageDisposition, StageScope,
};
use aruna_core::user::id::UserId;
use aruna_core::vfs_effects::InstanceSummary;
use aruna_ops_data::vfs::CHUNK_BYTES;
use aruna_ops_data::vfs::handles::{ListNext, ListResume};
use aruna_ops_data::vfs::publish::{CloseOutcome, Published};
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

/// A delete-on-close the file server fails transiently stays with the backend once the SMB
/// handle is gone: retried in the background and before an open, until the backend is dropped.
#[tokio::test(start_paused = true)]
async fn pending_delete_retries() {
    let server = backend(MemoryStorage::open());
    let mut open = joined_open(None);
    open.delete_on_close = true;
    let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
    let key = (scope.clone(), open.open_id);
    // A node without file servers answers every close as unconfigured, which may pass later.
    let closed = server.close_file(open.clone()).await;
    assert!(
        matches!(closed, Err(BackendError::Internal(_))),
        "{closed:?}"
    );
    assert!(server.pending.lock().contains_key(&key));
    server.queue_pending(open.clone(), None, None);
    assert_eq!(server.pending.lock().len(), 1, "one retry per failed close");

    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(
        server.pending.lock().contains_key(&key),
        "background retries keep a close that still fails"
    );
    assert!(!server.drain_pending(&scope).await);
    assert!(server.pending.lock().contains_key(&key));
    let other = BucketScope::new("other", open.incarnation);
    assert!(!server.drain_pending(&other).await);

    let pending = Arc::clone(&server.pending);
    drop(server);
    // The background retry ends with the backend and lets go of the shared table.
    while Arc::strong_count(&pending) > 1 {
        tokio::task::yield_now().await;
    }
}

/// A retried close keeps going while its own writer still owns unpublished dirty bytes or the
/// failure may pass, and ends on any finished close or a definitive refusal.
#[test]
fn retry_keeps_unpublished() {
    let published = Published {
        version_id: Ulid::from_parts(1, 1),
        life_id: None,
    };
    assert!(!writes::retry_done(Ok((CloseOutcome::Pending, true))));
    // A read-only open, a detached instance or a quota-blocked checkpoint leaves the bytes.
    assert!(writes::retry_done(Ok((CloseOutcome::Pending, false))));
    assert!(writes::retry_done(Ok((CloseOutcome::Closed, true))));
    assert!(writes::retry_done(Ok((
        CloseOutcome::Published(published),
        true
    ))));
    let unavailable = VfsError::Unavailable("store offline".into());
    assert!(!writes::retry_done(Err(unavailable)));
    assert!(writes::retry_done(Err(VfsError::Denied)));
    assert!(writes::retry_done(Err(VfsError::NotFound)));
}

/// A publication rebuild that fails busy or internally retries; any other failure ends it.
#[test]
fn prepare_errors_retry() {
    let transient = [
        BackendError::Internal("routing offline".into()),
        BackendError::Busy,
        BackendError::Sharing,
    ];
    for error in transient {
        let mapped = writes::retry_prepare_error(error);
        assert!(matches!(mapped, VfsError::Unavailable(_)), "{mapped:?}");
        assert!(!writes::retry_done(Err(mapped)));
    }
    let not_found = writes::retry_prepare_error(BackendError::NotFound);
    assert!(matches!(not_found, VfsError::NotFound), "{not_found:?}");
    let directory = writes::retry_prepare_error(BackendError::IsDirectory);
    assert!(matches!(directory, VfsError::IsDirectory), "{directory:?}");
    assert!(writes::retry_done(Err(directory)));
}

/// A live checkpoint row of `open`, waiting for quota when `retry` is set.
fn stage_row(open: &FileOpen, retry: Option<QuotaRetry>) -> NfsStageRecord {
    NfsStageRecord {
        stage_id: Ulid::from_parts(2, 2),
        handle_id: 4,
        owner: open.owner.clone(),
        key: open.key.clone(),
        generation: 1,
        checkpoint: "c-1".into(),
        logical_size: 12,
        base: StageBase::Unpublished {
            token: Ulid::from_parts(3, 3),
            predecessor: None,
        },
        source_copies: Vec::new(),
        source_policies: Vec::new(),
        destination_copies: Vec::new(),
        destination_policies: Vec::new(),
        disposition: StageDisposition::Live,
        quota_retry: retry,
        foreign_life: None,
        updated_at_ms: 0,
    }
}

/// Only the last writer publishes dirty bytes at close; a read-only open and a checkpoint the
/// quota retry owns leave them to their owner.
#[test]
fn reader_skips_publish() {
    let writer = joined_open(None);
    let mut reader = writer.clone();
    reader.write = false;
    let mut summary = renamed_instance(None, Ulid::from_parts(1, 1));
    summary.dirty = true;
    summary.writer_count = 0;
    let live = stage_row(&writer, None);
    let retry = QuotaRetry {
        attempts: 1,
        next_at_ms: 10,
    };
    let waiting = stage_row(&writer, Some(retry));
    let mut kept = waiting.clone();
    kept.disposition = StageDisposition::Retained {
        reason: DetachReason::LostLife,
    };
    assert!(writes::is_quota_blocked(Some(&waiting)));
    assert!(!writes::is_quota_blocked(Some(&live)));
    assert!(!writes::is_quota_blocked(Some(&kept)));
    assert!(!writes::is_quota_blocked(None));

    assert!(!writes::owns_publish(&reader, &summary, None));
    assert!(!writes::owns_publish(&reader, &summary, Some(&live)));
    assert!(!writes::owns_publish(&writer, &summary, Some(&waiting)));
    assert!(writes::owns_publish(&writer, &summary, None));
    assert!(writes::owns_publish(&writer, &summary, Some(&live)));
    summary.writer_count = 2;
    assert!(!writes::owns_publish(&writer, &summary, None));
}

/// A queued close keeps the publication attempt it left until a retry settles it, and the
/// retry settles it first and once, before a later step fails.
#[tokio::test(start_paused = true)]
async fn publish_retry_tracked() {
    let server = backend(MemoryStorage::open());
    let open = joined_open(None);
    let scope = BucketScope::new(open.bucket.clone(), open.incarnation);
    let key = (scope.clone(), open.open_id);
    let attempt = PublishRetry {
        attempt_id: Ulid::from_parts(5, 5),
        logical_size: 12,
        expected_edit: Some(3),
    };
    server.queue_pending(open.clone(), None, Some(attempt));
    let entry = server.pending.lock().get(&key).cloned();
    let entry = entry.expect("the failed close is queued");
    assert_eq!(*entry.publish.lock(), Some(attempt));
    // Another failed close of the same open never replaces the attempt still to settle.
    let other = PublishRetry {
        attempt_id: Ulid::from_parts(6, 6),
        ..attempt
    };
    server.queue_pending(open.clone(), None, Some(other));
    assert_eq!(*entry.publish.lock(), Some(attempt));

    // Without a receipt the attempt settles as rolled back; the instance lookup after it
    // still fails on a node without file servers, so the close stays queued.
    assert!(!server.drain_pending(&scope).await);
    assert!(server.pending.lock().contains_key(&key));
    assert_eq!(*entry.publish.lock(), None);
}
