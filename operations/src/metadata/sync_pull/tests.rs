//! Tests for the realm side of a device's synced folders.

use super::*;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument,
    RealmId, SourceMetadata,
};
use aruna_core::types::UserId;
use aruna_storage::FjallStorage;
use std::time::SystemTime;

fn deny_policy(expression: &str) -> RequestPolicy {
    RequestPolicy {
        policy_id: Ulid::from_bytes([7; 16]),
        name: "sync-pull-test".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: expression.to_string(),
        enabled: true,
    }
}

struct PolicyFixture {
    context: Arc<DriverContext>,
    auth: AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    _dir: tempfile::TempDir,
}

/// A realm whose owner may write everywhere, optionally under one deny
/// policy: the same state an S3 request of the same shape is judged against.
async fn policy_fixture(expression: Option<&str>) -> PolicyFixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let user_id = UserId::local(Ulid::from_bytes([5; 16]), realm_id);
    let group_id = Ulid::from_bytes([6; 16]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let auth_doc = GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
    let group = Group {
        display_name: "sync".to_string(),
        group_id,
        realm_id,
        roles: auth_doc.roles.keys().copied().collect(),
        owner: user_id,
    };
    let mut realm = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    realm.request_policies = expression.map(deny_policy).into_iter().collect();
    let _ = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: vec![
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    realm.to_bytes(&actor).unwrap().into(),
                ),
                (
                    GROUP_KEYSPACE.to_string(),
                    group_id.to_bytes().to_vec().into(),
                    group.to_bytes(&actor).unwrap().into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                        .to_bytes(&actor)
                        .unwrap()
                        .into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    group_id.to_bytes().to_vec().into(),
                    auth_doc.to_bytes(&actor).unwrap().into(),
                ),
            ],
            txn_id: None,
        })
        .await;
    PolicyFixture {
        context: Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }),
        auth: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        },
        node_id,
        group_id,
        _dir: dir,
    }
}

fn bucket_info(group_id: GroupId, user_id: UserId) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    }
}

fn pull_request(deleted: bool) -> PullRequest {
    PullRequest {
        auth_token: aruna_core::metadata::MetadataAuthToken::internal(AuthContext {
            user_id: UserId::local(Ulid::from_bytes([5; 16]), RealmId::from_bytes([4u8; 32])),
            realm_id: RealmId::from_bytes([4u8; 32]),
            path_restrictions: None,
        }),
        source: VersionedObjectArn::new(
            RealmId::from_bytes([4u8; 32]),
            iroh::SecretKey::from_bytes(&[9; 32]).public(),
            "folder-x".to_string(),
            "note.txt".to_string(),
            Ulid::from_bytes([2; 16]),
        )
        .unwrap(),
        blake3: None,
        size: 4,
        target_bucket: "lab".to_string(),
        target_key: "note.txt".to_string(),
        deleted,
    }
}

#[tokio::test]
async fn policy_denies_pull() {
    // A deny policy that would refuse the owner's own S3 put must refuse the
    // forwarded pull that performs the same write.
    let fixture = policy_fixture(Some("operation == 's3.PutObject'")).await;
    let bucket = bucket_info(fixture.group_id, fixture.auth.user_id);
    assert_eq!(
        ensure_write(
            &fixture.context,
            &fixture.auth,
            fixture.node_id,
            &bucket,
            &pull_request(false),
        )
        .await,
        Err(SyncRefusal::Forbidden)
    );
    // The same request passes once the policy is gone.
    let allowed = policy_fixture(None).await;
    assert!(
        ensure_write(
            &allowed.context,
            &allowed.auth,
            allowed.node_id,
            &bucket_info(allowed.group_id, allowed.auth.user_id),
            &pull_request(false),
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn delete_follows_upload() {
    // The same device version is uploaded and then deleted: the delete must
    // not read the upload's own version as its replay, or the realm object
    // would stay live after the owner removed the file.
    let fixture = policy_fixture(None).await;
    let request = pull_request(true);
    let mut version = BlobVersion::materialized(
        [4u8; 32],
        aruna_core::structs::BackendRef::node_default(),
        SystemTime::UNIX_EPOCH,
        fixture.auth.user_id,
        None,
    );
    version.metadata = HashMap::from([(
        SYNC_SOURCE_VERSION_TAG.to_string(),
        request.source.version.to_string(),
    )]);
    let key = VersionKey::new(
        &request.target_bucket,
        &request.target_key,
        Ulid::from_bytes([6; 16]),
    );
    let _ = fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes().unwrap().into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    assert_eq!(applied_version(&fixture.context, &request).await, None);
    // The upload itself still replays as the version it produced.
    assert_eq!(
        applied_version(&fixture.context, &pull_request(false)).await,
        Some(Ulid::from_bytes([6; 16]))
    );
}

#[tokio::test]
async fn replayed_delete_settles() {
    // A retried delete must answer with the marker it already produced; the
    // source version on the marker is what makes it recognizable.
    let fixture = policy_fixture(None).await;
    let request = pull_request(true);
    let version_id = Ulid::from_bytes([7; 16]);
    let mut marker = BlobVersion::deleted(SystemTime::UNIX_EPOCH, fixture.auth.user_id);
    marker.metadata = HashMap::from([(
        SYNC_SOURCE_VERSION_TAG.to_string(),
        request.source.version.to_string(),
    )]);
    let key = VersionKey::new(&request.target_bucket, &request.target_key, version_id);
    let _ = fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.to_bytes().unwrap().into(),
            value: marker.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    assert_eq!(
        applied_version(&fixture.context, &request).await,
        Some(version_id)
    );
    let newer = PullRequest {
        source: VersionedObjectArn::new(
            request.source.realm_id,
            request.source.node_id,
            request.source.bucket.clone(),
            request.source.key.clone(),
            Ulid::from_bytes([8; 16]),
        )
        .unwrap(),
        ..pull_request(true)
    };
    assert_eq!(applied_version(&fixture.context, &newer).await, None);
}

#[tokio::test]
async fn policy_denies_listing() {
    // The listing runs the boundary too, under the operation an S3 client
    // would be judged by.
    let fixture = policy_fixture(Some("operation == 's3.ListObjectVersions'")).await;
    assert_eq!(
        ensure_read(
            &fixture.context,
            &fixture.auth,
            fixture.node_id,
            fixture.group_id,
            "lab",
        )
        .await,
        Err(SyncRefusal::Forbidden)
    );
    let allowed = policy_fixture(None).await;
    assert!(
        ensure_read(
            &allowed.context,
            &allowed.auth,
            allowed.node_id,
            allowed.group_id,
            "lab",
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn policy_denies_delete() {
    // A delete pull is judged as the delete it performs, not as a put.
    let fixture = policy_fixture(Some("operation == 's3.DeleteObject'")).await;
    assert_eq!(
        ensure_write(
            &fixture.context,
            &fixture.auth,
            fixture.node_id,
            &bucket_info(fixture.group_id, fixture.auth.user_id),
            &pull_request(true),
        )
        .await,
        Err(SyncRefusal::Forbidden)
    );
}

fn version(key: &str, latest: bool) -> ListObjectVersionsItem {
    ListObjectVersionsItem::Version {
        key: key.to_string(),
        version_id: Ulid::from_bytes([1u8; 16]),
        is_latest: latest,
        location: None,
        source_metadata: Some(SourceMetadata {
            content_length: 7,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
        }),
        created_at: SystemTime::UNIX_EPOCH,
    }
}

#[test]
fn names_heads_relatively() {
    // The device binds a prefix, so the answer is relative to that prefix.
    let head = head_of(&version("notes/a/b.txt", true), "notes").expect("a head");
    assert_eq!(head.relative, "a/b.txt");
    assert_eq!(head.size, 7);
    assert!(!head.deleted);
    assert!(head_of(&version("notes/a/b.txt", false), "notes").is_none());
    assert!(head_of(&version("other/b.txt", true), "notes").is_none());
}

#[test]
fn parks_hash_mismatch() {
    // The device's file no longer answers for the bytes the upload named, so
    // re-reading it can only fail again: the row parks for the owner rather
    // than retrying forever, while transport faults stay retryable.
    assert_eq!(
        read_refusal(&BaoReadError::Refused(BaoReadRefusal::HashMismatch)),
        SyncRefusal::Invalid(
            "the device version no longer carries the bytes this pull named".to_string()
        )
    );
    assert_eq!(
        read_refusal(&BaoReadError::Refused(BaoReadRefusal::BackendFailure)),
        SyncRefusal::Unavailable
    );
    assert_eq!(
        read_refusal(&BaoReadError::NotFinished),
        SyncRefusal::Unavailable
    );
}

#[test]
fn keeps_reads_retryable() {
    // The drain parks a NotFound for good, so only a bucket that is really
    // absent may answer it; a storage fault must stay retryable.
    assert_eq!(
        bucket_refusal(GetBucketInfoError::NotFound),
        SyncRefusal::NotFound
    );
    assert_eq!(
        bucket_refusal(GetBucketInfoError::StorageError(
            StorageError::KeyspaceError("unavailable".to_string())
        )),
        SyncRefusal::Unavailable
    );
    assert_eq!(
        bucket_refusal(GetBucketInfoError::ConversionError(
            ConversionError::FromStrError("broken".to_string())
        )),
        SyncRefusal::Unavailable
    );
    assert_eq!(
        bucket_refusal(GetBucketInfoError::GetBucketInfoFailed),
        SyncRefusal::Unavailable
    );
}

#[test]
fn bounds_listing_prefix() {
    // `notes` must not list `notesbook`.
    assert_eq!(listing_prefix("notes"), "notes/");
    assert_eq!(listing_prefix("notes/"), "notes/");
    assert_eq!(relative_key("notesbook/x", "notes"), None);
    assert_eq!(relative_key("x", "").as_deref(), Some("x"));
}
