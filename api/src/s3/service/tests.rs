//! Tests the S3 adapter for content type, replication targets, watch events and refreshes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::bucket::MAX_REPLICATION_TARGETS;
use super::listing::next_marker_of;
use super::*;
use crate::s3::checksum::UploadChecksumRequest;
use crate::s3::scope::resolve_scope;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    NOTIFICATION_INBOX_KEYSPACE, REALM_CONFIG_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::execution::notification_watch::{
    WatchEventKind, WatchEventMask, WatchInterestEntry, WatchInterestTable, watch_resource_path,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{Actor, PathRestriction};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, VersionKey, bucket_permission_path,
};
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::auth::request_authorization::authorize;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::notifications::watch::subscriptions::create_local_watch;
use aruna_operations::replication::queue::{LiveObligationRecord, live_obligation_key};
use aruna_operations::s3::object::get::ObjectInfo;
use aruna_operations::s3::object::list::ListContinuationToken;
use aruna_operations::s3::object::metadata::{
    QueueRefreshOperation, ReferenceRefresh, refresh_reference_metadata,
};
use aruna_operations::s3::object::put::PutObjectResult;
use aruna_storage::storage;
use futures_util::{StreamExt, stream};
use http::Extensions;
use hyper::{HeaderMap, Method, Uri, body::Bytes};
use s3s::dto::{
    ChecksumType, DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination,
    ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tempfile::TempDir;
use ulid::Ulid;

struct TestState {
    _storage_dir: TempDir,
    context: Arc<DriverContext>,
    bucket: String,
    key: String,
    version_id: Ulid,
    created_by: UserId,
}

fn parser_service(realm_id: RealmId, node_id: NodeId) -> (TempDir, ArunaS3Service) {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let state = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    (
        storage_dir,
        ArunaS3Service {
            state,
            realm_id,
            node_id,
            rocrate_limits: RoCrateLimits::default(),
            completions: Arc::new(completion_registry()),
        },
    )
}

#[test]
fn tracks_content_type() {
    let realm_id = RealmId([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let (_dir, service) = parser_service(realm_id, node_id);
    let metadata = object_metadata(
        HashMap::from([("user".to_string(), "value".to_string())]),
        Some("application/vnd.eln+zip"),
    );

    let fields = service.build_response_fields(None, None, Some(&metadata), None, None, None);

    assert_eq!(
        fields.content_type.as_deref(),
        Some("application/vnd.eln+zip")
    );
    assert_eq!(
        fields.metadata,
        Some(HashMap::from([("user".to_string(), "value".to_string())]))
    );
}

// A holder-backed read has no location, so ObjectInfo supplies its fields.
#[test]
fn info_supplies_fields() {
    let realm_id = RealmId([4u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
    let (_dir, service) = parser_service(realm_id, node_id);
    let info = ObjectInfo {
        size: 42,
        version_created_at: Some(UNIX_EPOCH),
        etag: Some("0123456789abcdef0123456789abcdef".to_string()),
        checksum_type: MultipartChecksumType::FullObject,
        hashes: HashMap::new(),
        composite_hashes: HashMap::new(),
        part_count: None,
    };

    let fields =
        service.build_response_fields(None, Some(&info), Some(&HashMap::new()), None, None, None);

    assert_eq!(fields.content_length, Some(42));
    assert_eq!(fields.last_modified, Some(UNIX_EPOCH.into()));
    assert_eq!(
        fields.e_tag,
        Some(ETag::Strong("0123456789abcdef0123456789abcdef".to_string()))
    );
}

#[test]
fn info_prevents_fallbacks() {
    let realm_id = RealmId([6u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let (_dir, service) = parser_service(realm_id, node_id);
    let location = response_location(UserId::local(Ulid::generate(), realm_id));
    let info = ObjectInfo {
        size: 0,
        version_created_at: None,
        etag: None,
        checksum_type: MultipartChecksumType::FullObject,
        hashes: HashMap::new(),
        composite_hashes: HashMap::new(),
        part_count: None,
    };

    let fields =
        service.build_response_fields(Some(&location), Some(&info), None, None, None, None);

    assert_eq!(fields.content_length, Some(0));
    assert_eq!(fields.e_tag, None);
    assert_eq!(fields.last_modified, None);
}

#[test]
fn reference_fields_present() {
    // Mountpoint refuses a listing entry without ETag or LastModified, so a
    // reference whose source gave neither still answers both, and the same
    // way on every surface.
    let realm_id = RealmId([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let (_dir, service) = parser_service(realm_id, node_id);
    let refreshed = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let bare = aruna_core::structs::execution::source_access::SourceMetadata {
        content_length: 15,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
    };

    let fields =
        service.build_response_fields(None, None, None, Some(&bare), Some(refreshed), None);
    let ETag::Strong(derived) = fields.e_tag.clone().expect("an etag is derived") else {
        panic!("a derived etag is strong");
    };
    assert_eq!(derived.len(), 32);
    assert_eq!(fields.last_modified, Some(refreshed.into()));
    assert_eq!(
        service
            .build_response_fields(None, None, None, Some(&bare), Some(refreshed), None)
            .e_tag,
        fields.e_tag,
        "the derived etag is stable"
    );
    // A GET also passes its read information, which here holds neither.
    let info = ObjectInfo {
        size: 15,
        version_created_at: None,
        etag: None,
        checksum_type: MultipartChecksumType::FullObject,
        hashes: HashMap::new(),
        composite_hashes: HashMap::new(),
        part_count: None,
    };
    let read =
        service.build_response_fields(None, Some(&info), None, Some(&bare), Some(refreshed), None);
    assert_eq!(read.e_tag, fields.e_tag);
    assert_eq!(read.last_modified, fields.last_modified);

    let given = aruna_core::structs::execution::source_access::SourceMetadata {
        etag: Some("\"abc-1\"".to_string()),
        ..bare
    };
    assert_eq!(
        service
            .build_response_fields(None, None, None, Some(&given), Some(refreshed), None)
            .e_tag,
        Some(ETag::Strong("abc-1".to_string()))
    );
}

fn replication_config(bucket: String) -> ReplicationConfiguration {
    ReplicationConfiguration {
        role: "arn:aruna:replication-role".to_string(),
        rules: vec![ReplicationRule {
            delete_marker_replication: None,
            destination: Destination {
                access_control_translation: None,
                account: None,
                bucket,
                encryption_configuration: None,
                metrics: None,
                replication_time: None,
                storage_class: None,
            },
            existing_object_replication: None,
            filter: None,
            id: None,
            prefix: None,
            priority: None,
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }],
    }
}

#[test]
fn accepts_different_bucket() {
    let realm_id = RealmId([71u8; 32]);
    let source_node = iroh::SecretKey::from_bytes(&[72u8; 32]).public();
    let target_node = iroh::SecretKey::from_bytes(&[73u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, source_node);
    let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();

    let parsed = service
        .parse_replication_targets("source-bucket", &replication_config(target.to_string()))
        .unwrap();

    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].0.bucket(), Some("target-bucket"));
    assert_eq!(parsed[0].0.to_string(), target.to_string());
}

#[test]
fn deduplicates_targets() {
    let realm_id = RealmId([77u8; 32]);
    let source_node = iroh::SecretKey::from_bytes(&[78u8; 32]).public();
    let target_node = iroh::SecretKey::from_bytes(&[79u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, source_node);
    let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
    let mut configuration = replication_config(target.to_string());
    let rule = configuration.rules[0].clone();
    configuration.rules.push(rule);

    let parsed = service
        .parse_replication_targets("source-bucket", &configuration)
        .unwrap();

    assert_eq!(parsed.len(), 1);
}

#[test]
fn retains_delete_modes() {
    let realm_id = RealmId([80u8; 32]);
    let source_node = iroh::SecretKey::from_bytes(&[81u8; 32]).public();
    let target_node = iroh::SecretKey::from_bytes(&[82u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, source_node);
    let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
    let mut configuration = replication_config(target.to_string());
    let rule = configuration.rules[0].clone();
    configuration.rules.push(ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(
                DeleteMarkerReplicationStatus::ENABLED,
            )),
        }),
        ..rule
    });

    let parsed = service
        .parse_replication_targets("source-bucket", &configuration)
        .unwrap();

    assert_eq!(parsed.len(), 2);
    assert!(!parsed[0].1);
    assert!(parsed[1].1);
}

#[test]
fn rejects_target_cap() {
    let realm_id = RealmId([83u8; 32]);
    let source_node = iroh::SecretKey::from_bytes(&[84u8; 32]).public();
    let target_node = iroh::SecretKey::from_bytes(&[85u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, source_node);
    let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
    let mut configuration = replication_config(target.to_string());
    let rule = configuration.rules[0].clone();
    configuration
        .rules
        .extend((0..MAX_REPLICATION_TARGETS).map(|_| rule.clone()));

    assert!(
        service
            .parse_replication_targets("source-bucket", &configuration)
            .is_err()
    );
}

#[test]
fn rejects_prefix_target() {
    let realm_id = RealmId([74u8; 32]);
    let source_node = iroh::SecretKey::from_bytes(&[75u8; 32]).public();
    let target_node = iroh::SecretKey::from_bytes(&[76u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, source_node);
    let target =
        ArunaArn::s3_object_prefix(realm_id, target_node, "target-bucket", "prefix").unwrap();

    assert!(
        service
            .parse_replication_targets("source-bucket", &replication_config(target.to_string()),)
            .is_err()
    );
}

#[test]
fn upload_marker_ignored() {
    assert_eq!(parse_upload_marker(None, Some("not-a-ulid")).unwrap(), None);
    assert_eq!(
        parse_upload_marker(Some(""), Some("not-a-ulid")).unwrap(),
        None
    );

    let marker = Ulid::generate();
    assert_eq!(
        parse_upload_marker(Some("key"), Some(&marker.to_string())).unwrap(),
        Some(marker)
    );
    assert!(parse_upload_marker(Some("key"), Some("not-a-ulid")).is_err());
}

#[tokio::test]
async fn put_survives_queue() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([40u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
    let service = ArunaS3Service::new(context, realm_id, node_id).await;
    let bucket = "bucket".to_string();
    let key = "object".to_string();
    let version_id = Ulid::generate();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    write_storage_value(
        &storage_handle,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec(),
        b"not a bucket replication config".to_vec(),
    )
    .await;
    let obligation = LiveObligationRecord::new(
        node_id,
        auth.clone(),
        bucket.clone(),
        key.clone(),
        version_id,
        false,
    );
    let obligation_key = live_obligation_key(&obligation).unwrap();
    write_storage_value(
        &storage_handle,
        REPLICATION_OBLIGATION_KEYSPACE,
        obligation_key.as_ref().to_vec(),
        postcard::to_allocvec(&obligation).unwrap(),
    )
    .await;

    let checksum_request = UploadChecksumRequest {
        expected: Vec::new(),
        response_algorithm: None,
        checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        checksum_type_declared: false,
        composite_part_count: None,
    };
    let response = service
        .put_object_response(
            &checksum_request,
            auth,
            Ulid::generate(),
            bucket.clone(),
            key,
            PutObjectResult {
                location: response_location(user_id),
                version_id,
            },
        )
        .await
        .expect("committed PUT response should not fail on queue kick error");

    assert_eq!(response.output.version_id, Some(version_id.to_string()));
    assert!(
        read_storage_value(
            &storage_handle,
            REPLICATION_OBLIGATION_KEYSPACE,
            obligation_key.as_ref().to_vec(),
        )
        .await
        .is_some(),
        "durable obligation should remain repairable when queue kick fails"
    );
}

async fn build_watch_context(
    realm_id: RealmId,
    secret: [u8; 32],
) -> (TempDir, Arc<DriverContext>, NetHandle) {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    realm_config.ensure_node(net.node_id(), RealmNodeKind::Server);
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    write_storage_value(
        &storage_handle,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_config.to_bytes(&actor).unwrap(),
    )
    .await;
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    (storage_dir, context, net)
}

fn data_uploaded_interest(
    realm_id: RealmId,
    holder: NodeId,
    path_prefix: String,
) -> WatchInterestTable {
    let mut table = WatchInterestTable::default();
    table.insert(
        realm_id,
        holder,
        vec![WatchInterestEntry {
            path_prefix,
            event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        }],
    );
    table
}

async fn install_watch_authorization(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: Ulid,
    watcher: UserId,
) {
    let actor = Actor {
        node_id,
        user_id: watcher,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::default_group_doc(watcher, realm_id, group_id);
    let group = aruna_core::structs::identity::group::Group {
        display_name: "watched".to_string(),
        group_id,
        realm_id,
        owner: watcher,
        roles: group_auth.roles.keys().copied().collect(),
    };
    write_storage_value(
        &context.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &context.storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &context.storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
}

async fn read_watch_rows(context: &DriverContext) -> Vec<NotificationRecord> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1024,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| NotificationRecord::from_bytes(&value).unwrap())
            .collect(),
        other => panic!("unexpected inbox iter event: {other:?}"),
    }
}

#[tokio::test]
async fn put_expands_watch() {
    let realm_id = RealmId([41u8; 32]);
    let (_storage_dir, context, net) = build_watch_context(realm_id, [41u8; 32]).await;
    let holder = net.node_id();

    let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let watcher = UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let watch_prefix = watch_resource_path(group_id, net.node_id(), "bucket", "");
    net.replace_watch_interest(data_uploaded_interest(
        realm_id,
        holder,
        watch_prefix.clone(),
    ));
    install_watch_authorization(&context, realm_id, net.node_id(), group_id, watcher).await;
    create_local_watch(
        &context.storage_handle,
        watcher,
        watch_prefix,
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        0,
    )
    .await
    .expect("watch subscription creates");
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let checksum_request = UploadChecksumRequest {
        expected: Vec::new(),
        response_algorithm: None,
        checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        checksum_type_declared: false,
        composite_part_count: None,
    };

    service
        .put_object_response(
            &checksum_request,
            auth,
            group_id,
            "bucket".to_string(),
            "object".to_string(),
            PutObjectResult {
                location: response_location(user_id),
                version_id: Ulid::generate(),
            },
        )
        .await
        .expect("committed PUT response should succeed");

    let rows = read_watch_rows(context.as_ref()).await;
    assert_eq!(rows.len(), 1, "the local holder expands immediately");
    let record = &rows[0];
    assert_eq!(record.recipient, watcher);
    assert_eq!(record.class, NotificationClass::Transient);
    match &record.kind {
        NotificationKind::DataUploaded {
            path,
            group_id: event_group_id,
            node_id: event_node_id,
            bucket,
            key,
            size_bytes,
            actor_user_id,
        } => {
            assert_eq!(
                path,
                &watch_resource_path(group_id, net.node_id(), "bucket", "object")
            );
            assert_eq!(*event_group_id, group_id);
            assert_eq!(*event_node_id, net.node_id());
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "object");
            // response_location reports a 2-byte blob.
            assert_eq!(*size_bytes, 2);
            assert_eq!(*actor_user_id, user_id);
        }
        other => panic!("unexpected notification kind: {other:?}"),
    }

    net.shutdown().await;
}

#[tokio::test]
async fn anonymous_put_silent() {
    let realm_id = RealmId([42u8; 32]);
    let (_storage_dir, context, net) = build_watch_context(realm_id, [42u8; 32]).await;
    let holder = net.node_id();
    net.replace_watch_interest(data_uploaded_interest(
        realm_id,
        holder,
        watch_resource_path(Ulid::generate(), net.node_id(), "bucket", ""),
    ));

    let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
    let anonymous = UserId::nil(realm_id);
    let group_id = Ulid::generate();
    let auth = AuthContext {
        user_id: anonymous,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let checksum_request = UploadChecksumRequest {
        expected: Vec::new(),
        response_algorithm: None,
        checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        checksum_type_declared: false,
        composite_part_count: None,
    };

    service
        .put_object_response(
            &checksum_request,
            auth,
            group_id,
            "bucket".to_string(),
            "object".to_string(),
            PutObjectResult {
                location: response_location(anonymous),
                version_id: Ulid::generate(),
            },
        )
        .await
        .expect("committed PUT response should succeed");

    assert!(
        read_watch_rows(context.as_ref()).await.is_empty(),
        "an anonymous actor must not emit a watch event"
    );

    net.shutdown().await;
}

#[tokio::test]
async fn refresh_failure_hidden() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let refresh = ReferenceRefresh {
        bucket: "bucket".to_string(),
        key: "reference".to_string(),
        version_id: Ulid::generate(),
        metadata: source_metadata(2, "etag"),
        refreshed_at: UNIX_EPOCH.checked_sub(Duration::from_secs(1)).unwrap(),
    };

    let queue_result = drive(
        QueueRefreshOperation::new(refresh.clone()),
        context.as_ref(),
    )
    .await;
    assert!(
        queue_result.is_err(),
        "test refresh must fail queueing to exercise the callback error path"
    );

    let mut blob = attach_reference_refresh(
        BackendStream::new(stream::iter(vec![Ok::<_, std::io::Error>(
            Bytes::from_static(b"ok"),
        )])),
        context,
        refresh,
    );

    let mut body = Vec::new();
    let mut errors = Vec::new();
    while let Some(result) = blob.next().await {
        match result {
            Ok(bytes) => body.extend_from_slice(&bytes),
            Err(error) => errors.push(error.to_string()),
        }
    }

    assert_eq!(body, b"ok");
    assert!(errors.is_empty(), "unexpected stream errors: {errors:?}");
}

#[tokio::test]
async fn stale_refresh_ignored() {
    let test = setup_state();
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);
    let original_metadata = source_metadata(10, "original");
    write_reference_version(&test, original_metadata.clone(), last_refresh).await;

    refresh_reference_metadata(
        test.context.clone(),
        refresh(
            &test,
            source_metadata(20, "older"),
            UNIX_EPOCH + Duration::from_secs(10),
        ),
    )
    .await
    .unwrap();
    assert_reference_state(&test, &original_metadata, last_refresh).await;

    refresh_reference_metadata(
        test.context.clone(),
        refresh(&test, source_metadata(30, "equal"), last_refresh),
    )
    .await
    .unwrap();

    assert_reference_state(&test, &original_metadata, last_refresh).await;
}

#[tokio::test]
async fn newer_refresh_updates() {
    let test = setup_state();
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);
    let refreshed_at = UNIX_EPOCH + Duration::from_secs(30);
    let new_metadata = source_metadata(20, "newer");
    write_reference_version(&test, source_metadata(10, "original"), last_refresh).await;

    refresh_reference_metadata(
        test.context.clone(),
        refresh(&test, new_metadata.clone(), refreshed_at),
    )
    .await
    .unwrap();

    assert_reference_state(&test, &new_metadata, refreshed_at).await;
}

fn setup_state() -> TestState {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = aruna_core::structs::identity::realm::RealmId([9u8; 32]);

    TestState {
        _storage_dir: storage_dir,
        context,
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Ulid::generate(),
        created_by: UserId::local(Ulid::generate(), realm_id),
    }
}

fn response_location(created_by: UserId) -> BackendLocation {
    let mut hashes = HashMap::new();
    hashes.insert(HASH_MD5.to_string(), vec![1u8; 16]);

    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "bucket/object".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by,
        created_at: UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 2,
        hashes,
    }
}

async fn write_storage_value(
    storage: &storage::StorageHandle,
    keyspace: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) {
    let event = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: keyspace.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// Policy loading fails closed without the realm config document.
async fn write_realm_config(storage: &storage::StorageHandle, realm_id: RealmId, actor: &Actor) {
    write_storage_value(
        storage,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmConfigDocument::default_for_realm(realm_id, Vec::new())
            .to_bytes(actor)
            .unwrap(),
    )
    .await;
}

async fn read_storage_value(
    storage: &storage::StorageHandle,
    keyspace: &str,
    key: Vec<u8>,
) -> Option<byteview::ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };

    value
}

async fn write_reference_version(
    test: &TestState,
    cached_metadata: SourceMetadata,
    last_refresh: SystemTime,
) {
    let version = BlobVersion::reference(
        source_binding(),
        cached_metadata,
        UNIX_EPOCH,
        test.created_by,
        last_refresh,
    );
    let event = test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key(test).into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn assert_reference_state(
    test: &TestState,
    expected_metadata: &SourceMetadata,
    expected_last_refresh: SystemTime,
) {
    let event = test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key(test).into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = event
    else {
        panic!("unexpected version read event: {event:?}");
    };
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    let BlobVersionState::Reference {
        cached_metadata,
        last_refresh,
        ..
    } = version.state
    else {
        panic!("version was not a reference");
    };
    assert_eq!(cached_metadata, *expected_metadata);
    assert_eq!(last_refresh, expected_last_refresh);
}

fn refresh(
    test: &TestState,
    metadata: SourceMetadata,
    refreshed_at: SystemTime,
) -> ReferenceRefresh {
    ReferenceRefresh {
        bucket: test.bucket.clone(),
        key: test.key.clone(),
        version_id: test.version_id,
        metadata,
        refreshed_at,
    }
}

fn version_key(test: &TestState) -> Vec<u8> {
    VersionKey::new(&test.bucket, &test.key, test.version_id)
        .to_bytes()
        .unwrap()
}

fn source_metadata(content_length: u64, etag: &str) -> SourceMetadata {
    SourceMetadata {
        content_length,
        content_type: Some("application/octet-stream".to_string()),
        etag: Some(etag.to_string()),
        last_modified: Some(UNIX_EPOCH + Duration::from_secs(content_length)),
        source_version: None,
    }
}

fn source_binding() -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::new(),
            source_path: "source/path".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
        },
        connector_id: None,
    }
}

async fn write_head(storage: &storage::StorageHandle, bucket: &str, key: &str, version_id: Ulid) {
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn write_materialized_version(
    storage: &storage::StorageHandle,
    bucket: &str,
    key: &str,
    version_id: Ulid,
    hash: [u8; 32],
    created_by: UserId,
    created_at: SystemTime,
    blob_size: u64,
) {
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        created_at,
        created_by,
        None,
    );
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: format!("path/{key}"),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by,
        created_at,
        staging: false,
        partial: false,
        blob_size,
        hashes: HashMap::new(),
    };
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: BlobLocationKey::new(hash, location.backend.clone())
                .to_bytes()
                .into(),
            value: location.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn write_reference_metadata(
    storage: &storage::StorageHandle,
    bucket: &str,
    key: &str,
    version_id: Ulid,
    metadata: SourceMetadata,
    created_at: SystemTime,
    created_by: UserId,
    last_refresh: SystemTime,
) {
    let version = BlobVersion::reference(
        source_binding(),
        metadata,
        created_at,
        created_by,
        last_refresh,
    );
    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: version.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let _ = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await;
}

fn test_user_access(group_id: Ulid, realm_id: RealmId) -> UserAccess {
    UserAccess {
        access_key: "test-key".to_string(),
        user_identity: UserId::local(Ulid::generate(), realm_id),
        group_id,
        secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
        expiry: SystemTime::now() + Duration::from_secs(3600),
        path_restrictions: None,
        issued_by: [0u8; 32],
        revoked_at: None,
    }
}

/// Names a credential restricted to the `allowed` bucket path plus `scope`
/// sees; the group also owns a `hidden` bucket outside that restriction.
async fn visible_buckets(scope: &str) -> Vec<String> {
    let realm_id = RealmId([43u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, node_id);
    let group_id = Ulid::generate();
    let mut user_access = test_user_access(group_id, realm_id);
    let actor = Actor {
        node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::default_group_doc(
        user_access.user_identity,
        realm_id,
        group_id,
    );
    let group = aruna_core::structs::identity::group::Group {
        display_name: "listing".to_string(),
        group_id,
        realm_id,
        owner: user_access.user_identity,
        roles: group_auth.roles.keys().copied().collect(),
    };
    write_realm_config(&service.state.storage_handle, realm_id, &actor).await;
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &service.state.storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    for bucket in ["allowed", "hidden"] {
        write_storage_value(
            &service.state.storage_handle,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            test_bucket_info(group_id, user_access.user_identity)
                .to_bytes()
                .unwrap(),
        )
        .await;
    }
    user_access.path_restrictions = Some(vec![PathRestriction {
        pattern: format!(
            "{}{scope}",
            bucket_permission_path(realm_id, group_id, node_id, "allowed")
        ),
        permission: Permission::READ,
    }]);

    let mut extensions = Extensions::new();
    extensions.insert(user_access);
    extensions.insert(PolicyRequestExtras::operation("s3.ListBuckets"));
    let request = S3Request {
        input: ListBucketsInput::default(),
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    let response = service.list_buckets(request).await.unwrap();
    response
        .output
        .buckets
        .unwrap_or_default()
        .into_iter()
        .filter_map(|bucket| bucket.name)
        .collect()
}

/// A node whose caller holds one role granting read on `study/imaging`
/// only, with a second bucket and sibling keys the role never reaches.
async fn subpath_node() -> (TempDir, ArunaS3Service, UserAccess, Ulid) {
    use std::collections::{HashMap, HashSet};
    let realm_id = RealmId([51u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[51u8; 32]).public();
    let (storage_dir, service) = parser_service(realm_id, node_id);
    let group_id = Ulid::generate();
    let user_access = test_user_access(group_id, realm_id);
    let actor = Actor {
        node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let role_id = Ulid::generate();
    let group_auth = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::from([(
            role_id,
            aruna_core::structs::identity::auth::Role {
                role_id,
                name: "imaging-reader".to_string(),
                permissions: HashMap::from([(
                    format!(
                        "{}/imaging/**",
                        bucket_permission_path(realm_id, group_id, node_id, "study")
                    ),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([user_access.user_identity]),
            },
        )]),
        policies: Vec::new(),
    };
    let group = aruna_core::structs::identity::group::Group {
        display_name: "imaging".to_string(),
        group_id,
        realm_id,
        owner: user_access.user_identity,
        roles: group_auth.roles.keys().copied().collect(),
    };
    write_realm_config(&service.state.storage_handle, realm_id, &actor).await;
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &service.state.storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;
    for bucket in ["study", "other"] {
        write_storage_value(
            &service.state.storage_handle,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            test_bucket_info(group_id, user_access.user_identity)
                .to_bytes()
                .unwrap(),
        )
        .await;
    }
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &[
            "imaging/scan-a",
            "imaging/scan-b",
            "sequencing/reads",
            "notes.txt",
        ],
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;

    (storage_dir, service, user_access, group_id)
}

async fn subpath_request(
    service: &ArunaS3Service,
    user_access: &UserAccess,
    group_id: Ulid,
    prefix: Option<&str>,
) -> S3Request<ListObjectsV2Input> {
    let scope = resolve_scope(
        &service.state,
        user_access,
        &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
    )
    .await
    .unwrap();
    assert!(!scope.is_empty());
    let mut extensions = Extensions::new();
    extensions.insert(user_access.clone());
    extensions.insert(test_bucket_info(group_id, user_access.user_identity));
    extensions.insert(scope);
    list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "study".to_string(),
            delimiter: Some("/".to_string()),
            max_keys: Some(10),
            prefix: prefix.map(str::to_string),
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn subpath_sees_bucket() {
    // Only the bucket holding the granted folder may appear. The access hook
    // adds no scope to ListBuckets, which names no bucket.
    let (_storage_dir, service, user_access, _group_id) = subpath_node().await;
    assert_eq!(listed_buckets(&service, &user_access).await, vec!["study"]);
}

/// Adds a realm role that denies the caller the whole group data subtree.
async fn write_realm_deny(service: &ArunaS3Service, user_access: &UserAccess) {
    use std::collections::{HashMap, HashSet};
    let realm_id = service.realm_id;
    let actor = Actor {
        node_id: service.node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let role_id = Ulid::generate();
    realm_auth.roles.insert(
        role_id,
        aruna_core::structs::identity::auth::Role {
            role_id,
            name: "data-deny".to_string(),
            permissions: HashMap::from([(format!("/{realm_id}/g/**"), Permission::DENY)]),
            assigned_users: HashSet::from([user_access.user_identity]),
        },
    );
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        realm_auth.to_bytes(&actor).unwrap(),
    )
    .await;
}

async fn write_deny_policy(service: &ArunaS3Service, user_access: &UserAccess) {
    let realm_id = service.realm_id;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config
        .request_policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "no-reads".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'read'".to_string(),
            enabled: true,
        });
    let actor = Actor {
        node_id: service.node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    write_storage_value(
        &service.state.storage_handle,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor).unwrap(),
    )
    .await;
}

async fn listed_buckets(service: &ArunaS3Service, user_access: &UserAccess) -> Vec<String> {
    let mut extensions = Extensions::new();
    extensions.insert(user_access.clone());
    extensions.insert(PolicyRequestExtras::operation("s3.ListBuckets"));
    let request = S3Request {
        input: ListBucketsInput::default(),
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };
    service
        .list_buckets(request)
        .await
        .unwrap()
        .output
        .buckets
        .unwrap_or_default()
        .into_iter()
        .filter_map(|bucket| bucket.name)
        .collect()
}

#[tokio::test]
async fn deny_hides_subpath() {
    // A realm deny outranks the group role that grants the folder, so no
    // listing, head or bucket visibility survives it.
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    write_realm_deny(&service, &user_access).await;

    let scope = resolve_scope(
        &service.state,
        &user_access,
        &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
    )
    .await
    .unwrap();
    assert!(scope.is_empty());
    assert!(listed_buckets(&service, &user_access).await.is_empty());
}

/// Replaces the seeded folder role with the owner's default roles, so the
/// caller reads the whole bucket and only a policy can refuse it.
async fn grant_group_owner(service: &ArunaS3Service, user_access: &UserAccess, group_id: Ulid) {
    let actor = Actor {
        node_id: service.node_id,
        user_id: user_access.user_identity,
        realm_id: service.realm_id,
    };
    write_storage_value(
        &service.state.storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        GroupAuthorizationDocument::default_group_doc(
            user_access.user_identity,
            service.realm_id,
            group_id,
        )
        .to_bytes(&actor)
        .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn policy_hides_bucket() {
    // A policy denial is the verdict itself, never a reason to fall back on
    // the caller's role subtrees.
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    grant_group_owner(&service, &user_access, group_id).await;
    assert!(!listed_buckets(&service, &user_access).await.is_empty());

    write_deny_policy(&service, &user_access).await;
    assert!(listed_buckets(&service, &user_access).await.is_empty());
}

#[tokio::test]
async fn subpath_hides_siblings() {
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;

    let request = subpath_request(&service, &user_access, group_id, None).await;
    let output = service.list_objects_v2(request).await.unwrap().output;
    let prefixes: Vec<String> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();
    assert_eq!(prefixes, vec!["imaging/"]);
    assert!(output.contents.unwrap_or_default().is_empty());
    assert_eq!(output.is_truncated, Some(false));

    let request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
    let output = service.list_objects_v2(request).await.unwrap().output;
    let keys: Vec<String> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    assert_eq!(keys, vec!["imaging/scan-a", "imaging/scan-b"]);
}

async fn paged_request(
    service: &ArunaS3Service,
    user_access: &UserAccess,
    group_id: Ulid,
    token: Option<String>,
) -> S3Request<ListObjectsV2Input> {
    let scope = resolve_scope(
        &service.state,
        user_access,
        &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
    )
    .await
    .unwrap();
    let mut extensions = Extensions::new();
    extensions.insert(user_access.clone());
    extensions.insert(test_bucket_info(group_id, user_access.user_identity));
    extensions.insert(scope);
    list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "study".to_string(),
            max_keys: Some(1),
            continuation_token: token,
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn marker_stays_scoped() {
    // Paging one key at a time crosses keys the caller may not see, so no
    // token may name one and the last page must admit it is the last.
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    let mut token = None;
    let mut keys: Vec<String> = Vec::new();

    for _ in 0..4 {
        let request = paged_request(&service, &user_access, group_id, token.clone()).await;
        let output = service.list_objects_v2(request).await.unwrap().output;
        keys.extend(
            output
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|object| object.key),
        );
        token = output.next_continuation_token;
        assert_eq!(output.is_truncated, Some(token.is_some()));
        let Some(encoded) = token.clone() else {
            break;
        };
        let decoded = ArunaS3Service::decode_list_token(Some(&encoded))
            .unwrap()
            .unwrap();
        let head = BlobHeadKey::from_bytes(&decoded.last_key).unwrap();
        assert!(head.key.starts_with("imaging/"), "leaked {}", head.key);
    }

    assert_eq!(keys, vec!["imaging/scan-a", "imaging/scan-b"]);
    assert!(token.is_none());
}

#[tokio::test]
async fn exact_listing_scope() {
    let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
    grant_group_owner(&service, &user_access, group_id).await;
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &["imaging", "imaging/private/key"],
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let root = bucket_permission_path(service.realm_id, group_id, service.node_id, "study");
    for (suffix, expected) in [("/imaging", vec!["imaging"]), ("", vec![])] {
        user_access.path_restrictions = Some(vec![PathRestriction {
            pattern: format!("{root}{suffix}"),
            permission: Permission::READ,
        }]);
        for delimiter in [None, Some("/".to_string())] {
            let mut request = subpath_request(&service, &user_access, group_id, None).await;
            request.input.delimiter = delimiter;
            let output = service.list_objects_v2(request).await.unwrap().output;
            let keys: Vec<_> = output
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|object| object.key)
                .collect();
            assert_eq!(keys, expected);
            assert!(output.common_prefixes.unwrap_or_default().is_empty());
        }
    }
}

#[tokio::test]
async fn denied_listing_keys() {
    let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &[
            "imaging/private/key",
            "imaging/public/secret.txt",
            "imaging/public/open.txt",
            "imaging/hidden/secret.txt",
        ],
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let root = bucket_permission_path(service.realm_id, group_id, service.node_id, "study");
    user_access.path_restrictions = Some(
        [
            ("imaging/**", Permission::READ),
            ("imaging/private/**", Permission::DENY),
            ("imaging/*/secret*", Permission::DENY),
        ]
        .into_iter()
        .map(|(key, permission)| PathRestriction {
            pattern: format!("{root}/{key}"),
            permission,
        })
        .collect(),
    );
    let mut request = subpath_request(&service, &user_access, group_id, None).await;
    request.input.delimiter = None;
    let output = service.list_objects_v2(request).await.unwrap().output;
    let keys: Vec<_> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    assert_eq!(
        keys,
        vec![
            "imaging/public/open.txt",
            "imaging/scan-a",
            "imaging/scan-b"
        ]
    );
    let request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
    let output = service.list_objects_v2(request).await.unwrap().output;
    let prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();
    assert_eq!(prefixes, vec!["imaging/public/"]);
}

#[tokio::test]
async fn scoped_scan_bounded() {
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    let keys: Vec<_> = (0..101).map(|index| format!("hidden/{index:03}")).collect();
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let request = paged_request(&service, &user_access, group_id, None).await;
    let error = service.list_objects_v2(request).await.unwrap_err();
    assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
}

#[tokio::test]
async fn prefix_pages_progress() {
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    let keys: Vec<_> = (0..101)
        .map(|index| format!("imaging/{index:03}/key"))
        .collect();
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let mut request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
    request.input.max_keys = Some(0);
    let output = service.list_objects_v2(request).await.unwrap().output;
    assert!(output.contents.unwrap_or_default().is_empty());
    assert!(output.common_prefixes.unwrap_or_default().is_empty());
    assert!(output.next_continuation_token.is_none());
    let mut token = None;
    let mut prefixes = Vec::new();
    for _ in 0..3 {
        let mut request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
        request.input.max_keys = None;
        request.input.continuation_token = token.take();
        let output = service.list_objects_v2(request).await.unwrap().output;
        prefixes.extend(
            output
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .filter_map(|prefix| prefix.prefix),
        );
        token = output.next_continuation_token;
        if token.is_none() {
            break;
        }
    }
    assert!(token.is_none());
    assert_eq!(
        prefixes,
        (0..101)
            .map(|index| format!("imaging/{index:03}/"))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn prefix_token_precedence() {
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &["imaging/a/key", "imaging/b/key"],
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let mut request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
    request.input.start_after = Some("imaging/z".to_string());
    let token =
        crate::s3::service::listing::scoped_marker("study", Some("imaging/0"), None).unwrap();
    request.input.continuation_token = ArunaS3Service::encode_list_token(token.as_ref()).unwrap();
    let output = service.list_objects_v2(request).await.unwrap().output;
    let prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();
    assert_eq!(prefixes, vec!["imaging/a/", "imaging/b/"]);
}

#[tokio::test]
async fn subpath_refuses_sibling() {
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;

    let request = subpath_request(&service, &user_access, group_id, Some("sequencing/")).await;
    let error = service.list_objects_v2(request).await.unwrap_err();
    assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
}

/// Narrows the caller to `imaging/2026` and seeds keys around that folder.
async fn restrict_to_year(service: &ArunaS3Service, user_access: &mut UserAccess, group_id: Ulid) {
    seed_materialized_keys(
        &service.state.storage_handle,
        "study",
        &["imaging/2025/x", "imaging/2026/y", "imagery/z"],
        user_access.user_identity,
        UNIX_EPOCH,
    )
    .await;
    let root = bucket_permission_path(service.realm_id, group_id, service.node_id, "study");
    user_access.path_restrictions = Some(vec![PathRestriction {
        pattern: format!("{root}/imaging/2026/**"),
        permission: Permission::READ,
    }]);
}

async fn scoped_listing(
    service: &ArunaS3Service,
    user_access: &UserAccess,
    group_id: Ulid,
    prefix: &str,
    delimiter: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let mut request = subpath_request(service, user_access, group_id, Some(prefix)).await;
    request.input.delimiter = delimiter.map(str::to_string);
    let output = service.list_objects_v2(request).await.unwrap().output;
    let keys = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    let prefixes = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();
    (keys, prefixes)
}

#[tokio::test]
async fn partial_prefix_lists() {
    // Shell completion lists a partial segment, as in `aws s3 ls s3://study/imag`.
    let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
    let listed = scoped_listing(&service, &user_access, group_id, "imag", Some("/")).await;
    assert_eq!(listed, (vec![], vec!["imaging/".to_string()]));

    restrict_to_year(&service, &mut user_access, group_id).await;
    let listed = scoped_listing(&service, &user_access, group_id, "imaging/20", Some("/")).await;
    assert_eq!(listed, (vec![], vec!["imaging/2026/".to_string()]));
}

#[tokio::test]
async fn partial_prefix_filtered() {
    // A partial prefix reaches past the allowed folder, so keys stay filtered one by one.
    let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
    restrict_to_year(&service, &mut user_access, group_id).await;
    for prefix in ["imag", "imaging/20"] {
        let listed = scoped_listing(&service, &user_access, group_id, prefix, None).await;
        assert_eq!(listed, (vec!["imaging/2026/y".to_string()], vec![]));
    }
}

#[tokio::test]
async fn object_path_decides() {
    // Object reads stay authorized at their own path, inside and outside
    // the granted folder.
    let (_storage_dir, service, user_access, group_id) = subpath_node().await;
    let auth_context = AuthContext {
        user_id: user_access.user_identity,
        realm_id: service.realm_id,
        path_restrictions: None,
        session: None,
    };
    let object_path = |key: &str| {
        aruna_core::structs::storage::blob::object_permission_path(
            service.realm_id,
            group_id,
            service.node_id,
            "study",
            key,
        )
    };

    assert!(
        authorize(
            &service.state,
            service.realm_id,
            &auth_context,
            &object_path("imaging/scan-a"),
            &Permission::READ,
            PolicyRequestExtras::operation("s3.GetObject"),
        )
        .await
        .is_ok()
    );
    assert!(
        authorize(
            &service.state,
            service.realm_id,
            &auth_context,
            &object_path("sequencing/reads"),
            &Permission::READ,
            PolicyRequestExtras::operation("s3.GetObject"),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn filters_bucket_scope() {
    assert_eq!(visible_buckets("").await, vec!["allowed"]);
}

#[tokio::test]
async fn lists_prefix_scope() {
    // A credential scoped to a prefix inside a bucket must still see that
    // bucket, and only that one, in the listing.
    assert_eq!(visible_buckets("/logs/**").await, vec!["allowed"]);
}

fn test_bucket_info(group_id: Ulid, created_by: UserId) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: UNIX_EPOCH,
        created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    }
}

async fn setup_copy_authorization(
    same_group: bool,
    public: bool,
) -> (TempDir, ArunaS3Service, UserAccess, BucketInfo) {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([36u8; 32]);
    let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
    let credential_group_id = Ulid::generate();
    let source_group_id = if same_group {
        credential_group_id
    } else {
        Ulid::generate()
    };
    let user_access = test_user_access(credential_group_id, realm_id);
    let actor = Actor {
        node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let mut source_auth = GroupAuthorizationDocument::default_group_doc(
        user_access.user_identity,
        realm_id,
        source_group_id,
    );
    if public {
        source_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer")
            .unwrap()
            .assigned_users
            .insert(UserId::nil(realm_id));
    }

    let source_group = aruna_core::structs::identity::group::Group {
        display_name: "source".to_string(),
        group_id: source_group_id,
        realm_id,
        owner: user_access.user_identity,
        roles: source_auth.roles.keys().copied().collect(),
    };

    write_realm_config(&storage_handle, realm_id, &actor).await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        source_group_id.to_bytes().to_vec(),
        source_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        source_group_id.to_bytes().to_vec(),
        source_group.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        S3_BUCKET_KEYSPACE,
        b"source".to_vec(),
        test_bucket_info(source_group_id, user_access.user_identity)
            .to_bytes()
            .unwrap(),
    )
    .await;

    let destination_info = test_bucket_info(credential_group_id, user_access.user_identity);
    (
        storage_dir,
        ArunaS3Service::new(context, realm_id, node_id).await,
        user_access,
        destination_info,
    )
}

fn test_copy_request<T>(
    input: T,
    user_access: UserAccess,
    bucket_info: BucketInfo,
) -> S3Request<T> {
    let mut extensions = Extensions::new();
    extensions.insert(user_access);
    extensions.insert(bucket_info);
    extensions.insert(PolicyRequestExtras::rest());
    S3Request {
        input,
        method: Method::PUT,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
async fn copy_scopes_authorization() {
    for (same_group, public, allowed) in [
        (true, false, true),
        (false, true, true),
        (false, false, false),
    ] {
        let (_storage_dir, service, user_access, bucket_info) =
            setup_copy_authorization(same_group, public).await;
        let input = CopyObjectInput::builder()
            .bucket("destination".to_string())
            .key("copied".to_string())
            .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
            .metadata_directive(Some(MetadataDirective::from_static(
                MetadataDirective::REPLACE,
            )))
            .build()
            .unwrap();
        let error = service
            .copy_object(test_copy_request(input, user_access, bucket_info))
            .await
            .unwrap_err();
        // Passing source authorization reaches the copy itself, which fails
        // on the absent source object.
        let expected = if allowed {
            S3ErrorCode::NoSuchKey
        } else {
            S3ErrorCode::AccessDenied
        };
        assert_eq!(
            *error.code(),
            expected,
            "same_group={same_group}, public={public}"
        );
    }
}

#[tokio::test]
async fn part_copy_scopes() {
    for (same_group, public, allowed) in [
        (true, false, true),
        (false, true, true),
        (false, false, false),
    ] {
        let (_storage_dir, service, user_access, bucket_info) =
            setup_copy_authorization(same_group, public).await;
        let input = UploadPartCopyInput::builder()
            .bucket("destination".to_string())
            .key("copied".to_string())
            .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
            .upload_id(Ulid::generate().to_string())
            .part_number(1)
            .build()
            .unwrap();
        let error = service
            .upload_part_copy(test_copy_request(input, user_access, bucket_info))
            .await
            .unwrap_err();
        let expected = if allowed {
            S3ErrorCode::NoSuchUpload
        } else {
            S3ErrorCode::AccessDenied
        };
        assert_eq!(
            *error.code(),
            expected,
            "same_group={same_group}, public={public}"
        );
    }
}

#[tokio::test]
async fn source_policy_denied() {
    // A group deny policy on the source path blocks the copy even when RBAC
    // and destination write are allowed.
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([37u8; 32]);
    let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
    let group_id = Ulid::generate();
    let user_access = test_user_access(group_id, realm_id);
    let actor = Actor {
        node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let mut source_auth = GroupAuthorizationDocument::default_group_doc(
        user_access.user_identity,
        realm_id,
        group_id,
    );
    source_auth.policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::generate(),
        name: "no-reads".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'read'".to_string(),
        enabled: true,
    }];
    let source_group = aruna_core::structs::identity::group::Group {
        display_name: "src".to_string(),
        group_id,
        realm_id,
        owner: user_access.user_identity,
        roles: source_auth.roles.keys().copied().collect(),
    };
    write_realm_config(&storage_handle, realm_id, &actor).await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        source_auth.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        source_group.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        S3_BUCKET_KEYSPACE,
        b"source".to_vec(),
        test_bucket_info(group_id, user_access.user_identity)
            .to_bytes()
            .unwrap(),
    )
    .await;

    let service = ArunaS3Service::new(context, realm_id, node_id).await;
    let input = CopyObjectInput::builder()
        .bucket("destination".to_string())
        .key("copied".to_string())
        .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
        .metadata_directive(Some(MetadataDirective::from_static(
            MetadataDirective::REPLACE,
        )))
        .build()
        .unwrap();
    let bucket_info = test_bucket_info(group_id, user_access.user_identity);
    let error = service
        .copy_object(test_copy_request(input, user_access, bucket_info))
        .await
        .unwrap_err();
    assert_eq!(*error.code(), S3ErrorCode::AccessDenied);
}

#[tokio::test]
async fn delete_uses_context() {
    // The per-object policy must see the real query parameters and allowlisted
    // headers the access hook captured, not an empty operation-only context.
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([38u8; 32]);
    let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
    let group_id = Ulid::generate();
    let user_access = test_user_access(group_id, realm_id);
    let actor = Actor {
        node_id,
        user_id: user_access.user_identity,
        realm_id,
    };
    let mut auth_doc = GroupAuthorizationDocument::default_group_doc(
        user_access.user_identity,
        realm_id,
        group_id,
    );
    auth_doc.policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::generate(),
        name: "context-deny".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "('mode' in params && params['mode'] == 'purge') \
                || ('x-amz-meta-env' in headers && headers['x-amz-meta-env'] == 'prod')"
            .to_string(),
        enabled: true,
    }];
    let group = aruna_core::structs::identity::group::Group {
        display_name: "g".to_string(),
        group_id,
        realm_id,
        owner: user_access.user_identity,
        roles: auth_doc.roles.keys().copied().collect(),
    };
    write_realm_config(&storage_handle, realm_id, &actor).await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        auth_doc.to_bytes(&actor).unwrap(),
    )
    .await;
    write_storage_value(
        &storage_handle,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group.to_bytes(&actor).unwrap(),
    )
    .await;

    let service = ArunaS3Service::new(context, realm_id, node_id).await;
    let param_extras = PolicyRequestExtras {
        operation: "s3.DeleteObjects".to_string(),
        params: std::collections::BTreeMap::from([("mode".to_string(), "purge".to_string())]),
        headers: std::collections::BTreeMap::new(),
        body: None,
    };
    let header_extras = PolicyRequestExtras {
        operation: "s3.DeleteObjects".to_string(),
        params: std::collections::BTreeMap::new(),
        headers: std::collections::BTreeMap::from([(
            "x-amz-meta-env".to_string(),
            "prod".to_string(),
        )]),
        body: None,
    };

    for extras in [param_extras, header_extras] {
        let delete = s3s::dto::Delete {
            objects: vec![
                s3s::dto::ObjectIdentifier {
                    key: "a".to_string(),
                    ..Default::default()
                },
                s3s::dto::ObjectIdentifier {
                    key: "b".to_string(),
                    ..Default::default()
                },
            ],
            quiet: None,
        };
        let input = DeleteObjectsInput::builder()
            .bucket("bucket".to_string())
            .delete(delete)
            .build()
            .unwrap();
        let mut extensions = Extensions::new();
        extensions.insert(user_access.clone());
        extensions.insert(DeleteObjectsBody::default());
        extensions.insert(extras);
        let mut headers = HeaderMap::new();
        headers.insert("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==".parse().unwrap());
        let request = S3Request {
            input,
            method: Method::POST,
            uri: Uri::from_static("/"),
            headers,
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let output = service.delete_objects(request).await.unwrap();
        let errors = output.output.errors.unwrap_or_default();
        assert_eq!(errors.len(), 2);
        assert!(
            errors
                .iter()
                .all(|error| error.code.as_deref() == Some("AccessDenied"))
        );
    }
}

async fn seed_materialized_keys(
    storage_handle: &storage::StorageHandle,
    bucket: &str,
    keys: &[&str],
    created_by: UserId,
    created_at: SystemTime,
) {
    for key in keys {
        let version_id = Ulid::generate();
        let hash = [key.len() as u8; 32];
        write_head(storage_handle, bucket, key, version_id).await;
        write_materialized_version(
            storage_handle,
            bucket,
            key,
            version_id,
            hash,
            created_by,
            created_at,
            42,
        )
        .await;
    }
}

fn list_request(
    extensions: Extensions,
    input: ListObjectsV2Input,
) -> S3Request<ListObjectsV2Input> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

#[tokio::test]
async fn delimiter_groups_results() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([2u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH;

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["dir-a/1", "dir-a/2", "dir-b/1", "root.txt"],
        created_by,
        created_at,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: Some("/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    let mut common_prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|cp| cp.prefix)
        .collect();
    common_prefixes.sort();
    assert_eq!(common_prefixes, vec!["dir-a/", "dir-b/"]);

    let mut contents: Vec<_> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|obj| obj.key)
        .collect();
    contents.sort();
    assert_eq!(contents, vec!["root.txt"]);

    assert_eq!(output.is_truncated, Some(false));
}

#[tokio::test]
async fn delimiter_paginates() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([3u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH;

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a/1", "a/2", "b.txt"],
        created_by,
        created_at,
    )
    .await;

    let mut continuation_token = None;
    let mut all_keys = Vec::new();
    let mut all_prefixes = Vec::new();
    let mut total_pages = 0;

    loop {
        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = list_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(1),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        total_pages += 1;
        for obj in output.contents.unwrap_or_default() {
            if let Some(key) = obj.key {
                all_keys.push(key);
            }
        }
        for cp in output.common_prefixes.unwrap_or_default() {
            if let Some(prefix) = cp.prefix {
                all_prefixes.push(prefix);
            }
        }

        continuation_token = output.next_continuation_token;
        if continuation_token.is_none() {
            break;
        }
    }

    assert_eq!(all_prefixes, vec!["a/"]);
    assert_eq!(all_keys, vec!["b.txt"]);
    assert_eq!(total_pages, 2);
}

#[test]
fn marker_skips_group() {
    // A delimited marker names an already returned common prefix, so the
    // next page must skip that whole group instead of re-listing it.
    let token = marker_continuation_token("bucket", Some("a/"), None, Some("/"))
        .unwrap()
        .expect("delimited marker must resume past its group");
    assert_eq!(token.last_common_prefix.as_deref(), Some("a/"));
    assert_eq!(
        BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
        "a/".to_string()
    );
}

#[test]
fn plain_marker_preserved() {
    assert!(
        marker_continuation_token("bucket", Some("a/b.txt"), None, None)
            .unwrap()
            .is_none()
    );
    assert!(
        marker_continuation_token("bucket", None, None, Some("/"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn next_marker_group() {
    let token = ListContinuationToken {
        last_key: BlobHeadKey::object_prefix("bucket", "a/z.txt").unwrap(),
        last_common_prefix: Some("a/".to_string()),
    };
    assert_eq!(next_marker_of(&token).as_deref(), Some("a/"));

    let token = ListContinuationToken {
        last_key: BlobHeadKey::object_prefix("bucket", "b.txt").unwrap(),
        last_common_prefix: None,
    };
    assert_eq!(next_marker_of(&token).as_deref(), Some("b.txt"));
}

#[test]
fn marker_rescues_page() {
    // An undelimited page that filtered every key it scanned is truncated with
    // no `<Key>` to resume from, so it must carry the token-derived marker.
    let token = ListContinuationToken {
        last_key: BlobHeadKey::object_prefix("bucket", "b.txt").unwrap(),
        last_common_prefix: None,
    };

    assert_eq!(
        next_marker_for(None, Some(&token), true).as_deref(),
        Some("b.txt")
    );
    // A page with contents resumes from its last key: S3 sends no NextMarker.
    assert_eq!(next_marker_for(None, Some(&token), false), None);
    // A delimited page may end on a common prefix, so it always reports one.
    assert_eq!(
        next_marker_for(Some("/"), Some(&token), false).as_deref(),
        Some("b.txt")
    );
    // A complete listing has nothing to resume.
    assert_eq!(next_marker_for(None, None, true), None);
}

#[tokio::test]
async fn prefix_page_complete() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([33u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);

    let service =
        ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a/1", "a/2"],
        created_by,
        UNIX_EPOCH,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: Some("/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(1),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    let prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();

    assert_eq!(prefixes, vec!["a/"]);
    assert_eq!(output.contents.unwrap_or_default().len(), 0);
    assert_eq!(output.key_count, Some(1));
    assert_eq!(output.is_truncated, Some(false));
    assert!(output.next_continuation_token.is_none());
}

#[tokio::test]
async fn listing_counts_entries() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([34u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a.txt", "b/1", "c.txt"],
        created_by,
        UNIX_EPOCH,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let first_response = service
        .list_objects_v2(list_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(2),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        ))
        .await
        .unwrap()
        .output;

    let first_keys: Vec<_> = first_response
        .contents
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    let first_prefixes: Vec<_> = first_response
        .common_prefixes
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();

    assert_eq!(first_keys, vec!["a.txt"]);
    assert_eq!(first_prefixes, vec!["b/"]);
    assert_eq!(first_response.key_count, Some(2));
    assert_eq!(first_response.is_truncated, Some(true));

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let second_response = service
        .list_objects_v2(list_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: first_response.next_continuation_token,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(2),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        ))
        .await
        .unwrap()
        .output;

    let second_keys: Vec<_> = second_response
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    let second_prefixes: Vec<_> = second_response
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();

    assert_eq!(second_keys, vec!["c.txt"]);
    assert!(second_prefixes.is_empty());
    assert_eq!(second_response.key_count, Some(1));
    assert_eq!(second_response.is_truncated, Some(false));
}

#[tokio::test]
async fn large_group_collapses() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([4u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH + Duration::from_secs(1);

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    // Seed 305 keys under "dir/" so the delimiter collapses them into
    // one prefix; the scan seeks past the group instead of paging it.
    for i in 0..305 {
        let key = format!("dir/key_{:04}", i);
        let version_id = Ulid::generate();
        let hash = [i as u8; 32];
        write_head(&storage_handle, "bucket", &key, version_id).await;
        write_materialized_version(
            &storage_handle,
            "bucket",
            &key,
            version_id,
            hash,
            created_by,
            created_at,
            1,
        )
        .await;
    }

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: Some("/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(2),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    assert_eq!(output.is_truncated, Some(false));
    assert!(
        output.next_continuation_token.is_none(),
        "single visible entry must not be truncated"
    );
    assert_eq!(output.key_count, Some(1));

    let common_prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|cp| cp.prefix)
        .collect();
    assert_eq!(common_prefixes, vec!["dir/"]);
}

#[tokio::test]
async fn reference_returns_metadata() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([5u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH + Duration::from_secs(5);
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    let metadata = SourceMetadata {
        content_length: 100,
        content_type: Some("text/csv".to_string()),
        etag: Some("ref-etag-value".to_string()),
        last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
        source_version: None,
    };

    let version_id = Ulid::generate();
    write_reference_metadata(
        &storage_handle,
        "bucket",
        "ref-object",
        version_id,
        metadata.clone(),
        created_at,
        created_by,
        last_refresh,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = S3Request {
        input: ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    };

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    let objects: Vec<_> = output.contents.unwrap_or_default();
    assert_eq!(objects.len(), 1);

    let obj = &objects[0];
    assert_eq!(obj.key.as_deref(), Some("ref-object"));
    assert_eq!(obj.size, Some(100));

    let expected_etag = Some(ETag::Strong("ref-etag-value".to_string()));
    assert_eq!(obj.e_tag, expected_etag);

    assert_eq!(
        obj.last_modified,
        Some((UNIX_EPOCH + Duration::from_secs(10)).into())
    );

    assert_eq!(output.is_truncated, Some(false));
}

/// Mountpoint reads ETag and Last-Modified from HEAD. A reference answers from
/// its recorded observation instead of opening a source read.
#[tokio::test]
async fn bare_reference_head() {
    let realm_id = RealmId([6u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let (_storage_dir, service) = parser_service(realm_id, node_id);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let last_refresh = UNIX_EPOCH + Duration::from_secs(20);
    let bare = SourceMetadata {
        content_length: 15,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
    };
    write_reference_metadata(
        &service.state.storage_handle,
        "bucket",
        "ref-object",
        Ulid::generate(),
        bare.clone(),
        UNIX_EPOCH,
        created_by,
        last_refresh,
    )
    .await;
    let derived = service
        .build_response_fields(None, None, None, Some(&bare), Some(last_refresh), None)
        .e_tag;
    assert!(derived.is_some());
    let extensions = || {
        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));
        extensions
    };

    let head = service
        .head_object(S3Request {
            input: HeadObjectInput {
                bucket: "bucket".to_string(),
                key: "ref-object".to_string(),
                ..Default::default()
            },
            method: Method::HEAD,
            uri: Uri::from_static("/bucket/ref-object"),
            headers: HeaderMap::new(),
            extensions: extensions(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        })
        .await
        .unwrap()
        .output;
    assert_eq!(head.e_tag, derived);
    assert_eq!(head.last_modified, Some(last_refresh.into()));

    let attributes = service
        .get_object_attributes(S3Request {
            input: GetObjectAttributesInput {
                bucket: "bucket".to_string(),
                key: "ref-object".to_string(),
                object_attributes: vec![s3s::dto::ObjectAttributes::from_static(
                    s3s::dto::ObjectAttributes::ETAG,
                )],
                ..Default::default()
            },
            method: Method::GET,
            uri: Uri::from_static("/bucket/ref-object?attributes"),
            headers: HeaderMap::new(),
            extensions: extensions(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        })
        .await
        .unwrap()
        .output;
    assert_eq!(attributes.e_tag, derived);
}

#[tokio::test]
async fn zero_max_honored() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([6u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH + Duration::from_secs(5);

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["alpha"],
        created_by,
        created_at,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(0),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    assert_eq!(output.max_keys, Some(0));
    assert_eq!(output.key_count, Some(0));
    assert_eq!(output.is_truncated, Some(false));
    assert_eq!(output.contents.unwrap_or_default().len(), 0);
    assert_eq!(output.common_prefixes.unwrap_or_default().len(), 0);
    assert!(output.next_continuation_token.is_none());
}

#[tokio::test]
async fn start_after_prefixes() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([7u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);
    let created_at = UNIX_EPOCH;

    let service = ArunaS3Service::new(
        context.clone(),
        realm_id,
        NodeId::from_bytes(&[0u8; 32]).unwrap(),
    )
    .await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["dir-a/1", "dir-b/1", "root.txt"],
        created_by,
        created_at,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: Some("/".to_string()),
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: Some("dir-b/".to_string()),
        },
    );

    let response = service.list_objects_v2(req).await.unwrap();
    let output = response.output;

    let common_prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|cp| cp.prefix)
        .collect();
    let contents: Vec<_> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|obj| obj.key)
        .collect();

    assert_eq!(common_prefixes, vec!["dir-b/"]);
    assert_eq!(contents, vec!["root.txt"]);
    assert_eq!(output.key_count, Some(2));
}

#[tokio::test]
async fn listing_requires_access() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([8u8; 32]);
    let service =
        ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

    let req = list_request(
        Extensions::new(),
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );

    let err = service.list_objects_v2(req).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::UnexpectedContent);
}

#[tokio::test]
async fn max_keys_validated() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([35u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);

    let service =
        ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a", "b"],
        created_by,
        UNIX_EPOCH,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let input = ListObjectsV2Input {
        bucket: "bucket".to_string(),
        continuation_token: None,
        delimiter: None,
        encoding_type: None,
        expected_bucket_owner: None,
        fetch_owner: None,
        max_keys: Some(5000),
        optional_object_attributes: None,
        prefix: None,
        request_payer: None,
        start_after: None,
    };
    let req = list_request(extensions.clone(), input.clone());
    let output = service.list_objects_v2(req).await.unwrap().output;
    assert_eq!(output.max_keys, Some(1000));
    assert_eq!(output.key_count, Some(2));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            max_keys: Some(-1),
            ..input
        },
    );
    let err = service.list_objects_v2(req).await.unwrap_err();
    assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn listing_applies_encoding() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([36u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);

    let service =
        ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

    seed_materialized_keys(
        &storage_handle,
        "bucket",
        &["a b+c.txt", "d e/f.txt"],
        created_by,
        UNIX_EPOCH,
    )
    .await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: Some("/".to_string()),
            encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: Some("a".to_string()),
        },
    );
    let output = service.list_objects_v2(req).await.unwrap().output;

    let keys: Vec<_> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.key)
        .collect();
    let prefixes: Vec<_> = output
        .common_prefixes
        .unwrap_or_default()
        .into_iter()
        .filter_map(|prefix| prefix.prefix)
        .collect();

    assert_eq!(keys, vec!["a%20b%2Bc.txt"]);
    assert_eq!(prefixes, vec!["d%20e%2F"]);
    assert_eq!(output.delimiter.as_deref(), Some("%2F"));
    assert_eq!(output.start_after.as_deref(), Some("a"));
    assert_eq!(
        output
            .encoding_type
            .map(|encoding| encoding.as_str().to_string()),
        Some("url".to_string())
    );
}

#[tokio::test]
async fn fetch_owner_group() {
    let storage_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId([37u8; 32]);
    let group_id = Ulid::generate();
    let created_by = UserId::local(Ulid::generate(), realm_id);

    let service =
        ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

    seed_materialized_keys(&storage_handle, "bucket", &["a"], created_by, UNIX_EPOCH).await;

    let mut extensions = Extensions::new();
    extensions.insert(test_user_access(group_id, realm_id));
    extensions.insert(test_bucket_info(group_id, created_by));

    let req = list_request(
        extensions,
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: Some(true),
            max_keys: Some(10),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        },
    );
    let output = service.list_objects_v2(req).await.unwrap().output;

    let owners: Vec<_> = output
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| object.owner.and_then(|owner| owner.id))
        .collect();
    assert_eq!(owners, vec![group_id.to_string()]);
}
