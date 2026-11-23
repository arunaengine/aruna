//! Tests pushing object versions to target nodes and the replication scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Answers an encryption settings read for a bucket without settings.
fn plain_settings() -> IoResult {
    row(None)
}

fn row(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

fn event_result(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn blob(event: BlobEvent) -> IoResult {
    event_result(Event::blob(event))
}

fn asked<T: std::fmt::Debug, E: std::fmt::Debug>(turn: &Result<Next<T>, E>) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

use super::{
    MAX_SCOPE_VERSIONS, NATIVE_RELATIONSHIP_ID, NATIVE_SET_ID, ObjectTurn, Publication,
    ReplicateObjectError, ReplicateObjectOperation, ReplicateObjectState, ReplicateScopeError,
    ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeState, ReplicateScopeTarget,
    ReplicationFailure, ReplicationVersion, SourceAuthorization, SyncTransferContext, target_key,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{VersionReplicationMessage, VersionReplicationRequest};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::StoredFormat;
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary,
};
use aruna_core::structs::storage::replication::{
    ReplicationItemError, ReplicationItemKind, ReplicationNegotiationResult,
    ReplicationSuboperationResult,
};
use aruna_core::structs::storage::replication_queue::{
    ReferenceAdvance, ReplicationMode, SyncOrigin,
};
use aruna_core::structs::storage::routing::GroupRoutingInputs;
use aruna_core::structs::sync_relationship::ReferenceHandling;
use aruna_core::types::{Key, Value};
use aruna_core::user::id::UserId;
use aruna_runner::driver::{DriverContext, drive};
use aruna_storage::storage::memory::MemoryStorage;
use bytes::Bytes;
use futures_util::stream;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

/// Replays the routing loader and bucket-rules read the reference path adds.
fn load_routing(op: &mut ReplicateObjectOperation) -> ObjectTurn {
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::GroupRoutingLoaded {
            result: Box::new(Ok(GroupRoutingInputs::default())),
        },
    )))
    .unwrap();
    op.next(row(None))
}

#[test]
fn maps_sync_prefix() {
    assert_eq!(
        super::map_sync_key("photos/2026/a.jpg", Some("photos/"), Some("archive/")),
        Some("archive/2026/a.jpg".to_string())
    );
    assert_eq!(
        super::map_sync_key("photos/a.jpg", Some("photos/"), None),
        Some("a.jpg".to_string())
    );
    assert_eq!(
        super::map_sync_key("other/a.jpg", Some("photos/"), Some("archive/")),
        None
    );
}

fn test_realm_id() -> RealmId {
    RealmId::from_bytes([7u8; 32])
}

fn test_user_id() -> UserId {
    UserId::nil(test_realm_id())
}

fn bucket_info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::generate(),
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn auth_context() -> AuthContext {
    AuthContext {
        user_id: test_user_id(),
        realm_id: test_realm_id(),
        path_restrictions: None,
        session: None,
    }
}

fn scope_input(target: ReplicateScopeTarget) -> ReplicateScopeInput {
    ReplicateScopeInput {
        bucket: "bucket".to_string(),
        target,
        target_node_id: iroh::SecretKey::generate().public(),
        auth_context: auth_context(),
        replicate_delete_markers: true,
        mode: ReplicationMode::Live,
    }
}

async fn source_auth(input: &ReplicateScopeInput, allow: bool) -> (TempDir, SourceAuthorization) {
    let directory = tempfile::tempdir().unwrap();
    let storage = MemoryStorage::open();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = input.auth_context.realm_id;
    let group_id = Ulid::from_bytes([3u8; 16]);
    let actor = Actor {
        node_id: input.target_node_id,
        user_id: input.auth_context.user_id,
        realm_id,
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::new(),
        operation_restrictions: HashMap::new(),
    };
    let group = if allow {
        GroupAuthorizationDocument::default_group_doc(actor.user_id, realm_id, group_id)
    } else {
        GroupAuthorizationDocument {
            group_id,
            roles: HashMap::new(),
            policies: Vec::new(),
        }
    };
    let event = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: vec![
                // Policy loading fails closed without the realm config document.
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                        .to_bytes(&actor)
                        .unwrap()
                        .into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    realm.to_bytes(&actor).unwrap().into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                    group.to_bytes(&actor).unwrap().into(),
                ),
                // Policy loading resolves the group record before group policies apply.
                (
                    GROUP_KEYSPACE.to_string(),
                    group_id.to_bytes().into(),
                    Group {
                        display_name: "replication-group".to_string(),
                        group_id,
                        realm_id,
                        roles: group.roles.keys().copied().collect(),
                        owner: actor.user_id,
                    }
                    .to_bytes(&actor)
                    .unwrap()
                    .into(),
                ),
            ],
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let authorization = drive(
        crate::replication::source_rules::SourceAuthOperation::new(
            input.auth_context.clone(),
            group_id,
            input.target_node_id,
        ),
        &context,
    )
    .await
    .unwrap();
    (directory, authorization)
}

fn version_entry(
    key: &str,
    version_id: Ulid,
) -> (aruna_core::types::Key, aruna_core::types::Value) {
    let key_bytes = VersionKey::new("bucket", key, version_id)
        .to_bytes()
        .unwrap();
    let value_bytes = BlobVersion::deleted(SystemTime::now(), test_user_id())
        .to_bytes()
        .unwrap();
    (key_bytes.into(), value_bytes.into())
}

fn materialized_blob_version(
    location: &BackendLocation,
    source: Option<VersionSourceBinding>,
) -> BlobVersion {
    BlobVersion::materialized(
        location.get_blake3().unwrap().try_into().unwrap(),
        BackendRef::node_default(),
        EncodingClass::Raw,
        location.created_at,
        location.created_by,
        source,
    )
}

fn reference_source_binding() -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            source_path: "ref/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::from_bytes([9u8; 16])),
        mirrors: Vec::new(),
    }
}

fn reference_cached_metadata() -> SourceMetadata {
    SourceMetadata {
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        etag: Some("etag-1".to_string()),
        last_modified: Some(SystemTime::UNIX_EPOCH),
        source_version: None,
        digests: Default::default(),
    }
}

fn reference_blob_version() -> BlobVersion {
    BlobVersion::reference(
        reference_source_binding(),
        reference_cached_metadata(),
        SystemTime::now(),
        test_user_id(),
        SystemTime::now(),
    )
}

fn materialized_location() -> BackendLocation {
    let mut hashes = HashMap::new();
    hashes.insert("blake3".to_string(), vec![1u8; 32]);
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "blob-bucket".to_string(),
        backend_path: format!("bucket/key_{}", Ulid::generate()),
        ulid: Ulid::generate(),
        format: StoredFormat::default(),
        created_by: test_user_id(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: 42,
        hashes,
    }
}

fn request_with_mode(version_id: Ulid, mode: ReplicationMode) -> VersionReplicationRequest {
    VersionReplicationRequest {
        bucket: "bucket".to_string(),
        key: "dir/file.txt".to_string(),
        version_id,
        source_group_id: Ulid::generate(),
        target_node_id: iroh::SecretKey::generate().public(),
        auth_context: auth_context(),
        mode,
    }
}

fn version_request(version_id: Ulid) -> VersionReplicationRequest {
    request_with_mode(version_id, ReplicationMode::Live)
}

fn reference_sync() -> SyncTransferContext {
    SyncTransferContext {
        target_bucket: "target-bucket".to_string(),
        source_prefix: None,
        target_prefix: Some("copied/file.txt".to_string()),
        source_node_id: iroh::SecretKey::generate().public(),
        relationship_id: Ulid::generate(),
        reference_intent: true,
        reference_handling: ReferenceHandling::Materialize,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: None,
        distribution: None,
    }
}

#[test]
fn destination_maps_exact() {
    let version_id = Ulid::generate();
    let source_node_id = iroh::SecretKey::generate().public();
    let writer = auth_context();
    let operation = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Version {
        key: "stage-version".to_string(),
        version_id,
    }))
    .with_destination(
        "final-bucket".to_string(),
        "results/output.txt".to_string(),
        source_node_id,
        writer.clone(),
    );
    let sync = operation.sync.expect("destination mapping");

    assert_eq!(sync.target_bucket, "final-bucket");
    assert_eq!(sync.source_prefix.as_deref(), Some("stage-version"));
    assert_eq!(sync.target_prefix.as_deref(), Some("results/output.txt"));
    assert_eq!(sync.source_node_id, source_node_id);
    assert_eq!(sync.relationship_id, version_id);
    assert_eq!(sync.origin, None);
    assert_eq!(sync.writer_auth_context, Some(writer));
}

fn multipart_part_entry(
    version_id: Ulid,
    part_number: u16,
) -> (aruna_core::types::Key, aruna_core::types::Value) {
    let key = MultipartObjectKey::part(version_id, part_number)
        .to_bytes()
        .unwrap();
    let value = MultipartObjectPart {
        part_number,
        size: u64::from(part_number),
        hashes: HashMap::new(),
    }
    .to_bytes()
    .unwrap();
    (key.into(), value.into())
}

#[test]
fn exact_hit_iterates() {
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Object {
        key: "dir/file.txt".to_string(),
    }));

    op.first().unwrap();
    let effects = op.next(row(Some(bucket_info().to_bytes().unwrap().into())));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(Some(vec![2u8].into())));
    let Ok(Next::Call(IoCall::Page { .. })) = &effects else {
        panic!("expected iteration after exact object lookup")
    };

    let matching_version = Ulid::generate();
    let effects = op.next(page(
        vec![
            version_entry("dir/file.txt", matching_version),
            version_entry("dir/file.txt.bak", Ulid::generate()),
            version_entry("dir/sub/file.txt", Ulid::generate()),
        ],
        None,
    ));

    let Ok(Next::Call(IoCall::Child(_))) = &effects else {
        panic!("expected only matching object version to be enqueued")
    };
    assert_eq!(op.pending_versions.len(), 0);
}

#[test]
fn scope_dedups_versions() {
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
    op.state = ReplicateScopeState::IterateVersions;
    let version_id = Ulid::from_bytes([1u8; 16]);
    let cursor: aruna_core::types::Key = vec![9u8].into();

    let effects = op.next(page(
        vec![version_entry("dir/file.txt", version_id); 2],
        Some(cursor),
    ));

    assert!(matches!(effects, Ok(Next::Call(IoCall::Page { .. }))));
    assert_eq!(op.pending_versions.len(), 1);
    assert_eq!(op.pending_keys.len(), 1);
}

#[test]
fn scope_rejects_overflow() {
    // A page pushing the examined total past the budget is rejected before
    // any of its versions are enqueued.
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
    op.state = ReplicateScopeState::IterateVersions;
    let values = (0..=MAX_SCOPE_VERSIONS)
        .map(|index| {
            version_entry(
                &format!("key-{index}"),
                Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
            )
        })
        .collect();

    let effects = op.next(page(values, None));

    assert!(op.pending_versions.is_empty());
    assert_eq!(
        effects,
        Err(ReplicateScopeError::ScopeLimit {
            limit: MAX_SCOPE_VERSIONS,
        })
    );
}

/// Scope results with fixed inputs and no runtime, storage or network.
mod scope_results {
    use super::*;

    // A failed push drops its local bytes and closes its stream.
    #[test]
    fn object_abort_cleans() {
        let mut op = ReplicateObjectOperation::new(version_request(Ulid::from_parts(12, 12)));
        assert!(op.abort().is_empty());
        let location = materialized_location();
        let stream_id = Ulid::from_parts(13, 13);
        op.cleanup_reference_blob = Some(location.clone());
        op.stream_id = Some(stream_id);
        let delete = Effect::blob(BlobEffect::Delete { location });
        let close = Effect::blob(BlobEffect::CloseConnection { stream_id });
        assert_eq!(op.abort(), vec![delete.into(), close.into()]);
    }

    #[test]
    fn scope_failure_recorded() {
        // A refused child is recorded and still ends in the scope result.
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = ReplicateScopeState::RunVersionReplication;

        let turn = op.accept_item(Err(ReplicationItemError {
            failure: ReplicationFailure::AccessDenied,
            message: "denied".to_string(),
        }));

        assert_eq!(op.result.failed, 1);
        assert_eq!(op.result.failure, Some(ReplicationFailure::AccessDenied));
        assert_eq!(turn, Ok(Next::Done(op.result.clone())));
    }

    #[test]
    fn scope_reads_child() {
        // A child's failure counts as a failed item, with its stable category.
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = ReplicateScopeState::RunVersionReplication;
        let failed =
            Err::<ReplicationSuboperationResult, _>(ReplicateObjectError::PlaintextRefused);
        let turn = op.next(IoResult::Child(ChildOutcome::new(failed)));
        assert_eq!(op.result.failed, 1);
        assert_eq!(
            op.result.failure,
            Some(ReplicationFailure::PlaintextRefused)
        );
        assert_eq!(turn, Ok(Next::Done(op.result.clone())));
    }
}

#[test]
fn scope_paginates_cursor() {
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
    op.state = ReplicateScopeState::IterateVersions;
    let cursor: aruna_core::types::Key = vec![9u8].into();

    let effects = op.next(page(
        vec![version_entry("dir/first", Ulid::from_bytes([1u8; 16]))],
        Some(cursor.clone()),
    ));
    let Ok(Next::Call(IoCall::Page { after: start, .. })) = &effects else {
        panic!("expected next scope page")
    };
    assert_eq!(start, &Some(cursor));

    let effects = op.next(page(
        vec![version_entry("dir/second", Ulid::from_bytes([2u8; 16]))],
        None,
    ));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(op.pending_versions.len(), 1);
}

#[tokio::test]
async fn scope_caps_denied() {
    // Authorization-denied pages must consume the same finite scan budget.
    let input = scope_input(ReplicateScopeTarget::Bucket);
    let (_directory, authorization) = source_auth(&input, false).await;
    let mut op = ReplicateScopeOperation::new(input).with_source_authorization(authorization);
    op.state = ReplicateScopeState::IterateVersions;

    let first = (0..512)
        .map(|index| {
            version_entry(
                &format!("key-{index}"),
                Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
            )
        })
        .collect();
    let cursor = vec![1u8].into();
    let effects = op.next(page(first, Some(cursor)));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Page { .. }))));

    let second = (512..1024)
        .map(|index| {
            version_entry(
                &format!("key-{index}"),
                Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
            )
        })
        .collect();
    let cursor = vec![2u8].into();
    let effects = op.next(page(second, Some(cursor)));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Page { .. }))));
    assert_eq!(op.examined_versions, MAX_SCOPE_VERSIONS);
    assert!(op.pending_versions.is_empty());

    let effects = op.next(page(
        vec![version_entry(
            "key-1024",
            Ulid::from_bytes((MAX_SCOPE_VERSIONS as u128 + 1).to_be_bytes()),
        )],
        None,
    ));
    assert_eq!(
        effects,
        Err(ReplicateScopeError::ScopeLimit {
            limit: MAX_SCOPE_VERSIONS,
        })
    );
}

#[tokio::test]
async fn scope_admits_permitted() {
    // The source gate must admit a permitted read; a denying gate would stop
    // all replication. Roles bind to a real user, never the nil principal.
    let mut input = scope_input(ReplicateScopeTarget::Bucket);
    input.auth_context.user_id = UserId::local(Ulid::from_bytes([4u8; 16]), test_realm_id());
    let (_directory, authorization) = source_auth(&input, true).await;
    let mut op = ReplicateScopeOperation::new(input).with_source_authorization(authorization);
    op.state = ReplicateScopeState::IterateVersions;

    let effects = op.next(page(
        vec![version_entry("dir/file.txt", Ulid::from_bytes([1u8; 16]))],
        None,
    ));

    assert_eq!(op.result.last_error, None);
    assert_eq!(op.result.failed, 0);
    assert!(matches!(effects, Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn object_miss_stops() {
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Object {
        key: "dir/file".to_string(),
    }));

    op.first().unwrap();
    op.next(row(Some(bucket_info().to_bytes().unwrap().into())))
        .unwrap();
    let effects = op.next(row(None));
    let Ok(Next::Call(IoCall::Page { .. })) = &effects else {
        panic!("expected exact iteration after object miss")
    };

    let effects = op.next(page(
        vec![
            version_entry("dir/file-a", Ulid::generate()),
            version_entry("dir/file/b", Ulid::generate()),
            version_entry("dir/other", Ulid::generate()),
        ],
        None,
    ));

    assert_eq!(op.pending_versions.len(), 0);
    assert_eq!(op.result.replicated, 0);
    assert_eq!(op.result.skipped, 0);
    assert_eq!(op.result.failed, 0);
    assert_eq!(effects, Ok(Next::Done(op.result.clone())));
}

#[test]
fn scope_counts_bytes() {
    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
    op.state = ReplicateScopeState::RunVersionReplication;

    op.accept_item(Ok(ReplicationSuboperationResult::ReplicatedBytes(42)))
        .unwrap();

    assert_eq!(op.result.replicated, 1);
    assert_eq!(op.result.replicated_bytes, 42);
}

#[test]
fn advance_queue_cardinality() {
    let advance = ReferenceAdvance {
        generation: 2,
        predecessor: Ulid::generate(),
    };
    let mut denied = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket))
        .with_reference_advance(advance);
    denied.result.failed = 1;
    denied.result.failure = Some(ReplicationFailure::AccessDenied);
    denied.result.last_error = Some("source access denied".to_string());

    let effects = denied.run_next_replication();

    assert_eq!(denied.result.failed, 1);
    assert_eq!(
        denied.result.last_error.as_deref(),
        Some("source access denied")
    );
    assert_eq!(effects, Ok(Next::Done(denied.result.clone())));

    let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket))
        .with_reference_advance(advance);
    op.pending_versions = vec![
        version_request(Ulid::generate()),
        version_request(Ulid::generate()),
    ];

    let effects = op.run_next_replication();

    assert_eq!(
        effects,
        Err(ReplicateScopeError::ReplicateObjectError(
            ReplicateObjectError::InvalidReferenceAdvance,
        ))
    );
}

#[test]
fn multipart_metadata_paginates() {
    let version_id = Ulid::generate();
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    let location = materialized_location();

    let effects = op.first();
    assert!(matches!(effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(Some(
        materialized_blob_version(&location, None)
            .to_bytes()
            .unwrap()
            .into(),
    )));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(Some(location.to_bytes().unwrap().into())));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(Some(
        MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 3,
            composite_hashes: Default::default(),
        }
        .to_bytes()
        .unwrap()
        .into(),
    )));
    let Ok(Next::Call(IoCall::Page { after: start, .. })) = &effects else {
        panic!("expected multipart iter request")
    };
    assert!(start.is_none());

    let next_cursor: aruna_core::types::Key = vec![9u8].into();
    let effects = op.next(page(
        vec![
            multipart_part_entry(version_id, 2),
            multipart_part_entry(version_id, 1),
        ],
        Some(next_cursor.clone()),
    ));
    let Ok(Next::Call(IoCall::Page { after: start, .. })) = &effects else {
        panic!("expected paginated multipart iter request")
    };
    assert_eq!(start, &Some(next_cursor.clone()));

    let effects = op.next(page(vec![multipart_part_entry(version_id, 3)], None));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Read(_)))));
    assert_eq!(op.multipart_parts.len(), 3);
    assert_eq!(op.multipart_parts[0].part_number, 1);
    assert_eq!(op.multipart_parts[1].part_number, 2);
    assert_eq!(op.multipart_parts[2].part_number, 3);
}

#[test]
fn rejects_incomplete_parts() {
    let version_id = Ulid::generate();
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    let location = materialized_location();

    op.first().unwrap();
    op.next(row(Some(
        materialized_blob_version(&location, None)
            .to_bytes()
            .unwrap()
            .into(),
    )))
    .unwrap();
    op.next(row(Some(location.to_bytes().unwrap().into())))
        .unwrap();
    op.next(row(Some(
        MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 2,
            composite_hashes: Default::default(),
        }
        .to_bytes()
        .unwrap()
        .into(),
    )))
    .unwrap();

    let effects = op.next(page(vec![multipart_part_entry(version_id, 1)], None));
    assert_eq!(
        effects,
        Err(ReplicateObjectError::PartCountMismatch {
            expected: 2,
            actual: 1,
        })
    );
}

#[test]
fn manifest_includes_generation() {
    let version_id = Ulid::generate();
    let generation = 42;
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    op.replication_version = Some(ReplicationVersion::Deleted {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        metadata: HashMap::new(),
    });

    op.build_manifest(Some(CurrentVersionPointer::new_with_generation(
        version_id, generation,
    )))
    .unwrap();

    let manifest = op.manifest.expect("manifest built");
    assert!(manifest.current_version);
    assert_eq!(manifest.current_version_generation, Some(generation));
}

#[test]
fn advance_scope_lineage() {
    // A later head must not relabel the durable successor obligation.
    let version_id = Ulid::generate();
    let advance = ReferenceAdvance {
        generation: 42,
        predecessor: Ulid::generate(),
    };
    let mut input = scope_input(ReplicateScopeTarget::Version {
        key: "dir/file.txt".to_string(),
        version_id,
    });
    input.auth_context.path_restrictions = Some(vec![PathRestriction {
        pattern: "/realm/g/group/bucket/dir/file.txt".to_string(),
        permission: Permission::READ,
    }]);
    let scoped_auth = input.auth_context.clone();
    let persisted = reference_cached_metadata();
    let mut scope = ReplicateScopeOperation::new(input).with_reference_advance(advance);
    scope.source_group_id = Some(Ulid::generate());
    let mut sync = reference_sync();
    sync.origin = Some(SyncOrigin {
        relationship_id: sync.relationship_id,
        hop_count: 0,
    });
    scope.sync = Some(sync);
    scope
        .enqueue_version_request(VersionKey::new("bucket", "dir/file.txt", version_id))
        .unwrap();

    let Ok(Next::Call(IoCall::Child(mut operation))) = scope.run_next_replication() else {
        panic!("expected version suboperation")
    };
    assert!(scope.reference_advance.is_none());

    // The child runs as the runner runs it, so it answers storage events.
    let read = |value: Vec<u8>| {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![1u8].into(), Some(value.into()))],
        })
    };
    operation.start();
    let effects = operation.step(read(reference_blob_version().to_bytes().unwrap()));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })]
            if reads[0].0 == BLOB_HEAD_KEYSPACE
    ));
    let newer =
        CurrentVersionPointer::new_with_generation(Ulid::generate(), advance.generation + 1);
    let effects = operation.step(read(newer.to_bytes().unwrap()));
    assert!(matches!(
        effects.as_slice(),
        [effect] if matches!(effect.as_blob(), Some(BlobEffect::OpenConnection { .. }))
    ));

    let effects = operation.step(Event::blob(BlobEvent::ConnectionEstablished {
        stream_id: Ulid::generate(),
    }));
    let Some(BlobEffect::SendMessage { payload, .. }) = effects.first().and_then(Effect::as_blob)
    else {
        panic!("expected manifest message")
    };
    let VersionReplicationMessage::ReferenceAdvance {
        manifest,
        advance: wire_advance,
    } = VersionReplicationMessage::from_bytes(payload).unwrap()
    else {
        panic!("expected reference advance")
    };

    assert!(manifest.current_version);
    assert_eq!(
        manifest.current_version_generation,
        Some(advance.generation)
    );
    assert_eq!(manifest.auth_context, scoped_auth);
    assert_eq!(manifest.writer_auth_context, None);
    assert_eq!(manifest.reference_metadata, Some(persisted));
    assert_eq!(wire_advance, advance);
    assert_eq!(manifest.reference_advance, Some(advance));
}

// A version stored compact leaves with its root's settings: its root record is read first.
#[test]
fn compact_version_hydrated() {
    use aruna_core::keyspaces::ROOT_RECORD_KEYSPACE;
    use aruna_core::structs::storage::distribution::source_root::RootEntry;
    let full = reference_source_binding();
    let (compact, root) = RootEntry::compact(&full).unwrap().unwrap();
    let (now, user) = (SystemTime::now(), test_user_id());
    let stored = BlobVersion::reference(compact, reference_cached_metadata(), now, user, now);
    let mut op = ReplicateObjectOperation::new(version_request(Ulid::generate()))
        .with_sync(reference_sync());
    op.first().unwrap();

    let effects = op.next(row(Some(stored.to_bytes().unwrap().into())));
    let Effect::Storage(StorageEffect::Read { key_space, .. }) = asked(&effects) else {
        panic!("expected the root read, got {effects:?}");
    };
    assert_eq!(key_space, ROOT_RECORD_KEYSPACE);
    op.next(event_result(Event::Storage(StorageEvent::ReadResult {
        key: vec![2u8].into(),
        value: Some(root.to_bytes().unwrap().into()),
    })))
    .unwrap();

    let Some(ReplicationVersion::Reference { source, .. }) = op.replication_version.as_ref() else {
        panic!("the reference must be kept for its manifest");
    };
    assert_eq!(source, &full);
}

// Normal reference replication carries the stored cap, so repair and snapshot
// transfers cannot hand the target a fresh advance budget.
#[test]
fn manifest_carries_count() {
    let mut op = ReplicateObjectOperation::new(version_request(Ulid::generate()))
        .with_sync(reference_sync());
    op.preserve_reference = true;
    op.replication_version = Some(ReplicationVersion::Reference {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        source: reference_source_binding(),
        cached_metadata: reference_cached_metadata(),
        last_refresh: SystemTime::now(),
        metadata: HashMap::new(),
        advance_count: 5,
    });

    op.build_manifest(None).unwrap();

    assert_eq!(op.manifest.unwrap().reference_advance_count, Some(5));
}

#[test]
fn advance_rejects_nonreference() {
    let advance = ReferenceAdvance {
        generation: 2,
        predecessor: Ulid::generate(),
    };
    let mut materialized = ReplicateObjectOperation::new(version_request(Ulid::generate()))
        .with_sync(reference_sync())
        .with_reference_advance(advance);
    materialized.preserve_reference = true;
    materialized.replication_version = Some(ReplicationVersion::Materialized {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        location: materialized_location(),
        source: None,
        metadata: HashMap::new(),
    });
    assert_eq!(
        materialized.build_manifest(None),
        Err(ReplicateObjectError::InvalidReferenceAdvance)
    );

    materialized.multipart_summary = Some(MultipartObjectSummary {
        checksum_type: MultipartChecksumType::Composite,
        part_count: 0,
        composite_hashes: HashMap::new(),
    });
    assert_eq!(
        materialized.build_manifest(None),
        Err(ReplicateObjectError::InvalidReferenceAdvance)
    );

    let mut deleted = ReplicateObjectOperation::new(version_request(Ulid::generate()))
        .with_sync(reference_sync())
        .with_reference_advance(advance);
    deleted.preserve_reference = true;
    deleted.replication_version = Some(ReplicationVersion::Deleted {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        metadata: HashMap::new(),
    });
    assert_eq!(
        deleted.build_manifest(None),
        Err(ReplicateObjectError::InvalidReferenceAdvance)
    );
}

#[test]
fn manifest_writer_auth() {
    let version_id = Ulid::generate();
    let writer = auth_context();
    let mut op =
        ReplicateObjectOperation::new(version_request(version_id)).with_writer_auth(writer.clone());
    op.replication_version = Some(ReplicationVersion::Deleted {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        metadata: HashMap::new(),
    });

    op.build_manifest(None).unwrap();

    assert_eq!(op.manifest.unwrap().writer_auth_context, Some(writer));
}

#[test]
fn manifest_includes_binding() {
    let version_id = Ulid::generate();
    let source = reference_source_binding();
    let metadata = HashMap::from([("mtime".to_string(), "1753272000.123456789".to_string())]);
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    op.replication_version = Some(ReplicationVersion::Materialized {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        location: materialized_location(),
        source: Some(source.clone()),
        metadata: metadata.clone(),
    });

    op.build_manifest(None).unwrap();

    let manifest = op.manifest.expect("manifest built");
    assert_eq!(
        manifest.kind,
        aruna_core::structs::storage::replication::ReplicationItemKind::Materialized
    );
    assert_eq!(manifest.source, Some(source));
    assert_eq!(manifest.metadata, metadata);
}

#[test]
fn builds_reference_manifest() {
    let version_id = Ulid::generate();
    let sync = reference_sync();
    let location = materialized_location();
    let source_node_id = sync.source_node_id;
    let relationship_id = sync.relationship_id;
    let created_at = SystemTime::now();
    let mut op = ReplicateObjectOperation::new(version_request(version_id)).with_sync(sync);
    op.replication_version = Some(ReplicationVersion::Materialized {
        created_at,
        created_by: test_user_id(),
        location: location.clone(),
        source: None,
        metadata: HashMap::new(),
    });

    op.build_manifest(None).unwrap();

    let manifest = op.manifest.expect("manifest built");
    assert!(manifest.reference_intent);
    assert!(manifest.blob.is_none());
    assert_eq!(manifest.bucket, "target-bucket");
    assert_eq!(manifest.key, "copied/file.txt");
    assert_eq!(
        manifest.reference_metadata,
        Some(SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: Some(created_at),
            source_version: None,
            digests: Default::default(),
        })
    );
    // A native version exported as a reference is a fresh binding at the target.
    assert_eq!(manifest.reference_advance_count, Some(0));
    let source = manifest.source.expect("reference binding");
    assert_eq!(source.strategy, StagingStrategy::Reference);
    assert_eq!(source.connector_id, None);
    assert_eq!(source.descriptor.kind, SourceConnectorKind::ArunaNative);
    assert_eq!(source.descriptor.origin_node_id, Some(source_node_id));
    assert_eq!(source.descriptor.source_path, "bucket/dir/file.txt");
    assert_eq!(
        source.descriptor.version_selector.as_deref(),
        Some(format!("version:{version_id}").as_str())
    );
    assert_eq!(
        source.descriptor.public_config.get("relationship_id"),
        Some(&relationship_id.to_string())
    );
}

// The target heads this native source on first read and compares fingerprints,
// so the replicated observation must be the version's, not the location row's.
#[test]
fn observation_matches_head() {
    let version_id = Ulid::generate();
    let location = materialized_location();
    let created_at = SystemTime::now();
    let mut op =
        ReplicateObjectOperation::new(version_request(version_id)).with_sync(reference_sync());
    op.replication_version = Some(ReplicationVersion::Materialized {
        created_at,
        created_by: test_user_id(),
        location: location.clone(),
        source: None,
        metadata: HashMap::new(),
    });

    op.build_manifest(None).unwrap();

    // What the origin's native head reports for a materialized version;
    // source_version is transient and excluded from the fingerprint.
    let head = SourceMetadata {
        content_length: location.blob_size,
        content_type: None,
        etag: None,
        last_modified: Some(created_at),
        source_version: Some(version_id.to_string()),
        digests: Default::default(),
    };
    let replicated = op
        .manifest
        .expect("manifest built")
        .reference_metadata
        .expect("reference observation");
    assert_eq!(
        replicated.observation_fingerprint(),
        head.observation_fingerprint()
    );
    assert_ne!(location.created_at, created_at);
}

#[test]
fn delete_omits_binding() {
    let version_id = Ulid::generate();
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    op.replication_version = Some(ReplicationVersion::Deleted {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        metadata: HashMap::new(),
    });

    op.build_manifest(None).unwrap();

    let manifest = op.manifest.expect("manifest built");
    assert_eq!(
        manifest.kind,
        aruna_core::structs::storage::replication::ReplicationItemKind::DeleteMarker
    );
    assert_eq!(manifest.source, None);
}

#[test]
fn keeps_reference_deletes() {
    let version_id = Ulid::generate();
    let mut op =
        ReplicateObjectOperation::new(version_request(version_id)).with_sync(reference_sync());
    op.replication_version = Some(ReplicationVersion::Deleted {
        created_at: SystemTime::now(),
        created_by: test_user_id(),
        metadata: HashMap::new(),
    });

    op.build_manifest(None).unwrap();

    let manifest = op.manifest.expect("manifest built");
    assert_eq!(manifest.kind, ReplicationItemKind::DeleteMarker);
    assert!(manifest.blob.is_none());
    assert!(manifest.source.is_none());
    assert!(manifest.reference_metadata.is_none());
}

#[test]
fn rejects_unresolved_reference() {
    let version_id = Ulid::generate();
    let source = reference_source_binding();
    let cached_metadata = reference_cached_metadata();
    let created_at = SystemTime::now();
    let created_by = test_user_id();
    let last_refresh = SystemTime::now();
    let mut op = ReplicateObjectOperation::new(version_request(version_id));
    op.replication_version = Some(ReplicationVersion::Reference {
        created_at,
        created_by,
        source,
        cached_metadata,
        last_refresh,
        metadata: HashMap::new(),
        advance_count: 0,
    });

    assert_eq!(
        op.build_manifest(None),
        Err(ReplicateObjectError::UnresolvedReferenceVersion)
    );
}

#[test]
fn preserves_reference_source() {
    let version_id = Ulid::generate();
    let cached_metadata = reference_cached_metadata();
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand))
            .with_sync(reference_sync());

    op.first().unwrap();
    let effects = op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )));
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_HEAD_KEYSPACE
    ));
    let effects = op.next(row(None));
    assert!(matches!(
        (asked(&effects)).as_blob(),
        Some(BlobEffect::OpenConnection { .. })
    ));
    assert!(matches!(
        op.replication_version,
        Some(ReplicationVersion::Reference { .. })
    ));

    let manifest = op.manifest.expect("manifest built");
    assert!(manifest.blob.is_none());
    assert_eq!(manifest.reference_metadata, Some(cached_metadata));
    assert_eq!(
        manifest.source.expect("reference binding").descriptor.kind,
        SourceConnectorKind::ArunaNative
    );
}

#[test]
fn publication_keeps_source() {
    // Members of a set read the external source themselves, never through the coordinator.
    let version_id = Ulid::generate();
    let publication = Publication {
        set_id: Ulid::from_bytes([4; 16]),
        range_id: Ulid::from_bytes([5; 16]),
        revision: 3,
        epoch: aruna_core::structs::storage::distribution::PublicationEpoch(2),
        member_id: Ulid::from_bytes([6; 16]),
        incarnation: 7,
        bytes: false,
    };
    let mut sync = reference_sync();
    sync.distribution = Some(publication.clone());
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::Live))
            .with_sync(sync);
    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();
    op.next(row(None)).unwrap();
    let manifest = op.manifest.expect("manifest built");
    assert_eq!(manifest.distribution, Some(publication));
    let source = manifest.source.expect("reference binding");
    let mut expected = reference_source_binding();
    expected.connector_id = None;
    assert_eq!(source, expected);
}

#[test]
fn page_matches_single() {
    // A reference page carries exactly the manifest a single delivery of the job sends.
    use crate::replication::page::build::reference_manifest;
    use aruna_core::structs::storage::distribution::OriginalEpoch;
    use aruna_core::structs::storage::replication_queue::BlobJobRecord;
    let version_id = Ulid::generate();
    let publication = Publication {
        set_id: Ulid::from_bytes([4; 16]),
        range_id: Ulid::from_bytes([5; 16]),
        revision: 3,
        epoch: aruna_core::structs::storage::distribution::PublicationEpoch(2),
        member_id: Ulid::from_bytes([6; 16]),
        incarnation: 7,
        bytes: true,
    };
    let request = request_with_mode(version_id, ReplicationMode::Live);
    let input = ReplicateScopeInput {
        bucket: request.bucket.clone(),
        target: ReplicateScopeTarget::Version {
            key: request.key.clone(),
            version_id,
        },
        target_node_id: request.target_node_id,
        auth_context: request.auth_context.clone(),
        replicate_delete_markers: true,
        mode: ReplicationMode::Live,
    };
    let local = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let scope =
        ReplicateScopeOperation::new(input.clone()).with_distribution(publication.clone(), local);
    let mut sync = scope.sync.clone().expect("a publication");
    sync.target_prefix = Some(request.key.clone());
    let version = reference_blob_version()
        .with_epoch(OriginalEpoch(2))
        .with_generation(4);
    let head =
        CurrentVersionPointer::new_with_generation(version_id, 4).with_epoch(OriginalEpoch(2));
    let group_id = request.source_group_id;
    let mut op = ReplicateObjectOperation::new(request).with_sync(sync);
    op.first().unwrap();
    op.next(row(Some(version.to_bytes().unwrap().into())))
        .unwrap();
    op.next(row(Some(head.to_bytes().unwrap().into()))).unwrap();
    let single = op.manifest.expect("manifest built");
    let job = BlobJobRecord::new(input, Some(false), 0, Ulid::generate());
    let paged = reference_manifest(&job, version, Some(&head), group_id, &publication);
    assert_eq!(paged, Some(single));
}

#[test]
fn publication_keeps_key() {
    let mut sync = reference_sync();
    sync.target_prefix = None;
    assert_eq!(target_key(&sync, "/a/b").as_deref(), Some("a/b"));
    sync.distribution = Some(Publication {
        set_id: Ulid::from_bytes([4; 16]),
        range_id: Ulid::from_bytes([5; 16]),
        revision: 3,
        epoch: aruna_core::structs::storage::distribution::PublicationEpoch(2),
        member_id: Ulid::from_bytes([6; 16]),
        incarnation: 7,
        bytes: true,
    });
    assert_eq!(target_key(&sync, "/a/b").as_deref(), Some("/a/b"));
}

#[test]
fn records_name_set() {
    // A record-only member learns which set's holders keep the bytes it may fetch later.
    let set_id = Ulid::from_bytes([4; 16]);
    let mut sync = reference_sync();
    sync.distribution = Some(Publication {
        set_id,
        range_id: Ulid::from_bytes([5; 16]),
        revision: 3,
        epoch: aruna_core::structs::storage::distribution::PublicationEpoch(2),
        member_id: Ulid::from_bytes([6; 16]),
        incarnation: 7,
        bytes: false,
    });
    let op = ReplicateObjectOperation::new(version_request(Ulid::generate())).with_sync(sync);
    let binding = op.reference_binding(op.sync.as_ref().unwrap());
    assert_eq!(
        binding.descriptor.public_config.get(NATIVE_SET_ID),
        Some(&set_id.to_string())
    );
    assert!(
        !binding
            .descriptor
            .public_config
            .contains_key(NATIVE_RELATIONSHIP_ID)
    );
}

#[test]
fn stale_reference_skips() {
    let version_id = Ulid::generate();
    let mut sync = reference_sync();
    sync.reference_intent = false;
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand))
            .with_sync(sync);

    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();
    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::new(),
        path: "ref/file.txt".to_string(),
        version: None,
    };
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access)),
        },
    )))
    .unwrap();
    op.next(plain_settings()).unwrap();
    let mut stale = reference_cached_metadata();
    stale.etag = Some("etag-2".to_string());
    let effects = op.next(event_result(Event::staging(
        StagingSourceEvent::HeadResult { metadata: stale },
    )));

    assert_eq!(
        effects,
        Ok(Next::Done(ReplicationSuboperationResult::Skipped))
    );
}

#[test]
fn read_drift_skips() {
    let version_id = Ulid::generate();
    let mut sync = reference_sync();
    sync.reference_intent = false;
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand))
            .with_sync(sync);

    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();
    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::new(),
        path: "ref/file.txt".to_string(),
        version: None,
    };
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access)),
        },
    )))
    .unwrap();
    op.next(plain_settings()).unwrap();
    let metadata = reference_cached_metadata();
    op.next(event_result(Event::staging(
        StagingSourceEvent::HeadResult {
            metadata: metadata.clone(),
        },
    )))
    .unwrap();
    op.next(row(None)).unwrap();
    load_routing(&mut op).unwrap();

    let mut stale = metadata;
    stale.etag = Some("etag-2".to_string());
    let effects = op.next(event_result(Event::staging(
        StagingSourceEvent::ReadResult {
            metadata: stale,
            stream: BackendStream::new(stream::empty::<Result<Bytes, std::io::Error>>()),
        },
    )));

    assert_eq!(
        effects,
        Ok(Next::Done(ReplicationSuboperationResult::Skipped))
    );
}

// A source without validators is still materialized when recorded digests check the write.
#[test]
fn unknown_digest_matches() {
    use aruna_core::structs::checksum::ChecksumAlgorithm;
    use aruna_core::structs::execution::source_access::{DigestOrigin, DigestScope, SourceDigest};
    let mut cached = reference_cached_metadata();
    cached.etag = None;
    cached.last_modified = None;
    let mut op = ReplicateObjectOperation::new(version_request(Ulid::generate()));
    let reference = |cached_metadata: SourceMetadata| ReplicationVersion::Reference {
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        source: reference_source_binding(),
        cached_metadata,
        last_refresh: SystemTime::UNIX_EPOCH,
        metadata: HashMap::new(),
        advance_count: 0,
    };
    op.replication_version = Some(reference(cached.clone()));
    assert_eq!(op.reference_matches(&cached), Some(false));

    cached.digests.insert(
        ChecksumAlgorithm::Sha256,
        SourceDigest {
            digest: vec![1; 32],
            scope: DigestScope::FullObject,
            origin: DigestOrigin::Manifest,
        },
    );
    op.replication_version = Some(reference(cached.clone()));
    assert_eq!(op.reference_matches(&cached), Some(true));
}

#[test]
fn reference_versions_skipped() {
    let version_id = Ulid::generate();
    let mut op = ReplicateObjectOperation::new(version_request(version_id));

    op.first().unwrap();
    let effects = op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )));

    assert_eq!(
        effects,
        Ok(Next::Done(ReplicationSuboperationResult::Skipped))
    );
}

#[test]
fn fails_unreadable_rules() {
    // An unreadable bucket record must fail the write, not reroute it to the node default.
    let mut op = ReplicateObjectOperation::new(request_with_mode(
        Ulid::generate(),
        ReplicationMode::OnDemand,
    ));
    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::new(),
                path: "ref/file.txt".to_string(),
                version: None,
            })),
        },
    )))
    .unwrap();
    op.next(plain_settings()).unwrap();
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::GroupRoutingLoaded {
            result: Box::new(Ok(GroupRoutingInputs::default())),
        },
    )))
    .unwrap();

    let turn = op.next(row(Some(vec![0xff].into())));

    assert!(matches!(
        turn,
        Err(ReplicateObjectError::BucketRulesFailed(_))
    ));
}

#[test]
fn reference_materializes_first() {
    let version_id = Ulid::generate();
    let original_source = Some(reference_source_binding());
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand));

    op.first().unwrap();
    let effects = op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )));
    assert!(matches!(effects, Ok(Next::Call(IoCall::Child(_)))));

    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
        path: "ref/file.txt".to_string(),
        version: None,
    };
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access.clone())),
        },
    )))
    .unwrap();
    let effects = op.next(plain_settings());
    assert!(matches!(effects, Ok(Next::Call(IoCall::Child(_)))));
    let effects = load_routing(&mut op);
    assert!(
        matches!((asked(&effects)).as_staging(), Some(StagingSourceEffect::Read { access: emitted, range, expected })
            if emitted == &access && range.is_none()
                && expected.as_ref() == Some(&reference_cached_metadata()))
    );

    let effects = op.next(event_result(Event::staging(
        StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        },
    )));
    assert!(
        matches!((asked(&effects)).as_blob(), Some(BlobEffect::Write { bucket, key, .. })
            if bucket == "bucket" && key == "dir/file.txt")
    );

    let effects = op.next(blob(BlobEvent::WriteFinished {
        location: materialized_location(),
    }));
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_HEAD_KEYSPACE
    ));
    assert!(matches!(
        op.replication_version.as_ref(),
        Some(ReplicationVersion::Materialized { .. })
    ));
    assert!(matches!(
        op.replication_version.as_ref(),
        Some(ReplicationVersion::Materialized { source, .. }) if source.as_ref() == original_source.as_ref()
    ));

    op.build_manifest(None).unwrap();
    assert_eq!(op.manifest.as_ref().unwrap().source, original_source);
}

#[test]
fn cleanup_deletes_blob() {
    // A materialization that fails after the backend wrote data must still
    // surrender that location, or the partial blob leaks.
    let mut op = ReplicateObjectOperation::new(request_with_mode(
        Ulid::generate(),
        ReplicationMode::OnDemand,
    ));

    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                path: "ref/file.txt".to_string(),
                version: None,
            })),
        },
    )))
    .unwrap();
    op.next(plain_settings()).unwrap();
    load_routing(&mut op).unwrap();
    op.next(event_result(Event::staging(
        StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        },
    )))
    .unwrap();

    let temp_location = materialized_location();
    let effects = op.next(blob(BlobEvent::Error(BlobError::WriteCleanup {
        location: temp_location.clone(),
        message: "backend writer failed".to_string(),
    })));

    assert!(effects.is_err());
    let delete = Effect::blob(BlobEffect::Delete {
        location: temp_location,
    });
    assert_eq!(op.abort(), vec![delete.into()]);
}

#[test]
fn encrypted_source_refused() {
    let version_id = Ulid::generate();
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand));

    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();

    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
        path: "ref/file.txt".to_string(),
        version: None,
    };
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access)),
        },
    )))
    .unwrap();
    // The source bucket encrypts: nothing of the reference is read or written here.
    let settings = aruna_core::structs::storage::encryption::BucketEncryption {
        mode: aruna_core::structs::storage::encryption::EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::from_parts(77, 77)),
        key_generation: 1,
        ..Default::default()
    };
    let effects = op.next(row(Some(settings.to_bytes().unwrap().into())));
    assert_eq!(effects, Err(ReplicateObjectError::EncryptedVersion));
}

#[test]
fn reference_cleans_blob() {
    let version_id = Ulid::generate();
    let mut op =
        ReplicateObjectOperation::new(request_with_mode(version_id, ReplicationMode::OnDemand));

    op.first().unwrap();
    op.next(row(Some(
        reference_blob_version().to_bytes().unwrap().into(),
    )))
    .unwrap();

    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
        path: "ref/file.txt".to_string(),
        version: None,
    };
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::VersionAccessResolved {
            result: Box::new(Ok(access)),
        },
    )))
    .unwrap();
    op.next(plain_settings()).unwrap();
    load_routing(&mut op).unwrap();
    op.next(event_result(Event::staging(
        StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        },
    )))
    .unwrap();

    let temp_location = materialized_location();
    op.next(blob(BlobEvent::WriteFinished {
        location: temp_location.clone(),
    }))
    .unwrap();
    op.next(row(None)).unwrap();
    op.next(blob(BlobEvent::ConnectionEstablished {
        stream_id: Ulid::generate(),
    }))
    .unwrap();
    op.next(blob(BlobEvent::MessageSent {
        stream_id: op.stream_id.expect("stream id available"),
    }))
    .unwrap();

    let negotiation = VersionReplicationMessage::VersionNegotiationResponse(
        ReplicationNegotiationResult::NeedBlobVersion,
    )
    .to_bytes()
    .unwrap();
    op.next(blob(BlobEvent::MessageReceived {
        stream_id: op.stream_id.expect("stream id available"),
        payload: negotiation.clone(),
    }))
    .unwrap();
    // The source bucket has no encryption settings, so the transfer goes ahead.
    let effects = op.next(row(None));
    assert!(matches!(
        (asked(&effects)).as_blob(),
        Some(BlobEffect::Replicate { .. })
    ));
    op.next(blob(BlobEvent::ReplicationFinished {
        location: temp_location.clone(),
    }))
    .unwrap();

    let apply_complete = VersionReplicationMessage::VersionApplyComplete
        .to_bytes()
        .unwrap();
    let effects = op.next(blob(BlobEvent::MessageReceived {
        stream_id: op.stream_id.expect("stream id available"),
        payload: apply_complete,
    }));

    assert_eq!(op.state, ReplicateObjectState::CleanupReferenceBlob);
    let delete = Effect::blob(BlobEffect::Delete {
        location: temp_location,
    });
    assert_eq!(asked(&effects), &delete);
    assert_eq!(
        op.result,
        ReplicationSuboperationResult::ReplicatedBytes(42)
    );
}

/// Encrypted copies: the source decides after the target's answer and its own settings.
mod sealed {
    use super::*;
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyError, BucketKeyRef, EncryptionMode, ReadLease, SealPlan,
    };
    use aruna_core::structs::storage::format::PithosLayout;
    use std::sync::Arc;

    fn source_key() -> BucketKeyRef {
        BucketKeyRef::new(Ulid::from_parts(70, 70), 2)
    }

    fn target_plan() -> SealPlan {
        SealPlan {
            key: BucketKeyRef::new(Ulid::from_parts(71, 71), 1),
            public_key: [5; 32],
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 4,
        }
    }

    fn sealed_location() -> BackendLocation {
        let mut location = materialized_location();
        let layout = PithosLayout {
            stored_size: 99,
            metadata_digest: [3; 32],
            storage_generation: 1,
        };
        location.format = StoredFormat::pithos(layout, source_key());
        location
    }

    fn encrypting() -> IoResult {
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(source_key().bucket_id),
            key_generation: source_key().generation,
            ..Default::default()
        };
        row(Some(settings.to_bytes().unwrap().into()))
    }

    /// An operation that sent the manifest of `location` and waits for the target's answer.
    fn negotiating(location: BackendLocation, plaintext: bool) -> ReplicateObjectOperation {
        let version_id = Ulid::generate();
        let mut op =
            ReplicateObjectOperation::new(version_request(version_id)).with_plaintext(plaintext);
        op.replication_version = Some(ReplicationVersion::Materialized {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            location,
            source: None,
            metadata: HashMap::new(),
        });
        op.build_manifest(None).unwrap();
        op.stream_id = Some(Ulid::generate());
        op.state = ReplicateObjectState::AwaitNegotiation;
        op
    }

    fn answer(op: &mut ReplicateObjectOperation, result: ReplicationNegotiationResult) {
        let payload = VersionReplicationMessage::VersionNegotiationResponse(result)
            .to_bytes()
            .unwrap();
        let stream_id = op.stream_id.unwrap();
        let effects = op.next(blob(BlobEvent::MessageReceived { stream_id, payload }));
        assert!(matches!(
            &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE
        ));
    }

    fn admitted(key: BucketKeyRef, location: &BackendLocation) -> IoResult {
        let lease = ReadLease::new(key, ArchiveKey::of(location), Ulid::nil(), Arc::new(()));
        blob(BlobEvent::ReadAdmitted { lease })
    }

    #[test]
    fn plain_target_refused() {
        // Without a permitted plaintext request no byte of an encrypted source leaves.
        for result in [
            ReplicationNegotiationResult::NeedBlobVersion,
            ReplicationNegotiationResult::NeedVersionOnly,
        ] {
            let mut op = negotiating(sealed_location(), false);
            answer(&mut op, result);
            let error = op.next(encrypting()).unwrap_err();
            assert_eq!(error, ReplicateObjectError::PlaintextRefused);
            let stream_id = op.stream_id.unwrap();
            let close = Effect::blob(BlobEffect::CloseConnection { stream_id });
            assert_eq!(op.abort(), vec![close.into()]);
            assert_eq!(
                error.failure_category(),
                ReplicationFailure::PlaintextRefused
            );
        }
    }

    #[test]
    fn plaintext_copy_leased() {
        // A holder's plaintext request decrypts under a lease on today's path.
        let location = sealed_location();
        let mut op = negotiating(location.clone(), true);
        answer(&mut op, ReplicationNegotiationResult::NeedBlobVersion);
        let effects = op.next(encrypting());
        assert!(
            matches!((asked(&effects)).as_blob(), Some(BlobEffect::AdmitRead { key, .. }) if *key == source_key())
        );
        let effects = op.next(admitted(source_key(), &location));
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::ReplicateLeased { regrant: None, .. })
        ));
    }

    #[test]
    fn metadata_needs_admission() {
        let location = sealed_location();
        let mut op = negotiating(location.clone(), true);
        answer(&mut op, ReplicationNegotiationResult::NeedVersionOnly);
        let effects = op.next(encrypting());
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::AdmitRead { .. })
        ));
        let effects = op.next(admitted(source_key(), &location));
        let Some(BlobEffect::SendMessage { payload, .. }) = asked(&effects).as_blob() else {
            panic!("source approval missing");
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::VersionApplyApproved
        );
        let stream_id = op.stream_id.unwrap();
        let effects = op.next(blob(BlobEvent::MessageSent { stream_id }));
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::ReadMessage { .. })
        ));
    }

    #[test]
    fn sealed_target_regrants() {
        // An encrypting target gets the archive granted to its own key.
        let location = sealed_location();
        let mut op = negotiating(location.clone(), false);
        answer(
            &mut op,
            ReplicationNegotiationResult::NeedSealedBlob(target_plan()),
        );
        op.next(encrypting()).unwrap();
        let effects = op.next(admitted(source_key(), &location));
        assert!(
            matches!((asked(&effects)).as_blob(), Some(BlobEffect::ReplicateLeased { regrant: Some(plan), object: None, .. })
                if **plan == target_plan())
        );
    }

    #[test]
    fn object_key_regrants() {
        // The target's own object key is the only object recipient; the source key is not sent.
        let location = sealed_location();
        let mut op = negotiating(location.clone(), false);
        let result = ReplicationNegotiationResult::NeedAbeBlob(target_plan(), [8; 32]);
        answer(&mut op, result);
        op.next(encrypting()).unwrap();
        let effects = op.next(admitted(source_key(), &location));
        assert!(
            matches!((asked(&effects)).as_blob(), Some(BlobEffect::ReplicateLeased {
                regrant: Some(plan),
                object: Some([8, ..]),
                ..
            }) if **plan == target_plan())
        );
    }

    #[test]
    fn locked_source_waits() {
        // A locked source key ends the item without an error; its job parks.
        let location = sealed_location();
        let mut op = negotiating(location, false);
        answer(
            &mut op,
            ReplicationNegotiationResult::NeedSealedBlob(target_plan()),
        );
        op.next(encrypting()).unwrap();
        let locked = BlobError::BucketKey(BucketKeyError::Locked(source_key().bucket_id));
        let effects = op.next(blob(BlobEvent::Error(locked)));
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::CloseConnection { .. })
        ));
        let stream_id = op.stream_id.unwrap();
        assert_eq!(
            op.next(blob(BlobEvent::ConnectionClosed { stream_id })),
            Ok(Next::Done(ReplicationSuboperationResult::AwaitingKey(
                source_key()
            )))
        );
    }

    #[test]
    fn sealed_by_target() {
        // A plain bucket sends plaintext to an encrypting target, which seals it itself.
        let mut op = negotiating(materialized_location(), false);
        answer(
            &mut op,
            ReplicationNegotiationResult::NeedSealedBlob(target_plan()),
        );
        let effects = op.next(plain_settings());
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::Replicate { .. })
        ));
    }

    #[test]
    fn pending_source_waits() {
        // A pending source version waits for its key before any connection opens.
        let location = sealed_location();
        let archive = ArchiveKey::of(&location);
        let pending = BlobVersion::pending(archive, SystemTime::now(), test_user_id(), None);
        let mut op = ReplicateObjectOperation::new(version_request(Ulid::generate()));
        op.first().unwrap();
        let effects = op.next(row(Some(pending.to_bytes().unwrap().into())));
        assert!(matches!(
            &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == aruna_core::keyspaces::PENDING_LOCATION_KEYSPACE
        ));
        let effects = op.next(row(Some(location.to_bytes().unwrap().into())));
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::AdmitRead { .. })
        ));
        let locked = BlobError::BucketKey(BucketKeyError::Locked(source_key().bucket_id));
        assert_eq!(
            op.next(blob(BlobEvent::Error(locked))),
            Ok(Next::Done(ReplicationSuboperationResult::AwaitingKey(
                source_key()
            )))
        );
    }

    #[test]
    fn scope_collects_waits() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = ReplicateScopeState::RunVersionReplication;
        let result = Ok(ReplicationSuboperationResult::AwaitingKey(source_key()));
        op.accept_item(result).unwrap();
        assert_eq!(op.result.awaiting, vec![source_key()]);
        assert_eq!(op.result.failed, 0);
    }
}
