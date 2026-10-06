//! Tests sync relationship routes for input checks, listing, pausing and delete repair.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state as build_state,
    test_storage, write_doc,
};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE,
    MIRROR_REPAIR_KEYSPACE, REPLICATION_JOB_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, PathRestriction};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BackendRef, BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, KeyState,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use tempfile::TempDir;

fn test_node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn test_realm() -> RealmId {
    RealmId::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[1u8; 32])
            .verifying_key()
            .to_bytes(),
    )
}

fn test_group() -> Ulid {
    Ulid::from_bytes([6u8; 16])
}

fn test_relationship() -> SyncRelationship {
    let realm_id = test_realm();
    SyncRelationship {
        id: Ulid::from_bytes([2u8; 16]),
        source: ArunaArn::s3_object_prefix(realm_id, test_node(3), "source", "selected/").unwrap(),
        target: ArunaArn::s3_object_prefix(realm_id, test_node(4), "target", "replica/").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: UserId::local(Ulid::from_bytes([5u8; 16]), realm_id),
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    }
}

async fn test_state() -> (TempDir, Arc<ServerState>, AuthContext, SyncRelationship) {
    let (storage_dir, storage) = test_storage();
    let relationship = test_relationship();
    let realm_id = relationship.source.realm_id;
    let node_id = relationship.source.node_id;
    let state = Arc::new(
        build_state(
            Arc::new(test_context(storage)),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let actor = Actor {
        node_id,
        user_id: relationship.created_by,
        realm_id,
    };
    let group_id = test_group();
    // Request-policy loading fails closed without the realm config document.
    seed_realm_config(&state.get_ctx(), realm_id, &actor).await;
    seed_realm_auth(&state.get_ctx(), realm_id, &actor).await;
    seed_group_docs(
        &state.get_ctx(),
        realm_id,
        &actor,
        group_id,
        "sync-test",
        relationship.created_by,
    )
    .await;
    for bucket in ["source", "target"] {
        write_doc(
            &state.get_ctx(),
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec().into(),
            BucketInfo {
                group_id,
                created_at: SystemTime::UNIX_EPOCH,
                created_by: relationship.created_by,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
            }
            .to_bytes()
            .unwrap()
            .into(),
        )
        .await;
    }
    let auth = AuthContext {
        user_id: relationship.created_by,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (storage_dir, state, auth, relationship)
}

fn create_request(target_node: NodeId) -> CreateSyncRequest {
    CreateSyncRequest {
        source: SyncSourceRequest {
            bucket: "source".to_string(),
            prefix: None,
        },
        target: SyncTargetRequest {
            node_id: target_node.to_string(),
            bucket: "target".to_string(),
            prefix: None,
        },
        mode: ApiSyncMode::Once,
        reference_handling: ApiReferenceHandling::default(),
        replicate_deletes: false,
        plaintext: false,
    }
}

async fn decrypting_source(state: &ServerState, user: UserId) {
    let key = BucketKeyRef::new(Ulid::from_bytes([7; 16]), 1);
    let settings = BucketEncryption {
        bucket_id: Some(key.bucket_id),
        key_generation: 1,
        ..Default::default()
    };
    let mut record = BucketKeyRecord::new(key, Ulid::from_bytes([8; 16]), [9; 32], 0);
    record.state = KeyState::Retiring;
    let version = BlobVersion::materialized(
        [10; 32],
        BackendRef::node_default(),
        EncodingClass::Pithos { digest: [11; 32] },
        SystemTime::UNIX_EPOCH,
        user,
        None,
    );
    for (space, row, value) in [
        (
            BUCKET_ENCRYPTION_KEYSPACE,
            b"source".to_vec(),
            settings.to_bytes().unwrap(),
        ),
        (
            BUCKET_KEY_KEYSPACE,
            key.key().to_vec(),
            record.to_bytes().unwrap(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("source", "old", Ulid::from_bytes([12; 16]))
                .to_bytes()
                .unwrap(),
            version.to_bytes().unwrap(),
        ),
    ] {
        write_doc(&state.get_ctx(), space, row.into(), value.into()).await;
    }
}

#[tokio::test]
async fn decrypting_copy_consent() {
    use crate::routes::storage::blobs::{ReplicateBlobRequest, replicate_blob};
    use aruna_operations::jobs::store::iter_prefix_page;
    use aruna_operations::replication::plaintext::{job_consent, read_consent};
    let (_dir, state, auth, _) = test_state().await;
    decrypting_source(&state, auth.user_id).await;
    let (status, _) = replicate_blob(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Json(ReplicateBlobRequest {
            bucket: "source".to_string(),
            path: Some("old".to_string()),
            version_id: Some(Ulid::from_bytes([12; 16]).to_string()),
            node_id: test_node(4).to_string(),
            plaintext: true,
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    let (jobs, _) = iter_prefix_page(
        &state.get_ctx().storage_handle,
        REPLICATION_JOB_KEYSPACE,
        None,
        None,
        2,
        None,
    )
    .await
    .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        read_consent(&state.get_ctx(), job_consent(&jobs[0].0))
            .await
            .unwrap(),
        Some(auth.user_id)
    );
}

#[tokio::test]
async fn decrypting_sync_consent() {
    use aruna_operations::replication::plaintext::read_consent;
    let (_dir, state, auth, _) = test_state().await;
    decrypting_source(&state, auth.user_id).await;
    let mut request = create_request(state.get_node_id());
    request.plaintext = true;
    let (status, Json(response)) = create_sync(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("sync-test-token"))),
        Json(request),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert!(response.plaintext);
    let id = Ulid::from_string(&response.id).unwrap();
    assert_eq!(
        read_consent(&state.get_ctx(), relationship_consent(id))
            .await
            .unwrap(),
        Some(auth.user_id)
    );
}

#[test]
fn rejects_workspace_endpoints() {
    assert!(validate_endpoint("ws-temporary", None).is_err());
    assert!(validate_endpoint("bucket", Some("")).is_err());
    assert!(validate_endpoint("bucket/name", None).is_err());
    assert!(validate_endpoint("bucket", Some("selected/")).is_ok());
}

#[test]
fn rejects_unsafe_prefixes() {
    assert!(validate_endpoint("bucket", Some("../escape")).is_err());
    assert!(validate_endpoint("bucket", Some("nested/../escape")).is_err());
    assert!(validate_endpoint("bucket", Some("/absolute")).is_err());
    assert!(validate_endpoint("bucket", Some("with\u{7}control")).is_err());
    assert!(validate_endpoint("bucket", Some("nested/prefix/")).is_ok());
}

#[test]
fn serializes_canonical_arns() {
    let relationship = test_relationship();
    let response = map_relationship(&relationship);

    assert_eq!(
        ArunaArn::parse(&response.source).unwrap(),
        relationship.source
    );
    assert_eq!(
        ArunaArn::parse(&response.target).unwrap(),
        relationship.target
    );
}

#[test]
fn filters_prefix_overlap() {
    let relationship = test_relationship();
    let results = filter_relationships(
        vec![relationship.clone()],
        relationship.created_by,
        SyncRelationshipDirection::Outgoing,
        Some("selected/nested"),
    );
    assert_eq!(results.len(), 1);

    let results = filter_relationships(
        vec![relationship.clone()],
        relationship.created_by,
        SyncRelationshipDirection::Outgoing,
        Some("other"),
    );
    assert!(results.is_empty());
}

#[tokio::test]
async fn lists_stored_relationship() {
    let (_storage_dir, state, auth, relationship) = test_state().await;
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let Json(response) = list_sync(
        State(state),
        Extension(Some(auth)),
        Query(SyncListParams::default()),
    )
    .await
    .unwrap();

    assert_eq!(response.outgoing.len(), 1);
    assert!(response.incoming.is_empty());
    assert_eq!(
        ArunaArn::parse(&response.outgoing[0].source).unwrap(),
        relationship.source
    );
}

#[test]
fn state_toggles_link() {
    // Pausing and resuming are the only state changes a caller may request,
    // and resuming a stalled relationship clears its recorded failure.
    let mut relationship = test_relationship();
    assert!(apply_state(&mut relationship, SyncState::Paused));
    assert_eq!(relationship.state, SyncState::Paused);
    assert!(!apply_state(&mut relationship, SyncState::Paused));

    relationship.state = SyncState::Failed {
        reason: "peer gone".to_string(),
    };
    relationship.status.last_error = Some("peer gone".to_string());
    relationship.status.counters.consecutive_failures = 3;
    assert!(apply_state(&mut relationship, SyncState::Enabled));
    assert_eq!(relationship.state, SyncState::Enabled);
    assert!(relationship.status.last_error.is_none());
    assert_eq!(relationship.status.counters.consecutive_failures, 0);
}

#[tokio::test]
async fn refuses_foreign_pause() {
    // Only the creator may pause a relationship, even with realm auth.
    let (_storage_dir, state, mut auth, relationship) = test_state().await;
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    auth.user_id = UserId::local(Ulid::from_bytes([9u8; 16]), auth.realm_id);

    let error = update_sync(
        State(state),
        Extension(Some(auth)),
        Extension(Some(ValidatedBearer::new_for_test("sync-test-token"))),
        Path(relationship.id.to_string()),
        Json(UpdateSyncRequest {
            reference_handling: None,
            state: Some(ApiSyncState::Paused),
        }),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ServerError::Forbidden));
}

/// Both ends on the fixture node, so the mirror write stays local.
fn local_relationship(node_id: NodeId) -> SyncRelationship {
    let mut relationship = test_relationship();
    relationship.target =
        ArunaArn::s3_object_prefix(relationship.source.realm_id, node_id, "target", "replica/")
            .unwrap();
    relationship
}

async fn store_link(state: &ServerState, relationship: &SyncRelationship) {
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();
}

async fn patch(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    relationship: &SyncRelationship,
    request: UpdateSyncRequest,
) -> ServerResult<Json<SyncRelationshipResponse>> {
    update_sync(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("sync-test-token"))),
        Path(relationship.id.to_string()),
        Json(request),
    )
    .await
}

#[tokio::test]
async fn resume_queues_backfill() {
    // Nothing is queued while a relationship is paused, so versions written
    // in the meantime only replicate if resuming catches up on them.
    let (_storage_dir, state, auth, _) = test_state().await;
    let mut relationship = local_relationship(state.get_node_id());
    relationship.state = SyncState::Paused;
    store_link(&state, &relationship).await;
    assert_eq!(load_job_stats(&state, relationship.id).await.unwrap().0, 0);

    let Json(response) = patch(
        &state,
        &auth,
        &relationship,
        UpdateSyncRequest {
            reference_handling: None,
            state: Some(ApiSyncState::Enabled),
        },
    )
    .await
    .unwrap();

    assert_eq!(response.state, "enabled");
    assert_eq!(load_job_stats(&state, relationship.id).await.unwrap().0, 1);
}

#[tokio::test]
async fn edit_skips_backfill() {
    // An enabled relationship already queues its own work, so an unrelated
    // edit must not enqueue a full pass over its scope.
    let (_storage_dir, state, auth, _) = test_state().await;
    let relationship = local_relationship(state.get_node_id());
    store_link(&state, &relationship).await;

    let Json(response) = patch(
        &state,
        &auth,
        &relationship,
        UpdateSyncRequest {
            reference_handling: Some(ApiReferenceHandling::Preserve),
            state: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(response.reference_handling, ApiReferenceHandling::Preserve);
    assert_eq!(load_job_stats(&state, relationship.id).await.unwrap().0, 0);
}

#[tokio::test]
async fn rejects_invalid_id() {
    let (_storage_dir, state, auth, _) = test_state().await;
    let error = get_sync(
        State(state),
        Extension(Some(auth)),
        Path("invalid".to_string()),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ServerError::BadRequestReason(_)));
}

#[tokio::test]
async fn rejects_restricted_run() {
    for restrictions in [
        Some(vec![PathRestriction {
            pattern: "/restricted/**".to_string(),
            permission: Permission::READ,
        }]),
        Some(Vec::new()),
    ] {
        let (_storage_dir, state, mut auth, relationship) = test_state().await;
        auth.path_restrictions = restrictions;
        let error = run_sync(
            State(state),
            Extension(Some(auth)),
            Path(relationship.id.to_string()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
    }
}

#[tokio::test]
async fn rejects_restricted_control() {
    for restrictions in [
        Some(vec![PathRestriction {
            pattern: "/restricted/**".to_string(),
            permission: Permission::READ,
        }]),
        Some(Vec::new()),
    ] {
        let (_storage_dir, state, mut auth, _) = test_state().await;
        auth.path_restrictions = restrictions;

        assert!(matches!(
            list_sync(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Query(SyncListParams::default()),
            )
            .await,
            Err(ServerError::Forbidden)
        ));
        assert!(matches!(
            get_sync(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Path("invalid".to_string()),
            )
            .await,
            Err(ServerError::Forbidden)
        ));
        assert!(matches!(
            update_sync(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Extension(None),
                Path("invalid".to_string()),
                Json(UpdateSyncRequest {
                    reference_handling: Some(ApiReferenceHandling::Materialize),
                    state: None,
                }),
            )
            .await,
            Err(ServerError::Forbidden)
        ));
        assert!(matches!(
            delete_sync(
                State(state),
                Extension(Some(auth)),
                Path("invalid".to_string()),
            )
            .await,
            Err(ServerError::Forbidden)
        ));
    }
}

#[tokio::test]
async fn rejects_restricted_create() {
    for restrictions in [
        Some(vec![PathRestriction {
            pattern: "/restricted/**".to_string(),
            permission: Permission::READ,
        }]),
        Some(Vec::new()),
    ] {
        let (_storage_dir, state, mut auth, _) = test_state().await;
        auth.path_restrictions = restrictions;
        let error = create_sync(
            State(state.clone()),
            Extension(Some(auth)),
            Extension(None),
            Json(create_request(test_node(3))),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Forbidden));
    }
}

#[tokio::test]
async fn accepts_unrestricted_create() {
    let (_storage_dir, state, auth, _) = test_state().await;
    let (status, _) = create_sync(
        State(state),
        Extension(Some(auth)),
        Extension(Some(ValidatedBearer::new_for_test("sync-test-token"))),
        Json(create_request(test_node(3))),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn mirror_denies_create() {
    let (_storage_dir, state, auth, _) = test_state().await;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    };
    let mut group_auth =
        GroupAuthorizationDocument::default_group_doc(auth.user_id, auth.realm_id, test_group());
    group_auth
        .policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny-sync-create".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "operation == 's3.PutBucketReplication'".to_string(),
            enabled: true,
        });
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key: test_group().to_bytes().to_vec().into(),
            value: group_auth.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;

    let error = create_sync(
        State(state),
        Extension(Some(auth)),
        Extension(Some(ValidatedBearer::new_for_test("sync-test-token"))),
        Json(create_request(test_node(3))),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
}

#[tokio::test]
async fn delete_preserve_detaches() {
    let (_storage_dir, state, auth, mut relationship) = test_state().await;
    relationship.set_reference_handling(ReferenceHandling::Preserve);
    relationship.set_reference_handling(ReferenceHandling::Materialize);
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    assert_eq!(
        delete_sync(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(relationship.id.to_string()),
        )
        .await
        .unwrap(),
        StatusCode::NO_CONTENT
    );

    // The outgoing record survives as a detached serving stub ...
    let stored = drive(
        GetRelationshipOperation::new(relationship.id, SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();
    assert_eq!(stored.state, SyncState::Detached);

    // ... but the management API treats the relationship as removed.
    assert!(matches!(
        get_sync(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Path(relationship.id.to_string()),
        )
        .await,
        Err(ServerError::NotFound)
    ));
    let Json(listed) = list_sync(
        State(state),
        Extension(Some(auth)),
        Query(SyncListParams::default()),
    )
    .await
    .unwrap();
    assert!(listed.outgoing.is_empty());
}

#[tokio::test]
async fn delete_stages_repair() {
    let (_storage_dir, state, auth, relationship) = test_state().await;
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    assert_eq!(
        delete_sync(
            State(state.clone()),
            Extension(Some(auth)),
            Path(relationship.id.to_string()),
        )
        .await
        .unwrap(),
        StatusCode::NO_CONTENT
    );

    let Event::Storage(StorageEvent::ReadResult { value, .. }) = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: relationship.id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing mirror repair read result");
    };
    assert!(value.is_some());
    assert!(matches!(
        get_relationship(&state, relationship.id, SyncRelationshipDirection::Outgoing,).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn delete_respects_policy() {
    let (_storage_dir, state, auth, relationship) = test_state().await;
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &state.get_ctx(),
    )
    .await
    .unwrap();

    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    };
    let mut group_auth =
        GroupAuthorizationDocument::default_group_doc(auth.user_id, auth.realm_id, test_group());
    group_auth
        .policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny-sync-delete".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "operation == 's3.DeleteBucketReplication'".to_string(),
            enabled: true,
        });
    state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key: test_group().to_bytes().to_vec().into(),
            value: group_auth.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;

    let error = delete_sync(
        State(state.clone()),
        Extension(Some(auth)),
        Path(relationship.id.to_string()),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));
    assert!(
        get_relationship(&state, relationship.id, SyncRelationshipDirection::Outgoing,)
            .await
            .is_ok()
    );
}
