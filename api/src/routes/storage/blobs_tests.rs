use super::{
    BlobCopyCompliance, BlobCopyOrigin, BlobCopyState, BlobCopyStorage, BlobLocationsQuery,
    blob_locations, copy_response, pending_copy,
};
use crate::error::ServerError;
use crate::openapi::ApiDoc;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::blob::CopyOrigin;
use aruna_operations::driver::DriverContext;
use aruna_operations::replication::locations::LocationSummaryError;
use aruna_operations::replication::protocol::CopyCompliance;
use aruna_operations::replication::protocol::{LocationCopyStorage, LocationSummary};
use aruna_storage::FjallStorage;
use axum::Extension;
use axum::extract::{Query, State};
use byteview::ByteView;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

fn node_id() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[3u8; 32]).public()
}

#[test]
fn hides_backend_name() {
    // Node-managed copies expose the class only; backend names stay operator-side.
    let copy = copy_response(
        &(node_id(), "raw".to_string(), "a.tar".to_string()),
        true,
        LocationSummary {
            version_id: Some(Ulid::from_bytes([1u8; 16])),
            held: true,
            storage: Some(LocationCopyStorage::NodeManaged {
                storage_class: Some("cold".to_string()),
            }),
            origin: CopyOrigin::Write,
            compliance: CopyCompliance::Allowed,
            materialized: true,
            group_id: None,
            blob_size: None,
            hashes: Default::default(),
        },
    );

    assert_eq!(copy.state, BlobCopyState::Present);
    assert_eq!(copy.storage, Some(BlobCopyStorage::NodeManaged));
    assert_eq!(copy.storage_class.as_deref(), Some("cold"));
    assert!(copy.group_backend_id.is_none());
}

#[test]
fn names_group_backend() {
    let backend_id = Ulid::from_bytes([9u8; 16]);
    let copy = copy_response(
        &(node_id(), "raw".to_string(), "a.tar".to_string()),
        false,
        LocationSummary {
            version_id: Some(Ulid::from_bytes([1u8; 16])),
            held: true,
            storage: Some(LocationCopyStorage::GroupBackend {
                backend_id,
                name: Some("lab-minio".to_string()),
            }),
            origin: CopyOrigin::Write,
            compliance: CopyCompliance::Allowed,
            materialized: true,
            group_id: None,
            blob_size: None,
            hashes: Default::default(),
        },
    );

    assert_eq!(copy.storage, Some(BlobCopyStorage::GroupBackend));
    assert_eq!(
        copy.group_backend_id.as_deref(),
        Some(&*backend_id.to_string())
    );
    assert_eq!(copy.group_backend_name.as_deref(), Some("lab-minio"));
    assert!(copy.storage_class.is_none());
}

#[test]
fn reports_unstored_version() {
    // A delete marker resolves a version that holds bytes nowhere.
    let copy = copy_response(
        &(node_id(), "raw".to_string(), "a.tar".to_string()),
        false,
        LocationSummary {
            version_id: Some(Ulid::from_bytes([1u8; 16])),
            held: false,
            storage: None,
            origin: CopyOrigin::Unknown,
            compliance: CopyCompliance::Unknown,
            materialized: false,
            group_id: None,
            blob_size: None,
            hashes: Default::default(),
        },
    );

    assert_eq!(copy.state, BlobCopyState::NotStored);
    assert!(copy.storage.is_none());
    assert_eq!(
        serde_json::to_value(BlobCopyState::NotStored).unwrap(),
        serde_json::json!("not-stored")
    );
}

#[test]
fn lists_quarantined_copy() {
    // A copy the holder refuses to serve is present with its verdict, and a
    // sync-placed copy names the relationship that put it there.
    let relationship_id = Ulid::from_bytes([6u8; 16]);
    let copy = copy_response(
        &(node_id(), "raw".to_string(), "a.tar".to_string()),
        false,
        LocationSummary {
            version_id: Some(Ulid::from_bytes([1u8; 16])),
            held: true,
            storage: Some(LocationCopyStorage::NodeManaged {
                storage_class: None,
            }),
            origin: CopyOrigin::Sync { relationship_id },
            compliance: CopyCompliance::Quarantined,
            materialized: true,
            group_id: None,
            blob_size: None,
            hashes: Default::default(),
        },
    );

    assert_eq!(copy.state, BlobCopyState::Present);
    assert_eq!(copy.origin, BlobCopyOrigin::Sync);
    assert_eq!(copy.compliance, BlobCopyCompliance::Quarantined);
    assert_eq!(
        copy.sync_relationship_id.as_deref(),
        Some(&*relationship_id.to_string())
    );
    assert_eq!(
        serde_json::to_value(BlobCopyCompliance::Quarantined).unwrap(),
        serde_json::json!("quarantined")
    );
    assert_eq!(
        serde_json::to_value(BlobCopyOrigin::Sync).unwrap(),
        serde_json::json!("sync")
    );
}

#[test]
fn absent_copy_pends() {
    // No version at the peer is a copy still to come, not a missing one.
    let destination = (node_id(), "archive".to_string(), "images/a.jpg".to_string());
    let copy = copy_response(&destination, false, LocationSummary::absent());

    assert_eq!(copy.state, BlobCopyState::Pending);
    assert!(copy.storage.is_none());
    // The mapped destination travels with the answer, so two copies on one
    // node stay apart.
    assert_eq!(copy.bucket, "archive");
    assert_eq!(copy.key, "images/a.jpg");
    assert_eq!(
        pending_copy(&destination, BlobCopyState::Unreachable).state,
        BlobCopyState::Unreachable
    );
}

#[test]
fn caps_candidates() {
    // Past the cap a destination is dropped, and the caller has to be told.
    let mut candidates = std::collections::BTreeSet::new();
    for seed in 0..super::LOCATION_CANDIDATE_LIMIT {
        let node = iroh::SecretKey::from_bytes(&[seed as u8 + 1; 32]).public();
        assert!(super::add_candidate(
            &mut candidates,
            (node, "raw".to_string(), "a.tar".to_string())
        ));
    }

    let extra = iroh::SecretKey::from_bytes(&[0u8; 32]).public();
    assert!(!super::add_candidate(
        &mut candidates,
        (extra, "raw".to_string(), "a.tar".to_string())
    ));
    assert_eq!(candidates.len(), super::LOCATION_CANDIDATE_LIMIT);

    let known = candidates.iter().next().unwrap().0;
    assert!(super::add_candidate(
        &mut candidates,
        (known, "raw".to_string(), "a.tar".to_string())
    ));
}

#[test]
fn keeps_mapped_path() {
    // The same node under two destination paths is two questions: dropping
    // one would hide the copy stored under the other.
    let mut candidates = std::collections::BTreeSet::new();
    let node = node_id();
    assert!(super::add_candidate(
        &mut candidates,
        (node, "archive".to_string(), "images/a.jpg".to_string())
    ));
    assert!(super::add_candidate(
        &mut candidates,
        (node, "raw".to_string(), "photos/a.jpg".to_string())
    ));

    assert_eq!(candidates.len(), 2);
}

#[test]
fn drops_unheld_holder() {
    // Identical bytes under another object are not this version's copy.
    // Configured and queued targets remain copies before receipt.
    let absent = LocationSummary::absent();
    let destination = (node_id(), "raw".to_string(), "a.tar".to_string());
    assert!(super::peer_copy(&destination, false, Ok(absent.clone())).is_none());
    assert_eq!(
        super::peer_copy(&destination, true, Ok(absent.clone())).map(|copy| copy.state),
        Some(BlobCopyState::Pending)
    );
    assert_eq!(
        super::peer_copy(&destination, false, Err(LocationSummaryError::Denied))
            .map(|copy| copy.state),
        Some(BlobCopyState::Denied)
    );
    assert_eq!(
        super::peer_copy(&destination, false, Err(LocationSummaryError::Aborted))
            .map(|copy| copy.state),
        Some(BlobCopyState::Unreachable)
    );
}

#[test]
fn openapi_lists_locations() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

    assert!(openapi["paths"].get("/data/blobs/locations").is_some());
    assert!(
        openapi["components"]["schemas"]["LocationScanLimit"]["enum"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("holder-path-unknown"))
    );
    assert!(
        openapi["components"]["schemas"]["BlobCopyState"]["enum"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("not-stored"))
    );
    assert!(
        openapi["components"]["schemas"]["BlobCopyResponse"]["properties"]
            .get("key")
            .is_some()
    );
    assert!(
        openapi["components"]["schemas"]["BlobLocationsResponse"]["properties"]
            .get("complete")
            .is_some()
    );
}

#[test]
fn openapi_has_replication() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

    assert!(openapi["paths"].get("/data/blobs/replicate").is_some());
    assert!(
        openapi["components"]["schemas"]["ReplicateBlobResponse"]["properties"]
            .get("bucket")
            .is_some()
    );
    assert!(
        openapi["components"]["schemas"]["ReplicateBlobResponse"]["properties"]
            .get("target_node_id")
            .is_some()
    );
}

const TEST_BUCKET: &str = "locations-bucket";

fn realm_for(seed: u8) -> RealmId {
    RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .as_bytes(),
    )
}

async fn write_fixture(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected fixture write event: {other:?}"),
    }
}

/// A realm holding one group whose owner may read `TEST_BUCKET`.
async fn setup_bucket(realm_id: RealmId, owner: UserId) -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let state = Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            realm_id,
            node_id(),
            NodeCapabilities::user_node(realm_id).expect("capabilities"),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );
    let group_id = Ulid::from_bytes([11u8; 16]);
    let actor = Actor {
        node_id: node_id(),
        user_id: owner,
        realm_id,
    };
    write_fixture(
        &state,
        AUTH_KEYSPACE,
        ByteView::from(realm_id.as_bytes().to_vec()),
        ByteView::from(
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .expect("realm auth serializes"),
        ),
    )
    .await;
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    write_fixture(
        &state,
        AUTH_KEYSPACE,
        ByteView::from(group_id.to_bytes().to_vec()),
        ByteView::from(group_auth.to_bytes(&actor).expect("group auth serializes")),
    )
    .await;
    // Policy loading resolves the group record before group policies apply.
    write_fixture(
        &state,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        ByteView::from(group_id.to_bytes().to_vec()),
        ByteView::from(
            aruna_core::structs::identity::group::Group {
                display_name: "blob-group".to_string(),
                group_id,
                realm_id,
                roles: group_auth.roles.keys().copied().collect(),
                owner,
            }
            .to_bytes(&actor)
            .expect("group serializes"),
        ),
    )
    .await;
    write_fixture(
        &state,
        REALM_CONFIG_KEYSPACE,
        ByteView::from(realm_id.as_bytes().to_vec()),
        ByteView::from(
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .expect("realm config serializes"),
        ),
    )
    .await;
    write_fixture(
        &state,
        S3_BUCKET_KEYSPACE,
        ByteView::from(TEST_BUCKET.as_bytes().to_vec()),
        ByteView::from(
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: owner,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            }
            .to_bytes()
            .expect("bucket serializes"),
        ),
    )
    .await;
    (dir, state)
}

async fn install_deny_policy(state: &ServerState, realm_id: RealmId, expression: &str) {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.request_policies.push(RequestPolicy {
        policy_id: Ulid::generate(),
        name: "deny".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: expression.to_string(),
        enabled: true,
    });
    let actor = Actor {
        node_id: node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    write_fixture(
        state,
        REALM_CONFIG_KEYSPACE,
        ByteView::from(realm_id.as_bytes().to_vec()),
        ByteView::from(config.to_bytes(&actor).expect("config serializes")),
    )
    .await;
}

fn locations_query() -> Query<BlobLocationsQuery> {
    Query(BlobLocationsQuery {
        bucket: TEST_BUCKET.to_string(),
        path: "reports/a.csv".to_string(),
        version_id: None,
    })
}

fn auth_for(user_id: UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

#[tokio::test]
async fn locations_require_read() {
    // A member of no group must not learn where an object is held; the owner
    // passes authorization and only then reports the absent object.
    let realm_id = realm_for(31);
    let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
    let stranger = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
    let (_dir, state) = setup_bucket(realm_id, owner).await;

    let denied = blob_locations(
        State(state.clone()),
        Extension(Some(auth_for(stranger, realm_id))),
        locations_query(),
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    let allowed = blob_locations(
        State(state),
        Extension(Some(auth_for(owner, realm_id))),
        locations_query(),
    )
    .await;
    assert!(matches!(allowed, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn policy_denies_locations() {
    // A realm deny-reads policy must reach the location read, which only
    // ordinary RBAC used to gate.
    let realm_id = realm_for(32);
    let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
    let (_dir, state) = setup_bucket(realm_id, owner).await;
    install_deny_policy(&state, realm_id, "permission == 'read'").await;

    let denied = blob_locations(
        State(state),
        Extension(Some(auth_for(owner, realm_id))),
        locations_query(),
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
}
