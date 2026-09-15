use super::{ObjectPlacementQuery, get_object_placement};
use crate::error::ServerError;
use crate::openapi::ApiDoc;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::placement::placement_policy::{
    PlacementPolicy, PlacementPolicyRef, PlacementSelector, VerifiedPolicy,
};
use aruna_core::structs::placement::policy_document::{
    PlacementPolicyDocument, PolicyPublicationClaim, placement_policy_key,
};
use aruna_core::structs::storage::blob::{
    BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_storage::FjallStorage;
use axum::Extension;
use axum::extract::{Path, Query, State};
use byteview::ByteView;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

const BUCKET: &str = "datasets";
const KEY: &str = "raw/sample.fastq";

fn node_id() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[3u8; 32]).public()
}

fn realm_id() -> RealmId {
    RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .as_bytes(),
    )
}

fn policy() -> VerifiedPolicy {
    let policy = PlacementPolicy::new(
        Ulid::from_bytes([4u8; 16]),
        "eu-residency".to_string(),
        vec![PlacementSelector {
            node_id: None,
            location: Some("eu-west".to_string()),
            labels: Vec::new(),
            executor_kind: None,
        }],
    )
    .expect("policy is valid");
    VerifiedPolicy::verify(policy).expect("policy verifies")
}

async fn write_fixture(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected fixture write event: {other:?}"),
    }
}

/// One governed object whose head this node holds, plus the rule its ref
/// names, so a name can be resolved locally.
async fn setup(owner: UserId) -> (TempDir, Arc<ServerState>, Ulid) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let realm_id = realm_id();
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
            JobsRuntime::new(),
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
        realm_id.as_bytes().to_vec(),
        RealmAuthorizationDocument::default_realm_doc(realm_id)
            .to_bytes(&actor)
            .expect("realm auth serializes"),
    )
    .await;
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    write_fixture(
        &state,
        AUTH_KEYSPACE,
        group_id.to_bytes().to_vec(),
        group_auth.to_bytes(&actor).expect("group auth serializes"),
    )
    .await;
    write_fixture(
        &state,
        GROUP_KEYSPACE,
        group_id.to_bytes().to_vec(),
        Group {
            display_name: "placement-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        }
        .to_bytes(&actor)
        .expect("group serializes"),
    )
    .await;
    write_fixture(
        &state,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        RealmConfigDocument::default_for_realm(realm_id, Vec::new())
            .to_bytes(&actor)
            .expect("realm config serializes"),
    )
    .await;
    write_fixture(
        &state,
        S3_BUCKET_KEYSPACE,
        BUCKET.as_bytes().to_vec(),
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
        .to_bytes()
        .expect("bucket serializes"),
    )
    .await;

    let version_id = Ulid::from_bytes([9u8; 16]);
    let policy = policy();
    let version = BlobVersion::materialized(
        [7u8; 32],
        BackendRef::node_default(),
        SystemTime::UNIX_EPOCH,
        owner,
        None,
    )
    .with_policies(vec![policy.policy_ref()])
    .expect("refs stored");
    write_fixture(
        &state,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(BUCKET, KEY).to_bytes().expect("head key"),
        CurrentVersionPointer::new_with_generation(version_id, 7)
            .to_bytes()
            .expect("pointer serializes"),
    )
    .await;
    write_fixture(
        &state,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(BUCKET, KEY, version_id)
            .to_bytes()
            .expect("version key"),
        version.to_bytes().expect("version serializes"),
    )
    .await;
    let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
    let publication = PolicyPublicationClaim::new(
        realm_id,
        &policy,
        secret.public(),
        owner,
        Ulid::from_bytes([5u8; 16]),
        7,
        [0u8; 32],
    )
    .sign(&secret);
    let document = PlacementPolicyDocument::new(realm_id, &policy, publication);
    write_fixture(
        &state,
        aruna_core::keyspaces::PLACEMENT_POLICY_KEYSPACE,
        placement_policy_key(policy.policy().policy_id),
        document.to_bytes().expect("document serializes"),
    )
    .await;
    (dir, state, version_id)
}

fn auth(owner: UserId) -> AuthContext {
    AuthContext {
        user_id: owner,
        realm_id: realm_id(),
        path_restrictions: None,
        session: None,
    }
}

#[tokio::test]
async fn reads_object_refs() {
    // The head generation and version id are exactly what an exact-set
    // change presents, and a ref this node holds carries its name.
    let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
    let (_dir, state, version_id) = setup(owner).await;

    let Ok(axum::Json(view)) = get_object_placement(
        State(state),
        Extension(Some(auth(owner))),
        Path(BUCKET.to_string()),
        Query(ObjectPlacementQuery {
            key: KEY.to_string(),
        }),
    )
    .await
    else {
        panic!("the object placement read must succeed");
    };

    assert_eq!(view.bucket, BUCKET);
    assert_eq!(view.key, KEY);
    assert_eq!(view.version_id, version_id.to_string());
    assert_eq!(view.generation, 7);
    assert_eq!(view.policies.len(), 1);
    assert_eq!(view.policies[0].name.as_deref(), Some("eu-residency"));
    assert!(view.policies[0].owner_group_id.is_none());
    assert_eq!(
        PlacementPolicyRef::try_from(view.policies[0].clone()).expect("ref parses"),
        policy().policy_ref()
    );
}

#[tokio::test]
async fn unknown_key_missing() {
    let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
    let (_dir, state, _) = setup(owner).await;

    let error = get_object_placement(
        State(state),
        Extension(Some(auth(owner))),
        Path(BUCKET.to_string()),
        Query(ObjectPlacementQuery {
            key: "raw/missing.fastq".to_string(),
        }),
    )
    .await
    .expect_err("an unknown key has no head");

    assert!(matches!(error, ServerError::NotFound));
}

#[tokio::test]
async fn refuses_foreign_reader() {
    // A realm member outside the bucket's group holds no READ on the object.
    let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
    let (_dir, state, _) = setup(owner).await;
    let outsider = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id());

    let error = get_object_placement(
        State(state),
        Extension(Some(auth(outsider))),
        Path(BUCKET.to_string()),
        Query(ObjectPlacementQuery {
            key: KEY.to_string(),
        }),
    )
    .await
    .expect_err("a caller without READ is refused");

    assert!(matches!(error, ServerError::Forbidden));
}

#[test]
fn openapi_lists_objects() {
    let openapi = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
    let path = &openapi["paths"]["/data/buckets/{bucket}/placement/objects"];
    assert!(path.get("get").is_some());
    assert!(path.get("post").is_some());
    assert!(
        openapi["components"]["schemas"]
            .get("ObjectPlacementView")
            .is_some()
    );
}
