use super::{
    DrsBulkBody, GetObjectError, MAX_BULK_OBJECT_IDS, RequestedObjectId, ResolveOutcome,
    ResolvedObject, W3ID_DATA_PREFIX, build_object_response, download_error, drs_denied_error,
    encode_component, get_authorizations, get_object, parse_object_id, post_objects,
    resolve_object, routed_deadline,
};
use crate::openapi::ApiDoc;
use crate::server_state::ServerState;
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state as build_state,
    test_storage,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::{
    Actor, AuthContext, BackendLocation, BackendRef, BlobLocationKey, BlobVersion, BucketInfo,
    NodeCapabilities, RealmId, SourceMetadata, VersionKey, VersionedObjectArn,
};
use aruna_core::{NodeId, UserId};
use axum::Extension;
use axum::body::to_bytes;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

fn materialized_location(blake3: [u8; 32]) -> BackendLocation {
    let mut hashes = HashMap::new();
    hashes.insert("blake3".to_string(), blake3.to_vec());
    hashes.insert("sha256".to_string(), vec![0xabu8; 32]);
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "objects".to_string(),
        backend_path: "blob.bin".to_string(),
        ulid: Ulid::from_bytes([2u8; 16]),
        compressed: false,
        encrypted: false,
        created_by: UserId::nil(RealmId([3u8; 32])),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes,
    }
}

fn test_realm_id() -> RealmId {
    RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .as_bytes(),
    )
}

fn test_node_id() -> NodeId {
    NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6").unwrap()
}

async fn test_state() -> (TempDir, Arc<ServerState>) {
    let (dir, storage) = test_storage();
    let state = build_state(
        Arc::new(test_context(storage)),
        test_realm_id(),
        test_node_id(),
        NodeCapabilities::user_node(test_realm_id()).expect("capabilities"),
    )
    .await;
    (dir, Arc::new(state))
}

async fn write_fixture(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected fixture write event: {other:?}"),
    }
}

#[tokio::test]
async fn authorizations_shape() {
    let (_dir, state) = test_state().await;
    let response = get_authorizations(State(state), Path("object/id".to_string())).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({
            "drs_object_id": "object/id",
            "supported_types": ["BearerAuth"],
            "passport_auth_issuers": [],
            "bearer_auth_issuers": []
        })
    );
}

async fn seed_version(state: &ServerState) -> (AuthContext, AuthContext, VersionedObjectArn) {
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let group_id = Ulid::from_bytes([4u8; 16]);
    let owner = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
    let denied = UserId::new(Ulid::from_bytes([6u8; 16]), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    // Request-policy loading fails closed without the realm config, the group
    // record, and the group auth document.
    seed_realm_config(&state.get_ctx(), realm_id, &actor).await;
    seed_group_docs(
        &state.get_ctx(),
        realm_id,
        &actor,
        group_id,
        "drs-group",
        owner,
    )
    .await;
    seed_realm_auth(&state.get_ctx(), realm_id, &actor).await;

    let bucket = "mybucket";
    let key = "path/file @ 1.txt";
    let version = Ulid::from_bytes([7u8; 16]);
    let hash = [0x33u8; 32];
    let location = materialized_location(hash);
    let bucket_info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_fixture(
        state,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().to_vec(),
        bucket_info.to_bytes().expect("bucket serializes"),
    )
    .await;
    write_fixture(
        state,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version)
            .to_bytes()
            .expect("version key serializes"),
        BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            SystemTime::UNIX_EPOCH,
            owner,
            None,
        )
        .to_bytes()
        .expect("version serializes"),
    )
    .await;
    write_fixture(
        state,
        BLOB_LOCATIONS_KEYSPACE,
        BlobLocationKey::new(hash, location.backend.clone()).to_bytes(),
        location.to_bytes().expect("location serializes"),
    )
    .await;

    (
        AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        AuthContext {
            user_id: denied,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        VersionedObjectArn::new(realm_id, node_id, bucket, key, version).expect("versioned ARN"),
    )
}

#[test]
fn anonymous_denial_concealed() {
    let anonymous = drs_denied_error(true);
    assert_eq!(anonymous.status, axum::http::StatusCode::NOT_FOUND);
    assert_eq!(anonymous.message, "DRS object not found");

    let authenticated = drs_denied_error(false);
    assert_eq!(authenticated.status, axum::http::StatusCode::FORBIDDEN);
    assert_eq!(authenticated.message, "Forbidden");
}

#[test]
fn parses_w3id() {
    let expected_hash = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    let parsed = parse_object_id(
            "https://w3id.org/aruna/data/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        )
        .unwrap();

    match parsed {
        RequestedObjectId::CanonicalW3id(hash) => assert_eq!(hash, expected_hash),
        RequestedObjectId::ContentHashArn { .. } => panic!("expected canonical w3id id"),
        RequestedObjectId::VersionedObject(_) => panic!("expected canonical w3id id"),
    }
}

#[test]
fn parses_content_arn() {
    let realm_id = test_realm_id();
    let node_id = test_node_id();
    let arn = format!(
        "arn:aruna:{realm_id}:{node_id}:ch/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
    );

    let parsed = parse_object_id(&arn).unwrap();

    match parsed {
        RequestedObjectId::ContentHashArn {
            realm_id: parsed_realm_id,
            node_id: parsed_node_id,
            hash,
        } => {
            assert_eq!(parsed_realm_id, realm_id);
            assert_eq!(parsed_node_id, node_id);
            assert_eq!(
                hash,
                [
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                    0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
                    0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
                ]
            );
        }
        RequestedObjectId::CanonicalW3id(_) => panic!("expected content-hash arn"),
        RequestedObjectId::VersionedObject(_) => panic!("expected content-hash arn"),
    }
}

#[test]
fn rejects_malformed_version() {
    let realm_id = test_realm_id();
    let node_id = test_node_id();
    let bare = format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket/path/file.txt@invalid");

    for object_id in [bare.clone(), format!("{W3ID_DATA_PREFIX}{bare}")] {
        let error = parse_object_id(&object_id)
            .err()
            .expect("malformed version should be rejected");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("versioned object ARN has an invalid ULID")
        );
    }
}

#[tokio::test]
async fn resolves_versioned_ids() {
    let (_dir, state) = test_state().await;
    let (auth, _, arn) = seed_version(state.as_ref()).await;

    for object_id in [arn.to_string(), arn.to_w3id()] {
        let outcome = resolve_object(state.as_ref(), &auth, &object_id, routed_deadline())
            .await
            .expect("version resolves");
        let ResolveOutcome::Found(resolved) = outcome else {
            panic!("expected resolved version");
        };
        assert_eq!(resolved.bucket, arn.bucket);
        assert_eq!(resolved.key, arn.key);
        assert_eq!(resolved.version_id, arn.version);
        assert_eq!(resolved.group_id, Ulid::from_bytes([4u8; 16]));
    }
}

#[tokio::test]
async fn rejects_nonlocal_version() {
    let (_dir, state) = test_state().await;
    let auth = AuthContext {
        user_id: UserId::new(Ulid::from_bytes([5u8; 16]), state.get_realm_id()),
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    };
    let other_node = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let other_realm = RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[9u8; 32])
            .verifying_key()
            .as_bytes(),
    );
    let version = Ulid::from_bytes([7u8; 16]);
    // A foreign realm is definitive absence; a foreign node is only unproven.
    let cases = [
        (
            VersionedObjectArn::new(
                other_realm,
                state.get_node_id(),
                "mybucket",
                "path/file.txt",
                version,
            )
            .unwrap(),
            StatusCode::NOT_FOUND,
        ),
        (
            VersionedObjectArn::new(
                state.get_realm_id(),
                other_node,
                "mybucket",
                "path/file.txt",
                version,
            )
            .unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ];

    for (arn, expected) in cases {
        let response = get_object(
            State(state.clone()),
            Extension(Some(auth.clone())),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            HeaderMap::new(),
            Path(arn.to_string()),
        )
        .await;
        assert_eq!(response.status(), expected);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("typed error body");
        assert_eq!(payload["status_code"], expected.as_u16());
    }
}

#[tokio::test]
async fn enforces_version_auth() {
    let (_dir, state) = test_state().await;
    let (_, denied, arn) = seed_version(state.as_ref()).await;

    let outcome = resolve_object(state.as_ref(), &denied, &arn.to_string(), routed_deadline())
        .await
        .expect("authorization resolves");
    assert!(matches!(outcome, ResolveOutcome::Denied));
}

#[tokio::test]
async fn returns_missing_version() {
    let (_dir, state) = test_state().await;
    let (auth, _, arn) = seed_version(state.as_ref()).await;
    let missing = VersionedObjectArn::new(
        arn.realm_id,
        arn.node_id,
        arn.bucket,
        arn.key,
        Ulid::from_bytes([8u8; 16]),
    )
    .unwrap();

    let response = get_object(
        State(state),
        Extension(Some(auth)),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        HeaderMap::new(),
        Path(missing.to_string()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("typed error body");
    assert_eq!(payload["status_code"], 404);
    assert_eq!(payload["msg"], "DRS object not found");
}

#[tokio::test]
async fn caps_bulk_ids() {
    // An uncapped list would let one anonymous request spend a routed probe
    // per identifier, so the cap is refused before anything is resolved.
    let (_dir, state) = test_state().await;
    let id = format!("{W3ID_DATA_PREFIX}{}", hex::encode([1u8; 32]));
    let object_ids = std::iter::repeat_n(id, MAX_BULK_OBJECT_IDS + 1).collect();

    let response = post_objects(
        State(state),
        Extension(None),
        ConnectInfo("127.0.0.1:1".parse().unwrap()),
        HeaderMap::new(),
        axum::Json(DrsBulkBody { object_ids }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn canonical_response_complete() {
    let blake3 = [0x11u8; 32];
    let canonical_w3id = format!("{W3ID_DATA_PREFIX}{}", hex::encode(blake3));
    let resolved = ResolvedObject {
        bucket: "mybucket".to_string(),
        key: "path/file.txt".to_string(),
        group_id: Ulid::from_bytes([4u8; 16]),
        version_id: Ulid::from_bytes([5u8; 16]),
        canonical_w3id: canonical_w3id.clone(),
        requested_id: canonical_w3id.clone(),
        size: 42,
        hashes: materialized_location(blake3).hashes.into_iter().collect(),
        location: Some(materialized_location(blake3)),
        source_metadata: Some(SourceMetadata {
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag-from-materialized".to_string()),
            last_modified: None,
            source_version: None,
        }),
    };

    let response = build_object_response("https://drs.example.test", &resolved);

    assert!(response.aliases.is_empty());
    assert_eq!(response.id, canonical_w3id);
    assert_eq!(response.checksums.len(), 2);
    assert!(
        response
            .checksums
            .iter()
            .any(|checksum| checksum.kind == "blake3" && checksum.checksum == hex::encode(blake3))
    );
    assert!(
        response
            .checksums
            .iter()
            .any(|checksum| checksum.kind == "sha256" && checksum.checksum == "ab".repeat(32))
    );
    assert_eq!(response.access_methods.len(), 1);
    assert_eq!(response.access_methods[0].kind, "https");
    assert_eq!(
        response.access_methods[0].access_url.as_ref().unwrap().url,
        format!(
            "https://drs.example.test/api/v1/ga4gh/drs/v1/download?object_id={}",
            encode_component(&canonical_w3id)
        )
    );
}

#[test]
fn content_response_aliases() {
    let realm_id = test_realm_id();
    let node_id = test_node_id();
    let blake3 = [0x22u8; 32];
    let canonical_w3id = format!("{W3ID_DATA_PREFIX}{}", hex::encode(blake3));
    let requested_id = format!("arn:aruna:{realm_id}:{node_id}:ch/{}", hex::encode(blake3));
    let resolved = ResolvedObject {
        bucket: "mybucket".to_string(),
        key: "path/file.txt".to_string(),
        group_id: Ulid::from_bytes([6u8; 16]),
        version_id: Ulid::from_bytes([7u8; 16]),
        canonical_w3id: canonical_w3id.clone(),
        requested_id: requested_id.clone(),
        size: 42,
        hashes: materialized_location(blake3).hashes.into_iter().collect(),
        location: Some(materialized_location(blake3)),
        source_metadata: Some(SourceMetadata {
            content_length: 42,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("etag-from-materialized".to_string()),
            last_modified: None,
            source_version: None,
        }),
    };

    let response = build_object_response("https://drs.example.test", &resolved);

    assert_eq!(response.id, requested_id);
    assert_eq!(response.aliases, vec![canonical_w3id.clone()]);
    assert_eq!(response.checksums.len(), 2);
    assert_eq!(response.access_methods.len(), 1);
    assert_eq!(
        response.access_methods[0].access_url.as_ref().unwrap().url,
        format!(
            "https://drs.example.test/api/v1/ga4gh/drs/v1/download?object_id={}",
            encode_component(&requested_id)
        )
    );
}

// A lost historical observation is not a server fault, drift is transient,
// and an exhausted binding needs a rebind: three distinct statuses.
#[test]
fn maps_reference_errors() {
    for (error, status) in [
        (
            GetObjectError::HistoricalReferenceUnavailable,
            StatusCode::NOT_FOUND,
        ),
        (
            GetObjectError::ReferenceSourceChanged,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            GetObjectError::ReferenceAdvanceExhausted,
            StatusCode::CONFLICT,
        ),
        (GetObjectError::NoSuchKey, StatusCode::NOT_FOUND),
        (
            GetObjectError::GetObjectFailed,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        assert_eq!(download_error(error).status(), status);
    }
}

// Operators reading the spec must see the reference statuses the route can
// actually return.
#[test]
fn download_declares_statuses() {
    let openapi = ApiDoc::openapi();
    let responses = &openapi
        .paths
        .paths
        .get("/ga4gh/drs/v1/download")
        .expect("download path")
        .get
        .as_ref()
        .expect("download operation")
        .responses
        .responses;
    for status in ["404", "409", "503"] {
        assert!(responses.contains_key(status), "missing {status}");
    }
}

#[test]
fn openapi_has_drs() {
    let openapi = ApiDoc::openapi();
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/ga4gh/drs/v1/service-info")
    );
    assert!(openapi.paths.paths.contains_key("/ga4gh/drs/v1/objects"));
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/ga4gh/drs/v1/objects/{object_id}")
    );
    assert!(openapi.paths.paths.contains_key("/ga4gh/drs/v1/download"));
    let _ = W3ID_DATA_PREFIX;
}
