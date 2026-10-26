//! Tests the origin and sources routes: the answer shape, permissions, relayed reads to a
//! holder and the incoming paths of a bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api, write,
};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, RELATIONSHIP_IN_KEYSPACE};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::source_access::SourceDigest;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncRelationship, SyncState, SyncStatusSnapshot, sync_relationship_key};
use aruna_core::{NodeId, UserId};
use axum::http::StatusCode;
use std::time::SystemTime;

async fn store(node: &Node, key: &str) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let input = WriteObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        text: "x".to_string(),
        content_type: None,
    };
    store_text(&node.state, &node.owner, &info, input)
        .await
        .unwrap();
}

async fn origin(
    node: &Node,
    auth: &AuthContext,
    headers: HeaderMap,
    bucket: &str,
    key: &str,
    version_id: Option<Ulid>,
) -> ServerResult<OriginResponse> {
    let params = OriginParams {
        key: key.to_string(),
        version_id,
    };
    get_origin(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(bucket.to_string()),
        Query(params),
    )
    .await
    .map(|Json(response)| response)
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[41; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([3; 32])
}

#[tokio::test]
async fn reads_local_origin() {
    let node = node_state().await;
    store(&node, "a.md").await;
    let current = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    assert_eq!(current.created_by, node.owner.user_id.to_string());
    assert!(current.global_id.is_none());
    assert!(current.projection.is_empty(), "uploads project nothing");
    let version = Ulid::from_string(&current.version_id).unwrap();
    let named = origin(
        &node,
        &node.owner,
        HeaderMap::new(),
        BUCKET,
        "a.md",
        Some(version),
    )
    .await
    .unwrap();
    assert_eq!(named.version_id, current.version_id);
}

#[tokio::test]
async fn origin_refusals() {
    let node = node_state().await;
    store(&node, "a.md").await;
    let denied = origin(
        &node,
        &node.stranger,
        HeaderMap::new(),
        BUCKET,
        "a.md",
        None,
    )
    .await;
    assert!(matches!(denied, Err(ServerError::Forbidden)));
    let no_key = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "b.md", None).await;
    assert!(matches!(no_key, Err(ServerError::NotFound)));
    let no_version = Some(Ulid::from_bytes([42; 16]));
    let unknown = origin(
        &node,
        &node.owner,
        HeaderMap::new(),
        BUCKET,
        "a.md",
        no_version,
    )
    .await;
    assert!(matches!(unknown, Err(ServerError::NotFound)));
    let no_bucket = origin(&node, &node.owner, HeaderMap::new(), "absent", "a.md", None).await;
    assert!(matches!(no_bucket, Err(ServerError::NotFound)));
}

fn answer() -> String {
    serde_json::json!({
        "version_id": Ulid::from_bytes([43; 16]).to_string(),
        "global_id": null,
        "created_by": "peer-user",
        "created_at": "2026-08-20T10:12:00Z",
        "origin": null,
        "hop": null,
        "source": null,
        "projection": []
    })
    .to_string()
}

#[tokio::test]
async fn relays_to_holder() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    let peer = mock_peer(StatusCode::OK, &answer()).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    // The key lies in the peer's range, so the peer answers.
    let version = Some(Ulid::from_bytes([43; 16]));
    let relayed = origin(
        &node,
        &node.owner,
        HeaderMap::new(),
        BUCKET,
        "z.md",
        version,
    )
    .await
    .unwrap();
    assert_eq!(relayed.created_by, "peer-user");
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("/api/v1/data/buckets/routed/objects/origin?key=z.md"));
    assert!(paths[0].contains("version_id="));

    // A relayed call is never relayed again: this node holds no such key.
    let mut hop = HeaderMap::new();
    hop.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    let local = origin(&node, &node.owner, hop, BUCKET, "z.md", None).await;
    assert!(matches!(local, Err(ServerError::NotFound)));
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unanswered_holder_fails() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let failed = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "z.md", None).await;
    let Err(ServerError::ServiceUnavailableReason(reason)) = failed else {
        panic!("a range no holder answers fails the read");
    };
    assert!(reason.contains("no holder"));
    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    let refused = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "z.md", None).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
}

#[test]
fn view_keeps_fields() {
    let global = GlobalId::Version {
        realm_id: realm(),
        version_id: Ulid::from_bytes([44; 16]),
    };
    let digest = SourceDigest {
        digest: vec![0xab, 0xcd],
        scope: DigestScope::Composite,
        origin: DigestOrigin::VerifiedRead,
    };
    let full = SourceDigest {
        scope: DigestScope::FullObject,
        origin: DigestOrigin::Manifest,
        ..digest.clone()
    };
    let listing = SourceDigest {
        origin: DigestOrigin::ListingEtag,
        ..full.clone()
    };
    let view = OriginView {
        version_id: Ulid::from_bytes([45; 16]),
        global: Some(global.clone()),
        created_by: UserId::local(Ulid::from_bytes([46; 16]), realm()),
        created_at: SystemTime::UNIX_EPOCH,
        origin: Some(VersionOrigin::Copy {
            from: OriginVersion::Global(global.clone()),
        }),
        hop: Some(SyncHop {
            relationship_id: Ulid::from_bytes([47; 16]),
            from: OriginVersion::Global(global.clone()),
        }),
        source: Some(aruna_operations::s3::object::origin::SourceView {
            url: Some("https://s3.example.org/a".to_string()),
            path: "a".to_string(),
            etag: Some("etag".to_string()),
            digests: vec![
                (ChecksumAlgorithm::Md5, digest),
                (ChecksumAlgorithm::Sha256, full),
                (ChecksumAlgorithm::Sha1, listing),
            ],
        }),
        projection: vec![
            OriginProjection::BasedOn {
                id: global.to_url(),
            },
            OriginProjection::CreatedBy {
                run_id: Ulid::from_bytes([48; 16]),
            },
        ],
    };
    let response = OriginResponse::from(view);
    assert_eq!(response.global_id, Some(global.to_url()));
    assert!(matches!(
        response.origin,
        Some(OriginBody::Copy { ref from }) if *from == global.to_url()
    ));
    assert_eq!(response.hop.unwrap().from, global.to_url());
    let source = response.source.unwrap();
    let kinds: Vec<_> = source
        .digests
        .iter()
        .map(|digest| {
            (
                digest.scope.as_str(),
                digest.origin.as_str(),
                digest.digest.as_str(),
            )
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("composite", "verified_read", "abcd"),
            ("full_object", "manifest", "abcd"),
            ("full_object", "listing_etag", "abcd"),
        ]
    );
    assert_eq!(response.projection.len(), 2);
    assert!(matches!(
        response.projection[1],
        ProjectionBody::CreatedBy { ref run_id } if *run_id == Ulid::from_bytes([48; 16]).to_string()
    ));
}

#[test]
fn origin_kinds_named() {
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("laptop".to_string()),
    };
    let kinds = [
        upload,
        VersionOrigin::Source {
            database: Some(Ulid::from_bytes([49; 16])),
        },
        VersionOrigin::Storage {
            backend_id: Ulid::from_bytes([50; 16]),
            key: "share/a.txt".to_string(),
        },
        VersionOrigin::Job {
            run_id: Ulid::from_bytes([51; 16]),
        },
    ];
    let view = |origin| OriginView {
        version_id: Ulid::nil(),
        global: None,
        created_by: UserId::nil(realm()),
        created_at: SystemTime::UNIX_EPOCH,
        origin: Some(origin),
        hop: None,
        source: None,
        projection: Vec::new(),
    };
    let bodies: Vec<_> = kinds
        .into_iter()
        .map(|origin| OriginResponse::from(view(origin)).origin.unwrap())
        .collect();
    assert!(matches!(
        &bodies[0],
        OriginBody::Upload { device: Some(device), .. } if device == "laptop"
    ));
    assert!(matches!(
        &bodies[1],
        OriginBody::Source {
            database_entry: Some(_)
        }
    ));
    assert!(matches!(
        &bodies[2],
        OriginBody::Storage { key, .. } if key == "share/a.txt"
    ));
    assert!(matches!(&bodies[3], OriginBody::Job { .. }));
}

async fn seed_sources(node: &Node) {
    let relationship = SyncRelationship {
        id: Ulid::from_bytes([52; 16]),
        source: ArunaArn::s3_bucket(node.state.get_realm_id(), node_id(PEER), "upstream").unwrap(),
        target: ArunaArn::s3_bucket(
            node.state.get_realm_id(),
            node_id(crate::tests::distributed::LOCAL),
            BUCKET,
        )
        .unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: false,
        created_by: node.owner.user_id,
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };
    let key = sync_relationship_key(BUCKET, relationship.id);
    write(
        &node.state,
        RELATIONSHIP_IN_KEYSPACE,
        key,
        relationship.to_bytes().unwrap(),
    )
    .await;
    let import = BackendImport {
        import_id: Ulid::from_bytes([53; 16]),
        group_id: node.group_id,
        backend_id: Ulid::from_bytes([54; 16]),
        backend_prefix: "share/".to_string(),
        bucket: BUCKET.to_string(),
        key_prefix: "in/".to_string(),
        schedule: None,
        created_by: node.owner.user_id,
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: String::new(),
    };
    let key = import.key().to_vec();
    write(
        &node.state,
        BACKEND_IMPORT_KEYSPACE,
        key,
        import.to_bytes().unwrap(),
    )
    .await;
}

async fn sources(
    node: &Node,
    auth: &AuthContext,
    bucket: &str,
) -> ServerResult<FileSourcesResponse> {
    get_sources(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Path(bucket.to_string()),
    )
    .await
    .map(|Json(response)| response)
}

#[tokio::test]
async fn lists_incoming_paths() {
    let node = node_state().await;
    let empty = sources(&node, &node.owner, BUCKET).await.unwrap();
    assert!(empty.sources.is_empty());
    assert!(empty.complete);

    seed_sources(&node).await;
    let listed = sources(&node, &node.owner, BUCKET).await.unwrap();
    assert!(listed.complete);
    assert_eq!(listed.sources.len(), 2);
    assert!(listed.sources.iter().any(|source| matches!(
        source,
        FileSourceBody::Sync { mode, source, .. } if mode == "continuous" && source.ends_with("upstream")
    )));
    assert!(listed.sources.iter().any(|source| matches!(
        source,
        FileSourceBody::Storage { backend_prefix, key_prefix, .. }
            if backend_prefix == "share/" && key_prefix == "in/"
    )));
    assert!(matches!(
        sources(&node, &node.stranger, BUCKET).await,
        Err(ServerError::Forbidden)
    ));
    assert!(matches!(
        sources(&node, &node.owner, "absent").await,
        Err(ServerError::NotFound)
    ));
}

// A relayed origin read names the same key and version on the holder.
#[test]
fn relay_path_kept() {
    let params = OriginParams {
        key: "raw/a b.txt".to_string(),
        version_id: Some(Ulid::from_bytes([1; 16])),
    };
    let path = origin_path("lab", &params);
    assert_eq!(
        path,
        format!(
            "/data/buckets/lab/objects/origin?key=raw%2Fa+b.txt&version_id={}",
            Ulid::from_bytes([1; 16])
        )
    );
}
