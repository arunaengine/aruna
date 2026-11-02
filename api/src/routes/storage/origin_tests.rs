//! Tests the origin and sources routes: the answer shape, permissions, relayed reads to a
//! holder and the incoming paths of a bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::relay::RELAY_SET_HEADER;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
    replicate, write,
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
        node_id: None,
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        text: "x".to_string(),
        content_type: None,
    };
    store_text(&node.state, &node.owner, &info, input, Default::default())
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
    let set_id = distribute(&node.state, node.group_id, BUCKET)
        .await
        .set_id();
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

    // A relayed call is never relayed again: a current read outside this node's ranges fails.
    let mut hop = HeaderMap::new();
    hop.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    // Without the set it meant, the bucket of this name may be another one.
    let unnamed = origin(&node, &node.owner, hop.clone(), BUCKET, "z.md", None).await;
    assert!(matches!(unnamed, Err(ServerError::Conflict(_))));
    let mut other = hop.clone();
    other.insert(RELAY_SET_HEADER, Ulid::nil().to_string().parse().unwrap());
    let stranger = origin(&node, &node.owner, other, BUCKET, "z.md", None).await;
    assert!(matches!(stranger, Err(ServerError::Conflict(_))));
    hop.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
    let local = origin(&node, &node.owner, hop, BUCKET, "z.md", None).await;
    assert!(matches!(
        local,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

// A copy that has not received the version yet asks the range's coordinator.
#[tokio::test]
async fn copy_miss_relays() {
    let node = node_state().await;
    replicate(&node.state, node.group_id, BUCKET).await;
    let peer = mock_peer(StatusCode::OK, &answer()).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let relayed = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    assert_eq!(relayed.created_by, "peer-user");
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

// A copy may hold an older version, so a read of the current one goes to the coordinator.
#[tokio::test]
async fn stale_copy_relays() {
    let node = node_state().await;
    store(&node, "a.md").await;
    replicate(&node.state, node.group_id, BUCKET).await;
    let peer = mock_peer(StatusCode::OK, &answer()).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let relayed = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    assert_eq!(relayed.created_by, "peer-user");
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

// While the coordinator cannot answer, this node's copy answers, marked stale, and its miss
// says the version may exist.
#[tokio::test]
async fn down_coordinator_copy() {
    let node = node_state().await;
    store(&node, "a.md").await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let range_id = snapshot.config.ranges[0].range_id.to_string();
    let answer = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    assert_eq!(answer.stale_ranges, std::slice::from_ref(&range_id));
    let missing = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "b.md", None).await;
    let Err(ServerError::Refused(status, code, message)) = missing else {
        panic!("a copy's miss is marked");
    };
    assert_eq!((status, code), (StatusCode::NOT_FOUND, "stale_not_found"));
    assert!(message.contains(&range_id));
}

// A copy refuses a relayed read of the current version unless it was asked as a copy.
#[tokio::test]
async fn relayed_copy_read() {
    let node = node_state().await;
    store(&node, "a.md").await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    let mut hop = HeaderMap::new();
    hop.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    let set_id = snapshot.set_id().to_string();
    hop.insert(RELAY_SET_HEADER, set_id.parse().unwrap());
    let refused = origin(&node, &node.owner, hop.clone(), BUCKET, "a.md", None).await;
    assert!(matches!(
        refused,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    hop.insert(COPY_READ_HEADER, "1".parse().unwrap());
    let answer = origin(&node, &node.owner, hop, BUCKET, "a.md", None).await;
    assert!(answer.is_ok());
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
            job_id: None,
        }),
        hop: Some(SyncHop {
            relationship_id: Ulid::from_bytes([47; 16]),
            from: OriginVersion::Global(global.clone()),
        }),
        source: Some(aruna_operations::s3::object::origin::SourceView {
            url: Some("https://s3.example.org/a".to_string()),
            path: "a".to_string(),
            version: None,
            etag: Some("etag".to_string()),
            digests: vec![
                (ChecksumAlgorithm::Md5, digest),
                (ChecksumAlgorithm::Sha256, full),
                (ChecksumAlgorithm::Sha1, listing),
            ],
            sources: vec![
                aruna_operations::s3::object::origin::SourcePlace {
                    url: Some("https://s3.example.org/a".to_string()),
                    path: "a".to_string(),
                    connector_id: None,
                    connector_name: None,
                },
                aruna_operations::s3::object::origin::SourcePlace {
                    url: Some("https://mirror.example.org/a".to_string()),
                    path: "a".to_string(),
                    connector_id: Some(Ulid::from_bytes([48; 16])),
                    connector_name: Some("Mirror".to_string()),
                },
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
        bucket_metadata: None,
        identifiers: Vec::new(),
        content_hash: None,
        size: None,
    };
    let response = OriginResponse::from(view);
    assert_eq!(response.global_id, Some(global.to_url()));
    assert!(matches!(
        response.origin,
        Some(OriginBody::Copy { ref from, .. }) if *from == global.to_url()
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
    // Every source is listed, the mirror with the connector it reads through.
    let mirror = &source.sources[1];
    assert_eq!(source.sources.len(), 2);
    assert_eq!(mirror.url.as_deref(), Some("https://mirror.example.org/a"));
    let connector = Ulid::from_bytes([48; 16]).to_string();
    assert_eq!(mirror.connector_id.as_deref(), Some(connector.as_str()));
    assert_eq!(mirror.connector_name.as_deref(), Some("Mirror"));
    assert_eq!(response.projection.len(), 2);
    assert!(matches!(
        response.projection[1],
        ProjectionBody::CreatedBy { ref run_id } if *run_id == Ulid::from_bytes([48; 16]).to_string()
    ));
}

// Each kind answers with its client, job or starter under the names the portal reads.
#[test]
fn origin_kinds_named() {
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("laptop".to_string()),
        client: Some(UploadClient::Device),
    };
    let job = Ulid::from_bytes([53; 16]);
    let user = UserId::local(Ulid::from_bytes([54; 16]), realm());
    let from = OriginVersion::Global(GlobalId::Version {
        realm_id: realm(),
        version_id: Ulid::from_bytes([55; 16]),
    });
    let kinds = [
        upload,
        VersionOrigin::Source { job_id: Some(job) },
        VersionOrigin::Storage {
            backend_id: Ulid::from_bytes([50; 16]),
            key: "share/a.txt".to_string(),
            job_id: Some(job),
        },
        VersionOrigin::Job {
            run_id: Ulid::from_bytes([51; 16]),
            started_by: Some(user),
        },
        VersionOrigin::Copy {
            from: from.clone(),
            job_id: None,
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
        bucket_metadata: None,
        identifiers: Vec::new(),
        content_hash: None,
        size: None,
    };
    let bodies: Vec<_> = kinds
        .into_iter()
        .map(|origin| OriginResponse::from(view(origin)).origin.unwrap())
        .collect();
    let json: Vec<_> = (bodies.iter())
        .map(|body| serde_json::to_value(body).unwrap())
        .collect();
    let job = job.to_string();
    assert_eq!(
        json,
        vec![
            serde_json::json!({
                "kind": "upload", "node_id": node().to_string(), "device": "laptop",
                "client": "device",
            }),
            serde_json::json!({"kind": "source", "job_id": job}),
            serde_json::json!({
                "kind": "storage", "backend_id": Ulid::from_bytes([50; 16]).to_string(),
                "key": "share/a.txt", "job_id": job,
            }),
            serde_json::json!({
                "kind": "job", "run_id": Ulid::from_bytes([51; 16]).to_string(),
                "started_by": user.to_string(),
            }),
            serde_json::json!({"kind": "copy", "from": from.to_id(), "job_id": null}),
        ]
    );
    let portal = serde_json::to_value(UploadClientBody::from(UploadClient::Portal)).unwrap();
    assert_eq!(portal, "portal");
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

async fn by_version(
    node: &Node,
    named: Option<NodeId>,
    version: &str,
) -> ServerResult<OriginResponse> {
    let mut headers = HeaderMap::new();
    if let Some(named) = named {
        let value = named.to_string().parse().unwrap();
        headers.insert(crate::routes::storage::relay::RELAY_NODE_HEADER, value);
    }
    get_version_origin(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Query(VersionOriginParams {
            version: version.to_string(),
        }),
    )
    .await
    .map(|Json(response)| response)
}

fn arn(node: NodeId, realm_id: RealmId, key: &str, version: Ulid) -> String {
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    VersionedObjectArn::new(realm_id, node, BUCKET, key, version)
        .unwrap()
        .to_w3id()
}

#[tokio::test]
async fn reads_by_version() {
    use crate::tests::distributed::LOCAL;
    let node = node_state().await;
    store(&node, "a.md").await;
    let current = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    let version = Ulid::from_string(&current.version_id).unwrap();
    let local = arn(node_id(LOCAL), node.state.get_realm_id(), "a.md", version);
    let read = by_version(&node, None, &local).await.unwrap();
    assert_eq!(read.version_id, current.version_id);
    // Stored bytes carry their content id.
    assert!(
        read.content_id
            .unwrap()
            .starts_with("https://w3id.org/aruna/data/")
    );
    let directed = by_version(&node, Some(node_id(LOCAL)), &local)
        .await
        .unwrap();
    assert_eq!(directed.version_id, current.version_id);

    let foreign = arn(
        node_id(LOCAL),
        RealmId::from_bytes([4; 32]),
        "a.md",
        version,
    );
    assert!(matches!(
        by_version(&node, None, &foreign).await,
        Err(ServerError::NotFound)
    ));
    let malformed = by_version(&node, None, "s3://routed/a.md").await;
    assert!(matches!(malformed, Err(ServerError::BadRequestReason(_))));
    let elsewhere = by_version(&node, Some(node_id(PEER)), &local).await;
    assert!(matches!(elsewhere, Err(ServerError::Conflict(_))));

    // Another node's version is read on that node, never directed on from there.
    let peer = mock_peer(StatusCode::OK, &answer()).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let remote = arn(node_id(PEER), node.state.get_realm_id(), "b.md", version);
    let relayed = by_version(&node, None, &remote).await.unwrap();
    assert_eq!(relayed.created_by, "peer-user");
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("/api/v1/data/origin?version="));
    let again = by_version(&node, Some(node_id(LOCAL)), &remote).await;
    assert!(matches!(again, Err(ServerError::Conflict(_))));
    assert_eq!(peer.paths.lock().unwrap().len(), 1);

    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    let refused = by_version(&node, None, &remote).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn directed_relays_once() {
    // A version of a distributed bucket whose range the target keeps no records of is
    // answered through one set-directed relay to a holder.
    use crate::tests::distributed::LOCAL;
    let node = node_state().await;
    let set_id = distribute(&node.state, node.group_id, BUCKET)
        .await
        .set_id();
    let peer = mock_peer(StatusCode::OK, &answer()).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let version = Ulid::from_bytes([43; 16]);
    let local = arn(node_id(LOCAL), node.state.get_realm_id(), "z.md", version);
    let relayed = by_version(&node, Some(node_id(LOCAL)), &local)
        .await
        .unwrap();
    assert_eq!(relayed.created_by, "peer-user");
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("/api/v1/data/buckets/routed/objects/origin?key=z.md"));
    // The holder sees a set-directed relay, which it never relays again.
    let mut hop = HeaderMap::new();
    hop.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    hop.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
    let held = origin(&node, &node.owner, hop, BUCKET, "z.md", Some(version)).await;
    assert!(matches!(held, Err(ServerError::NotFound)));
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn reads_global_version() {
    use crate::tests::distributed::mirror;
    let node = node_state().await;
    mirror(&node.state, node.group_id, BUCKET).await;
    store(&node, "a.md").await;
    let current = origin(&node, &node.owner, HeaderMap::new(), BUCKET, "a.md", None)
        .await
        .unwrap();
    let global = current.global_id.clone().unwrap();
    let read = by_version(&node, None, &global).await.unwrap();
    assert_eq!(read.version_id, current.version_id);
    let unknown = GlobalId::Version {
        realm_id: node.state.get_realm_id(),
        version_id: Ulid::from_bytes([44; 16]),
    };
    // The peer member cannot be asked here, so an unknown version is not proven absent.
    let missing = by_version(&node, None, &unknown.to_url()).await;
    assert!(matches!(
        missing,
        Err(ServerError::NotFound | ServerError::ServiceUnavailableReason(_))
    ));
}

/// Marks the current version of `key` as a result of `run`.
async fn written_by(node: &Node, key: &str, run: u8) {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    let context = node.state.get_ctx();
    let read = |key_space: &str, key: Vec<u8>| {
        let read = StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        };
        let storage = context.storage_handle.clone();
        async move {
            match storage.send_storage_effect(read).await {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => value.unwrap().to_vec(),
                other => panic!("{other:?}"),
            }
        }
    };
    let head = BlobHeadKey::new(BUCKET, key).to_bytes().unwrap();
    let pointer = CurrentVersionPointer::from_bytes(&read(BLOB_HEAD_KEYSPACE, head).await);
    let row = VersionKey::new(BUCKET, key, pointer.unwrap().version_id).to_bytes();
    let row = row.unwrap();
    let mut version = BlobVersion::from_bytes(&read(BLOB_VERSIONS_KEYSPACE, row.clone()).await);
    let version = version.as_mut().unwrap();
    version.origin = Some(VersionOrigin::Job {
        run_id: Ulid::from_bytes([run; 16]),
        started_by: None,
    });
    let bytes = version.to_bytes().unwrap();
    crate::tests::distributed::write(&node.state, BLOB_VERSIONS_KEYSPACE, row, bytes).await;
}

#[tokio::test]
async fn lists_writing_runs() {
    // The runs that wrote current files under a prefix are named once each, newest first.
    let node = node_state().await;
    for key in ["out/a/1.txt", "out/a/2.txt", "out/b/1.txt", "in/x.txt"] {
        store(&node, key).await;
    }
    written_by(&node, "out/a/1.txt", 1).await;
    written_by(&node, "out/a/2.txt", 1).await;
    written_by(&node, "out/b/1.txt", 2).await;
    let runs = |auth: Option<AuthContext>, prefix: &str| {
        get_bucket_runs(
            State(node.state.clone()),
            Extension(auth),
            Path(BUCKET.to_string()),
            Query(BucketRunsParams {
                prefix: prefix.to_string(),
            }),
        )
    };
    let named = |response: BucketRunsResponse| {
        let runs = response.runs.into_iter().map(|run| run.run_id);
        (runs.collect::<Vec<_>>(), response.complete)
    };
    let id = |run: u8| Ulid::from_bytes([run; 16]).to_string();
    let Json(all) = runs(Some(node.owner.clone()), "").await.unwrap();
    assert_eq!(named(all), (vec![id(2), id(1)], true));
    let Json(one) = runs(Some(node.owner.clone()), "out/a/").await.unwrap();
    assert_eq!(named(one), (vec![id(1)], true));
    let Json(none) = runs(Some(node.owner.clone()), "in/").await.unwrap();
    assert_eq!(named(none), (Vec::new(), true));
    let stranger = runs(Some(node.stranger.clone()), "").await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));
    assert!(matches!(
        runs(None, "").await,
        Err(ServerError::Unauthorized)
    ));

    // A file a deny hides from the caller names no run.
    let (realm_id, local) = (node.state.get_realm_id(), node.state.get_node_id());
    let root = bucket_permission_path(realm_id, node.group_id, local, BUCKET);
    let rule = |pattern: String, permission| aruna_core::structs::identity::auth::PathRestriction {
        pattern,
        permission,
    };
    let narrowed = AuthContext {
        path_restrictions: Some(vec![
            rule(root.clone(), Permission::READ),
            rule(format!("{root}/**"), Permission::READ),
            rule(format!("{root}/out/b/**"), Permission::DENY),
        ]),
        ..node.owner.clone()
    };
    let Json(shown) = runs(Some(narrowed), "").await.unwrap();
    assert_eq!(named(shown), (vec![id(1)], true));
}

// Credentials in a connector endpoint never reach the origin answer or its projection.
#[tokio::test]
async fn hides_endpoint_credentials() {
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    use std::collections::HashMap;
    let node = node_state().await;
    let endpoint = "https://reader:hunter2@s3.example.org?token=t0ken&X-Amz-Signature=s1g";
    let descriptor = PortableSourceDescriptor {
        kind: SourceConnectorKind::S3,
        public_config: HashMap::from([
            ("endpoint".to_string(), endpoint.to_string()),
            ("bucket".to_string(), "lab".to_string()),
            ("skip_signature".to_string(), "true".to_string()),
        ]),
        source_path: "a.txt".to_string(),
        version_selector: None,
        capabilities: Vec::new(),
        origin_node_id: None,
        namespace: None,
        root: None,
    };
    let binding = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor,
        connector_id: None,
        mirrors: Vec::new(),
    };
    let metadata = SourceMetadata {
        content_length: 1,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let created = SystemTime::UNIX_EPOCH;
    let version = BlobVersion::reference(binding, metadata, created, node.owner.user_id, created)
        .with_origin(Some(VersionOrigin::Source { job_id: None }));
    let version_id = Ulid::generate();
    let rows = [
        (
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(BUCKET, "ref.txt").to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(BUCKET, "ref.txt", version_id)
                .to_bytes()
                .unwrap(),
            version.to_bytes().unwrap(),
        ),
    ];
    for (key_space, key, value) in rows {
        let effect = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        node.state
            .get_ctx()
            .storage_handle
            .send_storage_effect(effect)
            .await;
    }
    let answer = origin(
        &node,
        &node.owner,
        HeaderMap::new(),
        BUCKET,
        "ref.txt",
        None,
    )
    .await
    .unwrap();
    let bare = "https://s3.example.org/lab/a.txt";
    let source = answer.source.as_ref().unwrap();
    assert_eq!(source.url.as_deref(), Some(bare));
    assert_eq!(source.sources[0].url.as_deref(), Some(bare));
    let text = serde_json::to_string(&answer).unwrap();
    for secret in ["reader", "hunter2", "t0ken", "s1g", "token", "Signature"] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    let based_on =
        |item: &ProjectionBody| matches!(item, ProjectionBody::BasedOn { id } if id == bare);
    assert!(answer.projection.iter().any(based_on), "{text}");
}
