//! Tests that per-object reads on a member without the key's range go to a holder of it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::object_key::ObjectKey;
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::placement::{ObjectPlacementQuery, get_object_placement};
use crate::routes::storage::abe::{VersionQuery, envelope};
use crate::routes::storage::blobs::{BlobLocationsQuery, blob_locations};
use crate::tests::distributed::{
    BUCKET, MockPeer, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use tower::ServiceExt;

/// A key above `m`, whose range the peer coordinates.
const PEER_KEY: &str = "zebra";
const VERSION: &str = "01K2ZK4Q0X3D5M6P7R8S9T0V3B";

/// A node holding the lower shard of [`BUCKET`], with a peer answering `body`, and the set.
async fn sharded(body: &str) -> (Node, MockPeer, Ulid) {
    let node = node_state().await;
    let set_id = distribute(&node.state, node.group_id, BUCKET)
        .await
        .set_id();
    let peer = mock_peer(StatusCode::OK, body).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    (node, peer, set_id)
}

fn bearer() -> Extension<Option<ValidatedBearer>> {
    Extension(Some(ValidatedBearer::new_for_test("caller-token")))
}

fn asked(peer: &MockPeer) -> Vec<String> {
    peer.paths.lock().unwrap().clone()
}

#[tokio::test]
async fn placement_asks_holder() {
    let answer = serde_json::json!({
        "bucket": BUCKET, "key": PEER_KEY, "version_id": VERSION, "generation": 7, "policies": []
    });
    let (node, peer, set_id) = sharded(&answer.to_string()).await;
    let read = |key: &str, headers: HeaderMap| {
        get_object_placement(
            State(node.state.clone()),
            Extension(Some(node.owner.clone())),
            bearer(),
            headers,
            Path(BUCKET.to_string()),
            Query(ObjectPlacementQuery {
                key: key.to_string(),
            }),
        )
    };
    let Json(view) = read(PEER_KEY, HeaderMap::new()).await.unwrap();
    assert_eq!((view.version_id.as_str(), view.generation), (VERSION, 7));
    let path = format!("/api/v1/data/buckets/{BUCKET}/placement/objects?key={PEER_KEY}");
    assert_eq!(asked(&peer), [path]);
    // Its own range and a relayed read are answered here.
    let own = read("apple", HeaderMap::new()).await;
    assert!(matches!(own, Err(ServerError::NotFound)));
    let mut relayed = HeaderMap::new();
    relayed.insert(RELAY_HOP_HEADER, node_id(PEER).to_string().parse().unwrap());
    relayed.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
    let here = read(PEER_KEY, relayed).await;
    assert!(matches!(here, Err(ServerError::NotFound)));
    assert_eq!(asked(&peer).len(), 1);
}

#[tokio::test]
async fn blob_reads_ask_holder() {
    let answer = serde_json::json!({
        "bucket": BUCKET, "key": PEER_KEY, "version_id": VERSION, "copies": [],
        "complete": true, "limits": [],
        "context": {}, "parameters": {}, "envelope": {}
    });
    let (node, peer, _) = sharded(&answer.to_string()).await;
    let Json(locations) = blob_locations(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        bearer(),
        HeaderMap::new(),
        Query(BlobLocationsQuery {
            bucket: BUCKET.to_string(),
            path: PEER_KEY.to_string(),
            version_id: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(locations.version_id, VERSION);
    let Json(view) = envelope(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        bearer(),
        HeaderMap::new(),
        Query(VersionQuery {
            bucket: BUCKET.to_string(),
            key: PEER_KEY.to_string(),
            version_id: VERSION.to_string(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(view.version_id, VERSION);
    // The content stream comes back unchanged.
    let (router, _) = crate::routes::storage::abe::router().split_for_parts();
    let router = router
        .with_state(node.state.clone())
        .layer(Extension(Some(node.owner.clone())))
        .layer(bearer())
        .layer(Extension(ObjectKey(Ok(None))));
    let query = format!("bucket={BUCKET}&key={PEER_KEY}&version_id={VERSION}");
    let request = axum::http::Request::get(format!("/data/blobs/content?{query}"))
        .header(header::RANGE, "bytes=0-1")
        .body(Body::empty())
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body, answer.to_string().as_bytes());
    assert_eq!(
        asked(&peer),
        [
            format!("/api/v1/data/blobs/locations?bucket={BUCKET}&path={PEER_KEY}"),
            format!("/api/v1/data/blobs/envelope?{query}"),
            format!("/api/v1/data/blobs/content?{query}"),
        ]
    );
}
