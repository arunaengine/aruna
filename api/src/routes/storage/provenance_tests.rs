//! Tests the provenance export: the streamed crate, hidden and silent hops, and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::Read as _;

use aruna_core::NodeId;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use ulid::Ulid;

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::deletion::bucket_info;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, LOCAL, Node, PEER, node_id, node_state, publish_api,
};

async fn exported(node: &Node, auth: &AuthContext, version: &str) -> serde_json::Value {
    let params = ExportParams {
        version: Some(version.to_string()),
        dataset: None,
    };
    let response = export_provenance(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        Query(params),
    )
    .await
    .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let mut root = String::new();
    archive
        .by_name("ro-crate-metadata.json")
        .unwrap()
        .read_to_string(&mut root)
        .unwrap();
    serde_json::from_str(&root).unwrap()
}

fn arn(node: &Node, node_id: NodeId, key: &str, version: Ulid) -> String {
    VersionedObjectArn::new(node.state.get_realm_id(), node_id, BUCKET, key, version)
        .unwrap()
        .to_w3id()
}

fn entity<'a>(root: &'a serde_json::Value, id: &str) -> Option<&'a serde_json::Value> {
    let graph = root["@graph"].as_array().unwrap();
    graph.iter().find(|node| node["@id"] == id)
}

#[tokio::test]
async fn exports_local_version() {
    let node = node_state().await;
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let input = WriteObjectInput {
        bucket: BUCKET.to_string(),
        key: "notes/a.md".to_string(),
        text: "x".to_string(),
        content_type: None,
    };
    let stored = store_text(&node.state, &node.owner, &info, input)
        .await
        .unwrap();
    let version = Ulid::from_string(&stored.version_id).unwrap();
    let start = arn(&node, node_id(LOCAL), "notes/a.md", version);

    let root = exported(&node, &node.owner, &start).await;
    let file = entity(&root, &start).expect("the version is in the crate");
    assert_eq!(file["name"], "a.md");
    assert!(
        file["identifier"][0]
            .as_str()
            .unwrap()
            .starts_with("https://w3id.org/aruna/data/")
    );
    assert_eq!(
        entity(&root, "./").unwrap()["creativeWorkStatus"],
        "Complete"
    );

    // A caller without READ gets the crate, but not the file's place.
    let hidden = exported(&node, &node.stranger, &start).await;
    let label = entity(&hidden, "#hidden-1").expect("the hop is named by a label");
    assert_eq!(label["name"], "Not visible to you");
    assert!(entity(&hidden, &start).is_none());
}

#[tokio::test]
async fn silent_node_incomplete() {
    let node = node_state().await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let start = arn(&node, node_id(PEER), "b.md", Ulid::from_bytes([3; 16]));
    let root = exported(&node, &node.owner, &start).await;
    let status = entity(&root, "./").unwrap()["creativeWorkStatus"]
        .as_str()
        .unwrap();
    assert!(status.starts_with("Incomplete"));
    assert!(entity(&root, &start).is_some());

    let neither = export_provenance(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Extension(None),
        Query(ExportParams {
            version: None,
            dataset: None,
        }),
    )
    .await;
    assert!(matches!(neither, Err(ServerError::BadRequestReason(_))));
}

#[tokio::test]
async fn disconnect_stops_reads() {
    let node = node_state().await;
    let caller = Caller {
        state: node.state.clone(),
        auth: node.owner.clone(),
        bearer: None,
        deadline: Instant::now() + EXPORT_DEADLINE,
    };
    let gone = CancellationToken::new();
    gone.cancel();
    let read = bounded(&caller, &gone, std::future::pending::<()>()).await;
    assert!(read.is_none());
    let past = Caller {
        deadline: Instant::now(),
        ..caller
    };
    let late = bounded(
        &past,
        &CancellationToken::new(),
        std::future::pending::<()>(),
    )
    .await;
    assert!(late.is_none());
}
