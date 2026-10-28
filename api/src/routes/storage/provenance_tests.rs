//! Tests the provenance export: the streamed crate, hidden and silent hops, and refusals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::Read as _;
use std::sync::atomic::Ordering::SeqCst;

use aruna_core::NodeId;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use ulid::Ulid;

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::deletion::bucket_info;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, LOCAL, Node, PEER, node_id, node_state, publish_api,
};

/// A token check that names `auth` for the first `hops` checks, then reports it revoked.
fn recheck(auth: &AuthContext, hops: usize) -> Recheck {
    let (auth, left) = (
        auth.clone(),
        Arc::new(std::sync::atomic::AtomicUsize::new(hops)),
    );
    Arc::new(move |_, _| {
        let open = left
            .fetch_update(SeqCst, SeqCst, |left| left.checked_sub(1))
            .is_ok();
        let auth = auth.clone();
        Box::pin(async move { open.then_some(auth) })
    })
}

fn caller(node: &Node, recheck: Recheck) -> Caller {
    Caller {
        state: node.state.clone(),
        bearer: ValidatedBearer::new_for_test("caller-token"),
        recheck,
        deadline: Instant::now() + EXPORT_DEADLINE,
    }
}

async fn exported_with(node: &Node, recheck: Recheck, version: &str) -> serde_json::Value {
    exported_by(caller(node, recheck), version).await
}

async fn exported_by(caller: Caller, version: &str) -> serde_json::Value {
    let start = Start::Version(version.to_string());
    let response = start_export(caller, start).unwrap();
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

async fn exported(node: &Node, auth: &AuthContext, version: &str) -> serde_json::Value {
    exported_with(node, recheck(auth, usize::MAX), version).await
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

    let export = |bearer: Option<ValidatedBearer>| {
        export_provenance(
            State(node.state.clone()),
            Extension(Some(node.owner.clone())),
            Extension(bearer),
            Query(ExportParams {
                version: None,
                dataset: None,
            }),
        )
    };
    let neither = export(Some(ValidatedBearer::new_for_test("caller-token"))).await;
    assert!(matches!(neither, Err(ServerError::BadRequestReason(_))));
    // Each hop checks the token again, so an export without one is refused.
    assert!(matches!(export(None).await, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn revoked_token_stops() {
    // A token revoked during the walk stops it before the next hop; nothing more is read.
    let node = node_state().await;
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let input = WriteObjectInput {
        bucket: BUCKET.to_string(),
        key: "a.md".to_string(),
        text: "x".to_string(),
        content_type: None,
    };
    let stored = store_text(&node.state, &node.owner, &info, input)
        .await
        .unwrap();
    let version = Ulid::from_string(&stored.version_id).unwrap();
    let start = arn(&node, node_id(LOCAL), "a.md", version);
    let root = exported_with(&node, recheck(&node.owner, 0), &start).await;
    let status = entity(&root, "./").unwrap()["creativeWorkStatus"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(status, format!("Incomplete: {REVOKED}"));
    assert!(entity(&root, &start).is_none());
}

#[test]
fn deadline_follows_token() {
    let now = Instant::now();
    assert_eq!(
        export_deadline(now, 1_000, 1_060),
        now + Duration::from_secs(60)
    );
    assert_eq!(export_deadline(now, 1_000, u64::MAX), now + EXPORT_DEADLINE);
    assert_eq!(export_deadline(now, 2_000, 1_000), now);
}

#[tokio::test]
async fn disconnect_stops_reads() {
    let node = node_state().await;
    let caller = caller(&node, recheck(&node.owner, usize::MAX));
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

#[tokio::test]
async fn silent_crate_incomplete() {
    // A run whose crate does not answer, or answers too late, leaves the export incomplete.
    let hop = Hop {
        target: Target::Run("r1".to_string()),
        from: "file".to_string(),
        property: "result",
        digest: None,
    };
    let status = |read: Option<Read>| {
        let mut built = ProvenanceCrate::new("file", "2026-10-28T00:00:00Z").unwrap();
        let (_, _, go_on) = record_run(&mut built, &hop, read);
        let mut out = Vec::new();
        built.write_root(&mut out).unwrap();
        let root: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let graph = root["@graph"].as_array().unwrap().clone();
        let status =
            graph.iter().find(|node| node["@id"] == "./").unwrap()["creativeWorkStatus"].clone();
        (status.as_str().unwrap().to_string(), go_on)
    };
    assert_eq!(status(Some(Read::Hidden)), ("Complete".to_string(), true));
    let (silent, go_on) = status(Some(Read::Silent));
    assert!(silent.starts_with("Incomplete") && go_on);
    let (late, go_on) = status(None);
    assert!(late.starts_with("Incomplete") && !go_on);
    assert!(matches!(
        classify(Err(ServerError::ServiceUnavailableReason(
            "busy".to_string()
        ))),
        Read::Silent
    ));
    assert!(matches!(classify(Err(ServerError::NotFound)), Read::Hidden));
}

#[tokio::test]
async fn stalled_write_ends() {
    // A client that stops reading cannot hold the export past its deadline.
    let node = node_state().await;
    let caller = Caller {
        deadline: Instant::now() + std::time::Duration::from_millis(50),
        ..caller(&node, recheck(&node.owner, usize::MAX))
    };
    let (writer, _reader) = tokio::io::duplex(16);
    let mut zip = ZipFileWriter::with_tokio(writer);
    let file = CrateFile {
        path: "big.json".to_string(),
        bytes: vec![b'x'; 64 * 1024],
    };
    assert!(!write(&caller, &CancellationToken::new(), &mut zip, file).await);
}

#[tokio::test]
async fn crate_read_rechecks() {
    // A token revoked after the run read is checked again before its crate is read.
    let node = node_state().await;
    let gone = CancellationToken::new();
    let revoked = caller(&node, recheck(&node.owner, 0));
    assert!(
        crate_read(&revoked, &gone, "01K8W1C0000000000000000000")
            .await
            .is_none()
    );
    let valid = caller(&node, recheck(&node.owner, 1));
    let read = crate_read(&valid, &gone, "01K8W1C0000000000000000000").await;
    assert!(matches!(read, Some(Some(Read::Hidden | Read::Silent))));
}
