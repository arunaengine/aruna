//! Tests the bucket usage route: permissions, the held ranges a member answers to another one,
//! and the usage of a distributed bucket merged with the ranges a peer holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::storage::relay::RELAY_SET_HEADER;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
    replicate,
};
use axum::http::StatusCode;

async fn usage(
    node: &Node,
    auth: &AuthContext,
    headers: HeaderMap,
    held: bool,
) -> ServerResult<BucketUsageResponse> {
    let answer = get_bucket_usage(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(BUCKET.to_string()),
        Query(UsageQuery { held: Some(held) }),
    )
    .await?;
    Ok(answer.0)
}

/// A distributed bucket whose lower range this node holds.
async fn distributed(node: &Node) -> DistributionSnapshot {
    distribute(&node.state, node.group_id, BUCKET).await
}

/// The usage answer of the peer for the upper range, with three objects in one counter row.
fn peer_answer(snapshot: &DistributionSnapshot) -> String {
    let range_id = snapshot.config.ranges[1].range_id;
    serde_json::json!({
        "bucket": BUCKET,
        "objects": 0, "versions": 0, "delete_markers": 0, "open_multipart_uploads": 0,
        "logical_bytes": 0, "referenced_bytes": 0, "complete": false,
        "held_ranges": [{
            "range_id": range_id.to_string(),
            "complete": true,
            "rows": [{
                "row": "0a0b", "objects": 3, "current_bytes": 30, "versions": 3,
                "delete_markers": 0, "logical_bytes": 30, "referenced_bytes": 0,
                "open_uploads": 0
            }]
        }]
    })
    .to_string()
}

/// The coordinator's answer for the one range of [`replicate`], with `objects` counted.
fn coordinator_answer(snapshot: &DistributionSnapshot, objects: u64) -> String {
    let range_id = snapshot.config.ranges[0].range_id;
    serde_json::json!({
        "bucket": BUCKET,
        "objects": 0, "versions": 0, "delete_markers": 0, "open_multipart_uploads": 0,
        "logical_bytes": 0, "referenced_bytes": 0, "complete": false,
        "held_ranges": [{
            "range_id": range_id.to_string(),
            "complete": true,
            "coordinated": true,
            "rows": [{
                "row": "0c0d", "objects": objects, "current_bytes": 0, "versions": objects,
                "delete_markers": 0, "logical_bytes": 0, "referenced_bytes": 70,
                "open_uploads": 0
            }]
        }]
    })
    .to_string()
}

#[tokio::test]
async fn copy_asks_coordinator() {
    // A copy that lags counts its coordinator's rows, and says where each range was counted.
    let node = node_state().await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    let peer = mock_peer(StatusCode::OK, &coordinator_answer(&snapshot, 5)).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!((answer.objects, answer.referenced_bytes), (5, 70));
    assert_eq!(answer.current, Some(true));
    let sources = answer.range_sources.unwrap();
    assert_eq!(sources[0].source, Some(CounterSource::Coordinator));
    assert_eq!(sources[0].counted_by, Some(node_id(PEER).to_string()));
    assert_eq!(sources[0].coordinator, Some(node_id(PEER).to_string()));
    let bearer = ValidatedBearer::new_for_test("caller-token");
    let sizes = range_sizes(&node.state, Some(&bearer), snapshot.set_id()).await;
    let sizes = sizes.unwrap();
    assert_eq!(sizes.len(), 1);
    assert_eq!(
        sizes[0].objects, 5,
        "capacity sizes the coordinator's counters"
    );
}

#[tokio::test]
async fn silent_coordinator_unknown() {
    // Without its coordinator the local copy is counted, its freshness unknown, and capacity
    // does not size the range from it.
    let node = node_state().await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!(answer.ranges_counted, Some(1));
    assert_eq!(answer.current, None, "a copy's lag is unknown");
    let sources = answer.range_sources.unwrap();
    assert_eq!(sources[0].source, Some(CounterSource::Copy));
    assert_eq!(sources[0].current, None);
    assert_eq!(
        sources[0].counted_by,
        Some(node.state.get_node_id().to_string())
    );
    let bearer = ValidatedBearer::new_for_test("caller-token");
    let sizes = range_sizes(&node.state, Some(&bearer), snapshot.set_id()).await;
    assert!(sizes.unwrap().is_empty());
}

#[tokio::test]
async fn ordinary_usage_checked() {
    let node = node_state().await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!((answer.bucket.as_str(), answer.objects), (BUCKET, 0));
    assert!(answer.ranges.is_none());
    assert!(matches!(
        usage(&node, &node.stranger, HeaderMap::new(), false).await,
        Err(ServerError::Forbidden)
    ));
    // Held ranges are an answer to another member only.
    assert!(matches!(
        usage(&node, &node.owner, HeaderMap::new(), true).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn merges_peer_usage() {
    let node = node_state().await;
    let snapshot = distributed(&node).await;
    let peer = mock_peer(StatusCode::OK, &peer_answer(&snapshot)).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();

    assert_eq!((answer.ranges, answer.ranges_counted), (Some(2), Some(2)));
    assert_eq!((answer.objects, answer.logical_bytes), (3, 30));
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(
        paths,
        vec!["/api/v1/data/buckets/routed/usage?held=true".to_string()]
    );
}

#[tokio::test]
async fn silent_peer_incomplete() {
    let node = node_state().await;
    distributed(&node).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();
    assert_eq!((answer.ranges, answer.ranges_counted), (Some(2), Some(1)));
    assert!(!answer.complete);
}

#[tokio::test]
async fn answers_held_ranges() {
    let node = node_state().await;
    let snapshot = distributed(&node).await;
    let mut headers = HeaderMap::new();
    headers.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    headers.insert(
        RELAY_SET_HEADER,
        snapshot.set_id().to_string().parse().unwrap(),
    );
    let answer = usage(&node, &node.owner, headers, true).await.unwrap();
    let held = answer
        .held_ranges
        .expect("a member answers its held ranges");
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0].range_id,
        snapshot.config.ranges[0].range_id.to_string()
    );
    // A relay that names another set reaches no member bucket.
    let mut other = HeaderMap::new();
    other.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    other.insert(RELAY_SET_HEADER, Ulid::nil().to_string().parse().unwrap());
    assert!(matches!(
        usage(&node, &node.owner, other, true).await,
        Err(ServerError::Conflict(_))
    ));
}

// A reference page import into an ordinary bucket shows in the bucket's usage, as in its
// group's: objects, versions and referenced bytes.
#[tokio::test]
async fn reference_import_counted() {
    use aruna_core::structs::execution::job::{
        JobClaim, JobId, JobPayload, JobRecord, StagingJobCheckpoint, StagingJobPhase,
    };
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_operations::connectors::create_connector::{
        SourceConnectorInput, SourceConnectorOperation,
    };
    use aruna_operations::driver::drive;
    use aruna_operations::staging::page::import::{
        PageEntry, PageImportInput, PageImportOperation,
    };
    use std::time::{Duration, SystemTime};

    let node = node_state().await;
    let ctx = node.state.get_ctx();
    let input = SourceConnectorInput {
        group_id: node.group_id,
        created_by: node.owner.user_id,
        name: "public".to_string(),
        kind: SourceConnectorKind::S3,
        public_config: std::collections::HashMap::from([
            ("bucket".to_string(), "source".to_string()),
            ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ("skip_signature".to_string(), "true".to_string()),
        ]),
        secret_config: Default::default(),
    };
    let connector = drive(SourceConnectorOperation::new(input), ctx.as_ref())
        .await
        .unwrap()
        .connector;
    let (job_id, claim_token) = (JobId::from_bytes([7; 16]), Ulid::from_bytes([6; 16]));
    let payload = JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    };
    let local = node.state.get_node_id();
    let mut record = JobRecord::new(job_id, payload, node.owner.user_id, local, 1, 1, None);
    record.claim = Some(JobClaim {
        holder_node_id: local,
        claim_token,
        lease_expires_ms: u64::MAX,
    });
    aruna_operations::jobs::store::insert_job(&ctx.storage_handle, &record)
        .await
        .unwrap();
    let checkpoint = StagingJobCheckpoint {
        phase: StagingJobPhase::Registering,
        flat: Some(true),
        items_done: 0,
        prefixes_done: 1,
        walk: None,
        manifest: None,
        table: None,
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: None,
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    };
    let entry = |name: &str| PageEntry {
        source_path: format!("data/{name}"),
        target_key: name.to_string(),
        observation: SourceMetadata {
            content_length: 5,
            content_type: None,
            etag: Some(format!("\"{name}\"")),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(60)),
            source_version: None,
            digests: Default::default(),
        },
        metadata: Default::default(),
        mirrors: Vec::new(),
    };
    let page = PageImportInput {
        job_id,
        claim_token,
        realm_id: node.owner.realm_id,
        group_id: node.group_id,
        user_id: node.owner.user_id,
        auth_context: node.owner.clone(),
        node_id: local,
        bucket: BUCKET.to_string(),
        expected_bucket: bucket_info(&node.state, BUCKET).await.unwrap(),
        connector,
        secret_fingerprint: None,
        entries: vec![entry("a"), entry("b")],
        checkpoint,
        checkpoint_key: job_id.to_bytes().to_vec().into(),
        writes: Vec::new(),
        deletes: Vec::new(),
        now: SystemTime::UNIX_EPOCH + Duration::from_secs(120),
        ranges: None,
    };
    drive(PageImportOperation::new(page), ctx.as_ref())
        .await
        .unwrap();

    let answer = usage(&node, &node.owner, HeaderMap::new(), false)
        .await
        .unwrap();

    assert_eq!(
        (answer.objects, answer.versions, answer.referenced_bytes),
        (2, 2, 10)
    );
}
