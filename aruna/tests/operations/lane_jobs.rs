//! Tests staging jobs that import prefixes in lanes, run under the node's job host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::Event;
use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::job::StagingJobSpec;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::types::Key;
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::staging::StagingJobOperation;
use aruna_operations::jobs::staging::StagingRun;
use aruna_operations::jobs::staging::lanes::LANE_ROWS;
use aruna_operations::jobs::staging::lanes::parse_lanes;
use aruna_operations::tests::staging::spec;
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;

/// An S3 endpoint that lists `keys` page by page, as ListObjectsV2 does.
async fn s3_listing(keys: Vec<String>) -> (String, tokio::task::JoinHandle<()>) {
    use axum::http::{Method, StatusCode};
    use axum::response::IntoResponse;
    let answer = move |method: Method, uri: axum::http::Uri| {
        // A stat of one key, as the inclusive lower key of a lane is read.
        if method == Method::HEAD {
            let key = uri.path().trim_start_matches("/source/").to_string();
            let found = keys.contains(&key);
            return std::future::ready(match found {
                true => {
                    (StatusCode::OK, [("content-length", "3"), ("etag", "\"e\"")]).into_response()
                }
                false => StatusCode::NOT_FOUND.into_response(),
            });
        }
        let query: std::collections::HashMap<String, String> =
            url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let get = |name: &str| query.get(name).cloned().unwrap_or_default();
        let floor = get("start-after").max(get("continuation-token"));
        let limit = get("max-keys").parse::<usize>().unwrap_or(1000);
        let prefix = get("prefix");
        let listed: Vec<&String> = (keys.iter())
            .filter(|key| key.starts_with(&prefix) && **key > floor)
            .collect();
        let page = &listed[..limit.min(listed.len())];
        let truncated = page.len() < listed.len();
        let mut body = format!("<ListBucketResult><IsTruncated>{truncated}</IsTruncated>");
        if let (true, Some(last)) = (truncated, page.last()) {
            body.push_str(&format!(
                "<NextContinuationToken>{last}</NextContinuationToken>"
            ));
        }
        for key in page {
            body.push_str(&format!(
                "<Contents><Key>{key}</Key><LastModified>2026-01-02T03:04:05.678Z\
                 </LastModified><ETag>&quot;e&quot;</ETag><Size>3</Size></Contents>"
            ));
        }
        body.push_str("</ListBucketResult>");
        std::future::ready(([("content-type", "application/xml")], body).into_response())
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().fallback(answer);
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, task)
}

// Three prefixes of two pages each are walked in lanes with a page listed ahead; every
// file is registered once and no lane row is left behind.
#[tokio::test]
async fn lanes_import_prefixes() {
    let keys = (["a", "b", "c"].iter())
        .flat_map(|folder| (0..510).map(move |index| format!("{folder}/{index:04}")))
        .collect();
    let registered = import(keys, &[("a", "ta"), ("b", "tb"), ("c", "tc")]).await;
    assert_eq!(registered, 1530);
}

// One prefix whose keys differ only after a shared start is split into key range lanes;
// every file is registered once, the files at the lane borders included.
#[tokio::test]
async fn prefix_split_lanes() {
    let samples = ["SAMD", "SAMEA", "SAMN", "SAMN9"];
    let keys = (samples.iter())
        .flat_map(|sample| (0..260).map(move |index| format!("all/{sample}{index:04}")))
        .collect();
    let registered = import(keys, &[("all", "")]).await;
    assert_eq!(registered, 1040);
}

// A file the submitter may not write fails alone; the rest of its page registers.
#[tokio::test]
async fn denied_file_counted() {
    use aruna_core::structs::execution::job::JobResultPayload;
    use aruna_operations::jobs::executor::JobRunOutcome;
    let keys = (0..20).map(|index| format!("a/{index:04}")).collect();
    let (endpoint, server) = s3_listing(keys).await;
    let (ctx, spec, _dir) = job_context(endpoint, &[("a", "ta")], Some("ta/0007")).await;

    let outcome = run_staging_job(&ctx, &spec).await;
    server.abort();

    let JobRunOutcome::Succeeded(JobResultPayload::Staging {
        completed_items,
        failed_items,
    }) = outcome
    else {
        panic!("the import must succeed");
    };
    assert_eq!((completed_items, failed_items), (19, 1));
}

// A prefix larger than one gathered page commits in several gathered pages; every file
// is registered once.
#[tokio::test]
async fn gathered_pages_import() {
    let keys = (0..6_100).map(|index| format!("a/{index:05}")).collect();
    let registered = import(keys, &[("a", "ta")]).await;
    assert_eq!(registered, 6_100);
}

/// A claimed reference import of `prefixes`, source and target, from a connector at
/// `endpoint`; the directory guard keeps its storage alive.
async fn job_context(
    endpoint: String,
    prefixes: &[(&str, &str)],
    denied: Option<&str>,
) -> (JobContext, StagingJobSpec, tempfile::TempDir) {
    use aruna_core::structs::execution::job::{JobClaim, JobPayload, JobProgress, JobRecord};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::storage::blob::object_permission_path;
    use aruna_operations::connectors::create_connector::{
        SourceConnectorInput, SourceConnectorOperation,
    };
    use aruna_operations::driver::drive;
    use aruna_operations::jobs::executor::ProgressReporter;
    use aruna_operations::tests::staging::{create_test_bucket, setup_driver_context};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let test = setup_driver_context().await;
    let driver = test.driver_context;
    let targets: Vec<&str> = prefixes.iter().map(|(_, target)| *target).collect();
    let mut spec = spec(&targets);
    for (prefix, (source, _)) in spec.prefixes.iter_mut().zip(prefixes) {
        prefix.source_prefix = source.to_string();
    }
    let input = SourceConnectorInput {
        group_id: spec.group_id,
        created_by: Default::default(),
        name: "listing".to_string(),
        kind: SourceConnectorKind::S3,
        public_config: std::collections::HashMap::from([
            ("bucket".to_string(), "source".to_string()),
            ("endpoint".to_string(), endpoint),
            ("region".to_string(), "us-east-1".to_string()),
            ("skip_signature".to_string(), "true".to_string()),
        ]),
        secret_config: Default::default(),
    };
    let connector = drive(SourceConnectorOperation::new(input), &driver)
        .await
        .unwrap()
        .connector;
    spec.connector_id = connector.connector_id;
    let scope = (spec.auth_context.realm_id, spec.group_id, spec.node_id);
    let denied = denied.map(|key| {
        let (realm, group, node) = scope;
        object_permission_path(realm, group, node, &spec.bucket, key)
    });
    let denied = denied.as_deref().unwrap_or("/none");
    spec.auth_context = aruna_operations::testing_access::seed_access(&driver, scope, denied).await;
    let owner = spec.auth_context.user_id;
    let bucket = create_test_bucket(&driver, spec.group_id, owner, &spec.bucket).await;
    spec.bucket_identity = bucket.identity();
    let (job_id, claim_token) = (JobId::from_bytes([7; 16]), Ulid::from_bytes([6; 16]));
    let payload = JobPayload::Staging(spec.clone());
    let mut record = JobRecord::new(job_id, payload, owner, spec.node_id, 1, 1, None);
    record.claim = Some(JobClaim {
        holder_node_id: spec.node_id,
        claim_token,
        lease_expires_ms: u64::MAX,
    });
    aruna_operations::jobs::store::test_calls::insert_job(&driver, &record)
        .await
        .unwrap();
    let ctx = JobContext {
        driver: Arc::new(driver),
        job_id,
        owner_node_id: spec.node_id,
        claim_token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("items")),
        range_turns: Default::default(),
        handlers: Default::default(),
    };
    (ctx, spec, test._tempdir)
}

/// Runs a reference import of `keys` and returns how many files it registered, after
/// checking that each one has a head and no lane row is left.
async fn import(keys: Vec<String>, prefixes: &[(&str, &str)]) -> u64 {
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
    use aruna_core::structs::execution::job::JobResultPayload;
    use aruna_core::structs::storage::blob::BlobHeadKey;
    use aruna_operations::jobs::executor::JobRunOutcome;

    let (endpoint, server) = s3_listing(keys.clone()).await;
    let (ctx, spec, _dir) = job_context(endpoint, prefixes, None).await;

    let outcome = run_staging_job(&ctx, &spec).await;
    server.abort();

    let JobRunOutcome::Succeeded(JobResultPayload::Staging {
        completed_items,
        failed_items,
    }) = outcome
    else {
        panic!("the import must succeed");
    };
    assert_eq!(failed_items, 0);
    let prefix = ctx.job_id.to_bytes().to_vec().into();
    let (rows, _) = aruna_storage::scan::iter_prefix_page(
        &ctx.driver.storage_handle,
        STAGING_STATE_KEYSPACE,
        Some(prefix),
        None,
        LANE_ROWS,
        None,
    )
    .await
    .unwrap();
    assert!(parse_lanes(rows).unwrap().is_empty());
    assert_eq!(ctx.progress.snapshot().current, completed_items);
    for key in &keys {
        let (source, target) = (prefixes.iter())
            .find(|(source, _)| key.starts_with(&format!("{source}/")))
            .unwrap();
        let relative = &key[source.len() + 1..];
        let target = match target.is_empty() {
            true => relative.to_string(),
            false => format!("{target}/{relative}"),
        };
        let head = BlobHeadKey::new(&spec.bucket, &target).to_bytes().unwrap();
        let read = aruna_core::effects::StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: head.into(),
            txn_id: None,
        };
        let event = ctx.driver.storage_handle.send_storage_effect(read).await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("unexpected read event");
        };
        assert!(value.is_some(), "{target} must be registered");
    }
    completed_items
}

/// Runs a staging job under the node's job host, without the pacing of table imports.
pub(crate) async fn run_staging_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    let run = aruna_operations::jobs::staging::StagingRun {
        spec: std::sync::Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        routing: aruna_operations::driver::node_routing(&ctx.driver),
        seed: 7,
    };
    let job = aruna_operations::jobs::staging::StagingJobOperation::new(run);
    aruna::tasks::job_host::run_hosted(ctx, job).await
}
