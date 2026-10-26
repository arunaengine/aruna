//! Walks the flat spec prefixes of one reference import in parallel lanes, a prefix split into
//! key ranges where its listing allows, each lane from its own checkpoint row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};

use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::structs::execution::job::{
    JobId, StagingJobCheckpoint, StagingJobPhase, StagingJobSpec, StagingWalk,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use byteview::ByteView;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use super::walk::{PageFailure, directory_list_path, first_key, target_of, walk_page};
use super::{checkpoint_key, persist_rows};
use crate::driver::DriverContext;
use crate::jobs::executor::JobContext;
use crate::jobs::store::iter_prefix_page;
use crate::staging::page::rows::MAX_ERROR_SAMPLE;

/// Lanes one job walks at the same time. Each lane holds at most one listed page ahead, so a
/// job holds a bounded number of pages whatever the source holds.
pub(super) const LANE_LIMIT: usize = 4;
/// Slots per spec prefix: lane `slot` walks part `slot % PARTS` of prefix `slot / PARTS`.
const PARTS: u64 = LANE_LIMIT as u64;
/// One-key listings that split one prefix, whatever its key space looks like.
const MAX_PROBES: usize = 64;

/// One open lane: its checkpoint and the exclusive target key it stops before.
pub(super) type Lane = (u64, StagingJobCheckpoint, Option<String>);

/// Whether the job's remaining prefixes go to lanes: reference imports of flat listings without
/// a checksum manifest, whose prefixes write disjoint key ranges.
pub(super) fn applies(spec: &StagingJobSpec, checkpoint: &StagingJobCheckpoint) -> bool {
    let idle = checkpoint.walk.is_none()
        && checkpoint.manifest.is_none()
        && checkpoint.queue_head == checkpoint.queue_tail;
    spec.strategy == StagingStrategy::Reference
        && spec.checksum_manifest.is_none()
        && checkpoint.flat == Some(true)
        && idle
        && disjoint(spec)
}

/// No target prefix lies within another, so two lanes never write the same key.
fn disjoint(spec: &StagingJobSpec) -> bool {
    // The bucket root, an empty target prefix, holds every key.
    let folders: Vec<String> = (spec.prefixes.iter())
        .map(|prefix| prefix.target_prefix.trim_matches('/'))
        .map(|folder| match folder.is_empty() {
            true => String::new(),
            false => format!("{folder}/"),
        })
        .collect();
    folders.iter().enumerate().all(|(index, folder)| {
        (folders.iter().skip(index + 1))
            .all(|other| !folder.starts_with(other.as_str()) && !other.starts_with(folder.as_str()))
    })
}

/// The row of lane `slot` that holds where it stops, beside its checkpoint row.
fn bound_key(job_id: JobId, slot: u64) -> ByteView {
    let mut key = checkpoint_key(job_id, Some(slot)).to_vec();
    key.push(b'u');
    ByteView::from(key)
}

/// The open lanes of a job, each with its checkpoint and where it stops.
pub(super) async fn read_lanes(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<Lane>, String> {
    let prefix = ByteView::from(job_id.to_bytes().to_vec());
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        STAGING_STATE_KEYSPACE,
        Some(prefix),
        None,
        2 * LANE_LIMIT + 1,
        None,
    )
    .await?;
    let mut lanes: Vec<Lane> = Vec::new();
    for (key, value) in rows {
        let Some(slot) = (key.get(16..24)).and_then(|slot| <[u8; 8]>::try_from(slot).ok()) else {
            continue;
        };
        let slot = u64::from_be_bytes(slot);
        match key.len() {
            24 => lanes.push((slot, decode(&value)?, None)),
            // The bound row sorts right after its checkpoint row.
            _ => match lanes.last_mut() {
                Some(lane) if lane.0 == slot => lane.2 = decode(&value)?,
                _ => {}
            },
        }
    }
    Ok(lanes)
}

/// The job's checkpoint with its open lanes counted in. The first open lane's prefix reads as
/// the next to start, so a job handed over walks the open prefixes again.
pub(super) fn merged(mut main: StagingJobCheckpoint, lanes: Vec<Lane>) -> StagingJobCheckpoint {
    if let Some(first) = lanes.iter().map(|(slot, ..)| *slot).min() {
        main.prefixes_done = first / PARTS;
        main.phase = StagingJobPhase::Registering;
    }
    for (_, lane, _) in lanes {
        if main.current_path.is_none() {
            main.current_path = lane.current_path.clone();
        }
        fold(&mut main, lane);
    }
    main
}

/// Adds a lane's counters and errors to the job's.
fn fold(main: &mut StagingJobCheckpoint, lane: StagingJobCheckpoint) {
    main.items_current = main.items_current.saturating_add(lane.items_current);
    main.items_succeeded = main.items_succeeded.saturating_add(lane.items_succeeded);
    main.items_failed = main.items_failed.saturating_add(lane.items_failed);
    main.bytes_current = main.bytes_current.saturating_add(lane.bytes_current);
    main.bytes_discovered = main.bytes_discovered.saturating_add(lane.bytes_discovered);
    let room = MAX_ERROR_SAMPLE.saturating_sub(main.errors.len());
    main.errors.extend(lane.errors.into_iter().take(room));
}

/// Walks every prefix not walked yet, at most [`LANE_LIMIT`] at once, and folds each finished
/// lane into `main`. `Ok(false)` when cancellation or shutdown stopped the lanes first.
pub(super) async fn run_lanes(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    main: &mut StagingJobCheckpoint,
) -> Result<bool, PageFailure> {
    let mut open = read_lanes(&ctx.driver, ctx.job_id)
        .await
        .map_err(PageFailure::Retry)?;
    let resumed: u64 = open.iter().map(|(_, lane, _)| lane.items_current).sum();
    ctx.progress
        .set_current(main.items_current.saturating_add(resumed));
    let stop = AtomicBool::new(false);
    let mut lanes = FuturesUnordered::new();
    let mut failure = None;
    loop {
        while failure.is_none() && lanes.len() < LANE_LIMIT && !stopped(ctx, &stop) {
            if let Some(lane) = open.pop() {
                lanes.push(run_lane(ctx, spec, lane, &stop));
                continue;
            }
            let opened = start_prefix(ctx, spec, main, LANE_LIMIT - lanes.len()).await?;
            if opened.is_empty() {
                break;
            }
            open.extend(opened);
        }
        let Some((slot, outcome)) = lanes.next().await else {
            break;
        };
        match outcome {
            Ok(Some(lane)) => {
                fold(main, lane);
                let done = vec![
                    (
                        STAGING_STATE_KEYSPACE.to_string(),
                        checkpoint_key(ctx.job_id, Some(slot)),
                    ),
                    (
                        STAGING_STATE_KEYSPACE.to_string(),
                        bound_key(ctx.job_id, slot),
                    ),
                ];
                persist_rows(ctx, main, Vec::new(), done)
                    .await
                    .map_err(PageFailure::Retry)?;
            }
            Ok(None) => {}
            Err(error) => {
                // The other lanes finish their current page, so no page is left half done.
                failure.get_or_insert(error);
                stop.store(true, Ordering::Relaxed);
            }
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(!stopped(ctx, &stop) && open.is_empty()),
    }
}

fn stopped(ctx: &JobContext, stop: &AtomicBool) -> bool {
    stop.load(Ordering::Relaxed) || ctx.cancel.is_cancelled() || ctx.shutdown.is_cancelled()
}

/// Opens the lanes of the next spec prefix, at most `room`, recorded with the job's checkpoint
/// in one write; none once every prefix is open.
async fn start_prefix(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    main: &mut StagingJobCheckpoint,
    room: usize,
) -> Result<Vec<Lane>, PageFailure> {
    let index = main.prefixes_done;
    let Some(prefix) = usize::try_from(index)
        .ok()
        .and_then(|at| spec.prefixes.get(at))
    else {
        return Ok(Vec::new());
    };
    let walk = StagingWalk {
        source_path: directory_list_path(&prefix.source_prefix),
        target_prefix: prefix.target_prefix.clone(),
        cursor: None,
        queue_row: None,
        from: None,
    };
    let splits = split_points(ctx, spec, &walk, room).await?;
    let bounds: Vec<Option<String>> = std::iter::once(None)
        .chain(splits.iter().map(|split| target_of(&walk, split)))
        .collect();
    let mut lane = main.clone();
    lane.phase = StagingJobPhase::Registering;
    (lane.items_current, lane.items_succeeded, lane.items_failed) = (0, 0, 0);
    (lane.bytes_current, lane.bytes_discovered) = (0, 0);
    lane.current_path = None;
    lane.errors = Vec::new();
    let mut opened = Vec::new();
    let mut rows = Vec::new();
    for (part, from) in bounds.iter().enumerate() {
        let slot = index * PARTS + part as u64;
        let upper = bounds.get(part + 1).cloned().flatten();
        let mut part_lane = lane.clone();
        part_lane.walk = Some(StagingWalk {
            from: from.clone(),
            ..walk.clone()
        });
        let key = checkpoint_key(ctx.job_id, Some(slot));
        rows.push((STAGING_STATE_KEYSPACE.to_string(), key, encode(&part_lane)?));
        let bound = (
            STAGING_STATE_KEYSPACE.to_string(),
            bound_key(ctx.job_id, slot),
        );
        rows.push((bound.0, bound.1, encode(&upper)?));
        opened.push((slot, part_lane, upper));
    }
    main.prefixes_done += 1;
    persist_rows(ctx, main, rows, Vec::new())
        .await
        .map_err(PageFailure::Retry)?;
    Ok(opened)
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, String> {
    postcard::from_bytes(value).map_err(|error| error.to_string())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<ByteView, PageFailure> {
    let bytes = postcard::to_allocvec(value).map_err(|error| PageFailure::Retry(error.to_string()));
    Ok(ByteView::from(bytes?))
}

/// Source keys where the lanes of the prefix at `walk` start after its first: the distinct
/// characters that follow the longest start every key shares, found by one-key listings and
/// grouped into at most `parts` lanes. None when the keys cannot be told apart in a few probes.
async fn split_points(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    walk: &StagingWalk,
    parts: usize,
) -> Result<Vec<String>, PageFailure> {
    let mut shared = walk.source_path.clone();
    let mut probes = 0;
    while parts > 1 && probes < MAX_PROBES {
        let mut next = Vec::new();
        let mut lower = shared.clone();
        while probes < MAX_PROBES {
            probes += 1;
            let Some(key) = first_key(ctx, spec, walk, &lower).await? else {
                break;
            };
            let Some(rest) = key.strip_prefix(shared.as_str()) else {
                break;
            };
            let Some(first) = rest.chars().next() else {
                // A key that ends here sorts first; the probe goes on past it.
                lower = format!("{key}\0");
                continue;
            };
            next.push(first);
            match char::from_u32(u32::from(first) + 1) {
                Some(after) => lower = format!("{shared}{after}"),
                None => break,
            }
        }
        match next.as_slice() {
            [] => return Ok(Vec::new()),
            [only] => shared.push(*only),
            _ => {
                let lanes = parts.min(next.len());
                let starts = (1..lanes).map(|lane| next[lane * next.len() / lanes]);
                return Ok(starts.map(|first| format!("{shared}{first}")).collect());
            }
        }
    }
    Ok(Vec::new())
}

/// Walks one lane page by page; `Ok(None)` when it stopped early and stays open.
async fn run_lane(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    (slot, mut lane, upper): Lane,
    stop: &AtomicBool,
) -> (u64, Result<Option<StagingJobCheckpoint>, PageFailure>) {
    let mut ahead = None;
    loop {
        if lane.walk.is_none() {
            return (slot, Ok(Some(lane)));
        }
        if stopped(ctx, stop) {
            return (slot, Ok(None));
        }
        let bounds = (Some(slot), upper.as_deref());
        if let Err(error) = walk_page(ctx, spec, bounds, &mut lane, &mut ahead).await {
            return (slot, Err(error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::job::{StagingJobError, StagingJobPrefix};

    fn spec(targets: &[&str]) -> StagingJobSpec {
        let mut spec = super::super::pure_tests::test_spec();
        spec.prefixes = (targets.iter())
            .map(|target| StagingJobPrefix {
                source_prefix: format!("src/{target}"),
                target_prefix: target.to_string(),
            })
            .collect();
        spec
    }

    // Lanes only run when no two prefixes can write the same key.
    #[test]
    fn nested_targets_sequential() {
        assert!(disjoint(&spec(&["a", "b", "ab"])));
        assert!(!disjoint(&spec(&["a", "a/b"])));
        assert!(!disjoint(&spec(&["", "b"])));
    }

    // Open lanes count in the job's progress, and their prefixes read as not started.
    #[test]
    fn merged_counts_lanes() {
        let mut main = super::super::initial_checkpoint(&spec(&["a", "b", "c"]));
        main.prefixes_done = 3;
        main.items_current = 10;
        let mut lane = main.clone();
        lane.items_current = 5;
        lane.items_failed = 1;
        lane.errors = vec![StagingJobError {
            source_path: "src/b/x".to_string(),
            target_key: "b/x".to_string(),
            error: "denied".to_string(),
        }];

        let view = merged(main, vec![(4, lane.clone(), None), (5, lane, None)]);

        assert_eq!((view.prefixes_done, view.items_current), (1, 20));
        assert_eq!((view.items_failed, view.errors.len()), (2, 2));
    }

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
                    true => (StatusCode::OK, [("content-length", "3"), ("etag", "\"e\"")])
                        .into_response(),
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

    // The splits of a prefix are the distinct characters after the start its keys share.
    #[tokio::test]
    async fn splits_after_shared_start() {
        let keys = ["p/SAMD1", "p/SAMD2", "p/SAMEA1", "p/SAMN1", "p/SAMN2"];
        let (endpoint, server) = s3_listing(keys.iter().map(|key| key.to_string()).collect()).await;
        let (ctx, spec, _dir) = job_context(endpoint, &[("p", "t")]).await;
        let walk = StagingWalk {
            source_path: "p/".to_string(),
            target_prefix: "t".to_string(),
            cursor: None,
            queue_row: None,
            from: None,
        };

        let splits = split_points(&ctx, &spec, &walk, 4).await.ok().unwrap();
        server.abort();

        assert_eq!(splits, vec!["p/SAME".to_string(), "p/SAMN".to_string()]);
        assert_eq!(target_of(&walk, &splits[1]).as_deref(), Some("t/SAMN"));
    }

    /// A claimed reference import of `prefixes`, source and target, from a connector at
    /// `endpoint`; the directory guard keeps its storage alive.
    async fn job_context(
        endpoint: String,
        prefixes: &[(&str, &str)],
    ) -> (JobContext, StagingJobSpec, tempfile::TempDir) {
        use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
        use crate::driver::drive;
        use crate::jobs::executor::ProgressReporter;
        use crate::tests::staging::{create_test_bucket, setup_driver_context};
        use aruna_core::structs::execution::job::{JobClaim, JobPayload, JobProgress, JobRecord};
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;
        use ulid::Ulid;

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
        spec.auth_context = crate::tests::access::seed_access(&driver, scope, "/none").await;
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
        crate::jobs::store::insert_job(&driver.storage_handle, &record)
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
        };
        (ctx, spec, test._tempdir)
    }

    /// Runs a reference import of `keys` and returns how many files it registered, after
    /// checking that each one has a head and no lane row is left.
    async fn import(keys: Vec<String>, prefixes: &[(&str, &str)]) -> u64 {
        use crate::jobs::executor::JobRunOutcome;
        use aruna_core::events::{Event, StorageEvent};
        use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
        use aruna_core::structs::execution::job::JobResultPayload;
        use aruna_core::structs::storage::blob::BlobHeadKey;

        let (endpoint, server) = s3_listing(keys.clone()).await;
        let (ctx, spec, _dir) = job_context(endpoint, prefixes).await;

        let outcome = super::super::run_staging_job(&ctx, &spec).await;
        server.abort();

        let JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items,
            failed_items,
        }) = outcome
        else {
            panic!("the import must succeed");
        };
        assert_eq!(failed_items, 0);
        assert!(
            read_lanes(&ctx.driver, ctx.job_id)
                .await
                .unwrap()
                .is_empty()
        );
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
}
