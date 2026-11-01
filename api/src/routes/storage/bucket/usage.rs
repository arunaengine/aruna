//! Serves the bucket usage route that counts objects, versions and bytes of one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use aruna_core::structs::storage::distribution::capacity::RangeSize;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionSnapshot};
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
use aruna_operations::distribution::status::read_view;
use aruna_operations::distribution::usage::{HeldRange, RangeUsage, held_ranges, sum_held};
use aruna_operations::node::usage_stats::usage_complete;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::relay::{RestForward, relay_get};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketUsageDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketUsageDoc::openapi()).routes(routes!(get_bucket_usage))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketUsageResponse {
    pub bucket: String,
    /// Current heads that are not delete markers.
    pub objects: u64,
    /// Stored versions including noncurrent ones, excluding delete markers.
    pub versions: u64,
    pub delete_markers: u64,
    pub open_multipart_uploads: u64,
    /// Bytes of all stored versions, the group quota basis.
    pub logical_bytes: u64,
    /// Bytes reachable only through external references.
    pub referenced_bytes: u64,
    /// Counter completeness: false for a distributed bucket while a range was not counted or its
    /// counters are being rebuilt, and for any bucket while this node rebuilds its counters. It
    /// says nothing about replication; see `current`.
    pub complete: bool,
    /// Replication freshness of a distributed bucket: true when every range was counted at its
    /// coordinator, so the totals include every write the coordinators acknowledged. Absent
    /// when a range was counted from a copy or not at all, whose lag is unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<bool>,
    /// Where each range of a distributed bucket was counted; absent for an ordinary bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_sources: Option<Vec<RangeSourceResponse>>,
    /// Ranges of a distributed bucket; absent for an ordinary bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranges: Option<usize>,
    /// Ranges of a distributed bucket whose counters were summed, here or by their holders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ranges_counted: Option<usize>,
    /// The counter rows of each range this node holds; only on a `held` request another member
    /// relays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_ranges: Option<Vec<HeldRangeResponse>>,
}

/// The charged counter rows of one range a node holds.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HeldRangeResponse {
    pub range_id: String,
    pub complete: bool,
    /// The answering node coordinates the range; otherwise its rows are a copy's.
    #[serde(default)]
    pub coordinated: bool,
    pub rows: Vec<CounterRowResponse>,
}

/// Whose counters a range was counted from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CounterSource {
    /// The range's coordinator, which every write of the range passes.
    Coordinator,
    /// A holder's copy, which may lag behind the coordinator by the replication delay.
    Copy,
}

/// Where one range of a distributed bucket was counted, against the answering node's record.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RangeSourceResponse {
    pub range_id: String,
    /// The node coordinating the range, and its coordinator epoch.
    pub coordinator: Option<String>,
    pub epoch: u64,
    /// The record revision the answering node holds.
    pub revision: u64,
    /// The node whose counter rows were summed; absent when no holder answered.
    pub counted_by: Option<String>,
    pub source: Option<CounterSource>,
    /// Whether the counted rows include every write the coordinator acknowledged: true when
    /// counted at the coordinator, absent (unknown) for a copy without such evidence.
    pub current: Option<bool>,
    /// The counted rows are exact, as `complete` is for the bucket.
    pub complete: bool,
}

/// One counter row by its key, so rows two ranges share are added once.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CounterRowResponse {
    /// The row key, hex encoded.
    pub row: String,
    pub objects: i64,
    pub current_bytes: i64,
    pub versions: i64,
    pub delete_markers: i64,
    pub logical_bytes: i64,
    pub referenced_bytes: i64,
    pub open_uploads: i64,
}

impl CounterRowResponse {
    fn of(row: &[u8], counters: &RangeCounters) -> Self {
        let value = |value: i128| i64::try_from(value).unwrap_or(i64::MAX);
        Self {
            row: hex::encode(row),
            objects: value(counters.objects),
            current_bytes: value(counters.current_bytes),
            versions: value(counters.versions),
            delete_markers: value(counters.delete_markers),
            logical_bytes: value(counters.logical_bytes),
            referenced_bytes: value(counters.referenced_bytes),
            open_uploads: value(counters.open_uploads),
        }
    }

    fn counters(&self) -> Option<(Vec<u8>, RangeCounters)> {
        let counters = RangeCounters {
            objects: self.objects.into(),
            current_bytes: self.current_bytes.into(),
            versions: self.versions.into(),
            delete_markers: self.delete_markers.into(),
            logical_bytes: self.logical_bytes.into(),
            referenced_bytes: self.referenced_bytes.into(),
            open_uploads: self.open_uploads.into(),
            ..RangeCounters::default()
        };
        Some((hex::decode(&self.row).ok()?, counters))
    }
}

impl HeldRangeResponse {
    fn of(range: &HeldRange) -> Self {
        Self {
            range_id: range.range_id.to_string(),
            complete: range.complete,
            coordinated: range.coordinated,
            rows: (range.rows.iter())
                .map(|(row, counters)| CounterRowResponse::of(row, counters))
                .collect(),
        }
    }

    fn held(&self) -> Option<HeldRange> {
        Some(HeldRange {
            range_id: Ulid::from_string(&self.range_id).ok()?,
            complete: self.complete,
            coordinated: self.coordinated,
            rows: self
                .rows
                .iter()
                .map(CounterRowResponse::counters)
                .collect::<Option<_>>()?,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UsageQuery {
    /// Answers the counter rows of each range this node holds; only for requests another member
    /// of the distributed bucket relays.
    pub held: Option<bool>,
}

impl BucketUsageResponse {
    fn new(bucket: String, counters: BucketCounters, complete: bool) -> Self {
        Self {
            bucket,
            objects: counters.objects,
            versions: counters.versions,
            delete_markers: counters.delete_markers,
            open_multipart_uploads: counters.open_uploads,
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
            complete,
            current: None,
            range_sources: None,
            ranges: None,
            ranges_counted: None,
            held_ranges: None,
        }
    }

    fn of_ranges(bucket: String, usage: RangeUsage) -> Self {
        let count = |value: i128| u64::try_from(value.max(0)).unwrap_or(u64::MAX);
        let counters = usage.counters;
        Self {
            bucket,
            objects: count(counters.objects),
            versions: count(counters.versions),
            delete_markers: count(counters.delete_markers),
            open_multipart_uploads: count(counters.open_uploads),
            logical_bytes: count(counters.logical_bytes),
            referenced_bytes: count(counters.referenced_bytes),
            complete: usage.complete,
            current: None,
            range_sources: None,
            ranges: Some(usage.ranges),
            ranges_counted: Some(usage.counted),
            held_ranges: None,
        }
    }
}

/// Holders one usage answer asks at most for the ranges this node does not coordinate.
const MAX_HOLDERS_ASKED: usize = 16;

/// The counter rows counted for one range and the node that served them.
struct CountedRange {
    range: HeldRange,
    node: NodeId,
}

/// The usage of a distributed bucket and where each range was counted: the ranges this node
/// coordinates, the others at their coordinators, and a copy only where one did not answer.
async fn merged_usage(
    state: &ServerState,
    caller: (&str, Option<&ValidatedBearer>),
    (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
    held: Vec<HeldRange>,
) -> ServerResult<(RangeUsage, Vec<RangeSourceResponse>)> {
    let member = Some(binding.member_id);
    let ranges = holder_ranges(state, caller, (member, snapshot), held).await?;
    let wanted = snapshot.config.ranges.len();
    let mut sources = Vec::with_capacity(wanted);
    let mut counted = Vec::with_capacity(wanted);
    for range in &snapshot.config.ranges {
        let found = ranges.get(&range.range_id);
        let coordinator = snapshot.member(range.coordinator);
        let source = found.map(|found| match found.range.coordinated {
            true => CounterSource::Coordinator,
            false => CounterSource::Copy,
        });
        sources.push(RangeSourceResponse {
            range_id: range.range_id.to_string(),
            coordinator: coordinator.map(|member| member.node_id.to_string()),
            epoch: range.epoch,
            revision: snapshot.number,
            counted_by: found.map(|found| found.node.to_string()),
            source,
            current: (source == Some(CounterSource::Coordinator)).then_some(true),
            complete: found.is_some_and(|found| found.range.complete),
        });
        counted.extend(found.map(|found| &found.range));
    }
    let counters = sum_held(counted.iter().copied())
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let usage = RangeUsage {
        counters,
        ranges: wanted,
        counted: counted.len(),
        complete: counted.len() == wanted && counted.iter().all(|range| range.complete),
    };
    Ok((usage, sources))
}

/// Adds a holder's answer: a coordinator's rows count as they are, a copy's only as fallback.
fn merge_answer(
    answer: Option<BucketUsageResponse>,
    node: NodeId,
    ranges: &mut BTreeMap<Ulid, CountedRange>,
    copies: &mut BTreeMap<Ulid, CountedRange>,
) {
    let held = answer.and_then(|answer| answer.held_ranges);
    for range in held
        .unwrap_or_default()
        .iter()
        .filter_map(HeldRangeResponse::held)
    {
        let target = match range.coordinated {
            true => &mut *ranges,
            false => &mut *copies,
        };
        target
            .entry(range.range_id)
            .or_insert(CountedRange { range, node });
    }
}

/// The counter rows of every range: its coordinator's where it answers, else a copy's. A range
/// no holder answered for is left out.
async fn holder_ranges(
    state: &ServerState,
    (bucket, bearer): (&str, Option<&ValidatedBearer>),
    (member, snapshot): (Option<Ulid>, &DistributionSnapshot),
    held: Vec<HeldRange>,
) -> ServerResult<BTreeMap<Ulid, CountedRange>> {
    let local = state.get_node_id();
    let (mut ranges, mut copies) = (BTreeMap::new(), BTreeMap::new());
    for range in held {
        let target = match range.coordinated {
            true => &mut ranges,
            false => &mut copies,
        };
        target.insert(range.range_id, CountedRange { range, node: local });
    }
    let mut asked = BTreeSet::new();
    let path = format!("/data/buckets/{bucket}/usage?held=true");
    let set_id = snapshot.set_id();
    // Each range's coordinator first, so a lagging copy is never summed where it can be avoided.
    for range in &snapshot.config.ranges {
        let coordinator = snapshot.member(range.coordinator);
        let Some(coordinator) = coordinator.filter(|c| Some(c.member_id) != member) else {
            continue;
        };
        let node_id = coordinator.node_id;
        if ranges.contains_key(&range.range_id)
            || asked.contains(&node_id)
            || asked.len() >= MAX_HOLDERS_ASKED
        {
            continue;
        }
        asked.insert(node_id);
        let range_id = range.range_id;
        let forward = RestForward {
            set_id,
            range_id,
            node_id,
        };
        let answer = relay_get(state, &forward, &path, bearer, HeaderMap::new()).await?;
        merge_answer(answer, node_id, &mut ranges, &mut copies);
    }
    for range in &snapshot.config.ranges {
        if ranges.contains_key(&range.range_id) {
            continue;
        }
        if !copies.contains_key(&range.range_id) {
            let holders = snapshot.record_holders(range).into_iter();
            let holder = holders
                .filter(|holder| Some(holder.member_id) != member)
                .find(|holder| !asked.contains(&holder.node_id));
            if let Some(holder) = holder.filter(|_| asked.len() < MAX_HOLDERS_ASKED) {
                asked.insert(holder.node_id);
                let forward = RestForward {
                    set_id,
                    range_id: range.range_id,
                    node_id: holder.node_id,
                };
                let answer = relay_get(state, &forward, &path, bearer, HeaderMap::new()).await?;
                merge_answer(answer, holder.node_id, &mut ranges, &mut copies);
            }
        }
        if let Some(copy) = copies.remove(&range.range_id) {
            ranges.entry(range.range_id).or_insert(copy);
        }
    }
    Ok(ranges)
}

/// Each range's expected size from its coordinator's complete counters, for capacity admission.
/// A copy may lag, so a range whose coordinator did not answer is left out and refused; a split
/// child includes its parent's shared row, so sizes may exceed the data.
pub(crate) async fn range_sizes(
    state: &ServerState,
    bearer: Option<&ValidatedBearer>,
    set_id: Ulid,
) -> ServerResult<Vec<RangeSize>> {
    let internal = |error: String| ServerError::InternalError(error);
    let view = read_view(&state.get_ctx(), set_id).await;
    let Some(view) = view.map_err(|error| internal(error.to_string()))? else {
        return Ok(Vec::new());
    };
    let snapshot = &view.head.snapshot;
    let bucket = snapshot.config.bucket.as_str();
    let local = held_ranges(&state.get_ctx(), bucket).await;
    let local = local.map_err(|error| ServerError::InternalError(error.to_string()))?;
    let (member, held) = match local {
        Some((binding, _, held)) if binding.set_id == snapshot.set_id() => {
            (Some(binding.member_id), held)
        }
        _ => (None, Vec::new()),
    };
    let ranges = holder_ranges(state, (bucket, bearer), (member, snapshot), held).await?;
    let count = |value: i128| u64::try_from(value.max(0)).unwrap_or(u64::MAX);
    let mut sizes = Vec::with_capacity(ranges.len());
    let counted = (ranges.into_iter())
        .filter(|(_, counted)| counted.range.complete && counted.range.coordinated);
    for (range_id, counted) in counted {
        let counters = sum_held([&counted.range])
            .map_err(|error| ServerError::InternalError(error.to_string()))?;
        sizes.push(RangeSize {
            range_id,
            bytes: count(counters.logical_bytes),
            objects: count(counters.versions.saturating_add(counters.delete_markers)),
        });
    }
    Ok(sizes)
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/usage",
    tag = "data/storage",
    summary = "Read one bucket's stored inventory",
    description = r#"Reports how many objects, versions, delete markers and open multipart uploads a bucket holds on this node, and how many bytes they occupy.

**Authentication**: realm bearer token with READ on the bucket, the same right that lists its
objects. Without a token, READ granted by public roles applies, and a refusal answers 401.

**Behavior**
- Every number comes from this node's counter of the bucket, kept up to date in the same
  transaction as each write, delete, upload start, completion and abort, so the answer costs one
  read however large the bucket is.
- `objects` counts current heads that are not delete markers, `versions` every stored version
  including noncurrent ones, and `logical_bytes` the bytes of those versions, as the group quota
  does. `referenced_bytes` sums the sizes of versions that only point to external data.
- `open_multipart_uploads` counts uploads not yet completed or aborted, including one a
  completion or abort is finishing.
- A distributed bucket answers from the maintained counters of the key ranges: this node reads
  the ranges it coordinates and asks each other range's coordinator, each node once and at most
  16, with the caller's token. Only where a coordinator does not answer is a copy counted, this
  node's own or another holder's, which may lag behind by the replication delay. `ranges` and
  `ranges_counted` say how many ranges the set has and how many were summed; `complete` is false
  while a range had no answering holder or its counters are being rebuilt.
- `complete` is counter completeness only. Replication freshness is reported apart:
  `range_sources` names, per range, the coordinator, its epoch, the record revision, the node
  whose counters were summed and whether they are the coordinator's (`source`); `current` is
  true only when every range was counted at its coordinator and is absent, meaning unknown,
  when a copy was counted.
- `held=true` answers only the counter rows of each range this node holds, in `held_ranges`,
  each marked `coordinated` when this node coordinates it; it is accepted only on requests
  another member of the distributed bucket relays.
- For an ordinary bucket `complete` is false only while this node rebuilds its usage counters
  after a restart that found them missing or a rebuild interrupted; the numbers are then low."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        UsageQuery
    ),
    responses(
        (
            status = 200,
            description = "The bucket's node-local inventory and byte total",
            body = BucketUsageResponse,
            example = json!({
                "bucket": "research-raw",
                "objects": 128,
                "versions": 194,
                "delete_markers": 3,
                "open_multipart_uploads": 1,
                "logical_bytes": 84291337,
                "referenced_bytes": 0,
                "complete": true
            })
        ),
        (status = 400, description = "`held` on a request no member relayed", body = ErrorResponse),
        (status = 401, description = "Invalid bearer token, or no public READ without one", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
pub async fn get_bucket_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(query): Query<UsageQuery>,
) -> ServerResult<Json<BucketUsageResponse>> {
    // Signed out, only public roles grant READ, and a refusal hides whether the bucket exists.
    let signed_out = auth.is_none();
    let auth = auth.unwrap_or_else(|| AuthContext::anonymous(state.get_realm_id()));
    let auth = require_realm_auth(&state, Some(auth))?;
    let hidden = |error| match error {
        ServerError::NotFound | ServerError::Forbidden if signed_out => ServerError::Unauthorized,
        error => error,
    };
    let info = bucket_info(&state, &bucket).await.map_err(hidden)?;
    ensure_permission(
        &state,
        &auth,
        bucket_permission_path(
            state.get_realm_id(),
            info.group_id,
            state.get_node_id(),
            &bucket,
        ),
        Permission::READ,
    )
    .await
    .map_err(hidden)?;
    let held = query.held == Some(true);
    if held && !headers.contains_key(RELAY_HOP_HEADER) {
        return Err(ServerError::BadRequestReason(
            "held is only accepted from another member".into(),
        ));
    }
    crate::routes::storage::relay::relayed_set(&info, &headers)?;
    let ranges = held_ranges(&state.get_ctx(), &bucket).await;
    match ranges.map_err(|error| ServerError::InternalError(error.to_string()))? {
        Some((_, _, ranges)) if held => {
            let usage = RangeUsage {
                counters: RangeCounters::default(),
                ranges: 0,
                counted: ranges.len(),
                complete: false,
            };
            let mut response = BucketUsageResponse::of_ranges(bucket, usage);
            response.held_ranges = Some(ranges.iter().map(HeldRangeResponse::of).collect());
            Ok(Json(response))
        }
        Some((binding, snapshot, ranges)) => {
            let caller = (bucket.as_str(), bearer.as_ref());
            let merged = merged_usage(&state, caller, (&binding, &snapshot), ranges).await?;
            let (usage, sources) = merged;
            let current = sources.iter().all(|source| source.current == Some(true));
            let mut response = BucketUsageResponse::of_ranges(bucket, usage);
            response.current = current.then_some(true);
            response.range_sources = Some(sources);
            Ok(Json(response))
        }
        None => {
            let counters = bucket_counters(&state, &bucket).await?;
            let complete = usage_complete(&state.get_ctx())
                .await
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            Ok(Json(BucketUsageResponse::new(bucket, counters, complete)))
        }
    }
}

/// The bucket's counter row; a bucket without one holds nothing yet.
async fn bucket_counters(state: &ServerState, bucket: &str) -> ServerResult<BucketCounters> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;

    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key(bucket).into(),
        txn_id: None,
    };
    let value = match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(read)
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            let error = format!("could not read the bucket counter: {other:?}");
            return Err(ServerError::InternalError(error));
        }
    };
    let counters = value.as_deref().map(BucketCounters::from_bytes).transpose();
    let counters = counters.map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(counters.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_rows_decode() {
        let counters = RangeCounters {
            objects: 3,
            logical_bytes: -7,
            open_uploads: 1,
            ..RangeCounters::default()
        };
        let range = HeldRange {
            range_id: Ulid::from_bytes([4; 16]),
            complete: true,
            coordinated: true,
            rows: vec![(vec![1, 2, 255], counters)],
        };
        let back = HeldRangeResponse::of(&range).held().unwrap();
        assert_eq!(back, range);
        let mut broken = HeldRangeResponse::of(&range);
        broken.rows[0].row = "not hex".to_string();
        assert!(
            broken.held().is_none(),
            "an unreadable row is never counted"
        );
    }
}

#[cfg(test)]
#[path = "usage_tests.rs"]
mod route_tests;
