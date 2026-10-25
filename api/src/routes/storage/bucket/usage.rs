//! Serves the bucket usage route that counts objects, versions and bytes of one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionSnapshot};
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
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
    /// False for a distributed bucket while a range is not held here or is being rebuilt, and
    /// for any bucket while this node rebuilds its usage counters.
    pub complete: bool,
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
    pub rows: Vec<CounterRowResponse>,
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
            rows: (range.rows.iter())
                .map(|(row, counters)| CounterRowResponse::of(row, counters))
                .collect(),
        }
    }

    fn held(&self) -> Option<HeldRange> {
        Some(HeldRange {
            range_id: Ulid::from_string(&self.range_id).ok()?,
            complete: self.complete,
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
            ranges: Some(usage.ranges),
            ranges_counted: Some(usage.counted),
            held_ranges: None,
        }
    }
}

/// Holders one usage answer asks at most for the ranges this node does not hold.
const MAX_HOLDERS_ASKED: usize = 16;

/// The usage of a distributed bucket: the ranges this node holds, then the others from their
/// holders, each holder asked once and each shared counter row added once.
async fn merged_usage(
    state: &ServerState,
    (bucket, bearer): (&str, Option<&ValidatedBearer>),
    (binding, snapshot): (&DistributionBinding, &DistributionSnapshot),
    held: Vec<HeldRange>,
) -> ServerResult<RangeUsage> {
    let mut ranges: BTreeMap<Ulid, HeldRange> = held
        .into_iter()
        .map(|range| (range.range_id, range))
        .collect();
    let mut asked = BTreeSet::new();
    let path = format!("/data/buckets/{bucket}/usage?held=true");
    for range in &snapshot.config.ranges {
        if ranges.contains_key(&range.range_id) {
            continue;
        }
        let holders = snapshot.record_holders(range).into_iter();
        let holder = holders
            .filter(|member| member.member_id != binding.member_id)
            .find(|member| !asked.contains(&member.node_id));
        let Some(holder) = holder.filter(|_| asked.len() < MAX_HOLDERS_ASKED) else {
            continue;
        };
        asked.insert(holder.node_id);
        let forward = RestForward {
            set_id: binding.set_id,
            range_id: range.range_id,
            node_id: holder.node_id,
        };
        // A holder that does not answer leaves its ranges uncounted and the answer incomplete.
        let answer: Option<BucketUsageResponse> = relay_get(state, &forward, &path, bearer).await?;
        for other in answer
            .and_then(|answer| answer.held_ranges)
            .unwrap_or_default()
        {
            if let Some(other) = other.held() {
                ranges.entry(other.range_id).or_insert(other);
            }
        }
    }
    let wanted = snapshot.config.ranges.len();
    let counted: Vec<&HeldRange> = (snapshot.config.ranges.iter())
        .filter_map(|range| ranges.get(&range.range_id))
        .collect();
    let counters = sum_held(counted.iter().copied())
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(RangeUsage {
        counters,
        ranges: wanted,
        counted: counted.len(),
        complete: counted.len() == wanted && counted.iter().all(|range| range.complete),
    })
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/usage",
    tag = "data/storage",
    summary = "Read one bucket's stored inventory",
    description = r#"Reports how many objects, versions, delete markers and open multipart uploads a bucket holds on this node, and how many bytes they occupy.

**Authentication**: realm bearer token with READ on the bucket, the same right that lists its
objects.

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
  the ranges it holds and asks one holder of the others, each holder once and at most 16, with
  the caller's token. `ranges` and `ranges_counted` say how many ranges the set has and how many
  were summed; `complete` is false while a range had no answering holder or its counters are
  being rebuilt.
- `held=true` answers only the counter rows of each range this node holds, in `held_ranges`; it
  is accepted only on requests another member of the distributed bucket relays.
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Query(query): Query<UsageQuery>,
) -> ServerResult<Json<BucketUsageResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
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
    .await?;
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
            let usage = merged_usage(&state, caller, (&binding, &snapshot), ranges).await?;
            Ok(Json(BucketUsageResponse::of_ranges(bucket, usage)))
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
