//! Serves the saved coverage count of a whole bucket: read it, or start a new one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::coverage::CoverageCount;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_drivers::driver::drive;
use aruna_operations::blob::migration::count::{
    ReadCountOperation, StartCountError, StartCountOperation,
};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::coordinators::{bucket_path, coordinators};
use crate::auth::{RealmAuth, ValidatedBearer, ensure_permission};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::lookup::bucket_info;
use crate::routes::management_relay::RELAY_HOP_HEADER;
use crate::routes::storage::relay::{relay_get, relay_json, relayed_set};
use crate::server::state::ServerState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CountStatus {
    /// No count was started yet.
    Missing,
    Counting,
    Finished,
}

/// The saved count of a bucket's current versions against its rules.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CoverageCountResponse {
    pub bucket: String,
    pub status: CountStatus,
    /// Current versions counted, delete markers included.
    pub observed: u64,
    pub deleted: u64,
    /// Current versions that carry none or only some of the rules.
    pub gaps: u64,
    pub registered: u64,
    pub quarantined: u64,
    pub absent: u64,
    pub reference_only: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Set once every node that answered finished its count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// The bucket rules changed after the count compared against them.
    pub rules_changed: bool,
    /// Nodes that write part of a distributed bucket and did not answer; their objects are missing.
    #[serde(default)]
    pub unreached: Vec<String>,
}

impl CoverageCountResponse {
    fn of(bucket: &str, count: Option<CoverageCount>, generation: u64) -> Self {
        let count_or = |read: fn(&CoverageCount) -> u64| count.as_ref().map_or(0, read);
        let status = match &count {
            None => CountStatus::Missing,
            Some(count) if count.finished_at_ms.is_none() => CountStatus::Counting,
            Some(_) => CountStatus::Finished,
        };
        Self {
            bucket: bucket.to_string(),
            status,
            observed: count_or(|count| count.observed),
            deleted: count_or(|count| count.deleted),
            gaps: count_or(|count| count.gaps),
            registered: count_or(|count| count.registered),
            quarantined: count_or(|count| count.quarantined),
            absent: count_or(|count| count.absent),
            reference_only: count_or(|count| count.reference_only),
            started_at_ms: count.as_ref().map(|count| count.started_at_ms),
            finished_at_ms: count.as_ref().and_then(|count| count.finished_at_ms),
            rules_changed: status == CountStatus::Finished
                && count.is_some_and(|count| count.generation != generation),
            unreached: Vec::new(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/placement/coverage/count",
    tag = "data/placement",
    summary = "Read the saved coverage count of a bucket",
    description = r#"Returns the saved count of how many current versions of the bucket carry its placement rules.

**Authentication**: realm bearer token with READ on the realm-configuration path.

**Behavior**
- A count walks every current version once in the background and is saved; reading it walks
  nothing. `status` is `missing` before the first count and `counting` while one runs, with the
  numbers counted so far.
- Delete markers count in `observed` and `deleted` and carry no rules. `gaps` counts current
  versions that carry none or only some of the rules; the copy counts are separate answers.
- `rules_changed` is set when the bucket rules changed after a finished count. Writes after the
  count are not in it; start a new count to include them.
- In a distributed bucket every coordinator counts the ranges it writes. This node asks each with
  the caller's bearer token and adds their counts; `unreached` names those that did not answer.
  When none answers, the call fails with 503."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    responses(
        (status = 200, description = "The saved count", body = CoverageCountResponse, example = json!({
            "bucket": "datasets",
            "status": "finished",
            "observed": 45000000,
            "deleted": 12,
            "gaps": 0,
            "registered": 0,
            "quarantined": 0,
            "absent": 0,
            "reference_only": 44999988,
            "started_at_ms": 1763370000000u64,
            "finished_at_ms": 1763373600000u64,
            "rules_changed": false,
            "unreached": []
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the realm-configuration path", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse),
        (status = 503, description = "A distributed bucket: no coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_coverage_count(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Json<CoverageCountResponse>> {
    count_route(&state, auth, bearer.as_ref(), &bucket, &headers, false)
        .await
        .map(Json)
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/placement/coverage/count",
    tag = "data/placement",
    summary = "Start a new coverage count of a bucket",
    description = r#"Replaces the saved coverage count of the bucket with a new one that runs in the background.

**Authentication**: realm bearer token with READ on the realm-configuration path, as for reading it.

**Behavior**
- The answer is the new count, with `status` `counting`. Read the count again to follow it.
- A count that still runs is dropped and starts over.
- In a distributed bucket this node starts the count on every coordinator with the caller's bearer
  token; `unreached` names those that did not answer."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    responses(
        (status = 202, description = "The count started", body = CoverageCountResponse, example = json!({
            "bucket": "datasets",
            "status": "counting",
            "observed": 0,
            "deleted": 0,
            "gaps": 0,
            "registered": 0,
            "quarantined": 0,
            "absent": 0,
            "reference_only": 0,
            "started_at_ms": 1763370000000u64,
            "rules_changed": false,
            "unreached": []
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the realm-configuration path", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse),
        (status = 503, description = "A distributed bucket: no coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_coverage_count(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> ServerResult<(StatusCode, Json<CoverageCountResponse>)> {
    let answer = count_route(&state, auth, bearer.as_ref(), &bucket, &headers, true).await?;
    Ok((StatusCode::ACCEPTED, Json(answer)))
}

/// Both routes: check the caller, then answer for this node or for every coordinator.
async fn count_route(
    state: &ServerState,
    auth: AuthContext,
    bearer: Option<&ValidatedBearer>,
    bucket: &str,
    headers: &HeaderMap,
    start: bool,
) -> ServerResult<CoverageCountResponse> {
    // Checked before the bucket is read, so a refused caller cannot tell which buckets exist.
    ensure_permission(
        state,
        &auth,
        policy_admin_path(auth.realm_id),
        Permission::READ,
    )
    .await?;
    let info = bucket_info(state, bucket).await?;
    relayed_set(&info, headers)?;
    let relayed = headers.contains_key(RELAY_HOP_HEADER);
    match info.distribution.as_ref().filter(|_| !relayed) {
        Some(binding) => count_spread(state, bucket, binding, bearer, start).await,
        None => count_here(state, bucket, start).await,
    }
}

/// This node's saved count, after starting a new one when asked.
async fn count_here(
    state: &ServerState,
    bucket: &str,
    start: bool,
) -> ServerResult<CoverageCountResponse> {
    let context = state.get_ctx();
    if start {
        let operation = StartCountOperation::new(bucket.to_string());
        drive(operation, &context)
            .await
            .map_err(|error| match error {
                StartCountError::NoSuchBucket => ServerError::NotFound,
                other => ServerError::InternalError(other.into()),
            })?;
    }
    let count = drive(ReadCountOperation::new(bucket), &context).await;
    let count = count.map_err(|error| ServerError::InternalError(error.into()))?;
    let info = bucket_info(state, bucket).await?;
    Ok(CoverageCountResponse::of(
        bucket,
        count,
        info.placement_policy_generation,
    ))
}

/// Asks every coordinator, this node directly, and adds up their counts.
async fn count_spread(
    state: &ServerState,
    bucket: &str,
    binding: &DistributionBinding,
    bearer: Option<&ValidatedBearer>,
    start: bool,
) -> ServerResult<CoverageCountResponse> {
    let path = bucket_path(bucket, "coverage/count");
    let (mut answers, mut unreached) = (Vec::new(), Vec::new());
    for forward in coordinators(state, binding).await? {
        let answer = if forward.node_id == state.get_node_id() {
            count_here(state, bucket, start).await
        } else if start {
            relay_json(state, &forward, &path, bearer, &serde_json::json!({}))
                .await
                .map(|(_, answer)| answer)
        } else {
            match relay_get(state, &forward, &path, bearer, HeaderMap::new()).await {
                Ok(Some(answer)) => Ok(answer),
                Ok(None) => Err(ServerError::ServiceUnavailable),
                Err(error) => Err(error),
            }
        };
        match answer {
            Ok(answer) => answers.push(answer),
            Err(error) => {
                tracing::warn!(node = %forward.node_id, %error, "A coordinator did not count");
                unreached.push(forward.node_id.to_string());
            }
        }
    }
    join_counts(bucket, answers, unreached)
}

/// One count for the whole bucket: numbers add up and the least advanced status wins.
fn join_counts(
    bucket: &str,
    answers: Vec<CoverageCountResponse>,
    unreached: Vec<String>,
) -> ServerResult<CoverageCountResponse> {
    if answers.is_empty() {
        return Err(ServerError::ServiceUnavailableReason(
            "no coordinator of the bucket answered".to_string(),
        ));
    }
    let sum = |read: fn(&CoverageCountResponse) -> u64| answers.iter().map(read).sum::<u64>();
    let has = |status| answers.iter().any(|answer| answer.status == status);
    let status = match (has(CountStatus::Missing), has(CountStatus::Counting)) {
        (true, _) => CountStatus::Missing,
        (false, true) => CountStatus::Counting,
        (false, false) => CountStatus::Finished,
    };
    Ok(CoverageCountResponse {
        bucket: bucket.to_string(),
        status,
        observed: sum(|answer| answer.observed),
        deleted: sum(|answer| answer.deleted),
        gaps: sum(|answer| answer.gaps),
        registered: sum(|answer| answer.registered),
        quarantined: sum(|answer| answer.quarantined),
        absent: sum(|answer| answer.absent),
        reference_only: sum(|answer| answer.reference_only),
        started_at_ms: answers
            .iter()
            .filter_map(|answer| answer.started_at_ms)
            .min(),
        finished_at_ms: (status == CountStatus::Finished)
            .then(|| {
                answers
                    .iter()
                    .filter_map(|answer| answer.finished_at_ms)
                    .max()
            })
            .flatten(),
        rules_changed: answers.iter().any(|answer| answer.rules_changed),
        unreached,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counted(observed: u64, finished_at_ms: Option<u64>) -> CoverageCount {
        CoverageCount {
            generation: 3,
            observed,
            gaps: 1,
            finished_at_ms,
            ..CoverageCount::new(10)
        }
    }

    #[test]
    fn reports_changed_rules() {
        let finished = CoverageCountResponse::of("photos", Some(counted(5, Some(20))), 4);
        let running = CoverageCountResponse::of("photos", Some(counted(5, None)), 4);
        let missing = CoverageCountResponse::of("photos", None, 4);

        assert!(finished.rules_changed);
        assert!(!running.rules_changed);
        assert_eq!(running.status, CountStatus::Counting);
        assert_eq!(
            (missing.status, missing.started_at_ms),
            (CountStatus::Missing, None)
        );
    }

    #[test]
    fn joins_node_counts() {
        let done = CoverageCountResponse::of("photos", Some(counted(5, Some(20))), 3);
        let running = CoverageCountResponse::of("photos", Some(counted(7, None)), 3);
        let unreached = vec!["node-b".to_string()];

        let joined =
            join_counts("photos", vec![done.clone(), running], unreached.clone()).expect("joined");
        assert_eq!((joined.observed, joined.gaps), (12, 2));
        assert_eq!(joined.status, CountStatus::Counting);
        assert_eq!(joined.finished_at_ms, None);
        assert_eq!(joined.unreached, unreached);

        let finished = join_counts("photos", vec![done.clone(), done], Vec::new()).expect("joined");
        assert_eq!(finished.finished_at_ms, Some(20));
        assert!(join_counts("photos", Vec::new(), Vec::new()).is_err());
    }
}
