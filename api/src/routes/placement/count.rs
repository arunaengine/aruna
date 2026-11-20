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
    MintRunOperation, ReadCountOperation, StartCountError, StartCountOperation, current_run,
    other_runs,
};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

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
    /// The start this count belongs to; a distributed start gives every coordinator the same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub run: Option<Ulid>,
    /// The bucket rules changed after the count compared against them.
    pub rules_changed: bool,
    /// Nodes that write part of a distributed bucket and did not answer, or hold a count of another
    /// run; their objects are missing.
    #[serde(default)]
    pub unreached: Vec<String>,
}

/// Starts a count that joins a run; only a node relaying a distributed start names the run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct StartCountRequest {
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    pub run: Option<Ulid>,
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct CountQuery {
    /// The run a start answered with; omitted, the newest run any coordinator holds.
    #[serde(default)]
    #[param(value_type = Option<String>)]
    pub run: Option<Ulid>,
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
            run: count.as_ref().and_then(|count| count.run),
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
- With `run`, only a count of that run answers; a count of another run reads as `missing`.
- In a distributed bucket every coordinator counts the ranges it writes. This node asks each with
  the caller's bearer token and adds their counts of the asked `run`, else of the newest run any
  of them holds. `unreached` names those that did not answer or hold no count of that run. When
  none answers, the call fails with 503."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used by the S3 surface"),
        CountQuery
    ),
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
            "run": "01JCNCTR0123456789ABCDEFGH",
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
    Query(query): Query<CountQuery>,
    headers: HeaderMap,
) -> ServerResult<Json<CoverageCountResponse>> {
    count_route(
        &state,
        auth,
        bearer.as_ref(),
        &bucket,
        &headers,
        false,
        query.run,
    )
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
- The answer is the new count, with `status` `counting` and its `run`. Read the count with that
  `run` to follow it.
- A count that still runs is dropped and starts over.
- In a distributed bucket this node mints one `run` and starts the count of it on every coordinator
  with the caller's bearer token; `unreached` names those that did not answer.
- Only a node relaying a distributed start names the `run` in the body; any other caller that
  names one gets 400."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(
        content = StartCountRequest,
        description = "The run a relayed start joins",
        example = json!({"run": "01JCNCTR0123456789ABCDEFGH"})
    ),
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
            "run": "01JCNCTR0123456789ABCDEFGH",
            "rules_changed": false,
            "unreached": []
        })),
        (status = 400, description = "Only a relaying node names the run", body = ErrorResponse),
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
    body: Option<Json<StartCountRequest>>,
) -> ServerResult<(StatusCode, Json<CoverageCountResponse>)> {
    let run = body.and_then(|Json(request)| request.run);
    let answer = count_route(&state, auth, bearer.as_ref(), &bucket, &headers, true, run).await?;
    Ok((StatusCode::ACCEPTED, Json(answer)))
}

/// Both routes: check the caller, then answer for this node or for every coordinator. A start
/// joins `run`; a read reports it.
async fn count_route(
    state: &ServerState,
    auth: AuthContext,
    bearer: Option<&ValidatedBearer>,
    bucket: &str,
    headers: &HeaderMap,
    start: bool,
    run: Option<Ulid>,
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
    // Nodes mint every run, so the newest run id is the latest start.
    if start && run.is_some() && !relayed {
        return Err(ServerError::BadRequestReason(
            "only a node relaying a start names its run".to_string(),
        ));
    }
    match info.distribution.as_ref().filter(|_| !relayed) {
        Some(binding) => count_spread(state, bucket, binding, bearer, start, run).await,
        None => count_here(state, bucket, start, run).await,
    }
}

/// This node's saved count of `run`, or the count a start of it wrote, which a later start
/// cannot replace in the answer.
async fn count_here(
    state: &ServerState,
    bucket: &str,
    start: bool,
    run: Option<Ulid>,
) -> ServerResult<CoverageCountResponse> {
    let context = state.get_ctx();
    let count = match start {
        true => {
            let operation = StartCountOperation::new(bucket.to_string(), run);
            let started = drive(operation, &context).await;
            Some(started.map_err(|error| match error {
                StartCountError::NoSuchBucket => ServerError::NotFound,
                other => ServerError::InternalError(other.into()),
            })?)
        }
        false => {
            let count = drive(ReadCountOperation::new(bucket, run), &context).await;
            count.map_err(|error| ServerError::InternalError(error.into()))?
        }
    };
    let info = bucket_info(state, bucket).await?;
    Ok(CoverageCountResponse::of(
        bucket,
        count,
        info.placement_policy_generation,
    ))
}

/// Asks every coordinator, this node directly, and adds up their counts of `run`. A start mints
/// the run before the first coordinator starts, so all of them start the same.
async fn count_spread(
    state: &ServerState,
    bucket: &str,
    binding: &DistributionBinding,
    bearer: Option<&ValidatedBearer>,
    start: bool,
    run: Option<Ulid>,
) -> ServerResult<CoverageCountResponse> {
    let run = match start {
        true => {
            let minted = drive(MintRunOperation::default(), &state.get_ctx()).await;
            Some(minted.map_err(|error| ServerError::InternalError(error.into()))?)
        }
        false => run,
    };
    let path = bucket_path(bucket, "coverage/count");
    let (mut answers, mut unreached) = (Vec::new(), Vec::new());
    for forward in coordinators(state, binding).await? {
        let answer = if forward.node_id == state.get_node_id() {
            count_here(state, bucket, start, run).await
        } else if start {
            relay_json(state, &forward, &path, bearer, &StartCountRequest { run })
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
            Ok(answer) => answers.push((forward.node_id.to_string(), answer)),
            Err(error) => {
                tracing::warn!(node = %forward.node_id, %error, "A coordinator did not count");
                unreached.push(forward.node_id.to_string());
            }
        }
    }
    join_counts(bucket, answers, unreached, run)
}

/// One count for the whole bucket of the `wanted` run, else of the newest: numbers add up and the
/// least advanced status wins. Nodes without a count of that run count as unreached.
fn join_counts(
    bucket: &str,
    mut answers: Vec<(String, CoverageCountResponse)>,
    mut unreached: Vec<String>,
    wanted: Option<Ulid>,
) -> ServerResult<CoverageCountResponse> {
    if answers.is_empty() {
        return Err(ServerError::ServiceUnavailableReason(
            "no coordinator of the bucket answered".to_string(),
        ));
    }
    let runs: Vec<_> = answers.iter().map(|(_, answer)| answer.run).collect();
    let run = current_run(wanted, &runs);
    for index in other_runs(run, &runs).into_iter().rev() {
        let (node, _) = answers.remove(index);
        unreached.push(node);
    }
    let answers: Vec<_> = answers.into_iter().map(|(_, answer)| answer).collect();
    let sum = |read: fn(&CoverageCountResponse) -> u64| answers.iter().map(read).sum::<u64>();
    let has = |status| answers.iter().any(|answer| answer.status == status);
    let missing = answers.is_empty() || has(CountStatus::Missing);
    let status = match (missing, has(CountStatus::Counting)) {
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
        run,
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
            run: Some(Ulid::from_parts(10, 1)),
            ..CoverageCount::new(10)
        }
    }

    fn answer(node: &str, count: CoverageCount) -> (String, CoverageCountResponse) {
        let answer = CoverageCountResponse::of("photos", Some(count), 3);
        (node.to_string(), answer)
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
        let done = answer("node-a", counted(5, Some(20)));
        let running = answer("node-c", counted(7, None));
        let unreached = vec!["node-b".to_string()];

        let answers = vec![done.clone(), running];
        let joined = join_counts("photos", answers, unreached.clone(), None).expect("joined");
        assert_eq!((joined.observed, joined.gaps), (12, 2));
        assert_eq!(joined.status, CountStatus::Counting);
        assert_eq!(joined.finished_at_ms, None);
        assert_eq!(joined.unreached, unreached);

        let answers = vec![done.clone(), done];
        let finished = join_counts("photos", answers, Vec::new(), None).expect("joined");
        assert_eq!(finished.finished_at_ms, Some(20));
        assert!(join_counts("photos", Vec::new(), Vec::new(), None).is_err());
    }

    // A coordinator a start missed keeps its finished previous run; it must not pass for this one.
    #[test]
    fn drops_other_runs() {
        let previous = Ulid::from_parts(5, 1);
        let old = CoverageCount {
            run: Some(previous),
            ..counted(4, Some(9))
        };
        let answers = vec![
            answer("node-a", counted(7, Some(20))),
            answer("node-b", old),
        ];

        let joined = join_counts("photos", answers.clone(), Vec::new(), None).expect("joined");
        assert_eq!((joined.status, joined.observed), (CountStatus::Finished, 7));
        assert_eq!(joined.run, Some(Ulid::from_parts(10, 1)));
        assert_eq!(joined.unreached, ["node-b"]);

        let joined = join_counts("photos", answers, Vec::new(), Some(previous)).expect("joined");
        assert_eq!(
            (joined.observed, joined.unreached),
            (4, vec!["node-a".to_string()])
        );
    }
    // Once a run is known, a coordinator without a count of it missed the start; it is unreached.
    #[test]
    fn missing_count_unreached() {
        let missing = CoverageCountResponse::of("photos", None, 3);
        let answers = vec![
            answer("node-a", counted(7, None)),
            ("node-b".to_string(), missing),
        ];

        let joined = join_counts("photos", answers.clone(), Vec::new(), None).expect("joined");
        assert_eq!((joined.status, joined.observed), (CountStatus::Counting, 7));
        assert_eq!(joined.unreached, ["node-b"]);

        // A run no coordinator holds reads as missing, with every coordinator unreached.
        let asked = Some(Ulid::from_parts(4, 4));
        let joined = join_counts("photos", answers, Vec::new(), asked).expect("joined");
        assert_eq!((joined.status, joined.run), (CountStatus::Missing, asked));
        assert_eq!(joined.unreached, ["node-b", "node-a"]);
    }
}
