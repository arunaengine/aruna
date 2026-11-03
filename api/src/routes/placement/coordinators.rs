//! Asks every coordinator of a distributed bucket for a placement pass or a coverage page and
//! joins their answers into one for the whole bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use axum::http::HeaderMap;
use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::guard::bound_view;
use serde::{Deserialize, Serialize};

use super::{BulkRunRequest, BulkRunResponse, CoverageQuery, CoverageResponse};
use crate::auth::ValidatedBearer;
use crate::error::{ServerError, ServerResult};
use crate::routes::storage::distributed::map_guard_error;
use crate::routes::storage::relay::{RestForward, relay_get, relay_json};
use crate::server::state::ServerState;

/// Where each coordinator's own pass stands; a coordinator in neither field starts from the start.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct SpreadCursor {
    /// Node id to the cursor of its own pass.
    next: BTreeMap<String, String>,
    /// Nodes whose pass reached the end of their heads.
    done: BTreeSet<String>,
}

impl SpreadCursor {
    pub(super) fn decode(cursor: Option<&str>) -> ServerResult<Self> {
        let Some(cursor) = cursor else {
            return Ok(Self::default());
        };
        let bytes = hex::decode(cursor).map_err(|_| ServerError::BadRequest)?;
        serde_json::from_slice(&bytes).map_err(|_| ServerError::BadRequest)
    }

    fn encode(&self) -> ServerResult<String> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| ServerError::InternalError(error.to_string()))?;
        Ok(hex::encode(bytes))
    }
}

/// The nodes that coordinate a range of the set, from this node's record, each with one range.
async fn coordinators(
    state: &ServerState,
    binding: &DistributionBinding,
) -> ServerResult<Vec<RestForward>> {
    let view = bound_view(state.get_ctx().as_ref(), binding)
        .await
        .map_err(map_guard_error)?;
    let snapshot = view.snapshot();
    let mut nodes: Vec<RestForward> = Vec::new();
    for range in &snapshot.config.ranges {
        let Some(member) = snapshot.member(range.coordinator) else {
            continue;
        };
        if !nodes
            .iter()
            .any(|forward| forward.node_id == member.node_id)
        {
            nodes.push(RestForward {
                set_id: binding.set_id,
                range_id: range.range_id,
                node_id: member.node_id,
            });
        }
    }
    Ok(nodes)
}

/// A coordinator's answer, as far as the spread needs it: where its own pass goes on.
trait Paged {
    fn next(&self) -> Option<String>;
}

impl Paged for BulkRunResponse {
    fn next(&self) -> Option<String> {
        self.cursor.clone()
    }
}

impl Paged for CoverageResponse {
    fn next(&self) -> Option<String> {
        self.cursor.clone()
    }
}

/// The coordinators one call asks, each with its own position, and the cursor they move on.
struct Plan {
    cursor: SpreadCursor,
    asks: Vec<(RestForward, Option<String>)>,
}

/// What one call collected: the answers, the cursor after them, and who did not answer.
struct Spread<T> {
    answers: Vec<T>,
    cursor: SpreadCursor,
    unreached: Vec<String>,
}

/// Every coordinator whose pass or scan has not ended, from this node's record of the set.
async fn plan(
    state: &ServerState,
    binding: &DistributionBinding,
    cursor: Option<&str>,
) -> ServerResult<Plan> {
    let mut cursor = SpreadCursor::decode(cursor)?;
    let nodes = coordinators(state, binding).await?;
    let current: BTreeSet<String> = nodes
        .iter()
        .map(|forward| forward.node_id.to_string())
        .collect();
    // A node that coordinates nothing anymore is dropped; its keys belong to a current one.
    cursor.next.retain(|node, _| current.contains(node));
    cursor.done.retain(|node| current.contains(node));
    let asks = (nodes.into_iter())
        .filter(|forward| !cursor.done.contains(&forward.node_id.to_string()))
        .map(|forward| {
            let position = cursor.next.get(&forward.node_id.to_string()).cloned();
            (forward, position)
        })
        .collect();
    Ok(Plan { cursor, asks })
}

/// Moves the cursor by the answers; fails when no coordinator answered, since the same cursor
/// again would not move the call on.
fn collect<T: Paged>(
    mut cursor: SpreadCursor,
    results: Vec<(NodeId, ServerResult<T>)>,
) -> ServerResult<Spread<T>> {
    let (mut answers, mut unreached) = (Vec::new(), Vec::new());
    for (node, result) in results {
        let node = node.to_string();
        match result {
            Ok(answer) => {
                match answer.next() {
                    Some(next) => {
                        cursor.next.insert(node, next);
                    }
                    None => {
                        cursor.next.remove(&node);
                        cursor.done.insert(node);
                    }
                }
                answers.push(answer);
            }
            Err(error) => unreached.push((node, error)),
        }
    }
    if answers.is_empty()
        && let Some((node, error)) = unreached.first()
    {
        return Err(ServerError::ServiceUnavailableReason(format!(
            "coordinator {node} did not answer: {error}"
        )));
    }
    let unreached = unreached.into_iter().map(|(node, _)| node).collect();
    Ok(Spread {
        answers,
        cursor,
        unreached,
    })
}

fn bucket_path(bucket: &str, route: &str) -> String {
    let bucket: String = url::form_urlencoded::byte_serialize(bucket.as_bytes()).collect();
    format!("/data/buckets/{bucket}/placement/{route}")
}

/// Runs one pass on every coordinator whose pass has not ended, this node through `local` and the
/// others with the caller's bearer token, where each checks the caller again on its own path.
pub(super) async fn run_spread(
    state: &ServerState,
    bucket: &str,
    binding: &DistributionBinding,
    bearer: Option<&ValidatedBearer>,
    request: &BulkRunRequest,
    local: impl AsyncFnOnce(Option<String>) -> ServerResult<BulkRunResponse>,
) -> ServerResult<BulkRunResponse> {
    let plan = plan(state, binding, request.cursor.as_deref()).await?;
    let path = bucket_path(bucket, "runs");
    let mut local = Some(local);
    let mut results = Vec::with_capacity(plan.asks.len());
    for (forward, position) in plan.asks {
        let here = forward.node_id == state.get_node_id();
        let answer = match local.take_if(|_| here) {
            Some(run) => run(position).await,
            None => {
                let asked = BulkRunRequest {
                    operation_id: request.operation_id.clone(),
                    limit: request.limit,
                    cursor: position,
                };
                relay_json::<BulkRunResponse>(state, &forward, &path, bearer, &asked)
                    .await
                    .map(|(_, answer)| answer)
            }
        };
        results.push((forward.node_id, answer));
    }
    let spread = collect(plan.cursor, results)?;
    join(request, spread.answers, spread.cursor, spread.unreached)
}

/// The coverage route on another node with the caller's query and that node's own position.
fn coverage_path(base: &str, query: &CoverageQuery, cursor: Option<&str>) -> String {
    let mut asked = url::form_urlencoded::Serializer::new(String::new());
    if let Some(scope) = query.scope.as_deref() {
        asked.append_pair("scope", scope);
    }
    if let Some(limit) = query.limit {
        asked.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        asked.append_pair("cursor", cursor);
    }
    format!("{base}?{}", asked.finish())
}

/// Reads one coverage page from every coordinator whose scan has not ended and joins them, so
/// the report covers the whole bucket and names the coordinators that did not answer.
pub(super) async fn coverage_spread(
    state: &ServerState,
    bucket: &str,
    binding: &DistributionBinding,
    bearer: Option<&ValidatedBearer>,
    query: &CoverageQuery,
    local: impl AsyncFnOnce(Option<String>) -> ServerResult<CoverageResponse>,
) -> ServerResult<CoverageResponse> {
    let plan = plan(state, binding, query.cursor.as_deref()).await?;
    let base = bucket_path(bucket, "coverage");
    let mut local = Some(local);
    let mut results = Vec::with_capacity(plan.asks.len());
    for (forward, position) in plan.asks {
        let here = forward.node_id == state.get_node_id();
        let answer = match local.take_if(|_| here) {
            Some(read) => read(position).await,
            None => {
                let path = coverage_path(&base, query, position.as_deref());
                match relay_get::<CoverageResponse>(
                    state,
                    &forward,
                    &path,
                    bearer,
                    HeaderMap::new(),
                )
                .await
                {
                    Ok(Some(answer)) => Ok(answer),
                    Ok(None) => Err(ServerError::ServiceUnavailableReason(format!(
                        "node {} is not reachable",
                        forward.node_id
                    ))),
                    Err(error) => Err(error),
                }
            }
        };
        results.push((forward.node_id, answer));
    }
    join_coverage(bucket, collect(plan.cursor, results)?)
}

/// One coverage report for the whole bucket: counts and gaps add up, and the limits drop the
/// ones each coordinator states about covering only itself.
fn join_coverage(bucket: &str, spread: Spread<CoverageResponse>) -> ServerResult<CoverageResponse> {
    let complete = spread.cursor.next.is_empty() && spread.unreached.is_empty();
    let answers = &spread.answers;
    let mut limits: BTreeSet<String> = (answers.iter())
        .flat_map(|answer| answer.limits.iter().cloned())
        .filter(|limit| {
            !matches!(
                limit.as_str(),
                "responder_local" | "coordinated_ranges" | "bounded_page"
            )
        })
        .collect();
    if !complete {
        limits.insert("bounded_page".to_string());
    }
    let first = answers.first();
    let sum = |count: fn(&CoverageResponse) -> usize| answers.iter().map(count).sum::<usize>();
    Ok(CoverageResponse {
        bucket: bucket.to_string(),
        scope: first.map_or_else(|| "current".to_string(), |answer| answer.scope.clone()),
        generation: first.map_or(0, |answer| answer.generation),
        target_policies: first
            .map(|answer| answer.target_policies.clone())
            .unwrap_or_default(),
        observed: sum(|answer| answer.observed),
        deleted: sum(|answer| answer.deleted),
        gaps: answers
            .iter()
            .flat_map(|answer| answer.gaps.clone())
            .collect(),
        registered: sum(|answer| answer.registered),
        quarantined: sum(|answer| answer.quarantined),
        absent: sum(|answer| answer.absent),
        reference_only: sum(|answer| answer.reference_only),
        cursor: (!complete).then(|| spread.cursor.encode()).transpose()?,
        complete,
        limits: limits.into_iter().collect(),
        unreached: spread.unreached,
    })
}

/// One answer for the whole bucket: counts add up and the run is complete once every
/// coordinator's pass reached its end.
fn join(
    request: &BulkRunRequest,
    answers: Vec<BulkRunResponse>,
    cursor: SpreadCursor,
    unreached: Vec<String>,
) -> ServerResult<BulkRunResponse> {
    let complete = cursor.next.is_empty() && unreached.is_empty();
    let status = if answers.iter().any(|answer| answer.status == "superseded") {
        "superseded"
    } else if complete && answers.iter().all(|answer| answer.status == "completed") {
        "completed"
    } else {
        "active"
    };
    let first = answers.first();
    Ok(BulkRunResponse {
        operation_id: request.operation_id.clone(),
        status: status.to_string(),
        generation: first.map_or(0, |answer| answer.generation),
        target_policies: first
            .map(|answer| answer.target_policies.clone())
            .unwrap_or_default(),
        observed: answers.iter().map(|answer| answer.observed).sum(),
        covered: answers.iter().map(|answer| answer.covered).sum(),
        minted: answers.iter().map(|answer| answer.minted).sum(),
        replanned: answers.iter().map(|answer| answer.replanned).sum(),
        blocked: answers
            .iter()
            .flat_map(|answer| answer.blocked.clone())
            .collect(),
        cursor: (!complete).then(|| cursor.encode()).transpose()?,
        complete,
        unreached,
    })
}

#[cfg(test)]
#[path = "coordinators_tests.rs"]
mod tests;
