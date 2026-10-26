//! Runs a bucket default pass on every coordinator of a distributed bucket and joins the answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::guard::bound_view;
use serde::{Deserialize, Serialize};

use super::{BulkRunRequest, BulkRunResponse};
use crate::auth::ValidatedBearer;
use crate::error::{ServerError, ServerResult};
use crate::routes::storage::distributed::map_guard_error;
use crate::routes::storage::relay::{RestForward, relay_json};
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
    let mut cursor = SpreadCursor::decode(request.cursor.as_deref())?;
    let nodes = coordinators(state, binding).await?;
    let current: BTreeSet<String> = nodes
        .iter()
        .map(|forward| forward.node_id.to_string())
        .collect();
    // A node that coordinates nothing anymore is dropped; its keys belong to a current one.
    cursor.next.retain(|node, _| current.contains(node));
    cursor.done.retain(|node| current.contains(node));
    let here: NodeId = state.get_node_id();
    let mut local = Some(local);
    let bucket: String = url::form_urlencoded::byte_serialize(bucket.as_bytes()).collect();
    let path = format!("/data/buckets/{bucket}/placement/runs");
    let (mut answers, mut unreached) = (Vec::new(), Vec::new());
    for forward in nodes {
        let node = forward.node_id.to_string();
        if cursor.done.contains(&node) {
            continue;
        }
        let asked = BulkRunRequest {
            operation_id: request.operation_id.clone(),
            limit: request.limit,
            cursor: cursor.next.get(&node).cloned(),
        };
        let answer = match (forward.node_id == here, local.take()) {
            (true, Some(run)) => run(asked.cursor.clone()).await,
            _ => relay_json::<BulkRunResponse>(state, &forward, &path, bearer, &asked)
                .await
                .map(|(_, answer)| answer),
        };
        match answer {
            Ok(answer) => {
                match answer.cursor.clone() {
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
    // No pending coordinator answered: the same cursor again would not move the run.
    if answers.is_empty()
        && let Some((node, error)) = unreached.first()
    {
        return Err(ServerError::ServiceUnavailableReason(format!(
            "coordinator {node} did not run the pass: {error}"
        )));
    }
    join(
        request,
        answers,
        cursor,
        unreached.into_iter().map(|(node, _)| node).collect(),
    )
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
