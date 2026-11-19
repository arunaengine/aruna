//! Runs a data tool on the realm node that holds its bucket, as the caller with the caller's
//! own bearer token; that node checks the token and every permission again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::compute::request_bearer;
use super::data::map_bucket_error;
use super::{McpServer, explained, internal_error, server_error};
use crate::error::ServerError;
use crate::lookup::map_guard_error;
use crate::routes::storage::relay::node_directed;
use aruna_core::NodeId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::distribution::guard::GuardError;
use aruna_operations::driver::drive;
use aruna_operations::relay::tool::{RouteError, ToolCall, ToolCallOperation, ToolRefusal};
use aruna_operations::relay::tool_route::{Missed, ToolNodeOperation, ToolRoute, ToolRouteError};
use rmcp::model::{CallToolResult, ContentBlock};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::str::FromStr;

impl McpServer {
    /// Where a data tool call on `bucket` and `key` asks to run.
    fn tool_route(
        &self,
        (parts, auth): (&http::request::Parts, &AuthContext),
        bucket: &str,
        named: Option<NodeId>,
        key: &str,
    ) -> ToolRoute {
        ToolRoute {
            realm_id: self.state.get_realm_id(),
            here: self.state.get_node_id(),
            auth: auth.clone(),
            bearer: request_bearer(parts).map(|carrier| carrier.as_str().to_string()),
            bucket: bucket.to_string(),
            named,
            key: key.to_string(),
        }
    }

    /// The node a data tool call on `bucket` and `key` runs on, `None` for this node.
    pub(crate) async fn bucket_node(
        &self,
        call: (&http::request::Parts, &AuthContext),
        bucket: &str,
        named: Option<&str>,
        key: &str,
    ) -> Result<Option<NodeId>, CallToolResult> {
        // A call another node forwarded runs here; one meant for another node is refused.
        if node_directed(&self.state, &call.0.headers).map_err(server_error)? {
            return Ok(None);
        }
        let named = named.map(parse_node).transpose()?;
        let route = ToolNodeOperation::new(self.tool_route(call, bucket, named, key));
        (drive(route, &self.state.get_ctx()).await).map_err(|error| route_failure(error, bucket))
    }

    /// Runs `tool` with `input` on `node_id` as the caller and decodes its answer; the call is
    /// directed at that node, which answers itself and never forwards again.
    pub(crate) async fn forward<T: DeserializeOwned>(
        &self,
        parts: &http::request::Parts,
        node_id: NodeId,
        tool: &str,
        input: &impl Serialize,
    ) -> Result<T, CallToolResult> {
        let bearer =
            request_bearer(parts).ok_or_else(|| server_error(ServerError::Unauthorized))?;
        let call = ToolCall {
            realm_id: self.state.get_realm_id(),
            node_id,
            bearer: SharedSecret::new(SecretBytes::new(bearer.as_str().as_bytes().to_vec())),
            tool: tool.to_string(),
            arguments: serde_json::to_value(input).map_err(internal_error)?,
        };
        let call = ToolCallOperation::new(call);
        let body = (drive(call, &self.state.get_ctx()).await)
            .map_err(|error| tool_refusal(error, node_id))?;
        answer(node_id, &body)
    }
}

/// A forwarded call that brought no reply, as this node's answer naming that node.
fn tool_refusal(error: ToolRefusal, node_id: NodeId) -> CallToolResult {
    let silent = |why: String| {
        server_error(ServerError::ServiceUnavailableReason(format!(
            "node {node_id} holds the bucket but {why}; retry later"
        )))
    };
    let refused =
        |error: ServerError| explained(error, format!("node {node_id} refused the forwarded call"));
    match error {
        ToolRefusal::NotServer => explained(
            ServerError::BadRequest,
            format!(
                "node {node_id} is not a server node of this realm; pass a node_id from \
                 list_buckets or omit it"
            ),
        ),
        ToolRefusal::NoAddress => silent("publishes no api address".to_string()),
        ToolRefusal::Silent(why) => silent(format!("did not answer: {why}")),
        ToolRefusal::Late => silent("did not answer in time".to_string()),
        ToolRefusal::Unauthorized => refused(ServerError::Unauthorized),
        ToolRefusal::Forbidden => refused(ServerError::Forbidden),
        ToolRefusal::Status(status) => server_error(ServerError::BadGatewayReason(format!(
            "node {node_id} answered the forwarded call with HTTP {status}"
        ))),
        ToolRefusal::Failed(reason) => internal_error(reason),
        ToolRefusal::Io(error) => internal_error(error),
    }
}

/// The tool answer in a node's JSON-RPC reply; a tool error keeps its code and names the node.
fn answer<T: DeserializeOwned>(node_id: NodeId, body: &[u8]) -> Result<T, CallToolResult> {
    let broken = |why: String| {
        server_error(ServerError::BadGatewayReason(format!(
            "node {node_id} answered the forwarded call {why}"
        )))
    };
    let mut reply: Value =
        serde_json::from_slice(body).map_err(|error| broken(format!("with bad JSON: {error}")))?;
    if let Some(error) = reply.get("error") {
        return Err(broken(format!("with an error: {error}")));
    }
    let result: CallToolResult = serde_json::from_value(reply["result"].take())
        .map_err(|error| broken(format!("with no tool result: {error}")))?;
    if result.is_error == Some(true) {
        return Err(named_error(result, node_id));
    }
    let output = result
        .structured_content
        .ok_or_else(|| broken("without structured content".to_string()))?;
    serde_json::from_value(output).map_err(|error| broken(format!("in another shape: {error}")))
}

fn named_error(mut result: CallToolResult, node_id: NodeId) -> CallToolResult {
    let text = result
        .structured_content
        .as_mut()
        .and_then(|body| body.get_mut("error"))
        .and_then(|error| match error {
            Value::String(text) => {
                *text = format!("{text} (answered by node {node_id})");
                Some(text.clone())
            }
            _ => None,
        });
    if let Some(text) = text {
        result.content = vec![ContentBlock::text(text)];
    }
    result
}

/// Why a tool call has no node, as this node's answer.
fn route_failure(error: ToolRouteError, bucket: &str) -> CallToolResult {
    match error {
        ToolRouteError::Bucket(error) => map_bucket_error(error),
        ToolRouteError::Guard(error) => guard_error(error),
        ToolRouteError::Search(error) => server_error(crate::metadata::map_api_error(error)),
        ToolRouteError::SearchLate => explained(
            ServerError::ServiceUnavailable,
            format!(
                "the realm search for bucket {bucket} did not finish in time; retry, or pass \
                 node_id from list_buckets"
            ),
        ),
        ToolRouteError::Route(error, missed) => route_error(error, bucket, &missed),
        ToolRouteError::Io(error) => internal_error(error),
    }
}

fn guard_error(error: GuardError) -> CallToolResult {
    server_error(map_guard_error(error))
}

fn parse_node(value: &str) -> Result<NodeId, CallToolResult> {
    NodeId::from_str(value).map_err(|_| {
        super::bad_request(
            "node_id must be a node id as list_buckets reports it, or omitted to use the node \
             that holds the bucket",
        )
    })
}

fn route_error(error: RouteError, bucket: &str, missed: &Missed) -> CallToolResult {
    match error {
        RouteError::Missing if missed.incomplete => {
            let nodes = missed
                .nodes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let silent = match nodes.is_empty() {
                true => String::new(),
                false => format!(" (no answer from nodes {})", nodes.join(", ")),
            };
            explained(
                ServerError::ServiceUnavailable,
                format!(
                    "no answering node holds a readable bucket named {bucket}, but the realm \
                     search was incomplete{silent}; retry, or pass node_id from list_buckets"
                ),
            )
        }
        RouteError::Missing => explained(
            ServerError::NotFound,
            format!(
                "no node of the realm holds a bucket named {bucket} that the caller may read; \
                 call list_buckets for readable names, and pass the bare bucket name without an \
                 s3:// prefix or a key"
            ),
        ),
        RouteError::Ambiguous(nodes) => {
            let nodes = nodes.iter().map(ToString::to_string).collect::<Vec<_>>();
            explained(
                ServerError::BadRequest,
                format!(
                    "buckets named {bucket} exist on nodes {}; pass node_id from list_buckets \
                     to pick one",
                    nodes.join(", ")
                ),
            )
        }
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
