//! Runs a data tool on the realm node that holds its bucket, as the caller with the caller's
//! own bearer token; that node checks the token and every permission again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::compute::request_bearer;
use super::data::map_bucket_error;
use super::{McpServer, explained, internal_error, server_error};
use crate::error::ServerError;
use crate::routes::storage::relay::{RELAY_NODE_HEADER, capped_body, node_directed};
use aruna_core::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_operations::distribution::delegate::node_urls;
use aruna_operations::distribution::resolve::key_holders;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{BucketSearchRequest, search_buckets_distributed};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use rmcp::model::{CallToolResult, ContentBlock};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use ulid::Ulid;

const API_PREFIX: &str = "/api/v1";
const SEARCH_PAGE: usize = 50;
/// How long a forwarded call may take, its answer included; a scan may run for a while.
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default()
});

/// A realm node that holds a readable bucket of the asked name, as the bucket search finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Holder {
    pub node_id: NodeId,
    /// The distributed bucket this node's bucket is a member of.
    pub set_id: Option<Ulid>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RouteError {
    /// No node holds a bucket of the name that the caller may read.
    Missing,
    /// Several nodes hold different buckets of the name.
    Ambiguous(Vec<NodeId>),
}

/// The node a call on a bucket runs on, `None` for this node: the `named` node, else this node
/// when it holds the bucket, else the one holder; for a distributed bucket the first of
/// `members`, the members that keep the call's key, when this node's record names them.
pub(crate) fn pick_node(
    here: NodeId,
    (named, local): (Option<NodeId>, bool),
    holders: &[Holder],
    members: &[NodeId],
) -> Result<Option<NodeId>, RouteError> {
    if let Some(named) = named {
        return Ok((named != here).then_some(named));
    }
    if local {
        return Ok(None);
    }
    let Some(first) = holders.first() else {
        return Err(RouteError::Missing);
    };
    if first.set_id.is_some() && holders.iter().all(|holder| holder.set_id == first.set_id) {
        let member = members.iter().find(|member| **member != here);
        return Ok(Some(member.copied().unwrap_or(first.node_id)));
    }
    match holders {
        [one] => Ok(Some(one.node_id)),
        _ => Err(RouteError::Ambiguous(
            holders.iter().map(|holder| holder.node_id).collect(),
        )),
    }
}

/// Whether `node_id` is a server or management node in the realm's current configuration;
/// devices and nodes no longer configured never receive a forwarded call and its token.
pub(crate) fn realm_server(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let node_id = node_id.to_string();
    (config.nodes.iter()).any(|node| node.node_id == node_id && node.kind.is_sync_eligible())
}

/// The `/mcp` url on a node's published api base, which may carry the REST nest.
pub(crate) fn mcp_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    format!("{}/mcp", base.strip_suffix(API_PREFIX).unwrap_or(base))
}

impl McpServer {
    /// The node a data tool call on `bucket` and `key` runs on, `None` for this node.
    pub(crate) async fn bucket_node(
        &self,
        (parts, auth): (&http::request::Parts, &AuthContext),
        bucket: &str,
        named: Option<&str>,
        key: &str,
    ) -> Result<Option<NodeId>, CallToolResult> {
        // A call another node forwarded runs here; one meant for another node is refused.
        if node_directed(&self.state, &parts.headers).map_err(server_error)? {
            return Ok(None);
        }
        let named = named.map(parse_node).transpose()?;
        let context = self.state.get_ctx();
        let local = named.is_none()
            && match drive(GetBucketOperation::new(bucket.to_string()), &context).await {
                Ok(_) => true,
                Err(GetBucketError::NotFound) => false,
                Err(error) => return Err(map_bucket_error(error)),
            };
        let (mut holders, mut failed) = (Vec::new(), 0);
        if named.is_none() && !local {
            (holders, failed) = self.find_holders(parts, auth, bucket).await?;
        }
        let mut members = Vec::new();
        if let Some(set_id) = holders.first().and_then(|holder| holder.set_id) {
            members = key_holders(&context, set_id, key).await;
        }
        let here = self.state.get_node_id();
        pick_node(here, (named, local), &holders, &members)
            .map_err(|error| route_error(error, bucket, failed))
    }

    /// The nodes holding a readable bucket named `bucket`, each distributed bucket once, and
    /// how many nodes the search could not ask.
    async fn find_holders(
        &self,
        parts: &http::request::Parts,
        auth: &AuthContext,
        bucket: &str,
    ) -> Result<(Vec<Holder>, usize), CallToolResult> {
        let bearer = request_bearer(parts).map(|carrier| carrier.as_str().to_string());
        let (mut holders, mut cursor) = (Vec::<Holder>::new(), None);
        loop {
            let page = search_buckets_distributed(
                self.state.get_ctx().as_ref(),
                self.state.get_realm_id(),
                self.state.get_node_id(),
                BucketSearchRequest {
                    auth: auth.clone(),
                    bearer_token: bearer.clone(),
                    query: bucket.to_string(),
                    limit: SEARCH_PAGE,
                    group_id: None,
                    cursor,
                    target_nodes: None,
                    database: false,
                },
            )
            .await
            .map_err(crate::metadata::map_api_error)
            .map_err(server_error)?;
            for hit in page.hits.into_iter().filter(|hit| hit.bucket == bucket) {
                let holder = Holder {
                    node_id: hit.node_id,
                    set_id: hit.set_id,
                };
                let known = |other: &Holder| match holder.set_id {
                    Some(set_id) => other.set_id == Some(set_id),
                    None => *other == holder,
                };
                if !holders.iter().any(known) {
                    holders.push(holder);
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok((holders, page.fanout_stats.nodes_failed));
            }
        }
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
        let silent = |why: String| {
            server_error(ServerError::ServiceUnavailableReason(format!(
                "node {node_id} holds the bucket but {why}; retry later"
            )))
        };
        let context = self.state.get_ctx();
        let realm = GetConfigOperation::new(self.state.get_realm_id());
        let realm = drive(realm, &context).await.map_err(internal_error)?;
        if !realm_server(&realm, node_id) {
            return Err(explained(
                ServerError::BadRequest,
                format!(
                    "node {node_id} is not a server node of this realm; pass a node_id from \
                     list_buckets or omit it"
                ),
            ));
        }
        let urls = node_urls(context.as_ref(), node_id).await;
        let Some(base) = urls.and_then(|urls| urls.api) else {
            return Err(silent("publishes no api address".to_string()));
        };
        let mut arguments = serde_json::to_value(input).map_err(internal_error)?;
        arguments["node_id"] = Value::String(node_id.to_string());
        let call = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        });
        let sent = async {
            let response = (CLIENT.post(mcp_url(&base)))
                .bearer_auth(bearer.as_str())
                .header(RELAY_NODE_HEADER, node_id.to_string())
                .header(http::header::ACCEPT, "application/json, text/event-stream")
                .json(&call)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status();
            Ok::<_, String>((status, capped_body(response).await?))
        };
        let (status, body) = match tokio::time::timeout(CALL_TIMEOUT, sent).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(error)) => return Err(silent(format!("did not answer: {error}"))),
            Err(_) => return Err(silent("did not answer in time".to_string())),
        };
        let refused = |error: ServerError| {
            explained(error, format!("node {node_id} refused the forwarded call"))
        };
        match status.as_u16() {
            200..=299 => {}
            401 => return Err(refused(ServerError::Unauthorized)),
            403 => return Err(refused(ServerError::Forbidden)),
            status => {
                return Err(server_error(ServerError::BadGatewayReason(format!(
                    "node {node_id} answered the forwarded call with HTTP {status}"
                ))));
            }
        }
        answer(node_id, &body)
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

fn parse_node(value: &str) -> Result<NodeId, CallToolResult> {
    NodeId::from_str(value).map_err(|_| {
        super::bad_request(
            "node_id must be a node id as list_buckets reports it, or omitted to use the node \
             that holds the bucket",
        )
    })
}

fn route_error(error: RouteError, bucket: &str, failed: usize) -> CallToolResult {
    match error {
        RouteError::Missing if failed > 0 => explained(
            ServerError::ServiceUnavailable,
            format!(
                "no answering node holds a readable bucket named {bucket}, but the realm search \
                 missed {failed} nodes or could not list them; retry, or pass node_id from \
                 list_buckets"
            ),
        ),
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
