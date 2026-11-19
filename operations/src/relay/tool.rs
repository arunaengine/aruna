//! Runs a data tool on the realm node that holds its bucket, as the caller with the caller's own
//! bearer token: where the call goes, what is sent and what the answer means.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::member_read::MemberMethod;
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::node_info::NodeUrls;
use bytes::Bytes;
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

use super::call::{MAX_REPLY_BYTES, NodeCall, NodeCallOperation, RelayFailure, RelayReply};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::s3::object::member_rest::{API_PREFIX, RELAY_NODE_HEADER};

/// How long a forwarded call may take, its answer included; a scan may run for a while.
const CALL_TIMEOUT: Duration = Duration::from_secs(120);

/// A realm node that holds a readable bucket of the asked name, as the bucket search finds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Holder {
    pub node_id: NodeId,
    /// The distributed bucket this node's bucket is a member of.
    pub set_id: Option<Ulid>,
}

/// Adds `holder` unless the search already found it, each distributed bucket counting once.
pub fn add_holder(holders: &mut Vec<Holder>, holder: Holder) {
    let known = |other: &Holder| match holder.set_id {
        Some(set_id) => other.set_id == Some(set_id),
        None => *other == holder,
    };
    if !holders.iter().any(known) {
        holders.push(holder);
    }
}

/// Whether a holder search stops after a page: at the end, once a holder is known and nodes
/// failed, or when nodes failed and nothing came back. A node still scanning may hold a bucket
/// of the same name, so a page past the name does not end the search.
pub fn search_done(found: bool, (hits, failed): (bool, bool), end: bool) -> bool {
    end || (found && failed) || (failed && !hits)
}

#[derive(Debug, PartialEq, Eq)]
pub enum RouteError {
    /// No node holds a bucket of the name that the caller may read.
    Missing,
    /// Several nodes hold different buckets of the name.
    Ambiguous(Vec<NodeId>),
}

/// The node a call on a bucket runs on, `None` for this node: the `named` node; else this node
/// when it holds the bucket, unless `routed` names the member keeping the key's records; else
/// the one holder, for a distributed bucket the first of `members` that keep the call's key.
pub fn pick_node(
    here: NodeId,
    (named, local, routed): (Option<NodeId>, bool, Option<NodeId>),
    holders: &[Holder],
    members: &[NodeId],
) -> Result<Option<NodeId>, RouteError> {
    if let Some(named) = named.filter(|named| *named != here) {
        return Ok(Some(named));
    }
    if named.is_some() || local {
        return Ok(routed.filter(|routed| *routed != here));
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
fn realm_server(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let node_id = node_id.to_string();
    (config.nodes.iter()).any(|node| node.node_id == node_id && node.kind.is_sync_eligible())
}

/// The `/mcp` url on a node's published api base, which may carry the REST nest.
fn mcp_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    format!("{}/mcp", base.strip_suffix(API_PREFIX).unwrap_or(base))
}

/// A tool call for another node. Never shown in a `Debug` print, as it holds the caller's token.
#[derive(PartialEq)]
pub struct ToolCall {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bearer: SharedSecret,
    pub tool: String,
    pub arguments: Value,
}

impl std::fmt::Debug for ToolCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolCall")
    }
}

/// Why a forwarded tool call brought no answer to decode.
#[derive(Debug, Error, PartialEq)]
pub enum ToolRefusal {
    #[error("the node is not a server node of this realm")]
    NotServer,
    #[error("{0}")]
    Failed(String),
    #[error("the node publishes no api address")]
    NoAddress,
    #[error("the node did not answer: {0}")]
    Silent(String),
    #[error("the node did not answer in time")]
    Late,
    #[error("the node refused the caller")]
    Unauthorized,
    #[error("the node forbade the call")]
    Forbidden,
    #[error("the node answered with HTTP {0}")]
    Status(u16),
    #[error(transparent)]
    Io(#[from] IoError),
}

fn tool_answer(reply: RelayReply) -> Result<Bytes, ToolRefusal> {
    match reply.status {
        200..=299 => Ok(reply.body),
        401 => Err(ToolRefusal::Unauthorized),
        403 => Err(ToolRefusal::Forbidden),
        status => Err(ToolRefusal::Status(status)),
    }
}

fn tool_failure(failure: RelayFailure) -> ToolRefusal {
    match failure {
        RelayFailure::Late => ToolRefusal::Late,
        RelayFailure::Io(error) => ToolRefusal::Io(error),
        other => ToolRefusal::Silent(other.to_string()),
    }
}

#[derive(Debug, PartialEq)]
enum ToolStep {
    Config,
    Urls,
    Reply,
}

/// Sends a tool call to a server node of the realm, directed at that node so it answers itself
/// and never forwards again. The output is the node's JSON-RPC reply.
#[derive(Debug, PartialEq)]
pub struct ToolCallOperation {
    call: ToolCall,
    step: ToolStep,
}

impl ToolCallOperation {
    pub fn new(call: ToolCall) -> Self {
        Self {
            call,
            step: ToolStep::Config,
        }
    }

    fn request(&mut self, base: &str) -> Result<NodeCall, ToolRefusal> {
        let call = &mut self.call;
        let node_id = call.node_id.to_string();
        let mut arguments = call.arguments.take();
        let Some(fields) = arguments.as_object_mut() else {
            return Err(ToolRefusal::Failed("tool input is no object".to_string()));
        };
        fields.insert("node_id".to_string(), Value::String(node_id.clone()));
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": call.tool, "arguments": arguments },
        });
        let body = serde_json::to_vec(&body).map_err(|e| ToolRefusal::Failed(e.to_string()))?;
        let mut bearer = b"Bearer ".to_vec();
        bearer.extend_from_slice(call.bearer.bytes().expose());
        let text = |value: &str| value.as_bytes().to_vec();
        Ok(NodeCall {
            method: MemberMethod::Post,
            url: mcp_url(base),
            headers: vec![
                ("authorization".to_string(), bearer),
                (RELAY_NODE_HEADER.to_string(), node_id.into_bytes()),
                (
                    "accept".to_string(),
                    text("application/json, text/event-stream"),
                ),
                ("content-type".to_string(), text("application/json")),
            ],
            body: Bytes::from(body),
            limit: Some(CALL_TIMEOUT),
            no_redirect: false,
        })
    }
}

impl Operation for ToolCallOperation {
    type Output = Bytes;
    type Error = ToolRefusal;

    fn first(&mut self) -> Result<Next<Bytes>, ToolRefusal> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.call.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Bytes>, ToolRefusal> {
        match std::mem::replace(&mut self.step, ToolStep::Reply) {
            ToolStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| ToolRefusal::Failed(error.to_string()))?;
                if !realm_server(&config, self.call.node_id) {
                    return Err(ToolRefusal::NotServer);
                }
                self.step = ToolStep::Urls;
                let urls = NodeUrlsOperation::new(self.call.node_id);
                Ok(Next::Call(IoCall::child(urls)))
            }
            ToolStep::Urls => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                let base = (urls.ok().flatten()).and_then(|urls| urls.api);
                let base = base.ok_or(ToolRefusal::NoAddress)?;
                let call = NodeCallOperation::new(self.request(&base)?, Some(MAX_REPLY_BYTES));
                Ok(Next::Call(IoCall::child(call)))
            }
            ToolStep::Reply => {
                let reply = result.child::<RelayReply, RelayFailure>()?;
                tool_answer(reply.map_err(tool_failure)?).map(Next::Done)
            }
        }
    }
}

#[cfg(test)]
#[path = "tool_tests.rs"]
mod tests;
