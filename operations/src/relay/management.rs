//! Which REST routes go to a management node, and the relay that tries the targets in turn.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::member_read::{MemberBody, MemberMethod};
use aruna_core::operation::Operation;
use aruna_core::structs::CENTRAL_PROVIDER_ID;
use aruna_core::structs::identity::realm::RealmId;
use bytes::Bytes;
use tracing::{debug, warn};

use super::call::{
    NodeCall, NodeCallOperation, RawHeader, RelayFailure, RelayReply, finish_read, read_answer,
    start_read,
};
use super::targets::{RealmNodes, RealmNodesOperation, TargetError};
use crate::s3::object::member_rest::{API_PREFIX, RELAY_HOP_HEADER};

const RELAY_TIMEOUT: Duration = Duration::from_secs(30);
const RELAY_TARGET_LIMIT: usize = 3;
/// The answer headers a relayed call passes back.
const REPLY_HEADERS: &[&str] = &["content-type", "retry-after", "etag"];
/// The answer headers a streamed call passes back.
const STREAM_HEADERS: &[&str] = &["content-type", "retry-after", "cache-control"];

/// Management-only routes, by method and route template. Explicit by design:
/// node-local routes such as compute drain, placement diagnostics and sync
/// quarantine must keep answering on the node they were called on.
pub const RELAYED_ROUTES: &[(&str, &str)] = &[
    ("DELETE", "/access/devices/{node_id}"),
    ("DELETE", "/access/onboarding/secrets/{id}"),
    ("DELETE", "/access/users/me/devices/{id}"),
    ("DELETE", "/plugin-installs/{id}"),
    ("DELETE", "/system/realm/admins/{user_id}"),
    ("DELETE", "/system/realm/email"),
    ("DELETE", "/system/realm/oidc-providers/{id}"),
    ("DELETE", "/system/realm/secrets/{name}"),
    ("GET", "/access/groups/{id}/service-accounts"),
    ("GET", "/access/onboarding/secrets"),
    ("GET", "/access/onboarding/secrets/{id}/status"),
    ("GET", "/access/token"),
    ("GET", "/access/users/me/secrets/{name}/value"),
    ("GET", "/system/realm/placement"),
    ("PATCH", "/access/users/me/devices/{id}"),
    ("PATCH", "/system/realm/nodes/{node_id}"),
    ("PATCH", "/system/realm/placement"),
    ("POST", "/access/groups/{id}/service-accounts"),
    (
        "POST",
        "/access/groups/{id}/service-accounts/{account_id}/tokens",
    ),
    ("POST", "/access/onboarding/bootstrap"),
    ("POST", "/access/onboarding/secrets"),
    ("POST", "/access/sessions"),
    ("POST", "/access/users/register"),
    ("POST", "/plugin-installs/{id}/pause"),
    ("POST", "/plugin-installs/{id}/resume"),
    ("POST", "/plugins"),
    ("POST", "/plugins/{name}/installs"),
    ("POST", "/system/realm/nodes/{node_id}/promote"),
    ("POST", "/system/realm/secrets"),
    ("POST", "/system/realm/secrets/{name}/rotate"),
    ("PUT", "/access/policies/realm"),
    ("PUT", "/access/users/{id}/status"),
    ("PUT", "/compute/config"),
    ("PUT", "/plugin-installs/{id}/grants"),
    ("PUT", "/plugin-installs/{id}/version"),
    ("PUT", "/system/realm/admins/{user_id}"),
    ("PUT", "/system/realm/email"),
    ("PUT", "/system/realm/federation"),
    ("PUT", "/system/realm/oidc-providers/{id}"),
    ("PUT", "/system/realm/portal-features"),
    ("PUT", "/system/realm/quota"),
    ("PUT", "/system/realm/secrets/{name}"),
];

/// Routes every node passes to the realm's merge coordinator, a management node itself included.
const COORDINATED_ROUTES: &[(&str, &str)] = &[
    ("POST", "/access/users/me/merge"),
    ("POST", "/access/users/me/subjects"),
    ("PUT", "/system/realm/merge-coordinator"),
];

/// Routes only the central assistant's holder answers, with no failover: its key and counters
/// exist on that node alone. Provider routes count only for the id `central`.
const HOLDER_ROUTES: &[(&str, &str)] = &[
    ("DELETE", "/system/assistant/central"),
    ("GET", "/system/assistant/central"),
    ("GET", "/system/assistant/providers/{id}/models"),
    ("GET", "/system/assistant/providers/{id}/proxy/{*path}"),
    ("POST", "/system/assistant/central/test"),
    ("POST", "/system/assistant/providers/{id}/proxy/{*path}"),
    ("PUT", "/system/assistant/central"),
];

/// The allowlisted route this request must be relayed for, if any. A management node relays
/// none.
pub fn relay_route(
    method: &str,
    matched: Option<&str>,
    is_management: bool,
) -> Option<&'static str> {
    let matched = matched?;
    let path = matched.strip_prefix(API_PREFIX).unwrap_or(matched);
    RELAYED_ROUTES
        .iter()
        .find(|(route_method, route)| method == *route_method && path == *route)
        .map(|(_, route)| *route)
        .filter(|_| !is_management)
}

/// The coordinated route this request names.
pub fn coordinated_route(method: &str, matched: Option<&str>) -> Option<&'static str> {
    let path = matched?.strip_prefix(API_PREFIX).unwrap_or(matched?);
    COORDINATED_ROUTES
        .iter()
        .find(|(route_method, route)| method == *route_method && path == *route)
        .map(|(_, route)| *route)
}

/// Whether the central assistant's holder must answer this request.
pub fn holder_route(method: &str, matched: Option<&str>, request_path: &str) -> bool {
    let Some(matched) = matched else {
        return false;
    };
    let path = matched.strip_prefix(API_PREFIX).unwrap_or(matched);
    let listed = HOLDER_ROUTES
        .iter()
        .any(|(route_method, route)| method == *route_method && path == *route);
    let central = format!("/system/assistant/providers/{CENTRAL_PROVIDER_ID}/");
    listed && (!path.contains("{id}") || request_path.contains(&central))
}

/// The nodes a call passed, this one appended; `None` when the call came through this node or
/// the target already, which would make it circle.
pub fn next_hops(own: NodeId, passed: &str, target: Option<NodeId>) -> Option<String> {
    let own = own.to_string();
    let seen = |node: &str| passed.split(',').any(|hop| hop.trim() == node);
    if seen(&own) || target.is_some_and(|node| seen(&node.to_string())) {
        return None;
    }
    Some(match passed.is_empty() {
        true => own,
        false => format!("{passed},{own}"),
    })
}

/// Published api urls are bare origins, so the nest comes from the incoming path; a target that
/// already carries it contributes it.
pub fn target_url(target: &str, path: &str, query: Option<&str>) -> String {
    let base = target.trim_end_matches('/');
    let suffix = match base.ends_with(API_PREFIX) {
        true => path.strip_prefix(API_PREFIX).unwrap_or(path),
        false => path,
    };
    match query {
        Some(query) => format!("{base}{suffix}?{query}"),
        None => format!("{base}{suffix}"),
    }
}

/// The headers a streamed answer passes back.
pub fn stream_headers(body: &MemberBody) -> Vec<(String, String)> {
    (STREAM_HEADERS.iter())
        .filter_map(|name| Some((name.to_string(), body.header(name)?.to_string())))
        .collect()
}

/// Whether a send failure may be retried against the next target. A connect failure, also one
/// that timed out, predates any processing; later failures are only safe to repeat for a GET.
fn may_try_next(method: MemberMethod, is_connect: bool) -> bool {
    is_connect || method == MemberMethod::Get
}

/// Whether a gateway answer may move a GET on: an ingress in front of a stopped management node
/// answers 502, 503 or 504 instead of refusing the connection.
fn gateway_failed(method: MemberMethod, status: u16) -> bool {
    method == MemberMethod::Get && matches!(status, 502..=504)
}

/// Whether a node-local enrollment miss may be tried on the next target.
fn may_try_response(route: &str, status: u16) -> bool {
    matches!(
        (route, status),
        (
            "/access/onboarding/secrets/{id}"
                | "/access/onboarding/secrets/{id}/status"
                | "/access/users/me/devices/{id}",
            404
        ) | ("/access/onboarding/bootstrap", 401)
    )
}

/// A call for the management nodes. Never shown in a `Debug` print, as it holds the caller's
/// credentials.
#[derive(PartialEq)]
pub struct ManagementCall {
    pub route: &'static str,
    pub method: MemberMethod,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<RawHeader>,
    pub body: Bytes,
    pub targets: Vec<String>,
}

impl std::fmt::Debug for ManagementCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagementCall")
    }
}

/// How a target behaved: `failed` when it could not be reached or a gateway stood in for it.
#[derive(Debug, PartialEq)]
pub struct TargetMark {
    pub target: String,
    pub failed: bool,
}

#[derive(Debug, PartialEq)]
pub enum RelayEnd {
    Reply(RelayReply),
    /// No target answered and none may have seen the call.
    NoNode,
    /// A target may have applied the call, so it is not tried elsewhere.
    Failed,
}

/// How a relay ended, and what it learned about its targets.
#[derive(Debug, PartialEq)]
pub struct RelayOutcome {
    pub end: RelayEnd,
    pub marks: Vec<TargetMark>,
}

type RelayTurn = Result<Next<RelayOutcome>, IoError>;

/// Tries the targets in order. Only a failure that provably predates processing moves a call
/// that changes state on; an answer another target may improve is kept as the last resort.
#[derive(Debug, PartialEq)]
pub struct ManagementRelayOperation {
    call: ManagementCall,
    tried: usize,
    current: String,
    kept: Option<MemberBody>,
    head: Option<RelayReply>,
    marks: Vec<TargetMark>,
}

impl ManagementRelayOperation {
    pub fn new(call: ManagementCall) -> Self {
        Self {
            call,
            tried: 0,
            current: String::new(),
            kept: None,
            head: None,
            marks: Vec::new(),
        }
    }

    fn finish(&mut self, end: RelayEnd) -> RelayTurn {
        let marks = std::mem::take(&mut self.marks);
        Ok(Next::Done(RelayOutcome { end, marks }))
    }

    fn mark(&mut self, failed: bool) {
        let target = self.current.clone();
        self.marks.push(TargetMark { target, failed });
    }

    fn read(&mut self, body: MemberBody) -> RelayTurn {
        let (head, read) = start_read(body, None);
        self.head = Some(head);
        Ok(Next::Call(read))
    }

    fn try_next(&mut self) -> RelayTurn {
        let targets = self.call.targets.iter().take(RELAY_TARGET_LIMIT);
        let Some(target) = targets.clone().nth(self.tried).cloned() else {
            if let Some(body) = self.kept.take() {
                return self.read(body);
            }
            warn!(
                route = self.call.route,
                "No management node answered a relayed route"
            );
            return self.finish(RelayEnd::NoNode);
        };
        self.tried += 1;
        let call = NodeCall {
            method: self.call.method,
            url: target_url(&target, &self.call.path, self.call.query.as_deref()),
            headers: self.call.headers.clone(),
            body: self.call.body.clone(),
            limit: Some(RELAY_TIMEOUT),
            no_redirect: false,
        };
        self.current = target;
        Ok(Next::Call(call.fetch()))
    }

    fn answered(&mut self, body: MemberBody) -> RelayTurn {
        let (route, method) = (self.call.route, self.call.method);
        self.mark(gateway_failed(MemberMethod::Get, body.status));
        if may_try_response(route, body.status) || gateway_failed(method, body.status) {
            self.kept = Some(body);
            return self.try_next();
        }
        self.read(body)
    }

    fn unanswered(&mut self, failure: RelayFailure) -> RelayTurn {
        let is_connect = match failure {
            RelayFailure::Io(error) => return Err(error),
            RelayFailure::Silent => true,
            _ => false,
        };
        let (route, relay_target) = (self.call.route, &self.current);
        warn!(route, %relay_target, is_connect, error = %failure, "Management relay target failed");
        self.mark(is_connect);
        match may_try_next(self.call.method, is_connect) {
            true => self.try_next(),
            false => self.finish(RelayEnd::Failed),
        }
    }
}

impl Operation for ManagementRelayOperation {
    type Output = RelayOutcome;
    type Error = IoError;

    fn first(&mut self) -> RelayTurn {
        self.try_next()
    }

    fn next(&mut self, result: IoResult) -> RelayTurn {
        let route = self.call.route;
        let Some(head) = self.head.take() else {
            return match read_answer(result) {
                Ok(body) => self.answered(body),
                Err(failure) => self.unanswered(failure),
            };
        };
        match finish_read(head, result) {
            Ok(reply) => {
                debug!(route, status = reply.status, "Relayed a management route");
                self.finish(RelayEnd::Reply(reply.keeping(REPLY_HEADERS)))
            }
            Err(RelayFailure::Io(error)) => Err(error),
            // The management node already answered, so it may have applied the call.
            Err(error) => {
                warn!(route, error = %error, "Management relay response failed");
                self.finish(RelayEnd::Failed)
            }
        }
    }
}

/// A call for one named node. Never shown in a `Debug` print, as it holds the caller's
/// credentials.
#[derive(PartialEq)]
pub struct NodeRelay {
    pub realm_id: RealmId,
    pub local: NodeId,
    pub node: NodeId,
    pub method: MemberMethod,
    pub path: String,
    pub query: Option<String>,
    /// The hop header as received, empty when there was none.
    pub passed: String,
    pub headers: Vec<RawHeader>,
    pub body: Bytes,
}

impl std::fmt::Debug for NodeRelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeRelay")
    }
}

#[derive(Debug, PartialEq)]
pub enum NodeRelayEnd {
    Reply(RelayReply),
    NoConfig,
    NoAddress,
    Circling,
    Failed,
}

/// Passes a call once to one named node, such as the receiver of a plugin install.
#[derive(Debug, PartialEq)]
pub struct NodeRelayOperation {
    relay: NodeRelay,
    sent: bool,
}

impl NodeRelayOperation {
    pub fn new(relay: NodeRelay) -> Self {
        Self { relay, sent: false }
    }

    fn send(&mut self, nodes: RealmNodes) -> Result<Next<NodeRelayEnd>, IoError> {
        let relay = &mut self.relay;
        let Some(target) = nodes.urls.get(&relay.node) else {
            return Ok(Next::Done(NodeRelayEnd::NoAddress));
        };
        let Some(hops) = next_hops(relay.local, &relay.passed, Some(relay.node)) else {
            return Ok(Next::Done(NodeRelayEnd::Circling));
        };
        let mut headers = std::mem::take(&mut relay.headers);
        headers.push((RELAY_HOP_HEADER.to_string(), hops.into_bytes()));
        let call = NodeCall {
            method: relay.method,
            url: target_url(target, &relay.path, relay.query.as_deref()),
            headers,
            body: std::mem::take(&mut relay.body),
            limit: Some(RELAY_TIMEOUT),
            no_redirect: false,
        };
        self.sent = true;
        Ok(Next::Call(IoCall::child(NodeCallOperation::new(
            call, None,
        ))))
    }
}

impl Operation for NodeRelayOperation {
    type Output = NodeRelayEnd;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<NodeRelayEnd>, IoError> {
        let nodes = RealmNodesOperation::new(self.relay.realm_id);
        Ok(Next::Call(IoCall::child(nodes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeRelayEnd>, IoError> {
        if !self.sent {
            return match result.child::<RealmNodes, TargetError>()? {
                Ok(nodes) => self.send(nodes),
                Err(_) => Ok(Next::Done(NodeRelayEnd::NoConfig)),
            };
        }
        Ok(Next::Done(
            match result.child::<RelayReply, RelayFailure>()? {
                Ok(reply) => NodeRelayEnd::Reply(reply.keeping(REPLY_HEADERS)),
                Err(error) => {
                    let node = self.relay.node;
                    warn!(%node, error = %error, "Relay to a node failed");
                    NodeRelayEnd::Failed
                }
            },
        ))
    }
}

#[cfg(test)]
#[path = "management_tests.rs"]
mod tests;
