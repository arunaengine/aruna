//! Relays management-only REST routes to a management node and passes the answer back verbatim.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::ServerError;
use crate::routes::assistant::PROXY_BODY_LIMIT;
use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_core::member_read::MemberMethod;
use aruna_core::structs::SESSION_HEADER;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::driver::drive;
use aruna_operations::relay::call::{
    NodeCall, RawHeader, RelayFailure, RelayReply, StartCallOperation,
};
use aruna_operations::relay::management::{
    ManagementCall, ManagementRelayOperation, NodeRelay, NodeRelayEnd, NodeRelayOperation,
    RelayEnd, TargetMark, coordinated_route, holder_route, next_hops, relay_route, stream_headers,
    target_url,
};
use aruna_operations::relay::targets::{
    CoordinatorTarget, CoordinatorTargetOperation, HolderTargetOperation, PeerTargetsOperation,
};
use aruna_operations::s3::object::member_rest;
use axum::body::{Body, Bytes};
use axum::extract::{FromRequest, MatchedPath, OriginalUri, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// The nodes a relayed call passed, comma-separated. Anyone can send it, so it never makes a
/// node answer a call it would relay; it only stops a call that would circle.
pub(crate) const RELAY_HOP_HEADER: HeaderName =
    HeaderName::from_static(member_rest::RELAY_HOP_HEADER);

/// Realm membership changes rarely, so the resolved targets are reused inside
/// this window instead of re-reading realm state per request.
const MANAGEMENT_URL_TTL: Duration = Duration::from_secs(60);

/// Management api urls this node last resolved, and when.
#[derive(Debug, Default)]
pub struct ManagementUrlCache {
    urls: Vec<String>,
    refreshed_at: Option<Instant>,
    /// Targets whose last connect failed, and when; they are tried last while fresh.
    failed: BTreeMap<String, Instant>,
}

pub(crate) async fn relay_middleware(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let matched = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string());
    let matched = matched.as_deref();
    let method = request.method().as_str();
    let (realm_id, here, context) = (state.get_realm_id(), state.get_node_id(), state.get_ctx());
    // The holder is checked on every central call, relayed or not: a hop header proves nothing.
    if holder_route(method, matched, request.uri().path()) {
        let holder = HolderTargetOperation::new(realm_id, here);
        return match drive(holder, &context).await {
            Ok(None) => next.run(request).await,
            Ok(Some((node, target))) => match passed_hops(&state, request.headers(), node) {
                Some(hops) => stream_relay(&state, &target, hops, request).await,
                None => {
                    warn!(relay_target = %target, "Central assistant call would circle between nodes");
                    ServerError::NoManagementNode.into_response()
                }
            },
            Err(_) => ServerError::NoManagementNode.into_response(),
        };
    }
    if let Some(route) = coordinated_route(method, matched) {
        let target = CoordinatorTargetOperation::new(realm_id, here, state.is_management_node());
        return match drive(target, &context).await {
            Ok(CoordinatorTarget::Here) => next.run(request).await,
            Ok(CoordinatorTarget::Node(url)) => relay(&state, route, vec![url], request).await,
            Ok(CoordinatorTarget::Managers) => {
                let targets = management_targets(&state).await;
                relay(&state, route, targets, request).await
            }
            Err(_) => ServerError::NoManagementNode.into_response(),
        };
    }
    let Some(route) = relay_route(method, matched, state.is_management_node()) else {
        return next.run(request).await;
    };
    let urls = management_targets(&state).await;
    let targets = healthy_first(urls, &state.management_url_cache().read().await.failed);
    relay(&state, route, targets, request).await
}

/// The hop header a call came with, empty when it has none or it is no text.
pub(crate) fn passed_text(headers: &HeaderMap) -> &str {
    headers
        .get(RELAY_HOP_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
}

fn passed_hops(state: &ServerState, headers: &HeaderMap, target: Option<NodeId>) -> Option<String> {
    next_hops(state.get_node_id(), passed_text(headers), target)
}

/// The first value of each of `names` that `headers` holds, byte for byte.
fn picked(headers: &HeaderMap, names: &[HeaderName]) -> Vec<RawHeader> {
    (names.iter())
        .filter_map(|name| {
            let value = headers.get(name)?.as_bytes().to_vec();
            Some((name.as_str().to_string(), value))
        })
        .collect()
}

fn hop_header(hops: String) -> RawHeader {
    (RELAY_HOP_HEADER.as_str().to_string(), hops.into_bytes())
}

/// Inside the `/api/v1` nest the URI lacks that prefix; the original URI keeps it.
fn original_uri(request: &Request) -> Uri {
    request
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| request.uri().clone(), |original| original.0.clone())
}

/// Sets `headers` on `response`, replacing what it holds under the same names.
pub(crate) fn add_headers(response: &mut Response, headers: &[(String, String)]) {
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes());
        if let (Ok(name), Ok(value)) = (name, HeaderValue::from_str(value)) {
            response.headers_mut().insert(name, value);
        }
    }
}

pub(crate) fn reply_status(reply: &RelayReply) -> StatusCode {
    StatusCode::from_u16(reply.status).unwrap_or(StatusCode::BAD_GATEWAY)
}

/// Passes a central assistant call to its holder and streams the answer back as it arrives.
async fn stream_relay(
    state: &ServerState,
    target: &str,
    hops: String,
    request: Request,
) -> Response {
    let uri = original_uri(&request);
    let Some(method) = MemberMethod::parse(request.method().as_str()) else {
        return ServerError::RelayFailed.into_response();
    };
    let forwarded = [
        header::AUTHORIZATION,
        header::CONTENT_TYPE,
        header::ACCEPT,
        HeaderName::from_static(SESSION_HEADER),
    ];
    let mut headers = picked(request.headers(), &forwarded);
    headers.push(hop_header(hops));
    let body = match axum::body::to_bytes(request.into_body(), PROXY_BODY_LIMIT).await {
        Ok(body) => body,
        Err(_) => {
            return ServerError::PayloadTooLarge("request body exceeds 4 MiB".to_string())
                .into_response();
        }
    };
    let call = NodeCall {
        method,
        url: target_url(target, uri.path(), uri.query()),
        headers,
        body,
        limit: None,
        no_redirect: false,
    };
    let answer = match drive(StartCallOperation::new(call), &state.get_ctx()).await {
        Ok(answer) => answer,
        Err(error) => {
            warn!(relay_target = %target, error = %error, "Central assistant relay failed");
            return match error {
                RelayFailure::Silent => ServerError::NoManagementNode.into_response(),
                _ => ServerError::RelayFailed.into_response(),
            };
        }
    };
    let kept = stream_headers(&answer);
    let mut relayed = Response::new(Body::from_stream(answer.stream.0));
    *relayed.status_mut() = StatusCode::from_u16(answer.status).unwrap_or(StatusCode::BAD_GATEWAY);
    add_headers(&mut relayed, &kept);
    relayed
}

async fn relay(
    state: &Arc<ServerState>,
    route: &'static str,
    targets: Vec<String>,
    request: Request,
) -> Response {
    if targets.is_empty() {
        warn!(route, "No management node is known for a relayed route");
        return ServerError::NoManagementNode.into_response();
    }

    let Some(hop) = passed_hops(state, request.headers(), None) else {
        warn!(route, "A relayed route would circle between nodes");
        return ServerError::NoManagementNode.into_response();
    };
    let Some(method) = MemberMethod::parse(request.method().as_str()) else {
        return ServerError::RelayFailed.into_response();
    };
    let uri = original_uri(&request);
    let forwarded = [
        header::AUTHORIZATION,
        header::CONTENT_TYPE,
        header::IF_MATCH,
        header::IF_NONE_MATCH,
    ];
    let mut headers = picked(request.headers(), &forwarded);
    headers.push(hop_header(hop));
    // An artifact upload is read only for a caller who may publish, within this node's memory.
    let mut _memory = None;
    let body = if route == "/plugins" {
        let auth = request
            .extensions()
            .get::<Option<AuthContext>>()
            .cloned()
            .flatten();
        match crate::routes::plugins::read_upload(state, auth, request).await {
            Ok((_, body, memory)) => {
                _memory = Some(memory);
                body
            }
            Err(error) => return error.into_response(),
        }
    } else {
        match Bytes::from_request(request, &()).await {
            Ok(body) => body,
            Err(rejection) => return rejection.into_response(),
        }
    };
    let call = ManagementCall {
        route,
        method,
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        headers,
        body,
        targets,
    };
    let Ok(outcome) = drive(ManagementRelayOperation::new(call), &state.get_ctx()).await else {
        return ServerError::RelayFailed.into_response();
    };
    record_marks(state, outcome.marks).await;
    match outcome.end {
        RelayEnd::Reply(reply) => relayed_response(reply),
        RelayEnd::NoNode => ServerError::NoManagementNode.into_response(),
        RelayEnd::Failed => ServerError::RelayFailed.into_response(),
    }
}

/// Passes a call once to one named node, such as the receiver of a plugin install, and its
/// answer back. A node without a published address or a call that would circle is refused.
pub(crate) async fn relay_to_node(
    state: &Arc<ServerState>,
    node: NodeId,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let unavailable =
        |reason: String| ServerError::ServiceUnavailableReason(reason).into_response();
    let Some(method) = MemberMethod::parse(method.as_str()) else {
        return ServerError::RelayFailed.into_response();
    };
    let forwarded = [
        header::AUTHORIZATION,
        header::CONTENT_TYPE,
        HeaderName::from_static(SESSION_HEADER),
    ];
    let relay = NodeRelay {
        realm_id: state.get_realm_id(),
        local: state.get_node_id(),
        node,
        method,
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        passed: passed_text(headers).to_string(),
        headers: picked(headers, &forwarded),
        body,
    };
    match drive(NodeRelayOperation::new(relay), &state.get_ctx()).await {
        Ok(NodeRelayEnd::Reply(reply)) => relayed_response(reply),
        Ok(NodeRelayEnd::NoConfig) => unavailable("the realm configuration is not readable".into()),
        Ok(NodeRelayEnd::NoAddress) => unavailable(format!("node {node} has no published address")),
        Ok(NodeRelayEnd::Circling) => unavailable(format!(
            "the call to node {node} would circle between nodes"
        )),
        Ok(NodeRelayEnd::Failed) | Err(_) => ServerError::RelayFailed.into_response(),
    }
}

/// Targets with a fresh failure move last; the order is kept otherwise.
fn healthy_first(mut targets: Vec<String>, failed: &BTreeMap<String, Instant>) -> Vec<String> {
    targets.sort_by_key(|target| is_fresh(failed.get(target).copied()));
    targets
}

/// Remembers a connect failure or gateway answer of a target; any other answer clears the mark.
async fn record_marks(state: &ServerState, marks: Vec<TargetMark>) {
    let cache = state.management_url_cache();
    for TargetMark { target, failed } in marks {
        if !failed && !cache.read().await.failed.contains_key(&target) {
            continue;
        }
        let mut cached = cache.write().await;
        match failed {
            true => {
                cached.failed.retain(|_, at| is_fresh(Some(*at)));
                cached.failed.insert(target, Instant::now());
            }
            false => {
                cached.failed.remove(&target);
            }
        }
    }
}

fn relayed_response(reply: RelayReply) -> Response {
    let status = reply_status(&reply);
    let mut relayed = (status, reply.body).into_response();
    relayed.headers_mut().remove(header::CONTENT_TYPE);
    add_headers(&mut relayed, &reply.headers);
    relayed
}

/// Cached management targets; a stale window triggers a realm-config read,
/// and a failed read reuses the last known set rather than dropping every target.
async fn management_targets(state: &Arc<ServerState>) -> Vec<String> {
    let cache = state.management_url_cache();
    {
        let cached = cache.read().await;
        if is_fresh(cached.refreshed_at) {
            return cached.urls.clone();
        }
    }

    let mut cached = cache.write().await;
    if is_fresh(cached.refreshed_at) {
        return cached.urls.clone();
    }
    let peers = PeerTargetsOperation::new(state.get_realm_id(), state.get_node_id());
    match drive(peers, &state.get_ctx()).await {
        Ok(urls) => cached.urls = urls,
        Err(error) => debug!(error = %error, "Management relay reuses cached management urls"),
    }
    cached.refreshed_at = Some(Instant::now());
    cached.urls.clone()
}

fn is_fresh(refreshed_at: Option<Instant>) -> bool {
    refreshed_at.is_some_and(|refreshed_at| refreshed_at.elapsed() < MANAGEMENT_URL_TTL)
}

#[cfg(test)]
#[path = "management_relay_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relay_routes_tests.rs"]
mod test_routes;
