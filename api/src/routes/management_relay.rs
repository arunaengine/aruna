//! Relays management-only REST routes to a management node and passes the answer back verbatim.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::ServerError;
use crate::routes::info::{load_node_documents, management_node_urls};
use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_operations::device::realm_documents::installed_management_urls;
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::GetConfigOperation;
use axum::body::Bytes;
use axum::extract::{FromRequest, MatchedPath, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Loop guard: a request that already carries a hop is answered by the node it
/// reached, never relayed again.
pub(crate) const RELAY_HOP_HEADER: HeaderName = HeaderName::from_static("x-aruna-relay-hop");

/// The nest every REST route is served under, stripped before the path is
/// appended to a peer's published api base url.
const API_PREFIX: &str = "/api/v1";

/// Realm membership changes rarely, so the resolved targets are reused inside
/// this window instead of re-reading realm state per request.
const MANAGEMENT_URL_TTL: Duration = Duration::from_secs(60);
const RELAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RELAY_TIMEOUT: Duration = Duration::from_secs(30);
const RELAY_TARGET_LIMIT: usize = 3;

/// Management-only routes, by method and route template. Explicit by design:
/// node-local routes such as compute drain, placement diagnostics and sync
/// quarantine must keep answering on the node they were called on.
const RELAYED_ROUTES: &[(&str, &str)] = &[
    ("DELETE", "/access/devices/{node_id}"),
    ("DELETE", "/access/onboarding/secrets/{id}"),
    ("DELETE", "/access/users/me/devices/{id}"),
    ("GET", "/access/onboarding/secrets"),
    ("GET", "/access/onboarding/secrets/{id}/status"),
    ("GET", "/access/token"),
    ("GET", "/system/realm/placement"),
    ("PATCH", "/system/realm/placement"),
    ("POST", "/access/onboarding/bootstrap"),
    ("POST", "/access/onboarding/secrets"),
    ("POST", "/access/sessions"),
    ("PUT", "/access/policies/realm"),
    ("PUT", "/compute/config"),
    ("PUT", "/system/realm/quota"),
];

static RELAY_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(RELAY_CONNECT_TIMEOUT)
        .timeout(RELAY_TIMEOUT)
        .build()
        .unwrap_or_default()
});

/// Management api urls this node last resolved, and when.
#[derive(Debug, Default)]
pub struct ManagementUrlCache {
    urls: Vec<String>,
    refreshed_at: Option<Instant>,
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
    let route = relay_route(
        request.method(),
        matched.as_deref(),
        state.is_management_node(),
        request.headers().contains_key(RELAY_HOP_HEADER),
    );
    match route {
        Some(route) => relay(&state, route, request).await,
        None => next.run(request).await,
    }
}

/// The allowlisted route this request must be relayed for, if any.
fn relay_route(
    method: &Method,
    matched: Option<&str>,
    is_management: bool,
    has_hop: bool,
) -> Option<&'static str> {
    if is_management || has_hop {
        return None;
    }
    let matched = matched?;
    let path = matched.strip_prefix(API_PREFIX).unwrap_or(matched);
    RELAYED_ROUTES
        .iter()
        .find(|(route_method, route)| method.as_str() == *route_method && path == *route)
        .map(|(_, route)| *route)
}

async fn relay(state: &Arc<ServerState>, route: &'static str, request: Request) -> Response {
    let targets = management_targets(state).await;
    if targets.is_empty() {
        warn!(route, "No management node is known for a relayed route");
        return ServerError::NoManagementNode.into_response();
    }

    let method = request.method().clone();
    let uri = request.uri().clone();
    let authorization = request.headers().get(header::AUTHORIZATION).cloned();
    let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
    let body = match Bytes::from_request(request, &()).await {
        Ok(body) => body,
        Err(rejection) => return rejection.into_response(),
    };
    let hop = HeaderValue::from_str(&state.get_node_id().to_string())
        .unwrap_or_else(|_| HeaderValue::from_static("relayed"));

    // Targets are tried in order, but only a failure that provably predates
    // processing may move a non-idempotent request on to the next target.
    let mut unknown = None;
    for target in targets.iter().take(RELAY_TARGET_LIMIT) {
        let url = relay_url(target, &uri);
        let mut outgoing = RELAY_CLIENT
            .request(method.clone(), &url)
            .header(RELAY_HOP_HEADER, hop.clone())
            .body(body.clone());
        if let Some(authorization) = &authorization {
            outgoing = outgoing.header(header::AUTHORIZATION, authorization.clone());
        }
        if let Some(content_type) = &content_type {
            outgoing = outgoing.header(header::CONTENT_TYPE, content_type.clone());
        }
        match outgoing.send().await {
            Ok(response) if may_try_response(route, response.status()) => {
                unknown = Some((url, response));
            }
            Ok(response) => return relayed_response(route, &url, response).await,
            Err(error) => {
                let is_connect = error.is_connect();
                warn!(route, relay_target = %url, is_connect, error = %error, "Management relay target failed");
                if !may_try_next(&method, is_connect) {
                    return ServerError::RelayFailed.into_response();
                }
            }
        }
    }

    if let Some((url, response)) = unknown {
        return relayed_response(route, &url, response).await;
    }

    warn!(route, "No management node answered a relayed route");
    ServerError::NoManagementNode.into_response()
}

/// Whether a send failure may be retried against the next target. A connect
/// failure, which reqwest also reports for a connect timeout, provably predates
/// any processing; later failures are only safe to repeat for an idempotent method.
fn may_try_next(method: &Method, is_connect: bool) -> bool {
    is_connect || method == Method::GET
}

/// Whether a node-local enrollment miss may be tried on the next target.
fn may_try_response(route: &str, status: StatusCode) -> bool {
    matches!(
        (route, status),
        (
            "/access/onboarding/secrets/{id}"
                | "/access/onboarding/secrets/{id}/status"
                | "/access/users/me/devices/{id}",
            StatusCode::NOT_FOUND
        ) | ("/access/onboarding/bootstrap", StatusCode::UNAUTHORIZED)
    )
}

/// Published api urls are bare origins (`API_PUBLIC_URL`), so the nest comes
/// from the incoming path; a target that already carries it contributes it.
fn relay_url(target: &str, uri: &Uri) -> String {
    let base = target.trim_end_matches('/');
    let path = uri.path();
    let suffix = match base.ends_with(API_PREFIX) {
        true => path.strip_prefix(API_PREFIX).unwrap_or(path),
        false => path,
    };
    match uri.query() {
        Some(query) => format!("{base}{suffix}?{query}"),
        None => format!("{base}{suffix}"),
    }
}

async fn relayed_response(route: &'static str, url: &str, response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = relayed_headers(response.headers());
    match response.bytes().await {
        Ok(body) => {
            debug!(route, relay_target = %url, status = status.as_u16(), "Relayed a management route");
            let mut relayed = (status, body).into_response();
            relayed.headers_mut().remove(header::CONTENT_TYPE);
            for (name, value) in headers {
                relayed.headers_mut().insert(name, value);
            }
            relayed
        }
        Err(error) => {
            warn!(route, relay_target = %url, error = %error, "Management relay response failed");
            ServerError::BadGateway.into_response()
        }
    }
}

fn relayed_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    [&header::CONTENT_TYPE, &header::RETRY_AFTER]
        .into_iter()
        .filter_map(|name| {
            headers
                .get(name)
                .cloned()
                .map(|value| (name.clone(), value))
        })
        .collect()
}

/// Cached management targets; a stale window triggers a realm-config read, and
/// a failed read reuses the last known set rather than dropping every target.
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
    match drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(config) => {
            let documents = load_node_documents(state, &config).await;
            let peers = peer_management_urls(state.get_node_id(), &config, &documents);
            let installed = installed_management_urls(&state.get_ctx(), state.get_realm_id()).await;
            cached.urls = relay_targets(peers, installed);
        }
        Err(error) => debug!(error = %error, "Management relay reuses cached management urls"),
    }
    cached.refreshed_at = Some(Instant::now());
    cached.urls.clone()
}

fn is_fresh(refreshed_at: Option<Instant>) -> bool {
    refreshed_at.is_some_and(|refreshed_at| refreshed_at.elapsed() < MANAGEMENT_URL_TTL)
}

/// The targets a relayed route is tried against. A device holds no peer
/// node-info document, so the list a realm node installed on it is the only
/// address it has for a management node.
fn relay_targets(peers: Vec<String>, installed: Vec<String>) -> Vec<String> {
    match peers.is_empty() {
        true => installed,
        false => peers,
    }
}

/// Management peers in node-id order, this node excluded. The order is stable
/// so repeated calls pin the same peer: an onboarding secret is minted into one
/// management node's local store and its status and revoke must return there.
fn peer_management_urls(
    current: NodeId,
    config: &aruna_core::structs::identity::realm::RealmConfigDocument,
    documents: &BTreeMap<NodeId, aruna_core::structs::storage::node_info::NodeInfoDocument>,
) -> Vec<String> {
    let ordered: BTreeMap<NodeId, String> = management_node_urls(config, documents)
        .into_iter()
        .filter(|(node_id, _)| *node_id != current)
        .filter_map(|(node_id, url)| url.map(|url| (node_id, url)))
        .collect();
    let mut urls: Vec<String> = Vec::new();
    for url in ordered.into_values() {
        if !urls.contains(&url) {
            urls.push(url);
        }
    }
    urls
}

#[cfg(test)]
#[path = "management_relay_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "relay_routes_tests.rs"]
mod test_routes;
