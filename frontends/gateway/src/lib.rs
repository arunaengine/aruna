//! The stateless applet gateway: serves every applet host from the node's edge routes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod hosts;
mod notices;
mod pages;
mod policy;
mod preview;
mod runtime;
mod serve;
mod session;
pub mod settings;
mod signin;
mod upstream;
mod visit;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::extract::{Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::{CACHE_CONTROL, HOST, HeaderMap, HeaderValue, ORIGIN};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Deserialize;
use tracing::{info, warn};

use crate::hosts::{HostCache, normalize};
use crate::pages::{bare, page, unavailable};
use crate::policy::Frame;
use crate::session::Sealer;
use crate::settings::Settings;
use crate::upstream::{Answer, Upstream};
use crate::visit::Visit;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid gateway settings: {0}")]
    Settings(String),
    #[error("the gateway listener failed")]
    Listen(#[source] std::io::Error),
}

/// What every request shares: the settings, the node client, the decision cache and the
/// cookie sealer.
pub struct Gateway {
    settings: Settings,
    upstream: Upstream,
    hosts: HostCache,
    sealer: Sealer,
}

/// Serves until SIGTERM or Ctrl-C. The notice followers end with the runtime.
pub async fn run(settings: Settings) -> Result<(), GatewayError> {
    let gateway = Gateway::new(settings)?;
    for node in 0..gateway.upstream.nodes() {
        tokio::spawn(notices::follow(gateway.clone(), node));
    }
    let listen = gateway.settings.listen;
    let listener = (tokio::net::TcpListener::bind(listen).await).map_err(GatewayError::Listen)?;
    info!(%listen, "the applet gateway listens");
    let service = router(gateway).into_make_service_with_connect_info::<SocketAddr>();
    (axum::serve(listener, service)
        .with_graceful_shutdown(stopped())
        .await)
        .map_err(GatewayError::Listen)
}

pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/_edge/health", get(health))
        .route("/_edge/tls-check", get(tls_check))
        .fallback(entry)
        .with_state(gateway)
}

impl Gateway {
    pub fn new(settings: Settings) -> Result<Arc<Self>, GatewayError> {
        let upstream = Upstream::new(settings.upstreams.clone(), settings.edge_key.clone())?;
        Ok(Arc::new(Self {
            hosts: HostCache::new(settings.host_cache),
            sealer: Sealer::new(&settings.edge_key),
            upstream,
            settings,
        }))
    }

    /// The decision for a normalized host, from the cache or the node.
    async fn decide(&self, host: &str) -> Result<Option<Arc<Answer>>, reqwest::Error> {
        if let Some(cached) = self.hosts.get(host, Instant::now()) {
            return Ok(cached);
        }
        let answer = self.upstream.decide(host).await?.map(Arc::new);
        self.hosts.put(host, answer.clone(), Instant::now());
        Ok(answer)
    }
}

/// One unambiguous normalized host from `Host` and the URI authority; conflicting values fail.
fn request_host(request: &Request) -> Option<String> {
    let mut headers = request.headers().get_all(HOST).iter();
    let header_host = match (headers.next(), headers.next()) {
        (Some(_), Some(_)) => return None,
        (Some(value), None) => Some(normalize(value.to_str().ok()?)?),
        (None, _) => None,
    };
    let uri_host = match request.uri().host() {
        Some(raw) => Some(normalize(raw)?),
        None => None,
    };
    match (header_host, uri_host) {
        (Some(left), Some(right)) if left == right => Some(left),
        (Some(_), Some(_)) => None,
        (Some(host), None) | (None, Some(host)) => Some(host),
        (None, None) => None,
    }
}

/// Every applet request: find the host's decision, then serve `/_aruna/` or a pinned file.
async fn entry(State(gateway): State<Arc<Gateway>>, request: Request) -> Response {
    let none = Frame::Embed(&[]);
    let Some(host) = request_host(&request) else {
        let message = "The request names no valid host.";
        return page(StatusCode::BAD_REQUEST, "Unknown address", message, none);
    };
    let answer = match gateway.decide(&host).await {
        Ok(Some(answer)) => answer,
        Ok(None) => {
            let message = "This address does not serve an applet.";
            return page(StatusCode::NOT_FOUND, "No applet here", message, none);
        }
        Err(error) => {
            warn!(%error, %host, "no node answered for an applet host");
            return unavailable(none);
        }
    };
    let visit = Visit::read(&gateway, host, answer, request.headers());
    if service_worker(request.headers()) {
        let message = "Applet hosts do not allow service workers.";
        let mut response = page(
            StatusCode::FORBIDDEN,
            "Service workers disabled",
            message,
            visit.frame(),
        );
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return response;
    }
    match request.uri().path().strip_prefix("/_aruna/") {
        Some(rest) => {
            let rest = rest.to_string();
            runtime::route(&gateway, &visit, &rest, request).await
        }
        None => serve::file(&gateway, &visit, request).await,
    }
}

#[derive(Deserialize)]
struct TlsQuery {
    domain: String,
}

async fn health() -> Response {
    let mut response = (StatusCode::OK, "ok").into_response();
    let headers = response.headers_mut();
    policy::secure(headers, Frame::Embed(&[]));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn browser_request(headers: &HeaderMap) -> bool {
    headers.contains_key(ORIGIN)
        || headers.contains_key("sec-fetch-site")
        || headers.contains_key("sec-fetch-mode")
        || headers.contains_key("sec-fetch-dest")
}

/// Whether the browser is fetching a service-worker script for registration or update.
fn service_worker(headers: &HeaderMap) -> bool {
    headers.contains_key("service-worker")
        || headers
            .get("sec-fetch-dest")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|dest| dest.eq_ignore_ascii_case("serviceworker"))
}

/// Lets the TLS proxy issue a certificate on demand only for hosts an applet serves.
async fn tls_check(
    State(gateway): State<Arc<Gateway>>,
    headers: HeaderMap,
    query: Query<TlsQuery>,
) -> Response {
    let none = Frame::Embed(&[]);
    if browser_request(&headers) {
        return bare(StatusCode::NOT_FOUND, none);
    }
    let Some(host) = normalize(&query.domain) else {
        return bare(StatusCode::NOT_FOUND, none);
    };
    let status = match gateway.decide(&host).await {
        Ok(Some(_)) => StatusCode::OK,
        Ok(None) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::BAD_GATEWAY,
    };
    bare(status, none)
}

async fn stopped() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = interrupt => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = interrupt.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = interrupt.await;
    }
}
