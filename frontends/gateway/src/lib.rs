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
use axum::http::header::HOST;
use axum::response::Response;
use axum::routing::get;
use serde::Deserialize;
use tracing::{info, warn};

use crate::hosts::{HostCache, normalize};
use crate::pages::{page, unavailable};
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
        .route("/_edge/health", get(|| async { "ok" }))
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

/// Every applet request: find the host's decision, then serve `/_aruna/` or a pinned file.
async fn entry(State(gateway): State<Arc<Gateway>>, request: Request) -> Response {
    let none = Frame::Embed(&[]);
    let named = (request.headers().get(HOST))
        .and_then(|host| host.to_str().ok())
        .or(request.uri().host());
    let Some(host) = named.and_then(normalize) else {
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

/// Lets the TLS proxy issue a certificate on demand only for hosts an applet serves.
async fn tls_check(State(gateway): State<Arc<Gateway>>, query: Query<TlsQuery>) -> StatusCode {
    let Some(host) = normalize(&query.domain) else {
        return StatusCode::NOT_FOUND;
    };
    match gateway.decide(&host).await {
        Ok(Some(_)) => StatusCode::OK,
        Ok(None) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::BAD_GATEWAY,
    }
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
