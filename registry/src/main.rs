//! Runs the registry: reads its environment configuration, opens its store and serves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use aruna_blob::egress::EgressGuard;
use aruna_core::egress::EgressPolicy;
use aruna_core::structs::identity::realm::RealmId;
use aruna_registry::admin_api::AdminAuth;
use aruna_registry::resolver::{ClientSource, Resolver, parse_proxies};
use aruna_registry::store::Store;
use aruna_registry::{RegistryState, resolver, router};
use tracing::info;

const LISTEN_VAR: &str = "ARUNA_REGISTRY_LISTEN";
const DATA_VAR: &str = "ARUNA_REGISTRY_DATA";
/// The realm of ids that name none, such as content hashes; such ids answer 404 if unset.
const DEFAULT_REALM_VAR: &str = "ARUNA_RESOLVER_DEFAULT_REALM";
/// The header the ingress sets to the client address, for example `x-real-ip`.
const CLIENT_HEADER_VAR: &str = "ARUNA_RESOLVER_CLIENT_HEADER";
/// The proxy addresses or ranges whose client header is honoured, comma-separated.
const TRUSTED_PROXIES_VAR: &str = "ARUNA_RESOLVER_TRUSTED_PROXIES";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let listen = std::env::var(LISTEN_VAR).unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let data = PathBuf::from(std::env::var(DATA_VAR).unwrap_or_else(|_| "data".to_string()));
    let admin_auth = AdminAuth::from_env()?;
    let admin_enabled = admin_auth.enabled();
    let state = Arc::new(
        RegistryState::new(
            Store::open(&data)?,
            EgressGuard::new(EgressPolicy::strict())?,
        )
        .with_admin(admin_auth),
    );
    let default_realm = match std::env::var(DEFAULT_REALM_VAR) {
        Ok(value) => Some(RealmId::from_base64(value.trim())?),
        Err(_) => None,
    };
    let client_source = ClientSource {
        header: match std::env::var(CLIENT_HEADER_VAR) {
            Ok(value) => Some(axum::http::HeaderName::try_from(value.trim())?),
            Err(_) => None,
        },
        trusted_proxies: parse_proxies(&std::env::var(TRUSTED_PROXIES_VAR).unwrap_or_default())?,
    };
    let resolver = Arc::new(Resolver::new(state.clone(), default_realm, client_source));
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(listen = %listen, data = %data.display(), admin_enabled, "Registry listening");
    let app = router(state).merge(resolver::router(resolver));
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
