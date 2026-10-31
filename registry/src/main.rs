//! Runs the registry: reads its environment configuration, opens its store and serves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::sync::Arc;

use aruna_blob::egress::EgressGuard;
use aruna_core::egress::EgressPolicy;
use aruna_core::structs::identity::realm::RealmId;
use aruna_registry::admin_api::AdminAuth;
use aruna_registry::resolver::Resolver;
use aruna_registry::store::Store;
use aruna_registry::{RegistryState, resolver, router};
use tracing::info;

const LISTEN_VAR: &str = "ARUNA_REGISTRY_LISTEN";
const DATA_VAR: &str = "ARUNA_REGISTRY_DATA";
/// Comma-separated realm ids the resolver may send w3id paths to.
const REALMS_VAR: &str = "ARUNA_RESOLVER_REALMS";
/// The realm of ids that name none, such as content hashes; the first approved one if unset.
const DEFAULT_REALM_VAR: &str = "ARUNA_RESOLVER_DEFAULT_REALM";

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
    let approved = std::env::var(REALMS_VAR)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(RealmId::from_base64)
        .collect::<Result<Vec<_>, _>>()?;
    let default_realm = match std::env::var(DEFAULT_REALM_VAR) {
        Ok(value) => Some(RealmId::from_base64(value.trim())?),
        Err(_) => approved.first().copied(),
    };
    let resolver = Arc::new(Resolver::new(state.clone(), approved, default_realm));
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(listen = %listen, data = %data.display(), admin_enabled, "Registry listening");
    axum::serve(listener, router(state).merge(resolver::router(resolver)))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
