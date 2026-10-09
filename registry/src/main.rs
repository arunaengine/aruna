//! Runs the registry: reads its environment configuration, opens its store and serves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::sync::Arc;

use aruna_blob::egress::EgressGuard;
use aruna_core::egress::EgressPolicy;
use aruna_registry::store::Store;
use aruna_registry::{RegistryState, router};
use tracing::info;

const LISTEN_VAR: &str = "ARUNA_REGISTRY_LISTEN";
const DATA_VAR: &str = "ARUNA_REGISTRY_DATA";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let listen = std::env::var(LISTEN_VAR).unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let data = PathBuf::from(std::env::var(DATA_VAR).unwrap_or_else(|_| "data".to_string()));
    let state = Arc::new(RegistryState::new(
        Store::open(&data)?,
        EgressGuard::new(EgressPolicy::strict())?,
    ));
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(listen = %listen, data = %data.display(), "Registry listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
