//! Serves an OIDC discovery document and a JWKS for one Ed25519 key on a loopback port.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::OidcProviderConfig;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use base64::Engine;
use ed25519_dalek::SigningKey;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Clone)]
struct OidcProviderState {
    issuer: String,
    jwks_uri: String,
    jwks: serde_json::Value,
}

async fn oidc_discovery(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "issuer": state.issuer,
        "jwks_uri": state.jwks_uri,
    }))
}

async fn oidc_jwks(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
    Json(state.jwks)
}

/// Starts the provider for `issuer` with the key `kid`; returns its realm config and task.
pub(crate) async fn spawn_oidc_provider(
    issuer: &str,
    kid: &str,
    signing_key: &SigningKey,
) -> (OidcProviderConfig, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let jwks_uri = format!("http://{addr}/jwks.json");
    let discovery_url = format!("http://{addr}/.well-known/openid-configuration");
    let jwks = serde_json::json!({
        "keys": [{
            "kty": "OKP",
            "alg": "EdDSA",
            "use": "sig",
            "kid": kid,
            "crv": "Ed25519",
            "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
        }]
    });
    let router = Router::new()
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route("/jwks.json", get(oidc_jwks))
        .with_state(OidcProviderState {
            issuer: issuer.to_string(),
            jwks_uri: jwks_uri.clone(),
            jwks,
        });
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    (
        OidcProviderConfig {
            id: "main".to_string(),
            issuer: issuer.to_string(),
            audience: "aruna-api".to_string(),
            discovery_url,
        },
        task,
    )
}
