//! Tests promoting a server node to a management node that signs tokens while the old one is down.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::shared;

use aruna::identity::IdentityStore;
use aruna_core::onboarding::OnboardingMode;
use aruna_core::structs::identity::auth::NodeCapabilities;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use reqwest::StatusCode;
use shared::{
    TestResult, create_bearer_token, create_onboarding_secret, spawn_keyed_joiner, spawn_low_seed,
};

/// Node key of the joiner; its node id sorts above the low seed's, so it may be promoted.
const JOINER_KEY: [u8; 32] = [21; 32];

#[tokio::test(flavor = "multi_thread")]
async fn promoted_node_signs() -> TestResult<()> {
    let seed = spawn_low_seed().await?;
    let joiner_id = iroh::SecretKey::from_bytes(&JOINER_KEY).public();
    assert!(
        joiner_id > seed.net.node_id(),
        "the seed keeps the database requests"
    );
    let secret = create_onboarding_secret(&seed, OnboardingMode::Server).await?;
    let joiner = spawn_keyed_joiner(&seed, secret, JOINER_KEY).await?;
    assert_eq!(joiner.config.node_id, joiner_id);
    let NodeCapabilities::Management {
        realm_verifying_key,
        ..
    } = seed.capabilities.clone()
    else {
        panic!("the seed is a management node");
    };
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/system/realm/nodes/{joiner_id}/promote",
            seed.base_url
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    // The route answers only once the joiner's own config lists it, so no wait is needed here.
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await?;
    assert_eq!(body["ready"], true);
    let realm_id = seed.realm_id;

    // The harness cannot reopen a joiner; its next start runs exactly this step.
    let store = IdentityStore::from_storage(joiner.context.storage_handle.clone());
    let state = store.load().await?.expect("the joiner has an identity");
    let promoted = store.apply_promotion(state, joiner_id).await?;
    let (promoted_realm, capabilities) = store.capabilities(&promoted)?;
    assert_eq!(promoted_realm, realm_id);
    assert!(matches!(capabilities, NodeCapabilities::Management { .. }));

    let user_id = seed.user_id;
    seed.shutdown().await;
    let token =
        create_bearer_token(joiner.context.as_ref(), user_id, realm_id, capabilities).await?;
    let key = DecodingKey::from_ed_pem(&realm_verifying_key)?;
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.validate_aud = false;
    let claims = decode::<serde_json::Value>(&token, &key, &validation)?.claims;
    assert!(
        claims
            .get("delegation_signature")
            .is_none_or(serde_json::Value::is_null),
        "the promoted node signs with the realm key, not a delegation"
    );
    joiner.shutdown().await;
    Ok(())
}
