//! Tests the wait before registering a matching sign-in, attaching sign-ins and account merges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    RegisterUserRequest, RegisterUserResponse, TestNode, TestOidcClaims, register_via_oidc,
    sign_claims, spawn_oidc_provider, spawn_test_node,
};
use crate::routes::access::users::{GetTokenResponse, GetUserResponse};
use aruna_core::keys::generate_signing_key;
use axum::http::StatusCode;
use ed25519_dalek::SigningKey;
use serde_json::{Value, json};

const ISSUER: &str = "https://issuer.example";
const KID: &str = "main-key";

fn now() -> u64 {
    super::super::now_timestamp()
}

fn login_token(key: &SigningKey, subject: &str, name: &str, email: Option<(&str, bool)>) -> String {
    let claims = TestOidcClaims {
        sub: subject.to_string(),
        iss: ISSUER.to_string(),
        aud: "aruna-api".to_string(),
        exp: now() + 600,
        name: Some(name.to_string()),
        auth_time: Some(now()),
        email: email.map(|(email, _)| email.to_string()),
        email_verified: email.map(|(_, verified)| verified),
    };
    sign_claims(KID, key, &claims)
}

async fn register(node: &TestNode, oidc: &str, create_new: bool) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/access/users/register", node.base_url))
        .bearer_auth(oidc)
        .json(&RegisterUserRequest {
            onboarding_secret: None,
            create_new,
        })
        .send()
        .await
        .unwrap();
    (
        response.status(),
        response.json().await.unwrap_or(Value::Null),
    )
}

async fn realm_token(node: &TestNode, oidc: &str) -> Option<String> {
    let response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/token", node.base_url))
        .bearer_auth(oidc)
        .send()
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        return None;
    }
    Some(response.json::<GetTokenResponse>().await.unwrap().token)
}

async fn me(node: &TestNode, token: &str) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .get(format!("{}/api/v1/access/users/me", node.base_url))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    (
        response.status(),
        response.json().await.unwrap_or(Value::Null),
    )
}

async fn post(node: &TestNode, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1{path}", node.base_url))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap();
    (
        response.status(),
        response.json().await.unwrap_or(Value::Null),
    )
}

async fn stop(node: TestNode, oidc_task: tokio::task::JoinHandle<()>) {
    node.server_task.abort();
    node.net.shutdown().await;
    oidc_task.abort();
}

#[tokio::test]
async fn registration_waits_for_choice() {
    let key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(ISSUER, KID, &key).await;
    let node = spawn_test_node(provider, true).await;
    let verified = Some(("alice@example.org", true));
    let first = login_token(&key, "subject-1", "Alice", verified);
    let (status, alice) = register(&node, &first, false).await;
    assert_eq!(status, StatusCode::CREATED);

    // A new sign-in with the same verified email waits and creates nothing.
    let second = login_token(
        &key,
        "subject-2",
        "A. Example",
        Some(("ALICE@example.org", true)),
    );
    let (status, body) = register(&node, &second, false).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "account_match");
    assert!(!body.to_string().contains(alice["id"].as_str().unwrap()));
    assert!(realm_token(&node, &second).await.is_none());

    // The same name without a verified email gets no hint and registers.
    let unverified = login_token(
        &key,
        "subject-3",
        "Alice",
        Some(("alice@example.org", false)),
    );
    assert_eq!(
        register(&node, &unverified, false).await.0,
        StatusCode::CREATED
    );

    // Choosing a new account creates one.
    let (status, created) = register(&node, &second, true).await;
    assert_eq!(status, StatusCode::CREATED);
    let created: RegisterUserResponse = serde_json::from_value(created).unwrap();
    assert_ne!(created.id, alice["id"].as_str().unwrap());

    stop(node, oidc_task).await;
}

#[tokio::test]
async fn attach_links_subject() {
    let key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(ISSUER, KID, &key).await;
    let node = spawn_test_node(provider, true).await;
    let verified = Some(("alice@example.org", true));
    let first = login_token(&key, "subject-1", "Alice", verified);
    let (status, alice) = register(&node, &first, false).await;
    assert_eq!(status, StatusCode::CREATED);
    let alice_token = realm_token(&node, &first).await.unwrap();
    let second = login_token(&key, "subject-2", "Alice E.", verified);
    assert_eq!(
        register(&node, &second, false).await.0,
        StatusCode::CONFLICT
    );

    // "This is me": the existing account attaches the new sign-in.
    let body = json!({ "token": second });
    let (status, user) = post(
        &node,
        "/access/users/me/subjects",
        &alice_token,
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let user: GetUserResponse = serde_json::from_value(user).unwrap();
    assert_eq!(user.subject_ids.len(), 2);
    let (status, _) = post(&node, "/access/users/me/subjects", &alice_token, body).await;
    assert_eq!(status, StatusCode::OK);

    // The attached sign-in now opens the existing account.
    let attached = realm_token(&node, &second).await.unwrap();
    let (_, profile) = me(&node, &attached).await;
    assert_eq!(profile["user"]["user_id"], alice["id"]);

    // A sign-in of another account is refused; that needs a merge.
    let bob = login_token(&key, "subject-3", "Bob", None);
    assert_eq!(register(&node, &bob, false).await.0, StatusCode::CREATED);
    let bob_again = login_token(&key, "subject-3", "Bob", None);
    let (status, body) = post(
        &node,
        "/access/users/me/subjects",
        &alice_token,
        json!({ "token": bob_again }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "subject_taken");

    stop(node, oidc_task).await;
}

#[tokio::test]
async fn merge_moves_signin() {
    let key = generate_signing_key();
    let (provider, oidc_task) = spawn_oidc_provider(ISSUER, KID, &key).await;
    let node = spawn_test_node(provider, true).await;
    let (alice, alice_token) =
        register_via_oidc(&node, ISSUER, KID, &key, "subject-a", "Alice", None).await;
    let (bob, bob_token) =
        register_via_oidc(&node, ISSUER, KID, &key, "subject-b", "Bob", None).await;
    let merge = "/access/users/me/merge";

    // Refused: the same account, and a token that is no token.
    let (status, body) = post(&node, merge, &alice_token, json!({ "token": alice_token })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "merge_refused");
    let (status, _) = post(
        &node,
        merge,
        &alice_token,
        json!({ "token": "not-a-token" }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, outcome) = post(&node, merge, &alice_token, json!({ "token": bob_token })).await;
    assert_eq!(status, StatusCode::OK, "{outcome}");
    assert_eq!(outcome["merged_user_id"], bob.id.as_str());
    assert_eq!(outcome["subjects"], 1);
    assert_eq!(outcome["repeated"], false);

    // Bob's sessions end, and his next sign-in opens Alice's account.
    assert_eq!(me(&node, &bob_token).await.0, StatusCode::UNAUTHORIZED);
    let bob_login = super::sign_oidc_token(ISSUER, KID, &key, "subject-b", Some("Bob"), None);
    let landed = realm_token(&node, &bob_login).await.unwrap();
    let (_, profile) = me(&node, &landed).await;
    assert_eq!(profile["user"]["user_id"], alice.id.as_str());
    assert_eq!(profile["merged_user_ids"], json!([bob.id]));

    // A retry with the cut-off token repeats the finished merge.
    let (status, outcome) = post(&node, merge, &alice_token, json!({ "token": bob_token })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(outcome["repeated"], true);

    // History naming Bob resolves to Alice.
    let (status, resolved) = post(
        &node,
        "/access/users/resolve",
        &alice_token,
        json!({ "user_ids": [bob.id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resolved[0]["name"], "Alice");
    assert_eq!(resolved[0]["merged_into"], alice.id.as_str());

    stop(node, oidc_task).await;
}
