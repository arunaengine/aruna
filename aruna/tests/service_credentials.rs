//! Tests year-long S3 credentials that group admins issue for service accounts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::shared;

use aruna_api::routes::credentials::CreateS3Response;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    S3Credentials, TestResult, WAIT_CAP, create_bearer_token, create_group_http,
    create_onboarding_secret, create_s3_credentials, get_group_http, get_user_access, s3_client,
    s3_client_once, shutdown_pair, sign_token, spawn_complete_joiner, spawn_complete_seed,
    wait_group_member, wait_realm_nodes, wait_until,
};
use std::time::{Duration, Instant, SystemTime};
use tokio::time::sleep;

const YEAR_SECS: u64 = 365 * 24 * 60 * 60;

/// Creates a service account of the group and makes it a member with the group's `user` role.
async fn create_account(base_url: &str, token: &str, group_id: &str) -> TestResult<String> {
    let client = reqwest::Client::new();
    let created: Value = client
        .post(format!(
            "{base_url}/api/v1/access/groups/{group_id}/service-accounts"
        ))
        .bearer_auth(token)
        .json(&json!({ "name": "eLabFTW" }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let account = created["id"].as_str().ok_or("no account id")?.to_string();
    client
        .post(format!(
            "{base_url}/api/v1/access/groups/{group_id}/members"
        ))
        .bearer_auth(token)
        .json(&json!({ "user_id": account }))
        .send()
        .await?
        .error_for_status()?;
    Ok(account)
}

async fn issue(
    base_url: &str,
    token: &str,
    group_id: &str,
    account: &str,
    body: &Value,
) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!(
            "{base_url}/api/v1/access/groups/{group_id}/service-accounts/{account}/credentials"
        ))
        .bearer_auth(token)
        .json(body)
        .send()
        .await?)
}

fn as_credentials(created: CreateS3Response) -> S3Credentials {
    S3Credentials {
        access_key_id: created.access_key_id,
        access_secret: created.access_secret,
    }
}

async fn put(client: &S3Client, bucket: &str, key: &str) -> Result<(), String> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"entry"))
        .send()
        .await
        .map(|_| ())
        .map_err(|error| error.code().unwrap_or("unknown").to_string())
}

async fn get(client: &S3Client, bucket: &str, key: &str) -> Result<(), String> {
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map(|_| ())
        .map_err(|error| error.code().unwrap_or("unknown").to_string())
}

async fn audit_actions(base_url: &str, token: &str, group_id: &str) -> TestResult<Vec<Value>> {
    let page: Value = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/access/audit"))
        .query(&[("group_id", group_id), ("limit", "200")])
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["partial"], json!(true));
    Ok(page["records"].as_array().cloned().unwrap_or_default())
}

#[tokio::test]
async fn joiner_issues_credential() -> TestResult<()> {
    // A server node that is no management node issues, audits, enforces and revokes the key.
    let seed = spawn_complete_seed().await?;
    let secret =
        create_onboarding_secret(&seed, aruna_core::onboarding::OnboardingMode::Server).await?;
    let joiner = spawn_complete_joiner(&seed, secret).await?;
    let contexts = [seed.context.as_ref(), joiner.context.as_ref()];
    wait_realm_nodes(&contexts, &seed.realm_id, 2).await?;
    let (context, realm_id) = (seed.context.as_ref(), seed.realm_id);
    let admin =
        create_bearer_token(context, seed.user_id, realm_id, seed.capabilities.clone()).await?;
    let group = create_group_http(&seed.base_url, &admin, "service-credentials").await?;
    let group_id = group.group_id;
    let account = create_account(&seed.base_url, &admin, &group_id).await?;
    wait_group_member(&joiner.base_url, &admin, &group_id).await?;
    let joiner_s3 = joiner.s3.clone().ok_or("joiner has no S3 endpoint")?;
    let admin_keys = create_s3_credentials(&joiner.base_url, &admin, &group_id).await?;
    let admin_s3 = s3_client(&joiner_s3, &admin_keys);
    for bucket in ["shared-bucket", "other-bucket"] {
        admin_s3.create_bucket().bucket(bucket).send().await?;
    }

    // Each attempt signs a fresh short token, so the issuing one expires soon after.
    let body = json!({
        "path_restrictions": [{ "pattern": "shared-bucket/elabftw/**", "permission": "WRITE" }]
    });
    // The joiner refuses until the account and its role reached it.
    let deadline = Instant::now() + WAIT_CAP;
    let (created, short) = loop {
        let short = sign_token(&seed, seed.user_id, None, 15)?;
        let response = issue(&joiner.base_url, &short, &group_id, &account, &body).await?;
        if response.status() == StatusCode::CREATED {
            break (response.json::<CreateS3Response>().await?, short);
        }
        if Instant::now() >= deadline {
            return Err(format!("no credential: {}", response.status()).into());
        }
        sleep(Duration::from_millis(200)).await;
    };
    let access_key = created.access_key_id.clone();
    let stored = get_user_access(joiner.context.as_ref(), &access_key).await?;
    assert_eq!(stored.user_identity.to_string(), account);
    assert_eq!(stored.issued_by, *joiner.config.node_id.as_bytes());
    let floor = SystemTime::now() + Duration::from_secs(YEAR_SECS - 3_600);
    assert!(stored.expiry > floor, "the default lifetime is one year");

    // The audit row stays on the issuing node.
    let rows = audit_actions(&joiner.base_url, &admin, &group_id).await?;
    let row = rows
        .iter()
        .find(|row| row["action"] == "service_credential_issued")
        .ok_or("no audit row on the joiner")?;
    assert_eq!(row["target"], json!(account));
    assert_eq!(row["session_id"], json!(access_key));
    let seed_rows = audit_actions(&seed.base_url, &admin, &group_id).await?;
    assert!(
        seed_rows
            .iter()
            .all(|row| row["action"] != "service_credential_issued")
    );

    let group_path = &format!("{}/api/v1/access/groups/{group_id}", joiner.base_url);
    let short = &short;
    wait_until(
        "the issuing token to expire",
        WAIT_CAP,
        Duration::from_millis(500),
        || async move {
            let response = reqwest::Client::new()
                .get(group_path)
                .bearer_auth(short)
                .send()
                .await;
            response.is_ok_and(|response| response.status() == StatusCode::UNAUTHORIZED)
        },
    )
    .await?;

    let keys = as_credentials(created);
    let service = s3_client_once(&joiner_s3, &keys);
    put(&service, "shared-bucket", "elabftw/entry.txt").await?;
    let denied = Err("AccessDenied".to_string());
    assert_eq!(
        put(&service, "shared-bucket", "other/entry.txt").await,
        denied
    );
    assert_eq!(
        put(&service, "other-bucket", "elabftw/entry.txt").await,
        denied
    );
    let seed_s3 = seed.s3.clone().ok_or("seed has no S3 endpoint")?;
    let elsewhere = put(
        &s3_client_once(&seed_s3, &keys),
        "shared-bucket",
        "elabftw/a",
    )
    .await;
    assert_eq!(elsewhere, Err("InvalidAccessKeyId".to_string()));

    let response = reqwest::Client::new()
        .delete(format!(
            "{}/api/v1/access/groups/{group_id}/tokens/s3_credential/{access_key}",
            joiner.base_url
        ))
        .bearer_auth(&admin)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let revoked = put(&service, "shared-bucket", "elabftw/late.txt").await;
    assert_eq!(revoked, Err("InvalidAccessKeyId".to_string()));

    shutdown_pair(joiner, seed).await;
    Ok(())
}

#[tokio::test]
async fn account_changes_apply() -> TestResult<()> {
    // A role reduction narrows the key; deactivating the account ends it.
    let seed = spawn_complete_seed().await?;
    let (context, realm_id) = (seed.context.as_ref(), seed.realm_id);
    let admin =
        create_bearer_token(context, seed.user_id, realm_id, seed.capabilities.clone()).await?;
    let group = create_group_http(&seed.base_url, &admin, "service-changes").await?;
    let group_id = group.group_id;
    let account = create_account(&seed.base_url, &admin, &group_id).await?;
    let seed_s3 = seed.s3.clone().ok_or("seed has no S3 endpoint")?;
    let admin_keys = create_s3_credentials(&seed.base_url, &admin, &group_id).await?;
    s3_client(&seed_s3, &admin_keys)
        .create_bucket()
        .bucket("shared-bucket")
        .send()
        .await?;

    let body = json!({ "expires_in_seconds": YEAR_SECS });
    let response = issue(&seed.base_url, &admin, &group_id, &account, &body).await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let keys = as_credentials(response.json().await?);
    let service = s3_client_once(&seed_s3, &keys);
    put(&service, "shared-bucket", "entry.txt").await?;

    let roles = get_group_http(&seed.base_url, &admin, &group_id)
        .await?
        .roles;
    let role_id = |name: &str| {
        roles
            .iter()
            .find(|role| role.name == name)
            .map(|role| role.role_id.clone())
            .ok_or(format!("no {name} role"))
    };
    let (user, viewer) = (role_id("user")?, role_id("viewer")?);
    let members = format!("{}/api/v1/access/groups/{group_id}/members", seed.base_url);
    let client = reqwest::Client::new();
    let body = json!({ "user_id": account, "role_ids": [viewer] });
    let added = client.post(&members).bearer_auth(&admin).json(&body).send();
    added.await?.error_for_status()?;
    let removed = client
        .delete(format!("{members}/{account}"))
        .query(&[("role_id", user)])
        .bearer_auth(&admin)
        .send();
    removed.await?.error_for_status()?;
    let service = &service;
    wait_until(
        "the role reduction to narrow the key",
        WAIT_CAP,
        Duration::from_millis(200),
        || async move { put(service, "shared-bucket", "late.txt").await.is_err() },
    )
    .await?;
    let denied = put(service, "shared-bucket", "late.txt").await;
    assert_eq!(denied, Err("AccessDenied".to_string()));
    get(service, "shared-bucket", "entry.txt").await?;

    let status = format!("{}/api/v1/access/users/{account}/status", seed.base_url);
    let deactivated = client
        .put(&status)
        .bearer_auth(&admin)
        .json(&json!({ "active": false }));
    assert_eq!(deactivated.send().await?.status(), StatusCode::NO_CONTENT);
    wait_until(
        "deactivation to end the key",
        WAIT_CAP,
        Duration::from_millis(200),
        || async move { get(service, "shared-bucket", "entry.txt").await.is_err() },
    )
    .await?;
    // The refused request deleted the key, so it is now unknown.
    let ended = get(service, "shared-bucket", "entry.txt").await;
    assert_eq!(ended, Err("InvalidAccessKeyId".to_string()));

    seed.shutdown().await;
    Ok(())
}
