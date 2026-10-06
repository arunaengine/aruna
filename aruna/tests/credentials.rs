//! Tests scoped S3 credentials: inherited, narrowed, read-only and rejected path scopes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]

mod shared;

use aruna_api::routes::credentials::CreatePathRestriction;
use aruna_core::structs::identity::auth::{PathRestriction, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aws_sdk_s3::error::ProvideErrorMetadata;
use reqwest::StatusCode;
use shared::{
    TestResult, create_bearer_token, create_group_http, create_s3_credentials, get_user_access,
    request_credentials, s3_client, sign_scoped_token, spawn_complete_seed, spawn_seed_node,
    wait_group_http,
};

fn create_request_restriction(pattern: String, permission: Permission) -> CreatePathRestriction {
    CreatePathRestriction {
        pattern,
        permission: permission.to_string(),
    }
}

async fn post_credentials(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
    path_restrictions: Option<Vec<CreatePathRestriction>>,
) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!("{base_url}/api/v1/access/credentials"))
        .bearer_auth(bearer_token)
        .json(&aruna_api::routes::credentials::CreateS3Request {
            group_id: group_id.to_string(),
            expires_in_seconds: Some(600),
            path_restrictions,
            encrypted_buckets: None,
        })
        .send()
        .await?)
}

fn service_error_code<T, E>(result: &Result<T, aws_sdk_s3::error::SdkError<E>>) -> Option<String>
where
    E: ProvideErrorMetadata,
{
    result
        .as_ref()
        .err()
        .and_then(|err| err.as_service_error().and_then(|inner| inner.code()))
        .map(ToOwned::to_owned)
}

#[tokio::test]
async fn scoped_auth_inherits() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, "credentials-scope-a").await?;
    let group_root =
        group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let delegated_path = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: delegated_path.clone(),
            permission: Permission::WRITE,
        }],
    )?;

    wait_group_http(&seed.base_url, &admin_token, &group.group_id).await?;

    let credentials =
        request_credentials(&seed.base_url, &scoped_token, &group.group_id, None).await?;
    let access = get_user_access(seed.context.as_ref(), &credentials.access_key_id).await?;

    assert_eq!(
        access.path_restrictions,
        Some(vec![PathRestriction {
            pattern: delegated_path,
            permission: Permission::WRITE,
        }])
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn narrower_scope_stored() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, "credentials-scope-b").await?;
    let group_root =
        group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/**");
    let request_scope = format!("{group_root}/folder/narrow/**");
    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope,
            permission: Permission::WRITE,
        }],
    )?;

    wait_group_http(&seed.base_url, &admin_token, &group.group_id).await?;

    let credentials = request_credentials(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            request_scope.clone(),
            Permission::WRITE,
        )]),
    )
    .await?;
    let access = get_user_access(seed.context.as_ref(), &credentials.access_key_id).await?;

    assert_eq!(
        access.path_restrictions,
        Some(vec![PathRestriction {
            pattern: request_scope,
            permission: Permission::WRITE,
        }])
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn broader_scope_rejected() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, "credentials-scope-c").await?;
    let group_root =
        group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/narrow/**");
    let requested_scope = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope,
            permission: Permission::WRITE,
        }],
    )?;

    let response = post_credentials(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            requested_scope,
            Permission::WRITE,
        )]),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    seed.shutdown().await;
    Ok(())
}

// A read-only request narrows a write scope: the credential is issued as READ.
#[tokio::test]
async fn readonly_scope_stored() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, "credentials-scope-d").await?;
    let group_root =
        group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope.clone(),
            permission: Permission::WRITE,
        }],
    )?;

    wait_group_http(&seed.base_url, &admin_token, &group.group_id).await?;

    let credentials = request_credentials(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            auth_scope.clone(),
            Permission::READ,
        )]),
    )
    .await?;
    let access = get_user_access(seed.context.as_ref(), &credentials.access_key_id).await?;

    assert_eq!(
        access.path_restrictions,
        Some(vec![PathRestriction {
            pattern: auth_scope,
            permission: Permission::READ,
        }])
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn foreign_group_rejected() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group_a = create_group_http(&seed.base_url, &admin_token, "credentials-scope-e-a").await?;
    let group_b = create_group_http(&seed.base_url, &admin_token, "credentials-scope-e-b").await?;
    let group_a_root =
        group_permission_path(seed.realm_id, group_a.group_id.parse()?, seed.net.node_id());
    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: format!("{group_a_root}/folder/**"),
            permission: Permission::WRITE,
        }],
    )?;

    let response = post_credentials(&seed.base_url, &scoped_token, &group_b.group_id, None).await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn scoped_list_denies() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &admin_token, "credentials-scope-list").await?;
    let group_root =
        group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let bootstrap_credentials =
        create_s3_credentials(&seed.base_url, &admin_token, &group.group_id).await?;
    let s3_endpoint = seed
        .s3
        .as_ref()
        .ok_or_else(|| std::io::Error::other("missing s3 endpoint"))?;
    let bootstrap_client = s3_client(s3_endpoint, &bootstrap_credentials);

    bootstrap_client
        .create_bucket()
        .bucket("allowed")
        .send()
        .await?;
    bootstrap_client
        .create_bucket()
        .bucket("blocked")
        .send()
        .await?;

    let scoped_token = sign_scoped_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: format!("{group_root}/allowed/**"),
            permission: Permission::WRITE,
        }],
    )?;
    wait_group_http(&seed.base_url, &admin_token, &group.group_id).await?;

    let credentials =
        request_credentials(&seed.base_url, &scoped_token, &group.group_id, None).await?;
    let client = s3_client(s3_endpoint, &credentials);

    let blocked_list = client.list_objects_v2().bucket("blocked").send().await;
    assert_eq!(
        service_error_code(&blocked_list).as_deref(),
        Some("AccessDenied")
    );

    seed.shutdown().await;
    Ok(())
}

/// An S3 client of `credentials`, sending `token` as `aws_session_token`; no SDK retries.
fn token_client(
    endpoint: &shared::S3Endpoint,
    credentials: &shared::S3Credentials,
    token: Option<String>,
) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let credentials = Credentials::new(
        credentials.access_key_id.clone(),
        credentials.access_secret.clone(),
        token,
        None,
        "aruna-token-test",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(shared::AWS_REGION))
        .credentials_provider(credentials)
        .endpoint_url(endpoint.endpoint_url.clone())
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[tokio::test]
async fn token_reads_locked() -> TestResult<()> {
    const BUCKET: &str = "token-reads";
    const DATA: &[u8] = b"content of a locked bucket";
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "token-reads-group").await?;
        let plain = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let client = s3_client(endpoint, &plain);
        client.create_bucket().bucket(BUCKET).send().await?;
        let http = reqwest::Client::new();
        let encryption = format!(
            "{}/api/v1/data/buckets/{BUCKET}/storage/encryption",
            seed.base_url
        );
        let enabled = http
            .put(&encryption)
            .bearer_auth(&admin)
            .json(&serde_json::json!({ "mode": "node_managed", "expected_generation": 0 }))
            .send()
            .await?;
        let status = enabled.status();
        assert_eq!(status, StatusCode::OK, "{}", enabled.text().await?);
        client
            .put_object()
            .bucket(BUCKET)
            .key("locked.txt")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(DATA))
            .send()
            .await?;

        // The bucket creator takes a token credential while the bucket is unlocked.
        let created = http
            .post(format!("{}/api/v1/access/credentials", seed.base_url))
            .bearer_auth(&admin)
            .json(&aruna_api::routes::credentials::CreateS3Request {
                group_id: group.group_id.clone(),
                expires_in_seconds: Some(600),
                path_restrictions: None,
                encrypted_buckets: Some(vec![BUCKET.to_string()]),
            })
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;
        let token = created
            .session_token
            .ok_or_else(|| std::io::Error::other("no session token returned"))?;
        let credentials = shared::S3Credentials {
            access_key_id: created.access_key_id,
            access_secret: created.access_secret,
        };
        let locked = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(&admin)
            .send()
            .await?;
        assert_eq!(locked.status(), StatusCode::OK);

        // With `aws_session_token` the locked bucket reads; without it the key alone is refused.
        let object = token_client(endpoint, &credentials, Some(token.clone()))
            .get_object()
            .bucket(BUCKET)
            .key("locked.txt")
            .send()
            .await?;
        assert_eq!(&object.body.collect().await?.into_bytes()[..], DATA);
        let refused = token_client(endpoint, &credentials, None)
            .get_object()
            .bucket(BUCKET)
            .key("locked.txt")
            .send()
            .await;
        assert_eq!(
            service_error_code(&refused).as_deref(),
            Some("AccessDenied")
        );
        // Another token of the right length opens nothing.
        let wrong = "0".repeat(token.len());
        let refused = token_client(endpoint, &credentials, Some(wrong))
            .get_object()
            .bucket(BUCKET)
            .key("locked.txt")
            .send()
            .await;
        assert_eq!(
            service_error_code(&refused).as_deref(),
            Some("InvalidToken")
        );
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
