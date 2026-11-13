//! Tests S3 credentials: path scopes, token credentials and lifetimes beyond the issuing token.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::shared;

use aruna_api::routes::credentials::CreatePathRestriction;
use aruna_core::auth::{MAX_TOKEN_LIFETIME, REVOCATION_GRACE_SECS};
use aruna_core::structs::identity::auth::{PathRestriction, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::structs::storage::blob::never_expiry;
use aruna_operations::driver::drive;
use aruna_operations::s3::access::cutoff::{CutoffSweepConfig, CutoffSweepOperation};
use aws_sdk_s3::error::ProvideErrorMetadata;
use base64::engine::general_purpose::STANDARD;
use reqwest::StatusCode;
use shared::{
    S3Credentials, TestResult, WAIT_CAP, create_group_http, create_s3_credentials, get_user_access,
    request_credentials, revoke_s3_credentials, s3_client, s3_client_once, sign_scoped_token,
    sign_token, spawn_complete_seed, spawn_seed_node, wait_group_http, wait_until,
};
use std::time::{Duration, SystemTime};

const MONTH_SECS: u64 = 30 * 24 * 60 * 60;

async fn create_bearer_token(
    context: &aruna_operations::driver::DriverContext,
    user_id: aruna_core::UserId,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    capabilities: aruna_core::structs::identity::auth::NodeCapabilities,
) -> TestResult<String> {
    shared::add_user(context, user_id).await?;
    shared::create_bearer_token(context, user_id, realm_id, capabilities).await
}

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
            token_public_key: None,
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

/// Requests a credential through `token`, with `seconds` of lifetime or none.
async fn post_lifetime(
    base_url: &str,
    token: &str,
    group_id: &str,
    seconds: Option<u64>,
) -> TestResult<S3Credentials> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/access/credentials"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "group_id": group_id, "expires_in_seconds": seconds }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: aruna_api::routes::credentials::CreateS3Response = response.json().await?;
    Ok(S3Credentials {
        access_key_id: created.access_key_id,
        access_secret: created.access_secret,
    })
}

/// `None` for a stored object, the S3 error code for a refusal; any other failure is an error.
async fn put_code(client: &aws_sdk_s3::Client, key: &str) -> TestResult<Option<String>> {
    let body = aws_sdk_s3::primitives::ByteStream::from_static(b"entry");
    let put = client.put_object().bucket("lasting").key(key).body(body);
    let result = put.send().await;
    match service_error_code(&result) {
        Some(code) => Ok(Some(code)),
        None => result.map(|_| None).map_err(Into::into),
    }
}

/// A seed whose admin created a group with the bucket `lasting`, and the admin's token.
async fn lasting_seed(name: &str) -> TestResult<(shared::SeedNode, String, String)> {
    let seed = spawn_complete_seed().await?;
    let (context, realm_id) = (seed.context.as_ref(), seed.realm_id);
    let admin =
        create_bearer_token(context, seed.user_id, realm_id, seed.capabilities.clone()).await?;
    let group = create_group_http(&seed.base_url, &admin, name).await?;
    let s3 = seed.s3.clone().ok_or("missing s3 endpoint")?;
    let bootstrap = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
    let client = s3_client(&s3, &bootstrap);
    client.create_bucket().bucket("lasting").send().await?;
    Ok((seed, admin, group.group_id))
}

#[tokio::test]
async fn credential_outlives_session() -> TestResult<()> {
    // The credential keeps its requested lifetime, or none; revocation still ends it.
    let (seed, admin, group_id) = lasting_seed("credentials-lifetime").await?;
    let context = seed.context.as_ref();
    let s3 = seed.s3.clone().ok_or("missing s3 endpoint")?;

    let day = sign_token(&seed, seed.user_id, None, 24 * 60 * 60)?;
    let issued = post_lifetime(&seed.base_url, &day, &group_id, Some(MONTH_SECS)).await?;
    let stored = get_user_access(context, &issued.access_key_id).await?;
    let floor = SystemTime::now() + Duration::from_secs(MONTH_SECS - 3_600);
    assert!(stored.expiry > floor, "a 24 hour session keeps the 30 days");

    let short = sign_token(&seed, seed.user_id, None, 15)?;
    let keys = post_lifetime(&seed.base_url, &short, &group_id, None).await?;
    let stored = get_user_access(context, &keys.access_key_id).await?;
    assert_eq!(stored.expiry, never_expiry(), "no lifetime means no expiry");
    let group_path = &format!("{}/api/v1/access/groups/{group_id}", seed.base_url);
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
    let client = s3_client_once(&s3, &keys);
    assert_eq!(put_code(&client, "after-session.txt").await?, None);

    revoke_s3_credentials(&seed.base_url, &admin, &keys.access_key_id).await?;
    let revoked = put_code(&client, "after-revoke.txt").await?;
    assert_eq!(revoked.as_deref(), Some("InvalidAccessKeyId"));

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn restricted_token_caps() -> TestResult<()> {
    // A path-restricted token's credential ends no later than that token, asked or not.
    let (seed, admin, group_id) = lasting_seed("credentials-restricted").await?;
    let root = group_permission_path(seed.realm_id, group_id.parse()?, seed.net.node_id());
    let scope = PathRestriction {
        pattern: format!("{root}/**"),
        permission: Permission::WRITE,
    };
    let scoped = sign_scoped_token(&seed, seed.user_id, vec![scope])?;
    wait_group_http(&seed.base_url, &admin, &group_id).await?;
    let token_end = SystemTime::now() + Duration::from_secs(600);
    for seconds in [Some(MONTH_SECS), None] {
        let keys = post_lifetime(&seed.base_url, &scoped, &group_id, seconds).await?;
        let stored = get_user_access(seed.context.as_ref(), &keys.access_key_id).await?;
        assert!(
            stored.expiry <= token_end,
            "capped at the token for {seconds:?}"
        );
    }

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn deactivation_ends_credential() -> TestResult<()> {
    let (seed, admin, group_id) = lasting_seed("credentials-deactivate").await?;
    let (context, realm_id) = (seed.context.as_ref(), seed.realm_id);
    let s3 = seed.s3.clone().ok_or("missing s3 endpoint")?;
    let member = aruna_core::UserId::local(ulid::Ulid::generate(), realm_id);
    let member_token =
        create_bearer_token(context, member, realm_id, seed.capabilities.clone()).await?;
    reqwest::Client::new()
        .post(format!(
            "{}/api/v1/access/groups/{group_id}/members",
            seed.base_url
        ))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "user_id": member.to_string() }))
        .send()
        .await?
        .error_for_status()?;
    let used = post_lifetime(&seed.base_url, &member_token, &group_id, None).await?;
    let unused = post_lifetime(&seed.base_url, &member_token, &group_id, None).await?;
    let client = s3_client_once(&s3, &used);
    assert_eq!(put_code(&client, "member.txt").await?, None);

    let status = reqwest::Client::new()
        .put(format!(
            "{}/api/v1/access/users/{member}/status",
            seed.base_url
        ))
        .bearer_auth(&admin)
        .json(&serde_json::json!({ "active": false }))
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::NO_CONTENT);

    // Judged past the 366 day window the cutoff is gone, so a sweep then finds nothing; the
    // keys must be deleted before. A refused request deletes its key at once.
    let now = aruna_core::time::unix_timestamp_secs();
    let later = now + MAX_TOKEN_LIFETIME + REVOCATION_GRACE_SECS + 1;
    let node_id = seed.net.node_id();
    let sweep = |now| {
        CutoffSweepOperation::new(CutoffSweepConfig {
            realm_id,
            node_id,
            now,
        })
    };
    assert_eq!(drive(sweep(later), context).await?, 0);
    assert_eq!(
        put_code(&client, "late.txt").await?.as_deref(),
        Some("AccessDenied")
    );
    assert!(get_user_access(context, &used.access_key_id).await.is_err());

    // Inside the window the sweep deletes the key no request has refused yet.
    assert_eq!(drive(sweep(now), context).await?, 1);
    assert!(
        get_user_access(context, &unused.access_key_id)
            .await
            .is_err()
    );
    let unused = s3_client_once(&s3, &unused);
    assert_eq!(
        put_code(&unused, "late.txt").await?.as_deref(),
        Some("InvalidAccessKeyId")
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

        // The client keeps the token key; the node issues its grant at once in node managed mode.
        let (public, private) = aruna_core::structs::storage::encryption::generate_key()?;
        let token = hex::encode(private.bytes().expose());
        let created = http
            .post(format!("{}/api/v1/access/credentials", seed.base_url))
            .bearer_auth(&admin)
            .json(&aruna_api::routes::credentials::CreateS3Request {
                group_id: group.group_id.clone(),
                expires_in_seconds: Some(600),
                path_restrictions: None,
                encrypted_buckets: Some(vec![BUCKET.to_string()]),
                token_public_key: Some(base64::Engine::encode(&STANDARD, public)),
            })
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;
        assert!(created.key_requests.is_empty(), "the node issues at once");
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

#[tokio::test]
async fn token_reads_old() -> TestResult<()> {
    const BUCKET: &str = "token-old-epochs";
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "token-old-group").await?;
        let plain = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let client = s3_client(endpoint, &plain);
        client.create_bucket().bucket(BUCKET).send().await?;
        let http = reqwest::Client::new();
        let bucket_url = format!("{}/api/v1/data/buckets/{BUCKET}", seed.base_url);
        let encryption = format!("{bucket_url}/storage/encryption");
        let enabled = http
            .put(&encryption)
            .bearer_auth(&admin)
            .json(&serde_json::json!({ "mode": "node_managed", "expected_generation": 0 }))
            .send()
            .await?;
        assert_eq!(enabled.status(), StatusCode::OK);
        let settings: serde_json::Value = enabled.json().await?;
        let bucket_id = ulid::Ulid::from_string(settings["bucket_id"].as_str().unwrap())?;
        let put = |key: &'static str| {
            client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(
                    key.as_bytes(),
                ))
                .send()
        };
        put("old.txt").await?;
        // More raises than one token key admits epochs.
        for _ in 0..17 {
            let raised = http
                .post(format!("{bucket_url}/abe/epoch"))
                .bearer_auth(&admin)
                .send()
                .await?;
            assert_eq!(raised.status(), StatusCode::OK);
        }
        put("new.txt").await?;
        // A removal synced from another node left this bucket due; issuing the token raises first.
        let due = aruna_core::effects::StorageEffect::Write {
            key_space: aruna_core::keyspaces::ABE_DUE_KEYSPACE.to_string(),
            key: bucket_id.to_bytes().to_vec().into(),
            value: vec![1].into(),
            txn_id: None,
        };
        seed.context.storage_handle.send_storage_effect(due).await;

        let (public, private) = aruna_core::structs::storage::encryption::generate_key()?;
        let token = hex::encode(private.bytes().expose());
        let created = http
            .post(format!("{}/api/v1/access/credentials", seed.base_url))
            .bearer_auth(&admin)
            .json(&aruna_api::routes::credentials::CreateS3Request {
                group_id: group.group_id.clone(),
                expires_in_seconds: Some(600),
                path_restrictions: None,
                encrypted_buckets: Some(vec![BUCKET.to_string()]),
                token_public_key: Some(base64::Engine::encode(&STANDARD, public)),
            })
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;
        let grants = token_grants(&seed, &created.access_key_id).await?;
        assert_eq!(grants.len(), 2);
        let epochs = grants.iter().flat_map(|g| g.context.request.epochs.clone());
        assert_eq!(epochs.max(), Some(19));
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

        // The token opens data from the first epoch and from the newest one.
        let reader = token_client(endpoint, &credentials, Some(token));
        for key in ["old.txt", "new.txt"] {
            let object = reader.get_object().bucket(BUCKET).key(key).send().await?;
            assert_eq!(
                &object.body.collect().await?.into_bytes()[..],
                key.as_bytes()
            );
        }
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

/// The grants of credential `access_key`, read from the node's storage.
async fn token_grants(
    seed: &shared::SeedNode,
    access_key: &str,
) -> TestResult<Vec<aruna_core::structs::storage::abe_access::KeyGrant>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    let scan = StorageEffect::Iter {
        key_space: aruna_core::keyspaces::ABE_GRANT_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: usize::MAX,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::IterResult { values, .. }) =
        seed.context.storage_handle.send_storage_effect(scan).await
    else {
        return Err(std::io::Error::other("grant scan failed").into());
    };
    let mut grants = Vec::new();
    for (_, value) in values {
        let grant = aruna_core::structs::storage::abe_access::KeyGrant::from_bytes(&value)?;
        if grant.context.request.credential_id.as_deref() == Some(access_key) {
            grants.push(grant);
        }
    }
    Ok(grants)
}

#[tokio::test]
async fn token_opens_scope() -> TestResult<()> {
    use aruna_core::structs::storage::abe_access::KeyScope;
    const BUCKET: &str = "token-scope";
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "token-scope-group").await?;
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
        assert_eq!(enabled.status(), StatusCode::OK);
        let mut versions = Vec::new();
        for key in ["allowed/a.txt", "other/b.txt"] {
            let put = client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from(
                    key.as_bytes().to_vec(),
                ))
                .send()
                .await?;
            let version = put
                .version_id()
                .ok_or_else(|| std::io::Error::other("no version id"))?;
            versions.push(ulid::Ulid::from_string(version)?);
        }

        // A token restricted to `allowed/` gets one grant of that subtree, issued at once.
        let (public, private) = aruna_core::structs::storage::encryption::generate_key()?;
        let created = http
            .post(format!("{}/api/v1/access/credentials", seed.base_url))
            .bearer_auth(&admin)
            .json(&aruna_api::routes::credentials::CreateS3Request {
                group_id: group.group_id.clone(),
                expires_in_seconds: Some(600),
                path_restrictions: Some(vec![create_request_restriction(
                    format!("{BUCKET}/allowed/**"),
                    Permission::READ,
                )]),
                encrypted_buckets: Some(vec![BUCKET.to_string()]),
                token_public_key: Some(base64::Engine::encode(&STANDARD, public)),
            })
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;
        let access_key = created.access_key_id.clone();
        let grants = token_grants(&seed, &access_key).await?;
        let scopes: Vec<_> = grants.iter().map(|g| &g.context.request.scope).collect();
        assert_eq!(scopes, [&KeyScope::Subtree("allowed/".to_string())]);
        let locked = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(&admin)
            .send()
            .await?;
        assert_eq!(locked.status(), StatusCode::OK);

        // While locked the token reads its scope; a leaked token and grant open nothing else.
        let credentials = shared::S3Credentials {
            access_key_id: created.access_key_id,
            access_secret: created.access_secret,
        };
        let token = hex::encode(private.bytes().expose());
        let reader = token_client(endpoint, &credentials, Some(token.clone()));
        let object = reader
            .get_object()
            .bucket(BUCKET)
            .key("allowed/a.txt")
            .send()
            .await?;
        assert_eq!(
            &object.body.collect().await?.into_bytes()[..],
            b"allowed/a.txt"
        );
        let mut header = String::new();
        for (key, version) in ["allowed/a.txt", "other/b.txt"].into_iter().zip(&versions) {
            let read = aruna_operations::abe::envelope::EnvelopeOperation::new(
                BUCKET.to_string(),
                key.to_string(),
                *version,
            );
            let (envelope, _) = aruna_operations::driver::drive(read, &seed.context).await?;
            let opened = grants[0].open_object(private.bytes(), &envelope);
            assert_eq!(opened.is_ok(), key == "allowed/a.txt", "{key}");
            if let Ok(object) = opened {
                header = base64::Engine::encode(&STANDARD, object.bytes().expose());
            }
        }

        // The object key header opens the locked object only when the signature covers it.
        let keyed = token_client(endpoint, &plain, None);
        let get = || keyed.get_object().bucket(BUCKET).key("allowed/a.txt");
        let value = header.clone();
        let object = get()
            .customize()
            .mutate_request(move |request| {
                request
                    .headers_mut()
                    .insert("x-aruna-object-key", value.clone());
            })
            .send()
            .await?;
        assert_eq!(
            &object.body.collect().await?.into_bytes()[..],
            b"allowed/a.txt"
        );
        for listed in ["X-Aruna-Object-Key", " x-aruna-object-key"] {
            let tamper = Tamper {
                header: "x-aruna-object-key",
                listed: Some(listed),
                key: header.clone(),
            };
            let read = get().customize().interceptor(tamper).send().await;
            assert_eq!(
                service_error_code(&read).as_deref(),
                Some("SignatureDoesNotMatch"),
                "{listed:?}"
            );
        }
        let value = header.clone();
        let tamper = Tamper {
            header: "x-aruna-object-key",
            listed: None,
            key: base64::Engine::encode(&STANDARD, [7u8; 32]),
        };
        let read = get()
            .customize()
            .mutate_request(move |request| {
                request
                    .headers_mut()
                    .insert("x-aruna-object-key", value.clone());
            })
            .interceptor(tamper)
            .send()
            .await;
        assert_eq!(
            service_error_code(&read).as_deref(),
            Some("SignatureDoesNotMatch")
        );

        // A region holding a decoy SignedHeaders list, signed validly, protects neither header.
        for (header, value, client) in [
            ("x-aruna-object-key", header.clone(), &plain),
            ("x-amz-security-token", token.clone(), &credentials),
        ] {
            let region = format!("us-east-1,SignedHeaders={header},");
            let config = token_client(endpoint, client, None)
                .config()
                .to_builder()
                .region(aws_sdk_s3::config::Region::new(region))
                .build();
            let tamper = Tamper {
                header,
                listed: None,
                key: value,
            };
            let read = aws_sdk_s3::Client::from_conf(config)
                .get_object()
                .bucket(BUCKET)
                .key("allowed/a.txt")
                .customize()
                .interceptor(tamper)
                .send()
                .await;
            // s3s refuses the region before the object key check; the token is refused first.
            let expected = if header == "x-aruna-object-key" {
                "InvalidRequest"
            } else {
                "InvalidToken"
            };
            assert_eq!(
                service_error_code(&read).as_deref(),
                Some(expected),
                "{header}"
            );
        }

        // Revoking the credential deletes its grants.
        let revoked = http
            .delete(format!(
                "{}/api/v1/access/credentials/{access_key}",
                seed.base_url
            ))
            .bearer_auth(&admin)
            .send()
            .await?;
        assert!(revoked.status().is_success(), "{}", revoked.status());
        assert!(token_grants(&seed, &access_key).await?.is_empty());
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

/// The open key requests of credential `access_key`, read from the node's storage.
async fn token_requests(
    seed: &shared::SeedNode,
    access_key: &str,
) -> TestResult<Vec<aruna_core::structs::storage::abe_access::KeyRequest>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    let scan = StorageEffect::Iter {
        key_space: aruna_core::keyspaces::ABE_REQUEST_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: usize::MAX,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::IterResult { values, .. }) =
        seed.context.storage_handle.send_storage_effect(scan).await
    else {
        return Err(std::io::Error::other("request scan failed").into());
    };
    let mut requests = Vec::new();
    for (_, value) in values {
        let request = aruna_core::structs::storage::abe_access::KeyRequest::from_bytes(&value)?;
        if request.credential_id.as_deref() == Some(access_key) {
            requests.push(request);
        }
    }
    Ok(requests)
}

#[tokio::test]
async fn token_needs_credential() -> TestResult<()> {
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::storage::abe::AbeError;
    use aruna_core::structs::storage::abe_access::{GrantContext, KeyGrant, KeyIssuer};
    use aruna_operations::abe::{KeyAction, KeyError, KeyOperation};
    use aruna_operations::driver::drive;
    const BUCKET: &str = "token-live";
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "token-live-group").await?;
        let plain = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        s3_client(endpoint, &plain)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
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
        assert_eq!(enabled.status(), StatusCode::OK);
        let locked = http
            .post(format!("{encryption}/lock"))
            .bearer_auth(&admin)
            .send()
            .await?;
        assert_eq!(locked.status(), StatusCode::OK);

        // A token of the locked bucket keeps an open request for a holder to publish.
        let (public, _) = aruna_core::structs::storage::encryption::generate_key()?;
        let created = http
            .post(format!("{}/api/v1/access/credentials", seed.base_url))
            .bearer_auth(&admin)
            .json(&aruna_api::routes::credentials::CreateS3Request {
                group_id: group.group_id.clone(),
                expires_in_seconds: Some(600),
                path_restrictions: None,
                encrypted_buckets: Some(vec![BUCKET.to_string()]),
                token_public_key: Some(base64::Engine::encode(&STANDARD, public)),
            })
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;
        assert_eq!(created.key_requests.len(), 1);
        let access_key = created.access_key_id;
        let mut requests = token_requests(&seed, &access_key).await?;
        let request = requests
            .pop()
            .ok_or_else(|| std::io::Error::other("no open token request"))?;
        let grant = KeyGrant {
            context: GrantContext {
                request,
                issuer: KeyIssuer::User(seed.user_id),
            },
            enc: [1; 32],
            ciphertext: vec![1; 16],
        };
        let auth = AuthContext {
            user_id: seed.user_id,
            realm_id: seed.realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = seed.net.node_id();
        let now = aruna_core::time::unix_timestamp_millis();

        // Once the credential expires, a holder can no longer publish its request.
        let action = KeyAction::Publish(grant.clone());
        let late = KeyOperation::new(BUCKET.into(), auth.clone(), node, action, now + 601_000);
        assert_eq!(
            drive(late, &seed.context).await,
            Err(KeyError::Abe(AbeError::Stale))
        );

        // A holder publishes the live token's request without reading the creator's vault.
        let action = KeyAction::Publish(grant.clone());
        let mut operation = KeyOperation::new(BUCKET.into(), auth.clone(), node, action, now);
        let mut effects: std::collections::VecDeque<_> = operation.start().into_iter().collect();
        while let Some(effect) = effects.pop_front() {
            let aruna_core::effects::Effect::Storage(effect) = effect else {
                panic!("unexpected effect {effect:?}");
            };
            if let aruna_core::effects::StorageEffect::Iter { key_space, .. } = &effect {
                assert_ne!(key_space, aruna_core::keyspaces::USER_KEY_KEYSPACE);
            }
            let event = seed
                .context
                .storage_handle
                .send_storage_effect(effect)
                .await;
            if !operation.is_complete() {
                effects.extend(operation.step(event));
            }
        }
        assert!(operation.finalize().is_ok());
        assert_eq!(token_grants(&seed, &access_key).await?.len(), 1);

        // A request after revocation recreates no request or grant for the credential.
        let revoked = http
            .delete(format!(
                "{}/api/v1/access/credentials/{access_key}",
                seed.base_url
            ))
            .bearer_auth(&admin)
            .send()
            .await?;
        assert!(revoked.status().is_success(), "{}", revoked.status());
        let action = KeyAction::Token {
            access_key: access_key.clone(),
            public_key: public,
            restrictions: None,
        };
        let request = KeyOperation::new(BUCKET.into(), auth, node, action, now);
        assert_eq!(
            drive(request, &seed.context).await,
            Err(KeyError::Abe(AbeError::Stale))
        );
        assert!(token_requests(&seed, &access_key).await?.is_empty());
        assert!(token_grants(&seed, &access_key).await?.is_empty());
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test]
async fn token_limit_revokes() -> TestResult<()> {
    use aruna_api::routes::credentials::{CreateS3Request, ListS3Response};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    const BUCKET: &str = "token-limit";
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "token-limit-group").await?;
        let plain = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        s3_client(endpoint, &plain)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let http = reqwest::Client::new();
        let enabled = http
            .put(format!(
                "{}/api/v1/data/buckets/{BUCKET}/storage/encryption",
                seed.base_url
            ))
            .bearer_auth(&admin)
            .json(&serde_json::json!({ "mode": "node_managed", "expected_generation": 0 }))
            .send()
            .await?;
        assert_eq!(enabled.status(), StatusCode::OK);
        let (public, _) = aruna_core::structs::storage::encryption::generate_key()?;
        let request = CreateS3Request {
            group_id: group.group_id.clone(),
            expires_in_seconds: Some(600),
            path_restrictions: None,
            encrypted_buckets: Some(vec![BUCKET.to_string()]),
            token_public_key: Some(base64::Engine::encode(&STANDARD, public)),
        };
        let route = format!("{}/api/v1/access/credentials", seed.base_url);
        let created = http
            .post(&route)
            .bearer_auth(&admin)
            .json(&request)
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: aruna_api::routes::credentials::CreateS3Response = created.json().await?;

        // Copies of the first grant fill the caller's 64 grants of the bucket.
        let grant = token_grants(&seed, &created.access_key_id).await?.remove(0);
        let value = grant.to_bytes()?;
        let prefix = grant.context.request.prefix();
        let writes = (1..64u64)
            .map(|id| {
                let key = [
                    prefix.clone(),
                    ulid::Ulid::from_parts(id, 0).to_bytes().to_vec(),
                ];
                let space = aruna_core::keyspaces::ABE_GRANT_KEYSPACE.to_string();
                (space, key.concat().into(), value.clone().into())
            })
            .collect();
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        let written = seed.context.storage_handle.send_storage_effect(write).await;
        assert!(matches!(
            written,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));

        // The next token gets no grant, so its credential is revoked and the request fails.
        let refused = http
            .post(&route)
            .bearer_auth(&admin)
            .json(&request)
            .send()
            .await?;
        assert_eq!(refused.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let listed: ListS3Response = http
            .get(&route)
            .bearer_auth(&admin)
            .send()
            .await?
            .json()
            .await?;
        let mut keys: Vec<_> = listed
            .credentials
            .iter()
            .map(|c| &c.access_key_id)
            .collect();
        keys.sort();
        let mut expected = vec![&plain.access_key_id, &created.access_key_id];
        expected.sort();
        assert_eq!(keys, expected, "only the earlier credentials remain");
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

/// Changes a signed request: lists `listed` among its signed headers and sets `header` to `key`.
#[derive(Debug)]
struct Tamper {
    header: &'static str,
    listed: Option<&'static str>,
    key: String,
}

impl aws_sdk_s3::config::Intercept for Tamper {
    fn name(&self) -> &'static str {
        "Tamper"
    }

    fn modify_before_transmit(
        &self,
        context: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        let headers = context.request_mut().headers_mut();
        headers.insert(self.header, self.key.clone());
        if let Some(listed) = self.listed {
            let authorization = headers.get("authorization").unwrap_or_default();
            let authorization =
                authorization.replace("SignedHeaders=", &format!("SignedHeaders={listed};"));
            headers.insert("authorization", authorization);
        }
        Ok(())
    }
}
