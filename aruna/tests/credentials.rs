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
use base64::engine::general_purpose::STANDARD;
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
        assert_eq!(token_grants(&seed, &created.access_key_id).await?.len(), 2);
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
                Some("AccessDenied"),
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
