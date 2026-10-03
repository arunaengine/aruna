//! Tests who may read and change a bucket's compression through REST.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]

mod shared;

use aruna_api::routes::groups::AddMemberRequest;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    TestResult, create_bearer_token, create_group_http, create_s3_credentials, s3_client,
    spawn_complete_seed,
};
use ulid::Ulid;

const BUCKET: &str = "compression-routes";

fn route(base_url: &str, bucket: &str) -> String {
    format!("{base_url}/api/v1/data/buckets/{bucket}/storage/compression")
}

async fn get(base_url: &str, bucket: &str, token: Option<&str>) -> TestResult<(StatusCode, Value)> {
    let mut request = reqwest::Client::new().get(route(base_url, bucket));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    let status = response.status();
    Ok((status, response.json().await.unwrap_or(Value::Null)))
}

async fn put(
    base_url: &str,
    bucket: &str,
    token: Option<&str>,
    body: Value,
) -> TestResult<(StatusCode, Value)> {
    let mut request = reqwest::Client::new()
        .put(route(base_url, bucket))
        .json(&body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    let status = response.status();
    Ok((status, response.json().await.unwrap_or(Value::Null)))
}

#[tokio::test]
async fn routes_check_permissions() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;

    let result = async {
        let admin = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &admin, "compression-routes-group").await?;
        let credentials = create_s3_credentials(&seed.base_url, &admin, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        s3_client(endpoint, &credentials)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let token = |user_id, realm_id| {
            create_bearer_token(
                seed.context.as_ref(),
                user_id,
                realm_id,
                seed.capabilities.clone(),
            )
        };
        let outsider = token(
            UserId::local(Ulid::generate(), seed.realm_id),
            seed.realm_id,
        )
        .await?;
        let member_id = UserId::local(Ulid::generate(), seed.realm_id);
        let member = token(member_id, seed.realm_id).await?;
        let added = reqwest::Client::new()
            .post(format!(
                "{}/api/v1/access/groups/{}/members",
                seed.base_url, group.group_id
            ))
            .bearer_auth(&admin)
            .json(&AddMemberRequest {
                user_id: member_id.to_string(),
                role_ids: None,
            })
            .send()
            .await?;
        assert_eq!(added.status(), StatusCode::CREATED);
        let zstd = json!({ "mode": "zstd", "level": 9 });
        let url = seed.base_url.as_str();

        // Without a token nothing is read or changed.
        assert_eq!(get(url, BUCKET, None).await?.0, StatusCode::UNAUTHORIZED);
        let (status, _) = put(url, BUCKET, None, zstd.clone()).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        // A user outside the owning group may do neither.
        assert_eq!(
            get(url, BUCKET, Some(&outsider)).await?.0,
            StatusCode::FORBIDDEN
        );
        let (status, _) = put(url, BUCKET, Some(&outsider), zstd.clone()).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
        // A member reads the setting but only a group admin changes it.
        let (status, body) = get(url, BUCKET, Some(&member)).await?;
        assert_eq!((status, &body["mode"]), (StatusCode::OK, &json!("off")));
        let (status, _) = put(url, BUCKET, Some(&member), zstd.clone()).await?;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Invalid input is refused before anything is stored.
        let bad = [
            json!({ "mode": "zstd", "level": 23 }),
            json!({ "mode": "off", "level": 3 }),
        ];
        for body in bad {
            assert_eq!(
                put(url, BUCKET, Some(&admin), body).await?.0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            get(url, "missing-bucket", Some(&admin)).await?.0,
            StatusCode::NOT_FOUND
        );
        let (status, _) = put(url, "missing-bucket", Some(&admin), zstd.clone()).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, body) = put(url, BUCKET, Some(&admin), zstd.clone()).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!((&body["mode"], &body["level"]), (&json!("zstd"), &json!(9)));
        assert!(body["migration"]["started_at_ms"].is_u64());
        let (status, body) = get(url, BUCKET, Some(&member)).await?;
        assert_eq!((status, &body["level"]), (StatusCode::OK, &json!(9)));
        // The same setting again reports the migration instead of hiding it.
        let (status, body) = put(url, BUCKET, Some(&admin), zstd).await?;
        assert_eq!(status, StatusCode::OK);
        assert!(body["migration"]["started_at_ms"].is_u64());

        // A token of another realm does not validate here, even for the bucket's admin.
        let foreign = token(seed.user_id, RealmId::from_bytes([42u8; 32])).await?;
        assert_eq!(
            get(url, BUCKET, Some(&foreign)).await?.0,
            StatusCode::UNAUTHORIZED
        );
        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}
