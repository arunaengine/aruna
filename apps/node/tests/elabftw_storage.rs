//! Writes to a local node the way eLabFTW's S3 upload storage does, with a service account
//! credential confined to the eLabFTW prefix of a shared bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::service_credentials::{as_credentials, create_account, issue};
use crate::shared;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, ObjectCannedAcl,
};
use reqwest::StatusCode;
use serde_json::json;
use shared::{
    S3Credentials, S3Endpoint, TestResult, create_bearer_token, create_group_http,
    create_s3_credentials, s3_client, spawn_complete_seed,
};

const BUCKET: &str = "shared-bucket";
/// eLabFTW's default storage region; the node accepts the region the client signs with.
const REGION: &str = "fr-par";
const PART: usize = 5 * 1024 * 1024;

/// A client set up like eLabFTW's: path-style, its own region, no retries.
fn elabftw_client(endpoint: &S3Endpoint, keys: &S3Credentials) -> S3Client {
    let credentials = Credentials::new(
        keys.access_key_id.clone(),
        keys.access_secret.clone(),
        None,
        None,
        "elabftw-storage-test",
    );
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(credentials)
        .endpoint_url(endpoint.endpoint_url.clone())
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .build();
    S3Client::from_conf(config)
}

/// One upload as eLabFTW writes it: `x-amz-acl: private` and a CRC32 checksum.
async fn upload(client: &S3Client, key: &str, body: Vec<u8>) -> Result<(), String> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .acl(ObjectCannedAcl::Private)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from(body))
        .send()
        .await
        .map(drop)
        .map_err(|error| error.code().unwrap_or("unknown").to_string())
}

#[tokio::test]
async fn elabftw_storage_writes() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let (context, realm_id) = (seed.context.as_ref(), seed.realm_id);
        let admin =
            create_bearer_token(context, seed.user_id, realm_id, seed.capabilities.clone()).await?;
        let group = create_group_http(&seed.base_url, &admin, "elabftw-storage").await?;
        let group_id = group.group_id;
        let endpoint = seed.s3.clone().ok_or("seed has no S3 endpoint")?;
        let admin_keys = create_s3_credentials(&seed.base_url, &admin, &group_id).await?;
        s3_client(&endpoint, &admin_keys)
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await?;
        let account = create_account(&seed.base_url, &admin, &group_id).await?;
        let body = json!({
            "path_restrictions": [{ "pattern": "shared-bucket/elabftw/**", "permission": "WRITE" }]
        });
        let response = issue(&seed.base_url, &admin, &group_id, &account, &body).await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let client = elabftw_client(&endpoint, &as_credentials(response.json().await?));

        // Flysystem writes the folder as an empty object before each upload; thumbnails follow.
        upload(&client, "elabftw/ab/", Vec::new()).await?;
        let key = "elabftw/ab/ab12cd34-5e6f-4a1b-8c9d-0e1f2a3b4c5d.png";
        upload(&client, key, b"0123456789".to_vec()).await?;
        upload(&client, "elabftw/ab/ab12cd34_th.jpg", b"thumb".to_vec()).await?;
        let head = client.head_object().bucket(BUCKET).key(key).send().await?;
        assert_eq!(head.content_length(), Some(10));
        let ranged = client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .range("bytes=2-5")
            .send()
            .await?;
        assert_eq!(ranged.body.collect().await?.into_bytes().as_ref(), b"2345");

        // Large files go up in parts, each with its CRC32.
        let large = "elabftw/cd/cd34ef56-7a8b-4c9d-8e0f-1a2b3c4d5e6f.bin";
        let created = client
            .create_multipart_upload()
            .bucket(BUCKET)
            .key(large)
            .acl(ObjectCannedAcl::Private)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await?;
        let upload_id = created.upload_id().ok_or("no upload id")?;
        let mut parts = Vec::new();
        for (number, bytes) in [(1, vec![b'a'; PART]), (2, vec![b'b'; 1024])] {
            let part = client
                .upload_part()
                .bucket(BUCKET)
                .key(large)
                .upload_id(upload_id)
                .part_number(number)
                .checksum_algorithm(ChecksumAlgorithm::Crc32)
                .body(ByteStream::from(bytes))
                .send()
                .await?;
            let completed = CompletedPart::builder()
                .part_number(number)
                .set_e_tag(part.e_tag().map(String::from))
                .set_checksum_crc32(part.checksum_crc32().map(String::from))
                .build();
            parts.push(completed);
        }
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key(large)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await?;
        let head = client
            .head_object()
            .bucket(BUCKET)
            .key(large)
            .send()
            .await?;
        assert_eq!(head.content_length(), Some((PART + 1024) as i64));

        let listed = client
            .list_objects_v2()
            .bucket(BUCKET)
            .prefix("elabftw/")
            .send()
            .await?;
        let keys: Vec<&str> = listed
            .contents()
            .iter()
            .filter_map(|object| object.key())
            .collect();
        for wanted in ["elabftw/ab/", key, large] {
            assert!(keys.contains(&wanted), "{wanted} in {keys:?}");
        }
        client
            .delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await?;
        let gone = client.head_object().bucket(BUCKET).key(key).send().await;
        assert!(gone.is_err(), "the deleted upload is gone");

        // The same credential cannot write beside the prefix, also not under a longer name.
        for outside in ["other/x.png", "elabftw-old/x.png", "x.png"] {
            let refused = upload(&client, outside, b"x".to_vec()).await;
            assert_eq!(refused, Err("AccessDenied".to_string()), "{outside}");
        }
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}
