// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
mod shared;

use aruna_core::structs::HashPathIndexKey;
use aruna_operations::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use aruna_operations::driver::drive;
use aruna_operations::s3::head_object::{HeadObjectInput, HeadObjectOperation};
use aws_sdk_s3::primitives::ByteStream;
use reqwest::StatusCode;
use serde_json::Value;
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_onboarding_secret_via_http,
    create_s3_credentials_via_http, s3_client, shutdown_pair, spawn_full_joiner_node,
    spawn_full_seed_node, spawn_seed_node, wait_for_group_via_http, wait_for_realm_nodes,
};
use ulid::Ulid;

const FIXTURE_BYTES: &[u8] = b"drs placeholder fixture bytes";

fn fixture_bytes() -> Vec<u8> {
    FIXTURE_BYTES.to_vec()
}

async fn iter_hash_path_index(
    context: &aruna_operations::driver::DriverContext,
    hash: [u8; 32],
) -> TestResult<Vec<HashPathIndexKey>> {
    drive(ResolveBlobPermissionPathsOperation::new(hash), context)
        .await
        .map_err(Into::into)
}

#[tokio::test(flavor = "multi_thread")]
async fn drs_get_object_content_hash_arn_returns_404_on_non_owner_node() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;

    let result = async {
        let bearer_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;

        let group = create_group_via_http(&seed.base_url, &bearer_token, "drs-hash-lookup-e2e")
            .await?;
        let seed_s3 = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("seed node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&seed.base_url, &bearer_token, &group.group_id)
                .await?;
        let s3 = s3_client(seed_s3, &credentials);

        let bucket = "drs-hash-lookup-e2e";
        let key = "fixtures/t8.shakespeare.txt";
        let body = fixture_bytes();
        let hash = *blake3::hash(&body).as_bytes();
        let hash_hex = hex::encode(hash);
        let local_object_id = format!(
            "arn:aruna:{}:{}:ch/{}",
            seed.realm_id,
            seed.net.node_id(),
            hash_hex
        );
        let foreign_node = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        assert_ne!(foreign_node, seed.net.node_id());
        let foreign_object_id = format!(
            "arn:aruna:{}:{}:ch/{}",
            seed.realm_id, foreign_node, hash_hex
        );

        s3.create_bucket().bucket(bucket).send().await?;
        s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await?;

        let local_response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/objects/{}",
                seed.base_url, local_object_id
            ))
            .bearer_auth(&bearer_token)
            .send()
            .await?;
        let local_status = local_response.status();
        let local_body = local_response.text().await?;
        assert_eq!(
            local_status,
            StatusCode::OK,
            "same-node DRS lookup failed for {local_object_id}: {local_body}"
        );

        let foreign_response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/objects/{}",
                seed.base_url, foreign_object_id
            ))
            .bearer_auth(&bearer_token)
            .send()
            .await?;
        let foreign_status = foreign_response.status();
        let foreign_body = foreign_response.text().await?;

        assert_eq!(
            foreign_status,
            StatusCode::NOT_FOUND,
            "DRS lookup for foreign owner returned {foreign_status} for {foreign_object_id}: {foreign_body}"
        );

        Ok(())
    }
    .await;

    seed.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread")]
async fn drs_historical_materialized_hash_resolves_non_current_version() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let onboarding_secret =
        create_onboarding_secret_via_http(&seed, aruna_core::onboarding::OnboardingMode::Local)
            .await?;
    let joiner = spawn_full_joiner_node(&seed, onboarding_secret).await?;

    let result = async {
        wait_for_realm_nodes(
            &[seed.context.as_ref(), joiner.context.as_ref()],
            &seed.realm_id,
            2,
        )
        .await?;

        let bearer_token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;

        let group =
            create_group_via_http(&seed.base_url, &bearer_token, "drs-historical-hash-e2e").await?;
        wait_for_group_via_http(&joiner.base_url, &bearer_token, &group.group_id).await?;

        let joiner_s3 = joiner
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("joiner node did not start S3 server"))?;
        let credentials =
            create_s3_credentials_via_http(&joiner.base_url, &bearer_token, &group.group_id)
                .await?;
        let s3 = s3_client(joiner_s3, &credentials);

        let bucket = "drs-historical-hash-e2e";
        let key = "fixtures/versioned.txt";
        let old_body = fixture_bytes();
        let new_body = b"historical drs replacement body".to_vec();
        let old_hash = *blake3::hash(&old_body).as_bytes();
        let new_hash = *blake3::hash(&new_body).as_bytes();

        s3.create_bucket().bucket(bucket).send().await?;

        let old_put = s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(old_body.clone()))
            .send()
            .await?;
        let old_version_id = Ulid::from_string(old_put.version_id().ok_or_else(|| {
            std::io::Error::other("first put_object did not return a version id")
        })?)?;

        let new_put = s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(new_body.clone()))
            .send()
            .await?;
        let new_version_id = Ulid::from_string(new_put.version_id().ok_or_else(|| {
            std::io::Error::other("second put_object did not return a version id")
        })?)?;

        let old_mappings = iter_hash_path_index(joiner.context.as_ref(), old_hash).await?;
        assert!(
            old_mappings.iter().any(|mapping| {
                mapping.realm_id == seed.realm_id
                    && mapping.node_id == joiner.config.node_id
                    && mapping.bucket == bucket
                    && mapping.key == key
                    && mapping.version_id == old_version_id
            }),
            "hash-path index did not retain historical alias for {bucket}/{key}@{old_version_id}"
        );

        let new_mappings = iter_hash_path_index(joiner.context.as_ref(), new_hash).await?;
        assert!(
            new_mappings.iter().any(|mapping| {
                mapping.realm_id == seed.realm_id
                    && mapping.node_id == joiner.config.node_id
                    && mapping.bucket == bucket
                    && mapping.key == key
                    && mapping.version_id == new_version_id
            }),
            "hash-path index did not contain current alias for {bucket}/{key}@{new_version_id}"
        );

        let old_head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: Some(old_version_id),
            }),
            joiner.context.as_ref(),
        )
        .await?
        .ok_or_else(|| std::io::Error::other("head_object returned no historical result"))??;
        let old_location = old_head.location.ok_or_else(|| {
            std::io::Error::other("head_object returned no historical materialized location")
        })?;
        assert_eq!(old_location.get_blake3(), Some(old_hash.as_slice()));

        let historical_object_id = format!(
            "arn:aruna:{}:{}:ch/{}",
            seed.realm_id,
            joiner.config.node_id,
            hex::encode(old_hash)
        );

        let object_response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/objects/{}",
                joiner.base_url, historical_object_id
            ))
            .bearer_auth(&bearer_token)
            .send()
            .await?;
        let object_status = object_response.status();
        let object_body: Value = object_response.json().await?;
        assert_eq!(
            object_status,
            StatusCode::OK,
            "historical DRS lookup failed for {historical_object_id}: {object_body}"
        );
        assert_eq!(object_body["id"], historical_object_id);
        assert_eq!(
            object_body["aliases"],
            Value::Array(vec![Value::String(format!(
                "https://w3id.org/aruna/data/{}",
                hex::encode(old_hash)
            ))])
        );

        let download_response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/ga4gh/drs/v1/download?object_id={}",
                joiner.base_url, historical_object_id
            ))
            .bearer_auth(&bearer_token)
            .send()
            .await?;
        let download_status = download_response.status();
        let download_body = download_response.bytes().await?;
        assert_eq!(
            download_status,
            StatusCode::OK,
            "historical DRS download failed for {historical_object_id}"
        );
        assert_eq!(download_body.as_ref(), old_body.as_slice());

        Ok(())
    }
    .await;

    shutdown_pair(joiner, seed).await;
    result
}
