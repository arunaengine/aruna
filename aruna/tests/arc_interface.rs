//! Exercises the optional ARC bridge against real Aruna S3 and metadata services.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

mod shared;

use shared::{
    TestResult, create_bearer_token, create_group_http, create_s3_credentials, s3_client,
    spawn_complete_seed,
};
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
#[ignore = "requires ARUNA_ARC_PYTHON with the bridge dependencies, Git LFS and signing"]
async fn arc_roundtrip() -> TestResult<()> {
    let python = std::env::var("ARUNA_ARC_PYTHON")?;
    let seed = spawn_complete_seed().await?;
    let result = async {
        let token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        let group = create_group_http(&seed.base_url, &token, "arc-interface").await?;
        let credentials = create_s3_credentials(&seed.base_url, &token, &group.group_id).await?;
        let endpoint = seed
            .s3
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing S3 listener"))?;
        let bucket = format!("arc-{}", ulid::Ulid::generate().to_string().to_lowercase());
        s3_client(endpoint, &credentials)
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await?;
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace parent");
        let mut child = Command::new(python)
            .arg(root.join("scripts/arc-bridge/test_live.py"))
            .env("ARUNA_API_URL", &seed.base_url)
            .env("ARUNA_S3_URL", &endpoint.endpoint_url)
            .env("ARUNA_TOKEN", token)
            .env("ARUNA_GROUP_ID", &group.group_id)
            .env("ARUNA_BUCKET", bucket)
            .env("AWS_ACCESS_KEY_ID", &credentials.access_key_id)
            .env("AWS_SECRET_ACCESS_KEY", &credentials.access_secret)
            .env("AWS_DEFAULT_REGION", shared::AWS_REGION)
            .kill_on_drop(true)
            .spawn()?;
        let status = tokio::time::timeout(Duration::from_secs(900), child.wait()).await??;
        if !status.success() {
            return Err(std::io::Error::other("ARC bridge integration failed").into());
        }
        Ok(())
    }
    .await;
    seed.shutdown().await;
    result
}
