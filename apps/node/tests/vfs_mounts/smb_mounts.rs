//! SMB 3.1.1 mounts end to end: streamed writes published by FLUSH, renames over live names
//! and out of implicit directories, deletes, and live sessions ended by S3 key revocation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_smb::status;

use super::smb_client::{DELETE, DIRECTORY, FILE_CREATE, FILE_OPEN, NOT_DIRECTORY};
use super::smb_client::{READ_ATTRIBUTES, SmbClient, WRITER};
use super::{BUCKET, Mounts};
use crate::shared::{TestResult, WAIT_CAP, wait_until};

/// Past the 1 MiB publication chunk, so FLUSH streams the image in more than one chunk.
const ALPHA_BYTES: usize = (1 << 20) + 70_000;

fn pattern(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

#[tokio::test]
async fn smb_roundtrip_rename() -> TestResult<()> {
    let mounts = Mounts::start(true, None).await?;
    let result = roundtrip_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

async fn roundtrip_steps(mounts: &Mounts) -> TestResult<()> {
    let address = mounts.smb.ok_or("no SMB listener")?;
    // `dir` exists only through this object, which makes it an implicit directory.
    mounts.put("dir/keep.txt", b"keep").await?;
    let mut client = SmbClient::login(address, &mounts.keys, BUCKET).await?;
    let alpha = pattern(ALPHA_BYTES);
    let file = client
        .open("dir\\alpha.bin", WRITER, FILE_CREATE, NOT_DIRECTORY)
        .await?;
    client.write_all(file, &alpha).await?;
    client.flush(file).await?;
    assert!(
        client.read_all(file, alpha.len()).await? == alpha,
        "SMB reads its own bytes"
    );
    client.close(file).await?;
    assert!(
        mounts.get("dir/alpha.bin").await? == alpha,
        "FLUSH published to S3"
    );

    // An unpublished file renamed over a committed name replaces it atomically.
    mounts.put("target.bin", b"live target").await?;
    let staged = b"fresh staged bytes".to_vec();
    let file = client
        .open("staging.bin", WRITER, FILE_CREATE, NOT_DIRECTORY)
        .await?;
    client.write(file, 0, &staged).await?;
    client.rename(file, "target.bin", true).await?;
    client.close(file).await?;
    assert_eq!(mounts.get("target.bin").await?, staged);
    assert!(
        mounts.missing("staging.bin").await,
        "the staged name is gone"
    );

    // Moving the last child out of the implicit `dir` keeps `dir` as a marker.
    mounts.delete("dir/keep.txt").await?;
    let other = client
        .open("other", READ_ATTRIBUTES, FILE_CREATE, DIRECTORY)
        .await?;
    client.close(other).await?;
    let file = client
        .open("dir\\alpha.bin", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    client.rename(file, "other\\alpha.bin", false).await?;
    client.close(file).await?;
    assert!(
        mounts.get("other/alpha.bin").await? == alpha,
        "the move keeps the bytes"
    );
    assert!(
        mounts.missing("dir/alpha.bin").await,
        "the old name is gone"
    );
    assert!(
        mounts.exists("dir/").await,
        "the parent marker of `dir` was written"
    );
    let parent = client
        .open("dir", READ_ATTRIBUTES, FILE_OPEN, DIRECTORY)
        .await?;
    client.close(parent).await?;

    let access = DELETE | READ_ATTRIBUTES;
    let file = client
        .open("target.bin", access, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    client.dispose(file).await?;
    client.close(file).await?;
    assert!(
        mounts.missing("target.bin").await,
        "the delete-on-close removed the object"
    );
    Ok(())
}

#[tokio::test]
async fn smb_key_revocation() -> TestResult<()> {
    let mounts = Mounts::start(true, None).await?;
    let result = revocation_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

/// Whether the session answers as expired or deleted, or the connection is gone.
async fn session_ended(client: &mut SmbClient) -> bool {
    match client.probe().await {
        Ok(Some(answer)) => [
            status::NETWORK_SESSION_EXPIRED,
            status::USER_SESSION_DELETED,
        ]
        .contains(&answer),
        Ok(None) | Err(_) => true,
    }
}

async fn revocation_steps(mounts: &Mounts) -> TestResult<()> {
    let address = mounts.smb.ok_or("no SMB listener")?;
    let mut client = SmbClient::login(address, &mounts.keys, BUCKET).await?;
    assert_eq!(
        client.probe().await?,
        Some(status::SUCCESS),
        "the live session serves"
    );
    let file = client
        .open("held.bin", WRITER, FILE_CREATE, NOT_DIRECTORY)
        .await?;
    client.write(file, 0, b"held by the revoked key").await?;
    mounts.revoke_key().await?;
    let client = tokio::sync::Mutex::new(client);
    let shared = &client;
    wait_until(
        "the revoked SMB session ends",
        WAIT_CAP,
        move || async move { session_ended(&mut *shared.lock().await).await },
    )
    .await?;
    let mut fresh = SmbClient::connect(address).await?;
    fresh.negotiate().await?;
    let keys = &mounts.keys;
    let answer = fresh
        .setup(&keys.access_key_id, &keys.access_secret)
        .await?;
    assert_eq!(
        answer,
        status::LOGON_FAILURE,
        "a revoked key cannot log in again"
    );
    Ok(())
}
