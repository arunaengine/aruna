//! NFSv4.1 mounts end to end: REST host enrollment and bindings, loopback cleartext writes,
//! renames and removal checked through S3, and RFC 9289 mutual TLS ended by host revocation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::nfs_client::{self, CREATOR, NfsClient, UNSTABLE};
use super::{HostKey, Mounts, loopback_tls, server_tls};
use crate::shared::TestResult;

#[tokio::test]
async fn nfs_loopback_lifecycle() -> TestResult<()> {
    let mounts = Mounts::start(false, Some(loopback_tls())).await?;
    let result = lifecycle_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

async fn lifecycle_steps(mounts: &Mounts) -> TestResult<()> {
    let address = mounts.nfs.ok_or("no NFS listener")?;
    let (_, mount) = mounts.bind_host(&HostKey::generate()?).await?;
    let mut client = nfs_client::plain(address).await?;
    let root = client.export_root(&mount.mount_id).await?;
    let sub = client.mkdir(&root, "sub").await?;
    let hello = b"hello over NFSv4.1".to_vec();
    let (stateid, file) = client.open(&sub, "hello.txt", CREATOR).await?;
    client.write(&file, &stateid, &hello, UNSTABLE).await?;
    client.commit(&file).await?;
    client.close(&file, &stateid).await?;
    assert_eq!(
        mounts.get("sub/hello.txt").await?,
        hello,
        "the last close published"
    );
    assert_eq!(client.read_file(&sub, "hello.txt").await?, hello);

    let dest = client.mkdir(&root, "dest").await?;
    let (from, to) = (
        (sub.as_slice(), "hello.txt"),
        (dest.as_slice(), "hello.txt"),
    );
    client.rename(from, to).await?;
    assert_eq!(
        mounts.get("dest/hello.txt").await?,
        hello,
        "the move keeps the bytes"
    );
    assert!(
        mounts.missing("sub/hello.txt").await,
        "the old name is gone"
    );
    assert_eq!(client.read_file(&dest, "hello.txt").await?, hello);

    // An unpublished file renamed over a committed name replaces it atomically.
    mounts.put("live.txt", b"live bytes").await?;
    let draft = b"the draft replaces the live object".to_vec();
    let (stateid, file) = client.open(&root, "draft.txt", CREATOR).await?;
    client.write(&file, &stateid, &draft, UNSTABLE).await?;
    client.commit(&file).await?;
    let (from, to) = (
        (root.as_slice(), "draft.txt"),
        (root.as_slice(), "live.txt"),
    );
    client.rename(from, to).await?;
    client.close(&file, &stateid).await?;
    assert_eq!(mounts.get("live.txt").await?, draft);
    assert!(mounts.missing("draft.txt").await, "the draft name is gone");
    assert_eq!(client.read_file(&root, "live.txt").await?, draft);

    client.remove(&dest, "hello.txt").await?;
    assert!(
        mounts.missing("dest/hello.txt").await,
        "REMOVE deleted the object"
    );
    Ok(())
}

#[tokio::test]
async fn nfs_mtls_revocation() -> TestResult<()> {
    let (tls, authority) = server_tls()?;
    let mounts = Mounts::start(false, Some(tls)).await?;
    let result = revocation_steps(&mounts, &authority).await;
    mounts.shutdown().await;
    result
}

async fn revocation_steps(
    mounts: &Mounts,
    authority: &rustls::pki_types::CertificateDer<'static>,
) -> TestResult<()> {
    let address = mounts.nfs.ok_or("no NFS listener")?;
    let host = HostKey::generate()?;
    let (enrolled, mount) = mounts.bind_host(&host).await?;
    let report = b"report read over RPC-over-TLS".to_vec();
    mounts.put("report.txt", &report).await?;
    let stream = nfs_client::secure(address, authority, &host).await?;
    let (_, connection) = stream.get_ref();
    assert_eq!(
        connection.alpn_protocol(),
        Some(aruna_nfs::tls::SUNRPC_ALPN)
    );
    let mut client = NfsClient::start(stream).await?;
    let root = client.export_root(&mount.mount_id).await?;
    assert_eq!(client.read_file(&root, "report.txt").await?, report);

    mounts.revoke_host(&enrolled.host_id).await?;
    assert!(
        client.closed().await,
        "host revocation closes the live mTLS connection"
    );
    // The revoked key may finish TLS, but the server closes before any RPC is answered.
    let refused = match nfs_client::secure(address, authority, &host).await {
        Ok(stream) => NfsClient::start(stream).await.is_err(),
        Err(_) => true,
    };
    assert!(refused, "a revoked host key opens no NFS session");
    Ok(())
}
