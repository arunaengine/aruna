//! One bucket through SMB, NFS and S3 at once: publications seen by fresh and open handles, SMB
//! writes after an NFS rename, NFS deny modes and host revocation, and cold start recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_nfs::types::share;
use aruna_ops_data::vfs::recovery::{ColdStart, ColdStartOperation};
use aruna_runner::driver::{DriverContext, drive};
use aruna_smb::status;
use aruna_vfs::{VfsConfig, VfsHandle};
use rustls::pki_types::CertificateDer;
use ulid::Ulid;

use super::nfs_client::{self, CREATOR, FILE_SYNC, NfsClient, READER, Share, UNSTABLE};
use super::smb_client::{DELETE, FILE_CREATE, FILE_OPEN, NOT_DIRECTORY, READ_ATTRIBUTES};
use super::smb_client::{READ_DATA, SmbClient, WRITER};
use super::{BUCKET, HostKey, Mounts, loopback_tls, server_tls};
use crate::shared::{TestResult, WAIT_CAP, wait_until};

#[tokio::test]
async fn cross_protocol_mount() -> TestResult<()> {
    let mounts = Mounts::start(true, Some(loopback_tls())).await?;
    let result = cross_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

async fn cross_steps(mounts: &Mounts) -> TestResult<()> {
    let smb_address = mounts.smb.ok_or("no SMB listener")?;
    let nfs_address = mounts.nfs.ok_or("no NFS listener")?;
    let (_, mount) = mounts.bind_host(&HostKey::generate()?).await?;
    let mut smb = SmbClient::login(smb_address, &mounts.keys, BUCKET).await?;
    let mut nfs = nfs_client::plain(nfs_address).await?;
    let root = nfs.export_root(&mount.mount_id).await?;

    let first = b"written and flushed over SMB".to_vec();
    let file = smb
        .open("shared.txt", WRITER, FILE_CREATE, NOT_DIRECTORY)
        .await?;
    smb.write(file, 0, &first).await?;
    smb.flush(file).await?;
    smb.close(file).await?;
    assert_eq!(
        nfs.read_file(&root, "shared.txt").await?,
        first,
        "NFS reads SMB bytes"
    );
    assert_eq!(mounts.get("shared.txt").await?, first, "S3 reads SMB bytes");

    // Storage reports the S3 head change, so both file servers serve the new version.
    let second = b"overwritten through S3 with a longer body".to_vec();
    mounts.put("shared.txt", &second).await?;
    assert_eq!(
        nfs.read_file(&root, "shared.txt").await?,
        second,
        "NFS sees the S3 write"
    );
    let reader = READ_DATA | READ_ATTRIBUTES;
    let file = smb
        .open("shared.txt", reader, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    assert_eq!(
        smb.read_all(file, second.len()).await?,
        second,
        "SMB sees the S3 write"
    );
    smb.close(file).await?;

    // An NFS open that denies writers refuses an SMB writer until it closes.
    mounts.put("locked.txt", b"shared lock target").await?;
    let deny = Share {
        access: share::READ,
        deny: share::WRITE,
        create: false,
    };
    let (stateid, locked) = nfs.open(&root, "locked.txt", deny).await?;
    let refused = smb
        .create("locked.txt", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    assert_eq!(
        refused.status(),
        status::SHARING_VIOLATION,
        "the NFS deny mode holds"
    );
    nfs.close(&locked, &stateid).await?;
    let file = smb
        .open("locked.txt", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    smb.close(file).await?;
    Ok(())
}

#[tokio::test]
async fn smb_tracks_rename() -> TestResult<()> {
    let mounts = Mounts::start(true, Some(loopback_tls())).await?;
    let result = tracking_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

async fn tracking_steps(mounts: &Mounts) -> TestResult<()> {
    let smb_address = mounts.smb.ok_or("no SMB listener")?;
    let nfs_address = mounts.nfs.ok_or("no NFS listener")?;
    let (_, mount) = mounts.bind_host(&HostKey::generate()?).await?;
    let mut smb = SmbClient::login(smb_address, &mounts.keys, BUCKET).await?;
    let mut nfs = nfs_client::plain(nfs_address).await?;
    let root = nfs.export_root(&mount.mount_id).await?;

    // An SMB writer keeps its open handle while NFS renames the committed file under it.
    mounts.put("before.txt", b"committed").await?;
    let file = smb
        .open("before.txt", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    nfs.rename((&root, "before.txt"), (&root, "after.txt"))
        .await?;
    let written = b"written through the handle opened before the rename".to_vec();
    smb.write(file, 0, &written).await?;
    smb.flush(file).await?;
    smb.close(file).await?;
    assert_eq!(
        mounts.get("after.txt").await?,
        written,
        "the SMB write publishes at the new name"
    );
    assert!(
        mounts.missing("before.txt").await,
        "the old name stays gone"
    );
    Ok(())
}

#[tokio::test]
async fn live_shared_opens() -> TestResult<()> {
    let (tls, authority) = server_tls()?;
    let mounts = Mounts::start(true, Some(tls)).await?;
    let result = live_steps(&mounts, &authority).await;
    mounts.shutdown().await;
    result
}

/// Whether an SMB writer open of `name` is granted now; a granted open is closed again.
async fn writer_opens(client: &mut SmbClient, name: &str) -> bool {
    match client.open(name, WRITER, FILE_OPEN, NOT_DIRECTORY).await {
        Ok(file) => client.close(file).await.is_ok(),
        Err(_) => false,
    }
}

async fn live_steps(mounts: &Mounts, authority: &CertificateDer<'static>) -> TestResult<()> {
    let smb_address = mounts.smb.ok_or("no SMB listener")?;
    let nfs_address = mounts.nfs.ok_or("no NFS listener")?;
    let host = HostKey::generate()?;
    let (enrolled, mount) = mounts.bind_host(&host).await?;
    let mut smb = SmbClient::login(smb_address, &mounts.keys, BUCKET).await?;
    let stream = nfs_client::secure(nfs_address, authority, &host).await?;
    let mut nfs = NfsClient::start(stream).await?;
    let root = nfs.export_root(&mount.mount_id).await?;

    // An NFS reader kept open across an SMB FLUSH reads the published bytes without reopening.
    let first = b"committed before either open".to_vec();
    mounts.put("live.txt", &first).await?;
    let (reader, live) = nfs.open(&root, "live.txt", READER).await?;
    assert_eq!(nfs.read(&live, &reader).await?, first, "NFS reads the base");
    let second = b"written over SMB while the NFS handle stays open".to_vec();
    let file = smb
        .open("live.txt", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    smb.write(file, 0, &second).await?;
    smb.flush(file).await?;
    assert_eq!(
        nfs.read(&live, &reader).await?,
        second,
        "the open NFS handle reads the SMB publication"
    );
    smb.close(file).await?;
    nfs.close(&live, &reader).await?;
    assert_eq!(mounts.get("live.txt").await?, second, "S3 reads SMB bytes");

    // An NFS create that denies writers keeps that deny mode against SMB until it closes.
    let creator = Share {
        access: share::BOTH,
        deny: share::WRITE,
        create: true,
    };
    let (stateid, fresh) = nfs.open(&root, "fresh.txt", creator).await?;
    nfs.write(&fresh, &stateid, b"created over NFS", UNSTABLE)
        .await?;
    let refused = smb
        .create("fresh.txt", WRITER, FILE_OPEN, NOT_DIRECTORY)
        .await?;
    assert_eq!(
        refused.status(),
        status::SHARING_VIOLATION,
        "the NFS create keeps its deny mode"
    );
    nfs.close(&fresh, &stateid).await?;
    assert!(
        writer_opens(&mut smb, "fresh.txt").await,
        "SMB writes after the close"
    );

    // Revoking the host ends its connection and releases its deny-write open for SMB.
    let holder = Share {
        access: share::READ,
        deny: share::WRITE,
        create: false,
    };
    nfs.open(&root, "fresh.txt", holder).await?;
    assert!(
        !writer_opens(&mut smb, "fresh.txt").await,
        "the NFS deny mode holds"
    );
    mounts.revoke_host(&enrolled.host_id).await?;
    assert!(
        nfs.closed().await,
        "host revocation closes the NFS connection"
    );
    let smb = tokio::sync::Mutex::new(smb);
    let shared = &smb;
    wait_until(
        "an SMB writer opens the file a revoked NFS host held",
        WAIT_CAP,
        move || async move { writer_opens(&mut *shared.lock().await, "fresh.txt").await },
    )
    .await
}

/// The CREATE option `FILE_DELETE_ON_CLOSE`, which removes the file at its last close.
const DELETE_ON_CLOSE: u32 = 0x0000_1000;

#[tokio::test]
async fn nfs_finishes_delete() -> TestResult<()> {
    let mounts = Mounts::start(true, Some(loopback_tls())).await?;
    let result = finish_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

/// An SMB delete-on-close under an implicit directory waits for an NFS reader that stays open,
/// and the final NFS close removes the child while preserving the parent marker.
async fn finish_steps(mounts: &Mounts) -> TestResult<()> {
    let smb_address = mounts.smb.ok_or("no SMB listener")?;
    let nfs_address = mounts.nfs.ok_or("no NFS listener")?;
    let (_, mount) = mounts.bind_host(&HostKey::generate()?).await?;
    let mut smb = SmbClient::login(smb_address, &mounts.keys, BUCKET).await?;
    let mut nfs = nfs_client::plain(nfs_address).await?;
    let root = nfs.export_root(&mount.mount_id).await?;

    mounts
        .put("implicit/cross.txt", b"deleted across protocols")
        .await?;
    let dir = nfs.lookup(&root, "implicit").await?;
    let (reader, file) = nfs.open(&dir, "cross.txt", READER).await?;
    let access = DELETE | READ_ATTRIBUTES;
    let options = NOT_DIRECTORY | DELETE_ON_CLOSE;
    let doomed = smb
        .open("implicit\\cross.txt", access, FILE_OPEN, options)
        .await?;
    smb.close(doomed).await?;
    assert!(
        mounts.exists("implicit/cross.txt").await,
        "the open NFS reader keeps the object"
    );
    nfs.close(&file, &reader).await?;
    assert!(
        mounts.missing("implicit/cross.txt").await,
        "the final NFS close removed the object"
    );
    assert!(
        mounts.exists("implicit/").await,
        "the final NFS close preserved the implicit parent marker"
    );
    Ok(())
}

#[tokio::test]
async fn ro_nfs_deletes() -> TestResult<()> {
    let mounts = Mounts::start(true, Some(loopback_tls())).await?;
    let result = ro_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

/// An SMB delete-on-close waits for a reader on a read-only NFS mount, and that reader's final
/// close completes the authorized delete while preserving the implicit parent marker.
async fn ro_steps(mounts: &Mounts) -> TestResult<()> {
    let smb_address = mounts.smb.ok_or("no SMB listener")?;
    let nfs_address = mounts.nfs.ok_or("no NFS listener")?;
    mounts.put("implicit/ro.txt", b"delete from ro nfs").await?;
    let (_, mount) = mounts.bind_ro_host(&HostKey::generate()?).await?;
    let mut nfs = nfs_client::plain(nfs_address).await?;
    let root = nfs.export_root(&mount.mount_id).await?;
    let dir = nfs.lookup(&root, "implicit").await?;
    let (reader, file) = nfs.open(&dir, "ro.txt", READER).await?;
    let mut smb = SmbClient::login(smb_address, &mounts.keys, BUCKET).await?;
    let access = DELETE | READ_ATTRIBUTES;
    let options = NOT_DIRECTORY | DELETE_ON_CLOSE;
    let doomed = smb
        .open("implicit\\ro.txt", access, FILE_OPEN, options)
        .await?;
    smb.close(doomed).await?;
    assert!(
        mounts.exists("implicit/ro.txt").await,
        "the open read-only NFS reader keeps the object"
    );
    nfs.close(&file, &reader).await?;
    assert!(
        mounts.missing("implicit/ro.txt").await,
        "the final read-only NFS close completed the SMB delete"
    );
    assert!(
        mounts.exists("implicit/").await,
        "the final read-only NFS close preserved the implicit parent marker"
    );
    Ok(())
}

#[tokio::test]
async fn cold_start_recovery() -> TestResult<()> {
    let mounts = Mounts::start(false, Some(loopback_tls())).await?;
    let result = recovery_steps(&mounts).await;
    mounts.shutdown().await;
    result
}

/// The checkpoint files `chk-<stage>-<generation>.bin` in `stage_dir` and their stage ids.
fn checkpoints(stage_dir: &Path) -> TestResult<Vec<(PathBuf, Ulid)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(stage_dir)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let stem = name
            .strip_prefix("chk-")
            .and_then(|rest| rest.strip_suffix(".bin"));
        if let Some((stage, _)) = stem.and_then(|stem| stem.split_once('-')) {
            let stage_id = Ulid::from_string(stage)?;
            found.push((path, stage_id));
        }
    }
    Ok(found)
}

/// Opens a fresh file server state on the seed's stage folder, as a restarted node does, and
/// runs the startup recovery against the seed's storage.
async fn cold_start(mounts: &Mounts, stage_dir: &Path) -> TestResult<ColdStart> {
    let vfs = VfsHandle::open(VfsConfig {
        stage_dir: stage_dir.to_path_buf(),
        node_stage_bytes: 1 << 30,
        group_stage_bytes: 1 << 28,
    })?;
    let context = DriverContext {
        vfs_handle: Some(vfs),
        ..mounts.seed.context.as_ref().clone()
    };
    let cold =
        ColdStartOperation::new().with_admission(mounts.seed.realm_id, mounts.seed.net.node_id());
    Ok(drive(cold, &context).await?)
}

async fn recovery_steps(mounts: &Mounts) -> TestResult<()> {
    let address = mounts.nfs.ok_or("no NFS listener")?;
    let stage_dir = mounts
        .seed
        .vfs_stage
        .clone()
        .ok_or("the seed has no stage folder")?;
    let (_, mount) = mounts.bind_host(&HostKey::generate()?).await?;
    let mut client = nfs_client::plain(address).await?;
    let root = client.export_root(&mount.mount_id).await?;
    // A stable write of an open, unpublished file leaves a live durable checkpoint.
    let staged = b"staged bytes that outlive a restart".to_vec();
    let (stateid, file) = client.open(&root, "draft.bin", CREATOR).await?;
    client.write(&file, &stateid, &staged, FILE_SYNC).await?;
    let written = checkpoints(&stage_dir)?;
    let stage_id = written.first().map(|(_, stage_id)| *stage_id);
    let stage_id = stage_id.ok_or("the stable write left no checkpoint file")?;
    assert!(
        written.iter().all(|(_, other)| *other == stage_id),
        "one stage was written"
    );

    let report = cold_start(mounts, &stage_dir).await?;
    assert!(
        report.corrupt.is_empty(),
        "intact checkpoints recover: {report:?}"
    );
    assert!(
        report.restored >= 1,
        "the live stage is restored: {report:?}"
    );

    for (path, _) in checkpoints(&stage_dir)? {
        std::fs::write(path, b"not a checkpoint")?;
    }
    let report = cold_start(mounts, &stage_dir).await?;
    assert!(
        report.corrupt.contains(&stage_id),
        "startup fails closed: {report:?}"
    );
    Ok(())
}
