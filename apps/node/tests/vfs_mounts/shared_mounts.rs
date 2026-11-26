//! One bucket through SMB, NFS and S3 at once: published bytes and S3 overwrites seen by both
//! file servers, NFS deny modes refusing SMB writers, and cold start over a corrupt checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_nfs::types::share;
use aruna_ops_data::vfs::recovery::{ColdStart, ColdStartOperation};
use aruna_runner::driver::{DriverContext, drive};
use aruna_smb::status;
use aruna_vfs::{VfsConfig, VfsHandle};
use ulid::Ulid;

use super::nfs_client::{self, CREATOR, FILE_SYNC, Share};
use super::smb_client::{FILE_CREATE, FILE_OPEN, NOT_DIRECTORY, READ_ATTRIBUTES, READ_DATA};
use super::smb_client::{SmbClient, WRITER};
use super::{BUCKET, HostKey, Mounts, loopback_tls};
use crate::shared::TestResult;

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
    Ok(drive(ColdStartOperation::new(), &context).await?)
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
