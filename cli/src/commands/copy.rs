//! Copies one file or syncs a folder between this machine and a bucket, checking BLAKE3.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::Context;
use crate::args::{CopyArgs, SyncArgs};
use crate::client::Api;
use crate::error::CliError;
use crate::output::human_size;
use crate::remote::{self, Placement, RemotePath};
use crate::sync::{self, Direction, Plan, RemoteFile};
use crate::transfer::{self, S3Access};
use serde::Serialize;
use std::path::{Path, PathBuf};

const MIB: u64 = 1024 * 1024;

/// Whether `source` to `target` is an upload: `s3://` decides, else a source that exists here.
pub fn is_upload(source: &str, target: &str) -> Result<bool, CliError> {
    match (remote::is_explicit(source), remote::is_explicit(target)) {
        (true, true) => Err(CliError::usage("one side must be a local path")),
        (true, false) => Ok(false),
        (false, true) => Ok(true),
        (false, false) => Ok(Path::new(source).exists()),
    }
}

/// One bucket's node and group; its S3 access opens on first use and `close` revokes it.
struct Bucket {
    placement: Placement,
    node_api: Api,
    fallback: Option<String>,
    read_only: bool,
    access: Option<S3Access>,
}

impl Bucket {
    async fn open(context: &Context<'_>, bucket: &str, read_only: bool) -> Result<Self, CliError> {
        let api = context.api()?;
        let (placement, node_api) = remote::locate(&api, bucket, context.node.as_deref()).await?;
        Ok(Self {
            placement,
            node_api,
            fallback: context.s3_endpoint.clone(),
            read_only,
            access: None,
        })
    }

    async fn access(&mut self) -> Result<&mut S3Access, CliError> {
        let access = match self.access.take() {
            Some(access) => access,
            None => {
                S3Access::open(
                    &self.node_api,
                    &self.placement.group_id,
                    self.fallback.as_deref(),
                    self.read_only,
                )
                .await?
            }
        };
        Ok(self.access.insert(access))
    }

    async fn close(self) {
        if let Some(access) = self.access {
            access.revoke().await;
        }
    }
}

#[derive(Debug, Serialize)]
struct Copied {
    direction: Direction,
    local: String,
    bucket: String,
    key: String,
    node_id: String,
    size: u64,
    blake3: String,
    verified: bool,
    resumed: u64,
}

pub async fn copy(context: &mut Context<'_>, args: CopyArgs) -> Result<(), CliError> {
    let part_size = args.part_size * MIB;
    let copied = match is_upload(&args.source, &args.target)? {
        true => {
            let local = PathBuf::from(&args.source);
            if !local.is_file() {
                return Err(CliError::usage(format!(
                    "{} is not a file; use sync for folders",
                    local.display()
                )));
            }
            let mut target = RemotePath::parse(&args.target)?;
            if target.key.is_empty() || target.key.ends_with('/') {
                let name = local.file_name().unwrap_or_default().to_string_lossy();
                target.key = format!("{}{name}", target.key);
            }
            let mut bucket = Bucket::open(context, &target.bucket, false).await?;
            let copied = upload_one(&mut bucket, &local, &target.key, part_size).await;
            bucket.close().await;
            copied?
        }
        false => {
            let source = RemotePath::parse(&args.source)?;
            if source.key.is_empty() || source.key.ends_with('/') {
                return Err(CliError::usage(
                    "cp copies one object; use sync for folders",
                ));
            }
            let mut dest = PathBuf::from(&args.target);
            if dest.is_dir() || args.target.ends_with(['/', std::path::MAIN_SEPARATOR]) {
                let name = source.key.rsplit('/').next().unwrap_or(&source.key);
                dest = sync::local_path(&dest, name)?;
            }
            let mut bucket = Bucket::open(context, &source.bucket, true).await?;
            let copied = download_one(&mut bucket, &source, &dest).await;
            bucket.close().await;
            copied?
        }
    };
    if context.out.json {
        return context.out.json(&copied);
    }
    let arrow = match copied.direction {
        Direction::Upload => format!("{} -> {}/{}", copied.local, copied.bucket, copied.key),
        Direction::Download => format!("{}/{} -> {}", copied.bucket, copied.key, copied.local),
    };
    let check = match copied.verified {
        true => "BLAKE3 verified",
        false => "size checked",
    };
    context
        .out
        .line(format!("{arrow} ({}, {check})", human_size(copied.size)))
}

/// Downloads one object and checks it against the size and BLAKE3 the node lists.
async fn download_one(
    bucket: &mut Bucket,
    source: &RemotePath,
    dest: &Path,
) -> Result<Copied, CliError> {
    let entry = remote::find_object(&bucket.node_api, &source.bucket, &source.key)
        .await?
        .ok_or_else(|| CliError::usage(format!("no object {}/{}", source.bucket, source.key)))?;
    if entry.blake3().is_none() {
        eprintln!(
            "warning: the node has no BLAKE3 for {}; only the size is checked",
            entry.key
        );
    }
    let client = bucket.access().await?.client().await?;
    let done = transfer::download(
        &client,
        &source.bucket,
        &source.key,
        dest,
        entry.size,
        entry.blake3(),
    )
    .await?;
    Ok(Copied {
        direction: Direction::Download,
        local: dest.display().to_string(),
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        node_id: bucket.placement.node_id.clone(),
        size: done.size,
        blake3: done.blake3,
        verified: entry.blake3().is_some(),
        resumed: 0,
    })
}

/// Uploads one file and checks the node's BLAKE3 of the stored object against the local one.
async fn upload_one(
    bucket: &mut Bucket,
    local: &Path,
    key: &str,
    part_size: u64,
) -> Result<Copied, CliError> {
    let name = bucket.placement.bucket.clone();
    let done = transfer::upload(bucket.access().await?, &name, key, local, part_size).await?;
    let stored = remote::find_object(&bucket.node_api, &name, key).await?;
    let verified = verify_stored(stored.as_ref(), key, done.size, &done.blake3)?;
    Ok(Copied {
        direction: Direction::Upload,
        local: local.display().to_string(),
        bucket: name,
        key: key.to_string(),
        node_id: bucket.placement.node_id.clone(),
        size: done.size,
        blake3: done.blake3,
        verified,
        resumed: done.reused_parts as u64,
    })
}

/// Fails when the stored object differs; false when the node gives no BLAKE3 to compare.
pub fn verify_stored(
    stored: Option<&remote::ObjectEntry>,
    key: &str,
    size: u64,
    blake3: &str,
) -> Result<bool, CliError> {
    let stored = stored
        .ok_or_else(|| CliError::S3(format!("{key} was uploaded but is not listed on the node")))?;
    if stored.size.is_some_and(|stored| stored != size) {
        return Err(CliError::Checksum {
            path: key.to_string(),
            expected: format!("{size} bytes"),
            actual: format!("{} bytes", stored.size.unwrap_or_default()),
        });
    }
    match stored.blake3() {
        Some(digest) if !digest.eq_ignore_ascii_case(blake3) => Err(CliError::Checksum {
            path: key.to_string(),
            expected: blake3.to_string(),
            actual: digest.to_string(),
        }),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

#[derive(Debug, Serialize)]
struct SyncReport<'a> {
    direction: Direction,
    bucket: &'a str,
    prefix: &'a str,
    node_id: &'a str,
    dry_run: bool,
    plan: &'a Plan,
}

pub async fn sync(context: &mut Context<'_>, args: SyncArgs) -> Result<(), CliError> {
    let direction = match is_upload(&args.source, &args.target)? {
        true => Direction::Upload,
        false => Direction::Download,
    };
    let (local_raw, remote_raw) = match direction {
        Direction::Upload => (&args.source, &args.target),
        Direction::Download => (&args.target, &args.source),
    };
    let root = PathBuf::from(local_raw);
    if direction == Direction::Upload && !root.is_dir() {
        return Err(CliError::usage(format!(
            "{} is not a folder",
            root.display()
        )));
    }
    let target = RemotePath::parse(remote_raw)?;
    let prefix = target.folder();
    let read_only = direction == Direction::Download;
    let mut bucket = Bucket::open(context, &target.bucket, read_only).await?;
    let listed = remote::list_all(&bucket.node_api, &target.bucket, &prefix).await?;
    let remote_files: Vec<RemoteFile> = listed
        .iter()
        .filter(|entry| !entry.key.ends_with('/'))
        .filter_map(|entry| {
            Some(RemoteFile {
                path: entry.key.strip_prefix(&prefix)?.to_string(),
                size: entry.size.unwrap_or_default(),
                blake3: entry.blake3().map(str::to_string),
            })
        })
        .collect();
    let local_files = match root.is_dir() {
        true => sync::walk_local(&root)?,
        false => Vec::new(),
    };
    if direction == Direction::Download {
        check_targets(&root, &remote_files, &local_files)?;
    }
    let plan = {
        let root = root.clone();
        tokio::task::spawn_blocking(move || {
            sync::plan(direction, &local_files, &remote_files, |path| {
                let digest = blake3::Hasher::new()
                    .update_reader(std::fs::File::open(root.join(path))?)?
                    .finalize();
                Ok(digest.to_hex().to_string())
            })
        })
        .await
        .map_err(|error| CliError::Io(std::io::Error::other(error)))??
    };
    let node_id = bucket.placement.node_id.clone();
    let report = SyncReport {
        direction,
        bucket: &target.bucket,
        prefix: &prefix,
        node_id: &node_id,
        dry_run: args.dry_run,
        plan: &plan,
    };
    if context.out.json && args.dry_run {
        return context.out.json(&report);
    }
    if !context.out.json {
        print_plan(context, direction, &plan)?;
    }
    if args.dry_run {
        return Ok(());
    }
    let part_size = args.part_size * MIB;
    let done = async {
        for item in &plan.transfers {
            let key = format!("{prefix}{}", item.path);
            match direction {
                Direction::Upload => {
                    upload_one(&mut bucket, &root.join(&item.path), &key, part_size).await?;
                }
                Direction::Download => {
                    let local = sync::local_path(&root, &item.path)?;
                    let expected = remote_digest(&listed, &key);
                    let client = bucket.access().await?.client().await?;
                    let name = &bucket.placement.bucket;
                    let size = Some(item.size);
                    transfer::download(&client, name, &key, &local, size, expected).await?;
                }
            }
            if !context.out.json {
                context.out.line(format!("done {}", item.path))?;
            }
        }
        Ok::<(), CliError>(())
    }
    .await;
    bucket.close().await;
    done?;
    match context.out.json {
        true => context.out.json(&report),
        false => context.out.line(format!(
            "Synced {} files ({}).",
            plan.transfers.len(),
            human_size(plan.bytes())
        )),
    }
}

/// Refuses a download whose keys leave `root` or overwrite each other on some file systems.
fn check_targets(
    root: &Path,
    remote_files: &[RemoteFile],
    local_files: &[sync::LocalFile],
) -> Result<(), CliError> {
    let unsafe_keys: Vec<&str> = remote_files
        .iter()
        .map(|file| file.path.as_str())
        .filter(|path| !sync::is_safe_key(path))
        .collect();
    if !unsafe_keys.is_empty() {
        return Err(CliError::usage(format!(
            "these keys cannot be saved below {}: {}",
            root.display(),
            unsafe_keys.join(", ")
        )));
    }
    let paths = remote_files.iter().map(|file| file.path.as_str());
    let clashes =
        sync::case_clashes(paths.chain(local_files.iter().map(|file| file.path.as_str())));
    match clashes.is_empty() {
        true => Ok(()),
        false => Err(CliError::usage(format!(
            "these paths name the same file on some systems and would overwrite each other: {}",
            clashes.join(", ")
        ))),
    }
}

fn remote_digest<'a>(listed: &'a [remote::ObjectEntry], key: &str) -> Option<&'a str> {
    listed.iter().find(|entry| entry.key == key)?.blake3()
}

fn print_plan(
    context: &mut Context<'_>,
    direction: Direction,
    plan: &Plan,
) -> Result<(), CliError> {
    let verb = match direction {
        Direction::Upload => "upload",
        Direction::Download => "download",
    };
    for item in &plan.transfers {
        let reason = match item.reason {
            sync::Reason::Missing => "new",
            sync::Reason::Size => "size differs",
            sync::Reason::Checksum => "content differs",
        };
        context.out.line(format!(
            "{verb} {} ({}, {reason})",
            item.path,
            human_size(item.size)
        ))?;
    }
    context.out.line(format!(
        "{} to {verb} ({}), {} unchanged.",
        plan.transfers.len(),
        human_size(plan.bytes()),
        plan.same
    ))?;
    if !plan.unverified.is_empty() {
        context.out.line(format!(
            "{} have the same size but no BLAKE3 on the node, so they are skipped.",
            plan.unverified.len()
        ))?;
    }
    if !plan.extra.is_empty() {
        context.out.line(format!(
            "{} exist only on the target and are kept.",
            plan.extra.len()
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(size: u64, blake3: Option<&str>) -> remote::ObjectEntry {
        remote::ObjectEntry {
            key: "k".into(),
            size: Some(size),
            etag: None,
            last_modified: None,
            content_type: None,
            content_id: blake3.map(|digest| format!("https://w3id.org/aruna/data/{digest}")),
        }
    }

    #[test]
    fn decides_direction() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().to_string_lossy().to_string();
        assert!(is_upload(&local, "data/x").unwrap());
        assert!(!is_upload("data/x", &local).unwrap());
        assert!(!is_upload("s3://data/x", "missing-here").unwrap());
        assert!(is_upload("missing-here", "s3://data/x").unwrap());
        assert!(is_upload("s3://a/b", "s3://c/d").is_err());
    }

    #[test]
    fn verifies_stored_digest() {
        assert!(verify_stored(Some(&entry(3, Some("abc"))), "k", 3, "ABC").unwrap());
        assert!(!verify_stored(Some(&entry(3, None)), "k", 3, "abc").unwrap());
        let wrong = verify_stored(Some(&entry(3, Some("def"))), "k", 3, "abc").unwrap_err();
        assert!(wrong.to_string().contains("checksum mismatch"), "{wrong}");
        assert!(verify_stored(Some(&entry(4, Some("abc"))), "k", 3, "abc").is_err());
        assert!(verify_stored(None, "k", 3, "abc").is_err());
    }
}
