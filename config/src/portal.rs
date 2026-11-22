//! Downloads, verifies and unpacks the portal web artifact and reports its install status.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::PortalArtifactConfig;
use aruna_wire::info::PortalStatus;
use chrono::{SecondsFormat, Utc};
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs;
use std::io::{self, Cursor};
use std::path::{Component, Path, PathBuf, Prefix};
use std::time::{SystemTime, UNIX_EPOCH};
use tar::Archive;
use thiserror::Error;

const MANIFEST_FILE: &str = "portal-manifest.json";
const CHECKSUM_FILE: &str = "aruna-portal-dist.tar.gz.sha256";
/// Marks a recovery directory this installer created; the old portal waits beside it.
const BACKUP_MARKER: &str = "aruna-portal-backup";
const BACKUP_DIR: &str = "portal";
/// Bounds of one portal download, so a stalled or oversized answer cannot hang or exhaust a node.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const MAX_ARTIFACT_BYTES: usize = 256 << 20;
const MAX_CHECKSUM_BYTES: usize = 4 << 10;

pub async fn update_artifact(
    config: PortalArtifactConfig,
) -> Result<PortalStatus, PortalArtifactError> {
    download_and_install(config).await
}

/// The installed artifact, or a fresh install of it; a failure is reported in the status.
pub async fn prepare_artifact_status(config: PortalArtifactConfig) -> PortalStatus {
    let mut cache_error = None;
    match load_existing_status(&config) {
        Ok(Some(status)) => return status,
        Ok(None) => {}
        Err(error) => cache_error = Some(error.to_string()),
    }

    match download_and_install(config.clone()).await {
        Ok(status) => status,
        Err(error) => {
            let mut status = artifact_status_base(&config, false);
            status.last_error = Some(match cache_error {
                Some(cache_error) => {
                    format!(
                        "cached portal artifact is invalid: {cache_error}; update failed: {error}"
                    )
                }
                None => error.to_string(),
            });
            status
        }
    }
}

async fn download_and_install(
    config: PortalArtifactConfig,
) -> Result<PortalStatus, PortalArtifactError> {
    let artifact_url = config
        .artifact_url
        .clone()
        .ok_or(PortalArtifactError::MissingArtifactUrl)?;
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(DOWNLOAD_TIMEOUT)
        .build()?;
    let checksum_file =
        download_checksum_file(&client, &artifact_url, config.artifact_sha256.as_deref()).await?;

    let response = client.get(&artifact_url).send().await?;
    if !response.status().is_success() {
        return Err(PortalArtifactError::HttpStatus(response.status().as_u16()));
    }
    let bytes = read_capped(response, MAX_ARTIFACT_BYTES).await?;
    verify_sha256(&bytes, &checksum_file.checksum)?;
    let fetched_at = current_timestamp();

    tokio::task::spawn_blocking(move || {
        install_verified_artifact(&config, &bytes, checksum_file, fetched_at)
    })
    .await?
}

async fn download_checksum_file(
    client: &reqwest::Client,
    artifact_url: &str,
    pinned_checksum: Option<&str>,
) -> Result<DownloadedChecksumFile, PortalArtifactError> {
    let checksum_url = format!("{artifact_url}.sha256");
    let response = client.get(&checksum_url).send().await?;
    if !response.status().is_success() {
        return Err(PortalArtifactError::ChecksumHttpStatus(
            response.status().as_u16(),
        ));
    }

    let bytes = read_capped(response, MAX_CHECKSUM_BYTES).await?;
    let checksum = parse_checksum_file(&bytes)?;
    if let Some(pinned_checksum) = pinned_checksum.map(str::to_ascii_lowercase)
        && checksum != pinned_checksum
    {
        return Err(PortalArtifactError::ChecksumSidecarMismatch {
            expected: pinned_checksum,
            actual: checksum,
        });
    }

    Ok(DownloadedChecksumFile { bytes, checksum })
}

/// The body of `response`, refused once it passes `limit` bytes.
async fn read_capped(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, PortalArtifactError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(PortalArtifactError::TooLarge(limit));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn install_verified_artifact(
    config: &PortalArtifactConfig,
    archive_bytes: &[u8],
    checksum_file: DownloadedChecksumFile,
    fetched_at: String,
) -> Result<PortalStatus, PortalArtifactError> {
    let portal_dir = portal_dir(config);
    let parent = install_parent(&portal_dir)?;
    fs::create_dir_all(&parent)?;
    let temp_dir = TempInstallDir::create(&parent)?;

    unpack_archive(archive_bytes, temp_dir.path())?;
    require_index_html(temp_dir.path())?;
    let manifest = read_manifest(temp_dir.path())?;
    write_checksum_file(temp_dir.path(), &checksum_file.bytes)?;

    // The old portal stays live until the new one replaces it, and comes back on a failure.
    let previous = previous_install(&portal_dir)?;
    let _locked = lock_install(&previous)?;
    restore_previous(&portal_dir, &previous)?;
    if !existing_install(&portal_dir)? {
        temp_dir.persist(&portal_dir)?;
    } else if let Err(error) = exchange_dirs(temp_dir.path(), &portal_dir) {
        if error.kind() != io::ErrorKind::Unsupported {
            return Err(error.into());
        }
        replace_portable(temp_dir, &portal_dir, &previous)?;
    }

    Ok(status_from_parts(
        config,
        true,
        Some(checksum_file.checksum),
        manifest
            .as_ref()
            .and_then(|manifest| manifest.version.clone()),
        manifest
            .as_ref()
            .and_then(|manifest| manifest.source.clone()),
        Some(fetched_at),
        None,
    ))
}

fn load_existing_status(
    config: &PortalArtifactConfig,
) -> Result<Option<PortalStatus>, PortalArtifactError> {
    let portal_dir = portal_dir(config);
    validate_install_target(&portal_dir)?;
    let previous = previous_install(&portal_dir)?;
    if owned_backup(&previous) {
        let _locked = lock_install(&previous)?;
        restore_previous(&portal_dir, &previous)?;
    }
    if !portal_dir.join("index.html").is_file() {
        return Ok(None);
    }

    Ok(Some(installed_status(config, &portal_dir)))
}

fn installed_status(config: &PortalArtifactConfig, dir: &Path) -> PortalStatus {
    let manifest = read_manifest(dir).ok().flatten();
    let checksum = read_checksum_file(dir).or_else(|| config.artifact_sha256.clone());
    let version = manifest
        .as_ref()
        .and_then(|manifest| manifest.version.clone());
    let source = manifest
        .as_ref()
        .and_then(|manifest| manifest.source.clone());
    let fetched_at = manifest
        .as_ref()
        .and_then(|manifest| manifest.fetched_at.clone());

    status_from_parts(config, true, checksum, version, source, fetched_at, None)
}

pub fn artifact_status_base(config: &PortalArtifactConfig, installed: bool) -> PortalStatus {
    status_from_parts(
        config,
        installed,
        config.artifact_sha256.clone(),
        None,
        None,
        None,
        None,
    )
}

fn status_from_parts(
    config: &PortalArtifactConfig,
    installed: bool,
    checksum: Option<String>,
    version: Option<String>,
    source: Option<String>,
    fetched_at: Option<String>,
    last_error: Option<String>,
) -> PortalStatus {
    PortalStatus {
        installed,
        mode: "artifact".to_string(),
        version,
        source,
        url: config.artifact_url.clone(),
        checksum,
        fetched_at,
        last_error,
    }
}

pub fn portal_dir(config: &PortalArtifactConfig) -> PathBuf {
    config.portal_dir.clone()
}

fn install_parent(path: &Path) -> Result<PathBuf, PortalArtifactError> {
    validate_install_target(path)?;

    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .or_else(|| {
            if path.is_relative() {
                Some(PathBuf::from("."))
            } else {
                None
            }
        })
        .ok_or_else(|| PortalArtifactError::InvalidPortalDirectory(path.to_path_buf()))
}

fn validate_install_target(path: &Path) -> Result<(), PortalArtifactError> {
    if path.as_os_str().is_empty() {
        return Err(PortalArtifactError::InvalidPortalDirectory(
            path.to_path_buf(),
        ));
    }

    let mut normal_components = 0;
    for component in path.components() {
        match component {
            Component::Normal(_) => normal_components += 1,
            Component::CurDir | Component::RootDir => {}
            Component::Prefix(prefix)
                if path.is_absolute() && matches!(prefix.kind(), Prefix::Disk(_)) => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(PortalArtifactError::InvalidPortalDirectory(
                    path.to_path_buf(),
                ));
            }
        }
    }

    if normal_components == 0 || path.has_root() && normal_components == 1 {
        return Err(PortalArtifactError::InvalidPortalDirectory(
            path.to_path_buf(),
        ));
    }

    Ok(())
}

/// Answers whether an installed portal is at `path`; anything else there is refused.
fn existing_install(path: &Path) -> Result<bool, PortalArtifactError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && can_replace_dir(path)? => Ok(true),
        Ok(_) => Err(PortalArtifactError::InvalidPortalDirectory(
            path.to_path_buf(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// The one recovery directory where a portable replacement keeps the old portal of `path`.
fn previous_install(path: &Path) -> Result<PathBuf, PortalArtifactError> {
    let name = path
        .file_name()
        .ok_or_else(|| PortalArtifactError::InvalidPortalDirectory(path.to_path_buf()))?;
    let mut previous = OsString::from(".");
    previous.push(name);
    previous.push(".previous");
    Ok(path.with_file_name(previous))
}

/// Puts the old portal back when an interrupted replacement left `path` missing.
fn restore_previous(path: &Path, previous: &Path) -> Result<(), PortalArtifactError> {
    let backup = previous.join(BACKUP_DIR);
    if fs::symlink_metadata(path).is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
        && owned_backup(previous)
        && fs::symlink_metadata(&backup).is_ok_and(|metadata| metadata.is_dir())
    {
        rename_new(&backup, path)?;
        let _ = fs::remove_dir_all(previous);
    }
    Ok(())
}

/// Answers whether this installer created the recovery directory `previous`.
fn owned_backup(previous: &Path) -> bool {
    fs::symlink_metadata(previous).is_ok_and(|metadata| metadata.is_dir())
        && fs::symlink_metadata(previous.join(BACKUP_MARKER))
            .is_ok_and(|metadata| metadata.is_file())
}

/// Holds the install lock of the portal whose recovery directory is `previous` until the file
/// closes or the process exits, so recovery and replacement run one at a time across processes.
fn lock_install(previous: &Path) -> io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(previous.with_extension("lock"))?;
    file.lock()?;
    Ok(file)
}

/// Publishes the marked recovery directory `previous` whole with one rename of a staged copy, so
/// an interrupted claim leaves no unmarked directory there; another directory there stays.
fn claim_backup(previous: &Path) -> Result<(), PortalArtifactError> {
    if owned_backup(previous) {
        fs::remove_dir_all(previous)?;
    }
    let taken = || PortalArtifactError::InvalidPortalDirectory(previous.to_path_buf());
    if fs::symlink_metadata(previous).is_ok() {
        return Err(taken());
    }
    let staged = TempInstallDir::create(&install_parent(previous)?)?;
    fs::File::create_new(staged.path().join(BACKUP_MARKER))?;
    staged
        .persist(previous)
        .map_err(|error| match error.kind() {
            io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty => taken(),
            _ => error.into(),
        })
}

/// Swaps the new portal into `target` in one step; the old one is left at `staged`. A file
/// system without an exchange answers `Unsupported`.
#[cfg(not(windows))]
fn exchange_dirs(staged: &Path, target: &Path) -> io::Result<()> {
    use rustix::io::Errno;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staged,
        rustix::fs::CWD,
        target,
        rustix::fs::RenameFlags::EXCHANGE,
    )
    .map_err(|error| {
        match [Errno::INVAL, Errno::NOSYS, Errno::NOTSUP, Errno::OPNOTSUPP].contains(&error) {
            true => io::Error::from(io::ErrorKind::Unsupported),
            false => io::Error::from(error),
        }
    })
}

#[cfg(windows)]
fn exchange_dirs(_staged: &Path, _target: &Path) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Renames `from` to `to` only while nothing is at `to`. A file system that cannot refuse a
/// replacement fails the rename instead of replacing.
#[cfg(not(windows))]
fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    let flags = rustix::fs::RenameFlags::NOREPLACE;
    rustix::fs::renameat_with(rustix::fs::CWD, from, rustix::fs::CWD, to, flags)
        .map_err(io::Error::from)
}

/// Without `MOVEFILE_REPLACE_EXISTING` the move refuses any entry at `to`.
#[cfg(windows)]
fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    let wide =
        |path: &Path| -> Vec<u16> { path.as_os_str().encode_wide().chain(Some(0)).collect() };
    let (from, to) = (wide(from), wide(to));
    let moved = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(from.as_ptr(), to.as_ptr(), 0)
    };
    match moved {
        0 => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

/// Replaces `target` with two renames for file systems without an exchange.
fn replace_portable(
    staged: TempInstallDir,
    target: &Path,
    previous: &Path,
) -> Result<(), PortalArtifactError> {
    claim_backup(previous)?;
    let backup = previous.join(BACKUP_DIR);
    if let Err(error) = fs::rename(target, &backup) {
        let _ = fs::remove_dir_all(previous);
        return Err(error.into());
    }
    if let Err(install) = staged.persist(target) {
        return Err(restore_backup(previous, target, install));
    }
    // A backup that stays behind is removed by the next replacement.
    let _ = fs::remove_dir_all(previous);
    Ok(())
}

/// Moves the old portal from the recovery directory `previous` back to the vacant `target`
/// after a failed install. An entry that took `target` meanwhile stays, and so does the backup.
fn restore_backup(previous: &Path, target: &Path, install: io::Error) -> PortalArtifactError {
    let backup = previous.join(BACKUP_DIR);
    match rename_new(&backup, target) {
        Ok(()) => {
            let _ = fs::remove_dir_all(previous);
            install.into()
        }
        Err(restore) => PortalArtifactError::RollbackFailed {
            backup,
            install,
            restore,
        },
    }
}

fn can_replace_dir(path: &Path) -> io::Result<bool> {
    if fs::symlink_metadata(path.join(CHECKSUM_FILE))
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
    {
        return Ok(true);
    }

    match fs::read_dir(path)?.next() {
        Some(Ok(_)) => Ok(false),
        Some(Err(error)) => Err(error),
        None => Ok(true),
    }
}

fn verify_sha256(bytes: &[u8], expected: &str) -> Result<(), PortalArtifactError> {
    let actual = hex::encode(Sha256::digest(bytes));
    if actual != expected {
        return Err(PortalArtifactError::ChecksumMismatch {
            expected: expected.to_string(),
            actual,
        });
    }
    Ok(())
}

fn unpack_archive(archive_bytes: &[u8], dest: &Path) -> Result<(), PortalArtifactError> {
    let decoder = GzDecoder::new(Cursor::new(archive_bytes));
    let mut archive = Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let raw_path = entry.path()?.into_owned();
        let entry_type = entry.header().entry_type();
        let Some(relative_path) = safe_archive_path(&raw_path)? else {
            if entry_type.is_dir() {
                continue;
            }

            return Err(PortalArtifactError::UnsafeArchivePath {
                path: raw_path.display().to_string(),
                reason: "empty paths are not allowed".to_string(),
            });
        };
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(PortalArtifactError::ArchiveLink(
                raw_path.display().to_string(),
            ));
        }

        let target = dest.join(relative_path);
        if entry_type.is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry_type.is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry.unpack(&target)?;
        } else {
            return Err(PortalArtifactError::UnsupportedArchiveEntry {
                path: raw_path.display().to_string(),
                entry_type: format!("{entry_type:?}"),
            });
        }
    }

    Ok(())
}

fn safe_archive_path(path: &Path) -> Result<Option<PathBuf>, PortalArtifactError> {
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(PortalArtifactError::UnsafeArchivePath {
                    path: path.display().to_string(),
                    reason: "parent directory components are not allowed".to_string(),
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(PortalArtifactError::UnsafeArchivePath {
                    path: path.display().to_string(),
                    reason: "absolute paths are not allowed".to_string(),
                });
            }
        }
    }

    if relative.as_os_str().is_empty() {
        return Ok(None);
    }

    Ok(Some(relative))
}

fn require_index_html(dir: &Path) -> Result<(), PortalArtifactError> {
    if dir.join("index.html").is_file() {
        Ok(())
    } else {
        Err(PortalArtifactError::MissingIndexHtml)
    }
}

fn read_manifest(dir: &Path) -> Result<Option<PortalManifest>, PortalArtifactError> {
    read_optional_json(&dir.join(MANIFEST_FILE))
}

fn read_checksum_file(dir: &Path) -> Option<String> {
    let contents = fs::read(dir.join(CHECKSUM_FILE)).ok()?;
    parse_checksum_file(&contents).ok()
}

fn write_checksum_file(dir: &Path, contents: &[u8]) -> io::Result<()> {
    fs::write(dir.join(CHECKSUM_FILE), contents)
}

fn parse_checksum_file(contents: &[u8]) -> Result<String, PortalArtifactError> {
    let contents =
        std::str::from_utf8(contents).map_err(|_| PortalArtifactError::InvalidChecksumFile)?;
    let checksum = contents
        .split_whitespace()
        .next()
        .ok_or(PortalArtifactError::InvalidChecksumFile)?;
    if checksum.len() == 64 && checksum.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Ok(checksum.to_ascii_lowercase())
    } else {
        Err(PortalArtifactError::InvalidChecksumFile)
    }
}

fn read_optional_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, PortalArtifactError> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(serde_json::from_str(&contents)?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn current_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PortalManifest {
    version: Option<String>,
    source: Option<String>,
    fetched_at: Option<String>,
}

struct DownloadedChecksumFile {
    bytes: Vec<u8>,
    checksum: String,
}

struct TempInstallDir {
    path: PathBuf,
    cleanup: bool,
}

impl TempInstallDir {
    fn create(parent: &Path) -> io::Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..100 {
            let path = parent.join(format!(".portal-{}-{now}-{attempt}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        cleanup: true,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not create unique portal artifact temp directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn persist(mut self, dest: &Path) -> io::Result<()> {
        rename_new(&self.path, dest)?;
        self.cleanup = false;
        Ok(())
    }
}

impl Drop for TempInstallDir {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Debug, Error)]
pub enum PortalArtifactError {
    #[error("portal artifact URL is required to download the portal")]
    MissingArtifactUrl,
    #[error("portal directory {0:?} cannot be replaced safely")]
    InvalidPortalDirectory(PathBuf),
    #[error("portal artifact checksum file is invalid")]
    InvalidChecksumFile,
    #[error("portal artifact checksum file mismatch: expected {expected}, got {actual}")]
    ChecksumSidecarMismatch { expected: String, actual: String },
    #[error("portal artifact checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("portal artifact checksum download returned HTTP status {0}")]
    ChecksumHttpStatus(u16),
    #[error("portal artifact download returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("portal artifact archive path {path:?} is unsafe: {reason}")]
    UnsafeArchivePath { path: String, reason: String },
    #[error("portal artifact archive entry {path:?} uses unsupported type {entry_type}")]
    UnsupportedArchiveEntry { path: String, entry_type: String },
    #[error("portal artifact archive entry {0:?} is a symlink or hardlink")]
    ArchiveLink(String),
    #[error("portal artifact is missing index.html")]
    MissingIndexHtml,
    #[error("portal download is larger than {0} bytes")]
    TooLarge(usize),
    #[error("portal install failed ({install}); the previous portal is kept at {backup:?}")]
    RollbackFailed {
        backup: PathBuf,
        install: io::Error,
        #[source]
        restore: io::Error,
    },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use super::{
        BACKUP_DIR, CHECKSUM_FILE, DownloadedChecksumFile, PortalArtifactError, TempInstallDir,
        claim_backup, existing_install, install_parent, install_verified_artifact,
        installed_status, lock_install, portal_dir, prepare_artifact_status, previous_install,
        replace_portable, restore_backup, safe_archive_path, unpack_archive, verify_sha256,
    };
    use crate::PortalArtifactConfig;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use sha2::Digest;
    use std::fs;
    use std::io::Cursor;
    use std::path::Path;
    use tar::{Builder, EntryType, Header};
    use tempfile::tempdir;

    fn config(portal_dir: &Path) -> PortalArtifactConfig {
        PortalArtifactConfig {
            artifact_url: Some("https://example.test/portal.tar.gz".to_string()),
            artifact_sha256: Some(
                "0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61".to_string(),
            ),
            portal_dir: portal_dir.to_path_buf(),
        }
    }

    #[test]
    fn path_rejects_traversal() {
        assert!(matches!(
            safe_archive_path(Path::new("/index.html")),
            Err(PortalArtifactError::UnsafeArchivePath { .. })
        ));
        assert!(matches!(
            safe_archive_path(Path::new("assets/../index.html")),
            Err(PortalArtifactError::UnsafeArchivePath { .. })
        ));
        assert_eq!(
            safe_archive_path(Path::new("./assets/app.js")).unwrap(),
            Some(Path::new("assets/app.js").to_path_buf())
        );
        assert_eq!(safe_archive_path(Path::new("./")).unwrap(), None);
    }

    #[test]
    fn unpack_rejects_links() {
        let archive = archive_with_entry("index.html", b"ok", EntryType::Symlink);
        let tempdir = tempdir().unwrap();

        assert!(matches!(
            unpack_archive(&archive, tempdir.path()),
            Err(PortalArtifactError::ArchiveLink(_))
        ));
    }

    #[test]
    fn unpack_skips_root() {
        let archive = archive_with_entry("./", b"", EntryType::Directory);
        let tempdir = tempdir().unwrap();

        unpack_archive(&archive, tempdir.path()).unwrap();
    }

    #[test]
    fn parent_rejects_targets() {
        for path in ["", ".", "..", "../portal", "/", "/run", "/tmp"] {
            assert!(
                matches!(
                    install_parent(Path::new(path)),
                    Err(PortalArtifactError::InvalidPortalDirectory(_))
                ),
                "expected {path:?} to be rejected"
            );
        }

        assert_eq!(install_parent(Path::new("portal")).unwrap(), Path::new("."));
        assert_eq!(
            install_parent(Path::new("target/portal")).unwrap(),
            Path::new("target")
        );
        assert_eq!(
            install_parent(Path::new("/run/portal")).unwrap(),
            Path::new("/run")
        );
    }

    #[test]
    fn replace_rejects_files() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        fs::write(&target, "important data").unwrap();

        assert!(matches!(
            existing_install(&target),
            Err(PortalArtifactError::InvalidPortalDirectory(_))
        ));
        assert_eq!(fs::read_to_string(&target).unwrap(), "important data");
    }

    #[test]
    fn replace_rejects_nonempty() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("important.txt"), "important data").unwrap();

        assert!(matches!(
            existing_install(&target),
            Err(PortalArtifactError::InvalidPortalDirectory(_))
        ));
        assert_eq!(
            fs::read_to_string(target.join("important.txt")).unwrap(),
            "important data"
        );
    }

    #[test]
    fn replace_allows_owned() {
        let tempdir = tempdir().unwrap();
        let empty = tempdir.path().join("empty-portal");
        fs::create_dir_all(&empty).unwrap();
        assert!(existing_install(&empty).unwrap());

        let installed = tempdir.path().join("installed-portal");
        write_portal(&installed, "old");
        assert!(existing_install(&installed).unwrap());
        assert!(!existing_install(&tempdir.path().join("missing")).unwrap());
    }

    #[cfg(windows)]
    #[test]
    fn parent_allows_drive() {
        for path in [r"C:portal", r"C:\", r"C:\run", r"\\server\share\portal"] {
            assert!(
                matches!(
                    install_parent(Path::new(path)),
                    Err(PortalArtifactError::InvalidPortalDirectory(_))
                ),
                "expected {path:?} to be rejected"
            );
        }
        assert_eq!(
            install_parent(Path::new(r"C:\run\portal")).unwrap(),
            Path::new(r"C:\run")
        );
    }

    #[tokio::test]
    async fn startup_restores_previous() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        claim_backup(&previous).unwrap();
        fs::rename(&target, previous.join(BACKUP_DIR)).unwrap();
        let mut config = config(&target);
        config.artifact_url = None;

        let status = prepare_artifact_status(config).await;

        assert!(status.installed, "{:?}", status.last_error);
        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "old"
        );
        assert!(!previous.exists());
    }

    #[tokio::test]
    async fn startup_ignores_sibling() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        fs::create_dir_all(&previous).unwrap();
        fs::write(previous.join("index.html"), "saved site").unwrap();
        let mut config = config(&target);
        config.artifact_url = None;

        let status = prepare_artifact_status(config).await;

        assert!(!status.installed);
        assert!(!target.exists());
        assert_eq!(
            fs::read_to_string(previous.join("index.html")).unwrap(),
            "saved site"
        );
    }

    #[test]
    fn replace_keeps_backup() {
        // An operator's backup at the recovery name stays, and so does the live portal.
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        write_portal(&previous, "backup");
        let staged = tempdir.path().join("staged");
        write_portal(&staged, "new");
        let staged = TempInstallDir {
            path: staged,
            cleanup: true,
        };

        assert!(matches!(
            replace_portable(staged, &target, &previous),
            Err(PortalArtifactError::InvalidPortalDirectory(_))
        ));
        for (dir, index) in [(&target, "old"), (&previous, "backup")] {
            assert_eq!(fs::read_to_string(dir.join("index.html")).unwrap(), index);
        }
    }

    #[test]
    fn replace_reuses_backup() {
        // A recovery directory this installer left behind does not block the next replacement.
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        claim_backup(&previous).unwrap();
        let staged = tempdir.path().join("staged");
        write_portal(&staged, "new");
        let staged = TempInstallDir {
            path: staged,
            cleanup: true,
        };

        replace_portable(staged, &target, &previous).unwrap();

        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "new"
        );
        assert!(!previous.exists());
    }

    #[test]
    fn install_replaces_portal() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        write_portal(&target, "old");
        let archive = archive_with_entry("index.html", b"new", EntryType::Regular);
        let checksum = DownloadedChecksumFile {
            bytes: b"sum".to_vec(),
            checksum: "sum".to_string(),
        };

        install_verified_artifact(&config(&target), &archive, checksum, "now".to_string()).unwrap();

        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "new"
        );
        // The portal and its lock file.
        assert_eq!(fs::read_dir(tempdir.path()).unwrap().count(), 2);
    }

    // The install lock refuses a second handle on its file until the guard closes.
    #[test]
    fn lock_excludes_others() {
        let tempdir = tempdir().unwrap();
        let previous = previous_install(&tempdir.path().join("portal")).unwrap();
        let locked = lock_install(&previous).unwrap();
        let other = fs::File::open(previous.with_extension("lock")).unwrap();

        assert!(matches!(
            other.try_lock(),
            Err(fs::TryLockError::WouldBlock)
        ));
        drop(locked);
        other.try_lock().unwrap();
    }

    // An update that starts while another one has moved the old portal away still ends with the
    // old portal recovered and the new one installed.
    #[test]
    fn overlapping_install_waits() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        let locked = lock_install(&previous).unwrap();
        claim_backup(&previous).unwrap();
        let backup = previous.join(BACKUP_DIR);
        fs::rename(&target, &backup).unwrap();
        let second_config = config(&target);
        let second = std::thread::spawn(move || {
            let archive = archive_with_entry("index.html", b"new", EntryType::Regular);
            let checksum = DownloadedChecksumFile {
                bytes: b"sum".to_vec(),
                checksum: "sum".to_string(),
            };
            install_verified_artifact(&second_config, &archive, checksum, "now".to_string())
        });

        // The first install fails and restores its backup, as `replace_portable` does.
        fs::rename(&backup, &target).unwrap();
        fs::remove_dir_all(&previous).unwrap();
        drop(locked);
        second.join().unwrap().unwrap();

        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "new"
        );
        assert!(!previous.exists());
    }

    // A directory another tool creates at the recovery name after the absence check stays.
    #[test]
    fn claim_keeps_directory() {
        let tempdir = tempdir().unwrap();
        let previous = tempdir.path().join(".portal.previous");
        let staged = TempInstallDir::create(tempdir.path()).unwrap();
        let path = staged.path().to_path_buf();
        fs::create_dir(&previous).unwrap();

        let error = staged.persist(&previous).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(previous.is_dir());
        assert!(!path.exists());
    }

    // A claim interrupted before its marker leaves only its staging directory, so the next
    // replacement still claims the recovery directory.
    #[test]
    fn interrupted_claim_recovers() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        std::mem::forget(TempInstallDir::create(tempdir.path()).unwrap());
        let staged = tempdir.path().join("staged");
        write_portal(&staged, "new");
        let staged = TempInstallDir {
            path: staged,
            cleanup: true,
        };

        replace_portable(staged, &target, &previous).unwrap();

        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "new"
        );
        assert!(!previous.exists());
    }

    // A directory another tool creates at the live name before the rollback stays, and the old
    // portal waits in its backup.
    #[test]
    fn rollback_keeps_occupant() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        claim_backup(&previous).unwrap();
        fs::rename(&target, previous.join(BACKUP_DIR)).unwrap();
        fs::create_dir(&target).unwrap();

        let error = restore_backup(&previous, &target, std::io::Error::other("install"));
        assert!(matches!(error, PortalArtifactError::RollbackFailed { .. }));
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(
            fs::read_to_string(previous.join(BACKUP_DIR).join("index.html")).unwrap(),
            "old"
        );
    }

    #[test]
    fn failed_replace_restores() {
        let tempdir = tempdir().unwrap();
        let target = tempdir.path().join("portal");
        let previous = previous_install(&target).unwrap();
        write_portal(&target, "old");
        let staged = TempInstallDir {
            path: tempdir.path().join("missing"),
            cleanup: false,
        };

        assert!(matches!(
            replace_portable(staged, &target, &previous),
            Err(PortalArtifactError::Io(_))
        ));
        assert_eq!(
            fs::read_to_string(target.join("index.html")).unwrap(),
            "old"
        );
        assert!(!previous.exists());
    }

    fn write_portal(dir: &Path, index: &str) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("index.html"), index).unwrap();
        fs::write(
            dir.join(CHECKSUM_FILE),
            "0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61  aruna-portal-dist.tar.gz\n",
        )
        .unwrap();
    }

    #[test]
    fn manifest_status_safe() {
        let tempdir = tempdir().unwrap();
        let config = config(tempdir.path());
        let dir = portal_dir(&config);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("index.html"), "<html></html>").unwrap();
        fs::write(
            dir.join(CHECKSUM_FILE),
            "0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61  aruna-portal-dist.tar.gz\n",
        )
        .unwrap();
        fs::write(
            dir.join("portal-manifest.json"),
            r#"{
                "version": "0.1.0-portal.1",
                "source": "github-release",
                "checksum": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "fetched_at": "2026-07-01T00:00:00Z"
            }"#,
        )
        .unwrap();

        let status = installed_status(&config, &dir);

        assert!(status.installed);
        assert_eq!(status.mode, "artifact");
        assert_eq!(status.version.as_deref(), Some("0.1.0-portal.1"));
        assert_eq!(status.source.as_deref(), Some("github-release"));
        assert_eq!(
            status.checksum.as_deref(),
            config.artifact_sha256.as_deref()
        );
        assert_eq!(status.fetched_at.as_deref(), Some("2026-07-01T00:00:00Z"));
    }

    #[test]
    fn checksum_requires_exact() {
        let bytes = b"portal artifact";
        let expected = hex::encode(sha2::Sha256::digest(bytes));

        verify_sha256(bytes, &expected).unwrap();
        assert!(matches!(
            verify_sha256(bytes, &"0".repeat(64)),
            Err(PortalArtifactError::ChecksumMismatch { .. })
        ));
    }

    fn archive_with_entry(path: &str, data: &[u8], entry_type: EntryType) -> Vec<u8> {
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut builder = Builder::new(&mut gz);
            let mut header = Header::new_gnu();
            header.set_entry_type(entry_type);
            header.set_path(path).unwrap();
            header.set_size(if entry_type.is_file() {
                data.len() as u64
            } else {
                0
            });
            header.set_cksum();
            builder.append(&header, Cursor::new(data)).unwrap();
            builder.finish().unwrap();
        }
        gz.finish().unwrap()
    }
}
