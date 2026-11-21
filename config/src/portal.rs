//! Downloads, verifies and unpacks the portal web artifact and reports its install status.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::PortalArtifactConfig;
use aruna_wire::info::PortalStatus;
use chrono::{SecondsFormat, Utc};
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Cursor};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tar::Archive;
use thiserror::Error;

const MANIFEST_FILE: &str = "portal-manifest.json";
const CHECKSUM_FILE: &str = "aruna-portal-dist.tar.gz.sha256";
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

    // The old portal stays until the new one is in place, and comes back if that fails.
    let previous = set_aside_existing(&portal_dir)?;
    if let Err(error) = temp_dir.persist(&portal_dir) {
        if let Some(previous) = &previous {
            let _ = fs::rename(previous, &portal_dir);
        }
        return Err(error.into());
    }
    if let Some(previous) = previous {
        let _ = fs::remove_dir_all(previous);
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
            Component::ParentDir | Component::Prefix(_) => {
                return Err(PortalArtifactError::InvalidPortalDirectory(
                    path.to_path_buf(),
                ));
            }
        }
    }

    if normal_components == 0 || path.is_absolute() && normal_components == 1 {
        return Err(PortalArtifactError::InvalidPortalDirectory(
            path.to_path_buf(),
        ));
    }

    Ok(())
}

/// Moves an installed portal at `path` aside and answers where it went; anything else there is
/// refused.
fn set_aside_existing(path: &Path) -> Result<Option<PathBuf>, PortalArtifactError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            if can_replace_dir(path)? {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let name = format!(".portal-previous-{}-{nanos}", std::process::id());
                let aside = path.with_file_name(name);
                fs::rename(path, &aside)?;
                Ok(Some(aside))
            } else {
                Err(PortalArtifactError::InvalidPortalDirectory(
                    path.to_path_buf(),
                ))
            }
        }
        Ok(_) => Err(PortalArtifactError::InvalidPortalDirectory(
            path.to_path_buf(),
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
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
        fs::rename(&self.path, dest)?;
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
        CHECKSUM_FILE, PortalArtifactError, install_parent, installed_status, portal_dir,
        safe_archive_path, set_aside_existing, unpack_archive, verify_sha256,
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
            set_aside_existing(&target),
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
            set_aside_existing(&target),
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

        let aside = set_aside_existing(&empty).unwrap().unwrap();
        assert!(!empty.exists());
        assert!(aside.is_dir());

        let installed = tempdir.path().join("installed-portal");
        fs::create_dir_all(&installed).unwrap();
        fs::write(installed.join("index.html"), "<html></html>").unwrap();
        fs::write(
            installed.join(CHECKSUM_FILE),
            "0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61  aruna-portal-dist.tar.gz\n",
        )
        .unwrap();

        let aside = set_aside_existing(&installed).unwrap().unwrap();
        assert!(!installed.exists());
        assert!(aside.join("index.html").is_file());
        assert_eq!(set_aside_existing(&installed).unwrap(), None);
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
