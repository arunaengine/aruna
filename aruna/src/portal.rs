use crate::config::{PortalArtifactConfig, PortalConfig};
use aruna_api::server_state::{PortalStatus, ServerState};
use chrono::{SecondsFormat, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Cursor};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tar::Archive;
use thiserror::Error;
use tracing::{info, warn};

const CACHE_METADATA_FILE: &str = ".aruna-portal-cache.json";
const MANIFEST_FILE: &str = "portal-manifest.json";

pub async fn initialize(config: PortalConfig, state: Arc<ServerState>) {
    match config {
        PortalConfig::Disabled => {
            state.set_portal_status(PortalStatus::default()).await;
        }
        PortalConfig::Artifact(config) => {
            state
                .set_portal_status(artifact_status_base(&config, false))
                .await;
            tokio::spawn(async move {
                let status = prepare_artifact_status(config).await;
                if status.installed {
                    info!("Portal artifact cache prepared");
                } else if let Some(error) = &status.last_error {
                    warn!(error = %error, "Portal artifact preparation failed");
                }
                state.set_portal_status(status).await;
            });
        }
    }
}

async fn prepare_artifact_status(config: PortalArtifactConfig) -> PortalStatus {
    let mut cache_error = None;
    match load_cached_status(&config) {
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
    let response = reqwest::get(&config.artifact_url).await?;
    if !response.status().is_success() {
        return Err(PortalArtifactError::HttpStatus(response.status().as_u16()));
    }
    let bytes = response.bytes().await?;
    verify_sha256(&bytes, &config.artifact_sha256)?;
    let fetched_at = current_timestamp();

    tokio::task::spawn_blocking(move || install_verified_artifact(&config, &bytes, fetched_at))
        .await?
}

fn install_verified_artifact(
    config: &PortalArtifactConfig,
    archive_bytes: &[u8],
    fetched_at: String,
) -> Result<PortalStatus, PortalArtifactError> {
    fs::create_dir_all(&config.cache_dir)?;
    let artifact_dir = artifact_dir(config);
    let temp_dir = TempInstallDir::create(&config.cache_dir, &config.artifact_sha256)?;

    unpack_archive(archive_bytes, temp_dir.path())?;
    require_index_html(temp_dir.path())?;
    let manifest = read_manifest(temp_dir.path())?;
    let metadata = CacheMetadata::from_manifest(config, manifest.as_ref(), fetched_at.clone());
    write_cache_metadata(temp_dir.path(), &metadata)?;

    if artifact_dir.exists() {
        fs::remove_dir_all(&artifact_dir)?;
    }
    temp_dir.persist(&artifact_dir)?;

    Ok(status_from_parts(
        config,
        true,
        metadata.version,
        metadata.source,
        Some(fetched_at),
        None,
    ))
}

fn load_cached_status(
    config: &PortalArtifactConfig,
) -> Result<Option<PortalStatus>, PortalArtifactError> {
    let artifact_dir = artifact_dir(config);
    if !artifact_dir.exists() {
        return Ok(None);
    }

    require_index_html(&artifact_dir)?;
    Ok(Some(installed_status_from_dir(config, &artifact_dir)?))
}

fn installed_status_from_dir(
    config: &PortalArtifactConfig,
    dir: &Path,
) -> Result<PortalStatus, PortalArtifactError> {
    let manifest = read_manifest(dir)?;
    let metadata = read_cache_metadata(dir)?;
    if let Some(metadata) = &metadata
        && metadata.checksum != config.artifact_sha256
    {
        return Err(PortalArtifactError::CacheChecksumMismatch {
            expected: config.artifact_sha256.clone(),
            actual: metadata.checksum.clone(),
        });
    }

    let version = metadata
        .as_ref()
        .and_then(|metadata| metadata.version.clone())
        .or_else(|| {
            manifest
                .as_ref()
                .and_then(|manifest| manifest.version.clone())
        });
    let source = metadata
        .as_ref()
        .and_then(|metadata| metadata.source.clone())
        .or_else(|| {
            manifest
                .as_ref()
                .and_then(|manifest| manifest.source.clone())
        });
    let fetched_at = metadata
        .as_ref()
        .and_then(|metadata| metadata.fetched_at.clone())
        .or_else(|| {
            manifest
                .as_ref()
                .and_then(|manifest| manifest.fetched_at.clone())
        });

    Ok(status_from_parts(
        config, true, version, source, fetched_at, None,
    ))
}

fn artifact_status_base(config: &PortalArtifactConfig, installed: bool) -> PortalStatus {
    status_from_parts(config, installed, None, None, None, None)
}

fn status_from_parts(
    config: &PortalArtifactConfig,
    installed: bool,
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
        url: Some(config.artifact_url.clone()),
        checksum: Some(config.artifact_sha256.clone()),
        fetched_at,
        last_error,
    }
}

fn artifact_dir(config: &PortalArtifactConfig) -> PathBuf {
    config.cache_dir.join(&config.artifact_sha256)
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
        let relative_path = safe_archive_path(&raw_path)?;
        let entry_type = entry.header().entry_type();
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

fn safe_archive_path(path: &Path) -> Result<PathBuf, PortalArtifactError> {
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
        return Err(PortalArtifactError::UnsafeArchivePath {
            path: path.display().to_string(),
            reason: "empty paths are not allowed".to_string(),
        });
    }

    Ok(relative)
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

fn read_cache_metadata(dir: &Path) -> Result<Option<CacheMetadata>, PortalArtifactError> {
    read_optional_json(&dir.join(CACHE_METADATA_FILE))
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

fn write_cache_metadata(dir: &Path, metadata: &CacheMetadata) -> Result<(), PortalArtifactError> {
    let bytes = serde_json::to_vec_pretty(metadata)?;
    fs::write(dir.join(CACHE_METADATA_FILE), bytes)?;
    Ok(())
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CacheMetadata {
    url: String,
    checksum: String,
    fetched_at: Option<String>,
    version: Option<String>,
    source: Option<String>,
}

impl CacheMetadata {
    fn from_manifest(
        config: &PortalArtifactConfig,
        manifest: Option<&PortalManifest>,
        fetched_at: String,
    ) -> Self {
        Self {
            url: config.artifact_url.clone(),
            checksum: config.artifact_sha256.clone(),
            fetched_at: Some(fetched_at),
            version: manifest.and_then(|manifest| manifest.version.clone()),
            source: manifest.and_then(|manifest| manifest.source.clone()),
        }
    }
}

struct TempInstallDir {
    path: PathBuf,
    cleanup: bool,
}

impl TempInstallDir {
    fn create(parent: &Path, checksum: &str) -> io::Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..100 {
            let path = parent.join(format!(
                ".portal-{checksum}-{}-{now}-{attempt}",
                std::process::id()
            ));
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
enum PortalArtifactError {
    #[error("portal artifact cache metadata checksum mismatch: expected {expected}, got {actual}")]
    CacheChecksumMismatch { expected: String, actual: String },
    #[error("portal artifact checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
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
        PortalArtifactError, artifact_dir, installed_status_from_dir, safe_archive_path,
        unpack_archive, verify_sha256,
    };
    use crate::config::PortalArtifactConfig;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use sha2::Digest;
    use std::fs;
    use std::io::Cursor;
    use std::path::Path;
    use tar::{Builder, EntryType, Header};
    use tempfile::tempdir;

    fn config(cache_dir: &Path) -> PortalArtifactConfig {
        PortalArtifactConfig {
            artifact_url: "https://example.test/portal.tar.gz".to_string(),
            artifact_sha256: "0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61"
                .to_string(),
            cache_dir: cache_dir.to_path_buf(),
        }
    }

    #[test]
    fn archive_path_safety_rejects_absolute_and_traversal_paths() {
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
            Path::new("assets/app.js")
        );
    }

    #[test]
    fn unpack_rejects_archive_links() {
        let archive = archive_with_entry("index.html", b"ok", EntryType::Symlink);
        let tempdir = tempdir().unwrap();

        assert!(matches!(
            unpack_archive(&archive, tempdir.path()),
            Err(PortalArtifactError::ArchiveLink(_))
        ));
    }

    #[test]
    fn manifest_populates_status_without_trusting_manifest_checksum() {
        let tempdir = tempdir().unwrap();
        let config = config(tempdir.path());
        let dir = artifact_dir(&config);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("index.html"), "<html></html>").unwrap();
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

        let status = installed_status_from_dir(&config, &dir).unwrap();

        assert!(status.installed);
        assert_eq!(status.mode, "artifact");
        assert_eq!(status.version.as_deref(), Some("0.1.0-portal.1"));
        assert_eq!(status.source.as_deref(), Some("github-release"));
        assert_eq!(
            status.checksum.as_deref(),
            Some(config.artifact_sha256.as_str())
        );
        assert_eq!(status.fetched_at.as_deref(), Some("2026-07-01T00:00:00Z"));
    }

    #[test]
    fn verify_sha256_requires_exact_checksum() {
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
