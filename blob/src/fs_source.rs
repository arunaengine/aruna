//! Read-only access to a directory the owner offers or syncs from their own
//! device. The guarded write half lives in `fs_write`.
//!
//! Jail guarantee: the offered root and every requested entry are fully
//! resolved with `canonicalize`, and an entry is refused unless its resolved
//! path is inside the resolved root. A symlink is therefore followed only while
//! it stays inside the offered directory; a link, or a link component, leaving
//! it is refused. The check is not atomic with the open that follows it, so
//! this is a resolve-and-verify guarantee, not a kernel-enforced no-follow
//! open: only regular files are opened.
//!
use aruna_core::errors::StagingSourceError;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    OFFERED_DIRECTORY_ROOT, ResolvedSourceAccess, SourceConnectorKind, SourceEntry,
    SourceEntryKind, SourceMetadata, weak_fingerprint,
};
use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

/// The one directory this node maintains inside a folder. Nothing under it is
/// ever offered as the owner's data.
pub(crate) const RESERVED_DIR: &str = ".aruna";

/// Prefix of the temporary files a guarded write spools. They live beside the
/// file they will become, so they are skipped by name rather than by directory.
pub(crate) const SPOOL_PREFIX: &str = ".aruna-tmp-";

/// Candidate names one conflicted copy or move-aside may try before it gives
/// up. A folder with this many same-named copies needs the owner, not a retry.
pub(crate) const MAX_COPY_ATTEMPTS: usize = 100;

pub(crate) fn is_local_access(access: &ResolvedSourceAccess) -> bool {
    let ResolvedSourceAccess::OpenDal { kind, .. } = access;
    *kind == SourceConnectorKind::LocalDirectory
}

pub(crate) async fn check_local(access: &ResolvedSourceAccess) -> Result<(), StagingSourceError> {
    let (root, _) = access_parts(access)?;
    let resolved = canonical_root(&root).await?;
    // Every sweep starts by dropping the spool a crashed write left behind.
    // Those bytes are this node's own, never the owner's.
    crate::fs_write::sweep_spool(&root).await;
    match tokio::fs::metadata(&resolved).await {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(StagingSourceError::CheckError(
            "the offered root is not a directory".to_string(),
        )),
        Err(error) => Err(StagingSourceError::CheckError(error.to_string())),
    }
}

pub(crate) async fn head_local(
    access: &ResolvedSourceAccess,
) -> Result<SourceMetadata, StagingSourceError> {
    let (root, path) = access_parts(access)?;
    let resolved = jailed_file(&root, &path).await?;
    let metadata = tokio::fs::metadata(&resolved).await.map_err(map_io_error)?;
    Ok(file_metadata(&metadata))
}

pub(crate) async fn list_local(
    access: &ResolvedSourceAccess,
    offset: usize,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<SourceEntry>, bool), StagingSourceError> {
    let (root, path) = access_parts(access)?;
    let resolved_root = canonical_root(&root).await?;
    let start = jailed_entry(&resolved_root, &path).await?;
    // Resolved directories already queued. A link to an ancestor inside the
    // offered root would otherwise walk in circles forever.
    let mut visited = HashSet::from([start.clone()]);
    let mut queue = VecDeque::from([(start, path)]);
    let mut entries = Vec::new();
    let mut skipped = 0usize;

    while let Some((directory, prefix)) = queue.pop_front() {
        let mut reader = tokio::fs::read_dir(&directory)
            .await
            .map_err(|error| StagingSourceError::ListError(error.to_string()))?;
        while let Some(entry) = reader
            .next_entry()
            .await
            .map_err(|error| StagingSourceError::ListError(error.to_string()))?
        {
            let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
                continue;
            };
            // This node's own bookkeeping is never the owner's data. Only the
            // reserved directory and the spool prefix are skipped, so a file
            // the owner named `.aruna-notes` stays theirs.
            if name == RESERVED_DIR || name.starts_with(SPOOL_PREFIX) {
                continue;
            }
            let relative = join_relative(&prefix, &name);
            // A link out of the offered directory is skipped, not an error: one
            // stray link must not make the whole listing unusable.
            let Ok(resolved) = jailed_entry(&resolved_root, &relative).await else {
                continue;
            };
            let Ok(metadata) = tokio::fs::metadata(&resolved).await else {
                continue;
            };
            let kind = if metadata.is_dir() {
                if recursive && visited.insert(resolved.clone()) {
                    queue.push_back((resolved, relative.clone()));
                }
                if files_only {
                    continue;
                }
                SourceEntryKind::Directory
            } else if metadata.is_file() {
                SourceEntryKind::File
            } else {
                continue;
            };
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if entries.len() == limit {
                return Ok((entries, true));
            }
            entries.push(SourceEntry {
                name,
                path: relative,
                kind,
                size: (kind == SourceEntryKind::File).then_some(metadata.len()),
                modified: metadata.modified().ok(),
            });
        }
    }

    Ok((entries, false))
}

pub(crate) async fn read_local(
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<(SourceMetadata, BackendStream<Result<Bytes, StreamError>>), StagingSourceError> {
    let (root, path) = access_parts(access)?;
    let resolved = jailed_file(&root, &path).await?;
    let before = tokio::fs::metadata(&resolved).await.map_err(map_io_error)?;
    let metadata = file_metadata(&before);
    let fingerprint = weak_fingerprint(before.len(), before.modified().ok());

    let mut file = tokio::fs::File::open(&resolved)
        .await
        .map_err(map_io_error)?;
    let stream = match range {
        Some(range) if range.start < range.end => {
            file.seek(std::io::SeekFrom::Start(range.start))
                .await
                .map_err(map_io_error)?;
            let limit = range.end - range.start;
            BackendStream::new(ReaderStream::new(tokio::io::AsyncReadExt::take(
                file, limit,
            )))
        }
        Some(_) => {
            return Err(StagingSourceError::ReadError(
                "invalid source range".to_string(),
            ));
        }
        // Only a complete read may become an identity, so only it is verified.
        None => BackendStream::new(ReaderStream::new(file))
            .on_success_async(move || verify_stable(resolved, fingerprint)),
    };

    Ok((metadata, stream))
}

/// Refuses the bytes that were just streamed when the file no longer carries
/// the fingerprint it was opened with.
async fn verify_stable(path: PathBuf, fingerprint: String) -> Result<(), StreamError> {
    let after = tokio::fs::metadata(&path)
        .await
        .map_err(|error| StreamError(Box::new(error)))?;
    if weak_fingerprint(after.len(), after.modified().ok()) == fingerprint {
        return Ok(());
    }
    Err(StreamError(Box::new(StagingSourceError::SourceUnstable)))
}

fn access_parts(access: &ResolvedSourceAccess) -> Result<(String, String), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal { config, path, .. } = access;
    let root = config.get(OFFERED_DIRECTORY_ROOT).cloned().ok_or_else(|| {
        StagingSourceError::ReadError("missing offered directory root".to_string())
    })?;
    Ok((root, path.trim_matches('/').to_string()))
}

pub(crate) async fn canonical_root(root: &str) -> Result<PathBuf, StagingSourceError> {
    tokio::fs::canonicalize(root)
        .await
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))
}

/// Resolves one entry inside the offered root, refusing anything that leaves it
/// and any relative path that tries to climb out before resolution.
pub(crate) async fn jailed_entry(
    root: &Path,
    relative: &str,
) -> Result<PathBuf, StagingSourceError> {
    let candidate = Path::new(relative);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(StagingSourceError::NotFound);
    }
    let resolved = tokio::fs::canonicalize(root.join(candidate))
        .await
        .map_err(map_io_error)?;
    if !resolved.starts_with(root) {
        return Err(StagingSourceError::AccessDenied);
    }
    Ok(resolved)
}

/// The same resolution, restricted to regular files: opening a fifo or a device
/// node inside the offered directory would block or read something that is not
/// the owner's data.
pub(crate) async fn jailed_file(root: &str, relative: &str) -> Result<PathBuf, StagingSourceError> {
    let root = canonical_root(root).await?;
    let resolved = jailed_entry(&root, relative).await?;
    let metadata = tokio::fs::metadata(&resolved).await.map_err(map_io_error)?;
    if !metadata.is_file() {
        return Err(StagingSourceError::NotFound);
    }
    Ok(resolved)
}

fn join_relative(prefix: &str, name: &str) -> String {
    match prefix.is_empty() {
        true => name.to_string(),
        false => format!("{prefix}/{name}"),
    }
}

fn file_metadata(metadata: &std::fs::Metadata) -> SourceMetadata {
    let modified: Option<SystemTime> = metadata.modified().ok();
    SourceMetadata {
        content_length: metadata.len(),
        content_type: None,
        etag: Some(weak_fingerprint(metadata.len(), modified)),
        last_modified: modified,
        source_version: None,
    }
}

pub(crate) fn map_io_error(error: std::io::Error) -> StagingSourceError {
    match error.kind() {
        std::io::ErrorKind::NotFound => StagingSourceError::NotFound,
        std::io::ErrorKind::PermissionDenied => StagingSourceError::AccessDenied,
        _ => StagingSourceError::ReadError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::HashMap;

    fn access(root: &Path, path: &str) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::LocalDirectory,
            config: HashMap::from([(
                OFFERED_DIRECTORY_ROOT.to_string(),
                root.to_string_lossy().to_string(),
            )]),
            path: path.to_string(),
            version: None,
        }
    }

    async fn collect(stream: BackendStream<Result<Bytes, StreamError>>) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        let mut stream = stream.0;
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.map_err(|error| error.to_string())?);
        }
        Ok(bytes)
    }

    #[tokio::test]
    async fn reads_offered_file() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(root.path().join("note.txt"), b"hello")
            .await
            .unwrap();

        let head = head_local(&access(root.path(), "note.txt")).await.unwrap();
        assert_eq!(head.content_length, 5);
        assert!(head.etag.is_some());

        let (metadata, stream) = read_local(&access(root.path(), "note.txt"), None)
            .await
            .unwrap();
        assert_eq!(metadata.etag, head.etag);
        assert_eq!(collect(stream).await.unwrap(), b"hello");
    }

    // A link whose target sits outside the offered root must not be served,
    // however it is addressed.
    #[tokio::test]
    async fn refuses_escaping_link() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::write(outside.path().join("secret"), b"secret")
            .await
            .unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("link"))
            .unwrap();

        assert_eq!(
            head_local(&access(root.path(), "link")).await,
            Err(StagingSourceError::AccessDenied)
        );
        assert_eq!(
            head_local(&access(root.path(), "../secret")).await,
            Err(StagingSourceError::NotFound)
        );
        let (entries, _) = list_local(&access(root.path(), ""), 0, 10, true, false)
            .await
            .unwrap();
        assert!(entries.is_empty());
    }

    // A link that stays inside the offered directory is ordinary data.
    #[tokio::test]
    async fn follows_internal_link() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(root.path().join("real"), b"data")
            .await
            .unwrap();
        std::os::unix::fs::symlink(root.path().join("real"), root.path().join("alias")).unwrap();

        let head = head_local(&access(root.path(), "alias")).await.unwrap();
        assert_eq!(head.content_length, 4);
    }

    #[tokio::test]
    async fn lists_tree() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(root.path().join("nested"))
            .await
            .unwrap();
        tokio::fs::write(root.path().join("top.txt"), b"a")
            .await
            .unwrap();
        tokio::fs::write(root.path().join("nested/inner.txt"), b"bb")
            .await
            .unwrap();

        let (mut entries, truncated) = list_local(&access(root.path(), ""), 0, 10, true, true)
            .await
            .unwrap();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        assert!(!truncated);
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["nested/inner.txt", "top.txt"]);
        assert_eq!(entries[0].size, Some(2));
    }

    // A link back to an ancestor inside the offered root must not make the walk
    // circle forever.
    #[tokio::test]
    async fn bounds_linked_cycle() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(root.path().join("nested"))
            .await
            .unwrap();
        tokio::fs::write(root.path().join("nested/inner.txt"), b"in")
            .await
            .unwrap();
        std::os::unix::fs::symlink(root.path(), root.path().join("nested/loop")).unwrap();

        let (entries, truncated) = list_local(&access(root.path(), ""), 0, 100, true, true)
            .await
            .unwrap();
        assert!(!truncated);
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["nested/inner.txt"]);
    }

    // A rewrite during a complete read must fail the stream: those bytes are
    // not one representation and must never become a content identity.
    #[tokio::test]
    async fn refuses_unstable_read() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("moving.txt");
        tokio::fs::write(&file, b"before").await.unwrap();

        let (_, stream) = read_local(&access(root.path(), "moving.txt"), None)
            .await
            .unwrap();
        tokio::fs::write(&file, b"after-and-longer").await.unwrap();

        assert!(collect(stream).await.is_err());
    }
}
