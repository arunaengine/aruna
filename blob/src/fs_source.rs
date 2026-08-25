//! Access to a directory the owner offers or syncs from their own device.
//!
//! Jail guarantee: the offered root and every requested entry are fully
//! resolved with `canonicalize`, and an entry is refused unless its resolved
//! path is inside the resolved root. A symlink is therefore followed only while
//! it stays inside the offered directory; a link, or a link component, leaving
//! it is refused. The check is not atomic with the open that follows it, so
//! this is a resolve-and-verify guarantee, not a kernel-enforced no-follow
//! open: only regular files are opened.
//!
//! Writes carry the same jail plus a guard the adapter re-verifies immediately
//! before the rename that publishes them. This repeats the operation's decision
//! on purpose: it is the second half of the rule that local bytes are replaced
//! only while they still equal the recorded synced base. Nothing here ever
//! unlinks a file the owner has.

use aruna_core::errors::StagingSourceError;
use aruna_core::events::{LocalFileEvent, LocalFileRefusal};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    OFFERED_DIRECTORY_ROOT, ResolvedSourceAccess, SYNC_TRASH_DIR, SourceConnectorKind, SourceEntry,
    SourceEntryKind, SourceMetadata, WriteGuard, weak_fingerprint,
};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::{HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use ulid::Ulid;

/// Prefix of every path this node maintains inside a folder: the trash, and the
/// temporary files a guarded write spools. Nothing under it is ever offered.
const RESERVED_PREFIX: &str = ".aruna";

/// Candidate names one conflicted copy or move-aside may try before it gives
/// up. A folder with this many same-named copies needs the owner, not a retry.
const MAX_COPY_ATTEMPTS: usize = 100;

pub(crate) fn is_local_access(access: &ResolvedSourceAccess) -> bool {
    let ResolvedSourceAccess::OpenDal { kind, .. } = access;
    *kind == SourceConnectorKind::LocalDirectory
}

pub(crate) async fn check_local(access: &ResolvedSourceAccess) -> Result<(), StagingSourceError> {
    let (root, _) = access_parts(access)?;
    let resolved = canonical_root(&root).await?;
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
            // The trash and the write spool are this node's own bookkeeping,
            // never the owner's data.
            if name.starts_with(RESERVED_PREFIX) {
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

async fn canonical_root(root: &str) -> Result<PathBuf, StagingSourceError> {
    tokio::fs::canonicalize(root)
        .await
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))
}

/// Resolves one entry inside the offered root, refusing anything that leaves it
/// and any relative path that tries to climb out before resolution.
async fn jailed_entry(root: &Path, relative: &str) -> Result<PathBuf, StagingSourceError> {
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
async fn jailed_file(root: &str, relative: &str) -> Result<PathBuf, StagingSourceError> {
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

/// Why a spooled file could not be published. A refusal is the owner's data
/// winning; a failure is this node's problem.
enum PlaceError {
    Refused(LocalFileRefusal),
    Failed(String),
}

/// One fully written temporary file, and the identity of the bytes it holds.
/// The rename preserves size and modification time, so its fingerprint is the
/// fingerprint the published file will carry.
struct Spooled {
    path: PathBuf,
    fingerprint: String,
    blake3: [u8; 32],
    size: u64,
}

/// Writes one file into a synced folder, replacing an existing file only while
/// it still satisfies `guard`. The guard is checked immediately before the
/// rename, so bytes that changed since the operation decided are preserved.
pub(crate) async fn write_guarded(
    root: &str,
    relative: &str,
    guard: &WriteGuard,
    blob: BackendStream<Result<Bytes, StreamError>>,
) -> LocalFileEvent {
    let (parent, target) = match jailed_target(root, relative).await {
        Ok(resolved) => resolved,
        Err(event) => return event,
    };
    let spooled = match spool_temp(&parent, blob).await {
        Ok(spooled) => spooled,
        Err(message) => return LocalFileEvent::Error { message },
    };
    let placed = match guard {
        WriteGuard::MustNotExist => place_new(&spooled.path, &target),
        WriteGuard::MatchesBase {
            fingerprint,
            blake3,
        } => match verify_target(&target, fingerprint, blake3).await {
            Ok(()) => rename_over(&spooled.path, &target),
            Err(refusal) => Err(PlaceError::Refused(refusal)),
        },
    };
    finish_write(spooled, placed, |spooled| LocalFileEvent::Written {
        fingerprint: spooled.fingerprint.clone(),
        blake3: spooled.blake3,
        size: spooled.size,
    })
    .await
}

/// Adds the incoming bytes beside the file under a free conflicted-copy name.
/// The owner's own file is never opened for writing.
pub(crate) async fn write_conflicted(
    root: &str,
    relative: &str,
    at_ms: u64,
    blob: BackendStream<Result<Bytes, StreamError>>,
) -> LocalFileEvent {
    let (parent, _) = match jailed_target(root, relative).await {
        Ok(resolved) => resolved,
        Err(event) => return event,
    };
    let spooled = match spool_temp(&parent, blob).await {
        Ok(spooled) => spooled,
        Err(message) => return LocalFileEvent::Error { message },
    };
    let stamp = conflict_stamp(at_ms);
    let mut placed = Err(PlaceError::Refused(LocalFileRefusal::Exists));
    let mut chosen = String::new();
    for attempt in 1..=MAX_COPY_ATTEMPTS {
        chosen = copy_name(relative, &stamp, attempt);
        placed = place_new(&spooled.path, &parent.join(&chosen));
        if !matches!(placed, Err(PlaceError::Refused(LocalFileRefusal::Exists))) {
            break;
        }
    }
    let relative = sibling_path(relative, &chosen);
    finish_write(spooled, placed, move |spooled| LocalFileEvent::Copied {
        relative: relative.clone(),
        fingerprint: spooled.fingerprint.clone(),
        blake3: spooled.blake3,
        size: spooled.size,
    })
    .await
}

/// Answers the placement outcome and removes the spool when it did not publish.
async fn finish_write(
    spooled: Spooled,
    placed: Result<(), PlaceError>,
    written: impl FnOnce(&Spooled) -> LocalFileEvent,
) -> LocalFileEvent {
    match placed {
        Ok(()) => written(&spooled),
        Err(error) => {
            let _ = tokio::fs::remove_file(&spooled.path).await;
            match error {
                PlaceError::Refused(reason) => LocalFileEvent::Refused { reason },
                PlaceError::Failed(message) => LocalFileEvent::Error { message },
            }
        }
    }
}

/// Moves one file into the folder's trash. A removal is never an unlink: the
/// bytes stay on the owner's disk until the owner removes them.
pub(crate) async fn move_aside(root: &str, relative: &str) -> LocalFileEvent {
    let resolved_root = match canonical_root(root).await {
        Ok(root) => root,
        Err(error) => {
            return LocalFileEvent::Error {
                message: error.to_string(),
            };
        }
    };
    match jailed_file(root, relative).await {
        Ok(_) => {}
        Err(StagingSourceError::NotFound) => {
            return LocalFileEvent::Refused {
                reason: LocalFileRefusal::Missing,
            };
        }
        Err(StagingSourceError::AccessDenied) => {
            return LocalFileEvent::Refused {
                reason: LocalFileRefusal::Escaped,
            };
        }
        Err(error) => {
            return LocalFileEvent::Error {
                message: error.to_string(),
            };
        }
    }
    let trashed = format!("{SYNC_TRASH_DIR}/{relative}");
    let (parent, _) = match jailed_target(root, &trashed).await {
        Ok(resolved) => resolved,
        Err(event) => return event,
    };
    let source = resolved_root.join(relative);
    for attempt in 1..=MAX_COPY_ATTEMPTS {
        let candidate = suffixed_name(relative, attempt);
        match place_new(&source, &parent.join(&candidate)) {
            Ok(()) => {
                return LocalFileEvent::Moved {
                    to: sibling_path(&trashed, &candidate),
                };
            }
            Err(PlaceError::Refused(LocalFileRefusal::Exists)) => continue,
            Err(PlaceError::Refused(reason)) => return LocalFileEvent::Refused { reason },
            Err(PlaceError::Failed(message)) => return LocalFileEvent::Error { message },
        }
    }
    LocalFileEvent::Refused {
        reason: LocalFileRefusal::Exists,
    }
}

/// The stable identity of one file: its weak fingerprint and its blake3, read
/// as one observation and refused when the file moved underneath the read.
pub(crate) async fn hash_local(root: &str, relative: &str) -> LocalFileEvent {
    let resolved = match jailed_file(root, relative).await {
        Ok(resolved) => resolved,
        Err(StagingSourceError::NotFound) => {
            return LocalFileEvent::Refused {
                reason: LocalFileRefusal::Missing,
            };
        }
        Err(StagingSourceError::AccessDenied) => {
            return LocalFileEvent::Refused {
                reason: LocalFileRefusal::Escaped,
            };
        }
        Err(error) => {
            return LocalFileEvent::Error {
                message: error.to_string(),
            };
        }
    };
    match hash_stable(&resolved).await {
        Ok((fingerprint, blake3, size)) => LocalFileEvent::Hashed {
            fingerprint,
            blake3,
            size,
        },
        Err(error) => LocalFileEvent::Error {
            message: error.to_string(),
        },
    }
}

/// Resolves the directory a write lands in, creating missing directories under
/// the root, and refuses anything that resolves outside it.
async fn jailed_target(root: &str, relative: &str) -> Result<(PathBuf, PathBuf), LocalFileEvent> {
    let escaped = LocalFileEvent::Refused {
        reason: LocalFileRefusal::Escaped,
    };
    let root = canonical_root(root)
        .await
        .map_err(|error| LocalFileEvent::Error {
            message: error.to_string(),
        })?;
    let candidate = Path::new(relative);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(escaped);
    }
    let Some(name) = candidate.file_name() else {
        return Err(escaped);
    };
    let parent = match candidate.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => root.join(parent),
        _ => root.clone(),
    };
    tokio::fs::create_dir_all(&parent)
        .await
        .map_err(|error| LocalFileEvent::Error {
            message: error.to_string(),
        })?;
    let parent = tokio::fs::canonicalize(&parent)
        .await
        .map_err(|error| LocalFileEvent::Error {
            message: error.to_string(),
        })?;
    if !parent.starts_with(&root) {
        return Err(escaped);
    }
    let target = parent.join(name);
    Ok((parent, target))
}

/// Streams the incoming bytes into a temporary file in the directory the write
/// lands in, so publishing it is a rename inside one filesystem.
async fn spool_temp(
    parent: &Path,
    blob: BackendStream<Result<Bytes, StreamError>>,
) -> Result<Spooled, String> {
    let path = parent.join(format!("{RESERVED_PREFIX}-tmp-{}", Ulid::generate()));
    match write_temp(&path, blob).await {
        Ok(spooled) => Ok(spooled),
        Err(error) => {
            let _ = tokio::fs::remove_file(&path).await;
            Err(error)
        }
    }
}

async fn write_temp(
    path: &Path,
    blob: BackendStream<Result<Bytes, StreamError>>,
) -> Result<Spooled, String> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|error| error.to_string())?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut stream = blob.0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        hasher.update(&chunk);
        size = size.saturating_add(chunk.len() as u64);
        file.write_all(&chunk)
            .await
            .map_err(|error| error.to_string())?;
    }
    file.flush().await.map_err(|error| error.to_string())?;
    file.sync_all().await.map_err(|error| error.to_string())?;
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Spooled {
        path: path.to_path_buf(),
        fingerprint: weak_fingerprint(metadata.len(), metadata.modified().ok()),
        blake3: *hasher.finalize().as_bytes(),
        size,
    })
}

/// The second half of the replace rule: the target must still carry exactly the
/// bytes the guard names, by weak fingerprint and by strong hash.
async fn verify_target(
    target: &Path,
    fingerprint: &str,
    blake3: &[u8; 32],
) -> Result<(), LocalFileRefusal> {
    let Ok(link) = tokio::fs::symlink_metadata(target).await else {
        return Err(LocalFileRefusal::Missing);
    };
    if !link.is_file() {
        return Err(LocalFileRefusal::NotRegular);
    }
    let Ok((current, hash, _)) = hash_stable(target).await else {
        return Err(LocalFileRefusal::Drifted);
    };
    match current == fingerprint && &hash == blake3 {
        true => Ok(()),
        false => Err(LocalFileRefusal::Drifted),
    }
}

/// Hashes one file and refuses the result when the file changed while it was
/// read: those bytes are not one representation of anything.
async fn hash_stable(path: &Path) -> Result<(String, [u8; 32], u64), StagingSourceError> {
    let before = tokio::fs::metadata(path).await.map_err(map_io_error)?;
    let fingerprint = weak_fingerprint(before.len(), before.modified().ok());
    let mut file = tokio::fs::File::open(path).await.map_err(map_io_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let read = tokio::io::AsyncReadExt::read(&mut file, &mut buffer)
            .await
            .map_err(map_io_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = tokio::fs::metadata(path).await.map_err(map_io_error)?;
    if weak_fingerprint(after.len(), after.modified().ok()) != fingerprint {
        return Err(StagingSourceError::SourceUnstable);
    }
    Ok((fingerprint, *hasher.finalize().as_bytes(), before.len()))
}

/// Publishes a spooled file only where nothing exists, in one atomic step.
fn place_new(source: &Path, target: &Path) -> Result<(), PlaceError> {
    match rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        target,
        rustix::fs::RenameFlags::NOREPLACE,
    ) {
        Ok(()) => Ok(()),
        Err(rustix::io::Errno::EXIST) => Err(PlaceError::Refused(LocalFileRefusal::Exists)),
        // A filesystem without the atomic flag still must not overwrite: a hard
        // link fails when the name is taken, and the spool is removed after.
        Err(rustix::io::Errno::INVAL | rustix::io::Errno::NOSYS | rustix::io::Errno::OPNOTSUPP) => {
            link_new(source, target)
        }
        Err(error) => Err(PlaceError::Failed(error.to_string())),
    }
}

fn link_new(source: &Path, target: &Path) -> Result<(), PlaceError> {
    match std::fs::hard_link(source, target) {
        Ok(()) => {
            let _ = std::fs::remove_file(source);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(PlaceError::Refused(LocalFileRefusal::Exists))
        }
        Err(error) => Err(PlaceError::Failed(error.to_string())),
    }
}

fn rename_over(source: &Path, target: &Path) -> Result<(), PlaceError> {
    std::fs::rename(source, target).map_err(|error| PlaceError::Failed(error.to_string()))
}

/// `2026-08-25 1032`, the stamp a conflicted copy is named after.
fn conflict_stamp(at_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(at_ms as i64)
        .map(|stamp| stamp.format("%Y-%m-%d %H%M").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// `note (conflicted copy 2026-08-25 1032, realm).txt`, with a numeric suffix
/// from the second attempt on.
fn copy_name(relative: &str, stamp: &str, attempt: usize) -> String {
    let (stem, extension) = split_name(relative);
    let suffix = match attempt {
        1 => String::new(),
        other => format!(" ({other})"),
    };
    format!("{stem} (conflicted copy {stamp}, realm){suffix}{extension}")
}

/// The same file name, with a numeric suffix from the second attempt on.
fn suffixed_name(relative: &str, attempt: usize) -> String {
    let (stem, extension) = split_name(relative);
    match attempt {
        1 => format!("{stem}{extension}"),
        other => format!("{stem} ({other}){extension}"),
    }
}

/// Splits one relative path into the file's stem and its extension, dot kept.
fn split_name(relative: &str) -> (String, String) {
    let path = Path::new(relative);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(relative);
    match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => (stem.to_string(), format!(".{extension}")),
        _ => (name.to_string(), String::new()),
    }
}

/// The relative path of a file that sits next to `relative` under `name`.
fn sibling_path(relative: &str, name: &str) -> String {
    match relative.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/{name}"),
        None => name.to_string(),
    }
}

fn map_io_error(error: std::io::Error) -> StagingSourceError {
    match error.kind() {
        std::io::ErrorKind::NotFound => StagingSourceError::NotFound,
        std::io::ErrorKind::PermissionDenied => StagingSourceError::AccessDenied,
        _ => StagingSourceError::ReadError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn body(bytes: &'static [u8]) -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(tokio_util::io::ReaderStream::new(bytes))
    }

    async fn identity(path: &Path) -> (String, [u8; 32]) {
        let (fingerprint, blake3, _) = hash_stable(path).await.expect("file must hash");
        (fingerprint, blake3)
    }

    #[tokio::test]
    async fn writes_new_file() {
        // A path whose directories do not exist yet is created under the root.
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let event = write_guarded(
            &path,
            "nested/new.txt",
            &WriteGuard::MustNotExist,
            body(b"new"),
        )
        .await;
        assert!(matches!(event, LocalFileEvent::Written { size: 3, .. }));
        assert_eq!(
            tokio::fs::read(root.path().join("nested/new.txt"))
                .await
                .unwrap(),
            b"new"
        );
    }

    // A guard that forbids replacement must never overwrite, whatever is there.
    #[tokio::test]
    async fn refuses_existing_target() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"mine").await.unwrap();

        let event = write_guarded(
            &path,
            "note.txt",
            &WriteGuard::MustNotExist,
            body(b"theirs"),
        )
        .await;
        assert_eq!(
            event,
            LocalFileEvent::Refused {
                reason: LocalFileRefusal::Exists
            }
        );
        assert_eq!(tokio::fs::read(&file).await.unwrap(), b"mine");
    }

    #[tokio::test]
    async fn replaces_matching_base() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"base").await.unwrap();
        let (fingerprint, blake3) = identity(&file).await;

        let event = write_guarded(
            &path,
            "note.txt",
            &WriteGuard::MatchesBase {
                fingerprint,
                blake3,
            },
            body(b"newer"),
        )
        .await;
        assert!(matches!(event, LocalFileEvent::Written { size: 5, .. }));
        assert_eq!(tokio::fs::read(&file).await.unwrap(), b"newer");
    }

    // The one rule the whole design rests on: bytes that no longer equal the
    // recorded base are kept, even though the operation asked for a replace.
    #[tokio::test]
    async fn refuses_drifted_target() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"base").await.unwrap();
        let (fingerprint, blake3) = identity(&file).await;
        tokio::fs::write(&file, b"the owner's own edit")
            .await
            .unwrap();

        let event = write_guarded(
            &path,
            "note.txt",
            &WriteGuard::MatchesBase {
                fingerprint,
                blake3,
            },
            body(b"newer"),
        )
        .await;
        assert_eq!(
            event,
            LocalFileEvent::Refused {
                reason: LocalFileRefusal::Drifted
            }
        );
        assert_eq!(
            tokio::fs::read(&file).await.unwrap(),
            b"the owner's own edit"
        );
        let spooled = std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(RESERVED_PREFIX)
            })
            .count();
        assert_eq!(spooled, 0);
    }

    #[tokio::test]
    async fn names_conflicted_copies() {
        // A second conflict for the same file must not overwrite the first.
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::write(root.path().join("note.txt"), b"mine")
            .await
            .unwrap();

        let first = write_conflicted(&path, "note.txt", 1_756_000_000_000, body(b"a")).await;
        let second = write_conflicted(&path, "note.txt", 1_756_000_000_000, body(b"b")).await;
        let (
            LocalFileEvent::Copied {
                relative: first, ..
            },
            LocalFileEvent::Copied {
                relative: second, ..
            },
        ) = (first, second)
        else {
            panic!("both copies must land beside the file");
        };
        assert_ne!(first, second);
        assert!(first.starts_with("note (conflicted copy "));
        assert!(first.ends_with(", realm).txt"));
        assert!(second.ends_with(", realm) (2).txt"));
        assert_eq!(
            tokio::fs::read(root.path().join("note.txt")).await.unwrap(),
            b"mine"
        );
        assert_eq!(
            tokio::fs::read(root.path().join(&first)).await.unwrap(),
            b"a"
        );
    }

    #[tokio::test]
    async fn moves_file_aside() {
        // Removing a file is a move into the trash; the bytes stay on disk.
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::write(root.path().join("gone.txt"), b"kept")
            .await
            .unwrap();

        let LocalFileEvent::Moved { to } = move_aside(&path, "gone.txt").await else {
            panic!("the file must move into the trash");
        };
        assert_eq!(to, format!("{SYNC_TRASH_DIR}/gone.txt"));
        assert!(!root.path().join("gone.txt").exists());
        assert_eq!(
            tokio::fs::read(root.path().join(&to)).await.unwrap(),
            b"kept"
        );
        assert_eq!(
            move_aside(&path, "gone.txt").await,
            LocalFileEvent::Refused {
                reason: LocalFileRefusal::Missing
            }
        );
    }

    #[tokio::test]
    async fn refuses_escaping_write() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        std::os::unix::fs::symlink(outside.path(), root.path().join("away")).unwrap();

        let escaped = LocalFileEvent::Refused {
            reason: LocalFileRefusal::Escaped,
        };
        assert_eq!(
            write_guarded(&path, "../out.txt", &WriteGuard::MustNotExist, body(b"x")).await,
            escaped
        );
        assert_eq!(
            write_guarded(&path, "away/out.txt", &WriteGuard::MustNotExist, body(b"x")).await,
            escaped
        );
        assert!(!outside.path().join("out.txt").exists());
    }

    // The trash and the write spool are this node's bookkeeping, so a sweep
    // must never offer them back as the owner's data.
    #[tokio::test]
    async fn hides_reserved_entries() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::write(root.path().join("keep.txt"), b"keep")
            .await
            .unwrap();
        move_aside(&path, "keep.txt").await;
        tokio::fs::write(root.path().join("keep.txt"), b"again")
            .await
            .unwrap();

        let (entries, _) = list_local(&access(root.path(), ""), 0, 10, true, true)
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["keep.txt"]);
    }

    #[test]
    fn splits_copy_names() {
        assert_eq!(
            copy_name("a/note.txt", "s", 1),
            "note (conflicted copy s, realm).txt"
        );
        assert_eq!(
            copy_name("note", "s", 2),
            "note (conflicted copy s, realm) (2)"
        );
        assert_eq!(suffixed_name("a/note.txt", 3), "note (3).txt");
        assert_eq!(sibling_path("a/note.txt", "other.txt"), "a/other.txt");
        assert_eq!(sibling_path("note.txt", "other.txt"), "other.txt");
    }

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
