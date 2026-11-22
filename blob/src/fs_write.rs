//! Writes files into an owner's synced folder through a guard rechecked at rename time.
//! A conflict makes a copy instead of replacing, and a removal moves the file aside.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StagingSourceError;
use aruna_core::events::{LocalFileEvent, LocalFileRefusal};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::offered_directory::{FileStat, weak_fingerprint};
use aruna_core::structs::synced_folder::{SYNC_TRASH_DIR, WriteGuard};
use bytes::Bytes;
use futures::StreamExt;
use std::path::{Component, Path, PathBuf};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::fs_source::{
    KEPT_DIR, MAX_COPY_ATTEMPTS, SPOOL_DIR, SPOOL_GRACE, canonical_root, jailed_file, map_io_error,
};

/// Why a spooled file could not be published. A refusal is the owner's data
/// winning; a failure is this node's problem.
enum PlaceError {
    Refused(LocalFileRefusal),
    Failed(String),
    /// The owner's displaced bytes could not be put aside and stay at this path,
    /// which must survive the cleanup of a failed write.
    Kept(PathBuf),
}

/// One fully written temporary file, and the identity of the bytes it holds.
/// Its weak fingerprint is not the published file's: publishing moves the name,
/// which moves the change time, so the fingerprint is read at the target.
struct Spooled {
    path: PathBuf,
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
    let (_, target) = match jailed_target(root, relative).await {
        Ok(resolved) => resolved,
        Err(event) => return event,
    };
    let spooled = match spool_file(root, blob).await {
        Ok(spooled) => spooled,
        Err(message) => return LocalFileEvent::Error { message },
    };
    let placed = match guard {
        WriteGuard::MustNotExist => place_new(&spooled.path, &target),
        // The weak fingerprint is the operation's own pre-check; what the
        // adapter re-verifies under the swap is the strong hash.
        WriteGuard::MatchesBase { blake3, .. } => match reserved_file(root, KEPT_DIR).await {
            Ok(kept) => exchange_guarded(&spooled, &kept, &target, blake3).await,
            Err(message) => Err(PlaceError::Failed(message)),
        },
    };
    finish_write(spooled, placed, &target, |fingerprint, spooled| {
        LocalFileEvent::Written {
            fingerprint,
            blake3: spooled.blake3,
            size: spooled.size,
        }
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
    let spooled = match spool_file(root, blob).await {
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
    let published = parent.join(&chosen);
    let relative = sibling_path(relative, &chosen);
    finish_write(spooled, placed, &published, move |fingerprint, spooled| {
        LocalFileEvent::Copied {
            relative: relative.clone(),
            fingerprint,
            blake3: spooled.blake3,
            size: spooled.size,
        }
    })
    .await
}

/// Answers the placement outcome and removes the spool when it did not publish.
/// The fingerprint is read at the published path, in the same stable read that
/// checks its bytes, because the rename moves the change time a spool carries.
async fn finish_write(
    spooled: Spooled,
    placed: Result<(), PlaceError>,
    published: &Path,
    written: impl FnOnce(String, &Spooled) -> LocalFileEvent,
) -> LocalFileEvent {
    match placed {
        // The hash answers for the spool's bytes, so it is only reported with a
        // file that still has exactly those bytes.
        Ok(()) => match hash_stable(published).await {
            Ok((fingerprint, blake3, _)) if blake3 == spooled.blake3 => {
                written(fingerprint, &spooled)
            }
            _ => LocalFileEvent::Error {
                message: "the published file is not the one that was written".to_string(),
            },
        },
        Err(PlaceError::Kept(path)) => LocalFileEvent::Error {
            message: format!(
                "the displaced local file could not be put aside and was kept at {}",
                path.display()
            ),
        },
        Err(PlaceError::Refused(reason)) => {
            let _ = tokio::fs::remove_file(&spooled.path).await;
            LocalFileEvent::Refused { reason }
        }
        Err(PlaceError::Failed(message)) => {
            let _ = tokio::fs::remove_file(&spooled.path).await;
            LocalFileEvent::Error { message }
        }
    }
}

/// Moves one file into the folder's trash. A removal is never an unlink: the
/// bytes stay on the owner's disk until the owner removes them.
pub(crate) async fn move_aside(root: &str, relative: &str, guard: &WriteGuard) -> LocalFileEvent {
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
    let resolved = resolved_root.join(relative);
    if let WriteGuard::MatchesBase {
        fingerprint,
        blake3,
    } = guard
    {
        // The owner asked to remove exactly these bytes; anything else stays.
        match hash_stable(&resolved).await {
            Ok((current, hash, _)) if current == *fingerprint && hash == *blake3 => {}
            Ok(_) => {
                return LocalFileEvent::Refused {
                    reason: LocalFileRefusal::Drifted,
                };
            }
            Err(_) => {
                return LocalFileEvent::Refused {
                    reason: LocalFileRefusal::Drifted,
                };
            }
        }
    }
    let trashed = format!("{SYNC_TRASH_DIR}/{relative}");
    let (parent, _) = match jailed_target(root, &trashed).await {
        Ok(resolved) => resolved,
        Err(event) => return event,
    };
    let source = resolved;
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
            Err(PlaceError::Kept(path)) => {
                let message = format!("the file was kept at {}", path.display());
                return LocalFileEvent::Error { message };
            }
        }
    }
    LocalFileEvent::Refused {
        reason: LocalFileRefusal::Exists,
    }
}

/// The stable identity of one file: its weak fingerprint and its blake3, read
/// as one observation and refused when the file moved underneath the read.
pub(crate) async fn hash_local(root: &str, relative: &str) -> LocalFileEvent {
    let file = match crate::fs_source::open_file(root, relative).await {
        Ok(file) => file,
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
    match hash_file(file).await {
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
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new(""),
    };
    // Directories are created only after the deepest existing ancestor is
    // jailed inside the root, so a link on the way out is never followed.
    let anchor = jailed_ancestor(&root, parent).await?;
    if !anchor.starts_with(&root) {
        return Err(escaped);
    }
    let parent = root.join(parent);
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
    Ok((parent.clone(), parent.join(name)))
}

/// Resolves the deepest existing directory of `relative` under `root`, refusing
/// a symlink anywhere along the way. Every component that does exist must be a
/// real directory inside the root before anything is created.
async fn jailed_ancestor(root: &Path, relative: &Path) -> Result<PathBuf, LocalFileEvent> {
    let escaped = LocalFileEvent::Refused {
        reason: LocalFileRefusal::Escaped,
    };
    let mut anchor = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err(escaped);
        };
        let next = anchor.join(part);
        match tokio::fs::symlink_metadata(&next).await {
            // A link component is refused rather than resolved: a write must
            // never follow one, even one that stays inside the root.
            Ok(metadata) if metadata.file_type().is_symlink() => return Err(escaped),
            Ok(metadata) if metadata.is_dir() => anchor = next,
            Ok(_) => return Err(escaped),
            // The rest of the path does not exist yet, so the anchor is final.
            Err(_) => break,
        }
    }
    tokio::fs::canonicalize(&anchor)
        .await
        .map_err(|error| LocalFileEvent::Error {
            message: error.to_string(),
        })
}

/// Removes what a crashed write left in the spool directory. Nothing outside
/// that directory is ever touched, everything inside it is this node's own, and
/// a file young enough to belong to a write in flight is left alone.
pub(crate) async fn sweep_spool(root: &str) -> usize {
    let Ok(root) = canonical_root(root).await else {
        return 0;
    };
    let Ok(spool) = jailed_ancestor(&root, Path::new(SPOOL_DIR)).await else {
        return 0;
    };
    if spool != root.join(SPOOL_DIR) {
        return 0;
    }
    let Ok(mut reader) = tokio::fs::read_dir(spool).await else {
        return 0;
    };
    let now = std::time::SystemTime::now();
    let mut removed = 0usize;
    while let Ok(Some(entry)) = reader.next_entry().await {
        if in_flight(&entry, now).await {
            continue;
        }
        if tokio::fs::remove_file(entry.path()).await.is_ok() {
            removed += 1;
        }
    }
    removed
}

/// Whether a spool file is young enough that a concurrent pass may still be
/// writing it. An unreadable timestamp counts as in flight: the bytes are this
/// node's own either way, and the next sweep will find it again.
async fn in_flight(entry: &tokio::fs::DirEntry, now: std::time::SystemTime) -> bool {
    let Ok(metadata) = entry.metadata().await else {
        return true;
    };
    metadata.modified().ok().is_none_or(|modified| {
        now.duration_since(modified)
            .is_ok_and(|age| age < SPOOL_GRACE)
    })
}

/// Streams the incoming bytes into the folder's own spool directory, so
/// publishing them is a rename inside one filesystem and no temporary file ever
/// carries a name in the owner's own tree.
async fn spool_file(
    root: &str,
    blob: BackendStream<Result<Bytes, StreamError>>,
) -> Result<Spooled, String> {
    let path = reserved_file(root, SPOOL_DIR).await?;
    match write_temp(&path, blob).await {
        Ok(spooled) => Ok(spooled),
        Err(error) => {
            let _ = tokio::fs::remove_file(&path).await;
            Err(error)
        }
    }
}

/// A fresh path in one of the folder's reserved directories, created when missing.
async fn reserved_file(root: &str, directory: &str) -> Result<PathBuf, String> {
    let relative = format!("{directory}/{}", Ulid::generate());
    let (_, path) = jailed_target(root, &relative)
        .await
        .map_err(|event| format!("reserved path refused: {event:?}"))?;
    Ok(path)
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
    Ok(Spooled {
        path: path.to_path_buf(),
        blake3: *hasher.finalize().as_bytes(),
        size,
    })
}

/// The second half of the replace rule: the spool and target swap places
/// atomically, then only displaced bytes matching the guard are discarded;
/// anything else is swapped back (beside the target on Windows).
async fn exchange_guarded(
    spooled: &Spooled,
    kept: &Path,
    target: &Path,
    blake3: &[u8; 32],
) -> Result<(), PlaceError> {
    let Ok(link) = tokio::fs::symlink_metadata(target).await else {
        return Err(PlaceError::Refused(LocalFileRefusal::Missing));
    };
    if !link.is_file() {
        return Err(PlaceError::Refused(LocalFileRefusal::NotRegular));
    }
    swap_in(&spooled.path, kept, target).await?;
    // The displaced file now sits at the kept path, where only its strong
    // hash can answer for it: the exchange renamed away its fingerprint.
    if holds_hash(kept, blake3).await {
        let _ = tokio::fs::remove_file(kept).await;
        return Ok(());
    }
    #[cfg(not(windows))]
    let rolled = swap_back(kept, target).await;
    // Windows has no atomic exchange, so there the live file keeps the target name.
    #[cfg(windows)]
    let rolled = keep_displaced(kept, target).await;
    Err(rolled)
}

/// Swaps the spool in for the target through the kept path, so the bytes it
/// displaces never sit where a sweep removes them.
#[cfg(not(windows))]
async fn swap_in(spool: &Path, kept: &Path, target: &Path) -> Result<(), PlaceError> {
    place_new(spool, kept)?;
    let swapped = exchange(kept, target);
    if swapped.is_err() {
        // Nothing moved, so the kept path still holds only the incoming bytes.
        let _ = tokio::fs::remove_file(kept).await;
    }
    swapped
}

#[cfg(windows)]
async fn swap_in(spool: &Path, kept: &Path, target: &Path) -> Result<(), PlaceError> {
    match replace_file(target, spool, kept) {
        Ok(()) => Ok(()),
        Err(error) => Err(undo_replace(kept, target, error.to_string()).await),
    }
}

/// Undoes a failed replace. A partial one (Windows error 1177) left the owner's
/// file at `kept` and the target vacant, so it goes back there or beside a later save.
#[cfg(any(windows, test))]
async fn undo_replace(kept: &Path, target: &Path, error: String) -> PlaceError {
    if !kept.exists() {
        return PlaceError::Failed(error);
    }
    match put_back(kept, target).await {
        PlaceError::Kept(path) => PlaceError::Kept(path),
        _ => PlaceError::Failed(error),
    }
}

/// Rolls back a replace whose displaced owner file at `kept` drifted. The owner's
/// file first takes a visible copy name, then one swap puts it back at the target
/// and leaves the live file under a visible copy name, so nothing is hidden or lost.
#[cfg(not(windows))]
async fn swap_back(kept: &Path, target: &Path) -> PlaceError {
    let copy = match rescue_displaced(kept, target).await {
        Ok(copy) => copy,
        Err(_) => return PlaceError::Kept(kept.to_path_buf()),
    };
    // A directory or link a local tool put at the target stays, the owner's file beside it.
    let is_file = tokio::fs::symlink_metadata(target)
        .await
        .is_ok_and(|entry| entry.is_file());
    if !is_file || exchange(&copy, target).is_err() {
        // A vacant target takes the owner's file back; a taken one keeps its save.
        let _ = place_new(&copy, target);
    }
    PlaceError::Refused(LocalFileRefusal::Drifted)
}

/// Puts the owner's file back at the vacant target. A save that took the name
/// in the meantime is newer and stays; this file then goes beside it.
#[cfg(any(windows, test))]
async fn put_back(file: &Path, target: &Path) -> PlaceError {
    match place_new(file, target) {
        Ok(()) => PlaceError::Refused(LocalFileRefusal::Drifted),
        Err(_) => keep_displaced(file, target).await,
    }
}

/// Puts the owner's bytes at `kept` beside the target, or names the path they
/// stay at when that fails.
#[cfg(any(windows, test))]
async fn keep_displaced(kept: &Path, target: &Path) -> PlaceError {
    match rescue_displaced(kept, target).await {
        Ok(_) => PlaceError::Refused(LocalFileRefusal::Drifted),
        Err(_) => PlaceError::Kept(kept.to_path_buf()),
    }
}

/// Whether the file at `path` holds exactly the bytes `blake3` names.
async fn holds_hash(path: &Path, blake3: &[u8; 32]) -> bool {
    matches!(hash_stable(path).await, Ok((_, hash, _)) if hash == *blake3)
}

/// Swaps two existing paths in one step. Neither file is ever destroyed.
#[cfg(not(windows))]
fn exchange(left: &Path, right: &Path) -> Result<(), PlaceError> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        left,
        rustix::fs::CWD,
        right,
        rustix::fs::RenameFlags::EXCHANGE,
    )
    .map_err(|error| PlaceError::Failed(error.to_string()))
}

#[cfg(windows)]
fn replace_file(target: &Path, replacement: &Path, backup: &Path) -> std::io::Result<()> {
    let (target, replacement, backup) = (wide(target), wide(replacement), wide(backup));
    let result = unsafe {
        windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            target.as_ptr(),
            replacement.as_ptr(),
            backup.as_ptr(),
            0,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// The path as the nul-terminated UTF-16 string the Windows file functions take.
#[cfg(windows)]
fn wide(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

/// Puts displaced bytes beside the target under a free conflict copy name and
/// answers that path, so they stay on the owner's disk under a findable name.
async fn rescue_displaced(spool: &Path, target: &Path) -> Result<PathBuf, PlaceError> {
    for candidate in copy_paths(target)? {
        if place_new(spool, &candidate).is_ok() {
            return Ok(candidate);
        }
    }
    Err(PlaceError::Failed(
        "the displaced file could not be put aside".to_string(),
    ))
}

/// The conflict copy names beside the target, in the order a copy tries them.
fn copy_paths(target: &Path) -> Result<impl Iterator<Item = PathBuf>, PlaceError> {
    let Some(parent) = target.parent().map(Path::to_path_buf) else {
        return Err(PlaceError::Failed(
            "the target has no directory".to_string(),
        ));
    };
    let relative = target.to_string_lossy().to_string();
    let stamp = conflict_stamp(aruna_clock::unix_timestamp_millis());
    Ok((1..=MAX_COPY_ATTEMPTS)
        .map(move |attempt| parent.join(copy_name(&relative, &stamp, attempt))))
}

/// Hashes one file and refuses the result when the file changed while it was
/// read: those bytes are not one representation of anything.
async fn hash_stable(path: &Path) -> Result<(String, [u8; 32], u64), StagingSourceError> {
    let hashed = hash_file(tokio::fs::File::open(path).await.map_err(map_io_error)?).await?;
    let after = tokio::fs::metadata(path).await.map_err(map_io_error)?;
    if weak_fingerprint(&FileStat::from_metadata(&after)) != hashed.0 {
        return Err(StagingSourceError::SourceUnstable);
    }
    Ok(hashed)
}

async fn hash_file(
    mut file: tokio::fs::File,
) -> Result<(String, [u8; 32], u64), StagingSourceError> {
    let before = file.metadata().await.map_err(map_io_error)?;
    let fingerprint = weak_fingerprint(&FileStat::from_metadata(&before));
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
    let after = file.metadata().await.map_err(map_io_error)?;
    if weak_fingerprint(&FileStat::from_metadata(&after)) != fingerprint {
        return Err(StagingSourceError::SourceUnstable);
    }
    Ok((fingerprint, *hasher.finalize().as_bytes(), before.len()))
}

/// Publishes a spooled file only where nothing exists, in one atomic step.
#[cfg(not(windows))]
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

/// Without `MOVEFILE_REPLACE_EXISTING` the move refuses any entry at the target.
#[cfg(windows)]
fn place_new(source: &Path, target: &Path) -> Result<(), PlaceError> {
    let (source, target) = (wide(source), wide(target));
    let moved = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(source.as_ptr(), target.as_ptr(), 0)
    };
    if moved != 0 {
        return Ok(());
    }
    match std::io::Error::last_os_error() {
        error if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(PlaceError::Refused(LocalFileRefusal::Exists))
        }
        error => Err(PlaceError::Failed(error.to_string())),
    }
}

#[cfg(not(windows))]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs_source::list_local;
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;

    fn access(root: &Path, path: &str) -> ResolvedSourceAccess {
        crate::fs_source::tests::access(root, path)
    }

    fn body(bytes: &'static [u8]) -> BackendStream<Result<Bytes, StreamError>> {
        BackendStream::new(tokio_util::io::ReaderStream::new(bytes))
    }

    async fn identity(path: &Path) -> (String, [u8; 32]) {
        let (fingerprint, blake3, _) = hash_stable(path).await.expect("file must hash");
        (fingerprint, blake3)
    }

    // A displaced owner file that could not be put aside survives the cleanup.
    #[tokio::test]
    async fn keeps_unrescued_file() {
        let root = tempfile::tempdir().unwrap();
        let kept = root.path().join("displaced");
        tokio::fs::write(&kept, b"owner edit").await.unwrap();
        let spooled = Spooled {
            path: kept.clone(),
            blake3: [0; 32],
            size: 10,
        };
        let placed = Err(PlaceError::Kept(kept.clone()));
        let event = finish_write(spooled, placed, &root.path().join("target"), |_, _| {
            LocalFileEvent::Error {
                message: "not published".to_string(),
            }
        })
        .await;
        assert!(matches!(event, LocalFileEvent::Error { .. }));
        assert_eq!(tokio::fs::read(&kept).await.unwrap(), b"owner edit");
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

    // The fingerprint a write reports must match the next observation, or
    // every synced file would look changed and upload again.
    #[tokio::test]
    async fn write_matches_listing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();

        let LocalFileEvent::Written { fingerprint, .. } =
            write_guarded(&path, "note.txt", &WriteGuard::MustNotExist, body(b"hello")).await
        else {
            panic!("the file must be written");
        };

        let (entries, _) = list_local(&access(root.path(), ""), Vec::new(), 10, true, true)
            .await
            .unwrap();
        let observed = entries
            .iter()
            .find(|entry| entry.path == "note.txt")
            .and_then(|entry| entry.stat)
            .expect("the listing stats the file it lists");
        assert_eq!(fingerprint, weak_fingerprint(&observed));
    }

    // A guarded replace answers for the base by its strong hash: the swap moves
    // the displaced file's name, so its weak fingerprint cannot answer here.
    #[tokio::test]
    async fn replaces_under_guard() {
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

        let LocalFileEvent::Written { fingerprint, .. } = event else {
            panic!("an unchanged base must be replaced");
        };
        assert_eq!(tokio::fs::read(&file).await.unwrap(), b"newer");
        let (entries, _) = list_local(&access(root.path(), ""), Vec::new(), 10, true, true)
            .await
            .unwrap();
        let observed = entries
            .iter()
            .find(|entry| entry.path == "note.txt")
            .and_then(|entry| entry.stat)
            .expect("the listing stats the file it lists");
        assert_eq!(fingerprint, weak_fingerprint(&observed));
    }

    // The one rule the whole design rests on: bytes that no longer equal the
    // recorded base are kept, even though the operation asked for a replace.
    // Windows keeps the download at the target and the owner's edit beside it.
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
        let live: &[u8] = match cfg!(windows) {
            true => b"newer",
            false => b"the owner's own edit",
        };
        assert_eq!(tokio::fs::read(&file).await.unwrap(), live);
        for directory in [SPOOL_DIR, KEPT_DIR] {
            let left = std::fs::read_dir(root.path().join(directory))
                .map(|entries| entries.filter_map(Result::ok).count())
                .unwrap_or_default();
            assert_eq!(left, 0, "{directory} must be empty");
        }
        // The displaced version stays beside the live file as a visible copy.
        assert_eq!(
            listed_bytes(root.path()).await,
            vec![b"newer".to_vec(), b"the owner's own edit".to_vec()]
        );
    }

    /// Saves `bytes` the way an editor does: a new file renamed over the target.
    async fn editor_save(target: &Path, bytes: &[u8]) {
        let save = target.with_extension("save");
        tokio::fs::write(&save, bytes).await.unwrap();
        tokio::fs::rename(&save, target).await.unwrap();
    }

    /// The paths a listing of the whole folder reports, sorted.
    async fn listed_paths(root: &Path) -> Vec<String> {
        let (entries, _) = list_local(&access(root, ""), Vec::new(), 10, true, true)
            .await
            .unwrap();
        let mut paths: Vec<String> = entries.into_iter().map(|entry| entry.path).collect();
        paths.sort();
        paths
    }

    /// The bytes of every file a listing of the whole folder reports, sorted.
    async fn listed_bytes(root: &Path) -> Vec<Vec<u8>> {
        let mut versions = Vec::new();
        for path in listed_paths(root).await {
            versions.push(tokio::fs::read(root.join(path)).await.unwrap());
        }
        versions.sort();
        versions
    }

    /// A target holding `first save` with the incoming download swapped in.
    async fn swapped_folder(path: &str) -> (PathBuf, PathBuf) {
        let target = Path::new(path).join("note.txt");
        tokio::fs::write(&target, b"first save").await.unwrap();
        let spool = reserved_file(path, SPOOL_DIR).await.unwrap();
        tokio::fs::write(&spool, b"incoming").await.unwrap();
        let kept = reserved_file(path, KEPT_DIR).await.unwrap();
        assert!(swap_in(&spool, &kept, &target).await.is_ok());
        (target, kept)
    }

    /// The end of a rollback: the first save at the target, `beside` as its one
    /// visible copy, and nothing left hidden in the kept directory.
    #[cfg(not(windows))]
    async fn assert_rolled_back(root: &Path, beside: &[u8]) {
        let target = root.join("note.txt");
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"first save");
        let paths = listed_paths(root).await;
        assert_eq!(paths.len(), 2, "{paths:?}");
        assert!(paths[0].starts_with("note (conflicted copy "));
        assert_eq!(paths[1], "note.txt");
        assert_eq!(tokio::fs::read(root.join(&paths[0])).await.unwrap(), beside);
        assert_eq!(std::fs::read_dir(root.join(KEPT_DIR)).unwrap().count(), 0);
    }

    // A save the owner makes before the rollback is what the swap back displaces.
    // The target is never vacant, and a restart pass finds both versions visible.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn keeps_second_save() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let (target, kept) = swapped_folder(&path).await;
        editor_save(&target, b"second save").await;

        let refusal = swap_back(&kept, &target).await;
        assert!(matches!(
            refusal,
            PlaceError::Refused(LocalFileRefusal::Drifted)
        ));
        assert_rolled_back(root.path(), b"second save").await;
        crate::fs_source::check_local(&access(root.path(), ""))
            .await
            .unwrap();
        assert_rolled_back(root.path(), b"second save").await;
    }

    // An editor that rewrites the live download in place and closes it while the
    // rollback runs keeps its bytes: the rollback never deletes a file.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn keeps_rewritten_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let (target, kept) = swapped_folder(&path).await;
        let mut editor = tokio::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&target)
            .await
            .unwrap();
        editor.write_all(b"in place ").await.unwrap();

        let refusal = swap_back(&kept, &target).await;
        editor.write_all(b"save").await.unwrap();
        editor.sync_all().await.unwrap();
        drop(editor);

        assert!(matches!(
            refusal,
            PlaceError::Refused(LocalFileRefusal::Drifted)
        ));
        assert_eq!(
            listed_bytes(root.path()).await,
            vec![b"first save".to_vec(), b"in place save".to_vec()]
        );
        assert_eq!(
            std::fs::read_dir(root.path().join(KEPT_DIR))
                .unwrap()
                .count(),
            0
        );
    }

    // A new file holding exactly the incoming bytes is still the owner's save:
    // no hash decides about deleting it, so it stays beside the first save.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn keeps_identical_save() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let (target, kept) = swapped_folder(&path).await;
        editor_save(&target, b"incoming").await;

        let refusal = swap_back(&kept, &target).await;
        assert!(matches!(
            refusal,
            PlaceError::Refused(LocalFileRefusal::Drifted)
        ));
        assert_rolled_back(root.path(), b"incoming").await;
    }

    // A populated directory a local tool put at the target keeps its place and its
    // children; the owner's file waits beside it as a visible copy.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn keeps_replaced_directory() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let (target, kept) = swapped_folder(&path).await;
        tokio::fs::rename(&target, root.path().join("download.txt"))
            .await
            .unwrap();
        tokio::fs::create_dir(&target).await.unwrap();
        tokio::fs::write(target.join("child.txt"), b"child")
            .await
            .unwrap();

        let refusal = swap_back(&kept, &target).await;
        assert!(matches!(
            refusal,
            PlaceError::Refused(LocalFileRefusal::Drifted)
        ));
        assert_eq!(
            tokio::fs::read(target.join("child.txt")).await.unwrap(),
            b"child"
        );
        assert_eq!(
            listed_bytes(root.path()).await,
            vec![
                b"child".to_vec(),
                b"first save".to_vec(),
                b"incoming".to_vec()
            ]
        );
        assert_eq!(
            std::fs::read_dir(root.path().join(KEPT_DIR))
                .unwrap()
                .count(),
            0
        );
    }

    // The Windows rollback: the live file keeps the target name, and the owner's
    // file takes the first free copy name, so a copy already there stays.
    #[tokio::test]
    async fn keeps_live_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let (target, kept) = swapped_folder(&path).await;
        editor_save(&target, b"second save").await;
        let Some(taken) = copy_paths(&target).ok().and_then(|mut paths| paths.next()) else {
            panic!("no copy name");
        };
        tokio::fs::write(&taken, b"other copy").await.unwrap();

        let refusal = keep_displaced(&kept, &target).await;
        assert!(matches!(
            refusal,
            PlaceError::Refused(LocalFileRefusal::Drifted)
        ));
        crate::fs_source::check_local(&access(root.path(), ""))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"second save");
        assert_eq!(tokio::fs::read(&taken).await.unwrap(), b"other copy");
        assert_eq!(
            listed_bytes(root.path()).await,
            vec![
                b"first save".to_vec(),
                b"other copy".to_vec(),
                b"second save".to_vec()
            ]
        );
        assert_eq!(
            std::fs::read_dir(root.path().join(KEPT_DIR))
                .unwrap()
                .count(),
            0
        );
    }

    // A partial replace (Windows error 1177) left the owner's file at the backup
    // name and the target vacant: the file goes back to the target, not beside it.
    #[tokio::test]
    async fn restores_partial_replace() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let target = root.path().join("note.txt");
        let spool = reserved_file(&path, SPOOL_DIR).await.unwrap();
        tokio::fs::write(&spool, b"incoming").await.unwrap();
        let kept = reserved_file(&path, KEPT_DIR).await.unwrap();
        tokio::fs::write(&kept, b"owner edit").await.unwrap();

        let refusal = undo_replace(&kept, &target, "partial".to_string()).await;
        assert!(matches!(refusal, PlaceError::Failed(_)));
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"owner edit");
        assert!(!kept.exists());
        assert_eq!(
            listed_paths(root.path()).await,
            vec!["note.txt".to_string()]
        );
    }

    // A replace that failed before moving anything leaves the owner's file alone.
    #[tokio::test]
    async fn keeps_unmoved_target() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let target = root.path().join("note.txt");
        tokio::fs::write(&target, b"owner edit").await.unwrap();
        let kept = reserved_file(&path, KEPT_DIR).await.unwrap();

        let refusal = undo_replace(&kept, &target, "refused".to_string()).await;
        assert!(matches!(refusal, PlaceError::Failed(_)));
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"owner edit");
        assert_eq!(
            listed_paths(root.path()).await,
            vec!["note.txt".to_string()]
        );
    }

    // The file an exchange displaces sits where no folder check sweeps, so a
    // crash before the guard decided still leaves the owner's bytes in place.
    #[tokio::test]
    async fn sweep_keeps_displaced() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let target = root.path().join("note.txt");
        tokio::fs::write(&target, b"owner edit").await.unwrap();
        let spool = reserved_file(&path, SPOOL_DIR).await.unwrap();
        tokio::fs::write(&spool, b"incoming").await.unwrap();
        let kept = reserved_file(&path, KEPT_DIR).await.unwrap();
        assert!(swap_in(&spool, &kept, &target).await.is_ok());
        let stale = std::time::SystemTime::now() - SPOOL_GRACE - std::time::Duration::from_secs(60);
        let file = std::fs::File::options().write(true).open(&kept).unwrap();
        file.set_times(std::fs::FileTimes::new().set_modified(stale))
            .unwrap();
        drop(file);

        crate::fs_source::check_local(&access(root.path(), ""))
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&kept).await.unwrap(), b"owner edit");
        assert_eq!(tokio::fs::read(&target).await.unwrap(), b"incoming");
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

        let LocalFileEvent::Moved { to } =
            move_aside(&path, "gone.txt", &WriteGuard::MustNotExist).await
        else {
            panic!("the file must move into the trash");
        };
        assert_eq!(to, format!("{SYNC_TRASH_DIR}/gone.txt"));
        assert!(!root.path().join("gone.txt").exists());
        assert_eq!(
            tokio::fs::read(root.path().join(&to)).await.unwrap(),
            b"kept"
        );
        assert_eq!(
            move_aside(&path, "gone.txt", &WriteGuard::MustNotExist).await,
            LocalFileEvent::Refused {
                reason: LocalFileRefusal::Missing
            }
        );
    }

    #[cfg(unix)]
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
        // A link deeper in the path must not be extended either.
        std::fs::create_dir(root.path().join("nested")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("nested/away")).unwrap();
        assert_eq!(
            write_guarded(
                &path,
                "nested/away/deep/out.txt",
                &WriteGuard::MustNotExist,
                body(b"x")
            )
            .await,
            escaped
        );
        assert!(!outside.path().join("out.txt").exists());
        assert!(!outside.path().join("deep").exists());
    }

    /// Writes one spool file and dates it past the sweep's grace.
    async fn age_spool(path: &std::path::Path) {
        tokio::fs::write(path, b"spool").await.unwrap();
        let file = std::fs::File::options().write(true).open(path).unwrap();
        let stale = std::time::SystemTime::now() - SPOOL_GRACE - std::time::Duration::from_secs(60);
        file.set_times(std::fs::FileTimes::new().set_modified(stale))
            .unwrap();
    }

    // A write in flight spools into the same directory every sweep reads, so a
    // fresh file is left alone and only a leftover is removed.
    #[tokio::test]
    async fn spares_fresh_spool() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let spool = root.path().join(SPOOL_DIR);
        tokio::fs::create_dir_all(&spool).await.unwrap();
        tokio::fs::write(spool.join("in-flight"), b"streaming")
            .await
            .unwrap();
        age_spool(&spool.join("leftover")).await;

        assert_eq!(sweep_spool(&path).await, 1);
        assert!(spool.join("in-flight").exists());
        assert!(!spool.join("leftover").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_linked_spool() {
        // Reserved bookkeeping must never follow an owner-created symlink.
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::create_dir(root.path().join(".aruna"))
            .await
            .unwrap();
        age_spool(&outside.path().join("stale")).await;
        std::os::unix::fs::symlink(outside.path(), root.path().join(SPOOL_DIR)).unwrap();

        assert_eq!(sweep_spool(&path).await, 0);
        assert!(outside.path().join("stale").exists());
        assert!(matches!(
            write_guarded(&path, "note.txt", &WriteGuard::MustNotExist, body(b"new")).await,
            LocalFileEvent::Error { .. }
        ));
        assert!(!outside.path().join("note.txt").exists());
    }

    // Trash and write spool are this node's bookkeeping in the reserved
    // directory; a sweep must never remove the owner's other files.
    #[tokio::test]
    async fn hides_reserved_entries() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::write(root.path().join("keep.txt"), b"keep")
            .await
            .unwrap();
        move_aside(&path, "keep.txt", &WriteGuard::MustNotExist).await;
        tokio::fs::write(root.path().join("keep.txt"), b"again")
            .await
            .unwrap();

        for name in [".aruna-notes", ".aruna-tmp-notes"] {
            tokio::fs::write(root.path().join(name), b"mine")
                .await
                .unwrap();
        }
        tokio::fs::create_dir_all(root.path().join(SPOOL_DIR))
            .await
            .unwrap();
        age_spool(&root.path().join(SPOOL_DIR).join("stale")).await;

        let (entries, _) = list_local(&access(root.path(), ""), Vec::new(), 10, true, true)
            .await
            .unwrap();
        let mut paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        paths.sort();
        assert_eq!(paths, vec![".aruna-notes", ".aruna-tmp-notes", "keep.txt"]);
        assert_eq!(sweep_spool(&path).await, 1);
        assert!(root.path().join(".aruna-notes").exists());
        assert!(root.path().join(".aruna-tmp-notes").exists());
    }

    // A sweep runs on every folder check, so a file the owner named like an
    // older spool must survive it and stay listed as their own data.
    #[tokio::test]
    async fn keeps_owner_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        tokio::fs::write(root.path().join(".aruna-tmp-notes"), b"mine")
            .await
            .unwrap();

        assert_eq!(sweep_spool(&path).await, 0);
        assert_eq!(
            tokio::fs::read(root.path().join(".aruna-tmp-notes"))
                .await
                .unwrap(),
            b"mine"
        );
    }

    // Every spooled byte lands in the reserved directory, so a write never
    // creates a name inside the owner's own tree.
    #[tokio::test]
    async fn spools_in_reserved() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();
        let event = write_guarded(
            &path,
            "nested/note.txt",
            &WriteGuard::MustNotExist,
            body(b"hello"),
        )
        .await;

        assert!(matches!(event, LocalFileEvent::Written { .. }));
        assert!(root.path().join(SPOOL_DIR).is_dir());
        let leftovers = std::fs::read_dir(root.path().join(SPOOL_DIR))
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert_eq!(leftovers, 0);
        let nested = std::fs::read_dir(root.path().join("nested"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(nested, vec!["note.txt".to_string()]);
    }

    #[tokio::test]
    async fn hashes_stable_source() {
        // The identity the serve path checks a file against comes from here, so
        // the fingerprint and the hash must describe the same read.
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"hello").await.unwrap();
        let (fingerprint, blake3, size) = hash_stable(&file).await.expect("file hashes");
        assert_eq!(size, 5);
        assert_eq!(blake3, *::blake3::hash(b"hello").as_bytes());
        assert_eq!(
            crate::fs_source::current_fingerprint(&tokio::fs::File::open(&file).await.unwrap())
                .await
                .as_deref(),
            Some(fingerprint.as_str())
        );
    }

    // The reported hash is the spool's, so it may only be paired with a file
    // that still holds exactly those bytes, even one of the same length.
    #[tokio::test]
    async fn refuses_replaced_publish() {
        let root = tempfile::tempdir().unwrap();
        let spooled = Spooled {
            path: root.path().join("spool"),
            blake3: *::blake3::hash(b"hello").as_bytes(),
            size: 5,
        };
        let published = root.path().join("note.txt");
        tokio::fs::write(&published, b"olleh").await.unwrap();

        let event = finish_write(spooled, Ok(()), &published, |fingerprint, spooled| {
            LocalFileEvent::Written {
                fingerprint,
                blake3: spooled.blake3,
                size: spooled.size,
            }
        })
        .await;

        assert!(matches!(event, LocalFileEvent::Error { .. }));
    }

    // A file that changed since it was observed must not be resolvable under
    // the identity it had then.
    #[tokio::test]
    async fn detects_changed_source() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("note.txt");
        tokio::fs::write(&file, b"before").await.unwrap();
        let (_, before) = crate::fs_source::stable_source(&access(root.path(), "note.txt"))
            .await
            .expect("source resolves");
        tokio::fs::write(&file, b"after-and-longer").await.unwrap();
        let (_, after) = crate::fs_source::stable_source(&access(root.path(), "note.txt"))
            .await
            .expect("source resolves");
        assert_ne!(before, after);
    }

    // A link out of the offered directory must not resolve for a serve either.
    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_escaping_source() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        tokio::fs::write(outside.path().join("secret"), b"secret")
            .await
            .unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), root.path().join("link"))
            .unwrap();
        assert!(
            crate::fs_source::stable_source(&access(root.path(), "link"))
                .await
                .is_err()
        );
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
}
