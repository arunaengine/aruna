//! Guarded writes into a folder the owner syncs on their own machine.
//!
//! Every operation here is non-destructive by construction: a write lands only
//! through a guard the adapter re-verifies at rename time, a conflicted copy
//! never replaces anything, and a removal moves the file aside instead of
//! unlinking it. The guard repeats the operation's decision on purpose, because
//! the file may change between the decision and the write.

use aruna_core::errors::StagingSourceError;
use aruna_core::events::{LocalFileEvent, LocalFileRefusal};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{FileStat, SYNC_TRASH_DIR, WriteGuard, weak_fingerprint};
use bytes::Bytes;
use futures::StreamExt;
use std::path::{Component, Path, PathBuf};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::fs_source::{
    MAX_COPY_ATTEMPTS, SPOOL_DIR, SPOOL_GRACE, canonical_root, current_fingerprint, jailed_file,
    map_io_error,
};

/// Why a spooled file could not be published. A refusal is the owner's data
/// winning; a failure is this node's problem.
enum PlaceError {
    Refused(LocalFileRefusal),
    Failed(String),
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
        WriteGuard::MatchesBase { blake3, .. } => {
            exchange_guarded(&spooled.path, &target, blake3).await
        }
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
///
/// The fingerprint is read at the published path: the rename that puts it there
/// moves the change time, so one taken on the spool would never match again.
async fn finish_write(
    spooled: Spooled,
    placed: Result<(), PlaceError>,
    published: &Path,
    written: impl FnOnce(String, &Spooled) -> LocalFileEvent,
) -> LocalFileEvent {
    match placed {
        Ok(()) => match current_fingerprint(published).await {
            Some(fingerprint) => written(fingerprint, &spooled),
            None => LocalFileEvent::Error {
                message: "the published file could not be read back".to_string(),
            },
        },
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
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new(""),
    };
    // Directories are created only once the deepest part that already exists is
    // proved to be inside the root, so a link on the way out is never followed
    // and never extended.
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
    let Ok(mut reader) = tokio::fs::read_dir(root.join(SPOOL_DIR)).await else {
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
    let parent = canonical_root(root)
        .await
        .map_err(|error| error.to_string())?
        .join(SPOOL_DIR);
    tokio::fs::create_dir_all(&parent)
        .await
        .map_err(|error| error.to_string())?;
    let path = parent.join(Ulid::generate().to_string());
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
    Ok(Spooled {
        path: path.to_path_buf(),
        blake3: *hasher.finalize().as_bytes(),
        size,
    })
}

/// The second half of the replace rule, without a window between the check and
/// the write: the spool and the target swap places atomically, and the bytes
/// that were displaced are identified afterwards. Only bytes that match the
/// guard are discarded; anything else is put back, and if even that fails the
/// owner's bytes are moved aside rather than lost.
async fn exchange_guarded(
    spool: &Path,
    target: &Path,
    blake3: &[u8; 32],
) -> Result<(), PlaceError> {
    let Ok(link) = tokio::fs::symlink_metadata(target).await else {
        return Err(PlaceError::Refused(LocalFileRefusal::Missing));
    };
    if !link.is_file() {
        return Err(PlaceError::Refused(LocalFileRefusal::NotRegular));
    }
    exchange(spool, target)?;
    // The displaced file now sits at the spool path. Only its strong hash can
    // answer for it here: the exchange moved its name, and with it the change
    // time its weak fingerprint carries.
    let displaced = match hash_stable(spool).await {
        Ok((_, hash, _)) => &hash == blake3,
        Err(_) => false,
    };
    if displaced {
        let _ = tokio::fs::remove_file(spool).await;
        return Ok(());
    }
    match exchange(spool, target) {
        Ok(()) => Err(PlaceError::Refused(LocalFileRefusal::Drifted)),
        // The swap back failed, so the owner's bytes stay under a name of their
        // own instead of being left where the incoming version belongs.
        Err(_) => match rescue_displaced(spool, target).await {
            Ok(()) => Err(PlaceError::Refused(LocalFileRefusal::Drifted)),
            Err(error) => Err(error),
        },
    }
}

/// Swaps two existing paths in one step. Neither file is ever destroyed.
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

/// Puts bytes that could not be swapped back beside their own path, so a failed
/// exchange still leaves them on the owner's disk under a findable name.
async fn rescue_displaced(spool: &Path, target: &Path) -> Result<(), PlaceError> {
    let Some(parent) = target.parent() else {
        return Err(PlaceError::Failed(
            "the target has no directory".to_string(),
        ));
    };
    let relative = target.to_string_lossy().to_string();
    let stamp = conflict_stamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_millis() as u64)
            .unwrap_or_default(),
    );
    for attempt in 1..=MAX_COPY_ATTEMPTS {
        let candidate = parent.join(copy_name(&relative, &stamp, attempt));
        if place_new(spool, &candidate).is_ok() {
            return Ok(());
        }
    }
    Err(PlaceError::Failed(
        "the displaced file could not be put aside".to_string(),
    ))
}

/// Hashes one file and refuses the result when the file changed while it was
/// read: those bytes are not one representation of anything.
async fn hash_stable(path: &Path) -> Result<(String, [u8; 32], u64), StagingSourceError> {
    let before = tokio::fs::metadata(path).await.map_err(map_io_error)?;
    let fingerprint = weak_fingerprint(&FileStat::from_metadata(&before));
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
    if weak_fingerprint(&FileStat::from_metadata(&after)) != fingerprint {
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
    use aruna_core::structs::{OFFERED_DIRECTORY_ROOT, ResolvedSourceAccess, SourceConnectorKind};
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

    // The fingerprint a write reports must be the one the next observation
    // reads, or every synced file would look changed the moment it landed and
    // be uploaded and conflicted again on the next pass.
    #[tokio::test]
    async fn write_matches_listing() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_string_lossy().to_string();

        let LocalFileEvent::Written { fingerprint, .. } =
            write_guarded(&path, "note.txt", &WriteGuard::MustNotExist, body(b"hello")).await
        else {
            panic!("the file must be written");
        };

        let (entries, _) = list_local(&access(root.path(), ""), 0, 10, true, true)
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
        let (entries, _) = list_local(&access(root.path(), ""), 0, 10, true, true)
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
        let spooled = std::fs::read_dir(root.path().join(SPOOL_DIR))
            .map(|entries| entries.filter_map(Result::ok).count())
            .unwrap_or_default();
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

    // The trash and the write spool are this node's bookkeeping and live in the
    // reserved directory. Everything else in the folder is the owner's, whatever
    // it is named, and a sweep must never remove it.
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

        let (entries, _) = list_local(&access(root.path(), ""), 0, 10, true, true)
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
            crate::fs_source::current_fingerprint(&file)
                .await
                .as_deref(),
            Some(fingerprint.as_str())
        );
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
