//! Reads a directory the owner offers: lists entries, stats files and streams file contents.
//! Reads stay beneath an open root handle even when paths or symlinks are replaced.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::walk::{Child, Step, Walk, join_path};
use aruna_core::errors::StagingSourceError;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::offered_directory::{
    FileStat, OFFERED_DIRECTORY_ROOT, weak_fingerprint,
};
use aruna_core::structs::execution::source_access::{
    ResolvedSourceAccess, SourceEntry, SourceEntryKind, SourceMetadata,
};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

/// The one directory this node maintains inside a folder. Nothing under it is
/// ever offered as the owner's data.
pub(crate) const RESERVED_DIR: &str = ".aruna";

/// Directory a guarded write spools into, relative to the folder root. It is
/// inside the reserved directory, so a sweep only ever removes this node's own
/// files and never one the owner happens to have named like a spool.
pub(crate) const SPOOL_DIR: &str = ".aruna/tmp";

/// Directory a guarded write moves the owner's displaced file into. No sweep
/// reads it, so those bytes are removed only once they are known to be disposable.
pub(crate) const KEPT_DIR: &str = ".aruna/kept";

/// How long a spool file is left alone. A sweep runs on every folder check and
/// a write may be streaming into the directory at that moment, so only a file
/// old enough that no write can still be holding it is a leftover.
pub(crate) const SPOOL_GRACE: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Candidate names one conflicted copy or move-aside may try before it gives
/// up. A folder with this many same-named copies needs the owner, not a retry.
pub(crate) const MAX_COPY_ATTEMPTS: usize = 100;

pub(crate) fn is_local_access(access: &ResolvedSourceAccess) -> bool {
    let ResolvedSourceAccess::OpenDal { kind, .. } = access;
    *kind == SourceConnectorKind::LocalDirectory
}

pub(crate) async fn check_local(access: &ResolvedSourceAccess) -> Result<(), StagingSourceError> {
    let (root, _) = access_parts(access)?;
    // A root that does not resolve is as unusable as one that is not a directory.
    let resolved = tokio::fs::canonicalize(&root)
        .await
        .map_err(|error| StagingSourceError::CheckError(error.to_string()))?;
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

/// Opens one jailed file and fingerprints that handle before any bytes are read.
pub(crate) async fn stable_source(
    access: &ResolvedSourceAccess,
) -> Result<(tokio::fs::File, String), StagingSourceError> {
    let (root, path) = access_parts(access)?;
    let file = open_file(&root, &path).await?;
    let metadata = file.metadata().await.map_err(map_io_error)?;
    let fingerprint = weak_fingerprint(&FileStat::from_metadata(&metadata));
    Ok((file, fingerprint))
}

/// The weak fingerprint of the opened file, without resolving its path again.
pub(crate) async fn current_fingerprint(file: &tokio::fs::File) -> Option<String> {
    let metadata = file.metadata().await.ok()?;
    Some(weak_fingerprint(&FileStat::from_metadata(&metadata)))
}

pub(crate) async fn head_local(
    access: &ResolvedSourceAccess,
) -> Result<SourceMetadata, StagingSourceError> {
    let (root, path) = access_parts(access)?;
    tokio::task::spawn_blocking(move || {
        let root = OfferedRoot::open(&root)?;
        let metadata = root
            .directory
            .metadata(root.resolve(&path)?)
            .map_err(map_io_error)?;
        if !metadata.is_file() {
            return Err(StagingSourceError::NotFound);
        }
        Ok(SourceMetadata {
            content_length: metadata.len(),
            content_type: None,
            etag: Some(weak_fingerprint(&entry_stat(&metadata))),
            last_modified: metadata.modified().ok().map(|time| time.into_std()),
            source_version: None,
            digests: Default::default(),
        })
    })
    .await
    .map_err(|error| StagingSourceError::ReadError(error.to_string()))?
}

/// One page of a local listing in walk order, after the cursor names in `after`.
pub(crate) async fn list_local(
    access: &ResolvedSourceAccess,
    after: Vec<String>,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<SourceEntry>, bool), StagingSourceError> {
    let (root, path) = access_parts(access)?;
    tokio::task::spawn_blocking(move || {
        let walk = Walk::new(&path, after, recursive);
        walk_entries(
            &root,
            walk,
            (limit, recursive, files_only),
            MAX_LOCAL_EXAMINED,
        )
    })
    .await
    .map_err(|error| StagingSourceError::ListError(error.to_string()))?
}

/// Entries one local listing page may examine, skipped ones included.
const MAX_LOCAL_EXAMINED: usize = 250_000;

/// A listed local entry: its name at first, and its metadata once the walk visits it.
type LocalChild = Child<Option<cap_std::fs::Metadata>>;

/// One page of a local walk; each directory is scanned once per page, and examining more than
/// `examined` entries fails the page instead of walking on.
fn walk_entries(
    root: &str,
    mut walk: Walk<Option<cap_std::fs::Metadata>>,
    (limit, recursive, files_only): (usize, bool, bool),
    examined: usize,
) -> Result<(Vec<SourceEntry>, bool), StagingSourceError> {
    let root = OfferedRoot::open(root)?;
    // Where each listed directory resolves. A page lists every ancestor of a directory before
    // it, so a link back to an ancestor is found on any page, while other aliases list as paths.
    let mut listed = HashMap::new();
    let mut seen = 0usize;
    let mut entries = Vec::new();
    loop {
        let (path, child) = match walk.step_with(|path, child| visit_child(&root, path, child)) {
            Step::List(directory) => {
                let resolved = root.resolve(&directory)?;
                let looped = ancestors(&directory).any(|up| listed.get(up) == Some(&resolved));
                let from = walk.resume_name().map(ToOwned::to_owned);
                let scan = (from.as_deref(), &mut seen, examined);
                // A recursive walk keeps each directory whole while it descends; a flat page
                // keeps only the entries it lists, one more to tell whether more follow.
                let children = match (looped, recursive) {
                    (true, _) => Vec::new(),
                    (false, true) => local_children(&root, &resolved, scan)?,
                    (false, false) => {
                        let room = (limit + 1).saturating_sub(entries.len());
                        flat_window(&root, (&directory, &resolved), scan, (room, files_only))?
                    }
                };
                listed.insert(directory, resolved);
                walk.listed(children);
                continue;
            }
            Step::Entry { path, child } => (path, child),
            Step::Done => return Ok((entries, false)),
        };
        if child.is_dir && files_only {
            continue;
        }
        if entries.len() == limit {
            return Ok((entries, true));
        }
        let kind = match child.is_dir {
            true => SourceEntryKind::Directory,
            false => SourceEntryKind::File,
        };
        let Some(metadata) = child.value else {
            continue;
        };
        entries.push(SourceEntry {
            name: child.name,
            path,
            kind,
            size: (kind == SourceEntryKind::File).then_some(metadata.len()),
            modified: metadata.modified().ok().map(|time| time.into_std()),
            etag: None,
            // The listing carries the same stat the serve path reads, so an
            // observation and a later read agree on one identity.
            stat: Some(entry_stat(&metadata)),
        });
    }
}

/// Reads the metadata of a child the walk is about to visit, unless its scan read it already.
fn visit_child(root: &OfferedRoot, path: &str, child: &mut LocalChild) -> bool {
    if child.value.is_some() {
        return true;
    }
    let Some(metadata) = entry_metadata(root, path) else {
        return false;
    };
    child.is_dir = metadata.is_dir();
    child.value = Some(metadata);
    true
}

/// The metadata of a file or directory at `path`. A link out of the offered folder, a vanished
/// entry and one that is neither file nor directory have none; they are skipped, not errors.
fn entry_metadata(root: &OfferedRoot, path: &str) -> Option<cap_std::fs::Metadata> {
    let metadata = root.directory.metadata(root.resolve(path).ok()?).ok()?;
    (metadata.is_dir() || metadata.is_file()).then_some(metadata)
}

/// The paths that contain `path`, nearest first, down to the root of the offered folder.
fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    let parents = path.rmatch_indices('/').map(|(at, _)| &path[..at]);
    parents.chain((!path.is_empty()).then_some(""))
}

/// Feeds each name of the local directory `resolved` to `take`, except the node's reserved
/// folder; the page fails once it examined more than `examined` entries.
fn scan_names(
    root: &OfferedRoot,
    resolved: &Path,
    (seen, examined): (&mut usize, usize),
    mut take: impl FnMut(String),
) -> Result<(), StagingSourceError> {
    let reader = root
        .directory
        .read_dir(resolved)
        .map_err(|error| StagingSourceError::ListError(error.to_string()))?;
    for entry in reader {
        let entry = entry.map_err(|error| StagingSourceError::ListError(error.to_string()))?;
        *seen += 1;
        if *seen > examined {
            return Err(StagingSourceError::InvalidListing(format!(
                "a local listing page examines at most {examined} entries; list a narrower path"
            )));
        }
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        // This node's bookkeeping lives only in the reserved directory,
        // so a file the owner named `.aruna-notes` stays theirs.
        if name != RESERVED_DIR {
            take(name);
        }
    }
    Ok(())
}

/// The names in the local directory `resolved` from the name `from` on, for a recursive walk
/// that keeps the ordered directory while it descends; their metadata waits for the visit.
fn local_children(
    root: &OfferedRoot,
    resolved: &Path,
    (from, seen, examined): (Option<&str>, &mut usize, usize),
) -> Result<Vec<LocalChild>, StagingSourceError> {
    let mut children = Vec::new();
    scan_names(root, resolved, (seen, examined), |name| {
        // An earlier page listed every name before the cursor's.
        if from.is_none_or(|from| name.as_str() >= from) {
            children.push(Child {
                name,
                is_dir: false,
                value: None,
            });
        }
    })?;
    Ok(children)
}

/// The first `room` entries of a flat page after the name `after`, chosen in the one scan from
/// entries the page lists: a file, or a directory unless the page lists files only. Metadata is
/// read only for a name that would enter the window.
fn flat_window(
    root: &OfferedRoot,
    (directory, resolved): (&str, &Path),
    (after, seen, examined): (Option<&str>, &mut usize, usize),
    (room, files_only): (usize, bool),
) -> Result<Vec<LocalChild>, StagingSourceError> {
    let mut window: std::collections::BTreeMap<String, cap_std::fs::Metadata> = Default::default();
    scan_names(root, resolved, (seen, examined), |name| {
        let passed = after.is_some_and(|after| name.as_str() <= after);
        let beyond = window.len() >= room
            && (window.last_key_value()).is_some_and(|(last, _)| name >= *last);
        if passed || beyond {
            return;
        }
        let metadata = entry_metadata(root, &join_path(directory, &name));
        let Some(metadata) = metadata.filter(|metadata| !(files_only && metadata.is_dir())) else {
            return;
        };
        window.insert(name, metadata);
        if window.len() > room {
            window.pop_last();
        }
    })?;
    let children = (window.into_iter())
        .map(|(name, metadata)| Child {
            name,
            is_dir: metadata.is_dir(),
            value: Some(metadata),
        })
        .collect();
    Ok(children)
}

pub(crate) async fn read_local(
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<(SourceMetadata, BackendStream<Result<Bytes, StreamError>>), StagingSourceError> {
    let (root, path) = access_parts(access)?;
    let mut file = open_file(&root, &path).await?;
    let before = file.metadata().await.map_err(map_io_error)?;
    let metadata = file_metadata(&before);
    let fingerprint = weak_fingerprint(&FileStat::from_metadata(&before));
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
        None => {
            let observed = file.try_clone().await.map_err(map_io_error)?;
            BackendStream::new(ReaderStream::new(file))
                .on_success_async(move || verify_stable(observed, fingerprint))
        }
    };

    Ok((metadata, stream))
}

/// Refuses the bytes that were just streamed when the file no longer carries
/// the fingerprint it was opened with.
async fn verify_stable(file: tokio::fs::File, fingerprint: String) -> Result<(), StreamError> {
    let after = file
        .metadata()
        .await
        .map_err(|error| StreamError(Box::new(error)))?;
    if weak_fingerprint(&FileStat::from_metadata(&after)) == fingerprint {
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
    validate_relative(relative)?;
    let resolved = tokio::fs::canonicalize(root.join(relative))
        .await
        .map_err(map_io_error)?;
    if !resolved.starts_with(root) {
        return Err(StagingSourceError::AccessDenied);
    }
    Ok(resolved)
}

fn validate_relative(relative: &str) -> Result<(), StagingSourceError> {
    let candidate = Path::new(relative);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(StagingSourceError::NotFound);
    }
    Ok(())
}

struct OfferedRoot {
    directory: cap_std::fs::Dir,
    path: PathBuf,
}

impl OfferedRoot {
    fn open(root: &str) -> Result<Self, StagingSourceError> {
        let directory = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())
            .map_err(map_io_error)?;
        let path = std::fs::canonicalize(root).map_err(map_io_error)?;
        Ok(Self { directory, path })
    }

    fn resolve(&self, relative: &str) -> Result<PathBuf, StagingSourceError> {
        validate_relative(relative)?;
        // Preserve absolute in-root links; only the directory handle grants access.
        let resolved = std::fs::canonicalize(self.path.join(relative)).map_err(map_io_error)?;
        let path = resolved
            .strip_prefix(&self.path)
            .map_err(|_| StagingSourceError::AccessDenied)?;
        Ok(if path.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            path.to_path_buf()
        })
    }

    fn open_file(&self, relative: &Path) -> Result<std::fs::File, StagingSourceError> {
        if !self
            .directory
            .metadata(relative)
            .map_err(map_io_error)?
            .is_file()
        {
            return Err(StagingSourceError::NotFound);
        }
        let mut options = cap_std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt;
            options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32);
        }
        let file = self
            .directory
            .open_with(relative, &options)
            .map_err(map_io_error)?
            .into_std();
        if !file.metadata().map_err(map_io_error)?.is_file() {
            return Err(StagingSourceError::NotFound);
        }
        Ok(file)
    }
}

pub(crate) async fn open_file(
    root: &str,
    relative: &str,
) -> Result<tokio::fs::File, StagingSourceError> {
    let root = root.to_string();
    let relative = relative.to_string();
    let file = tokio::task::spawn_blocking(move || {
        let root = OfferedRoot::open(&root)?;
        root.open_file(&root.resolve(&relative)?)
    })
    .await
    .map_err(|error| StagingSourceError::ReadError(error.to_string()))??;
    Ok(tokio::fs::File::from_std(file))
}

fn entry_stat(metadata: &cap_std::fs::Metadata) -> FileStat {
    let stat = FileStat::partial(
        metadata.len(),
        metadata.modified().ok().map(|time| time.into_std()),
    );
    #[cfg(unix)]
    let stat = {
        use cap_std::fs::MetadataExt;
        let mut stat = stat;
        stat.inode = Some(metadata.ino());
        stat.changed_ns = u128::try_from(metadata.ctime())
            .ok()
            .zip(u128::try_from(metadata.ctime_nsec()).ok())
            .map(|(seconds, nanos)| seconds.saturating_mul(1_000_000_000).saturating_add(nanos));
        stat
    };
    stat
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

fn file_metadata(metadata: &std::fs::Metadata) -> SourceMetadata {
    let modified: Option<SystemTime> = metadata.modified().ok();
    SourceMetadata {
        content_length: metadata.len(),
        content_type: None,
        etag: Some(weak_fingerprint(&FileStat::from_metadata(metadata))),
        last_modified: modified,
        source_version: None,
        digests: Default::default(),
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
pub(crate) mod tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::HashMap;

    // The bound stops the scan of a wide directory itself, and names a page already passed get
    // no metadata.
    #[test]
    fn scan_stops_early() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a", "b", "c", "d", "e"] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        let root = OfferedRoot::open(dir.path().to_str().unwrap()).unwrap();
        let resolved = root.resolve("").unwrap();
        let mut seen = 0;
        let wide = local_children(&root, &resolved, (None, &mut seen, 3));
        assert!(matches!(wide, Err(StagingSourceError::InvalidListing(_))));
        assert_eq!(
            seen, 4,
            "the scan stopped at the first entry over the bound"
        );
        let mut seen = 0;
        let rest = local_children(&root, &resolved, (Some("d"), &mut seen, 10)).unwrap();
        let mut names: Vec<String> = rest.into_iter().map(|child| child.name).collect();
        names.sort();
        assert_eq!(names, ["d", "e"]);
        // A flat page keeps only its window of listable entries after the cursor.
        let mut seen = 0;
        let scan = (Some("a"), &mut seen, 10);
        let first = flat_window(&root, ("", &resolved), scan, (2, true)).unwrap();
        let names: Vec<String> = first.into_iter().map(|child| child.name).collect();
        assert_eq!(names, ["b", "c"]);
    }

    // A flat files-only page whose first names are folders scans the directory once: the window
    // takes only listable entries, so no rescan charges the directory again against the bound.
    #[test]
    fn flat_scan_once() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..10 {
            std::fs::create_dir(dir.path().join(format!("d{index}"))).unwrap();
        }
        std::fs::write(dir.path().join("z.txt"), b"x").unwrap();
        let root = dir.path().to_str().unwrap();
        let walk = Walk::new("", Vec::new(), false);
        let (entries, truncated) = walk_entries(root, walk, (1, false, true), 15).unwrap();
        let paths: Vec<String> = entries.into_iter().map(|entry| entry.path).collect();
        assert_eq!(paths, ["z.txt"]);
        assert!(!truncated);
    }

    // Empty directories still cost traversal, so a page fails once its bounds are spent.
    #[test]
    fn walk_bounds_fail() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..5 {
            std::fs::create_dir(dir.path().join(format!("empty-{index}"))).unwrap();
        }
        let root = dir.path().to_str().unwrap();
        let walk = |examined| {
            let walk = Walk::new("", Vec::new(), true);
            walk_entries(root, walk, (10, true, true), examined)
        };

        assert!(walk(10).is_ok());
        assert!(matches!(
            walk(3),
            Err(StagingSourceError::InvalidListing(_))
        ));
    }

    // An alias of a directory that is not its ancestor lists under its own path on every page;
    // before, a single page hid it as already visited while a later page listed it.
    #[cfg(unix)]
    #[tokio::test]
    async fn alias_pages_agree() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("a")).unwrap();
        std::fs::write(root.path().join("a/f.txt"), b"data").unwrap();
        std::fs::write(root.path().join("m.txt"), b"data").unwrap();
        std::os::unix::fs::symlink(root.path().join("a"), root.path().join("z")).unwrap();
        let listed = list_local(&access(root.path(), ""), Vec::new(), 10, true, true).await;
        let whole: Vec<String> = (listed.unwrap().0.into_iter())
            .map(|entry| entry.path)
            .collect();
        let (mut after, mut paged) = (Vec::new(), Vec::new());
        loop {
            let page = list_local(&access(root.path(), ""), after, 1, true, true).await;
            let (entries, truncated) = page.unwrap();
            paged.extend(entries.into_iter().map(|entry| entry.path));
            if !truncated {
                break;
            }
            after = paged
                .last()
                .unwrap()
                .split('/')
                .map(ToOwned::to_owned)
                .collect();
        }
        assert_eq!(whole, ["a/f.txt", "m.txt", "z/f.txt"]);
        assert_eq!(paged, whole);
    }

    // Each page continues after the last path of the one before, in name order, so a tree is
    // listed once without walking the paths before the cursor again.
    #[tokio::test]
    async fn pages_resume_walk() {
        let root = tempfile::tempdir().unwrap();
        for path in ["b/x.txt", "b/a/y.txt", "a.txt", "c.txt"] {
            let path = root.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"data").unwrap();
        }
        let (mut after, mut paths) = (Vec::new(), Vec::new());
        loop {
            let page = list_local(&access(root.path(), ""), after, 1, true, true).await;
            let (entries, truncated) = page.unwrap();
            paths.extend(entries.iter().map(|entry| entry.path.clone()));
            if !truncated {
                break;
            }
            after = paths
                .last()
                .unwrap()
                .split('/')
                .map(ToOwned::to_owned)
                .collect();
        }
        assert_eq!(paths, ["a.txt", "b/a/y.txt", "b/x.txt", "c.txt"]);
    }

    pub(crate) fn access(root: &Path, path: &str) -> ResolvedSourceAccess {
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
    #[cfg(unix)]
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
        let (entries, _) = list_local(&access(root.path(), ""), Vec::new(), 10, true, false)
            .await
            .unwrap();
        assert!(entries.is_empty());
    }

    // A link that stays inside the offered directory is ordinary data.
    #[cfg(unix)]
    #[tokio::test]
    async fn follows_internal_link() {
        let root = tempfile::tempdir().unwrap();
        tokio::fs::write(root.path().join("real"), b"data")
            .await
            .unwrap();
        std::os::unix::fs::symlink(root.path().join("real"), root.path().join("alias")).unwrap();

        let head = head_local(&access(root.path(), "alias")).await.unwrap();
        assert_eq!(head.content_length, 4);
        let (_, stream) = read_local(&access(root.path(), "alias"), None)
            .await
            .unwrap();
        assert_eq!(collect(stream).await.unwrap(), b"data");
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

        let listed = list_local(&access(root.path(), ""), Vec::new(), 10, true, true).await;
        let (mut entries, truncated) = listed.unwrap();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        assert!(!truncated);
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["nested/inner.txt", "top.txt"]);
        assert_eq!(entries[0].size, Some(2));
        let metadata = std::fs::metadata(root.path().join("nested/inner.txt")).unwrap();
        assert_eq!(entries[0].stat, Some(FileStat::from_metadata(&metadata)));
    }

    #[cfg(unix)]
    #[test]
    fn replaced_paths_refused() {
        for replace_parent in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            std::fs::create_dir(root.path().join("nested")).unwrap();
            std::fs::write(root.path().join("nested/file"), b"inside").unwrap();
            std::fs::write(outside.path().join("file"), b"secret").unwrap();
            let offered = OfferedRoot::open(root.path().to_str().unwrap()).unwrap();
            let resolved = offered.resolve("nested/file").unwrap();
            let (replaced, target) = if replace_parent {
                (root.path().join("nested"), outside.path().to_path_buf())
            } else {
                (root.path().join("nested/file"), outside.path().join("file"))
            };
            std::fs::rename(&replaced, root.path().join("saved")).unwrap();
            std::os::unix::fs::symlink(target, replaced).unwrap();
            assert!(matches!(
                offered.open_file(&resolved),
                Err(StagingSourceError::AccessDenied)
            ));
            assert!(offered.directory.metadata(&resolved).is_err());
            if replace_parent {
                assert!(offered.directory.read_dir("nested").is_err());
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn root_handle_pinned() {
        use std::io::Read;
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("offered");
        let outside = base.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(root.join("file"), b"inside").unwrap();
        std::fs::write(outside.join("file"), b"secret").unwrap();
        let offered = OfferedRoot::open(root.to_str().unwrap()).unwrap();
        let resolved = offered.resolve("file").unwrap();
        std::fs::rename(&root, base.path().join("saved")).unwrap();
        std::os::unix::fs::symlink(&outside, &root).unwrap();
        let mut bytes = Vec::new();
        offered
            .open_file(&resolved)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"inside");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn range_handle_pinned() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        std::fs::write(&path, b"inside").unwrap();
        std::fs::write(outside.path().join("file"), b"secret").unwrap();
        let (_, stream) = read_local(&access(root.path(), "file"), Some(1..4))
            .await
            .unwrap();
        std::fs::rename(&path, root.path().join("saved")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("file"), &path).unwrap();
        assert_eq!(collect(stream).await.unwrap(), b"nsi");
        assert_eq!(
            head_local(&access(root.path(), "file")).await,
            Err(StagingSourceError::AccessDenied)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let fifo = root.path().join("pipe");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        assert!(made.unwrap().success());
        assert!(matches!(
            open_file(root.path().to_str().unwrap(), "pipe").await,
            Err(StagingSourceError::NotFound)
        ));
    }

    // A link back to an ancestor inside the offered root must not make the walk
    // circle forever.
    #[cfg(unix)]
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

        let (entries, truncated) =
            list_local(&access(root.path(), ""), Vec::new(), 100, true, true)
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
