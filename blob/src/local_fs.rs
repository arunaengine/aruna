//! A read-only OpenDAL service for a local folder that never follows a symlink below its root.
//! Listings come sorted by key, so a recursive listing can resume with `start_after`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::Bytes;
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use opendal::raw::{
    OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, RpCreateDir,
    RpPresign, RpRename, RpStat, Service, ServiceInfo, Timestamp, new_std_io_error,
    new_task_join_error, oio,
};
use opendal::{
    Buffer, Builder, Capability, Configurator, Error, ErrorKind, Metadata, MetadataBuilder,
    OperationContext, Result,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// Decides whether a path below the root may be listed, stated or read. Folder paths end with
/// `/`. Every folder on the way to an entry is asked too, so a refused folder hides its content.
pub type PathFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

const SCHEME: &str = "local-fs";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct LocalFsConfig {
    /// Folder the service serves. It may itself be a symlink; nothing below it is followed.
    pub root: Option<String>,
}

impl Configurator for LocalFsConfig {
    type Builder = LocalFsBuilder;

    fn into_builder(self) -> Self::Builder {
        LocalFsBuilder {
            config: self,
            filter: None,
        }
    }
}

#[derive(Default)]
pub struct LocalFsBuilder {
    config: LocalFsConfig,
    filter: Option<PathFilter>,
}

impl fmt::Debug for LocalFsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFsBuilder")
            .field("config", &self.config)
            .field("filter", &self.filter.is_some())
            .finish()
    }
}

impl LocalFsBuilder {
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = Some(root.to_string());
        self
    }

    pub fn filter(mut self, filter: PathFilter) -> Self {
        self.filter = Some(filter);
        self
    }
}

impl Builder for LocalFsBuilder {
    type Config = LocalFsConfig;

    // No I/O here: a share that is mounted late only fails the operations made before it is back.
    fn build(self) -> Result<impl Service> {
        let root = self
            .config
            .root
            .filter(|root| !root.is_empty())
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "root is not specified"))?;
        Ok(LocalFs {
            core: Arc::new(Core {
                root: PathBuf::from(root),
                filter: self.filter,
            }),
        })
    }
}

struct Core {
    root: PathBuf,
    filter: Option<PathFilter>,
}

impl Core {
    fn allowed(&self, key: &str) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter(key))
    }

    /// Opens the folder `parts` names, one level at a time relative to the level above, so
    /// no symlink is followed even if a folder is swapped while it is walked.
    fn open_folder(&self, parts: &[&str]) -> Result<Dir> {
        let mut folder =
            Dir::open_ambient_dir(&self.root, ambient_authority()).map_err(io_error)?;
        let mut key = String::new();
        for part in parts {
            key.push_str(part);
            key.push('/');
            if !self.allowed(&key) {
                return Err(refused(&key));
            }
            folder = open_child(&folder, part)?.ok_or_else(|| missing(&key))?;
        }
        Ok(folder)
    }

    fn stat_path(&self, path: &str) -> Result<Metadata> {
        let parts = path_parts(path)?;
        let Some((name, folders)) = parts.split_last() else {
            self.open_folder(&[])?;
            return Ok(MetadataBuilder::dir().build());
        };
        let folder = self.open_folder(folders)?;
        let entry = folder.symlink_metadata(name).map_err(io_error)?;
        let mut key = parts.join("/");
        if entry.is_dir() {
            key.push('/');
            if !self.allowed(&key) {
                return Err(refused(&key));
            }
            return Ok(MetadataBuilder::dir().build());
        }
        if !entry.is_file() || path.ends_with('/') {
            return Err(missing(path));
        }
        if !self.allowed(&key) {
            return Err(refused(&key));
        }
        file_metadata(&entry)
    }

    fn open_file(&self, path: &str) -> Result<File> {
        let parts = path_parts(path)?;
        let Some((name, folders)) = parts.split_last().filter(|_| !path.ends_with('/')) else {
            return Err(missing(path));
        };
        let folder = self.open_folder(folders)?;
        let key = parts.join("/");
        if !self.allowed(&key) {
            return Err(refused(&key));
        }
        // The early check keeps devices from being opened; the late one catches a swap between.
        if !folder.symlink_metadata(name).map_err(io_error)?.is_file() {
            return Err(missing(path));
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        // A file swapped for a fifo after the check must not block the open.
        #[cfg(unix)]
        cap_std::fs::OpenOptionsExt::custom_flags(
            &mut options,
            rustix::fs::OFlags::NONBLOCK.bits() as i32,
        );
        let file = folder.open_with(name, &options).map_err(io_error)?;
        if !file.metadata().map_err(io_error)?.is_file() {
            return Err(missing(path));
        }
        Ok(file.into_std())
    }
}

/// Serves a local folder read-only. See [`LocalFsBuilder`].
#[derive(Clone)]
pub struct LocalFs {
    core: Arc<Core>,
}

impl fmt::Debug for LocalFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFs")
            .field("root", &self.core.root)
            .finish_non_exhaustive()
    }
}

impl Service for LocalFs {
    type Reader = oio::PositionReader<FileReader>;
    type Writer = ();
    type Lister = FolderLister;
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new(SCHEME, self.core.root.to_string_lossy(), "")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            list: true,
            list_with_recursive: true,
            list_with_start_after: true,
            shared: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(read_only())
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let (core, path) = (self.core.clone(), path.to_string());
        blocking(move || core.stat_path(&path))
            .await
            .map(RpStat::new)
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(FileReader {
            core: self.core.clone(),
            path: path.to_string(),
        }))
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        Err(read_only())
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Err(read_only())
    }

    fn list(&self, _: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Ok(FolderLister {
            core: self.core.clone(),
            path: path.to_string(),
            recursive: args.recursive(),
            start_after: args.start_after().map(str::to_string),
            frames: None,
        })
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(read_only())
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(read_only())
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(read_only())
    }
}

pub struct FileReader {
    core: Arc<Core>,
    path: String,
}

impl oio::PositionRead for FileReader {
    type Handle = Arc<File>;

    async fn open(&self) -> Result<Self::Handle> {
        let (core, path) = (self.core.clone(), self.path.clone());
        blocking(move || core.open_file(&path)).await.map(Arc::new)
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }
        let file = handle.clone();
        let bytes = blocking(move || {
            let mut buffer = vec![0; size];
            let read = read_at(&file, &mut buffer, offset).map_err(new_std_io_error)?;
            buffer.truncate(read);
            Ok(buffer)
        })
        .await?;
        Ok(Buffer::from(Bytes::from(bytes)))
    }
}

enum ChildKind {
    Folder,
    File(Metadata),
}

struct Child {
    name: String,
    key: String,
    kind: ChildKind,
}

struct Frame {
    folder: Arc<Dir>,
    children: std::vec::IntoIter<Child>,
}

/// Walks folders depth first. Each folder is sorted by full key, folders with their trailing
/// `/`, so the whole walk is in key order and a `start_after` can skip whole subtrees.
pub struct FolderLister {
    core: Arc<Core>,
    path: String,
    recursive: bool,
    start_after: Option<String>,
    frames: Option<Vec<Frame>>,
}

impl FolderLister {
    fn after(&self, key: &str) -> bool {
        self.start_after.as_deref().is_none_or(|after| key > after)
    }

    /// Whether everything below the folder `key` sorts before `start_after`.
    fn passed(&self, key: &str) -> bool {
        self.start_after
            .as_deref()
            .is_some_and(|after| after > key && !after.starts_with(key))
    }

    async fn start(&self) -> Result<Vec<Frame>> {
        let base = self.path.trim_start_matches('/');
        let folder = base[..base.rfind('/').map_or(0, |end| end + 1)].to_string();
        let core = self.core.clone();
        let opened = blocking(move || {
            let parts = path_parts(&folder)?;
            let handle = core.open_folder(&parts)?;
            let children = read_folder(&handle, &folder)?;
            Ok(Frame {
                folder: Arc::new(handle),
                children: children.into_iter(),
            })
        })
        .await;
        match opened {
            Ok(frame) => Ok(vec![frame]),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }
}

impl oio::List for FolderLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.frames.is_none() {
            let frames = self.start().await?;
            let listed = !frames.is_empty() && self.path.ends_with('/');
            self.frames = Some(frames);
            if listed && self.start_after.is_none() {
                return Ok(Some(oio::Entry::new(
                    &self.path,
                    MetadataBuilder::dir().build(),
                )));
            }
        }
        let prefix = self.path.trim_start_matches('/').to_string();
        loop {
            let frames = self.frames.as_mut().expect("frames are set above");
            let Some(frame) = frames.last_mut() else {
                return Ok(None);
            };
            let Some(child) = frame.children.next() else {
                frames.pop();
                continue;
            };
            if !child.key.starts_with(&prefix) || !self.core.allowed(&child.key) {
                continue;
            }
            match child.kind {
                ChildKind::File(metadata) => {
                    if self.after(&child.key) {
                        return Ok(Some(oio::Entry::new(&child.key, metadata)));
                    }
                }
                ChildKind::Folder => {
                    let parent = frame.folder.clone();
                    if self.recursive && !self.passed(&child.key) {
                        let (name, key) = (child.name.clone(), child.key.clone());
                        if let Some(frame) =
                            blocking(move || open_frame(&parent, &name, &key)).await?
                        {
                            self.frames
                                .as_mut()
                                .expect("frames are set above")
                                .push(frame);
                        }
                    }
                    if self.after(&child.key) {
                        return Ok(Some(oio::Entry::new(
                            &child.key,
                            MetadataBuilder::dir().build(),
                        )));
                    }
                }
            }
        }
    }
}

/// Opens the folder `name` names in `parent`, or `None` if it is not a folder. A symlink is
/// never a folder here, and one swapped in after the check fails the open instead.
fn open_child(parent: &Dir, name: &str) -> Result<Option<Dir>> {
    match parent.symlink_metadata(name) {
        Ok(entry) if entry.is_dir() => {}
        Ok(_) => return Ok(None),
        Err(error) if absent(&error) => return Ok(None),
        Err(error) => return Err(io_error(error)),
    }
    match parent.open_dir_nofollow(name) {
        Ok(folder) => Ok(Some(folder)),
        Err(error) if absent(&error) => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

/// A folder that became a symlink or vanished after its parent was read is skipped.
fn open_frame(parent: &Dir, name: &str, key: &str) -> Result<Option<Frame>> {
    let Some(folder) = open_child(parent, name)? else {
        return Ok(None);
    };
    let children = read_folder(&folder, key)?;
    Ok(Some(Frame {
        folder: Arc::new(folder),
        children: children.into_iter(),
    }))
}

/// Regular files and folders of one folder, sorted by key. Symlinks, special files and names
/// that are not UTF-8 are left out.
fn read_folder(folder: &Dir, prefix: &str) -> Result<Vec<Child>> {
    let mut children = Vec::new();
    for entry in folder.entries().map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|name| plain_name(name)) else {
            continue;
        };
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) if absent(&error) => continue,
            Err(error) => return Err(io_error(error)),
        };
        let (key, kind) = if kind.is_dir() {
            (format!("{prefix}{name}/"), ChildKind::Folder)
        } else if kind.is_file() {
            let metadata = match entry.metadata() {
                Ok(metadata) => file_metadata(&metadata)?,
                Err(error) if absent(&error) => continue,
                Err(error) => return Err(io_error(error)),
            };
            (format!("{prefix}{name}"), ChildKind::File(metadata))
        } else {
            continue;
        };
        children.push(Child {
            name: name.to_string(),
            key,
            kind,
        });
    }
    children.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(children)
}

/// The names of a normalized path. Each must be a plain name, see [`plain_name`].
fn path_parts(path: &str) -> Result<Vec<&str>> {
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parts: Vec<&str> = trimmed.split('/').collect();
    if !parts.iter().all(|part| plain_name(part)) {
        return Err(missing(path));
    }
    Ok(parts)
}

/// Empty names, `.` and `..` name nothing below the root. Windows also reads `\` as a
/// separator and `:` as a stream name.
fn plain_name(name: &str) -> bool {
    !matches!(name, "" | "." | "..")
        && !name.contains('\0')
        && !(cfg!(windows) && name.contains(['\\', ':']))
}

fn file_metadata(entry: &cap_std::fs::Metadata) -> Result<Metadata> {
    let modified = entry.modified().map_err(new_std_io_error)?.into_std();
    let mut metadata = MetadataBuilder::file(entry.len());
    metadata.last_modified(Timestamp::try_from(modified)?);
    Ok(metadata.build())
}

#[cfg(unix)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(new_task_join_error)?
}

/// A missing entry, or a file where a folder was expected, counts as absent.
fn absent(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
    )
}

fn io_error(error: std::io::Error) -> Error {
    if absent(&error) {
        return Error::new(ErrorKind::NotFound, "no such file or folder below the root");
    }
    new_std_io_error(error)
}

fn missing(path: &str) -> Error {
    Error::new(ErrorKind::NotFound, "no such file or folder below the root")
        .with_context("path", path)
}

fn refused(key: &str) -> Error {
    Error::new(
        ErrorKind::PermissionDenied,
        "the path filter refuses this path",
    )
    .with_context("path", key)
}

fn read_only() -> Error {
    Error::new(ErrorKind::Unsupported, "this backend is read-only")
}

#[cfg(test)]
#[path = "local_fs_tests.rs"]
mod tests;
