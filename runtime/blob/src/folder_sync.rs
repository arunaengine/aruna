//! Makes new files and folders of a local folder backend survive a crash, by syncing every
//! folder from the one holding a new entry up to the backend root once the entry is in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use opendal::raw::{
    Layer, OpCompose, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpRestore, OpStat,
    OpWrite, RpCreateDir, RpPresign, RpRename, RpRestore, RpStat, Service, ServiceInfo, Servicer,
    new_task_join_error, oio,
};
use opendal::{BytesRange, Capability, Error, ErrorKind, Metadata, OperationContext, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Wraps the OpenDAL `Fs` service, which syncs a written file but none of its folders.
#[derive(Debug)]
pub(crate) struct FolderSync;

impl Layer for FolderSync {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        let root = Arc::from(Path::new(inner.info().root().as_ref()));
        Arc::new(SyncedFs { inner, root })
    }
}

#[derive(Debug)]
struct SyncedFs {
    inner: Servicer,
    root: Arc<Path>,
}

impl Service for SyncedFs {
    type Reader = oio::Reader;
    type Writer = SyncedWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let created = self.inner.create_dir(ctx, path, args).await?;
        sync_parents(self.root.clone(), path.to_string()).await?;
        Ok(created)
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(SyncedWriter {
            inner: self.inner.write(ctx, path, args)?,
            root: self.root.clone(),
            path: path.to_string(),
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

/// Closes the inner writer, which syncs the file, and then syncs the folders above it.
struct SyncedWriter {
    inner: oio::Writer,
    root: Arc<Path>,
    path: String,
}

impl oio::Write for SyncedWriter {
    async fn write(&mut self, bs: opendal::Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn copy_from(&mut self, path: &str, args: OpRead, range: BytesRange) -> Result<()> {
        self.inner.copy_from(path, args, range).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let metadata = self.inner.close().await?;
        sync_parents(self.root.clone(), self.path.clone()).await?;
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

async fn sync_parents(root: Arc<Path>, path: String) -> Result<()> {
    let synced = tokio::task::spawn_blocking(move || {
        parent_chain(&root, &path)
            .iter()
            .try_for_each(|folder| sync_path(folder))
    });
    synced.await.map_err(new_task_join_error)?.map_err(unsynced)
}

/// The folders from the one holding `path` up to `root`, deepest first. All of them are
/// synced, also old ones, since a parallel write may have created one it has not synced yet.
fn parent_chain(root: &Path, path: &str) -> Vec<PathBuf> {
    let names: Vec<&str> = path.split('/').filter(|name| !name.is_empty()).collect();
    let depth = names.len().saturating_sub(1);
    (0..=depth)
        .rev()
        .map(|level| names[..level].iter().collect::<PathBuf>())
        .map(|relative| root.join(relative))
        .collect()
}

#[cfg(unix)]
fn sync_path(folder: &Path) -> std::io::Result<()> {
    sync_handle(std::fs::File::open(folder)?)
}

#[cfg(not(unix))]
fn sync_path(_: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Flushes an open file or folder. Windows can only flush handles opened for writing, which
/// folders never are, so there this does nothing.
#[cfg(unix)]
fn sync_handle(handle: impl std::os::fd::AsFd) -> std::io::Result<()> {
    rustix::fs::fsync(handle).map_err(std::io::Error::from)
}

/// Flushes a capability directory handle. On Linux `cap_std::fs::Dir` opens directories with
/// `O_PATH`, which `fsync` rejects with `EBADF`, so reopen `"."` relative to the handle first.
#[cfg(unix)]
pub(crate) fn sync_dir(folder: &cap_std::fs::Dir) -> std::io::Result<()> {
    use rustix::fs::{Mode, OFlags, openat};
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let opened = openat(folder, ".", flags, Mode::empty()).map_err(std::io::Error::from)?;
    sync_handle(&opened)
}

#[cfg(not(unix))]
pub(crate) fn sync_dir(_: &cap_std::fs::Dir) -> std::io::Result<()> {
    Ok(())
}

/// A failed folder sync. It is never temporary: a retried close would write the object again.
pub(crate) fn unsynced(error: std::io::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "a folder of the write could not be synced",
    )
    .set_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::{Operator, services};

    fn synced(root: &Path) -> Operator {
        let builder = services::Fs::default().root(root.to_str().unwrap());
        Operator::new(builder).unwrap().layer(FolderSync)
    }

    #[test]
    fn chain_reaches_root() {
        let root = Path::new("/served");

        assert_eq!(
            parent_chain(root, "a/b/c.bin"),
            [root.join("a/b"), root.join("a"), root.to_path_buf()]
        );
        assert_eq!(parent_chain(root, "c.bin"), [root.to_path_buf()]);
        assert_eq!(
            parent_chain(root, "made/folder/"),
            [root.join("made"), root.to_path_buf()]
        );
    }

    #[tokio::test]
    async fn writes_nested_folders() {
        let root = tempfile::tempdir().unwrap();
        let op = synced(root.path());

        op.write("a/b/c/blob", "nested").await.unwrap();
        assert_eq!(op.read("a/b/c/blob").await.unwrap().to_vec(), b"nested");
        assert_eq!(
            std::fs::read(root.path().join("a/b/c/blob")).unwrap(),
            b"nested"
        );
        op.write("a/b/next", "sibling").await.unwrap();
        assert_eq!(op.read("a/b/next").await.unwrap().to_vec(), b"sibling");
        op.create_dir("made/folder/").await.unwrap();
        assert!(root.path().join("made/folder").is_dir());
    }

    #[tokio::test]
    async fn writes_existing_parent() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("old")).unwrap();
        let op = synced(root.path());

        let mut writer = op.writer("old/blob").await.unwrap();
        writer.write("first ").await.unwrap();
        writer.write("second").await.unwrap();
        writer.close().await.unwrap();
        assert_eq!(op.read("old/blob").await.unwrap().to_vec(), b"first second");
        assert_eq!(op.stat("old/blob").await.unwrap().content_length(), 12);
    }

    #[tokio::test]
    async fn blocked_parent_fails() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("taken"), "file").unwrap();
        let op = synced(root.path());

        assert!(op.write("taken/blob", "data").await.is_err());
        assert_eq!(std::fs::read(root.path().join("taken")).unwrap(), b"file");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn missing_folder_fails() {
        let root = tempfile::tempdir().unwrap();
        let gone: Arc<Path> = Arc::from(root.path().join("gone").as_path());

        let error = sync_parents(gone, "a/blob".to_string()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(!error.is_temporary());
    }
}
