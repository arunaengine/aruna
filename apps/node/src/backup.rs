//! Serves consistent backups of a running node on a Unix socket inside its storage directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aruna_blob::git::GitStore;
use aruna_storage::snapshot::{
    BACKUP_SOCKET, GIT_KEYSPACE, PREFIXES, SnapshotError, SnapshotStats, SnapshotWriter,
    consistent_snapshots,
};
use tracing::warn;
#[cfg(unix)]
use {
    aruna_runner::driver::DriverContext,
    aruna_scheduler::shutdown::Shutdown,
    std::io::BufWriter,
    std::os::unix::fs::{FileTypeExt, PermissionsExt},
    std::time::Duration,
    tokio::net::{UnixListener, UnixStream},
    tokio_util::sync::CancellationToken,
    tracing::info,
};

/// How long a backup write may make no progress before the client is dropped.
#[cfg(unix)]
const BACKUP_STALL: Duration = Duration::from_secs(60);

/// The stores one backup covers.
#[derive(Clone)]
pub struct BackupSources {
    pub main: fjall::Database,
    pub sync: fjall::Database,
    pub metadata: fjall::Database,
    pub git_root: PathBuf,
    /// Holds each repository while it is read, so a backup has its refs with their objects.
    pub git: Option<Arc<GitStore>>,
}

/// Writes one backup stream. The three stores are read from snapshots of one instant; each Git
/// repository is read afterwards while no Git effect changes it. Blob contents are left out.
pub fn write_backup(
    out: impl Write,
    sources: &BackupSources,
    created_unix_seconds: u64,
) -> Result<SnapshotStats, SnapshotError> {
    let databases = [&sources.main, &sources.sync, &sources.metadata];
    let snapshots = consistent_snapshots(&databases)?;
    let mut writer = SnapshotWriter::new(out, created_unix_seconds)?;
    for ((db, snapshot), prefix) in databases.into_iter().zip(&snapshots).zip(PREFIXES) {
        writer.database_at(db, snapshot, prefix)?;
    }
    writer.begin_keyspace(GIT_KEYSPACE)?;
    write_files(&mut writer, &sources.git_root, "", sources.git.as_deref())?;
    writer.end_keyspace()?;
    let (_, stats) = writer.finish()?;
    Ok(stats)
}

/// Writes the tree below `dir` in name order. Lock files and entries that vanish while the
/// walk runs are skipped. A repository below `dir` is read while `git` holds it.
fn write_files(
    writer: &mut SnapshotWriter<impl Write>,
    dir: &Path,
    relative: &str,
    git: Option<&GitStore>,
) -> Result<(), SnapshotError> {
    let mut entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries.collect::<Result<Vec<_>, _>>()?,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            warn!(path = %entry.path().display(), "Skipping a non UTF-8 path in the backup");
            continue;
        };
        if name.ends_with(".lock") {
            continue;
        }
        let path = format!("{relative}{name}");
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if file_type.is_dir() {
            let id = name
                .strip_suffix(".git")
                .and_then(|id| id.parse::<ulid::Ulid>().ok());
            let _held = git.zip(id).map(|(store, id)| store.hold_repository(id));
            writer.entry(format!("{path}/").as_bytes(), &[])?;
            write_files(writer, &entry.path(), &format!("{path}/"), None)?;
        } else if file_type.is_file() {
            let file = match std::fs::File::open(entry.path()) {
                Ok(file) => file,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let len = file.metadata()?.len();
            writer.entry_from(path.as_bytes(), len, file)?;
        }
    }
    Ok(())
}

/// Listens on `<storage>/backup.sock` and writes one backup to each connection, one at a
/// time. Only the socket's owner may connect. A bind failure leaves the node running.
#[cfg(unix)]
pub(crate) fn serve(storage_path: &Path, ctx: &DriverContext, shutdown: &Shutdown) {
    let Some(sources) = node_sources(storage_path, ctx) else {
        return;
    };
    let path = storage_path.join(BACKUP_SOCKET);
    let listener = match bind_socket(&path) {
        Ok(listener) => listener,
        Err(error) => {
            warn!(path = %path.display(), error = %error, "Backup socket is unavailable");
            return;
        }
    };
    info!(path = %path.display(), "Serving live backups");
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let stream = tokio::select! {
                () = token.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        warn!(error = %error, "Backup socket accept failed");
                        continue;
                    }
                },
            };
            match write_client(stream, sources.clone(), BACKUP_STALL, &token).await {
                Ok(Ok(stats)) => info!(
                    keyspaces = stats.keyspace_count,
                    entries = stats.entry_count,
                    "Live backup written"
                ),
                Ok(Err(error)) => warn!(error = %error, "Live backup failed"),
                Err(error) => warn!(error = %error, "Live backup task failed"),
            }
        }
    });
}

/// Writes one backup to `stream` on a blocking thread and waits for it. A write without
/// progress for `stall` fails, and cancellation closes the socket so the writer stops.
#[cfg(unix)]
async fn write_client(
    stream: UnixStream,
    sources: BackupSources,
    stall: Duration,
    token: &CancellationToken,
) -> Result<Result<SnapshotStats, SnapshotError>, tokio::task::JoinError> {
    let (stream, closer) = match blocking_stream(stream, stall) {
        Ok(pair) => pair,
        Err(error) => return Ok(Err(error.into())),
    };
    let mut backup = tokio::task::spawn_blocking(move || {
        let now = aruna_clock::unix_timestamp_secs();
        write_backup(BufWriter::new(stream), &sources, now)
    });
    tokio::select! {
        () = token.cancelled() => {
            if let Err(error) = closer.shutdown(std::net::Shutdown::Both) {
                warn!(error = %error, "Backup socket did not close");
            }
            backup.await
        }
        result = &mut backup => result,
    }
}

/// The stream in blocking mode with a write timeout, and a second handle that can close it.
#[cfg(unix)]
fn blocking_stream(
    stream: UnixStream,
    stall: Duration,
) -> std::io::Result<(
    std::os::unix::net::UnixStream,
    std::os::unix::net::UnixStream,
)> {
    let stream = stream.into_std()?;
    stream.set_nonblocking(false)?;
    stream.set_write_timeout(Some(stall))?;
    let closer = stream.try_clone()?;
    Ok((stream, closer))
}

/// The stores of a node with its storage, document sync and metadata databases open.
#[cfg(unix)]
fn node_sources(storage_path: &Path, ctx: &DriverContext) -> Option<BackupSources> {
    Some(BackupSources {
        main: ctx.storage_handle.database()?,
        sync: ctx
            .net_handle
            .as_ref()?
            .document_sync_database()
            .inner()
            .clone(),
        metadata: ctx.graph_handle.as_ref()?.store_database(),
        git_root: storage_path.join("git"),
        git: ctx.blob_handle.as_ref().and_then(|blob| blob.git_store()),
    })
}

/// Binds the socket with owner-only access, replacing a socket left by an earlier process.
#[cfg(unix)]
fn bind_socket(path: &Path) -> std::io::Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => {
            return Err(std::io::Error::new(
                ErrorKind::AlreadyExists,
                "a non-socket file holds the backup socket path",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::snapshot::{SnapshotReader, SnapshotRecord};

    fn open_sources(root: &Path) -> BackupSources {
        let open = |name: &str| fjall::Database::builder(root.join(name)).open().unwrap();
        BackupSources {
            main: open("main"),
            sync: open("sync"),
            metadata: open("metadata"),
            git_root: root.join("git"),
            git: Some(Arc::new(GitStore::new(
                root.join("git"),
                root.join("helper"),
            ))),
        }
    }

    fn git_entries(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
        let mut reader = SnapshotReader::new(bytes).unwrap();
        let mut section = String::new();
        let mut entries = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            match record {
                SnapshotRecord::Keyspace(name) => section = name,
                SnapshotRecord::Entry { key, value } if section == GIT_KEYSPACE => {
                    entries.push((String::from_utf8(key).unwrap(), value));
                }
                SnapshotRecord::Entry { .. } | SnapshotRecord::EndKeyspace => {}
            }
        }
        entries
    }

    /// Sources whose Git files exceed a socket buffer, so a write waits for its reader.
    #[cfg(unix)]
    fn large_sources(root: &Path) -> BackupSources {
        std::fs::create_dir_all(root.join("git")).unwrap();
        std::fs::write(root.join("git/large"), vec![0_u8; 8 << 20]).unwrap();
        open_sources(root)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_joins_writer() {
        let root = tempfile::tempdir().unwrap();
        let (server, _client) = UnixStream::pair().unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let stall = Duration::from_secs(3600);
        let writing = write_client(server, large_sources(root.path()), stall, &token);
        let written = tokio::time::timeout(Duration::from_secs(300), writing).await;
        assert!(
            matches!(written, Ok(Ok(Err(_)))),
            "shutdown did not stop and join the writer"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stalled_client_dropped() {
        let root = tempfile::tempdir().unwrap();
        let (server, _client) = UnixStream::pair().unwrap();
        let token = CancellationToken::new();
        let stall = Duration::from_millis(100);
        let writing = write_client(server, large_sources(root.path()), stall, &token);
        let written = tokio::time::timeout(Duration::from_secs(300), writing).await;
        assert!(
            matches!(written, Ok(Ok(Err(_)))),
            "a stalled write did not fail"
        );
    }

    #[test]
    fn backup_includes_push() {
        let root = tempfile::tempdir().unwrap();
        let sources = open_sources(root.path());
        let id = ulid::Ulid::from(7_u128);
        let repository = root.path().join(format!("git/{id}.git"));
        std::fs::create_dir_all(repository.join("objects")).unwrap();
        std::fs::create_dir_all(repository.join("refs/heads")).unwrap();
        let store = sources.git.clone().unwrap();
        let held = store.hold_repository(id);
        let backup = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            write_backup(&mut bytes, &sources, 1).map(|_| bytes)
        });
        // A push writes its object and then its ref while it holds the repository.
        std::fs::write(repository.join("objects/pushed"), "object").unwrap();
        std::fs::write(repository.join("refs/heads/main"), "pushed\n").unwrap();
        drop(held);
        let entries = git_entries(&backup.join().unwrap().unwrap());
        for (path, value) in [
            ("objects/pushed", "object"),
            ("refs/heads/main", "pushed\n"),
        ] {
            let path = format!("{id}.git/{path}");
            assert!(
                entries.contains(&(path.clone(), value.as_bytes().to_vec())),
                "the backup misses {path}"
            );
        }
    }
}
