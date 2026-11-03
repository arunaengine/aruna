//! Serves consistent backups of a running node on a Unix socket inside its storage directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::{BufWriter, ErrorKind, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};

use aruna_core::shutdown::Shutdown;
use aruna_operations::driver::DriverContext;
use aruna_storage::snapshot::{SnapshotError, SnapshotStats, SnapshotWriter, consistent_snapshots};
use tokio::net::UnixListener;
use tracing::{info, warn};

/// File name of the backup socket in the node's storage directory.
pub const BACKUP_SOCKET: &str = "backup.sock";
/// Keyspace name prefixes of the node, document sync and metadata stores in a backup.
pub const MAIN_PREFIX: &str = "main/";
pub const SYNC_PREFIX: &str = "document-sync/";
pub const METADATA_PREFIX: &str = "craqle/";
/// Pseudo keyspace holding the Git repositories: relative path to file bytes, `/` ends a directory.
pub const GIT_KEYSPACE: &str = "git";
/// The store prefixes in stream order: node, document sync, metadata.
pub const PREFIXES: [&str; 3] = [MAIN_PREFIX, SYNC_PREFIX, METADATA_PREFIX];

/// The stores one backup covers.
#[derive(Clone)]
pub struct BackupSources {
    pub main: fjall::Database,
    pub sync: fjall::Database,
    pub metadata: fjall::Database,
    pub git_root: PathBuf,
}

/// Writes one backup stream. The three stores are read from snapshots of one instant; the Git
/// files are read afterwards. Blob contents are not part of a backup.
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
    write_files(&mut writer, &sources.git_root, "")?;
    writer.end_keyspace()?;
    let (_, stats) = writer.finish()?;
    Ok(stats)
}

/// Writes the tree below `dir` in name order. Lock files and entries that vanish while the
/// walk runs are skipped, since Git may update a repository meanwhile.
fn write_files(
    writer: &mut SnapshotWriter<impl Write>,
    dir: &Path,
    relative: &str,
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
            writer.entry(format!("{path}/").as_bytes(), &[])?;
            write_files(writer, &entry.path(), &format!("{path}/"))?;
        } else if file_type.is_file() {
            match std::fs::read(entry.path()) {
                Ok(bytes) => writer.entry(path.as_bytes(), &bytes)?,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

/// Listens on `<storage>/backup.sock` and writes one backup to each connection, one at a
/// time. Only the socket's owner may connect. A bind failure leaves the node running.
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
            let sources = sources.clone();
            let backup = tokio::task::spawn_blocking(move || {
                let stream = stream.into_std()?;
                stream.set_nonblocking(false)?;
                let now = aruna_core::time::unix_timestamp_secs();
                write_backup(BufWriter::new(stream), &sources, now)
            });
            tokio::select! {
                () = token.cancelled() => break,
                result = backup => match result {
                    Ok(Ok(stats)) => info!(
                        keyspaces = stats.keyspace_count,
                        entries = stats.entry_count,
                        "Live backup written"
                    ),
                    Ok(Err(error)) => warn!(error = %error, "Live backup failed"),
                    Err(error) => warn!(error = %error, "Live backup task failed"),
                },
            }
        }
    });
}

/// The stores of a node with its storage, document sync and metadata databases open.
fn node_sources(storage_path: &Path, ctx: &DriverContext) -> Option<BackupSources> {
    Some(BackupSources {
        main: ctx.storage_handle.database()?,
        sync: ctx
            .net_handle
            .as_ref()?
            .document_sync_database()
            .inner()
            .clone(),
        metadata: ctx.metadata_handle.as_ref()?.store_database(),
        git_root: storage_path.join("git"),
    })
}

/// Binds the socket with owner-only access, replacing a socket left by an earlier process.
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
