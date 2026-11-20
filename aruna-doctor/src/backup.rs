//! Takes a live backup from a running node's backup socket and restores a backup into a new
//! storage directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use crate::storage::{ImportKeyspaceState, ensure_target_path};
use aruna_core::keyspaces::{CHECK_QUEUE_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_CHECK_KEYSPACE};
use aruna_core::structs::storage::broken::StorageCheck;
use aruna_storage::snapshot::{GIT_KEYSPACE, PREFIXES};
use aruna_storage::snapshot::{SnapshotError, SnapshotReader, SnapshotRecord, SnapshotStats};
use craqle::{CraqleNode, CraqleOptions, SearchStorage};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PersistMode, Readable};
use std::fs::File;
use std::io::BufReader;
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use {
    aruna_storage::snapshot::BACKUP_SOCKET,
    std::io::{BufWriter, ErrorKind, Write},
    std::os::unix::net::UnixStream,
};

#[cfg(not(unix))]
pub async fn backup(_storage_path: String, _target_path: String) -> Result<(), CliError> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "live backups need the node's Unix socket, which this platform does not have",
    )
    .into())
}

#[cfg(unix)]
pub async fn backup(storage_path: String, target_path: String) -> Result<(), CliError> {
    let socket = PathBuf::from(storage_path).join(BACKUP_SOCKET);
    let target = PathBuf::from(target_path);
    let (bytes, stats) =
        tokio::task::spawn_blocking(move || backup_file(&socket, &target)).await??;
    println!(
        "Backup written: bytes={bytes}, keyspaces={}, entries={}, created_at={}",
        stats.keyspace_count, stats.entry_count, stats.created_unix_seconds,
    );
    Ok(())
}

pub async fn restore(backup_path: String, target_path: String) -> Result<(), CliError> {
    let backup = PathBuf::from(backup_path);
    let target = PathBuf::from(target_path);
    let stats = tokio::task::spawn_blocking(move || restore_backup(&backup, &target)).await??;
    println!(
        "Backup restored: keyspaces={}, entries={}, created_at={}",
        stats.keyspace_count, stats.entry_count, stats.created_unix_seconds,
    );
    println!(
        "Blob contents are not part of a backup. The node checks every bucket after its start; \
         GET /data/buckets/{{bucket}}/storage/check lists versions whose blob is missing."
    );
    Ok(())
}

/// Copies the node's backup stream to an owner-only temporary file beside `target`, checks the
/// whole file and only then publishes it as `target`, never over a file created meanwhile.
#[cfg(unix)]
pub fn backup_file(socket: &Path, target: &Path) -> Result<(u64, SnapshotStats), SnapshotError> {
    if target.exists() {
        return Err(SnapshotError::SnapshotPathExists(target.to_path_buf()));
    }
    let dir = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    // Every error drops the temporary file, which removes it.
    let partial = tempfile::Builder::new()
        .prefix(".backup-")
        .suffix(".partial")
        .tempfile_in(dir)?;
    let done = receive_backup(socket, partial.as_file().try_clone()?, partial.path())?;
    match partial.persist_noclobber(target) {
        Ok(_) => {}
        Err(failed) if failed.error.kind() == ErrorKind::AlreadyExists => {
            return Err(SnapshotError::SnapshotPathExists(target.to_path_buf()));
        }
        Err(failed) => return Err(failed.error.into()),
    }
    File::open(dir)?.sync_all()?;
    Ok(done)
}

#[cfg(unix)]
fn receive_backup(
    socket: &Path,
    file: File,
    partial: &Path,
) -> Result<(u64, SnapshotStats), SnapshotError> {
    let mut stream = UnixStream::connect(socket)?;
    let mut writer = BufWriter::new(file);
    let bytes = std::io::copy(&mut stream, &mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let mut reader = SnapshotReader::new(BufReader::new(File::open(partial)?))?;
    while reader.next_with(|_| Ok(Some(std::io::sink())))?.is_some() {}
    Ok((bytes, reader.stats()))
}

/// Builds a new storage directory from a backup: the node database at `target`, `document-sync`,
/// `craqle/store` and `git`. It grows in an owner-only staging folder beside `target`, uniquely
/// named so a crashed restore blocks no other, and is published once the whole file checked.
pub fn restore_backup(backup: &Path, target: &Path) -> Result<SnapshotStats, SnapshotError> {
    ensure_target_path(target)?;
    let mut reader = SnapshotReader::new(BufReader::new(File::open(backup)?))?;
    let parent = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    // Every error drops the staging directory, which removes it.
    let staging = staging_dir(parent)?;
    restore_into(&mut reader, staging.path())?;
    publish_dir(staging.path(), target)?;
    let _published = staging.keep();
    Ok(reader.stats())
}

/// An empty directory in `parent` that only its owner may enter.
fn staging_dir(parent: &Path) -> std::io::Result<tempfile::TempDir> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(".restore-");
    #[cfg(unix)]
    builder.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700));
    builder.tempdir_in(parent)
}

/// Moves the finished `staging` directory to `target`, refusing even an empty directory that
/// appeared there meanwhile; Windows refuses to rename over a directory by itself.
fn publish_dir(staging: &Path, target: &Path) -> Result<(), SnapshotError> {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    let moved = {
        use rustix::fs::{CWD, RenameFlags, renameat_with};
        renameat_with(CWD, staging, CWD, target, RenameFlags::NOREPLACE).map_err(Into::into)
    };
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    let moved = std::fs::rename(staging, target);
    match moved {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(SnapshotError::TargetPathExists(target.to_path_buf()))
        }
        Err(error) => Err(error.into()),
    }
}

/// Writes all records into the new directory `target` and closes its databases again.
fn restore_into(
    reader: &mut SnapshotReader<impl std::io::Read>,
    target: &Path,
) -> Result<(), SnapshotError> {
    let databases = open_targets(target)?;
    let git_root = target.join("git");
    let mut section = None;

    loop {
        // Git files stream to disk; store entries are held, as Fjall takes whole values.
        let git = matches!(section, Some(Section::Git));
        let record = reader.next_with(|key| {
            if git {
                git_file(&git_root, key)
            } else {
                Ok(None)
            }
        })?;
        let Some(record) = record else {
            break;
        };
        match record {
            SnapshotRecord::Keyspace(name) => section = Some(open_section(&databases, &name)?),
            SnapshotRecord::Entry { key, value } => match section.as_mut() {
                Some(Section::Store(index, state)) => {
                    state.insert(&databases[*index], key, value)?
                }
                Some(Section::Git) | None => {}
            },
            SnapshotRecord::EndKeyspace => {
                if let Some(Section::Store(_, mut state)) = section.take() {
                    state.finish()?;
                }
            }
        }
    }
    queue_checks(&databases[0])?;
    for database in &databases {
        database.persist(PersistMode::SyncAll)?;
    }
    Ok(())
}

/// Queues a storage check of every bucket, as a backup holds no blob contents: the node then
/// records each current version whose blob is missing as broken.
fn queue_checks(main: &OptimisticTxDatabase) -> Result<(), SnapshotError> {
    let db = main.inner();
    let open = |name| db.keyspace(name, KeyspaceCreateOptions::default);
    let (buckets, checks, queue) = (
        open(S3_BUCKET_KEYSPACE)?,
        open(STORAGE_CHECK_KEYSPACE)?,
        open(CHECK_QUEUE_KEYSPACE)?,
    );
    let record = StorageCheck::new(aruna_clock::unix_timestamp_millis())
        .to_bytes()
        .map_err(std::io::Error::other)?;
    for entry in db.snapshot().iter(&buckets) {
        let bucket = entry.key()?;
        checks.insert(bucket.clone(), &record)?;
        queue.insert(bucket, b"")?;
    }
    Ok(())
}

enum Section {
    Store(usize, ImportKeyspaceState),
    Git,
}

/// Opens the three target databases in `PREFIXES` order. Irokle and Craqle first create their
/// keyspaces, since Fjall keeps the options a keyspace was created with.
fn open_targets(target: &Path) -> Result<[OptimisticTxDatabase; 3], SnapshotError> {
    let open = |path: &Path| {
        OptimisticTxDatabase::builder(path)
            .manual_journal_persist(true)
            .open()
    };
    let main = open(target)?;
    let sync = open(&target.join("document-sync"))?;
    irokle::storage::FjallStorage::from_database(sync.clone()).map_err(std::io::Error::other)?;
    let craqle_root = target.join("craqle");
    let options = CraqleOptions::new().with_search_storage(SearchStorage::Memory);
    drop(CraqleNode::open_with_options(&craqle_root, options).map_err(std::io::Error::other)?);
    let metadata = open(&craqle_root.join("store"))?;
    Ok([main, sync, metadata])
}

fn open_section(
    databases: &[OptimisticTxDatabase; 3],
    name: &str,
) -> Result<Section, SnapshotError> {
    if name == GIT_KEYSPACE {
        return Ok(Section::Git);
    }
    for (index, prefix) in PREFIXES.iter().enumerate() {
        if let Some(keyspace) = name.strip_prefix(prefix) {
            if keyspace.is_empty() || keyspace.len() > usize::from(u8::MAX) {
                return Err(SnapshotError::InvalidStructure(
                    "invalid keyspace name in backup",
                ));
            }
            let keyspace = databases[index].keyspace(keyspace, KeyspaceCreateOptions::default)?;
            return Ok(Section::Store(index, ImportKeyspaceState::new(keyspace)));
        }
    }
    Err(SnapshotError::InvalidStructure(
        "unknown keyspace in backup",
    ))
}

/// Makes one Git entry below `root` and opens a file entry for its value; a key ending in `/` is
/// a directory.
fn git_file(root: &Path, key: &[u8]) -> Result<Option<File>, SnapshotError> {
    let relative = std::str::from_utf8(key)
        .map_err(|_| SnapshotError::InvalidStructure("non UTF-8 Git path in backup"))?;
    let path = Path::new(relative);
    if relative.is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(SnapshotError::InvalidStructure("unsafe Git path in backup"));
    }
    let path = root.join(path);
    if relative.ends_with('/') {
        std::fs::create_dir_all(path)?;
        return Ok(None);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(Some(File::create(path)?))
}

#[cfg(test)]
mod tests {
    use super::restore_backup;
    use aruna::backup::{BackupSources, write_backup};
    use aruna_core::keyspaces::{CHECK_QUEUE_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_CHECK_KEYSPACE};
    use aruna_storage::snapshot::GIT_KEYSPACE;
    use aruna_storage::snapshot::SnapshotWriter;
    use craqle::CraqleNode;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::path::Path;

    struct Source {
        main: OptimisticTxDatabase,
        sync: OptimisticTxDatabase,
        metadata: CraqleNode,
    }

    fn open_source(root: &Path) -> Source {
        let main = OptimisticTxDatabase::builder(root).open().unwrap();
        let sync = OptimisticTxDatabase::builder(root.join("document-sync"))
            .open()
            .unwrap();
        irokle::FjallStorage::from_database(sync.clone()).unwrap();
        let metadata = CraqleNode::open(root.join("craqle")).unwrap();
        for (db, value) in [
            (main.inner().clone(), "main"),
            (sync.inner().clone(), "sync"),
            (metadata.store_database(), "metadata"),
        ] {
            let keyspace = db
                .keyspace("backup-test", KeyspaceCreateOptions::default)
                .unwrap();
            keyspace.insert("key", value).unwrap();
        }
        main.inner()
            .keyspace(S3_BUCKET_KEYSPACE, KeyspaceCreateOptions::default)
            .unwrap()
            .insert("photos", "info")
            .unwrap();
        let repo = root.join("git/one.git");
        std::fs::create_dir_all(repo.join("objects")).unwrap();
        std::fs::create_dir_all(repo.join("refs/heads")).unwrap();
        std::fs::write(repo.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(repo.join("refs/heads/main"), "abc\n").unwrap();
        std::fs::write(repo.join("index.lock"), "partial").unwrap();
        Source {
            main,
            sync,
            metadata,
        }
    }

    fn backup_bytes(root: &Path, source: &Source) -> Vec<u8> {
        let sources = BackupSources {
            main: source.main.inner().clone(),
            sync: source.sync.inner().clone(),
            metadata: source.metadata.store_database(),
            git_root: root.join("git"),
            git: None,
        };
        let mut bytes = Vec::new();
        write_backup(&mut bytes, &sources, 11).unwrap();
        bytes
    }

    fn read_value(db: &fjall::Database, keyspace: &str, key: &str) -> Option<Vec<u8>> {
        let keyspace = db
            .keyspace(keyspace, KeyspaceCreateOptions::default)
            .unwrap();
        let value = db.snapshot().get(&keyspace, key).unwrap();
        value.map(|value| value.to_vec())
    }

    fn read_test_value(db: &fjall::Database) -> Vec<u8> {
        read_value(db, "backup-test", "key").unwrap()
    }

    #[test]
    fn restores_live_stores() {
        let dir = tempfile::tempdir().unwrap();
        let source_root = dir.path().join("source");
        let source = open_source(&source_root);
        let file = dir.path().join("node.backup");
        std::fs::write(&file, backup_bytes(&source_root, &source)).unwrap();

        let target = dir.path().join("restored");
        let stats = restore_backup(&file, &target).unwrap();
        assert_eq!(stats.created_unix_seconds, 11);

        assert!(!staging_left(dir.path()), "no staging directory is left");
        let main = OptimisticTxDatabase::builder(&target).open().unwrap();
        assert_eq!(read_test_value(main.inner()), b"main");
        let queued = read_value(main.inner(), CHECK_QUEUE_KEYSPACE, "photos");
        assert_eq!(queued.as_deref(), Some(&b""[..]));
        assert!(read_value(main.inner(), STORAGE_CHECK_KEYSPACE, "photos").is_some());
        let sync = OptimisticTxDatabase::builder(target.join("document-sync"))
            .open()
            .unwrap();
        irokle::FjallStorage::from_database(sync.clone()).unwrap();
        assert_eq!(read_test_value(sync.inner()), b"sync");
        let metadata = CraqleNode::open(target.join("craqle")).unwrap();
        assert_eq!(read_test_value(&metadata.store_database()), b"metadata");

        let repo = target.join("git/one.git");
        assert_eq!(
            std::fs::read_to_string(repo.join("HEAD")).unwrap(),
            "ref: refs/heads/main\n"
        );
        assert_eq!(
            std::fs::read_to_string(repo.join("refs/heads/main")).unwrap(),
            "abc\n"
        );
        assert!(repo.join("objects").is_dir());
        assert!(!repo.join("index.lock").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_cut_stream() {
        use super::backup_file;
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let source_root = dir.path().join("source");
        let source = open_source(&source_root);
        let bytes = backup_bytes(&source_root, &source);
        let socket = dir.path().join("backup.sock");
        let listener = UnixListener::bind(&socket).unwrap();

        for (name, len) in [
            ("cut.backup", bytes.len() / 2),
            ("full.backup", bytes.len()),
        ] {
            let target = dir.path().join(name);
            let result = std::thread::scope(|scope| {
                let server = scope.spawn(|| {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream.write_all(&bytes[..len]).unwrap();
                });
                let result = backup_file(&socket, &target);
                server.join().unwrap();
                result
            });
            assert!(!dir.path().join(format!("{name}.partial")).exists());
            if len == bytes.len() {
                assert_eq!(result.unwrap().0, bytes.len() as u64);
                assert_eq!(std::fs::read(&target).unwrap(), bytes);
                let mode = std::fs::metadata(&target).unwrap().permissions().mode();
                assert_eq!(mode & 0o777, 0o600);
            } else {
                assert!(result.is_err());
                assert!(!target.exists());
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn keeps_raced_target() {
        use super::{SnapshotError, backup_file};
        use std::io::Write;
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let source_root = dir.path().join("source");
        let bytes = backup_bytes(&source_root, &open_source(&source_root));
        let socket = dir.path().join("backup.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let target = dir.path().join("raced.backup");
        let result = std::thread::scope(|scope| {
            let server = scope.spawn(|| {
                let (mut stream, _) = listener.accept().unwrap();
                // Another writer takes the destination while the backup is received.
                std::fs::write(&target, "kept").unwrap();
                stream.write_all(&bytes).unwrap();
            });
            let result = backup_file(&socket, &target);
            server.join().unwrap();
            result
        });
        assert!(matches!(result, Err(SnapshotError::SnapshotPathExists(_))));
        assert_eq!(std::fs::read(&target).unwrap(), b"kept");
        let names: Vec<_> = (std::fs::read_dir(dir.path()).unwrap())
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            names.iter().all(|name| !name.ends_with(".partial")),
            "{names:?}"
        );
    }

    /// Whether a restore left a staging directory in `dir`.
    fn staging_left(dir: &Path) -> bool {
        std::fs::read_dir(dir).unwrap().any(|entry| {
            let name = entry.unwrap().file_name();
            name.to_string_lossy().starts_with(".restore-")
        })
    }

    // A target that appeared after the check survives, a crashed restore's leftover blocks
    // nothing, and the restored directory stays owner-only.
    #[cfg(unix)]
    #[test]
    fn restore_never_replaces() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        std::fs::create_dir(&staging).unwrap();
        let raced = dir.path().join("raced");
        std::fs::create_dir(&raced).unwrap();
        let refused = super::publish_dir(&staging, &raced);
        assert!(matches!(refused, Err(super::SnapshotError::TargetPathExists(_))));
        assert!(staging.is_dir() && raced.is_dir());

        let source_root = dir.path().join("source");
        let bytes = backup_bytes(&source_root, &open_source(&source_root));
        let file = dir.path().join("node.backup");
        std::fs::write(&file, bytes).unwrap();
        let target = dir.path().join("restored");
        std::fs::create_dir(dir.path().join("restored.partial")).unwrap();
        restore_backup(&file, &target).unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn rejects_unsafe_paths() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SnapshotWriter::new(Vec::new(), 1).unwrap();
        writer.begin_keyspace(GIT_KEYSPACE).unwrap();
        writer.entry(b"../escaped", b"data").unwrap();
        writer.end_keyspace().unwrap();
        let file = dir.path().join("crafted.backup");
        std::fs::write(&file, writer.finish().unwrap().0).unwrap();

        let target = dir.path().join("restored");
        assert!(restore_backup(&file, &target).is_err());
        assert!(!dir.path().join("escaped").exists());
        assert!(!target.exists());
    }

    #[test]
    fn rejects_cut_backup() {
        let dir = tempfile::tempdir().unwrap();
        let source_root = dir.path().join("source");
        let source = open_source(&source_root);
        let bytes = backup_bytes(&source_root, &source);
        let file = dir.path().join("cut.backup");
        std::fs::write(&file, &bytes[..bytes.len() - 1]).unwrap();

        let target = dir.path().join("restored");
        assert!(restore_backup(&file, &target).is_err());
        assert!(!target.exists());
        assert!(!staging_left(dir.path()), "no staging directory is left");
    }
}
