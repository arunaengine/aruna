//! Takes a live backup from a running node's backup socket and restores a backup into a new
//! storage directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use crate::storage::{ImportKeyspaceState, ensure_target_path};
use aruna::backup::{BACKUP_SOCKET, GIT_KEYSPACE, MAIN_PREFIX, METADATA_PREFIX, SYNC_PREFIX};
use aruna_storage::snapshot::{SnapshotError, SnapshotReader, SnapshotRecord, SnapshotStats};
use craqle::{CraqleNode, CraqleOptions, SearchStorage};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, PersistMode};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::{Component, Path, PathBuf};

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
    Ok(())
}

/// Copies the node's backup stream to `<target>.partial`, checks the whole file and only then
/// moves it to `target`, so `target` never holds a cut-off backup.
pub fn backup_file(socket: &Path, target: &Path) -> Result<(u64, SnapshotStats), SnapshotError> {
    if target.exists() {
        return Err(SnapshotError::SnapshotPathExists(target.to_path_buf()));
    }
    let mut partial = target.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&partial)?;
    let result = receive_backup(socket, file, &partial);
    match result {
        Ok(done) => {
            std::fs::rename(&partial, target)?;
            Ok(done)
        }
        Err(error) => {
            let _ = std::fs::remove_file(&partial);
            Err(error)
        }
    }
}

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
    while reader.next_record()?.is_some() {}
    Ok((bytes, reader.stats()))
}

/// Builds a new storage directory from a backup: the node database at `target`, document sync
/// in `document-sync`, the metadata store in `craqle/store` and the Git repositories in `git`.
pub fn restore_backup(backup: &Path, target: &Path) -> Result<SnapshotStats, SnapshotError> {
    ensure_target_path(target)?;
    let mut reader = SnapshotReader::new(BufReader::new(File::open(backup)?))?;
    std::fs::create_dir(target)?;
    let databases = open_targets(target)?;
    let git_root = target.join("git");
    let mut section = None;

    while let Some(record) = reader.next_record()? {
        match record {
            SnapshotRecord::Keyspace(name) => section = Some(open_section(&databases, &name)?),
            SnapshotRecord::Entry { key, value } => match section.as_mut() {
                Some(Section::Store(index, state)) => {
                    state.insert(&databases[*index], key, value)?
                }
                Some(Section::Git) => write_git(&git_root, &key, &value)?,
                None => {}
            },
            SnapshotRecord::EndKeyspace => {
                if let Some(Section::Store(_, mut state)) = section.take() {
                    state.finish()?;
                }
            }
        }
    }
    for database in &databases {
        database.persist(PersistMode::SyncAll)?;
    }
    Ok(reader.stats())
}

enum Section {
    Store(usize, ImportKeyspaceState),
    Git,
}

const PREFIXES: [&str; 3] = [MAIN_PREFIX, SYNC_PREFIX, METADATA_PREFIX];

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

/// Writes one Git entry below `root`; a key ending in `/` is a directory.
fn write_git(root: &Path, key: &[u8], value: &[u8]) -> Result<(), SnapshotError> {
    let relative = std::str::from_utf8(key)
        .map_err(|_| SnapshotError::InvalidStructure("non UTF-8 Git path in backup"))?;
    let path = Path::new(relative);
    if relative.is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return Err(SnapshotError::InvalidStructure("unsafe Git path in backup"));
    }
    let path = root.join(path);
    if relative.ends_with('/') {
        std::fs::create_dir_all(path)?;
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{backup_file, restore_backup};
    use aruna::backup::{BackupSources, GIT_KEYSPACE, write_backup};
    use aruna_storage::snapshot::SnapshotWriter;
    use craqle::CraqleNode;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::io::Write;
    use std::os::unix::net::UnixListener;
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
        };
        let mut bytes = Vec::new();
        write_backup(&mut bytes, &sources, 11).unwrap();
        bytes
    }

    fn read_test_value(db: &fjall::Database) -> Vec<u8> {
        let keyspace = db
            .keyspace("backup-test", KeyspaceCreateOptions::default)
            .unwrap();
        db.snapshot()
            .get(&keyspace, "key")
            .unwrap()
            .unwrap()
            .to_vec()
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

        let main = OptimisticTxDatabase::builder(&target).open().unwrap();
        assert_eq!(read_test_value(main.inner()), b"main");
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

    #[test]
    fn rejects_cut_stream() {
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
            } else {
                assert!(result.is_err());
                assert!(!target.exists());
            }
        }
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
    }
}
