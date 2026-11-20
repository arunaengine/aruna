//! Writes a database snapshot file and imports such a snapshot into a new database.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use aruna_storage::snapshot::{SnapshotError, SnapshotStats};
use aruna_storage::snapshot::{SnapshotReader, SnapshotRecord, SnapshotWriter};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PersistMode};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const TXN_ENTRY_LIMIT: usize = 1_000;
/// Bytes of keys and values one import transaction holds before it commits, so large values
/// commit in smaller batches than the entry limit alone gives.
const TXN_BYTE_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportStats {
    pub snapshot_created_unix: u64,
    pub keyspace_count: u64,
    pub entry_count: u64,
    pub target_path: PathBuf,
}

pub async fn snapshot(database_path: String, target_path: String) -> Result<(), CliError> {
    let snapshot_path = PathBuf::from(target_path);

    let stats =
        tokio::task::spawn_blocking(move || snapshot_database(&database_path, &snapshot_path))
            .await??;

    println!(
        "Snapshot created: keyspaces={}, entries={}, created_at={}",
        stats.keyspace_count, stats.entry_count, stats.created_unix_seconds,
    );

    Ok(())
}

pub async fn import(snapshot_path: String, target_path: String) -> Result<(), CliError> {
    let snapshot_path = PathBuf::from(snapshot_path);
    let target_path = PathBuf::from(target_path);

    let stats =
        tokio::task::spawn_blocking(move || import_new_database(&snapshot_path, &target_path))
            .await
            .map_err(std::io::Error::other)??;

    println!(
        "Snapshot imported: keyspaces={}, entries={}, target={}",
        stats.keyspace_count,
        stats.entry_count,
        stats.target_path.display(),
    );
    Ok(())
}

pub fn snapshot_database(
    db_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
) -> Result<SnapshotStats, SnapshotError> {
    let snapshot_path = snapshot_path.as_ref();
    if snapshot_path.exists() {
        return Err(SnapshotError::SnapshotPathExists(
            snapshot_path.to_path_buf(),
        ));
    }

    let db = OptimisticTxDatabase::builder(db_path.as_ref()).open()?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(snapshot_path)?;
    let mut writer = SnapshotWriter::new(BufWriter::new(file), unix_now()?)?;
    writer.database(db.inner(), "")?;
    let (writer, stats) = writer.finish()?;
    writer.get_ref().sync_all()?;
    Ok(stats)
}

/// Imports a snapshot into a new database at `target_db_path`. It is built in an owner-only
/// staging folder and published only after the footer checked, so a failed import can retry.
pub fn import_new_database(
    snapshot_path: impl AsRef<Path>,
    target_db_path: impl AsRef<Path>,
) -> Result<ImportStats, SnapshotError> {
    let snapshot_path = snapshot_path.as_ref();
    let target_db_path = target_db_path.as_ref();
    ensure_target_path(target_db_path)?;

    let file = File::open(snapshot_path)?;
    let mut reader = SnapshotReader::new(BufReader::new(file))?;
    // Every error drops the staging directory, which removes it.
    let staging = staging_dir(target_db_path)?;
    import_into(&mut reader, staging.path())?;
    publish_dir(staging.path(), target_db_path)?;
    let _published = staging.keep();
    let stats = reader.stats();
    Ok(ImportStats {
        snapshot_created_unix: stats.created_unix_seconds,
        keyspace_count: stats.keyspace_count,
        entry_count: stats.entry_count,
        target_path: target_db_path.to_path_buf(),
    })
}

/// Writes every record into a new database at `target` and closes it again.
fn import_into(
    reader: &mut SnapshotReader<impl std::io::Read>,
    target: &Path,
) -> Result<(), SnapshotError> {
    let db = OptimisticTxDatabase::builder(target)
        .manual_journal_persist(true)
        .open()?;
    let mut keyspace_state: Option<ImportKeyspaceState> = None;

    while let Some(record) = reader.next_record()? {
        match record {
            SnapshotRecord::Keyspace(name) => {
                let keyspace = db.keyspace(keyspace_name(&name)?, KeyspaceCreateOptions::default)?;
                keyspace_state = Some(ImportKeyspaceState::new(keyspace));
            }
            SnapshotRecord::Entry { key, value } => {
                if let Some(state) = keyspace_state.as_mut() {
                    state.insert(&db, key, value)?;
                }
            }
            SnapshotRecord::EndKeyspace => {
                if let Some(mut state) = keyspace_state.take() {
                    state.finish()?;
                }
            }
        }
    }

    db.persist(PersistMode::Buffer)?;
    Ok(())
}

/// A bare Fjall keyspace name from a snapshot: Fjall asserts a name is nonempty and at most 255
/// bytes, so a damaged one must fail the import instead.
pub(crate) fn keyspace_name(name: &str) -> Result<&str, SnapshotError> {
    if name.is_empty() || name.len() > usize::from(u8::MAX) {
        return Err(SnapshotError::InvalidStructure(
            "invalid keyspace name in snapshot",
        ));
    }
    Ok(name)
}

pub(crate) fn unix_now() -> Result<u64, SnapshotError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?
        .as_secs())
}

/// Commits one keyspace's imported entries in transactions of bounded entries and bytes.
pub(crate) struct ImportKeyspaceState {
    keyspace: OptimisticTxKeyspace,
    pending_txn: Option<fjall::OptimisticWriteTx>,
    pending_txn_entries: usize,
    pending_txn_bytes: usize,
}

impl ImportKeyspaceState {
    pub(crate) fn new(keyspace: OptimisticTxKeyspace) -> Self {
        Self {
            keyspace,
            pending_txn: None,
            pending_txn_entries: 0,
            pending_txn_bytes: 0,
        }
    }

    pub(crate) fn insert(
        &mut self,
        db: &OptimisticTxDatabase,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), SnapshotError> {
        if self.pending_txn.is_none() {
            self.pending_txn = Some(db.write_tx()?.durability(Some(PersistMode::Buffer)));
        }

        if let Some(txn) = self.pending_txn.as_mut() {
            self.pending_txn_bytes += key.len() + value.len();
            txn.insert(self.keyspace.clone(), key, value);
            self.pending_txn_entries += 1;
        }

        if self.pending_txn_entries >= TXN_ENTRY_LIMIT || self.pending_txn_bytes >= TXN_BYTE_LIMIT {
            self.commit_pending_txn()?;
        }

        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), SnapshotError> {
        self.commit_pending_txn()
    }

    fn commit_pending_txn(&mut self) -> Result<(), SnapshotError> {
        if let Some(txn) = self.pending_txn.take() {
            let _ = txn.commit()?;
        }
        self.pending_txn_entries = 0;
        self.pending_txn_bytes = 0;
        Ok(())
    }
}

/// An empty directory beside `target` that only its owner may enter, uniquely named so a crashed
/// run leaves nothing that blocks the next one.
pub(crate) fn staging_dir(target: &Path) -> std::io::Result<tempfile::TempDir> {
    let parent = parent_dir(target);
    let mut builder = tempfile::Builder::new();
    builder.prefix(".restore-");
    #[cfg(unix)]
    builder.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700));
    builder.tempdir_in(parent)
}

/// Moves the finished `staging` directory to `target`, refusing even an empty directory that
/// appeared there meanwhile; Windows refuses to rename over a directory by itself.
pub(crate) fn publish_dir(staging: &Path, target: &Path) -> Result<(), SnapshotError> {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    let moved = {
        use rustix::fs::{CWD, RenameFlags, renameat_with};
        renameat_with(CWD, staging, CWD, target, RenameFlags::NOREPLACE)
            .map_err(std::io::Error::from)
    };
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    let moved = std::fs::rename(staging, target);
    match moved {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(SnapshotError::TargetPathExists(target.to_path_buf()));
        }
        Err(error) => return Err(error.into()),
    }
    // The rename is durable only once the directory that now names the target is synced.
    sync_dir(parent_dir(target))?;
    Ok(())
}

/// The directory that holds `target`, the working directory for a bare name.
fn parent_dir(target: &Path) -> &Path {
    match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Syncs every file and directory below and including `root`, so restored files are durable
/// before they are published; answers how many it synced.
pub(crate) fn sync_tree(root: &Path) -> std::io::Result<usize> {
    let mut synced = 0;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            synced += sync_tree(&entry.path())?;
        } else if file_type.is_file() {
            File::open(entry.path())?.sync_all()?;
            synced += 1;
        }
    }
    sync_dir(root)?;
    Ok(synced + 1)
}

/// Syncs a directory's entries; Windows cannot open a directory to sync it, and needs no sync.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

pub(crate) fn ensure_target_path(target_db_path: &Path) -> Result<(), SnapshotError> {
    if target_db_path.exists() {
        return Err(SnapshotError::TargetPathExists(
            target_db_path.to_path_buf(),
        ));
    }

    if let Some(parent) = target_db_path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        return Err(SnapshotError::TargetParentMissing(parent.to_path_buf()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ImportKeyspaceState, KeyspaceCreateOptions, OptimisticTxDatabase, TXN_BYTE_LIMIT,
        import_new_database, snapshot_database,
    };
    use crate::tests::fixtures::env_lock;
    use aruna_api::server::state::ServerState;
    use aruna_blob::blob::BlobHandler;
    use aruna_config::load;
    use aruna_core::keyspaces::{
        API_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, NODE_STATE_KEYSPACE,
        REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USER_ACCESS_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::testing::EnvGuard;
    use aruna_drivers::driver::{DriverContext, drive, node_routing};
    use aruna_net::NetHandle;
    use aruna_net::config::{DiscoveryMethod, NetConfig, RelayMethod};
    use aruna_operations::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
    use aruna_operations::groups::routing::{BucketRules, SnapshotOperation};
    use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::s3::access::create::{
        CreateUserConfig, CreateUserOperation, DEFAULT_CREDENTIAL_TTL,
    };
    use aruna_operations::s3::bucket::create::CreateBucketOperation;
    use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_storage::snapshot::SnapshotError;
    use aruna_tasks::TaskHandle;
    use fjall::Readable;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use tokio_util::io::ReaderStream;
    use ulid::Ulid;

    // A bare keyspace name Fjall would refuse fails the import instead of panicking, and the
    // failed import leaves nothing at its destination, so it can be retried there.
    #[test]
    fn bad_name_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("imported");
        for name in [String::new(), "k".repeat(256)] {
            let mut writer = aruna_storage::snapshot::SnapshotWriter::new(Vec::new(), 1).unwrap();
            writer.begin_keyspace(&name).unwrap();
            writer.end_keyspace().unwrap();
            let file = dir.path().join("bad.snapshot");
            std::fs::write(&file, writer.finish().unwrap().0).unwrap();
            assert!(import_new_database(&file, &target).is_err());
            assert!(!target.exists(), "a failed import publishes nothing");
        }
        let staged = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(staged, 1, "only the snapshot file is left");
    }

    // Every restored file and directory is synced, each once.
    #[test]
    fn tree_synced() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("one.git/objects")).unwrap();
        std::fs::write(dir.path().join("one.git/HEAD"), b"ref").unwrap();
        std::fs::write(dir.path().join("one.git/objects/pack"), b"data").unwrap();
        assert_eq!(super::sync_tree(dir.path()).unwrap(), 5);
    }

    // Large values commit once their bytes reach the limit, long before the entry limit.
    #[test]
    fn large_values_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = OptimisticTxDatabase::builder(dir.path()).open().unwrap();
        let keyspace = db
            .keyspace("large", KeyspaceCreateOptions::default)
            .unwrap();
        let mut state = ImportKeyspaceState::new(keyspace);
        state.insert(&db, b"a".to_vec(), vec![0; 16]).unwrap();
        assert!(
            state.pending_txn.is_some(),
            "a small value waits for its batch"
        );
        state
            .insert(&db, b"b".to_vec(), vec![0; TXN_BYTE_LIMIT])
            .unwrap();
        assert!(
            state.pending_txn.is_none(),
            "the batch committed on its bytes"
        );
    }

    #[tokio::test]
    async fn snapshot_preserves_data() {
        let _guard = env_lock().lock().await;
        let temp = tempdir().unwrap();
        let source_db_path = temp.path().join("source-db");
        let snapshot_db_path = temp.path().join("snapshot-source-db");
        let snapshot_path = temp.path().join("backup.aruna");
        let restored_db_path = temp.path().join("restored-db");
        let blob_root = temp.path().join("blob-root");

        let _env = EnvGuard::set(&[
            (
                "DATABASE_DIR",
                source_db_path
                    .to_str()
                    .expect("invalid source path")
                    .to_string(),
            ),
            (
                "CONTENT_DIR",
                blob_root.to_str().expect("invalid blob path").to_string(),
            ),
            ("BLOB_BUCKET_PREFIX", "aruna_test".to_string()),
            ("BLOB_MAX_BUCKET_SIZE", "100000".to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("S3_HOST", "127.0.0.1:0".to_string()),
            ("S3_ADDRESS", "127.0.0.1:0".to_string()),
            ("OIDC_PROVIDER_IDS", "".to_string()),
        ]);

        {
            let (config, storage_handle) = load().await.unwrap();
            let task_handle = TaskHandle::new();
            let net_handle = NetHandle::new(
                NetConfig {
                    bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
                    secret_key: Some(config.net_secret_key.clone()),
                    realm_id: config.realm_id,
                    peer_nodes: Vec::new(),
                    discovery_method: DiscoveryMethod::None,
                    relay_method: RelayMethod::None,
                    ..Default::default()
                },
                storage_handle.clone(),
            )
            .await
            .unwrap();
            let blob_handle = BlobHandler::new(
                BackendConfig {
                    backend_type: Backend::FileSystem,
                    bucket_prefix: config.blob_bucket_prefix.clone(),
                    max_bucket_size: config.blob_bucket_size,
                    multipart_bucket: config.blob_multipart_bucket.clone(),
                    root: config.blob_root.clone(),
                    service_config: std::collections::HashMap::new(),
                    timeouts: config.blob_timeout_config(),
                },
                storage_handle.clone(),
                net_handle.clone(),
            )
            .await
            .unwrap();
            let context = Arc::new(DriverContext {
                storage_handle: storage_handle.clone(),
                net_handle: Some(net_handle.clone()),
                blob_handle: Some(blob_handle.clone()),
                graph_handle: None,
                task_handle: Some(task_handle.clone()),
                compute_handle: None,
                outbound_handle: None,
            });
            // No incoming loops are started: snapshot and restore are the
            // subject, and a background writer would conflict the fixture.
            let server_state = ServerState::new(
                context.clone(),
                config.realm_id,
                config.node_id,
                config.node_capabilities.clone(),
                false,
                None,
                aruna_drivers::jobs::runtime::JobsRuntime::new(),
            )
            .await;

            let realm_admin =
                aruna_core::user::id::UserId::local(Ulid::generate(), config.realm_id);
            drive(
                CreateRealmOperation::new(CreateRealmConfig {
                    actor: Actor {
                        node_id: config.node_id,
                        user_id: realm_admin,
                        realm_id: config.realm_id,
                    },
                    realm_description: "Snapshot Test Realm".to_string(),
                    oidc_providers: Vec::new(),
                    node_location: None,
                    node_weight: None,
                    node_labels: Default::default(),
                }),
                context.as_ref(),
            )
            .await
            .unwrap();

            drive(
                ClaimInitialOperation::new(ClaimInitialInput {
                    actor: Actor {
                        node_id: config.node_id,
                        user_id: realm_admin,
                        realm_id: config.realm_id,
                    },
                }),
                context.as_ref(),
            )
            .await
            .unwrap();

            let group = drive(
                CreateGroupOperation::new(CreateGroupConfig {
                    actor: Actor {
                        node_id: config.node_id,
                        user_id: realm_admin,
                        realm_id: config.realm_id,
                    },
                    display_name: "Snapshot Test Group".to_string(),
                    owner_cap: None,
                }),
                context.as_ref(),
            )
            .await
            .unwrap();

            let credentials = drive(
                CreateUserOperation::new(
                    CreateUserConfig {
                        user_identity: realm_admin,
                        group_id: group.0.group_id,
                        expiry: SystemTime::now() + DEFAULT_CREDENTIAL_TTL,
                        path_restrictions: None,
                        issued_by: *config.node_id.as_bytes(),
                    },
                    aruna_core::credential_encryption::CredentialEncryptionKey::random(),
                ),
                context.as_ref(),
            )
            .await
            .unwrap();
            assert!(!credentials.0.is_empty());
            assert!(!credentials.1.expose().is_empty());

            let bucket_name = "snapshot-bucket".to_string();
            drive(
                CreateBucketOperation::new(
                    bucket_name.clone(),
                    BucketInfo {
                        group_id: group.0.group_id,
                        created_at: SystemTime::now(),
                        created_by: realm_admin,
                        cors_configuration: None,
                        storage_routing: Vec::new(),
                        placement_policies: Vec::new(),
                        placement_policy_generation: 0,
                        compression: Compression::Off,
                        distribution: None,
                    },
                ),
                context.as_ref(),
            )
            .await
            .unwrap();

            let data = b"tiny snapshot object";
            let routing = drive(
                SnapshotOperation::new(
                    &node_routing(context.as_ref()),
                    group.0.group_id,
                    BucketRules::Named(bucket_name.to_string()),
                ),
                context.as_ref(),
            )
            .await
            .unwrap();
            let upload = drive(
                PutObjectOperation::new(PutObjectConfig {
                    user_id: realm_admin,
                    group_id: group.0.group_id,
                    realm_id: config.realm_id,
                    node_id: config.node_id,
                    request: PutObjectInput {
                        bucket: bucket_name,
                        key: "hello.txt".to_string(),
                        content_length: Some(data.len() as u64),
                        body: Some(BackendStream::new(ReaderStream::new(&data[..]))),
                    },
                    expected_checksums: vec![],
                    checksum_type: None,
                    version_source: None,
                    preassigned_version_id: None,
                    quota_ceiling: None,
                    exists: false,
                    routing,
                }),
                context.as_ref(),
            )
            .await
            .unwrap();
            assert_eq!(upload.location.blob_size, data.len() as u64);

            drop(server_state);
            drop(context);
            drop(task_handle);
            drop(blob_handle);
            net_handle.shutdown().await;
            drop(net_handle);
            drop(storage_handle);
            drop(config);
        }

        copy_dir_all(&source_db_path, &snapshot_db_path).unwrap();

        let before = read_database_contents(&snapshot_db_path).unwrap();
        assert!(before.contains_key(NODE_STATE_KEYSPACE));
        assert!(before.contains_key(API_STATE_KEYSPACE));
        assert!(before.contains_key(REALM_CONFIG_KEYSPACE));
        assert!(before.contains_key(AUTH_KEYSPACE));
        assert!(before.contains_key(GROUP_KEYSPACE));
        assert!(before.contains_key(USER_ACCESS_KEYSPACE));
        assert!(before.contains_key(S3_BUCKET_KEYSPACE));

        let snapshot_stats = snapshot_database(&snapshot_db_path, &snapshot_path).unwrap();
        assert!(snapshot_stats.keyspace_count >= 10);
        assert!(snapshot_stats.entry_count >= 10);

        let import_stats = import_new_database(&snapshot_path, &restored_db_path).unwrap();
        assert_eq!(import_stats.keyspace_count, snapshot_stats.keyspace_count);
        assert_eq!(import_stats.entry_count, snapshot_stats.entry_count);

        let after = read_database_contents(&restored_db_path).unwrap();

        assert_eq!(before, after);
    }

    type DatabaseContents = Result<BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>, SnapshotError>;

    fn read_database_contents(path: &std::path::Path) -> DatabaseContents {
        let db = OptimisticTxDatabase::builder(path).open()?;
        let snapshot = db.read_tx();
        let mut keyspace_names = db.list_keyspace_names();
        keyspace_names.sort();

        let mut contents = BTreeMap::new();
        for keyspace_name in keyspace_names {
            let keyspace = db.keyspace(&keyspace_name, KeyspaceCreateOptions::default)?;
            let mut entries = Vec::new();
            for entry in snapshot.iter(&keyspace) {
                let (key, value) = entry.into_inner()?;
                entries.push((key.to_vec(), value.to_vec()));
            }
            contents.insert(keyspace_name.to_string(), entries);
        }

        Ok(contents)
    }

    fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let entry_type = entry.file_type()?;
            let target = dst.join(entry.file_name());
            if entry_type.is_dir() {
                copy_dir_all(&entry.path(), &target)?;
            } else {
                std::fs::copy(entry.path(), target)?;
            }
        }

        Ok(())
    }
}
