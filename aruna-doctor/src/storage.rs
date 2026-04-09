use crate::error::CliError;
use blake3::Hasher;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

const SNAPSHOT_MAGIC: &[u8] = b"ARUNA_DB_SNAPSHOT";
const SNAPSHOT_VERSION: u16 = 1;
const RECORD_BEGIN_KEYSPACE: u8 = 1;
const RECORD_ENTRY: u8 = 2;
const RECORD_END_KEYSPACE: u8 = 3;
const RECORD_FOOTER: u8 = 4;
const IMPORT_TXN_ENTRY_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStats {
    pub created_at_unix_seconds: u64,
    pub keyspace_count: u64,
    pub entry_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportStats {
    pub snapshot_created_at_unix_seconds: u64,
    pub keyspace_count: u64,
    pub entry_count: u64,
    pub target_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error(transparent)]
    Setup(#[from] aruna::config::SetupError),
    #[error("snapshot path already exists: {0}")]
    SnapshotPathExists(PathBuf),
    #[error("target database path already exists: {0}")]
    TargetPathExists(PathBuf),
    #[error("target database parent directory does not exist: {0}")]
    TargetParentMissing(PathBuf),
    #[error("invalid snapshot magic")]
    InvalidMagic,
    #[error("unsupported snapshot version: {0}")]
    UnsupportedVersion(u16),
    #[error("invalid snapshot structure: {0}")]
    InvalidStructure(&'static str),
    #[error("duplicate keyspace in snapshot: {0}")]
    DuplicateKeyspace(String),
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("snapshot contains invalid UTF-8 keyspace name")]
    InvalidKeyspaceName,
    #[error("snapshot length does not fit in memory: {0}")]
    LengthOverflow(u64),
}

pub async fn snapshot(database_path: String, target_path: String) -> Result<(), CliError> {
    let snapshot_path = PathBuf::from(target_path);

    let stats =
        tokio::task::spawn_blocking(move || snapshot_database(&database_path, &snapshot_path))
            .await
            .map_err(std::io::Error::other)??;

    println!(
        "Snapshot created: keyspaces={}, entries={}, created_at={}",
        stats.keyspace_count, stats.entry_count, stats.created_at_unix_seconds,
    );

    Ok(())
}

pub async fn import(snapshot_path: String, target_path: String) -> Result<(), CliError> {
    let snapshot_path = PathBuf::from(snapshot_path);
    let target_path = PathBuf::from(target_path);

    let stats = tokio::task::spawn_blocking(move || {
        import_snapshot_into_new_database(&snapshot_path, &target_path)
    })
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
    let mut keyspace_names = db.list_keyspace_names();
    keyspace_names.sort();

    let created_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?
        .as_secs();

    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(snapshot_path)?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Hasher::new();
    let snapshot = db.read_tx();

    write_header(&mut writer, created_at_unix_seconds)?;

    let mut keyspace_count = 0_u64;
    let mut entry_count = 0_u64;

    for keyspace_name in keyspace_names {
        let keyspace = db.keyspace(&keyspace_name, KeyspaceCreateOptions::default)?;
        write_begin_keyspace_record(&mut writer, &mut hasher, &keyspace_name)?;

        let mut keyspace_entry_count = 0_u64;
        for entry in snapshot.iter(&keyspace) {
            let (key, value) = entry.into_inner()?;
            write_entry_record(&mut writer, &mut hasher, &key, &value)?;
            keyspace_entry_count += 1;
            entry_count += 1;
        }

        write_end_keyspace_record(&mut writer, &mut hasher, keyspace_entry_count)?;
        keyspace_count += 1;
    }

    let checksum = *hasher.finalize().as_bytes();
    write_footer_record(&mut writer, keyspace_count, entry_count, checksum)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;

    Ok(SnapshotStats {
        created_at_unix_seconds,
        keyspace_count,
        entry_count,
    })
}

pub fn import_snapshot_into_new_database(
    snapshot_path: impl AsRef<Path>,
    target_db_path: impl AsRef<Path>,
) -> Result<ImportStats, SnapshotError> {
    let snapshot_path = snapshot_path.as_ref();
    let target_db_path = target_db_path.as_ref();
    ensure_new_target_path(target_db_path)?;

    let file = File::open(snapshot_path)?;
    let mut reader = BufReader::new(file);
    let snapshot_created_at_unix_seconds = read_header(&mut reader)?;

    let db = OptimisticTxDatabase::builder(target_db_path).open()?;
    let mut hasher = Hasher::new();
    let mut seen_keyspaces = HashSet::new();
    let mut keyspace_state: Option<ImportKeyspaceState> = None;
    let mut keyspace_count = 0_u64;
    let mut entry_count = 0_u64;

    loop {
        let tag = match read_u8_plain(&mut reader) {
            Ok(tag) => tag,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Err(SnapshotError::InvalidStructure("missing footer record"));
            }
            Err(error) => return Err(error.into()),
        };

        match tag {
            RECORD_BEGIN_KEYSPACE => {
                if keyspace_state.is_some() {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered a new keyspace before closing the previous keyspace",
                    ));
                }

                hasher.update(&[tag]);
                let name_bytes = read_length_prefixed_bytes_hashed(&mut reader, &mut hasher)?;
                let name = String::from_utf8(name_bytes)
                    .map_err(|_| SnapshotError::InvalidKeyspaceName)?;

                if !seen_keyspaces.insert(name.clone()) {
                    return Err(SnapshotError::DuplicateKeyspace(name));
                }

                let keyspace = db.keyspace(&name, KeyspaceCreateOptions::default)?;
                keyspace_state = Some(ImportKeyspaceState::new(name, keyspace));
                keyspace_count += 1;
            }
            RECORD_ENTRY => {
                let Some(state) = keyspace_state.as_mut() else {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered an entry outside a keyspace section",
                    ));
                };

                hasher.update(&[tag]);
                let key = read_length_prefixed_bytes_hashed(&mut reader, &mut hasher)?;
                let value = read_length_prefixed_bytes_hashed(&mut reader, &mut hasher)?;

                state.insert(&db, key, value)?;
                state.keyspace_entry_count += 1;
                entry_count += 1;
            }
            RECORD_END_KEYSPACE => {
                let Some(mut state) = keyspace_state.take() else {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered end-of-keyspace outside a keyspace section",
                    ));
                };

                hasher.update(&[tag]);
                let expected_entry_count = read_u64_hashed(&mut reader, &mut hasher)?;
                if state.keyspace_entry_count != expected_entry_count {
                    return Err(SnapshotError::InvalidStructure(
                        "keyspace entry count did not match the section footer",
                    ));
                }

                state.finish()?;
            }
            RECORD_FOOTER => {
                if keyspace_state.is_some() {
                    return Err(SnapshotError::InvalidStructure(
                        "encountered footer before closing the active keyspace",
                    ));
                }

                let expected_keyspace_count = read_u64_plain(&mut reader)?;
                let expected_entry_count = read_u64_plain(&mut reader)?;
                let expected_checksum = read_fixed_bytes_plain::<32>(&mut reader)?;

                if keyspace_count != expected_keyspace_count {
                    return Err(SnapshotError::InvalidStructure(
                        "keyspace count did not match the footer",
                    ));
                }
                if entry_count != expected_entry_count {
                    return Err(SnapshotError::InvalidStructure(
                        "entry count did not match the footer",
                    ));
                }

                let checksum = *hasher.finalize().as_bytes();
                if checksum != expected_checksum {
                    return Err(SnapshotError::ChecksumMismatch);
                }

                ensure_reader_exhausted(&mut reader)?;
                return Ok(ImportStats {
                    snapshot_created_at_unix_seconds,
                    keyspace_count,
                    entry_count,
                    target_path: target_db_path.to_path_buf(),
                });
            }
            _ => return Err(SnapshotError::InvalidStructure("unknown record tag")),
        }
    }
}

struct ImportKeyspaceState {
    #[allow(dead_code)]
    name: String,
    keyspace: OptimisticTxKeyspace,
    keyspace_entry_count: u64,
    pending_txn: Option<fjall::OptimisticWriteTx>,
    pending_txn_entries: usize,
}

impl ImportKeyspaceState {
    fn new(name: String, keyspace: OptimisticTxKeyspace) -> Self {
        Self {
            name,
            keyspace,
            keyspace_entry_count: 0,
            pending_txn: None,
            pending_txn_entries: 0,
        }
    }

    fn insert(
        &mut self,
        db: &OptimisticTxDatabase,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), SnapshotError> {
        if self.pending_txn.is_none() {
            self.pending_txn = Some(db.write_tx()?);
        }

        if let Some(txn) = self.pending_txn.as_mut() {
            txn.insert(self.keyspace.clone(), key, value);
            self.pending_txn_entries += 1;
        }

        if self.pending_txn_entries >= IMPORT_TXN_ENTRY_LIMIT {
            self.commit_pending_txn()?;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<(), SnapshotError> {
        self.commit_pending_txn()
    }

    fn commit_pending_txn(&mut self) -> Result<(), SnapshotError> {
        if let Some(txn) = self.pending_txn.take() {
            let _ = txn.commit()?;
        }
        self.pending_txn_entries = 0;
        Ok(())
    }
}

fn ensure_new_target_path(target_db_path: &Path) -> Result<(), SnapshotError> {
    if target_db_path.exists() {
        return Err(SnapshotError::TargetPathExists(
            target_db_path.to_path_buf(),
        ));
    }

    if let Some(parent) = target_db_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            return Err(SnapshotError::TargetParentMissing(parent.to_path_buf()));
        }
    }

    Ok(())
}

fn write_header(
    writer: &mut BufWriter<File>,
    created_at_unix_seconds: u64,
) -> Result<(), SnapshotError> {
    writer.write_all(SNAPSHOT_MAGIC)?;
    writer.write_all(&SNAPSHOT_VERSION.to_be_bytes())?;
    writer.write_all(&created_at_unix_seconds.to_be_bytes())?;
    Ok(())
}

fn read_header(reader: &mut BufReader<File>) -> Result<u64, SnapshotError> {
    let mut magic = vec![0_u8; SNAPSHOT_MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic.as_slice() != SNAPSHOT_MAGIC {
        return Err(SnapshotError::InvalidMagic);
    }

    let version = read_u16_plain(reader)?;
    if version != SNAPSHOT_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }

    read_u64_plain(reader)
}

fn write_begin_keyspace_record(
    writer: &mut BufWriter<File>,
    hasher: &mut Hasher,
    keyspace_name: &str,
) -> Result<(), SnapshotError> {
    let mut record = Vec::with_capacity(1 + 8 + keyspace_name.len());
    record.push(RECORD_BEGIN_KEYSPACE);
    push_length_prefixed_bytes(&mut record, keyspace_name.as_bytes())?;
    write_payload_record(writer, hasher, &record)
}

fn write_entry_record(
    writer: &mut BufWriter<File>,
    hasher: &mut Hasher,
    key: &[u8],
    value: &[u8],
) -> Result<(), SnapshotError> {
    let mut record = Vec::with_capacity(1 + 16 + key.len() + value.len());
    record.push(RECORD_ENTRY);
    push_length_prefixed_bytes(&mut record, key)?;
    push_length_prefixed_bytes(&mut record, value)?;
    write_payload_record(writer, hasher, &record)
}

fn write_end_keyspace_record(
    writer: &mut BufWriter<File>,
    hasher: &mut Hasher,
    entry_count: u64,
) -> Result<(), SnapshotError> {
    let mut record = Vec::with_capacity(9);
    record.push(RECORD_END_KEYSPACE);
    record.extend_from_slice(&entry_count.to_be_bytes());
    write_payload_record(writer, hasher, &record)
}

fn write_footer_record(
    writer: &mut BufWriter<File>,
    keyspace_count: u64,
    entry_count: u64,
    checksum: [u8; 32],
) -> Result<(), SnapshotError> {
    writer.write_all(&[RECORD_FOOTER])?;
    writer.write_all(&keyspace_count.to_be_bytes())?;
    writer.write_all(&entry_count.to_be_bytes())?;
    writer.write_all(&checksum)?;
    Ok(())
}

fn write_payload_record(
    writer: &mut BufWriter<File>,
    hasher: &mut Hasher,
    record: &[u8],
) -> Result<(), SnapshotError> {
    writer.write_all(record)?;
    hasher.update(record);
    Ok(())
}

fn push_length_prefixed_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SnapshotError> {
    let len = u64::try_from(bytes.len()).map_err(|_| SnapshotError::LengthOverflow(u64::MAX))?;
    buffer.extend_from_slice(&len.to_be_bytes());
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn read_length_prefixed_bytes_hashed(
    reader: &mut BufReader<File>,
    hasher: &mut Hasher,
) -> Result<Vec<u8>, SnapshotError> {
    let len = read_u64_hashed(reader, hasher)?;
    let len = usize::try_from(len).map_err(|_| SnapshotError::LengthOverflow(len))?;
    let mut bytes = vec![0_u8; len];
    reader.read_exact(&mut bytes)?;
    hasher.update(&bytes);
    Ok(bytes)
}

fn read_u8_plain(reader: &mut BufReader<File>) -> Result<u8, std::io::Error> {
    let mut bytes = [0_u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn read_u16_plain(reader: &mut BufReader<File>) -> Result<u16, SnapshotError> {
    let bytes = read_fixed_bytes_plain::<2>(reader)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u64_plain(reader: &mut BufReader<File>) -> Result<u64, SnapshotError> {
    let bytes = read_fixed_bytes_plain::<8>(reader)?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u64_hashed(
    reader: &mut BufReader<File>,
    hasher: &mut Hasher,
) -> Result<u64, SnapshotError> {
    let bytes = read_fixed_bytes_plain::<8>(reader)?;
    hasher.update(&bytes);
    Ok(u64::from_be_bytes(bytes))
}

fn read_fixed_bytes_plain<const N: usize>(
    reader: &mut BufReader<File>,
) -> Result<[u8; N], SnapshotError> {
    let mut bytes = [0_u8; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn ensure_reader_exhausted(reader: &mut BufReader<File>) -> Result<(), SnapshotError> {
    let mut trailing = [0_u8; 1];
    match reader.read(&mut trailing) {
        Ok(0) => Ok(()),
        Ok(_) => Err(SnapshotError::InvalidStructure(
            "snapshot has trailing bytes after the footer",
        )),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{SnapshotError, import_snapshot_into_new_database, snapshot_database};
    use aruna::config::load;
    use aruna_api::server_state::ServerState;
    use aruna_blob::blob::BlobHandler;
    use aruna_core::keyspaces::{
        API_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, NODE_STATE_KEYSPACE,
        REALM_CONFIG_KEYSPACE, REALM_KEYSPACE, S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE,
        S3_VERSION_KEYSPACE, USER_ACCESS_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{Actor, Backend, BackendConfig, BucketInfo, UserIdentity};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_operations::automerge::AutomergeHandle;
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::s3::create_bucket::CreateBucketOperation;
    use aruna_operations::s3::create_user_access::{
        CreateUserAccessConfig, CreateUserAccessOperation, DEFAULT_CREDENTIAL_TTL,
    };
    use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_operations::task_incoming::initialize_task_incoming;
    use aruna_tasks::TaskHandle;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::SystemTime;
    use tempfile::tempdir;
    use tokio_util::io::ReaderStream;
    use ulid::Ulid;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct TestEnvGuard {
        previous: Vec<(String, Option<String>)>,
    }

    impl TestEnvGuard {
        fn set(vars: &[(&str, String)]) -> Self {
            let previous = vars
                .iter()
                .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
                .collect::<Vec<_>>();

            for (key, value) in vars {
                unsafe { std::env::set_var(key, value) };
            }

            Self { previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.previous.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_database_contents() {
        let _guard = env_lock().lock().unwrap();
        let temp = tempdir().unwrap();
        let source_db_path = temp.path().join("source-db");
        let snapshot_source_db_path = temp.path().join("snapshot-source-db");
        let snapshot_path = temp.path().join("backup.aruna");
        let restored_db_path = temp.path().join("restored-db");
        let blob_root = temp.path().join("blob-root");

        let _env = TestEnvGuard::set(&[
            (
                "STORAGE_PATH",
                source_db_path
                    .to_str()
                    .expect("invalid source path")
                    .to_string(),
            ),
            (
                "BLOB_ROOT",
                blob_root.to_str().expect("invalid blob path").to_string(),
            ),
            ("BLOB_BUCKET_PREFIX", "aruna_test".to_string()),
            ("BLOB_MAX_BUCKET_SIZE", "100000".to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:0".to_string()),
            ("S3_PORT", "0".to_string()),
            ("S3_HOST", "localhost".to_string()),
            ("S3_ADDRESS", "127.0.0.1".to_string()),
        ]);

        {
            let (config, storage_handle) = load().await.unwrap();
            let task_handle = TaskHandle::new();
            let net_handle = NetHandle::new(
                NetConfig {
                    bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
                    secret_key: Some(config.net_secret_key.clone()),
                    realm_id: config.realm_id.clone(),
                    bootstrap_nodes: Vec::new(),
                    use_dns_discovery: false,
                },
                storage_handle.clone(),
            )
            .await
            .unwrap();
            let blob_handle = BlobHandler::new(
                BackendConfig {
                    backend_type: Backend::FileSystem,
                    bucket_prefix: config.blob_bucket_prefix.clone(),
                    max_bucket_size: config.blob_max_bucket_size,
                    root: config.blob_root.clone(),
                    service_config: std::collections::HashMap::new(),
                },
                storage_handle.clone(),
                net_handle.clone(),
            )
            .await
            .unwrap();
            let automerge_handle = AutomergeHandle::new(Some(net_handle.clone()));

            let context = Arc::new(DriverContext {
                storage_handle: storage_handle.clone(),
                net_handle: Some(net_handle.clone()),
                blob_handle: Some(blob_handle.clone()),
                automerge_handle: Some(automerge_handle.clone()),
                task_handle: Some(task_handle.clone()),
            });
            initialize_net_incoming(context.clone());
            initialize_task_incoming(context.clone(), task_handle.clone()).await;

            let server_state = ServerState::new(
                context.clone(),
                config.realm_id.clone(),
                config.node_id,
                config.node_capabilities.clone(),
                false,
                None,
            )
            .await;

            let realm_admin = Ulid::new();
            drive(
                CreateRealmOperation::new(CreateRealmConfig {
                    actor: Actor {
                        node_id: config.node_id,
                        user_id: realm_admin,
                        realm_id: config.realm_id.clone(),
                    },
                    realm_description: "Snapshot Test Realm".to_string(),
                }),
                context.as_ref(),
            )
            .await
            .unwrap();

            drive(
                ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                    actor: Actor {
                        node_id: config.node_id,
                        user_id: realm_admin,
                        realm_id: config.realm_id.clone(),
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
                        realm_id: config.realm_id.clone(),
                    },
                    display_name: "Snapshot Test Group".to_string(),
                }),
                context.as_ref(),
            )
            .await
            .unwrap();

            let credentials = drive(
                CreateUserAccessOperation::new(CreateUserAccessConfig {
                    user_identity: UserIdentity {
                        user_id: realm_admin,
                        realm_key: config.realm_id.clone(),
                    },
                    group_id: group.0.group_id,
                    expiry: SystemTime::now() + DEFAULT_CREDENTIAL_TTL,
                    path_restrictions: None,
                    issued_by: *config.node_id.as_bytes(),
                }),
                context.as_ref(),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(!credentials.0.is_empty());
            assert!(!credentials.1.secret.is_empty());

            let bucket_name = "snapshot-bucket".to_string();
            drive(
                CreateBucketOperation::new(
                    bucket_name.clone(),
                    BucketInfo {
                        group_id: group.0.group_id,
                        created_at: SystemTime::now(),
                        created_by: realm_admin,
                    },
                ),
                context.as_ref(),
            )
            .await
            .unwrap()
            .expect("bucket creation returned no result")
            .unwrap();

            let data = b"tiny snapshot object";
            let upload = drive(
                PutObjectOperation::new(PutObjectConfig {
                    user_id: realm_admin,
                    group_id: group.0.group_id,
                    request: PutObjectInput {
                        bucket: bucket_name,
                        key: "hello.txt".to_string(),
                        content_length: Some(data.len() as u64),
                        body: Some(BackendStream::new(ReaderStream::new(&data[..]))),
                    },
                    expected_checksums: vec![],
                    checksum_type: None,
                    exists: false,
                }),
                context.as_ref(),
            )
            .await
            .unwrap()
            .expect("put object returned no result")
            .unwrap();
            assert_eq!(upload.location.blob_size, data.len() as u64);

            drop(server_state);
            drop(context);
            drop(automerge_handle);
            drop(task_handle);
            drop(blob_handle);
            net_handle.shutdown().await;
            drop(net_handle);
            drop(storage_handle);
            drop(config);
        }

        copy_dir_all(&source_db_path, &snapshot_source_db_path).unwrap();

        let before = read_database_contents(&snapshot_source_db_path).unwrap();
        assert!(before.contains_key(NODE_STATE_KEYSPACE));
        assert!(before.contains_key(API_STATE_KEYSPACE));
        assert!(before.contains_key(REALM_KEYSPACE));
        assert!(before.contains_key(REALM_CONFIG_KEYSPACE));
        assert!(before.contains_key(AUTH_KEYSPACE));
        assert!(before.contains_key(GROUP_KEYSPACE));
        assert!(before.contains_key(USER_ACCESS_KEYSPACE));
        assert!(before.contains_key(S3_BUCKET_KEYSPACE));
        assert!(before.contains_key(S3_LOOKUP_KEYSPACE));
        assert!(before.contains_key(S3_VERSION_KEYSPACE));

        let snapshot_stats = snapshot_database(&snapshot_source_db_path, &snapshot_path).unwrap();
        assert!(snapshot_stats.keyspace_count >= 10);
        assert!(snapshot_stats.entry_count >= 10);

        let import_stats =
            import_snapshot_into_new_database(&snapshot_path, &restored_db_path).unwrap();
        assert_eq!(import_stats.keyspace_count, snapshot_stats.keyspace_count);
        assert_eq!(import_stats.entry_count, snapshot_stats.entry_count);

        let after = read_database_contents(&restored_db_path).unwrap();

        assert_eq!(before, after);
    }

    fn read_database_contents(
        path: &std::path::Path,
    ) -> Result<BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>, SnapshotError> {
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
