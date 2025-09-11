use crate::config::config::BackendConfig;
use crate::error::ArunaDataError;
use crate::io::io_handler::tables::{
    BUCKET_STATE_DB_NAME, LOCATION_DB_NAME, LOCATION_STATS_DB_NAME, MULTIPART_DB_NAME,
    PATH_LOCATION_DB_NAME,
};
use crate::io::messages::{MessageType, ReplicationMessage};
use crate::network::network_handler::NetworkHandler;
use crate::util::bao_tree::{OpenDalWriter, RecvStreamWrapper};
use crate::util::hash::Hasher;
use crate::util::opendal::{
    Backend, create_paths, get_backend_operator, get_data_stream, get_reader,
};
use crate::util::s3::{ReplicationRule, ReplicationTask, make_bucket};
use crate::{REPLICATION_RULES_DB_NAME, logerr};
use aruna_net::actor_handle::ReceiveStreams;
use aruna_permission::manager::PermissionManager;
use aruna_permission::paths::{Path, PathBuilder, RealmKey};
use aruna_permission::{ResourceId, TokenSystem, UserIdentity};
use aruna_storage::storage::store::Store;
use bao_tree::io::fsm::decode_ranges;
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, BlockSize, ByteRanges};
use bytes::{Bytes, BytesMut};
use futures::{AsyncWriteExt, StreamExt};
use iroh::NodeAddr;
use opendal::{FuturesBytesStream, Operator};
use parking_lot::RwLock;
use s3s::StdError;
use s3s::stream::ByteStream;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, trace, warn};
use ulid::Ulid;

pub const REPLICATION_PROTOCOL_ID: u32 = 2;
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); //2^16 bytes
pub const SPECIAL_BUCKET: &'static str = "objects";

pub mod tables {
    pub const ACCESS_DB_NAME: &str = "user_access";
    pub const LOCATION_DB_NAME: &str = "locations";
    pub const LOCATION_STATS_DB_NAME: &str = "location_stats";
    pub const PATH_LOCATION_DB_NAME: &str = "path_locations_mapping";
    pub const BUCKET_STATE_DB_NAME: &str = "bucket_states";
    pub const BUCKET_LOCATION_DB_NAME: &str = "bucket_locations_mapping";
    pub const REPLICATION_RULES_DB_NAME: &str = "replication_rules";
    pub const MULTIPART_DB_NAME: &str = "multipart";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub sha256: String,
    pub md5: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub bucket: String,
    pub key: String,
    pub created_by: UserIdentity,
    pub created_at: SystemTime,
    pub staging: bool,
    pub compressed: bool,
    pub encrypted: bool,
    pub partial: bool, // Indicates wether object is partially synced or not
    // Ingested resources that exist at the root level
    // of the storage backend have empty storage root
    pub storage_root: String,
    pub storage_path: String,
    pub file_size: u64,
    pub file_hashes: Hashes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketState {
    pub id: Ulid,
    pub created_by: UserIdentity,
    pub created_at: SystemTime,
    pub name: String,

    // TODO:
    // - Create MerkleTree for bucket for syncing buckets
    // pub merkle_tree: MerkleTree,
    pub merkle_tree: BTreeSet<String>,   // None if empty
    pub root_hash: Option<blake3::Hash>, // None if empty
    pub backend_bucket: Option<String>,
    pub backend_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub id: Ulid,
    pub parts: Vec<(String, String, Hashes)>, // Storage paths (bucket, key, hashes)
    pub finished: bool,
    pub frontend_path: Option<String>,

    // ObjectInfo part
    pub bucket: String,
    pub key: String,
    pub created_by: UserIdentity,
    pub created_at: SystemTime,
    pub staging: bool,
    pub compressed: bool,
    pub encrypted: bool,
    pub partial: bool,
    pub storage_root: String,
    pub storage_path: String,
}

#[derive(Clone)]
pub struct IOHandler<St>
where
    for<'a> St: Store<'a> + 'static,
{
    backend: BackendConfig,
    backend_stats: Arc<RwLock<BTreeMap<String, u64>>>,
    pub store: St,
    pub permission_manager: PermissionManager,
    pub token_handler: Arc<RwLock<TokenSystem>>,
}

impl<St> IOHandler<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(
        level = "trace",
        skip(backend, store, permission_manager, token_handler)
    )]
    pub async fn new(
        backend: BackendConfig,
        store: St,
        permission_manager: PermissionManager,
        token_handler: Arc<RwLock<TokenSystem>>,
    ) -> Result<Arc<Self>, ArunaDataError> {
        let io_handler = Arc::new(Self {
            backend,
            backend_stats: Arc::new(RwLock::new(Self::load_stats(&store)?)),
            store,
            permission_manager,
            token_handler,
        });

        Ok(io_handler)
    }

    fn load_stats<S>(store: &S) -> Result<BTreeMap<String, u64>, ArunaDataError>
    where
        for<'a> S: Store<'a> + 'static,
    {
        let read_txn = store.create_txn(false)?;
        let list = store.iter_db(&read_txn, LOCATION_STATS_DB_NAME)?;
        Ok(list
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8(k.to_vec()).unwrap(),
                    u64::from_be_bytes(v.as_ref().try_into().unwrap()),
                )
            })
            .collect::<BTreeMap<String, u64>>())
    }

    pub async fn eval_suitable_bucket(&self) -> Result<String, ArunaDataError> {
        if let Some((bucket, _)) = self
            .backend_stats
            .read()
            .iter()
            //.inspect(|ele| debug!("{:#?}", ele))
            .find(|(_, v)| *v < &self.backend.max_bucket_size)
        {
            Ok(bucket.to_string())
        } else {
            // Make bucket
            let bucket_name = if let Some((bucket, _)) = self.backend_stats.read().last_key_value()
            {
                let parts = bucket.split('-').collect::<Vec<_>>();
                if parts.len() != 3 {
                    return Err(ArunaDataError::ServerError(
                        "Invalid backend bucket name".to_string(),
                    ));
                }
                format!(
                    "aruna-{}-{}",
                    Ulid::from_string(parts[1])?.to_string().to_lowercase(),
                    parts[1]
                        .parse::<u64>()
                        .map_err(|_e| ArunaDataError::ConversionError {
                            from: "&str".to_string(),
                            to: "u64".to_string()
                        })?
                        + 1
                )
            } else {
                format!("aruna-{}-1", Ulid::new().to_string().to_lowercase())
            };

            if self.backend.backend_type == Backend::S3 {
                // Try create bucket in backend if S3
                make_bucket(bucket_name.clone(), self.backend.access_config.clone()).await?;
            }

            Ok(bucket_name)
        }
    }

    fn modify_bucket_occupancy<'a>(
        &'a self,
        bucket: &str,
        increase: bool,
        mut txn: &mut <St as Store<'a>>::Txn,
    ) -> Result<(), ArunaDataError> {
        // Write changes to database and "cache"
        if let Some(val_bytes) = self
            .store
            .get(txn, LOCATION_STATS_DB_NAME, bucket.as_ref())?
        {
            let mut occupancy = u64::from_be_bytes(val_bytes.as_ref().try_into()?);
            if increase {
                occupancy += 1;
            } else if occupancy > 0 {
                occupancy -= 1;
            }
            self.store.put(
                &mut txn,
                LOCATION_STATS_DB_NAME,
                bucket.as_ref(),
                &occupancy.to_be_bytes(),
            )?
        }

        // Make changes in "cache"
        let mut lock = self.backend_stats.write();
        if let Some(occupancy) = lock.get_mut(bucket) {
            if increase {
                *occupancy += 1;
            } else if *occupancy > 0u64 {
                *occupancy -= 1;
            }
        } else {
            tracing::warn!("Bucket {bucket} does not exist in stats.")
        }

        Ok(())
    }

    //Note: Currently P2P streams are only used by replication
    #[tracing::instrument(level = "trace", skip(self, _sender, send_stream, recv_stream))]
    pub async fn handle_incoming_p2p_stream(
        &self,
        ReceiveStreams {
            sender: _sender,
            mut send_stream,
            mut recv_stream,
        }: ReceiveStreams,
        self_addr: NodeAddr,
        realm_key: RealmKey,
    ) -> Result<blake3::Hash, StdError> {
        trace!("Handle Replication @ {}", self_addr.node_id);
        // Read init message
        let message = ReplicationMessage::read(&mut recv_stream).await?;
        let hash = if let MessageType::InitReplicationRequest {
            user_id,
            group_id,
            bucket,
            key,
            partial,
            permission_path,
            size,
            root,
        } = &message.msg_type
        {
            // Create backend storage operator
            let backend_bucket = self.eval_suitable_bucket().await?;
            let operator = get_backend_operator(
                &self.backend.backend_type,
                self.backend.access_config.clone(),
                &backend_bucket,
            )
            .await?;

            let bucket_name = extract_frontend_bucket(bucket)?;
            if !partial {
                let self_clone = self.clone();
                let bucket_clone = bucket.clone();
                let bucket_name_clone = bucket_name.clone();
                let user_id_clone = user_id.clone();
                let backend_bucket_clone = backend_bucket.clone();

                let (bucket_path, backend_path) =
                    create_paths(None, &bucket_name, &group_id, false)?;
                let bucket_path = bucket_path.ok_or_else(|| {
                    ArunaDataError::ServerError("No bucket frontend path set".to_string())
                })?;
                let permission = PathBuilder::new()
                    .realm_id(realm_key)
                    .group_data_bucket(*group_id, bucket_name.clone())
                    .build()?;
                tokio::task::spawn_blocking(move || {
                    let mut wtxn = self_clone.store.create_txn(true)?;

                    // Try fetch content hash associated with frontend bucket
                    let res = self_clone.store.get(
                        &wtxn,
                        BUCKET_STATE_DB_NAME,
                        bucket_clone.as_bytes(),
                    )?;

                    if let None = res {
                        let state = BucketState {
                            created_by: user_id_clone,
                            created_at: SystemTime::now(),
                            id: Ulid::new(),
                            name: bucket_name_clone,
                            merkle_tree: BTreeSet::new(),
                            root_hash: None,
                            backend_bucket: Some(backend_bucket_clone.clone()),
                            backend_path: Some(backend_path.clone()),
                        };
                        self_clone.store_bucket(state, bucket_path, permission, &mut wtxn)?;
                    };

                    self_clone.store.commit(wtxn)?;

                    Ok::<(), ArunaDataError>(())
                })
                .await??;
            }

            // Create paths
            let (frontend_path, backend_path) =
                create_paths(Some(key), &bucket_name, group_id, false)?;

            // Handle replication init message
            let response = self
                .handle_replication_init(&operator, message.id, &backend_path, self_addr.clone())
                .await?;
            response.send(&mut send_stream).await?;

            // Decode chunks and write to backend
            let rx_wrapper = RecvStreamWrapper(recv_stream);
            let mut writer = OpenDalWriter {
                writer: operator
                    .writer(&backend_path)
                    .await?
                    .into_futures_async_write(),
                len: *size,
                hasher: Hasher::new(),
            };

            let mut ob = PreOrderOutboard {
                tree: BaoTree::new(*size, BLOCK_SIZE),
                root: *root,
                data: BytesMut::new(),
            };

            let ranges = ByteRanges::from(0..*size);
            let ranges = round_up_to_chunks(&ranges);

            debug!("Try to decode chunks received from bidi stream");
            decode_ranges(rx_wrapper, ranges, &mut writer, &mut ob).await?;
            writer.writer.close().await?;
            debug!("Decoded all chunks and wrote them into the backend");

            // Store & publish location + register permission
            let hashes = writer.hasher.finalize()?;
            let location = ObjectInfo {
                bucket: bucket.clone(),
                key: key.clone(),
                partial: *partial,
                created_by: user_id.to_owned(),
                created_at: SystemTime::now(),
                staging: true,
                compressed: false,
                encrypted: false,
                storage_root: backend_bucket.clone(),
                storage_path: backend_path.clone(),
                file_size: *size,
                file_hashes: hashes,
            };
            let self_clone = self.clone();
            let path = permission_path.clone();
            let current_span = tracing::Span::current();
            let self_addr_clone = self_addr.node_id.clone();
            tokio::task::spawn_blocking(move || {
                current_span.in_scope(|| {
                    trace!("Store replicated object @ {}", self_addr_clone);
                    let self_clone = self_clone;
                    let mut wtxn = self_clone.store.create_txn(true)?;

                    self_clone.store_location(location, frontend_path, Some(path), &mut wtxn)?;

                    self_clone.store.commit(wtxn)?;

                    Ok::<(), ArunaDataError>(())
                })
            })
            .await??;

            root
        } else {
            error!("Stream did not start with init message");
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                "Stream did not start with init message",
            )));
        };

        send_stream.finish()?;
        debug!("Handled stream.");
        Ok(*hash)
    }

    async fn handle_replication_init(
        &self,
        operator: &Operator,
        message_id: Ulid,
        path: &str,
        self_addr: NodeAddr,
        //message: ReplicationMessage,
    ) -> Result<ReplicationMessage, StdError> {
        //TODO: Validate conditions to accept replication request
        //  - User has write permissions      |
        //  - Backend path is not occupied    | DONE
        //  - Backend has enough storage left |
        if operator.exists(path).await? {
            return Ok(ReplicationMessage {
                id: message_id,
                sender: self_addr,
                msg_type: MessageType::InitReplicationResponse {
                    ack: false,
                    reason: Some("Path is already occupied".to_string()),
                },
            });
        }

        Ok(ReplicationMessage {
            id: message_id,
            sender: self_addr,
            msg_type: MessageType::InitReplicationResponse {
                ack: true,
                reason: None,
            },
        })
    }

    pub async fn handle_incoming_data_stream<S>(
        &self,
        user_identity: UserIdentity,
        realm_key: RealmKey,
        group_id: Ulid,
        frontend_path: String,
        backend_path: String,
        mut data_stream: S,
    ) -> Result<(blake3::Hash, ObjectInfo), ArunaDataError>
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + Unpin + 'static,
    {
        // Create backend storage operator
        let backend_bucket = self.eval_suitable_bucket().await?;
        let operator = get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            &backend_bucket,
        )
        .await?;

        // Handle incoming data stream
        let mut hasher = Hasher::new();
        let mut written_bytes = 0;
        let mut writer = operator
            .writer_with(&backend_path)
            .chunk(BLOCK_SIZE.bytes())
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))?
            .into_futures_async_write();

        // Fetch stream chunks
        while let Some(bytes) = data_stream.next().await {
            let bytes = bytes.map_err(|e| ArunaDataError::ServerError(e.to_string()))?;

            // Write first, then update hashes and written bytes
            writer.write_all(&bytes).await?;
            hasher.update(&bytes);
            written_bytes = written_bytes + bytes.len();
        }
        writer.flush().await?;
        writer.close().await?;

        // Store some technical metadata in persistence and publish content-hash
        let hashes = hasher.finalize()?;
        let blake3_hash = hashes.blake3.clone();
        let frontend_bucket_name = extract_frontend_bucket(&frontend_path)?;
        let bucket = format!("{}/{}", group_id, frontend_bucket_name);
        let key = frontend_path
            .strip_prefix(&format!("{}/", bucket))
            .ok_or_else(|| {
                ArunaDataError::ServerError("Invalid frontend path provided".to_string())
            })?;

        let info = ObjectInfo {
            bucket: bucket.clone(),
            key: key.to_string(),
            partial: false,
            created_by: user_identity,
            created_at: SystemTime::now(),
            staging: true,
            compressed: false,
            encrypted: false,
            storage_root: backend_bucket,
            storage_path: backend_path.clone(),
            file_size: written_bytes as u64,
            file_hashes: hashes,
        };

        let permission_path = PathBuilder::new()
            .realm_id(realm_key)
            .group_data(
                group_id,
                frontend_bucket_name.clone(),
                key.to_string(),
                blake3_hash.clone(),
            )
            .build()?;

        let self_clone = self.clone();
        let info_clone = info.clone();
        tokio::task::spawn_blocking(move || {
            let mut write_txn = self_clone.store.create_txn(true)?;
            self_clone.store_location(
                info_clone,
                Some(frontend_path),
                Some(permission_path),
                &mut write_txn,
            )?;
            self_clone.store.commit(write_txn)?;
            Ok::<(), ArunaDataError>(())
        });

        Ok((blake3_hash, info))
    }

    pub async fn register_backend_data(
        &self,
        user_identity: UserIdentity,
        group: Ulid,
        network: NetworkHandler,
        backend_path: &str,
        bucket: String,
        key: Option<String>,
        _create_frontend_path: bool,
    ) -> Result<String, ArunaDataError> {
        // Create backend storage operator
        let operator = get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            //&backend_path,
            &bucket,
        )
        .await?;

        // Check if backend path resource exists by fetching stats
        let meta = operator.stat(backend_path).await?;

        // Eval frontend path
        let frontend_path = if let Some(key) = &key {
            format!("{bucket}/{key}")
        } else {
            format!("{bucket}/{backend_path}")
        };

        // Read content and calculate hashes in tokio::spawn
        let self_clone = self.clone();
        let network = network.clone();
        let backend_path_clone = backend_path.to_string();
        let frontend_path_clone = frontend_path.clone();
        tokio::spawn(async move {
            let network_clone = network.clone();
            let mut hasher = Hasher::new();
            let mut reader = get_data_stream(
                &operator,
                &backend_path_clone,
                None,
                Some(BLOCK_SIZE.bytes()),
            )
            .await?;

            while let Some(bytes) = reader.next().await {
                hasher.update(&bytes?);
            }
            let hashes = hasher.finalize()?;
            let blake3_hash = hashes.blake3.clone();

            // Store location + Permission
            let key_clone = match key {
                Some(ref key) => key.clone(),
                None => backend_path_clone.clone(),
            };
            let perm_path = PathBuilder::new()
                .realm_id(network.get_realm_key())
                .group_data(group, bucket.clone(), key_clone.clone(), blake3_hash)
                .build()?;

            let self_clone = self_clone.clone();
            let user_id = user_identity.clone();
            let bucket = bucket.clone();
            tokio::task::spawn_blocking(move || {
                let mut write_txn = self_clone.store.create_txn(true)?;
                let bucket_exists: bool = self_clone
                    .store
                    .get(&write_txn, BUCKET_STATE_DB_NAME, bucket.as_bytes())?
                    .is_some();

                if !bucket_exists {
                    let bucket_perm_path = PathBuilder::new()
                        .realm_id(network_clone.get_realm_key())
                        .group_data(
                            group,
                            bucket.clone(),
                            key.unwrap_or(backend_path_clone.clone()),
                            blake3_hash,
                        )
                        .build()?;
                    let bucket_state = BucketState {
                        id: Ulid::new(),
                        created_by: user_id,
                        created_at: SystemTime::now(),
                        name: bucket.clone(),
                        merkle_tree: BTreeSet::new(),
                        root_hash: None,
                        // TODO: Fill these
                        backend_bucket: None,
                        backend_path: None,
                    };
                    self_clone.store_bucket(
                        bucket_state,
                        frontend_path_clone.clone(),
                        bucket_perm_path,
                        &mut write_txn,
                    )?;
                };

                let location = ObjectInfo {
                    bucket: bucket.clone(),
                    key: key_clone.clone(),
                    partial: false,
                    created_by: user_identity,
                    created_at: SystemTime::now(),
                    staging: false,
                    compressed: false,
                    encrypted: false,
                    storage_root: bucket.clone(),
                    storage_path: backend_path_clone.to_string(),
                    file_size: meta.content_length(),
                    file_hashes: hashes,
                };

                self_clone.store_location(
                    location,
                    Some(frontend_path_clone),
                    Some(perm_path),
                    &mut write_txn,
                )?;

                self_clone.store.commit(write_txn)?;

                Ok::<(), ArunaDataError>(())
            })
            .await??;

            // Publish hash in Kademlia
            network.store(blake3_hash).await?;

            Ok::<(), ArunaDataError>(())
        });

        Ok(frontend_path)
    }

    pub async fn read_data(
        operator: &Operator,
        path: &str,
    ) -> Result<FuturesBytesStream, ArunaDataError> {
        let reader = get_reader(operator, path, None, None).await?;
        Ok(reader.into_bytes_stream(..).await?)
    }

    pub async fn read_data_range(
        operator: &Operator,
        path: &str,
        range: Range<u64>,
    ) -> Result<FuturesBytesStream, ArunaDataError> {
        let reader = get_reader(operator, path, None, None).await?;
        Ok(reader.into_bytes_stream(range).await?)
    }

    async fn _move_encode(
        _temp_location: &str,
        _dest_location: &str,
        _compress: bool,
        _encrypt: bool,
    ) -> Result<(), ArunaDataError> {
        //TODO:
        // - Create operators
        // - Write (transformed) data to dest_location --> <group-id>/<random-ulid>
        // - Delete data at temp_location
        Ok(())
    }

    pub fn store_bucket<'a, 'b>(
        &'a self,
        state: BucketState,
        bucket_path: String,
        resource_permission: Path,
        write_txn: &'b mut <St as Store<'a>>::Txn,
    ) -> Result<(), ArunaDataError> {
        // Store object info with blake3 hash as key
        trace!("Create bucket {}", bucket_path);
        self.store.put(
            write_txn,
            BUCKET_STATE_DB_NAME,
            bucket_path.as_bytes(),
            &postcard::to_allocvec(&state)?,
        )?;

        self.permission_manager.add_resource(
            ResourceId::Ulid(state.id),
            &resource_permission,
            &self.store,
            write_txn,
        )?;

        Ok(())
    }

    pub fn store_location<'a, 'b>(
        &'a self,
        location: ObjectInfo,
        frontend_path: Option<String>,
        resource_permission: Option<Path>,
        write_txn: &'b mut <St as Store<'a>>::Txn,
    ) -> Result<(), ArunaDataError> {
        let data_hash = location.file_hashes.blake3.clone();

        // Update bucket
        let bucket = self
            .store
            .get(&write_txn, BUCKET_STATE_DB_NAME, location.bucket.as_bytes())?
            .ok_or_else(|| ArunaDataError::NotFound("Bucket not found".to_string()))?;
        let mut bucket: BucketState = postcard::from_bytes(&bucket)?;
        bucket.merkle_tree.insert(data_hash.to_string());
        bucket.root_hash = Some(blake3::hash(
            bucket
                .merkle_tree
                .clone()
                .into_iter()
                .reduce(|mut list, value| {
                    list.push_str(&value);
                    list
                })
                .ok_or_else(|| {
                    ArunaDataError::ServerError("Could not hash bucket merkle tree".to_string())
                })?
                .as_bytes(),
        ));
        self.store.put(
            write_txn,
            BUCKET_STATE_DB_NAME,
            location.bucket.as_bytes(),
            &postcard::to_allocvec(&bucket)?,
        )?;

        // Create location
        trace!("Storing object with hash {}", data_hash);
        self.store.put(
            write_txn,
            LOCATION_DB_NAME,
            data_hash.as_bytes(),
            &postcard::to_allocvec(&location)?,
        )?;

        // Increase bucket occupancy
        self.modify_bucket_occupancy(&location.storage_root, true, write_txn)?;

        // If provided store frontend path mapping
        if let Some(path) = frontend_path {
            trace!("Writing path {}", path);
            self.store.put(
                write_txn,
                PATH_LOCATION_DB_NAME,
                path.as_bytes(),
                data_hash.as_bytes(),
            )?;
        }

        // If provided store permission path
        if let Some(path) = resource_permission {
            trace!("Writing perm {}", path);
            self.permission_manager.add_resource(
                ResourceId::ContentHash(data_hash),
                &path,
                &self.store,
                write_txn,
            )?
        }
        Ok(())
    }

    #[tracing::instrument(err, skip(self))]
    pub async fn get_operator(&self, storage_root: &str) -> Result<Operator, ArunaDataError> {
        get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            storage_root,
        )
        .await
    }

    #[tracing::instrument(err, skip(self))]
    pub async fn store_replication_rule(
        &self,
        target: NodeAddr,
        realm_key: RealmKey,
        rule: ReplicationRule,
    ) -> Result<Vec<ReplicationTask>, ArunaDataError> {
        let self_clone = self.clone();
        let rule_clone = rule.clone();

        let current_span = tracing::Span::current();
        let tasks: Vec<ReplicationTask> = tokio::task::spawn_blocking(move || {
            current_span.in_scope(|| {
                let mut wtxn = self_clone.store.create_txn(true).map_err(logerr!())?;
                let ReplicationRule {
                    user_id,
                    group_id,
                    source_bucket,
                    source_filter,
                    ..
                } = rule;

                let bucket_state = self_clone
                    .store
                    .get(&wtxn, BUCKET_STATE_DB_NAME, source_bucket.as_bytes())
                    .map_err(logerr!())?
                    .ok_or_else(|| {
                        error!("Bucket {} not found", source_bucket);
                        ArunaDataError::NotFound(format!("Bucket {} not found", source_bucket))
                    })?;
                let bucket_state: BucketState =
                    postcard::from_bytes(&bucket_state).map_err(logerr!())?;

                self_clone.store.put(
                    &mut wtxn,
                    REPLICATION_RULES_DB_NAME,
                    source_bucket.as_bytes(),
                    &postcard::to_allocvec(&rule_clone).map_err(logerr!())?,
                )?;

                let mut collected_tasks: Vec<ReplicationTask> = Vec::new();
                for object_hash in bucket_state.merkle_tree.iter() {
                    let hash = blake3::Hash::from_str(&object_hash).map_err(|e| {
                        error!("{}", e);
                        ArunaDataError::ConversionError {
                            from: "String".to_string(),
                            to: "blake3::Hash".to_string(),
                        }
                    })?;
                    let object_info = self_clone
                        .store
                        .get(&wtxn, LOCATION_DB_NAME, hash.as_bytes())
                        .map_err(logerr!())?
                        .ok_or_else(|| {
                            ArunaDataError::NotFound(format!(
                                "Object with hash {} not found",
                                object_hash
                            ))
                        })?;

                    let object_info: ObjectInfo =
                        postcard::from_bytes(&object_info).map_err(logerr!())?;
                    if let Some(_filter) = &source_filter {
                        warn!("Filters are currently not supported")
                    }

                    let bucket_name =
                        extract_frontend_bucket(&object_info.bucket).map_err(logerr!())?;
                    let permission_path = PathBuilder::new()
                        .realm_id(realm_key)
                        .group_data(
                            group_id,
                            bucket_name,
                            object_info.key.clone(),
                            object_info.file_hashes.blake3,
                        )
                        .build()
                        .map_err(logerr!())?;

                    let replication_task = ReplicationTask {
                        user_id: user_id.clone(),
                        group_id,
                        replication_id: Ulid::new(),
                        replication_node: target.clone(),
                        permission_path,
                        object_info,
                    };

                    collected_tasks.push(replication_task);
                }

                self_clone.store.commit(wtxn).map_err(logerr!())?;

                Ok::<Vec<ReplicationTask>, ArunaDataError>(collected_tasks)
            })
        })
        .await??;
        Ok(tasks)
    }

    pub async fn create_multipart_upload(
        &self,
        multipart_upload: MultipartUpload,
    ) -> Result<(), ArunaDataError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let mut wtxn = store.create_txn(true)?;

            store.put(
                &mut wtxn,
                MULTIPART_DB_NAME,
                multipart_upload.id.to_bytes().as_slice(),
                &postcard::to_allocvec(&multipart_upload)?,
            )?;
            store.commit(wtxn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await??;
        Ok(())
    }

    pub async fn add_part<S>(
        &self,
        multipart_upload: Ulid,
        part_number: usize,
        mut data_stream: S,
    ) -> Result<blake3::Hash, ArunaDataError>
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + Unpin + 'static,
    {
        let backend_bucket = self.eval_suitable_bucket().await?;
        let backend_path = format!("multipart-{}-{}", multipart_upload.to_string(), part_number);

        let operator = get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            &backend_bucket,
        )
        .await?;

        let mut hasher = Hasher::new();
        let mut written_bytes = 0;
        let mut writer = operator
            .writer_with(&backend_path)
            .chunk(BLOCK_SIZE.bytes())
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))?
            .into_futures_async_write();

        // Fetch stream chunks
        while let Some(bytes) = data_stream.next().await {
            let bytes = bytes.map_err(|e| ArunaDataError::ServerError(e.to_string()))?;

            // Write first, then update hashes and written bytes
            writer.write_all(&bytes).await?;
            hasher.update(&bytes);
            written_bytes = written_bytes + bytes.len();
        }
        writer.flush().await?;
        writer.close().await?;

        // Store some technical metadata in persistence and publish content-hash
        let hashes = hasher.finalize()?;
        let blake3_hash = hashes.blake3.clone();

        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let mut wtxn = store.create_txn(true)?;
            let upload_id = multipart_upload.to_bytes();
            let multipart = store
                .get(&wtxn, MULTIPART_DB_NAME, upload_id.as_slice())?
                .ok_or_else(|| {
                    ArunaDataError::NotFound(format!(
                        "Upload with id {} not found",
                        multipart_upload
                    ))
                })?;
            let mut multipart: MultipartUpload = postcard::from_bytes(&multipart)?;
            multipart
                .parts
                .insert(part_number, (backend_bucket, backend_path, hashes));

            store.put(
                &mut wtxn,
                MULTIPART_DB_NAME,
                upload_id.as_slice(),
                &postcard::to_allocvec(&multipart)?,
            )?;

            store.commit(wtxn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await??;
        Ok(blake3_hash)
    }

    pub async fn finish_multipart_upload(
        &self,
        realm_key: RealmKey,
        group_id: Ulid,
        multipart_upload: Ulid,
        parts: Vec<(usize, String)>,
    ) -> Result<(String, String, blake3::Hash), ArunaDataError> {
        let store = self.store.clone();

        let etags = parts
            .iter()
            .map(|(_, etag)| {
                blake3::Hash::from_str(etag).map_err(|_| ArunaDataError::ConversionError {
                    from: "&str".to_string(),
                    to: "blake3::Hash".to_string(),
                })
            })
            .collect::<Result<Vec<blake3::Hash>, ArunaDataError>>()?;
        let multipart = tokio::task::spawn_blocking(move || {
            let mut wtxn = store.create_txn(true)?;

            let upload_id = multipart_upload.to_bytes();
            let multipart = store
                .get(&mut wtxn, MULTIPART_DB_NAME, upload_id.as_slice())?
                .ok_or_else(|| {
                    ArunaDataError::NotFound(format!(
                        "Upload with id {} not found",
                        multipart_upload
                    ))
                })?;
            let mut multipart: MultipartUpload = postcard::from_bytes(&multipart)?;
            multipart
                .parts
                .retain(|(_, _, hashes)| etags.contains(&hashes.blake3));
            multipart.finished = true;

            store.commit(wtxn)?;

            Ok::<MultipartUpload, ArunaDataError>(multipart)
        })
        .await??;

        let mut hasher = Hasher::new();
        let mut written_bytes = 0;

        let operator = get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            &multipart.storage_root,
        )
        .await?;

        let mut writer = operator
            .writer_with(&multipart.storage_path)
            .chunk(BLOCK_SIZE.bytes())
            .await
            .map_err(|e| ArunaDataError::ServerError(e.to_string()))?;

        for (bucket, key, _hashes) in multipart.parts {
            let reader = get_reader(&self.get_operator(&bucket).await?, &key, None, None).await?;
            let buffer = reader.read(..).await?;

            let bytes = buffer.to_bytes();
            let bytes = bytes.iter().as_slice();
            hasher.update(bytes);
            written_bytes = written_bytes + buffer.len();

            writer.write(buffer).await?;
        }
        writer.close().await?;

        let handler = self.clone();
        let bucket_name =
            extract_frontend_bucket(multipart.frontend_path.as_ref().ok_or_else(|| {
                ArunaDataError::NotFound("Frontendpath for multipart upload".to_string())
            })?)?;
        let location = ObjectInfo {
            bucket: multipart.bucket,
            key: multipart.key.clone(),
            created_by: multipart.created_by,
            created_at: multipart.created_at,
            staging: multipart.staging,
            compressed: multipart.compressed,
            encrypted: multipart.encrypted,
            partial: multipart.partial,
            storage_root: multipart.storage_root,
            storage_path: multipart.storage_path,
            file_size: written_bytes as u64,
            file_hashes: hasher.finalize()?,
        };

        let bucket = location.bucket.clone();
        let key = location.key.clone();

        let etag = location.file_hashes.blake3.clone();

        let permission = PathBuilder::new()
            .realm_id(realm_key)
            .group_data(
                group_id,
                bucket_name,
                multipart.key,
                etag,
            )
            .build()?;
        tokio::task::spawn_blocking(move || {
            let mut wtxn = handler.store.create_txn(true)?;

            handler.store_location(
                location,
                multipart.frontend_path,
                Some(permission),
                &mut wtxn,
            )?;

            handler
                .store
                .remove(&mut wtxn, MULTIPART_DB_NAME, &multipart_upload.to_bytes())?;

            handler.store.commit(wtxn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await??;

        Ok((bucket, key, etag))
    }
}

fn extract_frontend_bucket(frontend_path: &str) -> Result<String, ArunaDataError> {
    let parts = frontend_path.split("/").collect::<Vec<&str>>();
    if parts.len() < 2 {
        return Err(ArunaDataError::InvalidParameter {
            name: "frontend_path".to_string(),
            error: format!("{}", frontend_path),
        });
    }
    Ok(parts[1].to_string())
}
