use crate::config::config::BackendConfig;
use crate::error::ArunaDataError;
use crate::io::io_handler::tables::{
    BUCKET_STATE_DB_NAME, LOCATION_DB_NAME, LOCATION_STATS_DB_NAME, PATH_LOCATION_DB_NAME,
};
use crate::io::messages::{MessageType, ReplicationMessage};
use crate::network::network_handler::NetworkHandler;
use crate::util::bao_tree::{OpenDalWriter, RecvStreamWrapper};
use crate::util::hash::Hasher;
use crate::util::opendal::{
    Backend, create_paths, get_backend_operator, get_data_stream, get_reader,
};
use crate::util::s3::make_bucket;
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
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub sha256: String,
    pub md5: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub created_by: UserIdentity,
    pub created_at: SystemTime,
    pub staging: bool,
    pub compressed: bool,
    pub encrypted: bool,
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
    pub async fn handle_incoming_p2p_stream(
        &self,
        ReceiveStreams {
            sender: _sender,
            mut send_stream,
            mut recv_stream,
        }: ReceiveStreams,
        realm_key: RealmKey,
        self_addr: NodeAddr,
    ) -> Result<blake3::Hash, StdError> {
        // Read init message
        let message = ReplicationMessage::read(&mut recv_stream).await?;
        let hash = if let MessageType::InitReplicationRequest {
            user_id,
            group_id,
            bucket,
            key,
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

            // Create paths
            let (frontend_path, backend_path) =
                create_paths(key.as_deref(), bucket, group_id, false)?;

            // Handle replication init message
            let response = self
                .handle_replication_init(&operator, message.id, &backend_path, self_addr)
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
            let (bucket, path) = (bucket.clone(), permission_path.clone());
            tokio::task::spawn_blocking(|| {
                let self_clone = self_clone;
                let mut wtxn = self_clone.store.create_txn(true)?;

                self_clone.store_location(
                    location,
                    bucket,
                    frontend_path,
                    Some(path),
                    &mut wtxn,
                )?;

                self_clone.store.commit(wtxn)?;

                Ok::<(), ArunaDataError>(())
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
        let key = frontend_path
            .strip_prefix(&format!("{}/", group_id))
            .ok_or_else(|| {
                ArunaDataError::ServerError("Invalid frontend path provided".to_string())
            })?;
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
        let info = ObjectInfo {
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

        let frontend_bucket_name = extract_frontend_bucket(&frontend_path)?;
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
                format!("{}/{}", group_id, frontend_bucket_name),
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
            let key_clone = key.clone();
            let perm_path = PathBuilder::new()
                .realm_id(network.get_realm_key())
                .group_data(
                    group,
                    bucket.clone(),
                    key_clone.unwrap_or(backend_path_clone.clone()),
                    blake3_hash,
                )
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
                    bucket,
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
        println!("Create bucket {}", bucket_path);
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
        frontend_bucket: String,
        frontend_path: Option<String>,
        resource_permission: Option<Path>,
        write_txn: &'b mut <St as Store<'a>>::Txn,
    ) -> Result<(), ArunaDataError> {
        println!("Store location");
        let data_hash = location.file_hashes.blake3.clone();

        println!("Frontend bucket {}", frontend_bucket);
        let bucket = self
            .store
            .get(&write_txn, BUCKET_STATE_DB_NAME, frontend_bucket.as_bytes())?
            .ok_or_else(|| ArunaDataError::NotFound("Bucket not found".to_string()))?;
        println!("Here");
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

        println!("There");

        println!("Store location1");
        self.store.put(
            write_txn,
            BUCKET_STATE_DB_NAME,
            frontend_bucket.as_bytes(),
            &postcard::to_allocvec(&bucket)?,
        )?;

        println!("Store location2");
        self.store.put(
            write_txn,
            LOCATION_DB_NAME,
            data_hash.as_bytes(),
            &postcard::to_allocvec(&location)?,
        )?;

        println!("Store location3");
        // Increase bucket occupancy
        self.modify_bucket_occupancy(&location.storage_root, true, write_txn)?;

        println!("Store location4");
        // If provided store frontend path mapping
        if let Some(path) = frontend_path {
            println!("Writing path {}", path);
            self.store.put(
                write_txn,
                PATH_LOCATION_DB_NAME,
                path.as_bytes(),
                data_hash.as_bytes(),
            )?;
        } else {
            println!("No path");
        }

        println!("Store location5");
        // If provided store permission path
        if let Some(path) = resource_permission {
            println!("Writing perm {}", path);
            self.permission_manager.add_resource(
                ResourceId::ContentHash(data_hash),
                &path,
                &self.store,
                write_txn,
            )?
        }
        Ok(())
    }

    pub async fn get_operator(&self, storage_root: &str) -> Result<Operator, ArunaDataError> {
        get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            storage_root,
        )
        .await
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
