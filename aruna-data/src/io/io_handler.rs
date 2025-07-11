use crate::config::config::BackendConfig;
use crate::io::io_handler::tables::{
    LOCATION_DB_NAME, LOCATION_STATS_DB_NAME, PATH_LOCATION_DB_NAME,
};
use crate::io::messages::{MessageType, ReplicationMessage};
use crate::util::bao_tree::{
    FuturesAsyncReaderWrapper, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper,
};
use crate::util::hash::Hasher;
use crate::util::opendal::{
    Backend, create_paths, get_backend_operator, get_data_async_reader, get_data_stream, get_reader,
};
use crate::util::s3::make_bucket;
use anyhow::anyhow;
use aruna_net::Kademlia;
use aruna_net::actor_handle::{NetworkActorHandle, ReceiveStreams};
use aruna_permission::manager::PermissionManager;
use aruna_permission::paths::{Path, PathBuilder, RealmKey};
use aruna_permission::{ResourceId, UserIdentity};
use aruna_storage::storage::store::Store;
use bao_tree::io::fsm::{CreateOutboard, decode_ranges, encode_ranges_validated};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, BlockSize, ByteRanges};
use bytes::{Bytes, BytesMut};
use futures::{AsyncWriteExt, StreamExt};
use iroh::{NodeAddr, NodeId};
use opendal::{FuturesBytesStream, Operator};
use parking_lot::RwLock;
use s3s::StdError;
use s3s::stream::ByteStream;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, trace};
use ulid::Ulid;

pub const REPLICATION_PROTOCOL_ID: u32 = 2;
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); //2^16 bytes

pub mod tables {
    pub const ACCESS_DB_NAME: &str = "user_access";
    pub const LOCATION_DB_NAME: &str = "locations";
    pub const LOCATION_STATS_DB_NAME: &str = "location_stats";
    pub const PATH_LOCATION_DB_NAME: &str = "path_locations_mapping";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub sha256: String,
    pub md5: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    created_by: UserIdentity,
    created_at: SystemTime,
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

#[derive(Clone)]
pub struct IOHandler<St>
where
    for<'a> St: Store<'a> + 'static,
{
    node_addr: NodeAddr,
    network: NetworkActorHandle,
    kademlia: Kademlia,
    backend: BackendConfig,
    backend_stats: Arc<RwLock<BTreeMap<String, u64>>>,
    pub store: Arc<St>,
    pub permission_manager: PermissionManager,
    realm_key: RealmKey,
}

impl<St> IOHandler<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(
        level = "trace",
        skip(node_addr, backend, network, kademlia, store, permission_manager)
    )]
    pub async fn new(
        node_addr: NodeAddr,
        network: NetworkActorHandle,
        kademlia: Kademlia,
        backend: BackendConfig,
        store: Arc<St>,
        permission_manager: PermissionManager,
        realm_key: RealmKey,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let io_handler = Arc::new(Self {
            node_addr,
            backend,
            backend_stats: Arc::new(RwLock::new(Self::load_stats(store.as_ref())?)),
            network,
            kademlia,
            store,
            permission_manager,
            realm_key,
        });

        io_handler.clone().run().await;
        Ok(io_handler)
    }

    fn load_stats<S>(store: &S) -> anyhow::Result<BTreeMap<String, u64>>
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

    fn get_node_addr(&self) -> NodeAddr {
        self.node_addr.clone()
    }

    fn _get_node_id(&self) -> NodeId {
        self.node_addr.node_id
    }

    async fn eval_suitable_bucket(&self) -> anyhow::Result<String> {
        if let Some((bucket, _)) = self
            .backend_stats
            .read()
            .iter()
            .find(|(_, v)| *v < &self.backend.max_bucket_size)
        {
            Ok(bucket.to_string())
        } else {
            // Make bucket
            let bucket_name = if let Some((bucket, _)) = self.backend_stats.read().last_key_value()
            {
                let parts = bucket.split('-').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(anyhow::anyhow!("Invalid backend bucket name"));
                }
                format!("aruna-{}", parts[1].parse::<u64>()? + 1)
            } else {
                "aruna-1".to_string()
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
    ) -> anyhow::Result<()> {
        // Write changes to database and "cache"
        if let Some(val_bytes) =
            self.store
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

    pub async fn run(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle incoming streams
                    Ok(inc_stream) = self.network.receive() => {
                        let self_clone = self.clone();

                        trace!("{:?} Received stream from: {:?}", self.node_addr, inc_stream.sender);
                        tokio::spawn(
                            async move {
                                if let Err(e) = self_clone.handle_incoming_p2p_stream(
                                    inc_stream,
                                ).await{
                                    error!("Failed to handle incoming stream: {e:#}");
                                }
                            }
                        );
                    }

                    // Check for shutdown signal
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        });
    }

    //Note: Currently P2P streams are only used by replication
    pub async fn handle_incoming_p2p_stream(
        &self,
        ReceiveStreams {
            sender: _sender,
            mut send_stream,
            mut recv_stream,
        }: ReceiveStreams,
    ) -> Result<(), StdError> {
        // Read init message
        let message = ReplicationMessage::read(&mut recv_stream).await?;
        if let MessageType::InitReplicationRequest {
            user_id,
            group_id,
            path,
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
            let (fp, bp) = if let Some(path) = path {
                create_paths(path, None, group_id, false)?
            } else {
                // Randomized path if no custom path is provided
                (None, format!("{}/{}", group_id, Ulid::new()))
            };

            // Handle replication init message
            let response = self
                .handle_replication_init(&operator, message.id, &bp)
                .await?;
            response.send(&mut send_stream).await?;

            // Decode chunks and write to backend
            let rx_wrapper = RecvStreamWrapper(recv_stream);
            let mut writer = OpenDalWriter {
                writer: operator.writer(&bp).await?.into_futures_async_write(),
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
            let blake3_hash = hashes.blake3.clone();
            let location = ObjectInfo {
                created_by: user_id.to_owned(),
                created_at: SystemTime::now(),
                staging: true,
                compressed: false,
                encrypted: false,
                storage_root: backend_bucket.clone(),
                storage_path: bp.clone(),
                file_size: *size,
                file_hashes: hashes,
            };
            let perm_path = PathBuilder::new()
                .realm_id(self.realm_key)
                .group_data(*group_id, backend_bucket, bp, blake3_hash)
                .build()?;
            self.store_location(&location, fp, Some(perm_path))?;
            self.publish_hash(blake3_hash).await?
        } else {
            error!("Stream did not start with init message");
            return Err(Box::new(std::io::Error::new(
                ErrorKind::InvalidData,
                "Stream did not start with init message",
            )));
        }

        send_stream.finish()?;
        debug!("Handled stream.");
        Ok(())
    }

    async fn handle_replication_init(
        &self,
        operator: &Operator,
        message_id: Ulid,
        path: &str,
        //message: ReplicationMessage,
    ) -> Result<ReplicationMessage, StdError> {
        //TODO: Validate conditions to accept replication request
        //  - User has write permissions      |
        //  - Backend path is not occupied    | DONE
        //  - Backend has enough storage left |
        if operator.exists(path).await? {
            return Ok(ReplicationMessage {
                id: message_id,
                sender: self.get_node_addr(),
                msg_type: MessageType::InitReplicationResponse {
                    ack: false,
                    reason: Some("Path is already occupied".to_string()),
                },
            });
        }

        Ok(ReplicationMessage {
            id: message_id,
            sender: self.get_node_addr(),
            msg_type: MessageType::InitReplicationResponse {
                ack: true,
                reason: None,
            },
        })
    }

    pub async fn handle_incoming_data_stream<S>(
        &self,
        user_identity: UserIdentity,
        group_id: Ulid,
        frontend_path: String,
        backend_path: String,
        mut data_stream: S,
    ) -> Result<ObjectInfo, StdError>
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + Unpin + 'static,
    {
        // Create backend storage operator
        let backend_bucket = self.eval_suitable_bucket().await?;
        let frontend_bucket = extract_frontend_bucket(&frontend_path)?;
        let key = frontend_path
            .strip_prefix(&format!("{}/", group_id))
            .ok_or_else(|| anyhow!("Invalid frontend path provided"))?;
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
            .await?
            .into_futures_async_write();

        // Fetch stream chunks
        while let Some(bytes) = data_stream.next().await {
            let bytes = bytes?;

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

        let permission_path = PathBuilder::new()
            .realm_id(self.realm_key)
            .group_data(
                group_id,
                frontend_bucket,
                key.to_string(),
                blake3_hash.clone(),
            )
            .build()?;

        self.store_location(&info, Some(frontend_path), Some(permission_path))?;
        self.publish_hash(blake3_hash).await?;

        Ok(info)
    }

    pub async fn register_backend_data(
        &self,
        user_identity: UserIdentity,
        group: Ulid,
        backend_path: &str,
        bucket: String,
        key: Option<String>,
        _create_frontend_path: bool,
    ) -> anyhow::Result<String> {
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
        let backend_path_clone = backend_path.to_string();
        let frontend_path_clone = frontend_path.clone();
        tokio::spawn(async move {
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
            let perm_path = PathBuilder::new()
                .realm_id(self_clone.realm_key)
                .group_data(
                    group,
                    bucket.clone(),
                    key.unwrap_or(backend_path_clone.clone()),
                    blake3_hash,
                )
                .build()?;

            let location = ObjectInfo {
                created_by: user_identity,
                created_at: SystemTime::now(),
                staging: false,
                compressed: false,
                encrypted: false,
                storage_root: bucket,
                storage_path: backend_path_clone.to_string(),
                file_size: meta.content_length(),
                file_hashes: hashes,
            };

            self_clone.store_location(&location, Some(frontend_path_clone), Some(perm_path))?;

            // Publish hash in Kademlia
            self_clone.publish_hash(blake3_hash).await?;

            Ok::<(), anyhow::Error>(())
        });

        Ok(frontend_path)
    }

    pub async fn read_data(operator: &Operator, path: &str) -> anyhow::Result<FuturesBytesStream> {
        let reader = get_reader(operator, path, None, None).await?;
        Ok(reader.into_bytes_stream(..).await?)
    }

    pub async fn read_data_range(
        operator: &Operator,
        path: &str,
        range: Range<u64>,
    ) -> anyhow::Result<FuturesBytesStream> {
        let reader = get_reader(operator, path, None, None).await?;
        Ok(reader.into_bytes_stream(range).await?)
    }

    async fn _move_encode(
        _temp_location: &str,
        _dest_location: &str,
        _compress: bool,
        _encrypt: bool,
    ) -> anyhow::Result<()> {
        //TODO:
        // - Create operators
        // - Write (transformed) data to dest_location --> <group-id>/<random-ulid>
        // - Delete data at temp_location
        Ok(())
    }

    pub async fn bao_tree_replicate(
        &self,
        user_id: UserIdentity,
        group_id: Ulid,
        replication_id: Ulid,
        replication_node: NodeAddr,
        replication_path: Option<String>,
        object_info: &ObjectInfo,
    ) -> anyhow::Result<()> {
        // Create backend storage operator
        let operator = get_backend_operator(
            &self.backend.backend_type,
            self.backend.access_config.clone(),
            &object_info.storage_root,
        )
        .await?;

        // Get data stream
        let stream = get_data_async_reader(
            &operator,
            &object_info.storage_path,
            None,
            Some(BLOCK_SIZE.bytes()),
        )
        .await?;
        let mut stream_wrapper = FuturesAsyncReaderWrapper {
            stream,
            info: object_info.clone(),
        };
        debug!("Fetched data stream from backend.");
        let mut outboard =
            PreOrderOutboard::<BytesMut>::create(&mut stream_wrapper, BLOCK_SIZE).await?;

        // Send replication init
        let (mut sx, mut rx) = self.network.create_stream(replication_node.node_id).await?;
        let init_request = ReplicationMessage {
            id: replication_id,
            sender: self.get_node_addr(),
            msg_type: MessageType::InitReplicationRequest {
                user_id,
                group_id,
                path: replication_path,
                size: object_info.file_size,
                root: outboard.root,
            },
        };
        init_request.send(&mut sx).await?;
        debug!("Sent replication init message to {:?}", replication_node);

        // Wait for response if replication node accepts
        let response = ReplicationMessage::read(&mut rx).await?;
        if let MessageType::InitReplicationResponse { ack, reason } = response.msg_type {
            if !ack {
                return Err(anyhow!(
                    "Replication node {:?} rejected replication request: {:?}",
                    replication_node,
                    reason.ok_or_else(|| "No reason provided")
                ));
            }
        }

        // Send bao_tree encoded chunks
        let ranges = ByteRanges::from(0..object_info.file_size);
        let ranges = round_up_to_chunks(&ranges);
        debug!("Chunk Ranges: {:#?}", ranges.boundaries());

        let mut sx_wrapper = SendStreamWrapper(sx);
        encode_ranges_validated(stream_wrapper, &mut outboard, &ranges, &mut sx_wrapper).await?;
        debug!("Sent all chunks.");

        Ok(())
    }

    fn store_location(
        &self,
        location: &ObjectInfo,
        frontend_path: Option<String>,
        resource_permission: Option<Path>,
    ) -> anyhow::Result<()> {
        let data_hash = location.file_hashes.blake3.clone();

        // Store object info with blake3 hash as key
        let mut write_txn = self.store.create_txn(true)?;
        self.store.put(
            &mut write_txn,
            LOCATION_DB_NAME,
            data_hash.as_bytes(),
            &postcard::to_allocvec(location)?,
        )?;

        // Increase bucket occupancy
        self.modify_bucket_occupancy(&location.storage_root, true, &mut write_txn)?;

        // If provided store frontend path mapping
        if let Some(path) = frontend_path {
            self.store.put(
                &mut write_txn,
                PATH_LOCATION_DB_NAME,
                path.as_bytes(),
                data_hash.as_bytes(),
            )?;
        }

        // If provided store permission path
        if let Some(path) = resource_permission {
            self.permission_manager.add_resource(
                ResourceId::ContentHash(data_hash),
                &path,
                self.store.as_ref(),
                &mut write_txn,
            )?
        }
        self.store.commit(write_txn)?;

        Ok(())
    }

    async fn publish_hash(&self, hash: blake3::Hash) -> anyhow::Result<()> {
        self.kademlia
            .store(*hash.as_bytes(), self.get_node_addr(), None)
            .await
    }
}

fn extract_frontend_bucket(frontend_path: &str) -> anyhow::Result<String> {
    let parts = frontend_path.split("/").collect::<Vec<&str>>();
    if parts.len() < 2 {
        return Err(anyhow!("Invalid frontend path: {}", frontend_path));
    }
    Ok(parts[1].to_string())
}
