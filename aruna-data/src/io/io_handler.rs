use crate::io::messages::{MessageType, ReplicationMessage};
use crate::util::bao_tree::{
    FuturesAsyncReaderWrapper, OpenDalWriter, RecvStreamWrapper, SendStreamWrapper,
};
use crate::util::hash::Hasher;
use crate::util::opendal::{get_data_async_reader, get_data_stream, get_reader};
use anyhow::anyhow;
use aruna_net::Kademlia;
use aruna_net::actor_handle::{NetworkActorHandle, ReceiveStreams};
use aruna_permission::manager::{PermissionManager, ResourceId};
use aruna_permission::paths::{Path, RealmKey};
use aruna_storage::storage::store::Store;
use bao_tree::io::fsm::{CreateOutboard, decode_ranges, encode_ranges_validated};
use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::round_up_to_chunks;
use bao_tree::{BaoTree, BlockSize, ByteRanges};
use bytes::{Bytes, BytesMut};
use futures::{AsyncWriteExt, StreamExt};
use iroh::NodeAddr;
use opendal::{FuturesBytesStream, Operator};
use s3s::StdError;
use s3s::stream::ByteStream;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::ops::Range;
use std::sync::Arc;
use tracing::{debug, error, trace};
use ulid::Ulid;

pub const REPLICATION_PROTOCOL_ID: u32 = 2;
pub const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16);
pub const ACCESS_DB_NAME: &str = "user_access";
pub const LOCATION_DB_NAME: &str = "locations";
pub const PATH_LOCATION_DB_NAME: &str = "path_locations_mapping";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub sha256: String,
    pub md5: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub id: Ulid,
    pub staging: bool,
    pub compressed: bool,
    pub encrypted: bool,
    pub file_path: String,
    pub file_size: u64,
    pub file_hashes: Hashes,
}

#[derive(Debug, Clone)]
#[derive(Clone)]
pub struct IOHandler<St>
where
    for<'a> St: Store<'a> + 'static,
{
    node_addr: NodeAddr,
    pub operator: Operator,
    network: NetworkActorHandle,
    kademlia: Kademlia,
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
        skip(node_addr, operator, network, kademlia, store, permission_manager)
    )]
    pub async fn new(
        node_addr: NodeAddr,
        operator: Operator,
        network: NetworkActorHandle,
        kademlia: Kademlia,
        store: Arc<St>,
        permission_manager: PermissionManager,
        realm_key: RealmKey,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let repl_handler = Arc::new(Self {
            node_addr,
            operator,
            network,
            kademlia,
            store,
            permission_manager,
            realm_key,
        });

        repl_handler.clone().run().await;
        Ok(repl_handler)
    }

    fn get_node_addr(&self) -> NodeAddr {
        self.node_addr.clone()
    }

    pub async fn run(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle incoming streams
                    Ok(inc_stream) = self.network.receive() => {
                        let self_clone = self.clone();

                        trace!("Received stream from: {:?}", inc_stream.sender);
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
        if let MessageType::InitReplicationRequest { path, size, root } = &message.msg_type {
            // Handle replication init message
            let response = self.handle_replication_init(message.id, path).await?;
            response.send(&mut send_stream).await?;

            // Decode chunks and write to backend
            let rx_wrapper = RecvStreamWrapper(recv_stream);
            let mut writer = OpenDalWriter {
                writer: self.operator.writer(path).await?.into_futures_async_write(),
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
            //TODO: Store frontend path?
            //TODO: Store permissions?
            let hashes = writer.hasher.finalize()?;
            let blake3_clone = hashes.blake3.clone();
            let location = ObjectInfo {
                id: Ulid::new(),
                staging: true,
                compressed: false, //TODO
                encrypted: false,  //TODO
                file_path: path.to_string(),
                file_size: *size,
                file_hashes: hashes,
            };

            self.store_location(&location, None, None)?;
            self.publish_hash(blake3_clone).await?
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
        message_id: Ulid,
        path: &str,
        //message: ReplicationMessage,
    ) -> Result<ReplicationMessage, StdError> {
        //TODO: Validate conditions to accept replication request
        //  - User has write permissions      | 
        //  - Backend path is not occupied    | DONE
        //  - Backend has enough storage left | 
        if self.operator.exists(path).await? {
            return Ok(ReplicationMessage {
                id: message_id,
                sender: self.get_node_addr(),
                msg_type: MessageType::InitReplicationResponse {
                    ack: false,
                    reason: Some("Path is already occupied".to_string()),
                },
            })
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
        frontend_path: String,
        backend_path: String, // Should already be in final format
        mut data_stream: S,
    ) -> Result<ObjectInfo, StdError>
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + Unpin + 'static,
    {
        let mut hasher = Hasher::new();
        let mut written_bytes = 0;
        let mut writer = self
            .operator
            .writer_with(&backend_path)
            .chunk(BLOCK_SIZE.bytes())
            .await?
            .into_futures_async_write();

        // Fetch stream chunks
        while let Some(bytes) = data_stream.next().await {
            let bytes = bytes?;

            // Write first, then update
            writer.write_all(&bytes).await?;
            hasher.update(&bytes);
            written_bytes = written_bytes + bytes.len();
        }
        writer.flush().await?;
        writer.close().await?;

        // Store some technical metadata in persistence
        let hashes = hasher.finalize()?;
        let blake3_hash = hashes.blake3.clone();
        let info = ObjectInfo {
            id: Ulid::new(),
            staging: true,
            compressed: false, //TODO
            encrypted: false,  //TODO
            file_path: backend_path.clone(),
            file_size: written_bytes as u64,
            file_hashes: hashes,
        };
        debug!("New Object: {:#?}", info);

        let store_clone = self.store.clone();
        let info_clone = info.clone();
        tokio::task::spawn_blocking(move || {
            let mut write_txn = store_clone.create_txn(true)?;

            // Store object info with blake3 hash as key
            store_clone.put(
                &mut write_txn,
                LOCATION_DB_NAME,
                blake3_hash.as_bytes(), //&info.id.to_bytes(),
                &bincode::serde::encode_to_vec(info_clone, bincode::config::standard())?,
            )?;

            // Store mapping of frontend path <--> blake3 hash
            store_clone.put(
                &mut write_txn,
                PATH_LOCATION_DB_NAME,
                frontend_path.as_bytes(),
                blake3_hash.as_bytes(),
            )?;
            store_clone.commit(write_txn)?;

            Ok::<(), anyhow::Error>(())
        });

        // Publish content hash location via Kademlia
        self.kademlia
            .store(*blake3_hash.as_bytes(), self.node_addr.clone(), None)
            .await?;

        Ok(info)
    }

    pub async fn register_backend_data(&self, path: &str, group: Ulid) -> anyhow::Result<String> {
        // Check if backend path resource exists
        self.operator.exists(path).await?;
        let meta = self.operator.stat(path).await?;

        let mut hasher = Hasher::new();
        let mut reader =
            get_data_stream(&self.operator, path, None, Some(BLOCK_SIZE.bytes())).await?;

        while let Some(bytes) = reader.next().await {
            hasher.update(&bytes?);
        }
        let hashes = hasher.finalize()?;

        //TODO: Register resource in permission manager
        let perm_path = aruna_permission::paths::PathBuilder::new()
            .realm_id(self.realm_key) 
            .group_data(
                group,
                "bucket".to_string(), //TODO
                "key".to_string(), //TODO
                hashes.blake3,
            ) 
            .build()?;

        // Register resource
        let location = ObjectInfo {
            id: Ulid::new(),
            staging: false,
            compressed: false,
            encrypted: false,
            file_path: path.to_string(),
            file_size: meta.content_length(),
            file_hashes: hashes.clone(),
        };

        let mut manager = PermissionManager::new().await?;
        let store_clone = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let mut write_txn = store_clone.create_txn(true)?;
            manager.add_resource(
                ResourceId::Ulid(location.id),
                &perm_path,
                store_clone.as_ref(),
                &mut write_txn,
            )?;

            // Store content hash --> location
            store_clone.put(
                &mut write_txn,
                LOCATION_DB_NAME,
                hashes.blake3.as_bytes(), //&info.id.to_bytes(),
                &bincode::serde::encode_to_vec(location, bincode::config::standard())?,
            )?;

            //TODO: Store frontend path ?
            store_clone.commit(write_txn)?;

            Ok::<(), anyhow::Error>(())
        })
        .await??;

        //TODO: Store mapping of frontend path <--> blake3 hash ?

        // Publish content hash location via Kademlia
        self.kademlia
            .store(*hashes.blake3.as_bytes(), self.node_addr.clone(), None)
            .await?;

        Ok("".to_string())
    }

    pub async fn read_data(operator: &Operator, path: &str) -> anyhow::Result<FuturesBytesStream> {
        let reader = get_reader(operator, path, Some(4 * 1024 * 1024), None).await?;
        Ok(reader.into_bytes_stream(..).await?)
    }

    pub async fn read_data_range(
        operator: &Operator,
        path: &str,
        range: Range<u64>,
    ) -> anyhow::Result<FuturesBytesStream> {
        let reader = get_reader(&operator, path, Some(4 * 1024 * 1024), None).await?;
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
        // - Write (transformed) data to dest_location --> ???/<RealmId>/<GroupId>/uploads/<Hash>
        // - Delete data at temp_location
        Ok(())
    }

    pub async fn bao_tree_replicate(
        &self,
        replication_id: Ulid,
        replication_node: NodeAddr,
        object_info: &ObjectInfo,
    ) -> anyhow::Result<()> {
        // Get data stream
        let stream = get_data_async_reader(
            &self.operator,
            &object_info.file_path,
            None, //Some(8),
            Some(BLOCK_SIZE.bytes()),
        )
        .await?;
        let mut stream_wrapper = FuturesAsyncReaderWrapper {
            stream,
            info: object_info.clone(),
        };
        debug!("Fetched data stream from backend.");
        let mut ob = PreOrderOutboard::<BytesMut>::create(&mut stream_wrapper, BLOCK_SIZE).await?;

        // Send replication init
        let (mut sx, mut rx) = self.network.create_stream(replication_node.node_id).await?;
        let init_request = ReplicationMessage {
            id: replication_id,
            sender: self.get_node_addr(),
            msg_type: MessageType::InitReplicationRequest {
                path: object_info.file_path.clone(),
                size: object_info.file_size,
                root: ob.root,
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
        encode_ranges_validated(stream_wrapper, &mut ob, &ranges, &mut sx_wrapper).await?;
        debug!("Sent all chunks.");

        Ok(())
    }

    async fn store_location(
        &self,
        location: &ObjectInfo,
        frontend_path: Option<String>,
        permission_path: Option<Path>,
    ) -> anyhow::Result<()> {
        let data_hash = location.file_hashes.blake3.as_bytes();

        // Store object info with blake3 hash as key
        let mut write_txn = self.store.create_txn(true)?;
        self.store.put(
            &mut write_txn,
            LOCATION_DB_NAME,
            data_hash,
            &bincode::serde::encode_to_vec(location.clone(), bincode::config::standard())?,
        )?;

        // If provided store frontend path mapping
        if let Some(path) = frontend_path {
            self.store.put(
                &mut write_txn,
                PATH_LOCATION_DB_NAME,
                path.as_bytes(),
                data_hash,
            )?;
        }

        // If provided store permission path
        if let Some(path) = permission_path {
            self.permission_manager.add_resource(
                ResourceId::Ulid(location.id),
                &path,
                self.store.as_ref(),
                &mut write_txn,
            )?
        }

        self.store.commit(write_txn)?;
        Ok(())
    }
}
