use crate::util::opendal::get_data_stream;
use ahash::{HashMap, HashMapExt};
use anyhow::{Result, anyhow};
use aruna_net::Kademlia;
use aruna_net::actor_handle::NetworkActorHandle;
use futures::{SinkExt, StreamExt};
// TODO: We need to replace the old "ProtocolHandler" trait with a loop that handles incoming streams
use iroh::{NodeAddr, NodeId};
use opendal::{Buffer, BufferSink, Operator};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::fmt::Formatter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

pub const REPLICATION_PROTOCOL_ID: u32 = 2;

pub struct SinkWrapper(BufferSink);
impl std::fmt::Debug for SinkWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferSink").finish()
    }
}

#[derive(Clone)]
pub struct Md5Context(md5::Context);
impl std::fmt::Debug for Md5Context {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context").finish()
    }
}

pub struct ReplicationHandler {
    operator: Operator,
    network: RwLock<Option<(NodeAddr, NetworkActorHandle, Kademlia)>>,
    pub local_store: RwLock<HashMap<String, ObjectInfo>>, //TODO: Persistent store
    pub object_writers: RwLock<HashMap<String, (StagingObjectInfo, SinkWrapper)>>,
}

#[derive(Debug)]
pub struct Hashes {
    blake3: String,
    sha256: String,
    md5: String,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub file_path: String,
    pub file_size: u64,
    pub file_hashes: Hashes,
}

#[derive(Clone, Debug)]
pub struct StagingHashes {
    blake3: blake3::Hasher,
    sha256: sha2::Sha256,
    md5: Md5Context,
}

#[derive(Clone, Debug)]
pub struct StagingObjectInfo {
    pub file_path: String,
    pub bytes_written: u64,
    pub file_hashes: StagingHashes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageType {
    InitReplicationRequest { path: String },
    InitReplicationResponse { ack: bool },
    ReplicationChunkRequest { chunk_nr: u64, chunk: Vec<u8> },
    ReplicationChunkResponse { hash: [u8; 32] },
    EndReplicationRequest,
    EndReplicationResponse { hash: [u8; 32] },
    ReplicationError { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationMessage {
    pub id: Ulid,              // Message ID (for requests and responses)
    pub sender: NodeAddr,      // Sender of this message
    pub msg_type: MessageType, // Type of message with explicit request/response variants
}

impl ReplicationMessage {
    pub fn is_request(&self) -> bool {
        matches!(
            self.msg_type,
            MessageType::InitReplicationRequest { .. }
                | MessageType::ReplicationChunkRequest { .. }
                | MessageType::EndReplicationRequest
        )
    }
}

impl ReplicationHandler {
    pub fn new(operator: Operator) -> Self {
        Self {
            network: RwLock::new(None),
            operator,
            local_store: RwLock::new(HashMap::new()),
            object_writers: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_connection_handler(
        &self,
        node_addr: NodeAddr,
        chandler: NetworkActorHandle,
        kademlia: Kademlia,
    ) {
        self.network
            .write()
            .await
            .replace((node_addr.clone(), chandler, kademlia));
    }

    pub async fn dummy_sync(&self) -> Result<()> {
        let files = self.operator.list_with("/").recursive(true).await?;
        let mut guard = self.local_store.write().await;

        for entry in files {
            let path = std::path::Path::new(entry.path());
            let md5 = entry.metadata().content_md5().unwrap().to_string();
            let info = ObjectInfo {
                file_path: path.to_str().unwrap().to_string(),
                file_size: entry.metadata().content_length(),
                file_hashes: Hashes {
                    blake3: "".to_string(),
                    sha256: "".to_string(),
                    md5: md5.clone(),
                },
            };

            guard.insert(md5, info);
        }

        Ok(())
    }

    async fn get_node_addr(&self) -> NodeAddr {
        let network = self.network.read().await;
        network
            .as_ref()
            .map(|(addr, _, _)| addr.clone())
            .expect("Network not initialized")
    }

    async fn _node_id(&self) -> NodeId {
        self.get_node_addr().await.node_id
    }

    pub async fn handle_message(&self, msg: ReplicationMessage) -> Option<ReplicationMessage> {
        if msg.is_request() {
            match msg.msg_type {
                MessageType::InitReplicationRequest { path } => {
                    self.handle_init_request(msg.id, path).await
                }
                MessageType::ReplicationChunkRequest { chunk_nr, chunk } => {
                    info!("Received chunk request with chunk nr {chunk_nr}");
                    self.handle_chunk_request(msg.id, chunk).await
                }
                MessageType::EndReplicationRequest => {
                    self.handle_end_request(msg.id).await
                }
                _ => None, // Hm.
            }
        } else {
            //TODO: Handle responses
            match msg.msg_type {
                MessageType::EndReplicationResponse { hash } => {
                    self.handle_end_response(msg.sender, hash).await;
                    None
                }
                _ => None,
            }
        }
    }

    /// Handle an incoming init replication message
    pub async fn handle_init_request(
        &self,
        id: Ulid,
        object_path: String,
    ) -> Option<ReplicationMessage> {
        //todo!("Init file in local_store -> temp path ?")
        let writer = self
            .operator
            .writer(&object_path)
            .await
            .unwrap()
            .into_sink(); //TODO: Remove unwrap

        let info = StagingObjectInfo {
            file_path: object_path,
            bytes_written: 0,
            file_hashes: StagingHashes {
                blake3: blake3::Hasher::new(),
                sha256: sha2::Sha256::new(),
                md5: Md5Context(md5::Context::new()),
            },
        };

        //self.local_store.write().await.insert(id.to_string(), info);
        self.object_writers
            .write()
            .await
            .insert(id.to_string(), (info, SinkWrapper(writer)));

        Some(ReplicationMessage {
            id,
            sender: self.get_node_addr().await,
            msg_type: MessageType::InitReplicationResponse { ack: true },
        })
    }

    /// Handle an incoming replication chunk message
    pub async fn handle_chunk_request(
        &self,
        id: Ulid,
        chunk: Vec<u8>,
    ) -> Option<ReplicationMessage> {
        //TODO: Validate chunks
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(chunk.as_slice());
        let chunk_hash = chunk_hasher.finalize();

        let mut guard = self.object_writers.write().await;
        if let Some((info, writer)) = guard.get_mut(&id.to_string()) {
            // Update blake3, sha256 and md5 hash with chunk bytes
            info.file_hashes.blake3.update(chunk.as_slice());
            info.file_hashes.sha256.update(chunk.as_slice());
            info.file_hashes.md5.0.consume(chunk.as_slice());

            // Write chunk into backend
            writer.0.send(Buffer::from(chunk)).await.unwrap();
        }

        // Send response
        Some(ReplicationMessage {
            id,
            sender: self.get_node_addr().await,
            msg_type: MessageType::ReplicationChunkResponse {
                hash: *chunk_hash.as_bytes(),
            },
        })
    }

    /// Handle an incoming end replication message
    pub async fn handle_end_request(&self, id: Ulid) -> Option<ReplicationMessage> {
        let mut guard = self.object_writers.write().await;
        let response = if let Some((staging_info, writer)) = guard.get_mut(&id.to_string()) {
            // Close writer
            writer.0.close().await.unwrap();

            // Finalize hasher
            let blake3 = staging_info.file_hashes.blake3.finalize(); //.as_bytes(); //to_string();
            let sha256 = format!("{:x}", staging_info.file_hashes.sha256.clone().finalize());
            let md5 = format!("{:x}", staging_info.file_hashes.md5.clone().0.compute());

            // Insert object to local store
            self.local_store.write().await.insert(
                blake3.to_string(),
                ObjectInfo {
                    file_path: staging_info.file_path.clone(),
                    file_size: staging_info.bytes_written,
                    file_hashes: Hashes {
                        blake3: blake3.to_string(),
                        sha256,
                        md5,
                    },
                },
            );

            // Store data location in Kademlia
            let guard = self.network.read().await;
            if let Some((node_addr, _, kademlia)) = guard.as_ref() {
                if let Err(err) = kademlia.store(*blake3.as_bytes(), node_addr.clone()).await {
                    return Some(ReplicationMessage {
                        id,
                        sender: node_addr.clone(),
                        msg_type: MessageType::ReplicationError {
                            message: err.to_string(),
                        },
                    });
                }
            }

            ReplicationMessage {
                id,
                sender: self.get_node_addr().await,
                msg_type: MessageType::EndReplicationResponse {
                    hash: *blake3.as_bytes(),
                },
            }
        } else {
            ReplicationMessage {
                id,
                sender: self.get_node_addr().await,
                msg_type: MessageType::ReplicationError {
                    message: "Staging object not found on finish.".to_string(),
                },
            }
        };
        guard.remove(&id.to_string());

        //(TODO: Async move encode?)

        Some(response)
    }

    async fn handle_end_response(&self, sender_addr: NodeAddr, obj_hash: [u8; 32]) {
        // Store data location in Kademlia
        let guard = self.network.read().await;
        if let Some((_, _, kademlia)) = guard.as_ref() {
            if let Err(err) = kademlia.store(obj_hash, sender_addr).await {
                warn!("Failed to store data location: {err}");
            }
        }
    }

    pub fn replicate_object(&self, _replicas: u32) {
        todo!()
        // - Find n closes notes
        // - For each replicate_object_to_node(...)
    }

    pub async fn replicate_object_to_node(
        &self,
        replication_id: Ulid,
        object_path: String,
        replication_node: NodeAddr,
    ) -> Result<[u8; 32]> {
        debug!("Start replication for object: {}", object_path);

        // Get data stream
        let mut stream =
            get_data_stream(&self.operator, &object_path, Some(8), Some(1024 * 1024)).await?;
        debug!("Fetched data stream.");

        // Get connection to specific node
        let guard = self.network.read().await;
        let Some((_, chandler, _)) = guard.as_ref() else {
            return Err(anyhow!("Network not initialized"));
        };

        let (mut sx, mut rx) = chandler.create_stream(replication_node.node_id).await?;
        debug!("Created bidirectional stream.");

        // Send init and wait for ack
        let init_request = ReplicationMessage {
            id: replication_id,
            sender: self.get_node_addr().await,
            msg_type: MessageType::InitReplicationRequest { path: object_path },
        };
        let request_buf = postcard::to_allocvec(&init_request)
            .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;
        sx.write_u32(request_buf.len() as u32).await?;
        sx.write_all(&request_buf).await?;
        sx.flush().await?;
        debug!("Sent replication init message.");

        let len = rx.read_u32().await?;
        debug!("Length of response message: {}", len);
        let mut buf = vec![0; len as usize];
        rx.read_exact(&mut buf).await?;
        let response = postcard::from_bytes::<ReplicationMessage>(&buf)
            .map_err(|e| anyhow!("Failed to deserialize response: {e:#}"))?;

        if let MessageType::InitReplicationResponse { .. } = response.msg_type {
            debug!("Something something replication init.");
        }

        // Send data in chunks over connection handler
        let mut chunk_counter = 0;
        while let Some(bytes) = stream.next().await {
            let bytes = bytes?;
            debug!("Fetched {} bytes from OpenDAL stream", bytes.len());

            let request = ReplicationMessage {
                id: replication_id,
                sender: self.get_node_addr().await,
                msg_type: MessageType::ReplicationChunkRequest {
                    chunk_nr: chunk_counter,
                    chunk: Vec::from(bytes),
                },
            };
            let request_buf = postcard::to_allocvec(&request)
                .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;

            // Send the request
            sx.write_u32(request_buf.len() as u32).await?;
            sx.write_all(&request_buf).await?;

            // Wait for ack?
            let len = rx.read_u32().await?;
            let mut buf = vec![0; len as usize];
            rx.read_exact(&mut buf).await?;
            let response = postcard::from_bytes::<ReplicationMessage>(&buf)
                .map_err(|e| anyhow!("Failed to deserialize response: {e:#}"))?;

            if let MessageType::ReplicationChunkResponse { hash } = response.msg_type {
                info!("Something something chunk response: {:?}", hash);
            } else if let MessageType::ReplicationError { message } = response.msg_type {
                error!("Received replication error: {}", message);
                //TODO: Re-try, rollback,
                break;
            }

            chunk_counter += 1;
        }

        // Finish replication and wait for ack
        let init_request = ReplicationMessage {
            id: replication_id,
            sender: self.get_node_addr().await,
            msg_type: MessageType::EndReplicationRequest {},
        };
        let request_buf = postcard::to_allocvec(&init_request)
            .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;
        sx.write_u32(request_buf.len() as u32).await?;
        sx.write_all(&request_buf).await?;

        let len = rx.read_u32().await?;
        let mut buf = vec![0; len as usize];
        rx.read_exact(&mut buf).await?;
        let response = postcard::from_bytes::<ReplicationMessage>(&buf)
            .map_err(|e| anyhow!("Failed to deserialize response: {e:#}"))?;

        if let MessageType::EndReplicationResponse { hash } = response.msg_type {
            info!("Something something replication finish: {:?}", hash);
            Ok(hash)
        } else if let MessageType::ReplicationError { message } = response.msg_type {
            Err(anyhow!("Received replication error: {message}"))
        } else {
            Err(anyhow!("That should not happen."))
        }
    }
}
