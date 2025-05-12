use crate::{
    error::ArunaMetadataError,
    models::requests::{ForwardRequest, ForwardResponse},
    persistence::search::search::Search,
    transactions::controller::Controller,
};
use aruna_net::{actor::NetworkActorBuilder, actor_handle::NetworkActorHandle};
use aruna_storage::storage::store::Store;
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddrV4, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, trace};
use ulid::Ulid;

pub static METADATA_PROTOCOL_ID: u32 = 3;
pub static REPLICATION_POLICY: usize = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
//pub struct MetadataMessage<R: Request> {
pub struct MetadataMessage {
    pub from: [u8; 32],    // Node ID
    pub to: [u8; 32],      // Node ID
    pub subject: [u8; 32], // Object or User ID
    pub body: Body,
    // TODO:
    //pub request: R,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Body {
    Replicate(ReplicationSubject),
    Request {
        token: Option<String>,
        request: ForwardRequest,
    },
    Response {
        result: ForwardResponse,
    },
    Empty,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ReplicationSubject {
    User(Vec<u8>),
    Object(Vec<u8>),
}

#[async_trait::async_trait]
pub trait Network: Sync + Send {
    type Config;
    async fn new(config: Self::Config) -> Self;
    async fn get_id(&self) -> Result<Vec<u8>, ArunaMetadataError>;
    async fn replicate(
        &self,
        subject: ReplicationSubject,
        subject_id: &Ulid,
    ) -> Result<(), ArunaMetadataError>;

    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network;

    async fn forward(
        &self,
        body: Body,
        subject_id: &Ulid,
    ) -> Result<Option<ForwardResponse>, ArunaMetadataError>;
    async fn store(&self, subject_id: &Ulid) -> Result<(), ArunaMetadataError>;
    async fn find_in_realm(&self, subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError>;
    async fn store_in_realm(&self, subject_id: &Ulid) -> Result<(), ArunaMetadataError>;
}

pub struct NetworkDummy {
    self_id: NodeAddr,
}

#[async_trait::async_trait]
impl Network for NetworkDummy {
    type Config = ();
    async fn new(_config: Self::Config) -> Self {
        NetworkDummy {
            self_id: NodeAddr::new(PublicKey::from_bytes(&[0u8; 32]).unwrap()),
        }
    }
    async fn get_id(&self) -> Result<Vec<u8>, ArunaMetadataError> {
        Ok(self.self_id.node_id.as_bytes().to_vec())
    }

    async fn replicate(
        &self,
        _subject: ReplicationSubject,
        _subject_id: &Ulid,
    ) -> Result<(), ArunaMetadataError> {
        Ok(())
    }

    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        _controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        Ok(())
    }

    async fn forward(
        &self,
        _body: Body,
        _subject_id: &Ulid,
    ) -> Result<Option<ForwardResponse>, ArunaMetadataError> {
        Ok(None)
    }
    async fn store(&self, _subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        Ok(())
    }

    async fn find_in_realm(&self, _subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        Ok(vec![self.self_id.clone()])
    }

    async fn store_in_realm(&self, _subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        Ok(())
    }
}

pub struct NetworkConfig {
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
}

pub struct P2PNetwork {
    chandler: NetworkActorHandle,
}

#[async_trait::async_trait]
impl Network for P2PNetwork {
    type Config = NetworkConfig;

    async fn new(config: Self::Config) -> Self {
        let init_handle = NetworkActorBuilder::new(config.secret_key)
            .await
            .add_bind_addr_v4(config.socket_addr)
            .build(config.bootstrap_nodes)
            .await
            .unwrap();

        let chandler = init_handle
            .new_actor_handle(METADATA_PROTOCOL_ID)
            .await
            .unwrap();

        P2PNetwork { chandler }
    }

    async fn start_actor<St, Se, N>(
        self: Arc<Self>,
        controller: Arc<Controller<St, Se, N>>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a> + 'static,
        Se: Search + 'static,
        N: Network,
    {
        let network = self.clone();
        let controller = controller.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = network.dispatch_messages::<St, Se, N>(&controller).await {
                    error!("{err}");
                }
            }
        });
        Ok(())
    }

    async fn get_id(&self) -> Result<Vec<u8>, ArunaMetadataError> {
        self.chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))
            .map(|addr| addr.node_id.as_bytes().to_vec())
    }

    async fn replicate(
        &self,
        subject: ReplicationSubject,
        subject_id: &Ulid,
    ) -> Result<(), ArunaMetadataError> {
        trace!("Calling replicate");
        let chandler = self.chandler.clone();
        let kademlia = chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(async move {
            let node_id = chandler.get_node_addr().await?.node_id;
            trace!("{node_id}");

            // Calc hash
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(subject_id.to_bytes().as_slice());
            let id_hash = chunk_hasher.finalize();
            trace!("{id_hash}");

            // TODO:
            // Dont use find for placing objects,
            // but find realm nodes and place objects there
            let nodes = kademlia.find(*id_hash.as_bytes(), true).await?.nodes;
            trace!("{nodes:?}");

            let mut message = MetadataMessage {
                from: *node_id.as_bytes(),
                to: [0u8; 32],
                subject: *id_hash.as_bytes(),
                body: Body::Replicate(subject),
            };
            trace!("{message:?}");

            // Distribute message to closest nodes
            let mut counter = 0;
            for node in nodes {
                if node.node_id == node_id {
                    continue;
                }
                if counter == REPLICATION_POLICY {
                    break;
                }
                message.to = *node.node_id.as_bytes();
                let msg = postcard::to_allocvec(&message)?;

                let (mut sdx, _recv) = chandler.create_stream(node.node_id).await?;
                sdx.write_u32(msg.len() as u32).await?;
                sdx.write_all(msg.as_slice())
                    .await
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                //chandler.store(*id_hash.as_bytes(), node).await?; // TODO: Move this to handle_stream in persistence
                sdx.finish()
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                counter += 1;

                // TODO:
                // - [] Retries? -> If err, try again, if failed 5 go next node
                // - [] Replicate metadata to specific nodes
                // - [X] Store when create
                // - [] Store when rcv replicate msg
            }
            Ok::<(), ArunaMetadataError>(())
        });
        Ok(())
    }

    async fn forward(
        &self,
        body: Body,
        subject_id: &Ulid,
    ) -> Result<Option<ForwardResponse>, ArunaMetadataError> {
        let node_addr = self.chandler.get_node_addr().await?;

        // Calc hash
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(subject_id.to_bytes().as_slice());
        let id_hash = chunk_hasher.finalize();

        // TODO:
        // Dont use find for placing objects,
        // but find realm nodes and place objects there
        let result = self
            .chandler
            .get_kademlia_actor_handle()
            .await?
            .find(*id_hash.as_bytes(), true)
            .await?;
        if result.value.is_empty() {
            // TODO: Not sure if we need to do this
            // if result.nodes.contains(&node_addr) {
            //     return Ok(true)
            // }
            Ok(None)
        } else {
            if result
                .value
                .iter()
                .map(|addr| addr.addr())
                .any(|addr| addr == &node_addr)
            {
                return Ok(None);
            }

            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(subject_id.to_bytes().as_slice());
            let id_hash = chunk_hasher.finalize();

            // TODO: Choose fastest responding node
            let node = match result.value.first() {
                Some(node) => node.addr().node_id,
                None => return Ok(None),
            };

            let message = MetadataMessage {
                from: *node_addr.node_id.as_bytes(),
                to: *node.as_bytes(),
                subject: *id_hash.as_bytes(),
                body,
            };
            let msg = postcard::to_allocvec(&message)?;
            let (mut sdx, mut recv) = self.chandler.create_stream(node).await?;
            sdx.write_u32(msg.len() as u32).await?;
            sdx.write_all(msg.as_slice())
                .await
                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
            sdx.finish()
                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

            let len = recv.read_u32().await?;
            let mut buf = vec![0; len as usize];
            recv.read_exact(&mut buf)
                .await
                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
            let response = match postcard::from_bytes::<MetadataMessage>(&buf)
                .map_err(|e| {
                    ArunaMetadataError::NetworkError(format!(
                        "Failed to deserialize response: {e:#}"
                    ))
                })?
                .body
            {
                Body::Response { result } => result,
                e @ _ => {
                    return Err(ArunaMetadataError::NetworkError(format!(
                        "Got wrong response {e:?}, expected Body::Response"
                    )));
                }
            };

            Ok(Some(response))
        }
    }

    async fn store(&self, subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        let node_addr = self
            .chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;

        let kademlia = self.chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(async move {
            // Calc hash
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(subject_id.to_bytes().as_slice());
            let id_hash = chunk_hasher.finalize();
            kademlia.store(*id_hash.as_bytes(), node_addr, None).await?;
            Ok::<(), ArunaMetadataError>(())
        });
        Ok(())
    }

    async fn find_in_realm(&self, _subject_id: &Ulid) -> Result<Vec<NodeAddr>, ArunaMetadataError> {
        todo!()
    }

    async fn store_in_realm(&self, _subject_id: &Ulid) -> Result<(), ArunaMetadataError> {
        todo!()
    }
}

impl P2PNetwork {
    pub async fn dispatch_messages<St, Se, N>(
        &self,
        //controller: Arc<Controller<St, Se, N>>,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        let mut recv_stream = self.chandler.receive().await?;
        while let Ok(len) = recv_stream.recv_stream.read_u32().await {
            trace!("Got something");
            let mut buf = vec![0; len as usize];

            // TODO:
            // - dispatch into API requests
            recv_stream
                .recv_stream
                .read_exact(&mut buf)
                .await
                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
            let message = postcard::from_bytes::<MetadataMessage>(&buf).map_err(|e| {
                ArunaMetadataError::NetworkError(format!("Failed to deserialize message: {e:#}"))
            })?;
            trace!("{message:?}");
            match message.body {
                Body::Replicate(replication_subject) => {
                    match controller
                        .persistence
                        .handle_replication(replication_subject)
                        .await
                    {
                        Ok(_res) => {
                            recv_stream
                                .send_stream
                                .finish()
                                .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                        }
                        Err(err) => return Err(err),
                    }
                }
                Body::Request { token, request } => {
                    let result = match request {
                        ForwardRequest::GetResource(req) => Body::Response {
                            result: ForwardResponse::GetResource(
                                controller.request(req, token).await,
                            ),
                        },
                        ForwardRequest::UpdateResource(req) => Body::Response {
                            result: ForwardResponse::UpdateResource(
                                controller.request(req, token).await,
                            ),
                        },
                        ForwardRequest::Search(req) => Body::Response {
                            result: ForwardResponse::Search(controller.request(req, token).await),
                        },
                    };

                    let response = postcard::to_allocvec(&result)?;

                    recv_stream
                        .send_stream
                        .write_u32(response.len() as u32)
                        .await?;
                    recv_stream
                        .send_stream
                        .write_all(&response)
                        .await
                        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                }
                Body::Response { .. } => {
                    // Nothing to do here, there are currently no messages that send responses
                    // after replication/forwarding back
                }
                Body::Empty => {
                    // TODO: Nothing to do here, maybe remove enum variant
                }
            }
        }

        Ok(())
    }
}
