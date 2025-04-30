use crate::{
    error::ArunaError,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store}, transactions::request::Request,
};
use aruna_net::{actor::NetworkActorBuilder, actor_handle::NetworkActorHandle};
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, net::SocketAddrV4, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, trace};
use ulid::Ulid;

pub static METADATA_PROTOCOL_ID: u32 = 3;
pub static REPLICATION_POLICY: usize = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetadataMessage<R: Request> {
    pub from: [u8; 32],    // Node ID
    pub to: [u8; 32],      // Node ID
    pub subject: [u8; 32], // Object or User ID
    pub body: Body,
    // TODO:
    //pub request: R,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Body {
    User(Vec<u8>),
    Object(Vec<u8>),
    Empty,
}

#[async_trait::async_trait]
pub trait Network: Sync + Send {
    type Config;
    async fn new(config: Self::Config) -> Self;
    async fn get_id(&self) -> Result<Vec<u8>, ArunaError>;
    async fn replicate(&self, body: Body, subject_id: &Ulid) -> Result<(), ArunaError>;
    async fn store(&self, subject_id: &Ulid) -> Result<(), ArunaError>;
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
    async fn get_id(&self) -> Result<Vec<u8>, ArunaError> {
        Ok(self.self_id.node_id.as_bytes().to_vec())
    }
    async fn replicate(&self, _body: Body, _subject_id: &Ulid) -> Result<(), ArunaError> {
        Ok(())
    }
    async fn store(&self, _subject_id: &Ulid) -> Result<(), ArunaError> {
        Ok(())
    }
}

pub struct NetworkConfig<St, Se>
where
    for<'a> St: Store<'a>,
    Se: Search,
{
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub persistor: Arc<Persistor<St, Se>>,
}

pub struct P2PNetwork<St, Se> {
    chandler: NetworkActorHandle,
    pub phantom: PhantomData<(Se, St)>,
}

#[async_trait::async_trait]
impl<St, Se> Network for P2PNetwork<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    type Config = NetworkConfig<St, Se>;

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

        let p2p_network = P2PNetwork {
            chandler: chandler.clone(),
            phantom: PhantomData,
        };
        tokio::spawn(async move {
            loop {
                if let Err(err) = p2p_network.start_actor(config.persistor.clone()).await {
                    error!("{err}");
                }
            }
        });
        P2PNetwork {
            chandler,
            phantom: PhantomData,
        }
    }

    async fn get_id(&self) -> Result<Vec<u8>, ArunaError> {
        self.chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaError::NetworkError(e.to_string()))
            .map(|addr| addr.node_id.as_bytes().to_vec())
    }

    async fn replicate(&self, body: Body, subject_id: &Ulid) -> Result<(), ArunaError> {
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
                body,
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
                    .map_err(|e| ArunaError::NetworkError(e.to_string()))?;
                //chandler.store(*id_hash.as_bytes(), node).await?; // TODO: Move this to handle_stream in persistence
                counter += 1;

                // TODO:
                // - [] Retries? -> If err, try again, if failed 5 go next node
                // - [] Replicate metadata to specific nodes
                // - [X] Store when create
                // - [] Store when rcv replicate msg
            }
            Ok::<(), ArunaError>(())
        });
        Ok(())
    }

    async fn store(&self, subject_id: &Ulid) -> Result<(), ArunaError> {
        let node_addr = self
            .chandler
            .get_node_addr()
            .await
            .map_err(|e| ArunaError::NetworkError(e.to_string()))?;

        let kademlia = self.chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(async move {
            // Calc hash
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(subject_id.to_bytes().as_slice());
            let id_hash = chunk_hasher.finalize();
            kademlia.store(*id_hash.as_bytes(), node_addr).await?;
            Ok::<(), ArunaError>(())
        });
        Ok(())
    }
}

impl<St, Se> P2PNetwork<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    async fn start_actor(&self, persistor: Arc<Persistor<St, Se>>) -> Result<(), ArunaError> {
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
                .map_err(|e| ArunaError::NetworkError(e.to_string()))?;
            let message = postcard::from_bytes::<MetadataMessage>(&buf).map_err(|e| {
                ArunaError::NetworkError(format!("Failed to deserialize message: {e:#}"))
            })?;
            trace!("{message:?}");
            match persistor.handle_message(message).await {
                Ok(_res) => {
                    // TODO: Respond with something if need arises
                    //
                    // Serialize the response
                    // let response_buf = postcard::to_allocvec(&response)
                    //     .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;

                    // Send the response
                    // send_stream.write_u32(response_buf.len() as u32).await?;
                    // send_stream.write_all(&response_buf).await?;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }
}
