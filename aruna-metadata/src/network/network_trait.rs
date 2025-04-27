use crate::{
    error::ArunaError,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use aruna_net::{actor::NetworkActorBuilder, actor_handle::NetworkActorHandle};
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, net::SocketAddrV4, sync::Arc};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

pub static METADATA_PROTOCOL_ID: u32 = 3;
pub static REPLICATION_POLICY: usize = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetadataMessage {
    pub from: [u8; 32],
    pub to: [u8; 32],
    pub subject: [u8; 32], //Object or User ID
    pub body: Body,
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
    //async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError>;
    //fn spawn_acceptor(self: Self);
    //async fn get_bidi_stream(
    //    &self,
    //    node_id: NodeId,
    //    protocol_id: ProtocolId,
    //) -> Result<(RecvStream, SendStream), ArunaError>;
    //async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError>;
    //async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError>;
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
    //async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError> {
    //    Ok(self.self_id.clone())
    //}
    //fn spawn_acceptor(self: Self) {}
    //async fn get_bidi_stream(
    //    &self,
    //    node_id: NodeId,
    //    protocol_id: ProtocolId,
    //) -> Result<(RecvStream, SendStream), ArunaError> {
    //    todo!()
    //}
    //async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError> {
    //    Ok(FindResult {
    //        value: vec![self.self_id.clone()],
    //        nodes: vec![self.self_id.clone()],
    //    })
    //}
    //async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError> {
    //    Ok(())
    //}
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
            //.add_protocol_handler(METADATA_PROTOCOL_ID, config.persistor)
            .build(config.bootstrap_nodes)
            .await
            .unwrap();

        let chandler = init_handle
            .new_actor_handle(METADATA_PROTOCOL_ID)
            .await
            .unwrap();

        //.map_err(|e| ArunaError::NetworkError(e.to_string()))
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
        let chandler = self.chandler.clone();
        let kademlia = chandler.get_kademlia_actor_handle().await?;
        let subject_id = *subject_id;
        tokio::spawn(async move {
            let node_id = chandler.get_node_addr().await?.node_id;
            // Calc hash
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(subject_id.to_bytes().as_slice());
            let id_hash = chunk_hasher.finalize();

            // TODO:
            // Dont use find for placing objects,
            // but find realm nodes and place objects there
            let nodes = kademlia.find(*id_hash.as_bytes()).await?.nodes;

            let mut message = MetadataMessage {
                from: *node_id.as_bytes(),
                to: [0u8; 32],
                subject: *id_hash.as_bytes(),
                body,
            };

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
    // async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError> {
    //     self.get_node_addr().await
    // }
    // fn spawn_acceptor(self: Self) {
    //     ConnectionHandler::spawn_acceptor(self);
    // }
    // async fn get_bidi_stream(
    //     &self,
    //     node_id: NodeId,
    //     protocol_id: ProtocolId,
    // ) -> Result<(RecvStream, SendStream), ArunaError> {
    //     self.get_bidi_stream(node_id, protocol_id).await
    // }
    // async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError> {
    //     self.find(key).await
    // }
}
