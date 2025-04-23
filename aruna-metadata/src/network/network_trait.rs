use crate::{
    error::ArunaError,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use aruna_net::{ConnectionHandler, ConnectionHandlerBuilder};
use iroh::{NodeAddr, PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, net::SocketAddrV4, sync::Arc};

#[derive(Serialize, Deserialize, Clone)]
pub struct MetadataMessage {
    pub from: [u8; 32],
    pub to: [u8; 32],
    pub subject: [u8; 32],
    pub body: Body,
}

#[derive(Serialize, Deserialize, Clone)]
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
    async fn broadcast(&self, msg: MetadataMessage) -> Result<(), ArunaError>;
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
        Ok(vec![0u8; 32])
    }
    async fn broadcast(&self, msg: MetadataMessage) -> Result<(), ArunaError> {
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
    chandler: Arc<ConnectionHandler>,
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
        let chandler = ConnectionHandlerBuilder::new(config.secret_key)
            .add_bind_addr_v4(config.socket_addr)
            .add_protocol_handler(3, config.persistor)
            .build(config.bootstrap_nodes)
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
            .map(|addr| addr.node_id.as_bytes().to_vec())
            .map_err(|e| ArunaError::NetworkError(e.to_string()))
    }

    async fn broadcast(&self, msg: MetadataMessage) -> Result<(), ArunaError> {
        todo!()
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
    // async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError> {
    //     self.store(key, value).await
    // }
}
