use std::{net::SocketAddrV4, sync::Arc};

use aruna_net::{
    ConnectionHandler, ConnectionHandlerBuilder, FindResult, connection_handler::ProtocolId,
};
use iroh::{
    NodeAddr, NodeId, PublicKey, SecretKey,
    endpoint::{RecvStream, SendStream},
};
use crate::error::ArunaError;

#[async_trait::async_trait]
pub trait Network: Sync + Send {
    type Config;
    async fn new(config: Self::Config) -> Self;
    async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError>;
    fn spawn_acceptor(self: Self);
    async fn get_bidi_stream(
        &self,
        node_id: NodeId,
        protocol_id: ProtocolId,
    ) -> Result<(RecvStream, SendStream), ArunaError>;
    async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError>;
    async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError>;
}

pub struct NetworkDummy {
    self_id: NodeAddr,
}

#[async_trait::async_trait]
impl Network for NetworkDummy {
    type Config = ();
    async fn new(config: Self::Config) -> Self {
        NetworkDummy {
            self_id: NodeAddr::new(PublicKey::from_bytes(&[0u8; 32]).unwrap()),
        }
    }
    async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError> {
        Ok(self.self_id.clone())
    }
    fn spawn_acceptor(self: Self) {}
    async fn get_bidi_stream(
        &self,
        node_id: NodeId,
        protocol_id: ProtocolId,
    ) -> Result<(RecvStream, SendStream), ArunaError> {
        todo!()
    }
    async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError> {
        Ok(FindResult {
            value: vec![self.self_id.clone()],
            nodes: vec![self.self_id.clone()],
        })
    }
    async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError> {
        Ok(())
    }
}

pub struct NetworkConfig {
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
}

#[async_trait::async_trait]
impl Network for Arc<ConnectionHandler> {
    type Config = NetworkConfig;
    async fn new(config: Self::Config) -> Self {
        ConnectionHandlerBuilder::new(config.secret_key)
            .add_bind_addr_v4(config.socket_addr)
            .build(config.bootstrap_nodes)
            .await.unwrap()
            //.map_err(|e| ArunaError::NetworkError(e.to_string()))
    }
    async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError> {
        self.get_node_addr().await
    }
    fn spawn_acceptor(self: Self) {
        ConnectionHandler::spawn_acceptor(self);
    }
    async fn get_bidi_stream(
        &self,
        node_id: NodeId,
        protocol_id: ProtocolId,
    ) -> Result<(RecvStream, SendStream), ArunaError> {
        self.get_bidi_stream(node_id, protocol_id).await
    }
    async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError> {
        self.find(key).await
    }
    async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError> {
        self.store(key, value).await
    }
}
