use std::sync::Arc;

use aruna_net::{FindResult, connection_handler::ProtocolId};
use iroh::{
    NodeAddr, NodeId,
    endpoint::{RecvStream, SendStream},
};
use ulid::Ulid;

use crate::error::ArunaError;

#[async_trait::async_trait]
pub trait Network: Sync + Send {
    type Config;
    fn new(config: Self::Config) -> Self;
    async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError>;
    fn spawn_acceptor(self: Arc<Self>);
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
    type Config = NodeAddr;
    fn new(config: Self::Config) -> Self {
        NetworkDummy {
            self_id: config
        }
    }
    async fn get_node_addr(&self) -> Result<NodeAddr, ArunaError> {
        Ok(self.self_id.clone())
    }
    fn spawn_acceptor(self: Arc<Self>) {
    }
    async fn get_bidi_stream(
        &self,
        node_id: NodeId,
        protocol_id: ProtocolId,
    ) -> Result<(RecvStream, SendStream), ArunaError> {
        todo!()
    }
    async fn find(&self, key: [u8; 32]) -> Result<FindResult, ArunaError> {
        Ok(FindResult { value: vec![self.self_id.clone()], nodes: vec![self.self_id.clone()] })
    }
    async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<(), ArunaError> {
        Ok(())
    }
}
