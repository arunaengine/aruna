use crate::{
    error::ArunaError,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use aruna_net::{ConnectionHandler, ConnectionHandlerBuilder};
use iroh::{NodeAddr, PublicKey, SecretKey};
use std::{marker::PhantomData, net::SocketAddrV4, sync::Arc};

pub struct Message {
    from: [u8; 32],
    to: [u8; 32],
    subject: [u8; 32],
    body: Body,
}

pub enum Body {
    CreateUser(Vec<u8>),
    CreateObject(Vec<u8>),
    UpdateObject(Vec<u8>),
}

#[async_trait::async_trait]
pub trait Network<P, St, Se>: Sync + Send
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
{
    type Config;
    async fn new(config: Self::Config) -> Self;
    async fn broadcast(&self, msg: Message) -> Result<(), ArunaError>;
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

pub struct NetworkDummy<P, St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
{
    self_id: NodeAddr,
    _phantom: PhantomData<(P, St, Se)>,
}

#[async_trait::async_trait]
impl<St, Se, P> Network<P, St, Se> for NetworkDummy<P, St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
{
    type Config = ();
    async fn new(_config: Self::Config) -> Self {
        NetworkDummy {
            self_id: NodeAddr::new(PublicKey::from_bytes(&[0u8; 32]).unwrap()),
            _phantom: PhantomData,
        }
    }
    async fn broadcast(&self, msg: Message) -> Result<(), ArunaError> {
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

pub struct NetworkConfig<P, St, Se>
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
{
    pub secret_key: Option<SecretKey>,
    pub socket_addr: SocketAddrV4,
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub persistor: Arc<P>,
    pub phantom: PhantomData<(Se, St)>,
}

pub struct P2PNetwork<P, St, Se> {
    chandler: Arc<ConnectionHandler>,
    pub phantom: PhantomData<(P, Se, St)>,
}

#[async_trait::async_trait]
impl<P, St, Se> Network<P, St, Se> for P2PNetwork<P, St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
{
    type Config = NetworkConfig<P, St, Se>;
    async fn new(config: Self::Config) -> Self {
        let chandler = ConnectionHandlerBuilder::new(config.secret_key)
            .add_bind_addr_v4(config.socket_addr)
            .add_protocol_handler(3, config.persistor)
            .build(config.bootstrap_nodes)
            .await
            .unwrap();
        //.map_err(|e| ArunaError::NetworkError(e.to_string()))
        P2PNetwork { chandler, phantom: PhantomData }
    }

    async fn broadcast(&self, msg: Message) -> Result<(), ArunaError> {
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
