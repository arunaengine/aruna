use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use iroh::{
    Endpoint, NodeAddr, NodeId, RelayMode, SecretKey,
    endpoint::{Builder, Connection, RecvStream, SendStream},
};
use log::warn;
use std::fmt::Debug;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, sync::RwLock};

#[async_trait::async_trait]
pub trait ProtocolHandler: Send + Sync + Debug {
    async fn handle_stream(
        &self,
        mut send_stream: SendStream,
        mut recv_stream: RecvStream,
    ) -> anyhow::Result<()>;
}

use crate::{
    ARUNA_NET_ALPN, Kademlia,
    kademlia::{discovery::KademliaArc, messages::FindResult},
};

pub type ProtocolId = u32;

pub struct ConnectionHandlerBuilder {
    endpoint: iroh::endpoint::Builder,
    protocol_handler_map: HashMap<ProtocolId, Arc<dyn ProtocolHandler>>,
    kademlia: Arc<Kademlia>,
}

impl ConnectionHandlerBuilder {
    pub fn new(secret_key: Option<SecretKey>) -> Self {
        let kademlia = Kademlia::new();

        let kademlia_arc: KademliaArc = KademliaArc {
            kademlia: kademlia.clone(),
        };

        let mut endpoint = Builder::default()
            .alpns(vec![ARUNA_NET_ALPN.to_vec()])
            .add_discovery(move |_| Some(kademlia_arc))
            .relay_mode(RelayMode::Disabled);

        if let Some(secret_key) = secret_key {
            endpoint = endpoint.secret_key(secret_key);
        }

        let mut protocol_handler_map: HashMap<ProtocolId, Arc<dyn ProtocolHandler>> =
            HashMap::new();
        protocol_handler_map.insert(1, kademlia.clone());

        Self {
            endpoint,
            protocol_handler_map,
            kademlia,
        }
    }

    pub fn add_relay(self, relay_mode: RelayMode) -> Self {
        let endpoint = self.endpoint.relay_mode(relay_mode);
        Self {
            endpoint,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub fn add_bind_addr_v4(self, bind_addr: std::net::SocketAddrV4) -> Self {
        let endpoint = self.endpoint.bind_addr_v4(bind_addr);
        Self {
            endpoint,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub fn add_bind_addr_v6(self, bind_addr: std::net::SocketAddrV6) -> Self {
        let endpoint = self.endpoint.bind_addr_v6(bind_addr);
        Self {
            endpoint,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub fn add_protocol_handler(
        self,
        protocol_id: ProtocolId,
        protocol_handler: Arc<dyn ProtocolHandler>,
    ) -> Self {
        let mut protocol_handler_map = self.protocol_handler_map;
        protocol_handler_map.insert(protocol_id, protocol_handler);
        Self {
            endpoint: self.endpoint,
            protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub async fn build(self, bootstrap_nodes: Vec<NodeAddr>) -> Result<Arc<ConnectionHandler>> {
        let endpoint = self.endpoint.bind().await?;

        let chandler =
            ConnectionHandler::new(endpoint, self.protocol_handler_map, self.kademlia.clone());

        self.kademlia
            .init(chandler.clone(), bootstrap_nodes)
            .await?;

        Ok(chandler)
    }
}

#[derive(Debug)]
pub struct ConnectionHandler {
    endpoint: Endpoint,
    connections: RwLock<HashMap<NodeId, Connection>>,
    protocol_handler: HashMap<ProtocolId, Arc<dyn ProtocolHandler>>,
    kademlia: Arc<Kademlia>,
}

impl ConnectionHandler {
    fn new(
        endpoint: Endpoint,
        protocol_handler: HashMap<ProtocolId, Arc<dyn ProtocolHandler>>,
        kademlia: Arc<Kademlia>,
    ) -> Arc<Self> {
        let self_arc = Arc::new(Self {
            endpoint,
            connections: RwLock::new(HashMap::new()),
            protocol_handler,
            kademlia,
        });

        self_arc.clone().spawn_acceptor();

        self_arc
    }

    pub async fn get_node_addr(&self) -> anyhow::Result<NodeAddr> {
        self.endpoint.node_addr().await
    }

    pub fn spawn_acceptor(self: Arc<Self>) {
        tokio::spawn(async move {
            let endpoint = self.endpoint.clone();
            while let Some(incoming) = endpoint.accept().await {
                let connecting = match incoming.accept() {
                    Ok(connecting) => connecting,
                    Err(err) => {
                        warn!("incoming connection failed: {err:#}");
                        // we can carry on in these cases:
                        // this can be caused by retransmitted datagrams
                        continue;
                    }
                };
                let conn = match connecting.await {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!("incoming connection failed: {err:#}");
                        continue;
                    }
                };
                let node_id = match conn.remote_node_id() {
                    Ok(node_id) => node_id,
                    Err(err) => {
                        warn!("cannot retrieve node_id from conn: {err:#}");
                        continue;
                    }
                };
                self.connections.write().await.insert(node_id, conn.clone());
                let (rx, mut sx) = match conn.accept_bi().await {
                    Ok((rx, sx)) => (rx, sx),
                    Err(err) => {
                        warn!("cannot accept stream: {err:#}");
                        continue;
                    }
                };
                let Ok(protocol_id) = sx.read_u32().await else {
                    warn!("cannot read protocol id");
                    continue;
                };

                let protocol_handler = match self.protocol_handler.get(&protocol_id) {
                    Some(handler) => handler,
                    None => {
                        warn!("unknown protocol id: {protocol_id}");
                        continue;
                    }
                };
                let protocol_handler = protocol_handler.clone();
                tokio::spawn(async move {
                    if let Err(err) = protocol_handler.handle_stream(rx, sx).await {
                        warn!("cannot handle stream: {err:#}");
                    }
                });
            }
        });
    }

    async fn get_or_create_connection(&self, node_id: NodeId) -> anyhow::Result<Connection> {
        {
            let connections = self.connections.read().await;
            if let Some(connection) = connections.get(&node_id) {
                return Ok(connection.clone());
            }
        }
        let mut connections = self.connections.write().await;
        let connection = self.endpoint.connect(node_id, ARUNA_NET_ALPN).await?;
        connections.insert(node_id, connection.clone());
        Ok(connection)
    }

    pub async fn get_bidi_stream(
        &self,
        node_id: NodeId,
        protocol_id: ProtocolId,
    ) -> anyhow::Result<(RecvStream, SendStream)> {
        let connection = self.get_or_create_connection(node_id).await?;
        let (mut rx, sx) = connection.open_bi().await?;
        rx.write_u32(protocol_id).await?;
        Ok((sx, rx))
    }

    pub async fn find(&self, key: [u8; 32]) -> Result<FindResult> {
        self.kademlia.find(key, true).await
    }

    pub async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<()> {
        self.kademlia.store(key, value).await
    }
}
