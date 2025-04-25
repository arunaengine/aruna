use crate::{
    con_actor::{ConnectionActorHandle, ConnectionRequests, InitActorHandle, ReceiveStreams}, kademlia::{actor_handle::KademliaActorHandle, kademlia::KademliaActor, messages::FindResult}, utils::ChannelPair, ARUNA_NET_ALPN
};
use anyhow::Result;
use futures::stream::FuturesUnordered;
use iroh::{
    Endpoint, NodeAddr, NodeId, RelayMode, SecretKey,
    endpoint::{Builder, Connection, RecvStream, SendStream},
};
use log::warn;
use std::{collections::HashMap, sync::Arc};
use std::fmt::Debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const CHANNEL_SIZE: usize = 100;

pub type ProtocolId = u32;

pub struct ConnectionHandlerBuilder {
    endpoint: iroh::endpoint::Builder,
    command: ChannelPair<ConnectionRequests>,
    protocol_handler_map: HashMap<ProtocolId, async_channel::Sender<ReceiveStreams>>,
    kademlia: KademliaActorHandle,
}

impl ConnectionHandlerBuilder {
    pub async fn new(secret_key: Option<SecretKey>) -> Self {
        let command = ChannelPair::new();

        let (kademlia_stream_sender, kademlia_stream_receiver) =
            async_channel::bounded(CHANNEL_SIZE);
        let kademlia_actor_handle =
            ConnectionActorHandle::new(1, command.sender().clone(), kademlia_stream_receiver);

        let kademlia_handle = KademliaActor::new(kademlia_actor_handle).await;
        let handle_clone = kademlia_handle.clone();

        let mut endpoint = Builder::default()
            .alpns(vec![ARUNA_NET_ALPN.to_vec()])
            .add_discovery(move |_| Some(handle_clone))
            .relay_mode(RelayMode::Disabled);

        if let Some(secret_key) = secret_key {
            endpoint = endpoint.secret_key(secret_key);
        }

        let mut protocol_handler_map: HashMap<ProtocolId, async_channel::Sender<ReceiveStreams>> =
            HashMap::new();

        protocol_handler_map.insert(1, kademlia_stream_sender);

        Self {
            endpoint,
            command,
            protocol_handler_map,
            kademlia: kademlia_handle,
        }
    }

    pub fn add_relay(self, relay_mode: RelayMode) -> Self {
        let endpoint = self.endpoint.relay_mode(relay_mode);
        Self {
            endpoint,
            command: self.command,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub fn add_bind_addr_v4(self, bind_addr: std::net::SocketAddrV4) -> Self {
        let endpoint = self.endpoint.bind_addr_v4(bind_addr);
        Self {
            endpoint,
            command: self.command,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub fn add_bind_addr_v6(self, bind_addr: std::net::SocketAddrV6) -> Self {
        let endpoint = self.endpoint.bind_addr_v6(bind_addr);
        Self {
            endpoint,
            command: self.command,
            protocol_handler_map: self.protocol_handler_map,
            kademlia: self.kademlia,
        }
    }

    pub async fn build(self, bootstrap_nodes: Vec<NodeAddr>) -> Result<InitActorHandle> {
        let endpoint = self.endpoint.bind().await?;
        let init_actor_handle = InitActorHandle::new(self.command.sender().clone());
        ConnectionHandler::new(
            endpoint,
            self.command,
            self.protocol_handler_map,
            self.kademlia,
        )
        .await;
        Ok(init_actor_handle)
    }
}

pub struct ConnectionHandler {
    endpoint: Endpoint,
    connections: HashMap<NodeId, Connection>,
    command: ChannelPair<ConnectionRequests>,
    incoming_streams: ChannelPair<ReceiveStreams>,
    protocol_handler_map: HashMap<ProtocolId, async_channel::Sender<ReceiveStreams>>,
    kademlia: KademliaActorHandle,
    receiver_joinset: tokio::task::JoinSet<()>,
}

impl ConnectionHandler {
    async fn new(
        endpoint: Endpoint,
        command: ChannelPair<ConnectionRequests>,
        protocol_handler_map: HashMap<ProtocolId, async_channel::Sender<ReceiveStreams>>,
        kademlia: KademliaActorHandle,
    ) {
        let handler = Self {
            endpoint,
            connections: HashMap::new(),
            command,
            incoming_streams: ChannelPair::new(),
            protocol_handler_map,
            kademlia,
            receiver_joinset: tokio::task::JoinSet::new(),
        };

        handler.spawn_acceptor().await;
    }

    pub async fn get_node_addr(&self) -> anyhow::Result<NodeAddr> {
        self.endpoint.node_addr().await
    }

    pub async fn spawn_acceptor(mut self) {
        tokio::spawn(async move {
            loop {
                tokio::select!(
                    command = self.command.receiver().recv() => {
                        // Handle commands from the command channel
                    },
                    Some(incoming) = self.endpoint.accept() => {
                        // Handle incoming connections here
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
                        self.connections.insert(node_id, conn.clone());

                        tokio::spawn({});


                        bidi_accept_futures.push(Box::pin(async {
                            let conn = conn.clone();
                            loop {
                                let (sx, mut rx) = match conn.accept_bi().await {
                                    Ok((rx, sx)) => (rx, sx),
                                    Err(err) => {
                                        warn!("cannot accept stream: {err:#}");
                                        continue;
                                    }
                                };
                                let Ok(protocol_id) = rx.read_u32().await else {
                                    warn!("cannot read protocol id");
                                    continue;
                                };
                    
                                let protocol_handler = match self.protocol_handler_map.get(&protocol_id) {
                                    Some(handler) => handler,
                                    None => {
                                        warn!("unknown protocol id: {protocol_id}");
                                        continue;
                                    }
                                };
                                protocol_handler
                                    .send(ReceiveStreams {
                                        sender: conn.remote_node_id().unwrap(),
                                        send_stream: sx,
                                        recv_stream: rx,
                                    })
                                    .await
                                    .unwrap_or_else(|err| {
                                        warn!("cannot send stream to handler: {err:#}");
                                    });
                            }
                        }));

                    }
                )
            }

            // tokio::select!(
            //     command = self.command_receiver.recv() => {
            //         match command {
            //             Ok(ConnectionRequests::CreateStream { protocol_id, receiver, return_channel }) => {
            //                 let (send_stream, recv_stream) = self.get_bidi_stream(receiver, protocol_id).await.unwrap();
            //                 return_channel.send((send_stream, recv_stream)).unwrap();
            //             }
            //             Ok(ConnectionRequests::GetKademliaActorHandle { return_channel }) => {
            //                 return_channel.send(self.kademlia.clone()).unwrap();
            //             }
            //             Ok(ConnectionRequests::NewActorHandle { protocol_id, return_channel }) => {
            //                 let (send_stream, recv_stream) = self.get_bidi_stream(receiver, protocol_id).await.unwrap();
            //                 return_channel.send((send_stream, recv_stream)).unwrap();
            //             }
            //             Err(_) => {
            //                 warn!("command channel closed");
            //                 break;
            //             }
            //         }
            //     },
            //     _ = self.endpoint.accept()

            // async {
            //         loop {
            //             let incoming = self.endpoint.accept().await;
            //             if let Some(incoming) = incoming {
            //                 let connecting = match incoming.accept() {
            //                     Ok(connecting) => connecting,
            //                     Err(err) => {
            //                         warn!("incoming connection failed: {err:#}");
            //                         // we can carry on in these cases:
            //                         // this can be caused by retransmitted datagrams
            //                         continue;
            //                     }
            //                 };
            //                 let conn = match connecting.await {
            //                     Ok(conn) => conn,
            //                     Err(err) => {
            //                         warn!("incoming connection failed: {err:#}");
            //                         continue;
            //                     }
            //                 };
            //                 let node_id = match conn.remote_node_id() {
            //                     Ok(node_id) => node_id,
            //                     Err(err) => {
            //                         warn!("cannot retrieve node_id from conn: {err:#}");
            //                         continue;
            //                     }
            //                 };
            //                 self.connections.insert(node_id, conn.clone());

            //                 futures_unordered.push(Box::pin(self.accept_bidi_stream(conn)))
            //             }
            //         }
            //     }
            // ));
        });
    }

    async fn get_or_create_connection(self: Arc<Self>, node_id: NodeId) -> Result<Connection> {
        {
            let connections = self.connections.read().await;
            if let Some(connection) = connections.get(&node_id) {
                return Ok(connection.clone());
            }
        }
        let mut connections = self.connections.write().await;
        let connection = self.endpoint.connect(node_id, ARUNA_NET_ALPN).await?;

        self.clone().accept_bidi_stream(connection.clone()).await;

        connections.insert(node_id, connection.clone());
        Ok(connection)
    }

    async fn accept_bidi_stream(&self, conn: Connection) {
        
    }

    pub async fn get_bidi_stream(
        self: Arc<Self>,
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
