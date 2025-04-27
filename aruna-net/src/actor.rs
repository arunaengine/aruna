use crate::{
    ARUNA_NET_ALPN,
    actor_handle::{InitActorHandle, NetworkActorHandle, NetworkRequests, ReceiveStreams},
    kademlia::{
        actor::{KADEMLIA_PROTOCOL_ID, KademliaActor},
        actor_handle::KademliaActorHandle,
    },
    utils::ChannelPair,
};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use iroh::{
    Endpoint, NodeAddr, NodeId, RelayMode, SecretKey,
    endpoint::{Builder, Connection, Incoming, RecvStream, SendStream},
};
use log::warn;
use std::collections::HashMap;
use tokio::{io::AsyncWriteExt, sync::oneshot};

pub const CHANNEL_SIZE: usize = 100;

pub type ProtocolId = u32;

pub struct NetworkActorBuilder {
    endpoint: iroh::endpoint::Builder,
    command: ChannelPair<NetworkRequests>,
    protocol_handler_map: HashMap<ProtocolId, ChannelPair<ReceiveStreams>>,
    kademlia: KademliaActorHandle,
}

impl NetworkActorBuilder {
    pub async fn new(secret_key: Option<SecretKey>) -> Self {
        let command = ChannelPair::new();

        let kademlia_channel_pair = ChannelPair::new();
        let kademlia_actor_handle = NetworkActorHandle::new(
            KADEMLIA_PROTOCOL_ID,
            command.sender().clone(),
            kademlia_channel_pair.receiver().clone(),
        );

        let secret_key = secret_key.unwrap_or_else(|| {
            let mut rng = rand::rngs::OsRng;
            let secret_key = SecretKey::generate(&mut rng);
            secret_key
        });

        let kademlia_handle = KademliaActor::new(secret_key.public(), kademlia_actor_handle).await;
        let handle_clone = kademlia_handle.clone();

        let endpoint = Builder::default()
            .alpns(vec![ARUNA_NET_ALPN.to_vec()])
            .add_discovery(move |_| Some(handle_clone))
            .relay_mode(RelayMode::Disabled)
            .secret_key(secret_key);

        let mut protocol_handler_map: HashMap<ProtocolId, ChannelPair<ReceiveStreams>> =
            HashMap::new();

        protocol_handler_map.insert(KADEMLIA_PROTOCOL_ID, kademlia_channel_pair);

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
        self.kademlia.bootstrap(bootstrap_nodes).await?;
        NetworkActor::new(
            endpoint,
            self.command,
            self.protocol_handler_map,
            self.kademlia,
        )
        .await;
        Ok(init_actor_handle)
    }
}

pub struct CreateStream {
    protocol_id: ProtocolId,
    return_channel: oneshot::Sender<(SendStream, RecvStream)>,
}

// This is the main actor that handles all network connections
pub struct NetworkActor {
    // The endpoint that connects to other nodes
    endpoint: Endpoint,
    // A map of all existing connections and a channel to send CreateStream commands
    connections: HashMap<NodeId, ChannelPair<CreateStream>>,
    // The command channel to receive commands for this actor
    command: ChannelPair<NetworkRequests>,
    // Channel to send new incoming streams that needs to be assigned to a protocol handler
    incoming_streams: ChannelPair<ReceiveStreams>,
    // A map of all registered protocol handlers and their channels to receive incoming streams
    protocol_handler_map: HashMap<ProtocolId, ChannelPair<ReceiveStreams>>,
    // The Kademlia actor handle to send commands and clone it
    kademlia: KademliaActorHandle,
    // A join set that contains all connection loops
    receiver_joinset: tokio::task::JoinSet<()>,
}

pub async fn connection_loop(
    connection: Connection,
    incoming_streams: Sender<ReceiveStreams>,
    create_stream: Receiver<CreateStream>,
) {
    loop {
        tokio::select! {
            Ok(CreateStream { protocol_id, return_channel }) = create_stream.recv() => {
                let Ok((mut send_stream, recv_stream)) = connection.open_bi().await else {
                    warn!("cannot open stream");
                    continue;
                };

                // Write the protocol id to the stream
                if send_stream.write_u32(protocol_id).await.is_err() {
                    warn!("cannot write protocol id to stream");
                    continue;
                }

                if return_channel.send((send_stream, recv_stream)).is_err() {
                    warn!("cannot send stream to handler");
                    continue;
                }
            }

            Ok((send_stream, recv_stream)) = connection.accept_bi() => {
                let receive_streams = ReceiveStreams {
                    sender: connection.remote_node_id().unwrap(),
                    send_stream,
                    recv_stream,
                };
                if incoming_streams.send(receive_streams).await.is_err() {
                    warn!("cannot send stream to handler");
                    continue;
                }
            }

            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
}

impl NetworkActor {
    async fn new(
        endpoint: Endpoint,
        command: ChannelPair<NetworkRequests>,
        protocol_handler_map: HashMap<ProtocolId, ChannelPair<ReceiveStreams>>,
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

    // This spawns the main acceptor loop
    async fn spawn_acceptor(mut self) {
        tokio::spawn(async move {
            loop {
                tokio::select!(

                // Handle commands from the command channel
                Ok(command) = self.command.receiver().recv() => {
                    // Handle commands from the command channel
                    match command {
                        NetworkRequests::CreateStream { protocol_id, receiver, return_channel } => {
                            if self.create_stream(protocol_id, receiver, return_channel).await.is_err() {
                                warn!("cannot create stream");
                                continue;
                            }
                        }
                        NetworkRequests::GetKademliaActorHandle { return_channel } => {
                            if return_channel.send(self.kademlia.clone()).is_err() {
                                warn!("cannot send kademlia actor handle");
                                continue;
                            }
                        }
                        NetworkRequests::NewActorHandle { protocol_id, return_channel } => {

                            // Check if the protocol handler exists
                            match self.protocol_handler_map.get(&protocol_id) {
                                Some(handler_channel_pair) => {
                                    // If it exists, create a new copy of the actor handle
                                    // and send it to the command channel
                                    let actor_handle = NetworkActorHandle::new(
                                        protocol_id,
                                        self.command.sender().clone(),
                                        handler_channel_pair.receiver().clone(),
                                    );
                                    if return_channel.send(actor_handle).is_err() {
                                        warn!("cannot send protocol handler");
                                        continue;
                                    }
                                }
                                None => {
                                    // If it doesn't exist, create a new one
                                    let pair = ChannelPair::new();
                                    let handler_receiver = pair.receiver().clone();
                                    self.protocol_handler_map.insert(protocol_id, pair);
                                    let actor_handle = NetworkActorHandle::new(
                                        protocol_id,
                                        self.command.sender().clone(),
                                        handler_receiver,
                                    );
                                    if return_channel.send(actor_handle).is_err() {
                                        warn!("cannot send protocol handler");
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }

                // Handle incoming connections
                Some(incoming) = self.endpoint.accept() => {
                    self.handle_incoming(incoming).await;
                }

                // Try to join the receiver joinset
                Some(Ok(())) = self.receiver_joinset.join_next() => {
                    // If a task has finished, we can continue
                })
            }
        });
    }

    async fn create_stream(
        &mut self,
        protocol_id: ProtocolId,
        receiver_id: NodeId,
        return_channel: oneshot::Sender<(SendStream, RecvStream)>,
    ) -> Result<()> {
        // Check if a connection already exists
        match self.connections.get(&receiver_id) {
            Some(sender) => {
                // If a connection exists, send the CreateStream command
                sender
                    .sender()
                    .send(CreateStream {
                        protocol_id,
                        return_channel,
                    })
                    .await?;
                Ok(())
            }
            None => {
                // If no connection exists, create a new one

                // 1. Create a connection
                let conn = match self.endpoint.connect(receiver_id, ARUNA_NET_ALPN).await {
                    Ok(connecting) => connecting,
                    Err(err) => {
                        warn!("cannot connect to {receiver_id}: {err:#}");
                        return Err(err.into());
                    }
                };

                // 2. Create receiver and sender channels
                let pair = ChannelPair::new();
                // 3. Insert the new connection into the connections map
                self.connections.insert(receiver_id, pair.clone());
                // Spawn a task to handle future incoming streams
                self.receiver_joinset.spawn({
                    let create_stream_recv = pair.receiver().clone();
                    let incoming_streams = self.incoming_streams.sender().clone();
                    async move {
                        connection_loop(conn, incoming_streams, create_stream_recv).await;
                    }
                });

                pair.sender()
                    .send(CreateStream {
                        protocol_id,
                        return_channel,
                    })
                    .await?;
                Ok(())
            }
        }
    }

    async fn handle_incoming(&mut self, incoming: Incoming) {
        // Handle incoming connections
        // 1. Accept the incoming connection
        let connecting = match incoming.accept() {
            Ok(connecting) => connecting,
            Err(err) => {
                warn!("incoming connection failed: {err:#}");
                // we can carry on in these cases:
                // this can be caused by retransmitted datagrams
                return;
            }
        };
        // 2. Wait for the connection to be established
        let conn = match connecting.await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("incoming connection failed: {err:#}");
                return;
            }
        };
        // 3. Get the remote node pubkey
        let node_id = match conn.remote_node_id() {
            Ok(node_id) => node_id,
            Err(err) => {
                warn!("cannot retrieve node_id from conn: {err:#}");
                return;
            }
        };

        // Create a ChannelPair for CreateStream commands
        let pair = ChannelPair::new();

        // Insert the new connection into the connections map
        self.connections.insert(node_id, pair.clone());

        // Spawn a task to handle future incoming streams
        self.receiver_joinset.spawn({
            let conn = conn.clone();
            let create_stream_recv = pair.receiver().clone();
            let incoming_streams = self.incoming_streams.sender().clone();
            async move {
                connection_loop(conn, incoming_streams, create_stream_recv).await;
            }
        });
    }
}
