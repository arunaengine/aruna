use iroh::{
    NodeAddr, NodeId,
    endpoint::{RecvStream, SendStream},
};
use tokio::{io::AsyncReadExt, sync::oneshot};

use crate::Kademlia;

pub type ProtocolId = u32;

#[derive(Clone, Debug)]
pub struct NetworkActorHandle {
    pub protocol_id: ProtocolId,
    pub send_channel: async_channel::Sender<NetworkRequests>,
    pub recv_channel: async_channel::Receiver<ReceiveStreams>,
}

pub struct InitActorHandle {
    pub send_channel: async_channel::Sender<NetworkRequests>,
}

impl InitActorHandle {
    pub fn new(send_channel: async_channel::Sender<NetworkRequests>) -> Self {
        Self { send_channel }
    }

    pub async fn get_kademlia_actor_handle(&self) -> Result<Kademlia, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetKademlia {
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let kademlia_actor_handle = oneshot_rx.await?;

        Ok(kademlia_actor_handle)
    }

    pub async fn new_actor_handle(
        &self,
        protocol_id: ProtocolId,
    ) -> Result<NetworkActorHandle, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::NewActorHandle {
            protocol_id,
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let connection_actor_handle = oneshot_rx.await?;

        Ok(connection_actor_handle)
    }

    pub async fn get_node_addr(&self) -> Result<NodeAddr, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetNodeAddr {
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let node_addr = oneshot_rx.await?;

        Ok(node_addr)
    }
}

#[derive(Debug)]
pub enum NetworkRequests {
    CreateStream {
        protocol_id: ProtocolId,
        receiver: NodeId,
        return_channel: oneshot::Sender<(SendStream, RecvStream)>,
    },
    GetKademlia {
        return_channel: oneshot::Sender<Kademlia>,
    },
    NewActorHandle {
        protocol_id: ProtocolId,
        return_channel: oneshot::Sender<NetworkActorHandle>,
    },
    GetNodeAddr {
        return_channel: oneshot::Sender<NodeAddr>,
    },
}

pub struct ReceiveStreams {
    pub sender: NodeId,
    pub send_stream: SendStream,
    pub recv_stream: RecvStream,
}

impl ReceiveStreams {
    pub async fn read_protocol(&mut self) -> Result<ProtocolId, anyhow::Error> {
        self.recv_stream
            .read_u32()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read protocol id: {}", e))
    }
}

impl NetworkActorHandle {
    pub fn new(
        protocol_id: ProtocolId,
        send_channel: async_channel::Sender<NetworkRequests>,
        recv_channel: async_channel::Receiver<ReceiveStreams>,
    ) -> Self {
        Self {
            protocol_id,
            send_channel,
            recv_channel,
        }
    }

    pub async fn create_stream(
        &self,
        target: NodeId,
    ) -> Result<(SendStream, RecvStream), anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::CreateStream {
            protocol_id: self.protocol_id,
            receiver: target,
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let (send_stream, recv_stream) = oneshot_rx.await?;

        Ok((send_stream, recv_stream))
    }

    pub async fn get_kademlia_actor_handle(&self) -> Result<Kademlia, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetKademlia {
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let kademlia_actor_handle = oneshot_rx.await?;

        Ok(kademlia_actor_handle)
    }

    pub async fn new_actor_handle(
        &self,
        protocol_id: ProtocolId,
    ) -> Result<NetworkActorHandle, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::NewActorHandle {
            protocol_id,
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let connection_actor_handle = oneshot_rx.await?;

        Ok(connection_actor_handle)
    }

    pub async fn get_node_addr(&self) -> Result<NodeAddr, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetNodeAddr {
            return_channel: oneshot_tx,
        };

        self.send_channel.send(message).await?;

        let node_addr = oneshot_rx.await?;

        Ok(node_addr)
    }

    pub async fn receive(&self) -> Result<ReceiveStreams, anyhow::Error> {
        Ok(self.recv_channel.recv().await?)
    }
}
