use iroh::{
    NodeId,
    endpoint::{RecvStream, SendStream},
};
use tokio::sync::oneshot;

use crate::kademlia::actor_handle::KademliaActorHandle;

pub type ProtocolId = u32;

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

    pub async fn get_kademlia_actor_handle(&self) -> Result<KademliaActorHandle, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetKademliaActorHandle {
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
}

pub enum NetworkRequests {
    CreateStream {
        protocol_id: ProtocolId,
        receiver: NodeId,
        return_channel: oneshot::Sender<(SendStream, RecvStream)>,
    },
    GetKademliaActorHandle {
        return_channel: oneshot::Sender<KademliaActorHandle>,
    },
    NewActorHandle {
        protocol_id: ProtocolId,
        return_channel: oneshot::Sender<NetworkActorHandle>,
    },
}

pub struct ReceiveStreams {
    pub sender: NodeId,
    pub send_stream: SendStream,
    pub recv_stream: RecvStream,
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

    pub async fn get_kademlia_actor_handle(&self) -> Result<KademliaActorHandle, anyhow::Error> {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        let message = NetworkRequests::GetKademliaActorHandle {
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

    pub async fn receive(&self) -> Result<ReceiveStreams, anyhow::Error> {
        Ok(self.recv_channel.recv().await?)
    }
}
