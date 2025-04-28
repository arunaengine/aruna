use anyhow::Result;
use iroh::NodeAddr;
use tokio::sync::oneshot;
use ulid::Ulid;

use crate::FindResult;

use super::messages::{KademliaMessage, MessageType};

#[derive(Clone, Debug)]
pub struct KademliaActorHandle {
    sender: async_channel::Sender<KademliaRequest>,
}

pub enum KademliaRequest {
    Request {
        request: KademliaMessage,
        response: oneshot::Sender<KademliaMessage>,
    },
    SetNodeAddr {
        node_addr: NodeAddr,
    },
    Bootstrap {
        node_addrs: Vec<NodeAddr>,
    },
}

impl KademliaActorHandle {
    pub fn new(sender: async_channel::Sender<KademliaRequest>) -> Self {
        Self { sender }
    }

    pub async fn find(&self, target: [u8; 32]) -> Result<FindResult> {
        // We send a Kademlia request with an empty sender, this will be set by the kademlia actor
        let request =
            KademliaMessage::new_request(Ulid::new(), None, MessageType::FindRequest { target });

        let response = self.send_request(request).await?;

        match response.msg_type {
            MessageType::FindResponse { value, nodes } => Ok(FindResult { value, nodes }),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub async fn store(&self, key: [u8; 32], value: NodeAddr) -> Result<()> {
        let request = KademliaMessage::new_request(
            Ulid::new(),
            None,
            MessageType::StoreRequest { key, value },
        );

        let response = self.send_request(request).await?;

        match response.msg_type {
            MessageType::StoreResponse => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response type")),
        }
    }

    pub async fn set_node_addr(&self, node_addr: NodeAddr) -> Result<()> {
        self.sender
            .send(KademliaRequest::SetNodeAddr { node_addr })
            .await?;
        Ok(())
    }

    pub async fn bootstrap(&self, node_addrs: Vec<NodeAddr>) -> Result<()> {
        self.sender
            .send(KademliaRequest::Bootstrap { node_addrs })
            .await?;
        Ok(())
    }

    async fn send_request(
        &self,
        request: KademliaMessage,
    ) -> Result<KademliaMessage, anyhow::Error> {
        let (response_tx, response_rx) = oneshot::channel();
        let kademlia_request = KademliaRequest::Request {
            request,
            response: response_tx,
        };

        self.sender.send(kademlia_request).await?;

        Ok(response_rx.await?)
    }
}
