use std::{collections::HashMap, vec};

use iroh::{NodeAddr, NodeId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Message type to distinguish between requests and responses
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageType {
    // Request types
    PingRequest {
        node_addr: NodeAddr,
    },
    FindRequest {
        target: [u8; 32],
    },
    StoreRequest {
        key: [u8; 32],
        value: NodeAddr,
        signature: Option<Vec<u8>>,
    },

    // Response types
    PingResponse,
    FindResponse {
        value: Vec<MaybeSignedAddr>, // List of node addresses that do contain the requested value, empty if not found
        nodes: Vec<NodeAddr>,
    },
    StoreResponse,
}

/// Unified Message structure for Kademlia protocol
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KademliaMessage {
    pub id: Ulid,                 // Message ID (for requests and responses)
    pub sender: Option<NodeAddr>, // Sender of this message
    pub msg_type: MessageType,    // Type of message with explicit request/response variants
}

impl KademliaMessage {
    /// Create a new request message
    #[tracing::instrument(level = "trace")]
    pub fn new_request(id: Ulid, sender: Option<NodeAddr>, msg_type: MessageType) -> Self {
        Self {
            id,
            sender,
            msg_type,
        }
    }

    /// Create a response message for a given request
    #[tracing::instrument(level = "trace")]
    pub fn create_response(&self, sender: Option<NodeAddr>, msg_type: MessageType) -> Self {
        Self {
            id: self.id, // Keep same ID for request/response correlation
            sender,
            msg_type,
        }
    }

    /// Check if this message is a response
    #[tracing::instrument(level = "trace")]
    pub fn is_response(&self) -> bool {
        matches!(
            self.msg_type,
            MessageType::PingResponse
                | MessageType::FindResponse { .. }
                | MessageType::StoreResponse
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FindResult {
    pub value_at_nodes: HashMap<NodeId, Vec<MaybeSignedAddr>>, // The value(s) found, if any
    pub nodes: Vec<NodeAddr>,                                  // List of N closest nodes
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaybeSignedAddr {
    pub addr: NodeAddr,
    pub signature: Option<Vec<u8>>,
}

impl MaybeSignedAddr {
    #[tracing::instrument(level = "trace")]
    pub fn new(addr: NodeAddr, signature: Option<Vec<u8>>) -> Self {
        Self { addr, signature }
    }

    #[tracing::instrument(level = "trace")]
    pub fn addr(&self) -> &NodeAddr {
        &self.addr
    }

    #[tracing::instrument(level = "trace")]
    pub fn signature(&self) -> Option<&Vec<u8>> {
        self.signature.as_ref()
    }
}

impl FindResult {
    #[tracing::instrument(level = "trace")]
    pub fn empty() -> Self {
        Self {
            value_at_nodes: HashMap::default(),
            nodes: Vec::new(),
        }
    }

    #[tracing::instrument(level = "trace")]
    pub fn get_values(&self) -> Vec<MaybeSignedAddr> {
        self.value_at_nodes
            .values()
            .flat_map(|v| v.clone())
            .collect()
    }

    #[tracing::instrument(level = "trace")]
    pub fn values_at_closest(&self) -> Vec<MaybeSignedAddr> {
        let mut values = vec![];
        for node in self.nodes.iter() {
            if let Some(value) = self.value_at_nodes.get(&node.node_id) {
                values.extend(value.clone());
            }
        }
        values
    }
}
