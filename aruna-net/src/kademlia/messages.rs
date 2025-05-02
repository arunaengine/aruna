use iroh::NodeAddr;
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
        value: Vec<NodeAddr>, // List of node addresses that do contain the requested value, empty if not found
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
    pub fn new_request(id: Ulid, sender: Option<NodeAddr>, msg_type: MessageType) -> Self {
        Self {
            id,
            sender,
            msg_type,
        }
    }

    /// Create a response message for a given request
    pub fn create_response(&self, sender: Option<NodeAddr>, msg_type: MessageType) -> Self {
        Self {
            id: self.id, // Keep same ID for request/response correlation
            sender,
            msg_type,
        }
    }

    /// Check if this message is a response
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
    pub value: Vec<NodeAddr>, // The value(s) found, if any
    pub nodes: Vec<NodeAddr>, // List of N closest nodes
}

impl FindResult {
    pub fn empty() -> Self {
        Self {
            value: Vec::new(),
            nodes: Vec::new(),
        }
    }
}
