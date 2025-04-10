use iroh::NodeAddr;
use ulid::Ulid;

/// Message type to distinguish between requests and responses
#[derive(Clone, Debug)]
pub enum MessageType {
    // Request types
    PingRequest,
    FindRequest {
        target: [u8; 32],
    },
    StoreRequest {
        key: [u8; 32],
        value: NodeAddr,
    },

    // Response types
    PingResponse,
    FindResponse {
        value: Option<NodeAddr>,
        nodes: Vec<NodeAddr>,
    },
    StoreResponse,
}

/// Unified Message structure for Kademlia protocol
#[derive(Clone, Debug)]
pub struct KademliaMessage {
    pub id: Ulid,              // Message ID (for requests and responses)
    pub sender: NodeAddr,      // Sender of this message
    pub msg_type: MessageType, // Type of message with explicit request/response variants
}

impl KademliaMessage {
    /// Create a new request message
    pub fn new_request(id: Ulid, sender: NodeAddr, msg_type: MessageType) -> Self {
        Self {
            id,
            sender,
            msg_type,
        }
    }

    /// Create a response message for a given request
    pub fn create_response(&self, sender: NodeAddr, msg_type: MessageType) -> Self {
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

// Result types for public API methods
#[derive(Clone, Debug)]
pub struct FindResult {
    pub value: Option<NodeAddr>,
    pub nodes: Vec<NodeAddr>,
}
