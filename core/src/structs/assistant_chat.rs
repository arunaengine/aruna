use crate::errors::ConversionError;
use crate::types::UserId;
use serde::{Deserialize, Serialize};

/// Live chats one user may keep on the node.
pub const MAX_ASSISTANT_CHATS: usize = 20;
/// Live turns one chat keeps; an append past this drops the oldest turns.
pub const MAX_ASSISTANT_CHAT_TURNS: u32 = 120;
/// Bytes one turn payload may hold.
pub const MAX_ASSISTANT_TURN_BYTES: usize = 64 * 1024;
/// Bytes all live turns of one user may hold together.
pub const MAX_ASSISTANT_CHAT_BYTES: u64 = 8 * 1024 * 1024;

/// One assistant chat of a user without its turns.
///
/// The head is held on the node that received it and is not replicated. A
/// deleted chat keeps its head as a tombstone, so the id is never reused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantChatHead {
    pub user_id: UserId,
    pub chat_id: String,
    pub title: String,
    pub subject: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
    /// Live turns have a seq in `first_seq..next_seq`.
    pub first_seq: u32,
    pub next_seq: u32,
    /// Sum of the live turn payload lengths.
    pub bytes: u64,
    /// Bumped by every accepted head or turn write.
    pub revision: u64,
    pub deleted_at: Option<u64>,
}

/// One turn of a chat. The payload is the portal's own format and stays opaque.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantChatTurn {
    pub seq: u32,
    pub payload: String,
    pub updated_at: u64,
}

impl AssistantChatHead {
    pub fn is_live(&self) -> bool {
        self.deleted_at.is_none()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl AssistantChatTurn {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
