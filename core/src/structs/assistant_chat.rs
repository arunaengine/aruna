use crate::errors::ConversionError;
use crate::types::UserId;
use serde::{Deserialize, Serialize};

/// The assistant conversations of one user, as the portal keeps them.
///
/// The payload is the portal's own chat state and stays opaque here: the node
/// stores it so the chats follow the user between browsers, and reads it back
/// unchanged. It is held on the node that received it and is not replicated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantChats {
    pub user_id: UserId,
    pub payload: String,
    /// Bumped by every accepted write, so a stale write is refused.
    pub revision: u64,
    pub updated_at: u64,
}

/// How much chat state one user may keep on the node.
pub const MAX_ASSISTANT_CHAT_BYTES: usize = 2 * 1024 * 1024;

impl AssistantChats {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
