use crate::errors::ConversionError;
use crate::types::UserId;
use serde::{Deserialize, Serialize};

/// Bytes one vault payload may hold.
pub const MAX_USER_VAULT_BYTES: usize = 64 * 1024;

/// The passphrase-sealed keys of one user. The payload is the portal's own
/// ciphertext and stays opaque; the node holds no key that opens it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserVault {
    pub user_id: UserId,
    pub payload: String,
    /// Bumped by every accepted write.
    pub revision: u64,
    pub updated_at: u64,
}

impl UserVault {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
