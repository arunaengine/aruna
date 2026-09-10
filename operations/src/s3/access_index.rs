use aruna_core::errors::ConversionError;
use aruna_core::structs::UserAccess;
use aruna_core::types::{Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;

pub const MAX_ACTIVE_CREDENTIALS: usize = 16;

pub fn owner_key(user_identity: UserId) -> Key {
    crate::owner_index::owner_key(user_identity, None)
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    crate::owner_index::decode_index(
        value,
        MAX_ACTIVE_CREDENTIALS,
        || {
            ConversionError::InvalidLength(format!(
                "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
            ))
        },
        |index| {
            for access_key in index {
                UserAccess::build_access_key(access_key)?;
            }
            Ok(())
        },
    )
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    crate::owner_index::encode_index(
        index,
        MAX_ACTIVE_CREDENTIALS,
        || {
            ConversionError::InvalidLength(format!(
                "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
            ))
        },
        |_| Ok(()),
    )
}
