use aruna_core::errors::ConversionError;
use aruna_core::types::{Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;
use ulid::Ulid;

pub const MAX_USER_SESSIONS: usize = 256;

pub fn owner_key(user_id: UserId) -> Key {
    crate::owner_index::owner_key(user_id, None)
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    crate::owner_index::decode_index(
        value,
        MAX_USER_SESSIONS,
        || ConversionError::InvalidLength("session owner index exceeds limit".to_string()),
        |index| {
            for sid in index {
                Ulid::from_string(sid)?;
            }
            Ok(())
        },
    )
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    crate::owner_index::encode_index(
        index,
        MAX_USER_SESSIONS,
        || ConversionError::InvalidLength("session owner index exceeds limit".to_string()),
        |_| Ok(()),
    )
}
