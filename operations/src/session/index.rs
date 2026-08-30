use aruna_core::errors::ConversionError;
use aruna_core::types::{Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;
use ulid::Ulid;

pub const MAX_USER_SESSIONS: usize = 256;

pub fn owner_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> = postcard::from_bytes(value.as_ref())?;
    if index.len() > MAX_USER_SESSIONS {
        return Err(ConversionError::InvalidLength(
            "session owner index exceeds limit".to_string(),
        ));
    }
    for sid in &index {
        Ulid::from_string(sid)?;
    }
    Ok(index)
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    if index.len() > MAX_USER_SESSIONS {
        return Err(ConversionError::InvalidLength(
            "session owner index exceeds limit".to_string(),
        ));
    }
    Ok(ByteView::from(postcard::to_allocvec(index)?))
}
