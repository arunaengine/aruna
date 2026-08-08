use aruna_core::errors::ConversionError;
use aruna_core::structs::UserAccess;
use aruna_core::types::{Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;

pub const MAX_ACTIVE_CREDENTIALS: usize = 16;

pub fn owner_key(user_identity: UserId) -> Key {
    ByteView::from(user_identity.to_storage_key())
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> = postcard::from_bytes(value.as_ref())?;
    if index.len() > MAX_ACTIVE_CREDENTIALS {
        return Err(ConversionError::InvalidLength(format!(
            "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
        )));
    }
    for access_key in &index {
        UserAccess::build_access_key(access_key)?;
    }
    Ok(index)
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    if index.len() > MAX_ACTIVE_CREDENTIALS {
        return Err(ConversionError::InvalidLength(format!(
            "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
        )));
    }
    Ok(ByteView::from(postcard::to_allocvec(index)?))
}
