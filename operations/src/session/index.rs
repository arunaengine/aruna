use aruna_core::errors::ConversionError;
use aruna_core::types::{Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;
use ulid::Ulid;

pub fn owner_key(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> = postcard::from_bytes(value.as_ref())?;
    for sid in &index {
        Ulid::from_string(sid)?;
    }
    Ok(index)
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    Ok(ByteView::from(postcard::to_allocvec(index)?))
}
