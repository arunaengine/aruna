use aruna_core::errors::ConversionError;
use aruna_core::types::{GroupId, Key, UserId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;

pub(crate) fn owner_key(user_id: UserId, group_id: Option<GroupId>) -> Key {
    let mut key = user_id.to_storage_key();
    if let Some(group_id) = group_id {
        key.extend_from_slice(&group_id.to_bytes());
    }
    key.into()
}

pub(crate) fn decode_index<E>(
    value: Option<&ByteView>,
    cap: usize,
    overflow: impl FnOnce() -> E,
    validate: impl FnOnce(&BTreeSet<String>) -> Result<(), E>,
) -> Result<BTreeSet<String>, E>
where
    E: From<ConversionError>,
{
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    let index: BTreeSet<String> =
        postcard::from_bytes(value.as_ref()).map_err(ConversionError::from)?;
    if index.len() > cap {
        return Err(overflow());
    }
    validate(&index)?;
    Ok(index)
}

pub(crate) fn encode_index<E>(
    index: &BTreeSet<String>,
    cap: usize,
    overflow: impl FnOnce() -> E,
    validate: impl FnOnce(&BTreeSet<String>) -> Result<(), E>,
) -> Result<Value, E>
where
    E: From<ConversionError>,
{
    if index.len() > cap {
        return Err(overflow());
    }
    validate(index)?;
    Ok(ByteView::from(
        postcard::to_allocvec(index).map_err(ConversionError::from)?,
    ))
}
