use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::RealmId;
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use byteview::ByteView;

pub fn storage_keyspace(document: &DocumentSyncTarget) -> &'static str {
    document.storage_keyspace()
}

pub fn storage_key(document: &DocumentSyncTarget) -> Key {
    document.storage_key()
}

pub fn read_effect(document: &DocumentSyncTarget, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        txn_id,
    })
}

pub fn write_effect(
    document: &DocumentSyncTarget,
    value: Vec<u8>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Write {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        value: value.into(),
        txn_id,
    })
}

pub fn delete_effect(document: &DocumentSyncTarget, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        txn_id,
    })
}

pub fn parse_document_bytes(event: Event) -> Result<Option<Vec<u8>>, StorageError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::ReadError),
    }
}

pub fn parse_auth_document(key: &[u8]) -> Result<DocumentSyncTarget, ConversionError> {
    match key.len() {
        16 => {
            let mut group_bytes = [0u8; 16];
            group_bytes.copy_from_slice(key);
            Ok(DocumentSyncTarget::GroupAuthorization {
                group_id: GroupId::from_bytes(group_bytes),
            })
        }
        32 => {
            let mut realm_bytes = [0u8; 32];
            realm_bytes.copy_from_slice(key);
            Ok(DocumentSyncTarget::RealmAuthorization {
                realm_id: RealmId::from_bytes(realm_bytes),
            })
        }
        other => Err(ConversionError::InvalidLength(format!(
            "unexpected auth key length {other}"
        ))),
    }
}

pub fn parse_group_document(key: &[u8]) -> Result<DocumentSyncTarget, ConversionError> {
    if key.len() != 16 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected group key length {}",
            key.len()
        )));
    }

    let mut group_bytes = [0u8; 16];
    group_bytes.copy_from_slice(key);
    Ok(DocumentSyncTarget::Group {
        group_id: GroupId::from_bytes(group_bytes),
    })
}

pub fn parse_realm_config_document(key: &[u8]) -> Result<DocumentSyncTarget, ConversionError> {
    if key.len() != 32 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected realm config key length {}",
            key.len()
        )));
    }

    let mut realm_bytes = [0u8; 32];
    realm_bytes.copy_from_slice(key);
    Ok(DocumentSyncTarget::RealmConfig {
        realm_id: RealmId::from_bytes(realm_bytes),
    })
}

pub fn event_to_iter_values(event: Event) -> Result<Vec<(Key, ByteView)>, StorageError> {
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::ReadError),
    }
}

pub fn empty_effects() -> Effects {
    smallvec::smallvec![]
}
