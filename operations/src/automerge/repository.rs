use automerge::{AutoCommit, ChangeHash};
use byteview::ByteView;

use aruna_core::automerge::{AutomergeClock, AutomergeDocumentVariant};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::structs::RealmId;
use aruna_core::types::{Effects, GroupId, Key, TxnId};

pub fn storage_keyspace(document: &AutomergeDocumentVariant) -> &'static str {
    match document {
        AutomergeDocumentVariant::Group { .. } => GROUP_KEYSPACE,
        AutomergeDocumentVariant::GroupAuthorization { .. }
        | AutomergeDocumentVariant::RealmAuthorization { .. } => AUTH_KEYSPACE,
        AutomergeDocumentVariant::RealmConfig { .. } => REALM_CONFIG_KEYSPACE,
        AutomergeDocumentVariant::User { .. } => USER_KEYSPACE,
    }
}

pub fn storage_key(document: &AutomergeDocumentVariant) -> Key {
    match document {
        AutomergeDocumentVariant::Group { group_id } => {
            ByteView::from(group_id.to_bytes().to_vec())
        }
        AutomergeDocumentVariant::GroupAuthorization { group_id } => {
            ByteView::from(group_id.to_bytes().to_vec())
        }
        AutomergeDocumentVariant::RealmAuthorization { realm_id } => {
            ByteView::from(realm_id.as_bytes().to_vec())
        }
        AutomergeDocumentVariant::RealmConfig { realm_id } => {
            ByteView::from(realm_id.as_bytes().to_vec())
        }
        AutomergeDocumentVariant::User { user_id } => ByteView::from(user_id.to_bytes()),
    }
}

pub fn read_effect(document: &AutomergeDocumentVariant, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: storage_keyspace(document).to_string(),
        key: storage_key(document),
        txn_id,
    })
}

pub fn write_effect(
    document: &AutomergeDocumentVariant,
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

pub fn delete_effect(document: &AutomergeDocumentVariant, txn_id: Option<TxnId>) -> Effect {
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

pub fn automerge_heads(bytes: &[u8]) -> Result<Vec<ChangeHash>, ConversionError> {
    Ok(automerge_clock(bytes)?.heads)
}

pub fn automerge_clock(bytes: &[u8]) -> Result<AutomergeClock, ConversionError> {
    if bytes.is_empty() {
        return Ok(AutomergeClock::new(Vec::new(), 0));
    }

    let mut doc = AutoCommit::load(bytes)?;
    let heads = doc.get_heads();
    let change_count = doc.get_changes(&[]).len() as u64;
    Ok(AutomergeClock::new(heads, change_count))
}

pub fn parse_auth_document(key: &[u8]) -> Result<AutomergeDocumentVariant, ConversionError> {
    match key.len() {
        16 => {
            let mut group_bytes = [0u8; 16];
            group_bytes.copy_from_slice(key);
            Ok(AutomergeDocumentVariant::GroupAuthorization {
                group_id: GroupId::from_bytes(group_bytes),
            })
        }
        32 => {
            let mut realm_bytes = [0u8; 32];
            realm_bytes.copy_from_slice(key);
            Ok(AutomergeDocumentVariant::RealmAuthorization {
                realm_id: RealmId::from_bytes(realm_bytes),
            })
        }
        other => Err(ConversionError::InvalidLength(format!(
            "unexpected auth key length {other}"
        ))),
    }
}

pub fn parse_group_document(key: &[u8]) -> Result<AutomergeDocumentVariant, ConversionError> {
    if key.len() != 16 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected group key length {}",
            key.len()
        )));
    }

    let mut group_bytes = [0u8; 16];
    group_bytes.copy_from_slice(key);
    Ok(AutomergeDocumentVariant::Group {
        group_id: GroupId::from_bytes(group_bytes),
    })
}

pub fn parse_realm_config_document(
    key: &[u8],
) -> Result<AutomergeDocumentVariant, ConversionError> {
    if key.len() != 32 {
        return Err(ConversionError::InvalidLength(format!(
            "unexpected realm config key length {}",
            key.len()
        )));
    }

    let mut realm_bytes = [0u8; 32];
    realm_bytes.copy_from_slice(key);
    Ok(AutomergeDocumentVariant::RealmConfig {
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

pub fn maybe_changed(before: &[ChangeHash], after: &[ChangeHash]) -> bool {
    before != after
}

pub fn empty_effects() -> Effects {
    smallvec::smallvec![]
}
