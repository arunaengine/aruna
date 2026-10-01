//! Builds node vault read, write and delete effects and checks their results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::node_vault::VaultEntry;
use aruna_core::types::TxnId;

use crate::storage_read::StorageReadError;

pub fn read_secret(entry: VaultEntry, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::VaultRead { entry, txn_id })
}

pub fn write_secret(entry: VaultEntry, secret: SecretBytes, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::VaultWrite {
        entry,
        secret,
        txn_id,
    })
}

pub fn delete_secret(entry: VaultEntry, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::VaultDelete { entry, txn_id })
}

/// Decodes the vault record read for `entry`; a result for another entry is an error.
pub fn parse_secret<T>(
    event: Event,
    entry: VaultEntry,
    parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::VaultResult {
            entry: read,
            secret,
        }) if read == entry => secret
            .map(|secret| parse(secret.expose()).map_err(StorageReadError::Conversion))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError(
            "unexpected event".to_string(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::node_vault::VaultPurpose;
    use ulid::Ulid;

    #[test]
    fn rejects_other_entry() {
        let entry = VaultEntry::new(VaultPurpose::GroupBackend, Ulid::from_bytes([1u8; 16]));
        let other = VaultEntry::new(VaultPurpose::GroupBackend, Ulid::from_bytes([2u8; 16]));
        let event = |entry| {
            Event::Storage(StorageEvent::VaultResult {
                entry,
                secret: Some(SecretBytes::new(b"canary-c4d2".to_vec())),
            })
        };
        let parse = |bytes: &[u8]| Ok(bytes.to_vec());
        assert_eq!(
            parse_secret(event(entry), entry, parse).unwrap().unwrap(),
            b"canary-c4d2"
        );
        let error = parse_secret(event(other), entry, parse).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("canary"));
    }
}
