//! Runs node vault effects as sealed rows of the node vault keyspace and opens vault reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::NODE_VAULT_KEYSPACE;
use aruna_core::node_vault::{NodeVaultKey, VaultEntry};

fn not_open() -> StorageError {
    StorageError::KeyspaceError("node vault is not open".to_string())
}

/// Turns a vault effect into a row effect; the returned entry marks a read to open.
pub(super) fn vault_effect(
    vault_key: Option<&NodeVaultKey>,
    effect: StorageEffect,
) -> Result<(StorageEffect, Option<VaultEntry>), StorageError> {
    let key_space = NODE_VAULT_KEYSPACE.to_string();
    Ok(match effect {
        StorageEffect::VaultWrite {
            entry,
            secret,
            txn_id,
        } => {
            let value = vault_key
                .ok_or_else(not_open)?
                .seal(entry, secret.bytes())
                .map_err(|error| StorageError::WriteError(error.to_string()))?;
            let write = StorageEffect::Write {
                key_space,
                key: entry.key().into(),
                value: value.into(),
                txn_id,
            };
            (write, None)
        }
        StorageEffect::VaultRead { entry, txn_id } => {
            vault_key.ok_or_else(not_open)?;
            let read = StorageEffect::Read {
                key_space,
                key: entry.key().into(),
                txn_id,
            };
            (read, Some(entry))
        }
        StorageEffect::VaultDelete { entry, txn_id } => {
            let delete = StorageEffect::Delete {
                key_space,
                key: entry.key().into(),
                txn_id,
            };
            (delete, None)
        }
        other => (other, None),
    })
}

/// Opens the row a vault read returned; a row that does not open fails the read.
pub(super) fn vault_event(
    vault_key: Option<&NodeVaultKey>,
    entry: VaultEntry,
    event: StorageEvent,
) -> Result<StorageEvent, StorageError> {
    let StorageEvent::ReadResult { value, .. } = event else {
        return Ok(event);
    };
    let secret = match value {
        Some(value) => Some(
            vault_key
                .ok_or_else(not_open)?
                .open(entry, &value)
                .map_err(|error| StorageError::ReadError(error.to_string()))?,
        ),
        None => None,
    };
    Ok(StorageEvent::VaultResult { entry, secret })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FjallStorage, StorageHandle};
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::events::Event;
    use aruna_core::node_vault::VaultPurpose;
    use aruna_core::types::{TxnId, Value};
    use ulid::Ulid;

    const CANARY: &[u8] = b"canary-0b6e";

    fn entry(purpose: VaultPurpose, seed: u8) -> VaultEntry {
        VaultEntry::new(purpose, Ulid::from_bytes([seed; 16]))
    }

    fn write(entry: VaultEntry, txn_id: Option<TxnId>) -> StorageEffect {
        StorageEffect::VaultWrite {
            entry,
            secret: SharedSecret::new(SecretBytes::new(CANARY.to_vec())),
            txn_id,
        }
    }

    /// True when `text` shows the canary as text or as a list of bytes.
    fn shows_canary(text: &str) -> bool {
        let bytes = format!("{CANARY:?}");
        text.contains("canary") || text.contains(bytes.trim_matches(['[', ']']))
    }

    fn open(path: &str, node_secret: Option<u8>) -> StorageHandle {
        let handle = FjallStorage::open(path).unwrap();
        if let Some(seed) = node_secret {
            handle.open_vault(NodeVaultKey::derive(&[seed; 32]));
        }
        handle
    }

    async fn read(handle: &StorageHandle, entry: VaultEntry) -> StorageEvent {
        let effect = StorageEffect::VaultRead {
            entry,
            txn_id: None,
        };
        let Event::Storage(event) = handle.send_storage_effect(effect).await else {
            panic!("storage event expected");
        };
        event
    }

    async fn opened(handle: &StorageHandle, entry: VaultEntry) -> Option<Vec<u8>> {
        match read(handle, entry).await {
            StorageEvent::VaultResult { secret, .. } => {
                secret.map(|secret| secret.expose().to_vec())
            }
            other => panic!("vault result expected, got {other:?}"),
        }
    }

    async fn row(handle: &StorageHandle, entry: VaultEntry) -> Option<Value> {
        let effect = StorageEffect::Read {
            key_space: NODE_VAULT_KEYSPACE.to_string(),
            key: entry.key().into(),
            txn_id: None,
        };
        match handle.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("read result expected, got {other:?}"),
        }
    }

    #[test]
    fn formats_hide_canary() {
        let key = NodeVaultKey::derive(&[3u8; 32]);
        let source = entry(VaultPurpose::SourceConnector, 1);
        let (sealed, _) = vault_effect(Some(&key), write(source, None)).unwrap();
        let event = StorageEvent::VaultResult {
            entry: source,
            secret: Some(SecretBytes::new(CANARY.to_vec())),
        };
        let texts = [
            format!("{:?}", write(source, None)),
            format!("{sealed:?}"),
            format!("{event:?}"),
        ];
        for text in texts {
            assert!(!shows_canary(&text), "{text}");
        }
        assert!(shows_canary(&format!("{:?}", Value::from(CANARY))));
    }

    #[tokio::test]
    async fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let source = entry(VaultPurpose::SourceConnector, 1);
        let handle = open(path, Some(3));
        let event = handle.send_storage_effect(write(source, None)).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let stored = row(&handle, source).await.unwrap();
        assert!(!shows_canary(&format!("{stored:?}")));
        handle.close().await;

        let handle = open(path, Some(3));
        assert_eq!(opened(&handle, source).await.as_deref(), Some(CANARY));
        let other = entry(VaultPurpose::SourceConnector, 2);
        assert_eq!(opened(&handle, other).await, None);
        handle.close().await;

        // Another node secret never opens the record.
        let handle = open(path, Some(4));
        let failed = read(&handle, source).await;
        assert!(matches!(failed, StorageEvent::Error { .. }), "{failed:?}");
        assert!(!shows_canary(&format!("{failed:?}")));
    }

    #[tokio::test]
    async fn rows_bind_entry() {
        let dir = tempfile::tempdir().unwrap();
        let handle = open(dir.path().to_str().unwrap(), Some(3));
        let source = entry(VaultPurpose::SourceConnector, 1);
        handle.send_storage_effect(write(source, None)).await;
        let stored = row(&handle, source).await.unwrap();

        let moved = [
            entry(VaultPurpose::SourceConnector, 2),
            entry(VaultPurpose::GroupBackend, 1),
        ];
        for target in moved {
            let copy = StorageEffect::Write {
                key_space: NODE_VAULT_KEYSPACE.to_string(),
                key: target.key().into(),
                value: stored.clone(),
                txn_id: None,
            };
            handle.send_storage_effect(copy).await;
            let event = read(&handle, target).await;
            assert!(matches!(event, StorageEvent::Error { .. }), "{event:?}");
        }
    }

    #[tokio::test]
    async fn aborted_write_vanishes() {
        let dir = tempfile::tempdir().unwrap();
        let handle = open(dir.path().to_str().unwrap(), Some(3));
        let backend = entry(VaultPurpose::GroupBackend, 1);
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("transaction expected");
        };
        handle
            .send_storage_effect(write(backend, Some(txn_id)))
            .await;
        handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        assert_eq!(opened(&handle, backend).await, None);

        handle.send_storage_effect(write(backend, None)).await;
        let delete = StorageEffect::VaultDelete {
            entry: backend,
            txn_id: None,
        };
        let event = handle.send_storage_effect(delete).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
        assert_eq!(row(&handle, backend).await, None);
    }

    #[tokio::test]
    async fn closed_vault_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let handle = open(dir.path().to_str().unwrap(), None);
        let backend = entry(VaultPurpose::GroupBackend, 1);
        let event = handle.send_storage_effect(write(backend, None)).await;
        assert!(matches!(event, Event::Storage(StorageEvent::Error { .. })));
        assert_eq!(row(&handle, backend).await, None);
        assert!(matches!(
            read(&handle, backend).await,
            StorageEvent::Error { .. }
        ));
    }
}
