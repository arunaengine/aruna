//! Seals node-local secret rows at rest, bound to their keyspace and key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::credential_encryption::{CredentialEncryptionKey, open_bytes, seal_bytes};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{
    BACKEND_SECRET_KEYSPACE, CONNECTOR_SECRET_KEYSPACE, SOURCE_SECRET_KEYSPACE,
};
use aruna_core::types::{Key, Value};

/// Keyspaces whose rows hold plain connector or backend credentials.
pub const SEALED_KEYSPACES: [&str; 3] = [
    BACKEND_SECRET_KEYSPACE,
    SOURCE_SECRET_KEYSPACE,
    CONNECTOR_SECRET_KEYSPACE,
];

/// Which sealed rows a read returns, captured before its effect is dispatched.
pub(super) enum Opening {
    /// The keyspace of each requested row of a `Read` or `BatchRead`.
    Rows(Vec<String>),
    /// The keyspace of an `Iter` or `Last`.
    Scan(String),
}

fn is_sealed(key_space: &str) -> bool {
    SEALED_KEYSPACES.contains(&key_space)
}

/// Binds a sealed value to its row, so a value copied to another row never opens.
pub fn row_aad(key_space: &str, key: &[u8]) -> Vec<u8> {
    [key_space.as_bytes(), &[0], key].concat()
}

fn seal(
    secret_key: &CredentialEncryptionKey,
    key_space: &str,
    key: &Key,
    value: &Value,
) -> Result<Value, StorageError> {
    seal_bytes(secret_key, value, &row_aad(key_space, key))
        .map(Value::from)
        .map_err(|_| StorageError::WriteError("secret row could not be sealed".into()))
}

fn open(
    secret_key: &CredentialEncryptionKey,
    key_space: &str,
    key: &Key,
    value: &Value,
) -> Result<Value, StorageError> {
    open_bytes(secret_key, value, &row_aad(key_space, key))
        .map(Value::from)
        .map_err(|_| StorageError::ReadError("secret row could not be opened".into()))
}

/// Seals written secret rows and records which read results must be opened.
pub(super) fn seal_effect(
    secret_key: &CredentialEncryptionKey,
    mut effect: StorageEffect,
) -> Result<(StorageEffect, Option<Opening>), StorageError> {
    let opening = match &mut effect {
        StorageEffect::Write {
            key_space,
            key,
            value,
            ..
        } if is_sealed(key_space) => {
            *value = seal(secret_key, key_space, key, value)?;
            None
        }
        StorageEffect::BatchWrite { writes, .. } => {
            for (key_space, key, value) in writes.iter_mut() {
                if is_sealed(key_space) {
                    *value = seal(secret_key, key_space, key, value)?;
                }
            }
            None
        }
        StorageEffect::Read { key_space, .. } if is_sealed(key_space) => {
            Some(Opening::Rows(vec![key_space.clone()]))
        }
        StorageEffect::BatchRead { reads, .. }
            if reads.iter().any(|(key_space, _)| is_sealed(key_space)) =>
        {
            Some(Opening::Rows(
                reads
                    .iter()
                    .map(|(key_space, _)| key_space.clone())
                    .collect(),
            ))
        }
        StorageEffect::Iter { key_space, .. } | StorageEffect::Last { key_space, .. }
            if is_sealed(key_space) =>
        {
            Some(Opening::Scan(key_space.clone()))
        }
        _ => None,
    };
    Ok((effect, opening))
}

/// Opens the sealed values of a read result; a row that does not open fails the read.
pub(super) fn open_event(
    secret_key: &CredentialEncryptionKey,
    opening: Opening,
    mut event: StorageEvent,
) -> Result<StorageEvent, StorageError> {
    let opened = match (&opening, &mut event) {
        (
            Opening::Rows(spaces),
            StorageEvent::ReadResult {
                key,
                value: Some(value),
            },
        ) => open(secret_key, &spaces[0], key, value).map(|plain| *value = plain),
        (Opening::Rows(spaces), StorageEvent::BatchReadResult { values }) => spaces
            .iter()
            .zip(values.iter_mut())
            .filter(|(key_space, _)| is_sealed(key_space))
            .try_for_each(|(key_space, (key, value))| match value {
                Some(value) => open(secret_key, key_space, key, value).map(|plain| *value = plain),
                None => Ok(()),
            }),
        (Opening::Scan(key_space), StorageEvent::IterResult { values, .. }) => {
            values.iter_mut().try_for_each(|(key, value)| {
                open(secret_key, key_space, key, value).map(|plain| *value = plain)
            })
        }
        _ => Ok(()),
    };
    opened.map(|()| event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FjallStorage;
    use aruna_core::events::Event;

    const CANARY: &[u8] = b"canary-9a7c";

    fn write(key: &[u8]) -> StorageEffect {
        StorageEffect::Write {
            key_space: SOURCE_SECRET_KEYSPACE.to_string(),
            key: key.to_vec().into(),
            value: CANARY.to_vec().into(),
            txn_id: None,
        }
    }

    fn sealed_value(effect: StorageEffect) -> Value {
        let StorageEffect::Write { value, .. } = effect else {
            panic!("write expected");
        };
        value
    }

    #[test]
    fn rows_bind_placement() {
        let secret_key = CredentialEncryptionKey::derive(&[3u8; 32]);
        let (effect, opening) = seal_effect(&secret_key, write(b"row-a")).unwrap();
        assert!(opening.is_none());
        let value = sealed_value(effect);
        assert!(!value.windows(CANARY.len()).any(|window| window == CANARY));
        let read = |key: &[u8], secret_key: &CredentialEncryptionKey| {
            open_event(
                secret_key,
                Opening::Rows(vec![SOURCE_SECRET_KEYSPACE.to_string()]),
                StorageEvent::ReadResult {
                    key: key.to_vec().into(),
                    value: Some(value.clone()),
                },
            )
        };
        let Ok(StorageEvent::ReadResult {
            value: Some(plain), ..
        }) = read(b"row-a", &secret_key)
        else {
            panic!("sealed row must open");
        };
        assert_eq!(plain.as_ref(), CANARY);
        // A value moved to another row or read by another node never opens.
        assert!(read(b"row-b", &secret_key).is_err());
        assert!(read(b"row-a", &CredentialEncryptionKey::derive(&[4u8; 32])).is_err());
    }

    #[tokio::test]
    async fn handle_opens_rows() {
        let dir = tempfile::tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        handle.seal_secrets(CredentialEncryptionKey::derive(&[3u8; 32]));
        handle.send_storage_effect(write(b"row-a")).await;
        let reads = [
            StorageEffect::Read {
                key_space: SOURCE_SECRET_KEYSPACE.to_string(),
                key: b"row-a".to_vec().into(),
                txn_id: None,
            },
            StorageEffect::BatchRead {
                reads: vec![(SOURCE_SECRET_KEYSPACE.to_string(), b"row-a".to_vec().into())],
                txn_id: None,
            },
            StorageEffect::Iter {
                key_space: SOURCE_SECRET_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 10,
                txn_id: None,
            },
        ];
        for read in reads {
            let value = match handle.send_storage_effect(read).await {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => value,
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    values[0].1.clone().unwrap()
                }
                Event::Storage(StorageEvent::IterResult { values, .. }) => values[0].1.clone(),
                other => panic!("unexpected read result {other:?}"),
            };
            assert_eq!(value.as_ref(), CANARY);
        }
    }
}
