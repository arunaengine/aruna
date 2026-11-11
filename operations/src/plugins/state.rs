//! Private state of an install on its receiver: values by install and key, changed by compare
//! and swap, with the install's byte count and the call's settled reply in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_STATE_KEYSPACE;
use aruna_plugins::broker::{Entry, ErrorKind, HostError, StatePut};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::intents::settle_in;
use super::store::{StoreError, Txn, decode, encode, transact};
use crate::driver::DriverContext;

/// Bytes of one key, of one value, and of all keys and values of one install.
pub const MAX_STATE_KEY: usize = 256;
pub const MAX_STATE_VALUE: usize = 256 * 1024;
pub const MAX_STATE_BYTES: u64 = 16 << 20;

const VALUE_ROW: u8 = b'k';
const BYTES_ROW: u8 = b'b';

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StateRow {
    version: u64,
    value: Vec<u8>,
}

/// Why a put changed nothing, or failed in storage.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("{0}")]
    Refused(String, ErrorKind),
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn value_key(install_id: Ulid, key: &str) -> Vec<u8> {
    let mut row = vec![VALUE_ROW];
    row.extend_from_slice(&install_id.to_bytes());
    row.extend_from_slice(key.as_bytes());
    row
}

fn bytes_key(install_id: Ulid) -> Vec<u8> {
    let mut row = vec![BYTES_ROW];
    row.extend_from_slice(&install_id.to_bytes());
    row
}

/// A key the guest may use: not empty, within its bound.
pub fn check_key(key: &str) -> Result<(), HostError> {
    match key.is_empty() || key.len() > MAX_STATE_KEY {
        true => Err(HostError::new(
            ErrorKind::Invalid,
            "a state key has 1 to 256 bytes",
        )),
        false => Ok(()),
    }
}

pub async fn read_state(
    ctx: &DriverContext,
    install_id: Ulid,
    key: &str,
) -> Result<Option<Entry>, StoreError> {
    let row = (Txn::direct(ctx))
        .read(PLUGIN_STATE_KEYSPACE, &value_key(install_id, key))
        .await?;
    let row = row.map(|bytes| decode::<StateRow>(&bytes)).transpose()?;
    Ok(row.map(|row| Entry {
        version: row.version,
        value: row.value,
    }))
}

/// Writes the value if the stored version is still `expected` (`None`: no value yet) and the
/// install stays within its bytes, and settles the call's intent with the new version.
pub async fn put_state(
    ctx: &DriverContext,
    run_id: Ulid,
    install_id: Ulid,
    put: &StatePut,
) -> Result<u64, PutError> {
    if put.value.len() > MAX_STATE_VALUE {
        let message = "a state value has at most 256 KiB".to_string();
        return Err(PutError::Refused(message, ErrorKind::Limit));
    }
    transact(ctx, async |txn: &Txn<'_>| {
        let key = value_key(install_id, &put.key);
        let current = match txn.read(PLUGIN_STATE_KEYSPACE, &key).await? {
            Some(bytes) => Some(decode::<StateRow>(&bytes)?),
            None => None,
        };
        let version = current.as_ref().map(|row| row.version);
        if version != put.expected {
            let message = format!("the value is at version {version:?}");
            return Err(PutError::Refused(message, ErrorKind::Conflict));
        }
        let total = match txn
            .read(PLUGIN_STATE_KEYSPACE, &bytes_key(install_id))
            .await?
        {
            Some(bytes) => decode::<u64>(&bytes)?,
            None => 0,
        };
        let size = |value: &[u8]| (put.key.len() + value.len()) as u64;
        let old = current.as_ref().map_or(0, |row| size(&row.value));
        let total = (total.saturating_sub(old)).saturating_add(size(&put.value));
        if total > MAX_STATE_BYTES {
            let message = "the install's state would exceed 16 MiB".to_string();
            return Err(PutError::Refused(message, ErrorKind::Limit));
        }
        let next = version.map_or(1, |version| version + 1);
        let row = StateRow {
            version: next,
            value: put.value.clone(),
        };
        let rows = vec![
            (key, encode(&row)?),
            (bytes_key(install_id), encode(&total)?),
        ];
        txn.write(PLUGIN_STATE_KEYSPACE, rows).await?;
        settle_in(txn, run_id, &put.intent, next.to_string()).await?;
        Ok(next)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::intents::{Begun, begin};
    use crate::plugins::runs::tests::opened;
    use aruna_plugins::broker::Intent;

    fn put(key: &str, value: &[u8], expected: Option<u64>, ordinal: u32) -> StatePut {
        StatePut {
            key: key.into(),
            value: value.to_vec(),
            expected,
            intent: Intent::new(ordinal, None, &[key.as_bytes(), value]),
        }
    }

    #[tokio::test]
    async fn compare_and_swap() {
        let (_dir, ctx) = opened();
        let (run, install, other) = (
            Ulid::from_parts(9, 1),
            Ulid::from_parts(1, 1),
            Ulid::from_parts(1, 2),
        );
        let first = put("cursor", b"a", None, 0);
        assert_eq!(put_state(&ctx, run, install, &first).await.unwrap(), 1);
        assert_eq!(
            begin(&ctx, run, &first.intent).await.unwrap(),
            Begun::Settled("1".into())
        );
        let stale = put_state(&ctx, run, install, &put("cursor", b"b", None, 1)).await;
        assert!(matches!(
            stale,
            Err(PutError::Refused(_, ErrorKind::Conflict))
        ));
        assert_eq!(
            put_state(&ctx, run, install, &put("cursor", b"b", Some(1), 2))
                .await
                .unwrap(),
            2
        );
        let entry = read_state(&ctx, install, "cursor").await.unwrap().unwrap();
        assert_eq!((entry.version, entry.value), (2, b"b".to_vec()));
        assert_eq!(read_state(&ctx, other, "cursor").await.unwrap(), None);

        // The bytes of all keys of one install are bounded.
        let big = vec![0u8; MAX_STATE_VALUE];
        let count = MAX_STATE_BYTES as usize / MAX_STATE_VALUE;
        for n in 1..count {
            let key = format!("k{n}");
            put_state(&ctx, run, install, &put(&key, &big, None, 10 + n as u32))
                .await
                .unwrap();
        }
        let full = put_state(&ctx, run, install, &put("last", &big, None, 900)).await;
        assert!(matches!(full, Err(PutError::Refused(_, ErrorKind::Limit))));
        assert!(
            put_state(&ctx, run, other, &put("last", &big, None, 901))
                .await
                .is_ok()
        );
        assert!(check_key("").is_err() && check_key(&"k".repeat(257)).is_err());
    }
}
