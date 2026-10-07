//! Encodes and decodes the per-user index of active S3 access key ids.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{ABE_GRANT_KEYSPACE, ABE_REQUEST_KEYSPACE, TOKEN_GRANT_KEYSPACE};
use aruna_core::structs::storage::abe_access::token_prefix;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use std::collections::BTreeSet;

pub const MAX_ACTIVE_CREDENTIALS: usize = 16;
/// Token grant rows of one credential deleted per batch.
const TOKEN_PAGE: usize = 256;

pub fn owner_key(user_identity: UserId) -> Key {
    crate::owner_index::owner_key(user_identity, None)
}

pub fn decode_index(value: Option<&ByteView>) -> Result<BTreeSet<String>, ConversionError> {
    crate::owner_index::decode_index(
        value,
        MAX_ACTIVE_CREDENTIALS,
        || {
            ConversionError::InvalidLength(format!(
                "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
            ))
        },
        |index| {
            for access_key in index {
                UserAccess::build_access_key(access_key)?;
            }
            Ok(())
        },
    )
}

pub fn encode_index(index: &BTreeSet<String>) -> Result<Value, ConversionError> {
    crate::owner_index::encode_index(
        index,
        MAX_ACTIVE_CREDENTIALS,
        || {
            ConversionError::InvalidLength(format!(
                "credential owner index exceeds {MAX_ACTIVE_CREDENTIALS} entries"
            ))
        },
        |_| Ok(()),
    )
}

/// Reads one page of the token grant rows of `access_key`, after `start`, in `txn_id`.
pub fn token_scan(access_key: &str, start: Option<Key>, txn_id: TxnId) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: TOKEN_GRANT_KEYSPACE.to_string(),
        prefix: Some(token_prefix(access_key).into()),
        start: start.map(IterStart::After),
        limit: TOKEN_PAGE,
        txn_id: Some(txn_id),
    })
}

/// The key requests, grants and rows that a page of the token grant rows of `access_key` names.
pub fn token_deletes(
    access_key: &str,
    rows: Vec<(Key, Value)>,
) -> Result<Vec<(String, Key)>, ConversionError> {
    let prefix = token_prefix(access_key);
    let mut deletes = Vec::with_capacity(rows.len() * 3);
    for (key, _) in rows {
        let request = key
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| ConversionError::InvalidLength("token grant key".to_string()))?;
        deletes.push((ABE_REQUEST_KEYSPACE.to_string(), request.to_vec().into()));
        deletes.push((ABE_GRANT_KEYSPACE.to_string(), request.to_vec().into()));
        deletes.push((TOKEN_GRANT_KEYSPACE.to_string(), key));
    }
    Ok(deletes)
}
