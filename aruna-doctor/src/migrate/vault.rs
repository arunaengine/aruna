//! Moves source connector and group backend secrets from their old keyspaces into the node
//! vault. Rows sealed with the S3 credential key and plain rows both move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{decode_error, row_note};
use crate::explorer::ExplorerError;
use aruna_core::compute::SecretBytes;
use aruna_core::credential_encryption::{CredentialEncryptionKey, open_bytes};
use aruna_core::keyspaces::{BACKEND_SECRET_KEYSPACE, SOURCE_SECRET_KEYSPACE};
use aruna_core::node_vault::{NodeVaultKey, VaultEntry, VaultPurpose};
use aruna_core::structs::execution::source_connector::SourceConnectorSecret;
use aruna_core::structs::storage::group_backend::GroupStorageSecret;
use aruna_storage::row_aad;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use ulid::Ulid;

/// Old secret keyspaces and the vault purpose of their rows.
pub(super) const MOVED_KEYSPACES: [(&str, VaultPurpose); 2] = [
    (SOURCE_SECRET_KEYSPACE, VaultPurpose::SourceConnector),
    (BACKEND_SECRET_KEYSPACE, VaultPurpose::GroupBackend),
];

/// Vault rows to write and old rows to remove for one keyspace.
pub(super) struct Moves {
    pub(super) scanned: usize,
    pub(super) rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) removed: Vec<Vec<u8>>,
}

/// Rows that open neither way are added to `skipped` and left where they are.
pub(super) fn vault_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    (name, purpose): (&str, VaultPurpose),
    keys: Option<&(CredentialEncryptionKey, NodeVaultKey)>,
    skipped: &mut Vec<String>,
) -> Result<Moves, ExplorerError> {
    let plain = |value: &[u8]| match purpose {
        VaultPurpose::SourceConnector => SourceConnectorSecret::from_bytes(value).is_ok(),
        VaultPurpose::GroupBackend => GroupStorageSecret::from_bytes(value).is_ok(),
    };
    let mut moves = Moves {
        scanned: 0,
        rows: Vec::new(),
        removed: Vec::new(),
    };
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        moves.scanned += 1;
        let Some((secret_key, vault_key)) = keys else {
            skipped.push(row_note(name, &key, "no node state to derive the key from"));
            continue;
        };
        let Ok(id) = <[u8; 16]>::try_from(key.as_ref()) else {
            skipped.push(row_note(name, &key, "key is not an id"));
            continue;
        };
        let secret = match open_bytes(secret_key, &value, &row_aad(name, &key)) {
            Ok(opened) => opened,
            Err(_) if plain(&value) => value.to_vec(),
            Err(_) => {
                skipped.push(row_note(name, &key, "neither sealed nor a plain secret"));
                continue;
            }
        };
        let entry = VaultEntry::new(purpose, Ulid::from_bytes(id));
        let sealed = vault_key
            .seal(entry, &SecretBytes::new(secret))
            .map_err(|error| decode_error(name, &key, error))?;
        moves.rows.push((entry.key(), sealed));
        moves.removed.push(key.to_vec());
    }
    Ok(moves)
}
