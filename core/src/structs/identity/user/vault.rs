//! The placed records of one user's vault: immutable vault revisions and public key records.
//! Holders keep every head; the node never opens a vault payload.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision, DocumentTarget};
use crate::errors::ConversionError;
use crate::keyspaces::{VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE};
use crate::storage_entries::{shard_manifest_entry, sync_revision_entry};
use crate::structs::placement::record::PlacementRef;
use crate::types::{Key, KeySpace, Value};
use crate::vault_format::key_fingerprint;
use crate::{NodeId, UserId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Bytes one vault payload may hold.
pub const MAX_VAULT_BYTES: usize = 64 * 1024;
/// Heads one save may name as its predecessors.
pub const MAX_PREDECESSORS: usize = 32;
/// Heads one read returns; a merge of them shrinks the rest.
pub const MAX_VAULT_HEADS: usize = 32;
/// Public key records one user may publish.
pub const MAX_KEY_RECORDS: usize = 64;
/// Bytes a key id may hold.
pub const MAX_KEY_ID_BYTES: usize = 128;

/// Why a vault or key record is not admissible.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VaultRecordError {
    #[error("a vault payload may hold at most 64 KiB")]
    TooLarge,
    #[error("a save may name at most {MAX_PREDECESSORS} distinct predecessors")]
    Predecessors,
    #[error("the key fingerprint does not match the public key")]
    Fingerprint,
    #[error("a key id holds 1 to {MAX_KEY_ID_BYTES} bytes")]
    KeyId,
}

/// The passphrase-sealed keys of one user. The payload is the portal's own
/// ciphertext and stays opaque; the node holds no key that opens it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserVault {
    pub user_id: UserId,
    pub payload: String,
    /// Bumped by every accepted write.
    pub revision: u64,
    pub updated_at: u64,
}

impl UserVault {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One immutable save of a user's vault. `None` as payload records a delete.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultRevision {
    pub user_id: UserId,
    pub revision_id: Ulid,
    /// The heads this save replaces; a merge names every head it read.
    pub predecessors: Vec<Ulid>,
    pub payload: Option<String>,
    /// The node that admitted the user's own request and published the record.
    pub node_id: NodeId,
    pub placement: PlacementRef,
    pub created_at_ms: u64,
}

/// One published public key of a user, readable by every realm node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserKeyRecord {
    pub user_id: UserId,
    pub record_id: Ulid,
    /// The id of the keypair in the vault `keys` slot.
    pub key_id: String,
    pub public_key: [u8; 32],
    pub fingerprint: [u8; 32],
    /// Declared by the client: the vault holds a recovery code.
    pub has_recovery: bool,
    pub node_id: NodeId,
    pub placement: PlacementRef,
    pub created_at_ms: u64,
}

/// Shows the payload size only, so no formatted message carries vault ciphertext.
impl std::fmt::Debug for VaultRevision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultRevision")
            .field("user_id", &self.user_id)
            .field("revision_id", &self.revision_id)
            .field("predecessors", &self.predecessors)
            .field("payload_bytes", &self.payload.as_ref().map(String::len))
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl VaultRevision {
    pub fn target(&self) -> DocumentTarget {
        DocumentTarget::VaultRevision {
            user_id: self.user_id,
            revision_id: self.revision_id,
        }
    }

    pub fn validate(&self) -> Result<(), VaultRecordError> {
        if self
            .payload
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_VAULT_BYTES)
        {
            return Err(VaultRecordError::TooLarge);
        }
        let mut seen = self.predecessors.clone();
        seen.sort_unstable();
        seen.dedup();
        if self.predecessors.len() > MAX_PREDECESSORS
            || seen.len() != self.predecessors.len()
            || seen.contains(&self.revision_id)
        {
            return Err(VaultRecordError::Predecessors);
        }
        Ok(())
    }

    pub fn sync_change(&self) -> DocumentChange {
        record_change(
            self.revision_id,
            self.node_id,
            self.created_at_ms,
            self.placement,
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl UserKeyRecord {
    pub fn target(&self) -> DocumentTarget {
        DocumentTarget::UserKey {
            user_id: self.user_id,
            record_id: self.record_id,
        }
    }

    pub fn validate(&self) -> Result<(), VaultRecordError> {
        if key_fingerprint(&self.public_key) != self.fingerprint {
            return Err(VaultRecordError::Fingerprint);
        }
        if self.key_id.is_empty() || self.key_id.len() > MAX_KEY_ID_BYTES {
            return Err(VaultRecordError::KeyId);
        }
        Ok(())
    }

    pub fn sync_change(&self) -> DocumentChange {
        record_change(
            self.record_id,
            self.node_id,
            self.created_at_ms,
            self.placement,
        )
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

fn record_change(
    event_id: Ulid,
    actor: NodeId,
    created_at_ms: u64,
    placement: PlacementRef,
) -> DocumentChange {
    DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: created_at_ms,
            event_id,
            actor,
            updated_at_ms: created_at_ms,
        },
        kind: DocumentChangeKind::Upsert,
        placement,
    }
}

/// Row key of one user's record: user storage key, then the record id.
pub fn user_record_key(user_id: UserId, record_id: Ulid) -> Key {
    let mut bytes = user_id.to_storage_key();
    bytes.extend_from_slice(&record_id.to_bytes());
    ByteView::from(bytes)
}

/// Prefix of every record row of one user.
pub fn user_record_prefix(user_id: UserId) -> Key {
    ByteView::from(user_id.to_storage_key())
}

/// Records a holder answers for one user: the vault heads or the public key records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VaultRecords {
    Heads(Vec<VaultRevision>),
    Keys(Vec<UserKeyRecord>),
}

impl VaultRecords {
    pub fn len(&self) -> usize {
        match self {
            Self::Heads(heads) => heads.len(),
            Self::Keys(keys) => keys.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Row of a placed record plus the revision and manifest rows a shard handover proves it by.
pub fn record_rows(
    target: &DocumentTarget,
    bytes: &[u8],
    change: &DocumentChange,
) -> Result<Vec<(KeySpace, Key, Value)>, ConversionError> {
    let mut rows = vec![
        (
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Value::from(bytes),
        ),
        sync_revision_entry(target, change)?,
    ];
    rows.extend(shard_manifest_entry(target, change)?);
    Ok(rows)
}

/// Writes and deletes that make `revision` a head. Every predecessor loses its
/// row and gains a retired marker, so a replaced save that arrives late stays retired.
pub fn head_rows(
    revision: &VaultRevision,
    bytes: &[u8],
) -> Result<(Vec<(KeySpace, Key, Value)>, Vec<(KeySpace, Key)>), ConversionError> {
    let mut writes = record_rows(&revision.target(), bytes, &revision.sync_change())?;
    let mut deletes = Vec::with_capacity(revision.predecessors.len());
    for predecessor in &revision.predecessors {
        let key = user_record_key(revision.user_id, *predecessor);
        writes.push((
            VAULT_RETIRED_KEYSPACE.to_string(),
            key.clone(),
            Value::from(&[][..]),
        ));
        deletes.push((VAULT_REVISION_KEYSPACE.to_string(), key));
    }
    Ok((writes, deletes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    fn user() -> UserId {
        UserId::new(Ulid::from_bytes([7; 16]), RealmId::from_bytes([3; 32]))
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[5; 32]).public()
    }

    fn revision(id: u8, predecessors: &[u8]) -> VaultRevision {
        VaultRevision {
            user_id: user(),
            revision_id: Ulid::from_bytes([id; 16]),
            predecessors: predecessors
                .iter()
                .map(|id| Ulid::from_bytes([*id; 16]))
                .collect(),
            payload: Some("sealed".to_string()),
            node_id: node(),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        }
    }

    #[test]
    fn retires_predecessors() {
        // A merge retires every head it names, even one this holder has not seen yet.
        let merge = revision(4, &[2, 3]);
        let (writes, deletes) = head_rows(&merge, &merge.to_bytes().unwrap()).unwrap();
        let retired: Vec<_> = writes
            .iter()
            .filter(|(key_space, _, _)| key_space == VAULT_RETIRED_KEYSPACE)
            .map(|(_, key, _)| key.clone())
            .collect();
        let removed: Vec<_> = deletes.iter().map(|(_, key)| key.clone()).collect();
        let expected = vec![
            user_record_key(user(), Ulid::from_bytes([2; 16])),
            user_record_key(user(), Ulid::from_bytes([3; 16])),
        ];
        assert_eq!(retired, expected);
        assert_eq!(removed, expected);
        assert!(
            deletes
                .iter()
                .all(|(key_space, _)| key_space == VAULT_REVISION_KEYSPACE)
        );
        assert!(
            writes
                .iter()
                .any(|(key_space, key, _)| key_space == VAULT_REVISION_KEYSPACE
                    && *key == user_record_key(user(), merge.revision_id))
        );
    }

    #[test]
    fn bounds_revisions() {
        let mut large = revision(1, &[]);
        large.payload = Some("x".repeat(MAX_VAULT_BYTES + 1));
        assert_eq!(large.validate(), Err(VaultRecordError::TooLarge));
        assert_eq!(
            revision(1, &[2, 2]).validate(),
            Err(VaultRecordError::Predecessors)
        );
        assert_eq!(
            revision(1, &[1]).validate(),
            Err(VaultRecordError::Predecessors)
        );
        let many: Vec<u8> = (2..=(MAX_PREDECESSORS as u8 + 2)).collect();
        assert_eq!(
            revision(1, &many).validate(),
            Err(VaultRecordError::Predecessors)
        );
        let mut deleted = revision(1, &[2]);
        deleted.payload = None;
        assert_eq!(deleted.validate(), Ok(()));
    }

    #[test]
    fn checks_key_fingerprint() {
        let public_key = [9u8; 32];
        let mut record = UserKeyRecord {
            user_id: user(),
            record_id: Ulid::from_bytes([1; 16]),
            key_id: "key-1".to_string(),
            public_key,
            fingerprint: key_fingerprint(&public_key),
            has_recovery: true,
            node_id: node(),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        };
        assert_eq!(record.validate(), Ok(()));
        record.fingerprint[0] ^= 1;
        assert_eq!(record.validate(), Err(VaultRecordError::Fingerprint));
    }
}
