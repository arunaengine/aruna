//! Records node-local blob deletes: markers, version deletes and purges, with their scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::NodeId;
use crate::UserId;
use crate::errors::ConversionError;
use crate::structs::identity::realm::RealmId;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What a node-local delete removed. Markers and version deletes name one
/// object; a purge names the scope the job cleared.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobAuditKind {
    DeleteMarker,
    DeleteVersion,
    Purge(BlobPurgeKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobPurgeKind {
    File,
    Prefix,
    Bucket,
}

/// One node-local S3 deletion, written in the same transaction as the delete it
/// records. Deletes never propagate, so this trail is node-local as well.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobAuditRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub user_id: UserId,
    pub kind: BlobAuditKind,
    pub bucket: String,
    /// Object key for a marker or version delete, the purged prefix or key for a
    /// scoped purge, and empty when a whole bucket was purged.
    pub key: String,
    /// The marker version for `DeleteMarker`, the removed version for
    /// `DeleteVersion`, and absent for a purge.
    pub version_id: Option<Ulid>,
    pub occurred_at_ms: u64,
}

impl BlobAuditRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Group prefix followed by a time-ordered id, so one group's trail scans in
/// deletion order.
pub fn delete_audit_key(group_id: GroupId, audit_id: Ulid) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&audit_id.to_bytes());
    bytes
}

/// Where `record` is indexed in the trail by key. The audit id keeps records apart.
pub fn trail_key(record: &BlobAuditRecord, audit_id: Ulid) -> Vec<u8> {
    let mut key = match record.kind {
        BlobAuditKind::Purge(_) => purge_prefix(record.group_id),
        _ => delete_prefix(record.group_id, &record.bucket, &record.key),
    };
    key.extend_from_slice(&audit_id.to_bytes());
    key
}

/// The marker and version deletes of one object key. Lengths keep a key apart from longer ones.
pub fn delete_prefix(group_id: GroupId, bucket: &str, key: &str) -> Vec<u8> {
    let mut bytes = group_id.to_bytes().to_vec();
    bytes.push(0);
    for part in [bucket, key] {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes
}

/// The purges of one group, whatever their scope.
pub fn purge_prefix(group_id: GroupId) -> Vec<u8> {
    let mut bytes = group_id.to_bytes().to_vec();
    bytes.push(1);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_trail() {
        let group = Ulid::from_parts(1, 1);
        let record = |kind, key: &str| BlobAuditRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: group,
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
            user_id: Default::default(),
            kind,
            bucket: "b".to_string(),
            key: key.to_string(),
            version_id: None,
            occurred_at_ms: 0,
        };
        let id = Ulid::from_parts(2, 2);
        let marker = trail_key(&record(BlobAuditKind::DeleteMarker, "a"), id);
        assert!(marker.starts_with(&delete_prefix(group, "b", "a")));
        // A longer key or another bucket never shares the prefix of a key.
        let longer = trail_key(&record(BlobAuditKind::DeleteVersion, "ab"), id);
        assert!(!longer.starts_with(&delete_prefix(group, "b", "a")));
        assert!(!marker.starts_with(&delete_prefix(group, "ba", "")));
        let purge = trail_key(
            &record(BlobAuditKind::Purge(BlobPurgeKind::Prefix), "a"),
            id,
        );
        assert!(purge.starts_with(&purge_prefix(group)));
        assert!(!purge.starts_with(&delete_prefix(group, "b", "a")));
    }
}
