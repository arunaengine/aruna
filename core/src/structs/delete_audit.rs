use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::realm::RealmId;
use crate::types::{GroupId, UserId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What a node-local delete removed. Markers and version deletes name one
/// object; a purge names the scope the job cleared.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobDeleteAuditKind {
    DeleteMarker,
    DeleteVersion,
    Purge(BlobPurgeScopeKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobPurgeScopeKind {
    File,
    Prefix,
    Bucket,
}

/// One node-local S3 deletion, written in the same transaction as the delete it
/// records. Deletes never propagate, so this trail is node-local as well.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobDeleteAuditRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub user_id: UserId,
    pub kind: BlobDeleteAuditKind,
    pub bucket: String,
    /// Object key for a marker or version delete, the purged prefix or key for a
    /// scoped purge, and empty when a whole bucket was purged.
    pub key: String,
    /// The marker version for `DeleteMarker`, the removed version for
    /// `DeleteVersion`, and absent for a purge.
    pub version_id: Option<Ulid>,
    pub occurred_at_ms: u64,
}

impl BlobDeleteAuditRecord {
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
