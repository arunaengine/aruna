//! Defines the realm's database list: bounded pointers from a bucket to the dataset that
//! describes it, and the owner consent a curator's mark needs when the bucket's group admins
//! are other people. Entries hold only ids; texts stay in the dataset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::UserId;
use crate::admin_documents::AdminDocumentDot;
use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::data_link::{DataLink, MAX_BUCKET_BYTES};
use crate::types::GroupId;

/// At most this many database entries per realm.
pub const MAX_DATABASES: usize = 1024;
/// The encoded database list stays below this size.
pub const MAX_DATABASE_BYTES: usize = 256 * 1024;
const CONSENT_DOMAIN: &[u8] = b"aruna-database-consent-v1";

/// The permission path whose WRITE allows marking buckets as databases.
pub fn database_admin_path(realm_id: RealmId) -> String {
    format!("/{realm_id}/admin/databases")
}

/// The admin path of the group that owns a database's bucket: its consent and removal right.
pub fn owner_admin_path(realm_id: RealmId, group_id: GroupId) -> String {
    format!("/{realm_id}/g/{group_id}/admin")
}

/// The bucket of a database: a distributed bucket by its set, an ordinary one by node and name.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DatabaseTarget {
    Distributed { set_id: Ulid },
    Ordinary { node_id: NodeId, bucket: String },
}

impl DatabaseTarget {
    /// The bucket root link the dataset must name as a Dataset-typed `hasPart`.
    pub fn root_link(&self, realm_id: RealmId) -> DataLink {
        match self {
            Self::Distributed { set_id } => DataLink::SetPrefix {
                realm_id,
                set_id: *set_id,
                prefix: String::new(),
            },
            Self::Ordinary { node_id, bucket } => DataLink::NodePrefix {
                realm_id,
                node_id: *node_id,
                bucket: bucket.clone(),
                prefix: String::new(),
            },
        }
    }
}

/// One database: the bucket, the group that owns it and the dataset that describes it.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct DatabaseEntry {
    pub entry_id: Ulid,
    pub dataset_id: Ulid,
    pub target: DatabaseTarget,
    pub owner_group: GroupId,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DatabaseError {
    #[error("at most {MAX_DATABASES} databases fit in the realm configuration")]
    TooMany,
    #[error("the database list exceeds {MAX_DATABASE_BYTES} bytes")]
    TooLarge,
    #[error("database entry ids must be unique")]
    DuplicateEntry,
    #[error("a database bucket name holds 1 to {MAX_BUCKET_BYTES} bytes without `/`")]
    InvalidBucket,
    #[error("the owner consent does not match the entry it is published with")]
    ConsentMismatch,
    #[error("the owner consent is not signed by the node it names")]
    ConsentSignature,
}

impl DatabaseEntry {
    pub fn validate(&self) -> Result<(), DatabaseError> {
        match &self.target {
            DatabaseTarget::Ordinary { bucket, .. }
                if bucket.is_empty() || bucket.len() > MAX_BUCKET_BYTES || bucket.contains('/') =>
            {
                Err(DatabaseError::InvalidBucket)
            }
            _ => Ok(()),
        }
    }
}

/// Bounds the materialized list every node and device replicates.
pub fn validate_databases(entries: &[DatabaseEntry]) -> Result<(), DatabaseError> {
    if entries.len() > MAX_DATABASES {
        return Err(DatabaseError::TooMany);
    }
    let mut ids = std::collections::BTreeSet::new();
    for entry in entries {
        entry.validate()?;
        if !ids.insert(entry.entry_id) {
            return Err(DatabaseError::DuplicateEntry);
        }
    }
    let size = postcard::to_allocvec(entries)
        .map_err(|_| DatabaseError::TooLarge)?
        .len();
    if size > MAX_DATABASE_BYTES {
        return Err(DatabaseError::TooLarge);
    }
    Ok(())
}

/// What an admin of the owning group approves: exactly this entry, published on exactly this
/// predecessor version of its register.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsentClaim {
    pub realm_id: RealmId,
    pub entry: DatabaseEntry,
    /// The register version the publication replaces; `None` for a never used entry id.
    pub predecessor: Option<AdminDocumentDot>,
    pub approver: UserId,
}

impl ConsentClaim {
    pub fn signing_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        let mut bytes = CONSENT_DOMAIN.to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }
}

/// An owner consent, signed by the node that authenticated the approver's own request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseConsent {
    pub claim: ConsentClaim,
    pub signer: NodeId,
    pub signature: iroh::Signature,
}

impl DatabaseConsent {
    /// Checks the signature and that the consent names exactly `entry` on `predecessor`.
    pub fn verify(
        &self,
        realm_id: RealmId,
        entry: &DatabaseEntry,
        predecessor: Option<&AdminDocumentDot>,
    ) -> Result<(), DatabaseError> {
        if self.claim.realm_id != realm_id
            || self.claim.entry != *entry
            || self.claim.predecessor.as_ref() != predecessor
        {
            return Err(DatabaseError::ConsentMismatch);
        }
        let bytes = self
            .claim
            .signing_bytes()
            .map_err(|_| DatabaseError::ConsentSignature)?;
        self.signer
            .verify(&bytes, &self.signature)
            .map_err(|_| DatabaseError::ConsentSignature)
    }
}

/// A curator's mark waiting for an admin of the owning group. It stays on the management node
/// that took it and is never broadcast.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseRequest {
    pub request_id: Ulid,
    pub entry: DatabaseEntry,
    /// The register version the mark replaces, which the consent will name.
    pub predecessor: Option<AdminDocumentDot>,
    pub curator: UserId,
    pub created_at_ms: u64,
}

impl DatabaseRequest {
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::errors::ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::errors::ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
