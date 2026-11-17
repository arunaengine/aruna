//! The record-only versions this member reads from the holders of their range: what the record
//! gives without a byte, and the kept copies this member may serve instead.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::keyspaces::{ATTACHMENT_KEYSPACE, CACHE_ENTRY_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_CAPACITY_BYTES, CacheEntry, KeptKind, attachment_key,
};
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_core::types::{Key, KeySpace, Value};
use ulid::Ulid;

use crate::connectors::resolver::NATIVE_SET_ID;
use crate::s3::object::get::{ObjectInfo, ObjectRangeRequest};

/// A distributed version whose record this member holds without its bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct HeldElsewhere {
    pub set_id: Ulid,
    pub version_id: Ulid,
    /// The logical identity the set fixed; a cache entry is keyed by it.
    pub identity: [u8; 32],
    /// The BLAKE3 of the bytes when the record names it. An external reference names none: its
    /// identity digests the source observation, so the serving copy's verified hash binds.
    pub blake3: Option<[u8; 32]>,
    pub size: u64,
    pub metadata: HashMap<String, String>,
    pub version_created_at: Option<SystemTime>,
    pub source_policies: Vec<PlacementPolicyRef>,
}

impl HeldElsewhere {
    /// The set and BLAKE3 of a record-only version a member of a distributed bucket received.
    pub fn of(
        source: &VersionSourceBinding,
        identity: Option<[u8; 32]>,
    ) -> Option<(Ulid, [u8; 32])> {
        if !record_only(source) {
            return None;
        }
        let set_id = source.descriptor.public_config.get(NATIVE_SET_ID)?;
        Some((Ulid::from_string(set_id).ok()?, identity?))
    }

    /// The whole-object facts the record gives without any byte.
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo {
            size: self.size,
            version_created_at: self.version_created_at,
            etag: None,
            checksum_type: MultipartChecksumType::FullObject,
            hashes: (self.blake3.iter())
                .map(|blake3| (HASH_BLAKE3.to_string(), blake3.to_vec()))
                .collect(),
            composite_hashes: HashMap::new(),
            part_count: None,
        }
    }
}

/// Whether a version is a record a member received without bytes: they stay with the holders
/// of its range, so a copy reads them there instead of keeping the record as a reference.
pub fn record_only(source: &VersionSourceBinding) -> bool {
    source.descriptor.kind == SourceConnectorKind::ArunaNative
        && source.descriptor.public_config.contains_key(NATIVE_SET_ID)
}

/// The reads of the copies this member may keep of `held`: its attachment, its cache entry.
pub fn kept_reads(held: &HeldElsewhere) -> Vec<(KeySpace, Key)> {
    kept_source_reads(held.set_id, held.version_id, held.identity)
}

/// The reads of a version's kept copies, by set and version id, and by logical identity.
pub fn kept_source_reads(
    set_id: Ulid,
    version_id: Ulid,
    identity: [u8; 32],
) -> Vec<(KeySpace, Key)> {
    vec![
        (
            ATTACHMENT_KEYSPACE.to_string(),
            attachment_key(set_id, version_id).into(),
        ),
        (CACHE_ENTRY_KEYSPACE.to_string(), identity.to_vec().into()),
    ]
}

/// The kept copy of an external reference: complete, and holding the bytes it was hashed as.
pub fn kept_source_location(values: &[(Key, Option<Value>)], size: u64) -> Option<BackendLocation> {
    let fits = |location: &BackendLocation, blake3: [u8; 32]| {
        location.blob_size == size && location.get_blake3() == Some(blake3.as_slice())
    };
    let attached = values
        .first()
        .and_then(|(_, value)| Attachment::from_bytes(value.as_deref()?).ok())
        .filter(|attached| fits(&attached.location, attached.blake3))
        .map(|attached| attached.location);
    attached.or_else(|| {
        values
            .get(1)
            .and_then(|(_, value)| CacheEntry::from_bytes(value.as_deref()?).ok())
            .filter(|entry| fits(&entry.location, entry.blake3))
            .map(|entry| entry.location)
    })
}

/// The kept copy a read uses: one of the version's size whose BLAKE3 is the version's, or for
/// an external reference the one it was verified as.
pub fn kept_location(
    held: &HeldElsewhere,
    values: &[(Key, Option<Value>)],
) -> Option<BackendLocation> {
    let fits = |location: &BackendLocation, blake3: [u8; 32]| {
        held.blake3.unwrap_or(blake3) == blake3 && location.blob_size == held.size
    };
    let attached = values
        .first()
        .and_then(|(_, value)| Attachment::from_bytes(value.as_deref()?).ok())
        .filter(|attached| fits(&attached.location, attached.blake3))
        .map(|attached| attached.location);
    attached.or_else(|| {
        values
            .get(1)
            .and_then(|(_, value)| CacheEntry::from_bytes(value.as_deref()?).ok())
            .filter(|entry| fits(&entry.location, entry.blake3))
            .map(|entry| entry.location)
    })
}

/// One read through the holders, with the caller it is read for.
#[derive(Clone, Debug, PartialEq)]
pub struct PullRequest {
    pub bucket: String,
    pub key: String,
    pub range: Option<ObjectRangeRequest>,
    /// The caller; every holder checks it on its own path.
    pub auth: AuthContext,
    pub held: HeldElsewhere,
}

/// Whether a kept copy of `size` bytes may be fetched: a cache entry must fit the cache.
pub fn admitted(kind: KeptKind, size: u64) -> bool {
    kind != KeptKind::Cache || size <= CACHE_CAPACITY_BYTES
}
