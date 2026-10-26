//! Re-encodes blob versions from before materialized versions named their encoding class or
//! versions carried their distributed bucket fields, and indexes references by connector.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::Layout;
use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendRef, BlobVersion, BlobVersionState, VersionKey};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::{NodeId, UserId};
use aruna_operations::connectors::repository::connector_reference_write;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(super) struct LegacyVersion {
    created_at: SystemTime,
    created_by: UserId,
    state: State,
    metadata: HashMap<String, String>,
    published_by: Option<NodeId>,
    placement_policies: Vec<PlacementPolicyRef>,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
enum State {
    Materialized {
        blob_hash: [u8; 32],
        backend: BackendRef,
        source: Option<VersionSourceBinding>,
    },
    Reference {
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        advance_count: u16,
    },
    Deleted,
}

/// Versions before the distributed bucket fields, whose sources had no digests or namespace.
pub(super) const LAYOUT: Layout = &[
    ("BlobVersion", 6),
    ("PortableSourceDescriptor", 6),
    ("SourceMetadata", 4),
];

impl From<LegacyVersion> for BlobVersion {
    fn from(old: LegacyVersion) -> Self {
        let state = match old.state {
            State::Materialized {
                blob_hash,
                backend,
                source,
            } => BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding: EncodingClass::Raw,
                source,
            },
            State::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            } => BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            },
            State::Deleted => BlobVersionState::Deleted,
        };
        BlobVersion {
            created_at: old.created_at,
            created_by: old.created_by,
            state,
            metadata: old.metadata,
            published_by: old.published_by,
            placement_policies: old.placement_policies,
            original_epoch: Default::default(),
            generation: 0,
            identity: None,
            origin: None,
            hop: None,
        }
    }
}

/// Connector reference rows missing for reference versions, reading `rewrites` over the stored
/// rows. References from before the index have none, so connector checks would miss them.
pub(super) fn reference_rows(
    db: &OptimisticTxDatabase,
    versions: &OptimisticTxKeyspace,
    rewrites: &[(Vec<u8>, Vec<u8>)],
    index: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let read = db.read_tx();
    let (mut scanned, mut rows) = (0, Vec::new());
    for entry in read.iter(versions) {
        let (key, stored) = entry.into_inner()?;
        scanned += 1;
        let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
        let version =
            BlobVersion::from_bytes(value).map_err(|error| decode_error(name, &key, error))?;
        let BlobVersionState::Reference { source, .. } = &version.state else {
            continue;
        };
        if source.connector_id.is_none() {
            continue;
        }
        let row = VersionKey::from_bytes(&key)
            .and_then(|version_key| connector_reference_write(&version, &version_key));
        let Some((_, row, value)) = row.map_err(|error| decode_error(name, &key, error))? else {
            continue;
        };
        if read.get(index, row.as_ref())?.is_none() {
            rows.push((row.to_vec(), value.to_vec()));
        }
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, strip, write};
    use super::*;
    use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{PortableSourceDescriptor, StagingStrategy};
    use aruna_core::structs::identity::realm::RealmId;
    use std::collections::BTreeMap;

    #[test]
    fn adds_raw_encoding() {
        let old = LegacyVersion {
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(RealmId([1; 32])),
            state: State::Materialized {
                blob_hash: [4; 32],
                backend: BackendRef::node_default(),
                source: None,
            },
            metadata: HashMap::from([("a".to_string(), "b".to_string())]),
            published_by: None,
            placement_policies: Vec::new(),
        };
        let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, UserId::default());
        let rows = vec![
            (
                b"old".as_slice(),
                postcard::to_allocvec(&old).expect("encodes"),
            ),
            (b"new".as_slice(), deleted.to_bytes().expect("encodes")),
        ];
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, BLOB_VERSIONS_KEYSPACE, rows);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.versions_rewritten, 1);
        let rows = read(&path, BLOB_VERSIONS_KEYSPACE);
        let version = BlobVersion::from_bytes(&rows[b"old".as_slice()]).expect("decodes");
        assert_eq!(version.metadata["a"], "b");
        let key = version.location_key().expect("materialized");
        assert_eq!(key.encoding, EncodingClass::Raw);
        assert_eq!(key.blake3_hash, [4; 32]);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.versions_rewritten, 0);
    }

    #[test]
    fn pads_distributed_fields() {
        // A reference stored before the distributed bucket fields, source digests and source
        // namespace gains their zero values; the rewrite is the current encoding.
        let descriptor = PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::new(),
            source_path: "data/a.csv".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
        };
        let metadata = SourceMetadata {
            content_length: 42,
            content_type: None,
            etag: Some("etag".to_string()),
            last_modified: None,
            source_version: None,
            digests: BTreeMap::new(),
        };
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: descriptor.clone(),
            connector_id: None,
        };
        let now = SystemTime::UNIX_EPOCH;
        let reference =
            BlobVersion::reference(source, metadata.clone(), now, UserId::default(), now);
        let deleted = BlobVersion::deleted(now, UserId::default());
        let current = reference.to_bytes().expect("encodes");
        let head = &current[..current.len() - 5];
        let head = strip(
            head,
            &postcard::to_allocvec(&descriptor).expect("encodes"),
            1,
        );
        let old = strip(
            &head,
            &postcard::to_allocvec(&metadata).expect("encodes"),
            1,
        );
        let gone = deleted.to_bytes().expect("encodes");
        let rows = vec![
            (b"reference".as_slice(), old),
            (b"deleted".as_slice(), gone[..gone.len() - 5].to_vec()),
        ];
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(&path, BLOB_VERSIONS_KEYSPACE, rows);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.versions_rewritten, 2);
        let rows = read(&path, BLOB_VERSIONS_KEYSPACE);
        assert_eq!(rows[b"reference".as_slice()], current);
        assert_eq!(rows[b"deleted".as_slice()], gone);
    }

    #[test]
    fn indexes_connector_references() {
        // A reference bound to a connector, stored before the reference index existed.
        let connector_id = ulid::Ulid::from_bytes([6; 16]);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::new(),
                source_path: "data/a.csv".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
            },
            connector_id: Some(connector_id),
        };
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: BTreeMap::new(),
        };
        let now = SystemTime::UNIX_EPOCH;
        let reference = BlobVersion::reference(source, metadata, now, UserId::default(), now);
        let key = VersionKey::new("bucket", "a.csv", ulid::Ulid::from_bytes([7; 16]));
        let key = key.to_bytes().expect("key encodes");
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let value = reference.to_bytes().expect("encodes");
        write(&path, BLOB_VERSIONS_KEYSPACE, vec![(key.as_slice(), value)]);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.connector_references_written, 1);
        let rows = read(&path, CONNECTOR_REFERENCE_KEYSPACE);
        let row = [connector_id.to_bytes().as_slice(), &key].concat();
        assert!(rows.contains_key(&row));
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.connector_references_written, 0);
    }
}
