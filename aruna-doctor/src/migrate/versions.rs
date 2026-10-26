//! Brings stored blob versions to the current shape, stores external bindings compact with their
//! root records, and indexes references by connector.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::{self, Layout};
use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendRef, BlobVersion, BlobVersionState, VersionKey};
use aruna_core::structs::storage::broken::BrokenVersion;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::{NodeId, UserId};
use aruna_operations::connectors::repository::connector_reference_write;
use aruna_operations::staging::root_record::{compact_version, root_write};
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

/// Versions from before a source binding could name its root by digest.
pub(super) const BEFORE_ROOT: Layout = &[("PortableSourceDescriptor", 7)];

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

/// A stored version in the current shape, from whichever shape it was stored in, and whether it
/// was stored in an older one.
fn current_version(value: &[u8]) -> Result<(BlobVersion, bool), postcard::Error> {
    if let Ok(version) = session_refs::decode::<BlobVersion>(value, &[]) {
        return Ok((version, false));
    }
    if let Ok(version) = session_refs::decode::<BlobVersion>(value, BEFORE_ROOT) {
        return Ok((version, true));
    }
    match session_refs::decode::<BlobVersion>(value, LAYOUT) {
        Ok(version) => Ok((version, true)),
        Err(_) => Ok((
            session_refs::decode::<LegacyVersion>(value, LAYOUT)?.into(),
            true,
        )),
    }
}

/// Root record rows by digest.
type RootRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Stored versions rewritten into the current shape, an external source binding compact, with
/// the root records the compact bindings name, by digest.
pub(super) fn compact_rows(
    db: &OptimisticTxDatabase,
    versions: &OptimisticTxKeyspace,
    name: &str,
) -> Result<(Rewrites, RootRows), ExplorerError> {
    let (mut scanned, mut rows, mut roots) = (0, Vec::new(), BTreeMap::new());
    for entry in db.read_tx().iter(versions) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        let failed = |error: &dyn std::fmt::Display| decode_error(name, &key, error);
        let (version, relaid) = current_version(&value).map_err(|error| failed(&error))?;
        let (version, root) = compact_version(version).map_err(|error| failed(&error))?;
        if let Some(root) = root {
            let (_, digest, record) = root_write(&root).map_err(|error| failed(&error))?;
            roots.insert(digest.to_vec(), record.to_vec());
        } else if !relaid {
            continue;
        }
        let bytes = version.to_bytes().map_err(|error| failed(&error))?;
        rows.push((key.to_vec(), bytes));
    }
    Ok((Rewrites { scanned, rows }, roots.into_iter().collect()))
}

/// Broken versions keep their full bindings; only their shape is brought up to date.
pub(super) fn broken_version(value: &[u8]) -> Result<Option<Vec<u8>>, postcard::Error> {
    if session_refs::decode::<BrokenVersion>(value, &[]).is_ok() {
        return Ok(None);
    }
    let broken = session_refs::decode::<BrokenVersion>(value, BEFORE_ROOT)
        .or_else(|_| session_refs::decode::<BrokenVersion>(value, LAYOUT))?;
    Ok(Some(postcard::to_allocvec(&broken)?))
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
            root: None,
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
        // The namespace and the root are the descriptor's last two fields.
        let head = strip(
            head,
            &postcard::to_allocvec(&descriptor).expect("encodes"),
            2,
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
        let (compact, _) = compact_version(reference).expect("compacts");
        let compact = compact.to_bytes().expect("encodes");
        assert_eq!(rows[b"reference".as_slice()], compact);
        assert_eq!(rows[b"deleted".as_slice()], gone);
    }

    #[test]
    fn stores_roots_once() {
        // Two references to one public S3 source, stored before bindings named their root:
        // both are stored compact and name one root record that gives their settings back.
        use aruna_core::keyspaces::ROOT_RECORD_KEYSPACE;
        use aruna_core::structs::storage::distribution::source_root::{RootEntry, SourceRoot};
        let reference = |path: &str| {
            let source = VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::S3,
                    public_config: HashMap::from([
                        ("bucket".to_string(), "bakrep".to_string()),
                        ("endpoint".to_string(), "https://s3.example.org".to_string()),
                    ]),
                    source_path: path.to_string(),
                    version_selector: None,
                    capabilities: vec!["head".to_string(), "read".to_string()],
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: Some(ulid::Ulid::from_bytes([6; 16])),
            };
            let metadata = SourceMetadata {
                content_length: 3,
                content_type: None,
                etag: Some("e".to_string()),
                last_modified: None,
                source_version: None,
                digests: BTreeMap::new(),
            };
            let now = SystemTime::UNIX_EPOCH;
            BlobVersion::reference(source, metadata, now, UserId::default(), now)
        };
        let stored = |version: &BlobVersion| {
            let current = version.to_bytes().expect("encodes");
            let descriptor = &version.source_binding().expect("bound").descriptor;
            strip(
                &current,
                &postcard::to_allocvec(descriptor).expect("encodes"),
                1,
            )
        };
        let (first, second) = (reference("a/1.gff"), reference("b/2.gff"));
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        let key = |name: &str, id: u8| {
            let key = VersionKey::new("bucket", name, ulid::Ulid::from_bytes([id; 16]));
            key.to_bytes().expect("key encodes")
        };
        let (first_key, second_key) = (key("a/1.gff", 1), key("b/2.gff", 2));
        let rows = vec![
            (first_key.as_slice(), stored(&first)),
            (second_key.as_slice(), stored(&second)),
        ];
        write(&path, BLOB_VERSIONS_KEYSPACE, rows);

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(
            (output.versions_rewritten, output.source_roots_written),
            (2, 1)
        );
        let roots = read(&path, ROOT_RECORD_KEYSPACE);
        let root = SourceRoot::from_bytes(roots.values().next().expect("one root")).unwrap();
        let rows = read(&path, BLOB_VERSIONS_KEYSPACE);
        let compact = BlobVersion::from_bytes(&rows[&first_key]).expect("decodes");
        let binding = compact.source_binding().expect("bound");
        assert!(binding.descriptor.public_config.is_empty());
        let full = RootEntry::hydrate(binding, &root)
            .unwrap()
            .expect("its root");
        let expected = first.source_binding().expect("bound");
        assert_eq!(
            full.descriptor.public_config,
            expected.descriptor.public_config
        );
        assert_eq!(full.descriptor.source_path, "a/1.gff");
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(
            (again.versions_rewritten, again.source_roots_written),
            (0, 0)
        );
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
                root: None,
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
