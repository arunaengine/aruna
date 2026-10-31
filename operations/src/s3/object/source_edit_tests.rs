// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::keyspaces::CONNECTOR_REFERENCE_KEYSPACE;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{PortableSourceDescriptor, StagingStrategy};
use aruna_core::structs::identity::realm::RealmId;

use super::*;

fn connector(byte: u8, endpoint: &str) -> SourceConnector {
    SourceConnector::new(
        Ulid::from_bytes([byte; 16]),
        Ulid::from_bytes([9; 16]),
        "mirror".to_string(),
        SourceConnectorKind::Http,
        HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH,
        Default::default(),
    )
}

fn observed() -> SourceMetadata {
    SourceMetadata {
        content_length: 4,
        content_type: None,
        etag: Some("first".to_string()),
        last_modified: Some(SystemTime::UNIX_EPOCH),
        source_version: None,
        digests: Default::default(),
    }
}

fn reference(path: &str) -> BlobVersion {
    let binding = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::new(),
            source_path: path.to_string(),
            version_selector: Some("version:1".to_string()),
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::from_bytes([1; 16])),
        mirrors: Vec::new(),
    };
    let user = UserId::local(Ulid::from_bytes([2; 16]), RealmId([0; 32]));
    let at = SystemTime::UNIX_EPOCH;
    BlobVersion::reference(binding, observed(), at, user, at)
}

fn row(key: &str, version: &BlobVersion) -> (Key, Value) {
    let version_key = VersionKey::new("refseq", key, Ulid::from_bytes([3; 16]));
    (
        version_key.to_bytes().unwrap().into(),
        version.to_bytes().unwrap().into(),
    )
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([9; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: aruna_core::structs::storage::format::Compression::Off,
        distribution: None,
    }
}

/// The edit's guard reads, answered with `bucket` and, for an addition, `connector`.
fn guarded(bucket: &BucketInfo, connector: Option<&SourceConnector>) -> Event {
    let mut values = vec![(
        Key::from(b"bucket".to_vec()),
        Some(bucket.to_bytes().unwrap().into()),
    )];
    if let Some(connector) = connector {
        let value = Some(connector.to_bytes().unwrap().into());
        values.push((Key::from(b"connector".to_vec()), value));
    }
    Event::Storage(StorageEvent::BatchReadResult { values })
}

fn started(change: SourceChange) -> SourceEditOperation {
    let mut operation = SourceEditOperation::new(SourceEditInput {
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        bucket: "refseq".to_string(),
        expected_bucket: bucket().identity(),
        prefix: "genomes/".to_string(),
        exact: false,
        change,
        after: None,
        page: 10,
    });
    operation.start();
    let txn_id = Ulid::generate();
    let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ));
    operation
}

fn edit(change: SourceChange) -> SourceEditOperation {
    let connector = match &change {
        SourceChange::Add { connector, .. } => Some(connector.clone()),
        SourceChange::Remove { .. } => None,
    };
    let mut operation = started(change);
    operation.step(guarded(&bucket(), connector.as_ref()));
    operation
}

// A bucket recreated under the name, or a connector replaced since authorization, ends the
// edit before any row is read.
#[test]
fn changed_guards_refused() {
    let mirror = connector(5, "https://mirror.example.org");
    let add = || SourceChange::Add {
        mapping: SourceMapping {
            connector_id: mirror.connector_id,
            from: String::new(),
            to: String::new(),
        },
        connector: mirror.clone(),
        rules: PermissionRules::default(),
        read_root: String::new(),
    };
    let mut recreated = bucket();
    recreated.created_at = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1);
    let mut replaced = mirror.clone();
    replaced
        .public_config
        .insert("endpoint".into(), "https://other.example.org".into());
    let cases = [
        (
            guarded(&recreated, Some(&mirror)),
            SourceEditError::BucketChanged,
        ),
        (
            guarded(&bucket(), Some(&replaced)),
            SourceEditError::ConnectorChanged,
        ),
    ];
    for (event, expected) in cases {
        let mut operation = started(add());
        let effects = operation.step(event);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(operation.finalize(), Err(expected));
    }
}

fn written(effects: &Effects) -> Vec<(KeySpace, Key, Value)> {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected one batch write, got {effects:?}")
    };
    writes.clone()
}

// A mapping adds the mirror's path to every reference under the prefix, once, with a
// connector reference row; versions outside the prefix stay as they are.
#[test]
fn mapping_adds_sources() {
    let mirror = connector(5, "https://mirror.example.org");
    let mapping = SourceMapping {
        connector_id: mirror.connector_id,
        from: "genomes/".to_string(),
        to: "refseq/".to_string(),
    };
    let mut operation = edit(SourceChange::Add {
        mapping,
        connector: mirror.clone(),
        rules: readers("/sources/refseq/a*"),
        read_root: "/sources".to_string(),
    });
    let first = reference("genomes/a.fna");
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![
            row("genomes/a.fna", &first),
            row("other/b.fna", &reference("genomes/b.fna")),
            // The caller may not read this file's new source, so it stays as it is.
            row("genomes/c.fna", &reference("genomes/c.fna")),
        ],
        next_start_after: Some(b"cursor".to_vec().into()),
    }));
    let writes = written(&effects);
    assert_eq!(writes.len(), 3, "{writes:?}");
    let references = writes
        .iter()
        .filter(|w| w.0 == CONNECTOR_REFERENCE_KEYSPACE);
    assert_eq!(references.count(), 2);
    let version = BlobVersion::from_bytes(&writes[2].2).unwrap();
    let source = version.source_binding().unwrap();
    assert_eq!(source.mirrors.len(), 1);
    let added = &source.mirrors[0];
    assert_eq!(added.descriptor.source_path, "refseq/a.fna");
    assert_eq!(added.descriptor.version_selector, None);
    assert_eq!(added.connector_id, Some(mirror.connector_id));
    // The same mapping again adds nothing.
    let BlobVersionState::Reference { source, .. } = version.state else {
        unreachable!()
    };
    let mut again = source.clone();
    let mapping = SourceMapping {
        connector_id: mirror.connector_id,
        from: "genomes/".to_string(),
        to: "refseq/".to_string(),
    };
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    assert!(!added_twice(&mut again, (&mapping, &mirror), node));
    let txn_written = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        txn_written.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
    let txn_id = operation.txn_id.unwrap();
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    let result = operation.finalize().unwrap();
    assert_eq!((result.scanned, result.changed, result.denied), (3, 1, 1));
    assert_eq!(result.next.as_deref(), Some(&b"cursor"[..]));
}

fn added_twice(
    source: &mut VersionSourceBinding,
    rule: (&SourceMapping, &SourceConnector),
    node_id: NodeId,
) -> bool {
    added(source, &observed(), rule, node_id, |_| true) == Addition::Added
}

/// Rules granting READ on `pattern` only.
fn readers(pattern: &str) -> PermissionRules {
    use crate::auth::permission_rules::CollectedRole;
    use aruna_core::structs::identity::auth::{Permission, Role};
    let role = Role {
        role_id: Ulid::from_bytes([6; 16]),
        name: "reader".into(),
        permissions: [(pattern.to_string(), Permission::READ)].into(),
        assigned_users: Default::default(),
    };
    let collected = CollectedRole {
        role,
        direct: true,
        public: false,
    };
    PermissionRules::from_roles(vec![collected], None).unwrap()
}

// Removing the first source makes the next one first and drops the first one's validators;
// the last source is never removed.
#[test]
fn removal_keeps_last() {
    let mirror = connector(5, "https://mirror.example.org");
    let mut source = reference("genomes/a.fna").source_binding().unwrap().clone();
    let mapping = SourceMapping {
        connector_id: mirror.connector_id,
        from: "genomes/".to_string(),
        to: "refseq/".to_string(),
    };
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let rule = (&mapping, &mirror);
    let addition = added(&mut source, &observed(), rule, node, |_| true);
    assert_eq!(addition, Addition::Added);
    let mut kept = source.clone();
    let mut metadata = observed();
    assert_eq!(removed(&mut kept, &mut metadata, None, ""), Removal::Last);
    assert_eq!(kept, source);
    assert_eq!(metadata.etag.as_deref(), Some("first"));
    let first = Some(Ulid::from_bytes([1; 16]));
    // Without checksums nothing proves the next source's bytes, so the first one stays.
    let mut unproven = source.clone();
    assert_eq!(
        removed(&mut unproven, &mut metadata, first, "genomes/"),
        Removal::Unproven
    );
    assert_eq!(unproven, source);
    metadata.digests.insert(
        aruna_core::structs::checksum::ChecksumAlgorithm::Sha256,
        aruna_core::structs::execution::source_access::SourceDigest {
            digest: vec![1; 32],
            scope: aruna_core::structs::execution::source_access::DigestScope::FullObject,
            origin: aruna_core::structs::execution::source_access::DigestOrigin::Manifest,
        },
    );
    assert_eq!(
        removed(&mut source, &mut metadata, first, "genomes/"),
        Removal::Removed
    );
    assert_eq!(source.descriptor.source_path, "refseq/a.fna");
    assert!(source.mirrors.is_empty());
    assert_eq!((&metadata.etag, metadata.last_modified), (&None, None));
    assert_eq!(
        removed(&mut source, &mut metadata, first, "genomes/"),
        Removal::None
    );
}
