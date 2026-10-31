// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::execution::source_access::{DigestOrigin, SourceDigest, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::execution::table::{RowMapper, RowOutcome, TableRows, parse_rows};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::format::EncodingClass;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

const CONNECTOR: ulid::Ulid = ulid::Ulid::from_parts(9, 9);

fn reference(path: &str) -> BlobVersion {
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::new(),
            source_path: path.to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: Some([3; 32]),
        },
        connector_id: Some(CONNECTOR),
        mirrors: Vec::new(),
    };
    let md5 = SourceDigest {
        digest: vec![0xab; 16],
        scope: DigestScope::FullObject,
        origin: DigestOrigin::Manifest,
    };
    let seen = SourceMetadata {
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        etag: Some("\"e1\"".to_string()),
        last_modified: Some(UNIX_EPOCH + Duration::from_secs(1_763_401_101)),
        source_version: None,
        digests: BTreeMap::from([(ChecksumAlgorithm::Md5, md5)]),
    };
    let mut version =
        BlobVersion::reference(source, seen, UNIX_EPOCH, Default::default(), UNIX_EPOCH);
    version.metadata = HashMap::from([
        ("organism".to_string(), "E. coli \"K-12\"".to_string()),
        ("aruna-search-text".to_string(), "organism".to_string()),
    ]);
    version
}

fn stored(hash: [u8; 32]) -> BlobVersion {
    let backend = BackendRef::node_default();
    BlobVersion::materialized(
        hash,
        backend,
        EncodingClass::Raw,
        UNIX_EPOCH,
        Default::default(),
        None,
    )
}

/// Writes heads and versions of `rows` into a new store.
fn store(path: &Path, rows: Vec<(&str, &str, BlobVersion)>) {
    let db = OptimisticTxDatabase::builder(path).open().unwrap();
    let heads = db
        .keyspace(BLOB_HEAD_KEYSPACE, KeyspaceCreateOptions::default)
        .unwrap();
    let versions = db
        .keyspace(BLOB_VERSIONS_KEYSPACE, KeyspaceCreateOptions::default)
        .unwrap();
    let mut txn = db.write_tx().unwrap();
    for (at, (bucket, key, version)) in rows.into_iter().enumerate() {
        let id = ulid::Ulid::from_parts(at as u64, 1);
        let pointer = CurrentVersionPointer::new(id).to_bytes().unwrap();
        txn.insert(
            &heads,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            pointer,
        );
        let row = VersionKey::new(bucket, key, id).to_bytes().unwrap();
        txn.insert(&versions, row, version.to_bytes().unwrap());
    }
    txn.commit().unwrap().unwrap();
}

// Every current object becomes one row, by connector or as an empty object; the rows map back
// through a record import to the stored values.
#[test]
fn bucket_records_exported() {
    let dir = tempfile::tempdir().unwrap();
    let mut folder = stored(*blake3::hash(b"").as_bytes());
    folder.metadata = HashMap::from([("organism".to_string(), "E. coli".to_string())]);
    store(
        &dir.path().join("db"),
        vec![
            ("genomes", "g/a.txt", reference("all/g/a.txt")),
            ("genomes", "g/", folder),
            ("genomes", "g/data.bin", stored([7; 32])),
            (
                "genomes",
                "g/gone",
                BlobVersion {
                    state: BlobVersionState::Deleted,
                    ..reference("x")
                },
            ),
            ("other", "g/b.txt", reference("all/g/b.txt")),
        ],
    );
    let out = dir.path().join("out");
    let db = dir.path().join("db");
    let output = export_rows(db.to_str().unwrap(), "genomes", out.to_str().unwrap()).unwrap();
    assert_eq!(
        output.files,
        BTreeMap::from([(CONNECTOR.to_string(), 1), ("empty".to_string(), 1)])
    );
    assert_eq!(
        output.skipped,
        BTreeMap::from([("delete marker", 1), ("stored bytes", 1)])
    );

    let rows: TableRows = serde_json::from_value(serde_json::json!({
        "kind": "record", "source": "{source}", "target": "{key}", "keep": [],
        "size": "size", "modified": "modified",
        "checksums": [{ "algorithm": "md5", "column": "md5" }]
    }))
    .unwrap();
    let format = aruna_core::structs::execution::table::TableFormat {
        delimiter: '\t',
        quote: None,
        ..Default::default()
    };
    let map = |name: &str| {
        let text = std::fs::read_to_string(out.join(format!("{name}.tsv"))).unwrap();
        let (header, body) = text.split_once('\n').unwrap();
        let columns: Vec<String> = header.split('\t').map(String::from).collect();
        let mapper = RowMapper::new(&rows, &columns).unwrap();
        let (parsed, _) = parse_rows(&format, Some(columns.len()), body.as_bytes(), true).unwrap();
        match mapper.map(&parsed[0].0.clone().unwrap(), None) {
            RowOutcome::Import(entry) => entry,
            other => panic!("{other:?}"),
        }
    };
    let file = map(&CONNECTOR.to_string());
    assert_eq!(
        (file.source_path.as_str(), file.target.as_str()),
        ("all/g/a.txt", "g/a.txt")
    );
    assert_eq!(
        (
            file.size,
            file.etag.as_deref(),
            file.content_type.as_deref()
        ),
        (Some(42), Some("\"e1\""), Some("text/plain"))
    );
    assert_eq!(
        file.modified,
        Some(UNIX_EPOCH + Duration::from_secs(1_763_401_101))
    );
    assert_eq!(file.digests, [(ChecksumAlgorithm::Md5, vec![0xab; 16])]);
    assert_eq!(file.metadata["organism"], "E. coli \"K-12\"");
    assert_eq!(file.metadata["aruna-search-text"], "organism");
    let empty = map("empty");
    assert_eq!(
        (
            empty.source_path.as_str(),
            empty.target.as_str(),
            empty.size
        ),
        ("", "g/", Some(0))
    );
    assert_eq!(empty.metadata["organism"], "E. coli");
}
