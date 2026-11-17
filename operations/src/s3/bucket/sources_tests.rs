//! Tests the bucket source listing against fixed storage pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;

use super::*;

fn group() -> GroupId {
    Ulid::from_parts(1, 1)
}

fn import(bucket: &str, seed: u64) -> BackendImport {
    BackendImport {
        import_id: Ulid::from_parts(seed, 2),
        group_id: group(),
        backend_id: Ulid::from_parts(seed, 3),
        backend_prefix: "data/".to_string(),
        bucket: bucket.to_string(),
        key_prefix: "in/".to_string(),
        schedule: None,
        created_by: UserId::nil(RealmId::from_bytes([1; 32])),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: String::new(),
    }
}

fn page(values: Vec<Vec<u8>>, more: bool) -> IoResult {
    IoResult::Page {
        rows: (values.into_iter())
            .map(|value| (Vec::new().into(), value.into()))
            .collect(),
        next: more.then(|| b"next".to_vec().into()),
    }
}

fn syncs() -> Next<(Vec<FileSource>, bool)> {
    Next::Call(rows(
        RELATIONSHIP_IN_KEYSPACE,
        sync_relationship_prefix("b"),
    ))
}

fn imports() -> Next<(Vec<FileSource>, bool)> {
    Next::Call(rows(BACKEND_IMPORT_KEYSPACE, group().to_bytes().to_vec()))
}

#[test]
fn lists_bucket_imports() {
    // Imports of other buckets of the group are left out; a cut page marks the list partial.
    let mut operation = BucketSourcesOperation::new(group(), "b");
    let rows = vec![
        import("b", 1).to_bytes().unwrap(),
        import("other", 2).to_bytes().unwrap(),
    ];
    assert_eq!(operation.first(), Ok(syncs()));
    assert_eq!(operation.next(page(Vec::new(), false)), Ok(imports()));
    let Ok(Next::Done((sources, complete))) = operation.next(page(rows, true)) else {
        panic!("expected the sources");
    };
    let wanted = import("b", 1);
    assert_eq!(
        sources,
        [FileSource::Storage {
            import_id: wanted.import_id,
            backend_id: wanted.backend_id,
            backend_prefix: wanted.backend_prefix,
            key_prefix: wanted.key_prefix,
        }]
    );
    assert!(!complete);
}

#[test]
fn errors_fail() {
    let mut storage = BucketSourcesOperation::new(group(), "b");
    let failed = SourcesError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(failed, SourcesError::Storage(StorageError::Timeout));
    assert!(matches!(
        storage.next(IoResult::Done),
        Err(SourcesError::Io(IoError::Unexpected { .. }))
    ));
    // A row that does not decode fails the listing instead of hiding a source.
    let mut corrupt = BucketSourcesOperation::new(group(), "b");
    assert!(matches!(
        corrupt.next(page(vec![vec![0xff]], false)),
        Err(SourcesError::Conversion(_))
    ));
}
