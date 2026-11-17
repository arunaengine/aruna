//! Tests the bucket source listing against fixed storage pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
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

fn page(values: Vec<Vec<u8>>, more: bool) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: (values.into_iter())
            .map(|value| (Vec::new().into(), value.into()))
            .collect(),
        next_start_after: more.then(|| b"next".to_vec().into()),
    })
}

fn syncs() -> Effect {
    rows(RELATIONSHIP_IN_KEYSPACE, sync_relationship_prefix("b"))
}

fn imports() -> Effect {
    rows(BACKEND_IMPORT_KEYSPACE, group().to_bytes().to_vec())
}

#[test]
fn lists_bucket_imports() {
    // Imports of other buckets of the group are left out; a cut page marks the list partial.
    let mut operation = BucketSourcesOperation::new(group(), "b");
    let rows = vec![
        import("b", 1).to_bytes().unwrap(),
        import("other", 2).to_bytes().unwrap(),
    ];
    let steps = vec![
        (page(Vec::new(), false), vec![imports()]),
        (page(rows, true), vec![]),
    ];
    check_trace(&mut operation, &[syncs()], steps);
    let wanted = import("b", 1);
    let (sources, complete) = operation.finalize().unwrap();
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
    storage.start();
    storage.step(page(Vec::new(), false));
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    assert!(storage.step(failed).is_empty());
    assert_eq!(
        storage.finalize(),
        Err(SourcesError::Storage(StorageError::Timeout))
    );
    // A row that does not decode fails the listing instead of hiding a source.
    let mut corrupt = BucketSourcesOperation::new(group(), "b");
    corrupt.start();
    assert!(corrupt.step(page(vec![vec![0xff]], false)).is_empty());
    assert!(matches!(
        corrupt.finalize(),
        Err(SourcesError::Conversion(_))
    ));
}

#[test]
fn rejects_unexpected_events() {
    let reading = || {
        let mut operation = BucketSourcesOperation::new(group(), "b");
        operation.start();
        operation
    };
    let iter_or_error = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        )
    };
    assert!(!rejects_events(reading, sample_events, iter_or_error).is_empty());
    let fresh = || BucketSourcesOperation::new(group(), "b");
    let refused = rejects_events(fresh, sample_events, |_| false);
    assert_eq!(refused.len(), sample_events().len());
}

#[test]
fn abort_and_finalize() {
    // Reads only, so no phase has cleanup.
    let mut fresh = BucketSourcesOperation::new(group(), "b");
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
    let mut syncs = BucketSourcesOperation::new(group(), "b");
    syncs.start();
    check_abort(&mut syncs, &[]);
    rejects_finalize(syncs);
    let mut imports = BucketSourcesOperation::new(group(), "b");
    imports.start();
    imports.step(page(Vec::new(), false));
    check_abort(&mut imports, &[]);
    rejects_finalize(imports);
}
