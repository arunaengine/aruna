//! Tests the group record listing: live records only, refused past its candidate budget.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::auth_storage;
use super::effect::memory_handle;
use super::lifecycle::registry_record;
use super::*;
use crate::driver::drive;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::list::GroupRecordsOperation;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::metadata::GraphLifecycleRecord;
use byteview::ByteView;

async fn store_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected batch write result: {other:?}"),
    }
}

fn group_record(group_id: GroupId, document_path: &str) -> MetadataRegistryRecord {
    let mut record = registry_record(document_path);
    record.group_id = group_id;
    record.public = false;
    record.permission_path = MetadataRegistryRecord::permission_path_for(
        &record.realm_id,
        group_id,
        document_path,
        record.document_id,
    );
    record
}

fn registry_entries(record: &MetadataRegistryRecord) -> Vec<(String, ByteView, ByteView)> {
    aruna_core::storage_entries::registry_write_entries(record).expect("registry entries encode")
}
#[tokio::test]
async fn group_records_live() {
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let live = group_record(group_id, "datasets/live");
    let gone = group_record(group_id, "datasets/gone");
    let tombstone = GraphLifecycleRecord::deleted(
        gone.graph_iri.clone(),
        gone.realm_id,
        gone.group_id,
        gone.document_id,
        2,
    );
    let mut writes = registry_entries(&live);
    writes.extend(registry_entries(&gone));
    writes.push(
        aruna_core::storage_entries::graph_lifecycle_entry(&tombstone).expect("tombstone encodes"),
    );
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let records = drive(GroupRecordsOperation::new(group_id, 16), &handle)
        .await
        .expect("group listing succeeds");

    assert_eq!(
        records
            .iter()
            .map(|record| record.document_id)
            .collect::<Vec<_>>(),
        vec![live.document_id]
    );
}
#[tokio::test]
async fn group_records_capped() {
    // The scan must refuse to answer past its candidate budget instead of
    // returning a silently truncated group listing.
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let mut writes = registry_entries(&group_record(group_id, "datasets/one"));
    writes.extend(registry_entries(&group_record(group_id, "datasets/two")));
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let within = drive(GroupRecordsOperation::new(group_id, 2), &handle)
        .await
        .expect("group listing succeeds");
    let over = drive(GroupRecordsOperation::new(group_id, 1), &handle).await;

    assert_eq!(within.len(), 2);
    assert_eq!(over, Err(MetadataApiError::ServiceUnavailable));
}
