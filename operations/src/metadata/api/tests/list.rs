use super::*;

#[tokio::test]
async fn filters_graph_delete() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let live = public_record(group_id, Ulid::generate());
    let deleted = public_record(group_id, Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        deleted.graph_iri.clone(),
        deleted.realm_id,
        deleted.group_id,
        deleted.document_id,
        2,
    );
    write_entry(
        &test,
        metadata_graph_lifecycle_write_entry(&tombstone).expect("lifecycle entry"),
    )
    .await;

    let records = filter_live_records(&test.context.storage_handle, &[live.clone(), deleted])
        .await
        .expect("lifecycle filter succeeds");
    assert_eq!(records, vec![live]);
}

#[tokio::test]
async fn filters_document_delete() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let deleted = public_record(group_id, Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        deleted.graph_iri.clone(),
        deleted.realm_id,
        deleted.group_id,
        deleted.document_id,
        2,
    );
    let lifecycle = aruna_core::metadata::MetadataDocumentLifecycleRecord::Delete {
        event: aruna_core::metadata::MetadataDocumentDeleteRecord {
            event_id: Ulid::generate(),
            tombstone,
            deleted_after_event_id: deleted.last_event_id,
        },
    };
    write_entry(
        &test,
        metadata_document_lifecycle_write_entry(&lifecycle).expect("lifecycle entry"),
    )
    .await;

    let records = filter_live_records(&test.context.storage_handle, &[deleted])
        .await
        .expect("lifecycle filter succeeds");
    assert!(records.is_empty());
}

#[tokio::test]
async fn rejects_bad_lifecycle() {
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    write_entry(
        &test,
        (
            METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            metadata_graph_lifecycle_key(&record.graph_iri),
            ByteView::from(vec![1u8]),
        ),
    )
    .await;

    assert!(
        filter_live_records(&test.context.storage_handle, &[record])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn foreign_lifecycle_rejected() {
    // A well-formed lifecycle entry that belongs to another document must
    // not be allowed to decide this record's visibility.
    let test = metadata_test();
    let graph_record = public_record(Ulid::generate(), Ulid::generate());
    let document_record = public_record(Ulid::generate(), Ulid::generate());
    let stranger = public_record(Ulid::generate(), Ulid::generate());
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        stranger.graph_iri.clone(),
        stranger.realm_id,
        stranger.group_id,
        stranger.document_id,
        2,
    );
    write_entry(
        &test,
        (
            METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            metadata_graph_lifecycle_key(&graph_record.graph_iri),
            ByteView::from(postcard::to_allocvec(&tombstone).expect("tombstone encodes")),
        ),
    )
    .await;
    let lifecycle = MetadataDocumentLifecycleRecord::Delete {
        event: aruna_core::metadata::MetadataDocumentDeleteRecord {
            event_id: Ulid::generate(),
            tombstone,
            deleted_after_event_id: stranger.last_event_id,
        },
    };
    write_entry(
        &test,
        (
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            metadata_document_lifecycle_key(document_record.document_id),
            ByteView::from(postcard::to_allocvec(&lifecycle).expect("lifecycle encodes")),
        ),
    )
    .await;

    assert!(
        filter_live_records(&test.context.storage_handle, &[graph_record])
            .await
            .is_err()
    );
    assert!(
        filter_live_records(&test.context.storage_handle, &[document_record])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn group_scan_capped() {
    // Without the visibility cache the listing scans storage directly, and
    // must refuse rather than hand back a truncated group.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let first = public_record(group_id, Ulid::generate());
    let second = public_record(group_id, Ulid::generate());
    for record in [&first, &second] {
        for entry in aruna_core::storage_entries::metadata_registry_write_entries(record)
            .expect("registry entries encode")
        {
            write_entry(&test, entry).await;
        }
    }
    let context = DriverContext {
        storage_handle: test.context.storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let within = load_group_records(&context, group_id, 2)
        .await
        .expect("group scan succeeds");
    let over = load_group_records(&context, group_id, 1).await;

    assert_eq!(within.len(), 2);
    assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test]
async fn pending_scan_capped() {
    // The pending-projection sweep is bounded by the candidate budget so a
    // large backlog cannot be silently cut short.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let first = public_record(group_id, Ulid::generate());
    let second = public_record(group_id, Ulid::generate());
    write_pending_marker(&test, &first).await;
    write_pending_marker(&test, &second).await;

    let within = load_pending_records(&test.context, Some(group_id), 2)
        .await
        .expect("pending scan succeeds");
    let over = load_pending_records(&test.context, Some(group_id), 1).await;

    assert_eq!(within.get(&group_id).map(Vec::len), Some(2));
    assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
}
