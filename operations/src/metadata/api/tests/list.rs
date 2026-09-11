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

// The page window must not truncate the estimate, and paging must not move it.
#[tokio::test]
async fn estimate_beyond_page() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let seeded = METADATA_ESTIMATE_MIN_LIMIT + 2;
    for _ in 0..seeded {
        seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
    }

    let page = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(page.documents.len(), METADATA_ESTIMATE_MIN_LIMIT);
    assert_eq!(page.total_returned, METADATA_ESTIMATE_MIN_LIMIT);
    assert_eq!(page.total_estimate, Some(seeded));

    let tail = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            offset: Some(seeded - 1),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(tail.documents.len(), 1);
    assert_eq!(tail.total_estimate, Some(seeded));
}

// A targeted lookup must not pay for the realm-wide estimate scan, and
// must report the estimate as absent rather than as a truncated count.
#[tokio::test]
async fn lookup_omits_estimate() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    for _ in 0..3 {
        seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
    }

    let lookup = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(lookup.total_returned, 3);
    assert_eq!(lookup.total_estimate, None);

    let browse = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        ListVisibleMetadataDocumentsRequest {
            limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
            ..summary_request(group_id, false)
        },
    )
    .await
    .expect("listing succeeds");
    assert_eq!(browse.total_estimate, Some(3));
}

// Anonymous callers collect no rules, so only public records count.
#[tokio::test]
async fn estimate_skips_private() {
    let test = metadata_test();
    let group_id = Ulid::generate();
    let readable = public_record(group_id, Ulid::generate());
    seed_registry_cache(&test, &readable).await;
    let mut private = public_record(group_id, Ulid::generate());
    private.public = false;
    seed_registry_cache(&test, &private).await;

    let result = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .expect("listing succeeds");

    assert_eq!(result.documents.len(), 1);
    assert_eq!(result.documents[0].record.document_id, readable.document_id);
    assert_eq!(result.total_estimate, Some(1));
}

#[tokio::test]
async fn cross_shard_unknown() {
    // Local listings cannot resolve claims that may live on another registry shard.
    let test = metadata_test();
    let group_id = Ulid::generate();
    let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
    config.seed_default_placement();
    let mut first = public_record(group_id, Ulid::generate());
    let first_shard = registry_placement(&config, &first).shard;
    let mut second = loop {
        let candidate = public_record(group_id, Ulid::generate());
        if registry_placement(&config, &candidate).shard != first_shard {
            break candidate;
        }
    };
    second.document_path = first.document_path.clone();
    second.permission_path = MetadataRegistryRecord::permission_path_for(
        &TEST_REALM_ID,
        group_id,
        &second.document_path,
        second.document_id,
    );
    let claims = [&first, &second]
        .into_iter()
        .map(|record| PathClaimRecord {
            document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
            establishing_event_id: record.establishing_event_id,
            requested_path: record.document_path.clone(),
        })
        .collect::<Vec<_>>();
    let resolution = aruna_core::structs::resolve_path_claim(&claims).unwrap();
    let winner_id = resolution.winner.document_id.as_ulid();
    let loser_id = resolution.conflicts[0].document_id.as_ulid();
    first.public = first.document_id == loser_id;
    second.public = second.document_id == loser_id;
    seed_registry_cache(&test, &first).await;
    seed_registry_cache(&test, &second).await;

    let listed = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(group_id, false),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert_eq!(listed.documents[0].record.document_id, loser_id);
    assert_ne!(winner_id, loser_id);
}
