use super::*;

fn raw_request(document_id: Ulid) -> ExportMetadataRoCrateRequest {
    ExportMetadataRoCrateRequest {
        document_id,
        auth: None,
        view: MetadataRoCrateExportView::Raw,
        limit: None,
        offset: None,
        after: None,
    }
}

async fn seed_raw_document(test: &MetadataTest, record: &MetadataRegistryRecord) {
    seed_policy_docs(test, record.group_id).await;
    for entry in aruna_core::storage_entries::metadata_registry_write_entries(record)
        .expect("registry entries encode")
    {
        write_entry(test, entry).await;
    }
    let event = MetadataCreateEventRecord {
        event_id: record.last_event_id,
        record: record.clone(),
        user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        payload: MetadataCreateEventPayload::RoCrate {
            jsonld: "{\"@context\":\"https://w3id.org/ro/crate/1.1/context\",\"@graph\":[]}"
                .to_string(),
        },
        occurred_at_ms: 1,
    };
    write_entry(
        test,
        aruna_core::storage_entries::metadata_create_event_write_entry(&event)
            .expect("event entry encodes"),
    )
    .await;
}

#[tokio::test]
async fn raw_export_fenced() {
    // The raw export answers from one read snapshot: a document tombstoned
    // after its registry row was written must no longer export.
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    seed_raw_document(&test, &record).await;

    let exported = export_metadata_rocrate(
        &test.context,
        TEST_REALM_ID,
        raw_request(record.document_id),
    )
    .await
    .expect("raw export succeeds");
    assert!(matches!(exported, ExportMetadataRoCrateResult::Raw { .. }));
    let foreign = export_metadata_rocrate(
        &test.context,
        RealmId::from_bytes([9u8; 32]),
        raw_request(record.document_id),
    )
    .await;
    assert!(matches!(foreign, Err(MetadataApiError::NotFound)));

    let tombstone = MetadataGraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        2,
    );
    write_entry(
        &test,
        metadata_graph_lifecycle_write_entry(&tombstone).expect("tombstone encodes"),
    )
    .await;

    let fenced = export_metadata_rocrate(
        &test.context,
        TEST_REALM_ID,
        raw_request(record.document_id),
    )
    .await;

    assert!(matches!(fenced, Err(MetadataApiError::NotFound)));
}

#[test]
fn metadata_read_operation() {
    let request = metadata_read_request("/realm/g/group/meta/document", None);
    assert_eq!(request.operation, "metadata.read");
}

#[test]
fn raw_identity_fence() {
    let record = public_record(Ulid::generate(), Ulid::generate());
    assert!(raw_identity_matches(
        &record,
        TEST_REALM_ID,
        record.document_id
    ));
    assert!(!raw_identity_matches(
        &record,
        RealmId::from_bytes([8; 32]),
        record.document_id
    ));
    assert!(!raw_identity_matches(
        &record,
        TEST_REALM_ID,
        Ulid::generate()
    ));
}

// The record lives only in the registry cache and the graph was never
// projected, so a returned summary can only come from the summary cache.
#[tokio::test]
async fn summary_from_cache() {
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    seed_registry_cache(&test, &record).await;
    summary_cache().insert(
        &record.graph_iri,
        record.last_event_id,
        "{\"cached\":true}",
        Instant::now(),
    );

    let result = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, true),
    )
    .await
    .expect("summary listing succeeds");

    assert_eq!(result.documents.len(), 1);
    assert_eq!(
        result.documents[0].rocrate_summary_jsonld.as_deref(),
        Some("{\"cached\":true}")
    );
}

#[tokio::test]
async fn stale_summary_refused() {
    // A cursor advance must fall through to the handle, not serve the entry.
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    seed_registry_cache(&test, &record).await;
    summary_cache().insert(
        &record.graph_iri,
        Ulid::generate(),
        "{\"stale\":true}",
        Instant::now(),
    );

    let result = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, true),
    )
    .await
    .expect("summary listing succeeds");

    assert_eq!(result.documents.len(), 1);
    assert!(result.documents[0].rocrate_summary_jsonld.is_none());
}

#[tokio::test]
async fn pending_summary_listed() {
    let test = metadata_test();
    let record = public_record(Ulid::generate(), Ulid::generate());
    write_pending_marker(&test, &record).await;

    let result = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, true),
    )
    .await
    .expect("summary listing succeeds");
    assert_eq!(result.documents.len(), 1);
    assert_eq!(result.documents[0].record.document_id, record.document_id);
    assert!(result.documents[0].rocrate_summary_jsonld.is_none());

    let plain = list_visible_metadata_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, false),
    )
    .await
    .expect("plain listing succeeds");
    assert!(plain.documents.is_empty());
}
