use super::list::summary_request;
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
    for entry in aruna_core::storage_entries::registry_write_entries(record)
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
        aruna_core::storage_entries::create_event_entry(&event).expect("event entry encodes"),
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
        graph_lifecycle_entry(&tombstone).expect("tombstone encodes"),
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

    let result = list_visible_documents(
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

    let result = list_visible_documents(
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

    let result = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, true),
    )
    .await
    .expect("summary listing succeeds");
    assert_eq!(result.documents.len(), 1);
    assert_eq!(result.documents[0].record.document_id, record.document_id);
    assert!(result.documents[0].rocrate_summary_jsonld.is_none());

    let plain = list_visible_documents(
        &test.context,
        TEST_REALM_ID,
        summary_request(record.group_id, false),
    )
    .await
    .expect("plain listing succeeds");
    assert!(plain.documents.is_empty());
}

pub(super) async fn seed_registry_cache(test: &MetadataTest, record: &MetadataRegistryRecord) {
    seed_policy_docs(test, record.group_id).await;
    let handle = test
        .context
        .metadata_handle
        .as_ref()
        .expect("metadata handle");
    handle
        .list_cached_group(record.group_id)
        .await
        .expect("registry cache fills");
    handle.cache_registry_record(record.clone());
}

// Policy loading fails closed without realm config and group documents.
pub(super) async fn seed_policy_docs(test: &MetadataTest, group_id: GroupId) {
    let owner = UserId::nil(TEST_REALM_ID);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        user_id: owner,
        realm_id: TEST_REALM_ID,
    };
    let config = RealmConfigDocument::default_for_realm(TEST_REALM_ID, Vec::new());
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(TEST_REALM_ID);
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, TEST_REALM_ID, group_id);
    let group = Group {
        display_name: "policy-fixture".to_string(),
        group_id,
        realm_id: TEST_REALM_ID,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let writes = [
        (
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
            ByteView::from(*TEST_REALM_ID.as_bytes()),
            config.to_bytes(&actor).expect("config serializes"),
        ),
        (
            AUTH_KEYSPACE,
            ByteView::from(*TEST_REALM_ID.as_bytes()),
            realm_auth.to_bytes(&actor).expect("realm auth serializes"),
        ),
        (
            AUTH_KEYSPACE,
            ByteView::from(group_id.to_bytes().to_vec()),
            group_auth.to_bytes(&actor).expect("group auth serializes"),
        ),
        (
            GROUP_KEYSPACE,
            ByteView::from(group_id.to_bytes().to_vec()),
            group.to_bytes(&actor).expect("group serializes"),
        ),
    ];
    for (key_space, key, value) in writes {
        let existing = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.clone(),
                txn_id: None,
            })
            .await;
        if matches!(
            existing,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ) {
            continue;
        }
        let event = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: ByteView::from(value),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}

pub(super) async fn write_pending_marker(test: &MetadataTest, record: &MetadataRegistryRecord) {
    seed_policy_docs(test, record.group_id).await;
    let event = MetadataCreateEventRecord {
        event_id: record.last_event_id,
        record: record.clone(),
        user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
        node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Pending".to_string(),
            description: "Projection in flight".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
        },
        occurred_at_ms: 1,
    };
    for (key_space, key, value) in create_projection_entries(&event).expect("event encodes") {
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }
}

pub(super) async fn write_entry(test: &MetadataTest, entry: (String, ByteView, ByteView)) {
    let (key_space, key, value) = entry;
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }
}
