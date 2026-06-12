use std::sync::Arc;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
use aruna_core::storage_entries::{metadata_create_event_write_entry, metadata_event_log_prefix};
use aruna_core::structs::{Actor, MetadataRegistryRecord, RealmId};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::delete_metadata_document::DeleteMetadataDocumentOperation;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::{
    GetMetadataDocumentError, GetMetadataDocumentOperation,
};
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::{
    project_metadata_create_event_from_log, replay_metadata_event_log,
};
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentMutation, UpdateMetadataDocumentOperation,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

struct TestContext {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    actor: Actor,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn metadata_crud_roundtrip_uses_craqle_backend() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            document_path: "datasets/public-dataset".to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Initial Dataset".to_string(),
                description: "Created through Craqle".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        test.context.as_ref(),
    )
    .await?
    .record;

    assert_eq!(created.document_id, document_id);
    assert_eq!(
        created.graph_iri,
        format!("https://w3id.org/aruna/{document_id}")
    );
    assert_eq!(created.document_path, "datasets/public-dataset");
    assert_eq!(created.holder_node_ids, vec![test.actor.node_id]);

    let create_events = read_create_events(&test, document_id).await?;
    assert_eq!(create_events.len(), 1);
    let create_event = &create_events[0];
    assert_eq!(create_event.record, created);
    assert_eq!(create_event.user_id, test.actor.user_id);
    assert_eq!(create_event.node_id, test.actor.node_id);
    assert!(matches!(
        &create_event.payload,
        MetadataCreateEventPayload::Scaffold { name, .. } if name == "Initial Dataset"
    ));

    let listed_before_projection = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert!(listed_before_projection.is_empty());

    let replayed = replay_metadata_event_log(test.context.as_ref()).await?;
    assert_eq!(replayed, 1);
    let materialized = process_metadata_materialization_batch(test.context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);

    let listed = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(listed, vec![created.clone()]);

    let fetched = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(fetched.record, created);
    assert!(fetched.jsonld.contains("Initial Dataset"));
    assert!(
        fetched
            .jsonld
            .contains(&format!("https://w3id.org/aruna/{document_id}"))
    );

    let updated_jsonld = format!(
        r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Updated Dataset",
      "description": "Updated through Craqle",
      "datePublished": "2026-02-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );

    let updated = drive(
        UpdateMetadataDocumentOperation::new(UpdateMetadataDocumentConfig {
            actor: test.actor.clone(),
            group_id,
            document_id,
            public: true,
            mutation: UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: updated_jsonld,
            },
        }),
        test.context.as_ref(),
    )
    .await?;
    assert!(updated.public);

    let fetched_after_update = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert!(fetched_after_update.record.public);
    assert!(fetched_after_update.jsonld.contains("Updated Dataset"));
    assert!(
        fetched_after_update
            .jsonld
            .contains("Updated through Craqle")
    );

    drive(
        DeleteMetadataDocumentOperation::new(test.actor.clone(), group_id, document_id),
        test.context.as_ref(),
    )
    .await?;

    let deleted = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await;
    assert!(matches!(
        deleted,
        Err(GetMetadataDocumentError::DocumentNotFound)
    ));

    if let Some(net_handle) = &test.context.net_handle {
        net_handle.shutdown().await;
    }

    Ok(())
}

#[tokio::test]
async fn generated_metadata_create_foreground_storage_effect_count_is_reduced()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let before = test
        .context
        .storage_handle
        .snapshot_metrics()
        .requests_total;

    let created = drive(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor: test.actor.clone(),
                group_id,
                document_id,
                document_path: "datasets/generated-fast-path".to_string(),
                public: true,
                payload: CreateMetadataDocumentPayload::Scaffold {
                    name: "Generated Fast Path".to_string(),
                    description: "Generated ids avoid duplicate foreground reads".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                },
            },
        ),
        test.context.as_ref(),
    )
    .await?;

    let after = test
        .context
        .storage_handle
        .snapshot_metrics()
        .requests_total;

    assert_eq!(created.record.document_id, document_id);
    assert_eq!(after - before, 1);
    Ok(())
}

#[tokio::test]
async fn metadata_event_log_replay_repairs_wal_only_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let document_path = "datasets/replay-repair";
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
    let event_id = Ulid::new();
    let record = MetadataRegistryRecord {
        realm_id: test.actor.realm_id,
        group_id,
        document_id,
        document_path: MetadataRegistryRecord::normalize_document_path(document_path),
        graph_iri: graph_iri.clone(),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &test.actor.realm_id,
            group_id,
            document_path,
            document_id,
        ),
        holder_node_ids: vec![test.actor.node_id],
        created_at_ms: 1,
        updated_at_ms: 1,
        last_event_id: event_id,
    };
    let create_event = MetadataCreateEventRecord {
        event_id,
        record: record.clone(),
        user_id: test.actor.user_id,
        node_id: test.actor.node_id,
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Replayed Dataset".to_string(),
            description: "Recovered from the metadata WAL".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
        occurred_at_ms: 1,
    };
    let (key_space, key, value) = metadata_create_event_write_entry(&create_event)?;
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
        other => return Err(format!("unexpected create event write result: {other:?}").into()),
    }

    let before_replay = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await;
    assert!(matches!(
        before_replay,
        Err(GetMetadataDocumentError::DocumentNotFound)
    ));

    let replayed = replay_metadata_event_log(test.context.as_ref()).await?;
    assert_eq!(replayed, 1);
    let materialized = process_metadata_materialization_batch(test.context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);

    let fetched = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(fetched.record, record);
    assert!(fetched.jsonld.contains("Replayed Dataset"));
    assert!(fetched.jsonld.contains(&graph_iri));

    if let Some(net_handle) = &test.context.net_handle {
        net_handle.shutdown().await;
    }

    Ok(())
}

#[tokio::test]
async fn metadata_event_log_targeted_projection_repairs_only_requested_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::new();
    let document_id = Ulid::new();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/targeted-projection",
        "Targeted Dataset",
    );
    let other_group_id = Ulid::new();
    let other_document_id = Ulid::new();
    let (_, other_create_event) = build_create_event(
        &test,
        other_group_id,
        other_document_id,
        "datasets/not-projected",
        "Unprojected Dataset",
    );
    write_create_event(&test, &create_event).await?;
    write_create_event(&test, &other_create_event).await?;

    project_metadata_create_event_from_log(
        test.context.as_ref(),
        document_id,
        create_event.event_id,
    )
    .await?;
    let materialized = process_metadata_materialization_batch(test.context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);

    let fetched = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(fetched.record, record);
    assert!(fetched.jsonld.contains("Targeted Dataset"));

    let unprojected = drive(
        GetMetadataDocumentOperation::new(other_group_id, other_document_id),
        test.context.as_ref(),
    )
    .await;
    assert!(matches!(
        unprojected,
        Err(GetMetadataDocumentError::DocumentNotFound)
    ));

    if let Some(net_handle) = &test.context.net_handle {
        net_handle.shutdown().await;
    }

    Ok(())
}

fn build_create_event(
    test: &TestContext,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
    name: &str,
) -> (MetadataRegistryRecord, MetadataCreateEventRecord) {
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
    let event_id = Ulid::new();
    let record = MetadataRegistryRecord {
        realm_id: test.actor.realm_id,
        group_id,
        document_id,
        document_path: MetadataRegistryRecord::normalize_document_path(document_path),
        graph_iri,
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &test.actor.realm_id,
            group_id,
            document_path,
            document_id,
        ),
        holder_node_ids: vec![test.actor.node_id],
        created_at_ms: 1,
        updated_at_ms: 1,
        last_event_id: event_id,
    };
    let event = MetadataCreateEventRecord {
        event_id,
        record: record.clone(),
        user_id: test.actor.user_id,
        node_id: test.actor.node_id,
        payload: MetadataCreateEventPayload::Scaffold {
            name: name.to_string(),
            description: "Recovered from the metadata WAL".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
        occurred_at_ms: 1,
    };
    (record, event)
}

async fn write_create_event(
    test: &TestContext,
    create_event: &MetadataCreateEventRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let (key_space, key, value) = metadata_create_event_write_entry(create_event)?;
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
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected create event write result: {other:?}").into()),
    }
}

async fn read_create_events(
    test: &TestContext,
    document_id: Ulid,
) -> Result<Vec<MetadataCreateEventRecord>, Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            prefix: Some(metadata_event_log_prefix(document_id)),
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| postcard::from_bytes(&value).map_err(Into::into))
            .collect(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn build_context() -> Result<TestContext, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage_handle =
        FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone()).await?;
    let node_id = net_handle.node_id();
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )?;
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::local(Ulid::new(), RealmId([5u8; 32])),
        realm_id: RealmId([5u8; 32]),
    };
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(TaskHandle::new()),
    });
    Ok(TestContext {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        context,
    })
}

async fn build_context_without_net() -> Result<TestContext, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage_handle =
        FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;
    let realm_id = RealmId([6u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )?;
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
        realm_id,
    };
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(TaskHandle::new()),
    });
    Ok(TestContext {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        context,
    })
}
