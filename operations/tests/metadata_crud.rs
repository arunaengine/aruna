use std::sync::Arc;
use std::time::Duration;

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DOCUMENT_SYNC_OUTBOX_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataGraphLifecycleRecord,
};
use aruna_core::storage_entries::{
    metadata_create_event_and_pending_projection_write_entries, metadata_create_event_write_entry,
    metadata_document_key, metadata_event_log_prefix, metadata_graph_lifecycle_write_entry,
    metadata_pending_projection_key, metadata_registry_key, metadata_registry_write_entries,
};
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
};
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
    drain_pending_metadata_projection_queue, project_metadata_create_event_from_log,
    project_metadata_create_events, replay_metadata_event_log,
    schedule_pending_metadata_projection_drain,
};
use aruna_operations::placement::{
    PlacementResolutionContext, choose_origin_bucket, strategy_for_target, subject_bytes,
};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentConfig, UpdateMetadataDocumentMutation, UpdateMetadataDocumentOperation,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tempfile::TempDir;
use ulid::Ulid;

struct TestContext {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    actor: Actor,
    config: RealmConfigDocument,
    context: Arc<DriverContext>,
}

impl TestContext {
    // The bucket the create operation would have chosen on this node.
    fn placement(&self, group_id: Ulid, document_id: Ulid, document_path: &str) -> PlacementRef {
        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let (strategy, _) = strategy_for_target(
            &self.config,
            &target,
            PlacementResolutionContext {
                group_id: Some(group_id),
                metadata_path: Some(document_path),
            },
        )
        .expect("metadata strategy resolves");
        choose_origin_bucket(
            &self.config,
            strategy,
            self.actor.node_id,
            &subject_bytes(&target),
        )
        .expect("origin holds a bucket")
    }
}

#[tokio::test]
async fn metadata_crud_roundtrip_uses_craqle_backend() -> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();

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

    let materialized = process_metadata_materialization_batch(test.context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);

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
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
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
    // Realm config read (bucket choice) plus the event append; a generated id
    // still skips the existing-document read a client-supplied id needs.
    assert_eq!(after - before, 2);
    Ok(())
}

#[tokio::test]
async fn metadata_event_log_replay_repairs_wal_only_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let document_path = "datasets/replay-repair";
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
    let event_id = Ulid::r#gen();
    let placement = test.placement(group_id, document_id, document_path);
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
        placement,
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
async fn scheduled_projection_queue_recovers_event_log_only_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/scheduled-recovery",
        "Scheduled Recovery Dataset",
    );
    write_create_event(&test, &create_event).await?;

    let before_recovery = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert!(before_recovery.is_empty());

    schedule_pending_metadata_projection_drain(test.context.as_ref(), Duration::ZERO).await?;
    initialize_task_incoming(test.context.clone(), TaskHandle::new()).await;

    wait_for_projected_record(&test, group_id, &record).await?;
    // The restored background timer may materialize the job before this manual batch runs.
    let materialized = process_metadata_materialization_batch(test.context.as_ref()).await?;
    assert!(materialized.processed <= 1);

    let mut fetched = None;
    for _ in 0..200 {
        if let Ok(document) = drive(
            GetMetadataDocumentOperation::new(group_id, document_id),
            test.context.as_ref(),
        )
        .await
        {
            fetched = Some(document);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let fetched = fetched.ok_or("timed out waiting for metadata materialization")?;
    assert_eq!(fetched.record, record);
    assert!(fetched.jsonld.contains("Scheduled Recovery Dataset"));
    Ok(())
}

#[tokio::test]
async fn pending_projection_marker_recovers_event_log_only_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/pending-marker-recovery",
        "Pending Marker Recovery Dataset",
    );
    write_pending_create_event(&test, &create_event).await?;
    assert!(pending_projection_marker_exists(&test, document_id, create_event.event_id).await?);

    let drained = drain_pending_metadata_projection_queue(test.context.as_ref()).await?;

    assert_eq!(drained.markers_examined, 1);
    assert_eq!(drained.projected, 1);
    assert!(!drained.has_more);
    assert!(!pending_projection_marker_exists(&test, document_id, create_event.event_id).await?);
    let listed = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(listed, vec![record]);
    Ok(())
}

#[tokio::test]
async fn targeted_projection_deletes_pending_projection_marker()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/pending-marker-delete",
        "Pending Marker Delete Dataset",
    );
    write_pending_create_event(&test, &create_event).await?;

    project_metadata_create_event_from_log(
        test.context.as_ref(),
        document_id,
        create_event.event_id,
    )
    .await?;

    assert!(!pending_projection_marker_exists(&test, document_id, create_event.event_id).await?);
    let listed = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(listed, vec![record]);
    Ok(())
}

#[tokio::test]
async fn projection_queue_replay_is_idempotent_for_already_projected_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/idempotent-replay",
        "Idempotent Replay Dataset",
    );
    write_create_event(&test, &create_event).await?;

    let projected = replay_metadata_event_log(test.context.as_ref()).await?;
    assert_eq!(projected, 1);
    assert_eq!(
        iter_keyspace_count(&test, METADATA_MATERIALIZATION_JOB_KEYSPACE).await?,
        1
    );

    let replayed_again = replay_metadata_event_log(test.context.as_ref()).await?;
    assert_eq!(replayed_again, 0);
    assert_eq!(
        iter_keyspace_count(&test, METADATA_MATERIALIZATION_JOB_KEYSPACE).await?,
        1
    );

    let listed = drive(
        ListMetadataDocumentsOperation::new(group_id),
        test.context.as_ref(),
    )
    .await?;
    assert_eq!(listed, vec![record]);
    Ok(())
}

#[tokio::test]
async fn metadata_event_log_targeted_projection_repairs_only_requested_create()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/targeted-projection",
        "Targeted Dataset",
    );
    let other_group_id = Ulid::r#gen();
    let other_document_id = Ulid::r#gen();
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

#[tokio::test]
async fn metadata_event_log_replay_does_not_resurrect_deleted_document()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/deleted-replay",
        "Deleted Replay Dataset",
    );
    write_create_event(&test, &create_event).await?;
    write_tombstone(&test, &record).await?;

    let replayed = replay_metadata_event_log(test.context.as_ref()).await?;

    assert_eq!(replayed, 0);
    assert_projection_absent(&test, &record).await?;
    Ok(())
}

#[tokio::test]
async fn projector_skips_stale_create_when_graph_tombstone_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/deleted-direct",
        "Deleted Direct Dataset",
    );
    write_tombstone(&test, &record).await?;

    let projected = project_metadata_create_events(
        test.context.as_ref(),
        vec![create_event],
        Some(test.actor.node_id),
    )
    .await?;

    assert_eq!(projected, 0);
    assert_projection_absent(&test, &record).await?;
    Ok(())
}

#[tokio::test]
async fn projector_deletes_stale_registry_when_tombstone_fence_wins()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context_without_net().await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();
    let (record, create_event) = build_create_event(
        &test,
        group_id,
        document_id,
        "datasets/deleted-repair",
        "Deleted Repair Dataset",
    );
    write_registry_rows(&test, &record).await?;
    let metadata_handle = test
        .context
        .metadata_handle
        .as_ref()
        .expect("metadata handle installed");
    let warm = metadata_handle
        .list_cached_registry_records_for_group(group_id)
        .await?;
    assert_eq!(warm.as_ref(), &vec![record.clone()]);
    metadata_handle.upsert_cached_registry_records(std::slice::from_ref(&record));
    write_tombstone(&test, &record).await?;

    let projected = project_metadata_create_events(
        test.context.as_ref(),
        vec![create_event],
        Some(test.actor.node_id),
    )
    .await?;

    assert_eq!(projected, 0);
    assert_projection_absent(&test, &record).await?;
    let visible = metadata_handle
        .list_cached_registry_records_for_group(group_id)
        .await?;
    assert!(visible.is_empty());
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
    let event_id = Ulid::r#gen();
    let placement = test.placement(group_id, document_id, document_path);
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
        placement,
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

async fn write_pending_create_event(
    test: &TestContext,
    create_event: &MetadataCreateEventRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let writes = metadata_create_event_and_pending_projection_write_entries(create_event)?;
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected pending create event write result: {other:?}").into()),
    }
}

async fn pending_projection_marker_exists(
    test: &TestContext,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<bool, Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            key: metadata_pending_projection_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected pending projection marker read: {other:?}").into()),
    }
}

async fn write_tombstone(
    test: &TestContext,
    record: &MetadataRegistryRecord,
) -> Result<MetadataGraphLifecycleRecord, Box<dyn std::error::Error>> {
    let lifecycle = MetadataGraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        record.updated_at_ms.saturating_add(1),
    );
    let (key_space, key, value) = metadata_graph_lifecycle_write_entry(&lifecycle)?;
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
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(lifecycle),
        other => Err(format!("unexpected lifecycle write result: {other:?}").into()),
    }
}

async fn write_registry_rows(
    test: &TestContext,
    record: &MetadataRegistryRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: metadata_registry_write_entries(record)?,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected registry batch write result: {other:?}").into()),
    }
}

async fn assert_projection_absent(
    test: &TestContext,
    record: &MetadataRegistryRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    assert!(
        read_storage_value(
            test,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(record.group_id, record.document_id),
        )
        .await?
        .is_none()
    );
    assert!(
        read_storage_value(
            test,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(record.document_id),
        )
        .await?
        .is_none()
    );
    assert!(
        read_storage_value(
            test,
            METADATA_HOLDERS_KEYSPACE,
            metadata_registry_key(record.group_id, record.document_id),
        )
        .await?
        .is_none()
    );
    assert_eq!(
        iter_keyspace_count(test, DOCUMENT_SYNC_OUTBOX_KEYSPACE).await?,
        0
    );
    assert_eq!(
        iter_keyspace_count(test, METADATA_MATERIALIZATION_JOB_KEYSPACE).await?,
        0
    );
    let fetched = drive(
        GetMetadataDocumentOperation::new(record.group_id, record.document_id),
        test.context.as_ref(),
    )
    .await;
    assert!(matches!(
        fetched,
        Err(GetMetadataDocumentError::DocumentNotFound)
    ));
    Ok(())
}

async fn read_storage_value(
    test: &TestContext,
    key_space: &str,
    key: aruna_core::types::Key,
) -> Result<Option<aruna_core::types::Value>, Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage read result: {other:?}").into()),
    }
}

async fn iter_keyspace_count(
    test: &TestContext,
    key_space: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    match test
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage iter result: {other:?}").into()),
    }
}

async fn wait_for_projected_record(
    test: &TestContext,
    group_id: Ulid,
    expected: &MetadataRegistryRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..200 {
        let listed = drive(
            ListMetadataDocumentsOperation::new(group_id),
            test.context.as_ref(),
        )
        .await?;
        if listed == vec![expected.clone()] {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err("timed out waiting for metadata projection".into())
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
        user_id: aruna_core::UserId::local(Ulid::r#gen(), RealmId([5u8; 32])),
        realm_id: RealmId([5u8; 32]),
    };
    let config = seed_realm_config(&storage_handle, &actor).await?;
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
        config,
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
        user_id: aruna_core::UserId::local(Ulid::r#gen(), realm_id),
        realm_id,
    };
    let config = seed_realm_config(&storage_handle, &actor).await?;
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
        config,
        context,
    })
}

async fn seed_realm_config(
    storage: &aruna_storage::StorageHandle,
    actor: &Actor,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*actor.realm_id.as_bytes()),
            value: ByteView::from(config.to_bytes(actor)?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(config),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected realm config write event: {other:?}").into()),
    }
}
