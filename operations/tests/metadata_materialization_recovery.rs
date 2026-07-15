use std::sync::Arc;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_MATERIALIZATION_JOB_KEYSPACE;
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataBatch, MetadataCreateCrateRequest,
    MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataEffect, MetadataEvent,
    MetadataGraphPolicy, MetadataMaterializationState, MetadataMaterializationStatusRecord,
    MetadataRequestDurability, MetadataUpsertEntityRequest, deterministic_materialization_actor,
};
use aruna_core::storage_entries::{
    metadata_create_event_write_entry, metadata_materialization_job_write_entry,
    metadata_materialization_status_key, metadata_materialization_status_write_entry,
};
use aruna_core::structs::{Actor, MetadataRegistryRecord, PlacementRef, RealmId};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::{
    new_materialization_job, new_pending_materialization_status,
    process_metadata_materialization_batch,
};
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::TempDir;
use ulid::Ulid;

struct TestContext {
    _storage_dir: TempDir,
    _metadata_dir: Option<TempDir>,
    actor: Actor,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn crash_after_craqle_apply_before_finish_retries_idempotently()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(true).await?;
    let document_id = Ulid::from_bytes([1u8; 16]);
    let event_id = Ulid::from_parts(10, 1);
    let event = create_event(&test, document_id, event_id, "crash-window");
    let status = new_pending_materialization_status(&event, 1);
    let job = new_materialization_job(&event, 1);
    write_entries(
        &test.context.storage_handle,
        vec![
            metadata_create_event_write_entry(&event)?,
            metadata_materialization_status_write_entry(&status)?,
            metadata_materialization_job_write_entry(&job)?,
        ],
    )
    .await?;

    let metadata_handle = test
        .context
        .metadata_handle
        .as_ref()
        .expect("metadata handle installed");
    match metadata_handle
        .send_effect(materialization_effect(&event))
        .await
    {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. }) => {}
        other => return Err(format!("unexpected metadata event: {other:?}").into()),
    }

    let drained = process_metadata_materialization_batch(test.context.as_ref()).await?;

    assert_eq!(drained.processed, 1);
    assert_eq!(job_count(&test.context.storage_handle).await?, 0);
    let status = read_status(&test.context.storage_handle, document_id)
        .await?
        .expect("materialization status exists");
    assert_eq!(status.event_id, event_id);
    assert_eq!(status.state, MetadataMaterializationState::Materialized);
    assert_eq!(status.last_error, None);
    Ok(())
}

#[tokio::test]
async fn final_status_with_leftover_job_is_cleaned_without_reapply()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(false).await?;
    let document_id = Ulid::from_bytes([2u8; 16]);
    let event_id = Ulid::from_parts(20, 1);
    let event = create_event(&test, document_id, event_id, "final-leftover");
    let job = new_materialization_job(&event, 1);
    let final_status = MetadataMaterializationStatusRecord {
        document_id,
        event_id,
        graph_iri: event.record.graph_iri.clone(),
        state: MetadataMaterializationState::Materialized,
        attempts: 1,
        last_error: None,
        updated_at_ms: 2,
    };
    write_entries(
        &test.context.storage_handle,
        vec![
            metadata_materialization_status_write_entry(&final_status)?,
            metadata_materialization_job_write_entry(&job)?,
        ],
    )
    .await?;

    let drained = process_metadata_materialization_batch(test.context.as_ref()).await?;

    assert_eq!(drained.processed, 1);
    assert_eq!(job_count(&test.context.storage_handle).await?, 0);
    assert_eq!(
        read_status(&test.context.storage_handle, document_id).await?,
        Some(final_status)
    );
    Ok(())
}

#[tokio::test]
async fn entity_upsert_materialization_replay_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let test = build_context(true).await?;
    let document_id = Ulid::from_bytes([3u8; 16]);
    let create_event = create_event(&test, document_id, Ulid::from_parts(30, 1), "entity-replay");
    let metadata_handle = test
        .context
        .metadata_handle
        .as_ref()
        .expect("metadata handle installed");
    match metadata_handle
        .send_effect(materialization_effect(&create_event))
        .await
    {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. }) => {}
        other => return Err(format!("unexpected metadata event: {other:?}").into()),
    }

    let upsert_event_id = Ulid::from_parts(30, 2);
    let upsert_event = create_event_with_payload(
        &test,
        document_id,
        upsert_event_id,
        "entity-replay",
        MetadataCreateEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file"}"#.to_string(),
        },
    );

    assert_replayed_upsert_is_idempotent(metadata_handle, &upsert_event).await?;

    let contextual_event_id = Ulid::from_parts(30, 3);
    let contextual_event = create_event_with_payload(
        &test,
        document_id,
        contextual_event_id,
        "entity-replay",
        MetadataCreateEventPayload::UpsertContextualEntity {
            jsonld: r##"{"@id":"#lab","@type":"Organization","name":"lab"}"##.to_string(),
        },
    );
    assert_replayed_upsert_is_idempotent(metadata_handle, &contextual_event).await?;
    Ok(())
}

async fn assert_replayed_upsert_is_idempotent(
    metadata_handle: &MetadataHandle,
    event: &MetadataCreateEventRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let first_batch = materialize_entity_upsert(metadata_handle, event).await?;
    assert_eq!(
        first_batch.actor,
        deterministic_materialization_actor(event.event_id)
    );
    assert!(!first_batch.ops.is_empty());

    let replay_batch = materialize_entity_upsert(metadata_handle, event).await?;
    assert_eq!(
        replay_batch.actor,
        deterministic_materialization_actor(event.event_id)
    );
    assert_eq!(replay_batch.counter, 0);
    assert!(replay_batch.ops.is_empty());
    Ok(())
}

async fn materialize_entity_upsert(
    metadata_handle: &MetadataHandle,
    event: &MetadataCreateEventRecord,
) -> Result<MetadataBatch, Box<dyn std::error::Error>> {
    match metadata_handle
        .send_effect(materialization_effect(event))
        .await
    {
        Event::Metadata(MetadataEvent::EntityUpsertResult { batch, .. }) => Ok(batch),
        other => Err(format!("unexpected metadata event: {other:?}").into()),
    }
}

fn create_event(
    test: &TestContext,
    document_id: MetaResourceId,
    event_id: Ulid,
    name: &str,
) -> MetadataCreateEventRecord {
    create_event_with_payload(
        test,
        document_id,
        event_id,
        name,
        MetadataCreateEventPayload::Scaffold {
            name: name.to_string(),
            description: "Materialization recovery".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
    )
}

fn create_event_with_payload(
    test: &TestContext,
    document_id: MetaResourceId,
    event_id: Ulid,
    name: &str,
    payload: MetadataCreateEventPayload,
) -> MetadataCreateEventRecord {
    let group_id = Ulid::from_parts(1, 1);
    let document_path = format!("datasets/{name}");
    let record = MetadataRegistryRecord {
        realm_id: test.actor.realm_id,
        group_id,
        document_id,
        document_path: document_path.clone(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &test.actor.realm_id,
            group_id,
            &document_path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![test.actor.node_id],
        created_at_ms: 1,
        updated_at_ms: 1,
        last_event_id: event_id,
    };
    MetadataCreateEventRecord {
        event_id,
        record,
        user_id: test.actor.user_id,
        node_id: test.actor.node_id,
        payload,
        occurred_at_ms: 1,
    }
}

fn materialization_effect(event: &MetadataCreateEventRecord) -> Effect {
    let policy = MetadataGraphPolicy {
        public: event.record.public,
        permission_paths: vec![event.record.permission_path.clone()],
    }
    .normalized();
    let deterministic_actor = Some(deterministic_materialization_actor(event.event_id));
    match &event.payload {
        MetadataCreateEventPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => Effect::Metadata(MetadataEffect::CreateCrate {
            request: MetadataCreateCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        }),
        MetadataCreateEventPayload::RoCrate { jsonld }
        | MetadataCreateEventPayload::ReplaceRoCrate { jsonld } => {
            Effect::Metadata(MetadataEffect::ApplyRoCrate {
                request: MetadataApplyRoCrateRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataCreateEventPayload::UpsertDataEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertDataEntity {
                request: MetadataUpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataCreateEventPayload::UpsertContextualEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity {
                request: MetadataUpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
    }
}

async fn write_entries(
    storage: &StorageHandle,
    writes: Vec<(String, aruna_core::types::Key, aruna_core::types::Value)>,
) -> Result<(), Box<dyn std::error::Error>> {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn read_status(
    storage: &StorageHandle,
    document_id: MetaResourceId,
) -> Result<Option<MetadataMaterializationStatusRecord>, Box<dyn std::error::Error>> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: aruna_core::keyspaces::METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: metadata_materialization_status_key(document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(postcard::from_bytes(&value)?)),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn job_count(storage: &StorageHandle) -> Result<usize, Box<dyn std::error::Error>> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(format!("unexpected storage event: {other:?}").into()),
    }
}

async fn build_context(with_metadata: bool) -> Result<TestContext, Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let storage_handle = FjallStorage::open(storage_dir.path().to_str().ok_or("invalid path")?)?;
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::local(Ulid::from_parts(8, 1), realm_id),
        realm_id,
    };
    let metadata_dir = if with_metadata {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let metadata_handle = match metadata_dir.as_ref() {
        Some(dir) => Some(MetadataHandle::new(
            dir.path(),
            node_id,
            storage_handle.clone(),
            None,
            None,
            None,
        )?),
        None => None,
    };
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle,
        task_handle: None,
    });
    Ok(TestContext {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        actor,
        context,
    })
}
