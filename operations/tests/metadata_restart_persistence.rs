use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::metadata::{MetadataEffect, MetadataEvent};
use aruna_core::structs::{Actor, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document_id,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
use aruna_storage::{FjallPersistPolicy, FjallStorage};
use tempfile::TempDir;
use ulid::Ulid;

const CHILD_MODE_ENV: &str = "ARUNA_METADATA_RESTART_PERSISTENCE_CHILD";
const CHILD_STORAGE_PATH_ENV: &str = "ARUNA_METADATA_RESTART_STORAGE_PATH";
const CHILD_METADATA_PATH_ENV: &str = "ARUNA_METADATA_RESTART_METADATA_PATH";
const CHILD_TEST_NAME: &str = "metadata_backend_restart_child_writes_and_flushes";

#[tokio::test]
async fn metadata_backend_restart_persists_after_child_flush_without_destructors()
-> Result<(), Box<dyn std::error::Error>> {
    let storage_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;

    let output = Command::new(env::current_exe()?)
        .arg("--ignored")
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, "1")
        .env(CHILD_STORAGE_PATH_ENV, storage_dir.path())
        .env(CHILD_METADATA_PATH_ENV, metadata_dir.path())
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "metadata restart child failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    let (context, _actor, _config) = build_context(storage_dir.path(), metadata_dir.path())?;

    // The child mints a structured document id at create time, so the parent
    // rediscovers it from the persisted registry rather than assuming a fixed id.
    let documents = drive(
        ListMetadataDocumentsOperation::new(group_id()),
        context.as_ref(),
    )
    .await?;
    let record = documents
        .first()
        .ok_or("no metadata document survived the restart")?;
    let document_id = record.document_id;
    let graph_iri = record.graph_iri.clone();
    assert_graph_exists(&context, &graph_iri).await?;

    let document = drive(
        GetMetadataDocumentOperation::new(group_id(), document_id),
        context.as_ref(),
    )
    .await?;
    assert_eq!(document.record.document_id, document_id);
    assert_eq!(document.record.graph_iri, graph_iri);
    assert!(document.jsonld.contains(document_name()));
    assert!(document.jsonld.contains(&document.record.graph_iri));

    Ok(())
}

#[tokio::test]
#[ignore = "spawned by metadata_backend_restart_persists_after_child_flush_without_destructors"]
async fn metadata_backend_restart_child_writes_and_flushes()
-> Result<(), Box<dyn std::error::Error>> {
    if env::var(CHILD_MODE_ENV).ok().as_deref() != Some("1") {
        return Ok(());
    }

    let storage_path = child_path(CHILD_STORAGE_PATH_ENV)?;
    let metadata_path = child_path(CHILD_METADATA_PATH_ENV)?;
    let (context, actor, config) = build_context(&storage_path, &metadata_path)?;
    seed_realm_config(&context, &actor, &config).await?;
    create_and_materialize_document(&context, actor, &config).await?;
    context
        .metadata_handle
        .as_ref()
        .expect("metadata handle installed")
        .flush_persistence()
        .await?;
    context.storage_handle.sync_all().await?;

    std::process::exit(0);
}

async fn create_and_materialize_document(
    context: &Arc<DriverContext>,
    actor: Actor,
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    let document_id =
        mint_local_document_id(config, &actor, group_id(), "datasets/restart-persistence")?;
    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor,
            group_id: group_id(),
            document_id,
            document_path: "datasets/restart-persistence".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: document_name().to_string(),
                description: "Restart persistence contract".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        context.as_ref(),
    )
    .await?;
    assert_eq!(created.record.document_id, document_id);

    let replayed = replay_metadata_event_log(context.as_ref()).await?;
    assert_eq!(replayed, 1);
    let materialized = process_metadata_materialization_batch(context.as_ref()).await?;
    assert_eq!(materialized.processed, 1);
    assert!(!materialized.has_more_due);

    Ok(())
}

async fn assert_graph_exists(
    context: &Arc<DriverContext>,
    graph_iri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match context
        .metadata_handle
        .as_ref()
        .expect("metadata handle installed")
        .send_effect(Effect::Metadata(MetadataEffect::ContainsGraph {
            graph_iri: graph_iri.to_string(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::ContainsGraphResult { exists: true, .. }) => Ok(()),
        Event::Metadata(MetadataEvent::ContainsGraphResult { exists: false, .. }) => {
            Err("metadata graph was not restored after reopen".into())
        }
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        other => Err(format!("unexpected contains-graph event: {other:?}").into()),
    }
}

fn build_context(
    storage_path: &Path,
    metadata_path: &Path,
) -> Result<(Arc<DriverContext>, Actor, RealmConfigDocument), Box<dyn std::error::Error>> {
    let storage_handle = FjallStorage::open_with_persist_policy(
        storage_path.to_str().ok_or("invalid storage path")?,
        FjallPersistPolicy::SyncAll,
    )?;
    let actor = actor();
    let metadata_handle = MetadataHandle::new_with_options(
        metadata_path,
        actor.node_id,
        storage_handle.clone(),
        None,
        None,
        None,
        MetadataHandleOptions::default()
            .with_search_storage(MetadataSearchStorage::Disk)
            .with_document_sync_persist_policy(FjallPersistPolicy::SyncAll),
    )?;
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    Ok((
        Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
        }),
        actor,
        config,
    ))
}

async fn seed_realm_config(
    context: &Arc<DriverContext>,
    actor: &Actor,
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*actor.realm_id.as_bytes()).into(),
            value: config.to_bytes(actor)?.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected realm config write event: {other:?}").into()),
    }
}

fn child_path(name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {name}").into())
}

fn actor() -> Actor {
    let realm_id = RealmId([9u8; 32]);
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        user_id: aruna_core::UserId::local(Ulid::from_parts(9, 3), realm_id),
        realm_id,
    }
}

fn group_id() -> Ulid {
    Ulid::from_parts(9, 1)
}

fn document_name() -> &'static str {
    "Restart Persistence Dataset"
}
