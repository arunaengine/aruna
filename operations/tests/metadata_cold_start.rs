use std::time::Instant;

use aruna_core::MetaResourceId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_INDEX_KEYSPACE;
use aruna_core::metadata::{
    MetadataCreateCrateRequest, MetadataEffect, MetadataEvent, MetadataGraphPolicy,
    MetadataQueryResults, MetadataRequestDurability,
};
use aruna_core::storage_entries::metadata_registry_key;
use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
use aruna_core::types::GroupId;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
use aruna_storage::FjallStorage;
use ulid::Ulid;

fn doc_id(seed: u64) -> MetaResourceId {
    MetaResourceId::try_from((1u128 << 60) | u128::from(seed)).unwrap()
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const REALM: RealmId = RealmId([9u8; 32]);

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn open_handle(
    metadata_dir: &std::path::Path,
    storage: &aruna_storage::StorageHandle,
) -> Result<MetadataHandle, BoxError> {
    let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    Ok(MetadataHandle::new_with_options(
        metadata_dir,
        node_id,
        storage.clone(),
        None,
        None,
        None,
        MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory),
    )?)
}

fn graph_iri(index: usize) -> String {
    format!("https://w3id.org/aruna/cold-{index:05}")
}

fn registry_record(group_id: GroupId, index: usize) -> MetadataRegistryRecord {
    let document_id = doc_id(index as u64 + 1);
    MetadataRegistryRecord {
        realm_id: REALM,
        group_id,
        document_id,
        document_path: format!("datasets/doc-{index:05}"),
        graph_iri: graph_iri(index),
        public: true,
        permission_path: format!("/realm/g/{group_id}/meta/datasets/doc-{index:05}@{document_id}"),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        last_event_id: Ulid::nil(),
    }
}

async fn write_registry_records(
    storage: &aruna_storage::StorageHandle,
    records: &[MetadataRegistryRecord],
) -> Result<(), BoxError> {
    for chunk in records.chunks(512) {
        let writes = chunk
            .iter()
            .map(|record| {
                Ok((
                    METADATA_INDEX_KEYSPACE.to_string(),
                    metadata_registry_key(record.group_id, record.document_id),
                    postcard::to_allocvec(record)?.into(),
                ))
            })
            .collect::<Result<Vec<_>, BoxError>>()?;
        match storage
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => return Err(format!("registry batch write failed: {other:?}").into()),
        }
    }
    Ok(())
}

async fn create_crate_graph(handle: &MetadataHandle, index: usize) -> Result<(), BoxError> {
    let event = handle
        .send_metadata_effect(MetadataEffect::CreateCrate {
            request: MetadataCreateCrateRequest {
                graph_iri: graph_iri(index),
                name: format!("Cold Start Dataset {index:05}"),
                description: format!("Cold start corpus graph {index}"),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                policy: MetadataGraphPolicy {
                    public: true,
                    permission_paths: Vec::new(),
                },
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor: None,
            },
        })
        .await;
    match event {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. }) => Ok(()),
        other => Err(format!("create crate failed: {other:?}").into()),
    }
}

async fn timed_query(
    handle: &MetadataHandle,
    label: &str,
) -> Result<std::time::Duration, BoxError> {
    let started = Instant::now();
    let results = handle
        .query_authorized_local(
            None,
            None,
            "SELECT ?s ?name WHERE { ?s a schema:Dataset . ?s schema:name ?name } LIMIT 25"
                .to_string(),
        )
        .await?;
    let elapsed = started.elapsed();
    let MetadataQueryResults::Solutions(rows) = results else {
        return Err("expected solutions".into());
    };
    assert_eq!(rows.len(), 25, "{label}: expected 25 rows");
    println!("{label}: {elapsed:?}");
    Ok(elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "cold-start profile over a 40k-doc node; run manually (release)"]
async fn first_query_on_cold_node_with_40k_docs() -> Result<(), BoxError> {
    let docs = env_usize("ARUNA_COLD_START_DOCS", 40_000);
    let writers = env_usize("ARUNA_COLD_START_WRITERS", 16);

    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;

    {
        let handle = std::sync::Arc::new(open_handle(metadata_dir.path(), &storage)?);
        let group_id = Ulid::r#gen();

        let seed_started = Instant::now();
        let mut tasks = Vec::new();
        for writer in 0..writers {
            let handle = handle.clone();
            tasks.push(tokio::spawn(async move {
                let mut index = writer;
                while index < docs {
                    create_crate_graph(&handle, index).await?;
                    index += writers;
                }
                Ok::<_, BoxError>(())
            }));
        }
        for task in tasks {
            task.await??;
        }
        let records = (0..docs)
            .map(|index| registry_record(group_id, index))
            .collect::<Vec<_>>();
        write_registry_records(&storage, &records).await?;
        println!(
            "seeded {docs} docs ({} graphs) in {:?}",
            docs,
            seed_started.elapsed()
        );
    }

    // Fresh handle over the same stores: caches empty, craqle reopened.
    let open_started = Instant::now();
    let handle = open_handle(metadata_dir.path(), &storage)?;
    println!("metadata handle reopen: {:?}", open_started.elapsed());

    let cold = timed_query(&handle, "first query (true cold)").await?;
    let warm = timed_query(&handle, "second query (warm)").await?;

    // Cold fill of the handle-owned list-path registry cache over the same storage.
    let list_fill_started = Instant::now();
    let listed = handle.list_cached_registry_records().await?;
    println!(
        "registry cache cold fill: {:?} ({} records)",
        list_fill_started.elapsed(),
        listed.len()
    );

    // Warmed boot path: a third handle primed via warm_caches before the
    // first query, mirroring spawn_metadata_warmup at node boot.
    drop(handle);
    let handle = open_handle(metadata_dir.path(), &storage)?;
    let warmup_started = Instant::now();
    handle.warm_caches().await?;
    println!("warm_caches after reopen: {:?}", warmup_started.elapsed());
    let warmed = timed_query(&handle, "first query (after warmup)").await?;

    println!("summary: docs={docs} cold={cold:?} warm={warm:?} warmed-first={warmed:?}");
    Ok(())
}
