use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_INDEX_KEYSPACE};
use aruna_core::metadata::{
    MetadataCreateCrateRequest, MetadataEffect, MetadataEvent, MetadataGraphLifecycleRecord,
    MetadataGraphPolicy, MetadataQueryResults, MetadataRequestDurability,
    MetadataUpsertEntityRequest,
};
use aruna_core::storage_entries::{metadata_graph_lifecycle_write_entry, metadata_registry_key};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, MetadataRegistryRecord, PlacementRef,
    RealmAuthorizationDocument, RealmId,
};
use aruna_core::types::{GroupId, Key, Value};
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
use aruna_storage::FjallStorage;
use tempfile::TempDir;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const REALM: RealmId = RealmId([9u8; 32]);

fn init_logging() {
    if std::env::var("RUST_LOG").is_ok() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .try_init();
    }
}

struct TestHarness {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    storage: aruna_storage::StorageHandle,
    handle: MetadataHandle,
    group_id: GroupId,
}

async fn build_harness(backend_pool_size: Option<usize>) -> Result<TestHarness, BoxError> {
    let storage_dir = tempfile::tempdir()?;
    let metadata_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(storage_dir.path().to_str().ok_or("invalid storage path")?)?;
    let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let mut options =
        MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory);
    if let Some(pool_size) = backend_pool_size {
        options = options.with_backend_pool_size(pool_size);
    }
    let handle = MetadataHandle::new_with_options(
        metadata_dir.path(),
        node_id,
        storage.clone(),
        None,
        None,
        None,
        options,
    )?;
    Ok(TestHarness {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        storage,
        handle,
        group_id: Ulid::r#gen(),
    })
}

fn registry_record(
    group_id: GroupId,
    index: usize,
    graph_iri: Option<String>,
) -> MetadataRegistryRecord {
    let document_id = Ulid::r#gen();
    MetadataRegistryRecord {
        realm_id: REALM,
        group_id,
        document_id,
        document_path: format!("datasets/doc-{index:05}"),
        graph_iri: graph_iri.unwrap_or_else(|| MetadataRegistryRecord::graph_iri_for(document_id)),
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
    harness: &TestHarness,
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
        match harness
            .storage
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

fn crate_name(index: usize) -> String {
    format!("needle-{index:04}")
}

async fn create_crate_graph(harness: &TestHarness, index: usize) -> Result<String, BoxError> {
    let graph_iri = format!("https://w3id.org/aruna/bench-{index:04}");
    create_crate(harness, &graph_iri, &crate_name(index)).await?;
    Ok(graph_iri)
}

async fn create_crate(harness: &TestHarness, graph_iri: &str, name: &str) -> Result<(), BoxError> {
    let event = harness
        .handle
        .send_metadata_effect(MetadataEffect::CreateCrate {
            request: MetadataCreateCrateRequest {
                graph_iri: graph_iri.to_string(),
                name: name.to_string(),
                description: format!("Crate graph {name}"),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                policy: MetadataGraphPolicy {
                    public: true,
                    permission_paths: Vec::new(),
                },
                durability: MetadataRequestDurability::Durable,
                deterministic_actor: None,
            },
        })
        .await;
    match event {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. }) => Ok(()),
        other => Err(format!("create crate failed: {other:?}").into()),
    }
}

async fn query_names(harness: &TestHarness) -> Result<Vec<String>, BoxError> {
    query_names_as(harness, None).await
}

async fn query_names_as(
    harness: &TestHarness,
    auth: Option<AuthContext>,
) -> Result<Vec<String>, BoxError> {
    let results = harness
        .handle
        .query_authorized_local(
            auth,
            None,
            "SELECT ?s ?name WHERE { ?s a schema:Dataset . ?s schema:name ?name }".to_string(),
        )
        .await?;
    let MetadataQueryResults::Solutions(rows) = results else {
        return Err("expected solutions".into());
    };
    Ok(rows
        .into_iter()
        .filter_map(|row| row.get("name").cloned())
        .collect())
}

fn names_contain(names: &[String], index: usize) -> bool {
    let marker = crate_name(index);
    names.iter().any(|name| name.contains(&marker))
}

async fn wait_for(
    deadline: Duration,
    mut condition: impl AsyncFnMut() -> Result<bool, BoxError>,
) -> Result<bool, BoxError> {
    let started = Instant::now();
    loop {
        if condition().await? {
            return Ok(true);
        }
        if started.elapsed() > deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_visibility_cache_serves_reads_and_refreshes_in_background() -> Result<(), BoxError> {
    let harness = build_harness(None).await?;
    let initial_graphs = 12usize;

    let mut records = Vec::new();
    for index in 0..initial_graphs {
        let graph_iri = create_crate_graph(&harness, index).await?;
        records.push(registry_record(harness.group_id, index, Some(graph_iri)));
    }
    write_registry_records(&harness, &records).await?;

    // Cold query blocks on the first fill and sees every graph.
    let names = query_names(&harness).await?;
    for index in 0..initial_graphs {
        assert!(
            names_contain(&names, index),
            "missing graph {index} after cold fill"
        );
    }

    // A new graph lands in storage without touching the cache.
    let new_index = initial_graphs;
    let new_graph_iri = create_crate_graph(&harness, new_index).await?;
    let new_record = registry_record(harness.group_id, new_index, Some(new_graph_iri.clone()));
    write_registry_records(&harness, std::slice::from_ref(&new_record)).await?;

    harness.handle.expire_visibility_caches();

    // Stale serve: the query right after expiry must not block on a refill,
    // so it still sees the old visible set.
    let stale_names = query_names(&harness).await?;
    assert!(
        !names_contain(&stale_names, new_index),
        "stale read unexpectedly observed the new graph"
    );

    // The background refill converges to the new registry state.
    let converged = wait_for(Duration::from_secs(10), async || {
        Ok(names_contain(&query_names(&harness).await?, new_index))
    })
    .await?;
    assert!(converged, "background refill never exposed the new graph");

    // A lifecycle tombstone written to storage is picked up by the next
    // background sweep without removing the registry record.
    let lifecycle = MetadataGraphLifecycleRecord::deleted(
        new_graph_iri,
        REALM,
        harness.group_id,
        new_record.document_id,
        1,
    );
    let (key_space, key, value) = metadata_graph_lifecycle_write_entry(&lifecycle)?;
    match harness
        .storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("lifecycle write failed: {other:?}").into()),
    }

    harness.handle.expire_visibility_caches();
    // The stale read must not block on the sweep; lazy per-graph visibility
    // reads the lifecycle state at evaluation time, so it may serve either
    // the pre- or post-sweep state before converging to hidden.
    let _ = query_names(&harness).await?;
    let converged = wait_for(Duration::from_secs(10), async || {
        Ok(!names_contain(&query_names(&harness).await?, new_index))
    })
    .await?;
    assert!(converged, "background sweep never hid the tombstoned graph");

    Ok(())
}

fn visibility_record(group_id: GroupId, path: &str, public: bool) -> MetadataRegistryRecord {
    let document_id = Ulid::r#gen();
    MetadataRegistryRecord {
        realm_id: REALM,
        group_id,
        document_id,
        document_path: path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &REALM,
            group_id,
            path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        last_event_id: Ulid::nil(),
    }
}

async fn write_value(
    harness: &TestHarness,
    key_space: &str,
    key: Key,
    value: Value,
) -> Result<(), BoxError> {
    match harness
        .storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("storage write failed: {other:?}").into()),
    }
}

fn contains_name(names: &[String], marker: &str) -> bool {
    names.iter().any(|name| name.contains(marker))
}

async fn search_probe_graphs(
    harness: &TestHarness,
    auth: Option<AuthContext>,
) -> Result<std::collections::HashSet<String>, BoxError> {
    Ok(search_graph_iris(harness, auth, None, "probe", 20)
        .await?
        .into_iter()
        .collect())
}

async fn search_graph_iris(
    harness: &TestHarness,
    auth: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: &str,
    limit: usize,
) -> Result<Vec<String>, BoxError> {
    let hits = harness
        .handle
        .search_authorized_local(auth, graph_iris, query.to_string(), limit)
        .await?;
    Ok(hits.into_iter().map(|hit| hit.graph_iri).collect())
}

fn repeated_search_name(marker: &str, label: &str, repeats: usize) -> String {
    let mut name = label.to_string();
    for _ in 0..repeats {
        name.push(' ');
        name.push_str(marker);
    }
    name
}

async fn write_deleted_lifecycle(
    harness: &TestHarness,
    record: &MetadataRegistryRecord,
) -> Result<(), BoxError> {
    let lifecycle = MetadataGraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        REALM,
        record.group_id,
        record.document_id,
        1,
    );
    let (key_space, key, value) = metadata_graph_lifecycle_write_entry(&lifecycle)?;
    write_value(harness, &key_space, key, value).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_fills_visible_limit_after_invisible_matches_are_removed() -> Result<(), BoxError> {
    let harness = build_harness(None).await?;
    let marker = "visibilitylimitneedle";
    let group_id = harness.group_id;

    let public_a = visibility_record(group_id, "datasets/search-public-a", true);
    let public_b = visibility_record(group_id, "datasets/search-public-b", true);
    let mut records = vec![public_a.clone(), public_b.clone()];
    let mut private_graphs = Vec::new();
    let mut deleted_records = Vec::new();
    let mut unregistered_graphs = Vec::new();

    for index in 0..24 {
        let private = visibility_record(
            group_id,
            &format!("datasets/search-private-{index:02}"),
            false,
        );
        let private_name = repeated_search_name(marker, &format!("private-{index:02}"), 32);
        create_crate(&harness, &private.graph_iri, &private_name).await?;
        private_graphs.push(private.graph_iri.clone());
        records.push(private);

        let deleted = visibility_record(
            group_id,
            &format!("datasets/search-deleted-{index:02}"),
            true,
        );
        let deleted_name = repeated_search_name(marker, &format!("deleted-{index:02}"), 32);
        create_crate(&harness, &deleted.graph_iri, &deleted_name).await?;
        deleted_records.push(deleted.clone());
        records.push(deleted);

        let unregistered_iri = MetadataRegistryRecord::graph_iri_for(Ulid::r#gen());
        let unregistered_name =
            repeated_search_name(marker, &format!("unregistered-{index:02}"), 32);
        create_crate(&harness, &unregistered_iri, &unregistered_name).await?;
        unregistered_graphs.push(unregistered_iri);
    }

    create_crate(&harness, &public_a.graph_iri, &format!("public a {marker}")).await?;
    create_crate(&harness, &public_b.graph_iri, &format!("public b {marker}")).await?;
    write_registry_records(&harness, &records).await?;
    for deleted in &deleted_records {
        write_deleted_lifecycle(&harness, deleted).await?;
    }

    harness.handle.flush_search_updates().await?;
    let hits = search_graph_iris(&harness, None, None, marker, 2).await?;

    assert_eq!(hits.len(), 2, "search should fill the visible limit");
    assert!(hits.contains(&public_a.graph_iri));
    assert!(hits.contains(&public_b.graph_iri));
    for graph_iri in private_graphs
        .iter()
        .chain(deleted_records.iter().map(|record| &record.graph_iri))
        .chain(unregistered_graphs.iter())
    {
        assert!(!hits.contains(graph_iri));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_honors_explicit_graph_filter_before_visible_limit() -> Result<(), BoxError> {
    let harness = build_harness(None).await?;
    let marker = "graphfilterlimitneedle";
    let group_id = harness.group_id;

    let inside = visibility_record(group_id, "datasets/search-filter-inside", true);
    let mut records = vec![inside.clone()];

    for index in 0..70 {
        let outside = visibility_record(
            group_id,
            &format!("datasets/search-filter-outside-{index:02}"),
            true,
        );
        let outside_name = repeated_search_name(marker, &format!("outside-{index:02}"), 32);
        create_crate(&harness, &outside.graph_iri, &outside_name).await?;
        records.push(outside);
    }

    create_crate(&harness, &inside.graph_iri, &format!("inside {marker}")).await?;
    write_registry_records(&harness, &records).await?;

    harness.handle.flush_search_updates().await?;
    let hits = search_graph_iris(
        &harness,
        None,
        Some(vec![inside.graph_iri.clone()]),
        marker,
        1,
    )
    .await?;

    assert_eq!(hits, vec![inside.graph_iri]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_visibility_matches_eager_query_and_search_semantics() -> Result<(), BoxError> {
    let harness = build_harness(None).await?;
    let group_id = harness.group_id;
    let member = aruna_core::UserId::local(Ulid::r#gen(), REALM);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        user_id: member,
        realm_id: REALM,
    };

    let group_auth = GroupAuthorizationDocument::new_default_group_doc(member, REALM, group_id);
    let group = Group {
        display_name: "visibility-group".to_string(),
        group_id,
        realm_id: REALM,
        roles: group_auth.roles.keys().copied().collect(),
        owner: member,
    };
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(REALM);
    write_value(
        &harness,
        AUTH_KEYSPACE,
        (*REALM.as_bytes()).into(),
        realm_auth.to_bytes(&actor)?.into(),
    )
    .await?;
    write_value(
        &harness,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(&actor)?.into(),
    )
    .await?;
    write_value(
        &harness,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor)?.into(),
    )
    .await?;

    let public_record = visibility_record(group_id, "datasets/probe-public", true);
    let private_record = visibility_record(group_id, "datasets/probe-private", false);
    let deleted_record = visibility_record(group_id, "datasets/probe-deleted", true);
    let unregistered_iri = MetadataRegistryRecord::graph_iri_for(Ulid::r#gen());
    create_crate(&harness, &public_record.graph_iri, "probe public").await?;
    create_crate(&harness, &private_record.graph_iri, "probe private").await?;
    create_crate(&harness, &deleted_record.graph_iri, "probe deleted").await?;
    create_crate(&harness, &unregistered_iri, "probe unregistered").await?;
    write_registry_records(
        &harness,
        &[
            public_record.clone(),
            private_record.clone(),
            deleted_record.clone(),
        ],
    )
    .await?;

    let lifecycle = MetadataGraphLifecycleRecord::deleted(
        deleted_record.graph_iri.clone(),
        REALM,
        group_id,
        deleted_record.document_id,
        1,
    );
    let (key_space, key, value) = metadata_graph_lifecycle_write_entry(&lifecycle)?;
    write_value(&harness, &key_space, key, value).await?;

    let member_auth = AuthContext {
        user_id: member,
        realm_id: REALM,
        path_restrictions: None,
    };
    let outsider_auth = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::r#gen(), REALM),
        realm_id: REALM,
        path_restrictions: None,
    };

    let anonymous = query_names_as(&harness, None).await?;
    assert!(contains_name(&anonymous, "probe public"));
    assert!(!contains_name(&anonymous, "probe private"));
    assert!(!contains_name(&anonymous, "probe deleted"));
    assert!(!contains_name(&anonymous, "probe unregistered"));

    let member_names = query_names_as(&harness, Some(member_auth.clone())).await?;
    assert!(contains_name(&member_names, "probe public"));
    assert!(contains_name(&member_names, "probe private"));
    assert!(!contains_name(&member_names, "probe deleted"));
    assert!(!contains_name(&member_names, "probe unregistered"));

    let outsider_names = query_names_as(&harness, Some(outsider_auth.clone())).await?;
    assert!(contains_name(&outsider_names, "probe public"));
    assert!(!contains_name(&outsider_names, "probe private"));
    assert!(!contains_name(&outsider_names, "probe deleted"));
    assert!(!contains_name(&outsider_names, "probe unregistered"));

    harness.handle.flush_search_updates().await?;
    let anonymous_hits = search_probe_graphs(&harness, None).await?;
    assert!(anonymous_hits.contains(&public_record.graph_iri));
    assert!(!anonymous_hits.contains(&private_record.graph_iri));
    assert!(!anonymous_hits.contains(&deleted_record.graph_iri));
    assert!(!anonymous_hits.contains(&unregistered_iri));

    let member_hits = search_probe_graphs(&harness, Some(member_auth)).await?;
    assert!(member_hits.contains(&public_record.graph_iri));
    assert!(member_hits.contains(&private_record.graph_iri));
    assert!(!member_hits.contains(&deleted_record.graph_iri));
    assert!(!member_hits.contains(&unregistered_iri));

    let outsider_hits = search_probe_graphs(&harness, Some(outsider_auth)).await?;
    assert!(outsider_hits.contains(&public_record.graph_iri));
    assert!(!outsider_hits.contains(&private_record.graph_iri));

    // A doc created after the snapshot fill becomes visible through the
    // incremental registry upsert without waiting for a refill.
    let late_record = visibility_record(group_id, "datasets/probe-late", true);
    create_crate(&harness, &late_record.graph_iri, "probe late").await?;
    write_registry_records(&harness, std::slice::from_ref(&late_record)).await?;
    harness
        .handle
        .upsert_cached_registry_record(late_record.clone());
    let names = query_names_as(&harness, None).await?;
    assert!(contains_name(&names, "probe late"));

    Ok(())
}

fn percentile(sorted: &[Duration], pct: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[((sorted.len() - 1) * pct) / 100]
}

fn print_stats(label: &str, mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    let p50 = percentile(&samples, 50);
    println!(
        "{label}: n={} p50={:?} p95={:?} max={:?}",
        samples.len(),
        p50,
        percentile(&samples, 95),
        samples.last().copied().unwrap_or_default(),
    );
    p50
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing-sensitive concurrency profile; run manually"]
async fn concurrent_queries_with_mutation_load_profile() -> Result<(), BoxError> {
    init_logging();
    // Pool sized like a typical 8-core cluster node so mutation pressure on
    // the permit pools is visible regardless of the host's core count.
    let harness = Arc::new(build_harness(Some(8)).await?);
    let real_graphs = 16usize;
    let synthetic_records = 20_000usize;
    let concurrency = 8usize;
    let samples = 10usize;
    let writer_tasks = 32usize;

    let mut records = Vec::new();
    for index in 0..real_graphs {
        let graph_iri = create_crate_graph(&harness, index).await?;
        records.push(registry_record(harness.group_id, index, Some(graph_iri)));
    }
    for index in real_graphs..(real_graphs + synthetic_records) {
        records.push(registry_record(harness.group_id, index, None));
    }
    write_registry_records(&harness, &records).await?;

    let cold_started = Instant::now();
    let names = query_names(&harness).await?;
    println!(
        "cold query: {:?} ({} rows over {} registry records)",
        cold_started.elapsed(),
        names.len(),
        records.len()
    );

    let seq = {
        let mut latencies = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            let _ = query_names(&harness).await?;
            latencies.push(started.elapsed());
        }
        print_stats("sequential", latencies)
    };

    let stale_started = Instant::now();
    harness.handle.expire_visibility_caches();
    let _ = query_names(&harness).await?;
    println!(
        "stale-serve query after TTL expiry: {:?}",
        stale_started.elapsed()
    );

    let run_concurrent = |label: &'static str| {
        let harness = harness.clone();
        async move {
            let wall = Instant::now();
            let mut tasks = Vec::new();
            for _ in 0..concurrency {
                let harness = harness.clone();
                tasks.push(tokio::spawn(async move {
                    let mut latencies = Vec::with_capacity(samples);
                    for _ in 0..samples {
                        let started = Instant::now();
                        query_names(&harness).await.expect("query failed");
                        latencies.push(started.elapsed());
                    }
                    latencies
                }));
            }
            let mut latencies = Vec::new();
            for task in tasks {
                latencies.extend(task.await.expect("task panicked"));
            }
            let wall = wall.elapsed();
            let p50 = print_stats(label, latencies);
            println!(
                "{label}: wall={:?} {:.1} qps",
                wall,
                (concurrency * samples) as f64 / wall.as_secs_f64()
            );
            p50
        }
    };

    let conc_idle = run_concurrent("concurrent idle").await;

    // Sustained heavy mutation load saturating the mutation permit pool,
    // mirroring the materialization queue draining apply batches in the
    // cluster while reads arrive.
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for writer in 0..writer_tasks {
        let harness = harness.clone();
        let stop = stop.clone();
        writers.push(tokio::spawn(async move {
            let keywords = (0..1024)
                .map(|keyword| format!("\"keyword-{keyword:03}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let mut round = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let graph_iri =
                    format!("https://w3id.org/aruna/bench-{:04}", writer % real_graphs);
                let jsonld = format!(
                    "{{\"@id\": \"./load-{writer}-{round}.dat\", \"@type\": \"MediaObject\", \"name\": \"load-{writer}-{round}\", \"keywords\": [{keywords}]}}"
                );
                let event = harness
                    .handle
                    .send_metadata_effect(MetadataEffect::UpsertDataEntity {
                        request: MetadataUpsertEntityRequest {
                            graph_iri,
                            jsonld,
                            durability: Default::default(),
                            deterministic_actor: None,
                        },
                    })
                    .await;
                if let Event::Metadata(MetadataEvent::Error { error, .. }) = event {
                    panic!("mutation load failed: {error:?}");
                }
                round += 1;
            }
        }));
    }

    let conc_loaded = run_concurrent("concurrent with mutation load").await;
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.await.expect("writer panicked");
    }

    println!(
        "summary: seq p50={seq:?} concurrent idle p50={conc_idle:?} concurrent loaded p50={conc_loaded:?}"
    );
    Ok(())
}
