//! Debug probe (not a release gate): measures create-path latency with the
//! materialization drain running concurrently vs. left idle. Run pinned to a
//! few cores to mimic a cluster pod:
//! `taskset -c 0-3 cargo test -p aruna-operations --test metadata_create_backpressure -- --ignored --nocapture`

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::UserId;
use aruna_core::structs::{Actor, RealmId};
use aruna_core::types::GroupId;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::projector::project_metadata_create_events_from_log;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const WRITERS: usize = 96;
const PER_WRITER: usize = 64;
const PROJECTION_BATCH: usize = 16;

struct ProbeNode {
    _temp_dir: TempDir,
    context: Arc<DriverContext>,
}

async fn spawn_probe_node(with_drains: bool) -> Result<ProbeNode, BoxError> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(
        temp_dir
            .path()
            .join("fjall")
            .to_str()
            .ok_or("invalid storage path")?,
    )?;
    let node_id = iroh::SecretKey::generate().public();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        node_id,
        storage.clone(),
        None,
        None,
        None,
    )?;
    let task_handle = with_drains.then(TaskHandle::new);
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: task_handle.clone(),
    });
    if let Some(task_handle) = task_handle {
        initialize_task_incoming(context.clone(), task_handle).await;
    }
    Ok(ProbeNode {
        _temp_dir: temp_dir,
        context,
    })
}

fn scaffold_payload(writer: usize, index: usize) -> CreateMetadataDocumentPayload {
    CreateMetadataDocumentPayload::Scaffold {
        name: format!("Probe Dataset {writer}-{index}"),
        description: "Create backpressure probe".to_string(),
        date_published: "2026-06-11".to_string(),
        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
    }
}

fn rocrate_payload(document_id: Ulid) -> CreateMetadataDocumentPayload {
    let jsonld = format!(
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
      "name": "Probe Crate {document_id}",
      "description": "Create backpressure probe crate",
      "datePublished": "2026-06-11",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
    );
    CreateMetadataDocumentPayload::RoCrate { jsonld }
}

async fn run_writer(
    realm_id: RealmId,
    group_id: GroupId,
    writer: usize,
    context: Arc<DriverContext>,
) -> Result<Vec<Duration>, BoxError> {
    let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let mut latencies = Vec::with_capacity(PER_WRITER);
    let mut batch = Vec::new();
    for index in 0..PER_WRITER {
        let document_id = Ulid::r#gen();
        let payload = if index % 2 == 0 {
            scaffold_payload(writer, index)
        } else {
            rocrate_payload(document_id)
        };
        let started = Instant::now();
        let created = drive(
            CreateMetadataDocumentOperation::new_for_generated_document_id(
                CreateMetadataDocumentConfig {
                    actor: Actor {
                        node_id,
                        user_id: UserId::local(Ulid::r#gen(), realm_id),
                        realm_id,
                    },
                    group_id,
                    document_id,
                    document_path: format!("datasets/probe-{writer}-{index}"),
                    public: true,
                    payload,
                },
            ),
            context.as_ref(),
        )
        .await
        .map_err(|error| format!("create failed writer={writer} index={index}: {error:?}"))?;
        latencies.push(started.elapsed());
        batch.push((created.record.document_id, created.event_id));
        if batch.len() >= PROJECTION_BATCH {
            project_metadata_create_events_from_log(context.as_ref(), batch.drain(..))
                .await
                .map_err(|error| format!("projection failed: {error:?}"))?;
        }
    }
    if !batch.is_empty() {
        project_metadata_create_events_from_log(context.as_ref(), batch)
            .await
            .map_err(|error| format!("projection failed: {error:?}"))?;
    }
    Ok(latencies)
}

async fn run_phase(label: &str, with_drains: bool) -> Result<(), BoxError> {
    let node = spawn_probe_node(with_drains).await?;
    let realm_id = RealmId([55u8; 32]);
    let group_id = Ulid::r#gen();

    let started = Instant::now();
    let mut handles = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let context = node.context.clone();
        handles.push(tokio::spawn(async move {
            run_writer(realm_id, group_id, writer, context).await
        }));
    }
    let mut latencies = Vec::with_capacity(WRITERS * PER_WRITER);
    for handle in handles {
        latencies.extend(handle.await??);
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let ops = latencies.len() as f64 / elapsed;
    let percentile = |fraction: f64| {
        let index = ((latencies.len() - 1) as f64 * fraction).round() as usize;
        latencies[index].as_secs_f64() * 1_000.0
    };
    println!(
        "{label}: creates={} elapsed={elapsed:.3}s ops_per_sec={ops:.1} p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        latencies.len(),
        percentile(0.50),
        percentile(0.95),
        percentile(0.99),
    );
    Ok(())
}

#[test]
#[ignore]
fn create_backpressure_probe() -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_phase("drains_idle", false))?;
    runtime.block_on(run_phase("drains_active", true))?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}
