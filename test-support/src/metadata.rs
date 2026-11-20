//! Runs the metadata materialization and graph prune passes, exports RO-Crates and refuses
//! writes to one keyspace for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::git::GitNode;
use aruna_core::metadata::{GraphLifecycleRecord, MetadataEffect, MetadataError, MetadataEvent};
use aruna_drivers::driver::{DriverContext, drive, node_routing};
use aruna_drivers::testing::send_metadata_effect;
use aruna_operations::metadata::materialization_queue::{
    JobsExistOperation, MaterializationBatchOperation, MetadataDrainResult,
    MetadataMaterializationError, materialization_group_concurrency,
};
use aruna_operations::metadata::prune_queue::{
    GraphDrainResult, MetadataGraphError, PruneBatchOperation, TombstoneProcessingResult,
    TombstonesOperation, jobs_queued,
};
use aruna_storage::storage::handle::StorageHandle;

/// Runs one pass of the metadata materialization drain, as the drain task does.
pub async fn process_materialization_batch(
    context: &DriverContext,
) -> Result<MetadataDrainResult, MetadataMaterializationError> {
    let node = GitNode {
        ids: context.node_and_realm(),
        routing: node_routing(context),
    };
    let concurrency = materialization_group_concurrency();
    let link_timer = context.task_handle.is_some();
    drive(
        MaterializationBatchOperation::new(node, concurrency, link_timer),
        context,
    )
    .await
}

/// Whether a live metadata materialization job is queued; dead ones it meets are deleted.
pub async fn materialization_jobs_exist(
    context: &DriverContext,
) -> Result<bool, MetadataMaterializationError> {
    drive(JobsExistOperation::new(), context).await
}

/// Runs one pass of the graph prune drain.
pub async fn process_prune_batch(
    context: &DriverContext,
) -> Result<GraphDrainResult, MetadataGraphError> {
    drive(PruneBatchOperation::new(context.node_scope()), context).await
}

/// Queues and runs the prunes for deleted graphs.
pub async fn process_graph_tombstones(
    context: &DriverContext,
    tombstones: impl IntoIterator<Item = GraphLifecycleRecord>,
) -> TombstoneProcessingResult {
    drive(TombstonesOperation::new(tombstones), context)
        .await
        .unwrap_or_default()
}

/// Whether any graph prune job is queued.
pub async fn prune_jobs_exist(context: &DriverContext) -> Result<bool, MetadataGraphError> {
    let due = drive(jobs_queued(), context).await?;
    Ok(!due.jobs.is_empty())
}

/// The RO-Crate JSON-LD of one graph, read from this node's graph store.
pub async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: String,
) -> Result<String, MetadataError> {
    let export = MetadataEffect::ExportRoCrate { graph_iri };
    match send_metadata_effect(context, export).await {
        Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => Ok(jsonld),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata export event: {}",
            other.label()
        ))),
    }
}

/// A storage that forwards every effect to `real`, except writes and deletes that touch
/// `key_space`, which it refuses as a failed write.
pub fn refusing(real: StorageHandle, key_space: &'static str) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    for receiver in [receivers.foreground, receivers.bulk] {
        let real = real.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("proxy runtime builds");
            while let Ok((effect, response, ..)) = receiver.recv() {
                let touched = match &effect {
                    StorageEffect::Write {
                        key_space: space, ..
                    }
                    | StorageEffect::Delete {
                        key_space: space, ..
                    } => space == key_space,
                    StorageEffect::BatchWrite { writes, .. } => {
                        writes.iter().any(|(space, ..)| space == key_space)
                    }
                    StorageEffect::BatchDelete { deletes, .. } => {
                        deletes.iter().any(|(space, _)| space == key_space)
                    }
                    _ => false,
                };
                if touched {
                    let error = StorageError::WriteError("refused by the test".to_string());
                    response.send(StorageEvent::Error { error });
                    continue;
                }
                let Event::Storage(event) = runtime.block_on(real.send_storage_effect(effect))
                else {
                    panic!("storage answers with storage events");
                };
                response.send(event);
            }
        });
    }
    handle
}
