//! Drives outbox reads and the projection, materialization and prune passes, and builds job ids,
//! refreshes and group backends for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentOutboxRecord;
use aruna_core::document_apply::ApplyError;
use aruna_core::id_mint::IdMint;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structured_id::{BucketId, ClockHealthError, PlacementHandle};
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;

use crate::metadata::deletion_guard::DeletionGuardOperation;
use crate::sync::document_outbox::{
    OutboxPagePlan, OutboxReadBatch, OutboxTailsOperation, ReadOutboxPlan,
};
use aruna_drivers::driver::{DriverContext, drive};
use aruna_drivers::testing::test_context;

/// The outbox record stored at `key`.
pub async fn read_outbox_record(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<DocumentOutboxRecord>, ApplyError> {
    let plan = ReadOutboxPlan { key: key.to_vec() };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// One page of outbox records under `prefix`.
pub async fn read_outbox_records(
    storage: &StorageHandle,
    prefix: &[u8],
    start_after: Option<Vec<u8>>,
    limit: usize,
) -> Result<OutboxReadBatch, ApplyError> {
    let plan = OutboxPagePlan {
        prefix: prefix.to_vec(),
        start_after,
        limit,
    };
    drive(PlanOperation::new(plan), &test_context(storage.clone())).await
}

/// The last key of every outbox stream that holds a record.
pub async fn read_outbox_tails(
    storage: &StorageHandle,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ApplyError> {
    drive(OutboxTailsOperation::new(), &test_context(storage.clone())).await
}

/// Runs one pass of the pending projection drain.
pub async fn drain_projection_queue(
    context: &DriverContext,
) -> Result<
    crate::metadata::projection::PendingProjectionResult,
    crate::metadata::projection::MetadataProjectionError,
> {
    use crate::metadata::projection::DrainProjectionOperation;
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let drain = DrainProjectionOperation::new(local_node);
    aruna_drivers::driver::drive(drain, context).await
}

/// Projects carried events.
pub async fn project_create_events(
    context: &DriverContext,
    events: Vec<aruna_core::metadata::MetadataEventRecord>,
    local_node_id: Option<aruna_core::NodeId>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::ProjectEventsOperation;
    let project = ProjectEventsOperation::new(events, local_node_id);
    aruna_drivers::driver::drive(project, context).await
}

/// Runs one pass of the metadata materialization drain, as the drain task does.
pub async fn process_materialization_batch(
    context: &DriverContext,
) -> Result<
    crate::metadata::materialization_queue::MetadataDrainResult,
    crate::metadata::materialization_queue::MetadataMaterializationError,
> {
    use crate::metadata::materialization_queue::{
        MaterializationBatchOperation, materialization_group_concurrency,
    };
    let node = crate::git::GitNode {
        ids: context.node_and_realm(),
        routing: aruna_drivers::driver::node_routing(context),
    };
    let concurrency = materialization_group_concurrency();
    let batch =
        MaterializationBatchOperation::new(node, concurrency, context.task_handle.is_some());
    drive(batch, context).await
}

/// Whether a live metadata materialization job is queued; dead ones it meets are deleted.
pub async fn materialization_jobs_exist(
    context: &DriverContext,
) -> Result<bool, crate::metadata::materialization_queue::MetadataMaterializationError> {
    let probe = crate::metadata::materialization_queue::JobsExistOperation::new();
    drive(probe, context).await
}

/// Runs one pass of the graph prune drain.
pub async fn process_prune_batch(
    context: &DriverContext,
) -> Result<
    crate::metadata::prune_queue::GraphDrainResult,
    crate::metadata::prune_queue::MetadataGraphError,
> {
    let batch = crate::metadata::prune_queue::PruneBatchOperation::new(context.node_scope());
    aruna_drivers::driver::drive(batch, context).await
}

/// Queues and runs the prunes for deleted graphs.
pub async fn process_graph_tombstones(
    context: &DriverContext,
    tombstones: impl IntoIterator<Item = aruna_core::metadata::GraphLifecycleRecord>,
) -> crate::metadata::prune_queue::TombstoneProcessingResult {
    let tombstones = crate::metadata::prune_queue::TombstonesOperation::new(tombstones);
    aruna_drivers::driver::drive(tombstones, context)
        .await
        .unwrap_or_default()
}

/// Whether any graph prune job is queued.
pub async fn prune_jobs_exist(
    context: &DriverContext,
) -> Result<bool, crate::metadata::prune_queue::MetadataGraphError> {
    let due =
        aruna_drivers::driver::drive(crate::metadata::prune_queue::jobs_queued(), context).await?;
    Ok(!due.jobs.is_empty())
}

/// A fresh job id for `(handle, bucket)` at the current time, as a submit would mint it.
pub fn fresh_job_id(handle: PlacementHandle, bucket: BucketId) -> Result<JobId, ClockHealthError> {
    let mut ids = IdMint::new(ulid::Ulid::generate().random());
    crate::jobs::submit::mint_job_id(handle, bucket, &mut ids, unix_timestamp_millis())
}

/// The RO-Crate JSON-LD of one graph.
pub async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: String,
) -> Result<String, MetadataError> {
    let export = MetadataEffect::ExportRoCrate {
        graph_iri: graph_iri.clone(),
    };
    match drive(DeletionGuardOperation::new(graph_iri, export), context).await? {
        MetadataEvent::RoCrateExportResult { jsonld, .. } => Ok(jsonld),
        MetadataEvent::Error { error, .. } => Err(error),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata export event: {}",
            other.label()
        ))),
    }
}

/// A refresh of `version_id` at `bucket`/`key` with `metadata`, by node 4 for a user of realm 9.
pub fn reference_refresh(
    bucket: String,
    key: String,
    version_id: ulid::Ulid,
    metadata: aruna_core::structs::execution::source_access::SourceMetadata,
    refreshed_at: std::time::SystemTime,
) -> crate::s3::object::metadata::ReferenceRefresh {
    crate::s3::object::metadata::ReferenceRefresh {
        bucket,
        key,
        version_id,
        metadata: Some(metadata),
        refreshed_at,
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        auth_context: aruna_core::structs::identity::auth::AuthContext::new(
            Default::default(),
            RealmId([9u8; 32]),
        ),
    }
}

/// An enabled or `disabled` S3 backend of group 1 that keeps its data on removal.
pub fn group_storage(
    backend_id: ulid::Ulid,
    disabled: bool,
) -> aruna_core::structs::storage::group_backend::GroupStorage {
    aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id,
        group_id: ulid::Ulid::from_bytes([1u8; 16]),
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: std::collections::HashMap::new(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    }
}
