use aruna_blob::blob::{BlobHandle, GroupHold};
use aruna_compute::ExecutorRegistry;
use aruna_core::effects::{AuditPageEffect, Effect, JobControlEffect, NetEffect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{
    AuditPageEvent, BlobEvent, Event, JobControlEvent, NetEvent, StorageEvent, SubOperationEvent,
};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::{Operation, SubOperation};
use aruna_core::structs::{
    BackendCatalog, BackendRef, BucketInfo, GroupRoutingInputs, NodeRouting, RoutingSnapshot,
    StorageRoutingRule, UsageCounters, usage_backend_keys,
};
use aruna_core::types::{GroupId, TxnId};
use aruna_net::NetHandle;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use futures_util::{StreamExt, stream};
use std::any::{type_name, type_name_of_val};
use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;
use tracing::{Instrument, debug, debug_span, error, trace, warn};

use crate::group_backends::{RecordReadError, parse_read};
use crate::group_routing::{GroupRoutingInputsError, GroupRoutingInputsOperation};
use crate::metadata::MetadataHandle;
use crate::task_persistence::persist_task_effect;
use aruna_core::events::NetError;
use aruna_core::metadata::{MetadataError, MetadataEvent};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};

/// Node-local routing inputs for a caller assembling an operation config.
/// Pure in-memory state: operations never fetch this from inside a step.
pub fn node_routing(context: &DriverContext) -> NodeRouting {
    context
        .blob_handle
        .as_ref()
        .map(|handle| handle.routing())
        .unwrap_or_default()
}

/// Why a write could not learn where it belongs. Absent records are not an
/// error; only an unreadable or undecodable one is.
#[derive(Debug, Error, PartialEq)]
pub enum RoutingInputsError {
    #[error("group routing inputs unavailable: {0}")]
    GroupInputs(#[from] GroupRoutingInputsError),
    #[error("bucket routing rules unavailable: {0}")]
    BucketRules(#[from] RecordReadError),
    /// Not `#[from]`: `BucketRules` already owns the conversion from a read.
    #[error("backend usage counters unavailable: {0}")]
    BackendUsage(#[source] RecordReadError),
}

impl RoutingInputsError {
    /// The underlying storage failure, so retrying callers can tell a transient
    /// read failure from a record that will never decode.
    pub fn storage(&self) -> Option<&aruna_core::errors::StorageError> {
        let read = match self {
            Self::GroupInputs(GroupRoutingInputsError::Read(read)) => read,
            Self::BucketRules(read) | Self::BackendUsage(read) => read,
            Self::GroupInputs(GroupRoutingInputsError::Incomplete) => return None,
        };
        match read {
            RecordReadError::Storage(error) => Some(error),
            RecordReadError::Conversion(_) | RecordReadError::Unexpected => None,
        }
    }
}

/// The group's default target plus the ids of the backends it registered. Only
/// the named group's ids are ever loaded.
async fn group_inputs(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<GroupRoutingInputs, RoutingInputsError> {
    Ok(drive(GroupRoutingInputsOperation::new(group_id), context).await?)
}

/// Bucket rules for callers that do not already hold the bucket record. A
/// bucket without a record simply has no rules.
async fn bucket_rules(
    context: &DriverContext,
    bucket: &str,
) -> Result<Vec<StorageRoutingRule>, RoutingInputsError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    Ok(parse_read(event, BucketInfo::from_bytes)?
        .map(|info| info.storage_routing)
        .unwrap_or_default())
}

/// Routing inputs for one bucket write, assembled before the operation starts.
/// Failing here fails the write: a partial snapshot would route it to the node
/// default and D3/D4 record that choice for good.
pub async fn routing_snapshot(
    context: &DriverContext,
    group_id: GroupId,
    bucket: &str,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let snapshot = node_routing(context)
        .snapshot(group_id)
        .with_group_inputs(group_inputs(context, group_id).await?)
        .with_bucket_rules(bucket_rules(context, bucket).await?);
    mark_full_backends(context, snapshot).await
}

/// The same inputs when the caller already holds the bucket record, as the S3
/// surface does from its auth middleware.
pub async fn bucket_snapshot(
    context: &DriverContext,
    bucket: &BucketInfo,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let snapshot = node_routing(context)
        .snapshot(bucket.group_id)
        .with_group_inputs(group_inputs(context, bucket.group_id).await?)
        .with_bucket_rules(bucket.storage_routing.clone());
    mark_full_backends(context, snapshot).await
}

/// Node routing whose capped backends already carry their fullness, for the
/// background writers that build their own snapshot later. Replication reads
/// the same catalog, so an unreadable counter refuses it too and it retries.
pub async fn quota_marked_routing(
    context: &DriverContext,
) -> Result<NodeRouting, RoutingInputsError> {
    let routing = node_routing(context);
    let catalog = mark_full_catalog(context, routing.catalog.clone()).await?;
    Ok(NodeRouting { catalog, ..routing })
}

async fn mark_full_backends(
    context: &DriverContext,
    snapshot: RoutingSnapshot,
) -> Result<RoutingSnapshot, RoutingInputsError> {
    let catalog = mark_full_catalog(context, snapshot.catalog.clone()).await?;
    Ok(RoutingSnapshot {
        catalog,
        ..snapshot
    })
}

/// Freezes each capped backend's fullness for one request, exactly like the
/// group quota ceiling. Concurrent writes can overshoot by their own bytes; an
/// unreadable counter fails the caller rather than routing past the cap.
async fn mark_full_catalog(
    context: &DriverContext,
    catalog: BackendCatalog,
) -> Result<BackendCatalog, RoutingInputsError> {
    let quotas = catalog.quotas();
    if quotas.is_empty() {
        return Ok(catalog);
    }
    let mut catalog = catalog;
    for (name, quota) in quotas {
        let used = backend_used_bytes(context, &BackendRef::Node(name.clone()))
            .await
            .map_err(RoutingInputsError::BackendUsage)?;
        if used >= quota {
            warn!(backend = %name, quota_bytes = quota, "Storage backend reached its quota");
            catalog = catalog.mark_full(&name);
        }
    }
    Ok(catalog)
}

/// Sums one backend's stored-byte shards. A missing row reads as zero, so a node
/// whose counters were never built reports no usage; an unreadable or
/// undecodable shard is an error, never a zero.
pub async fn backend_used_bytes(
    context: &DriverContext,
    backend: &BackendRef,
) -> Result<u64, RecordReadError> {
    let reads = usage_backend_keys(backend)
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
        .collect::<Vec<_>>();
    let values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(RecordReadError::Unexpected),
    };
    let mut total = 0u64;
    for (_, value) in values {
        let Some(value) = value else { continue };
        total = total.saturating_add(UsageCounters::from_bytes(value.as_ref())?.stored_bytes);
    }
    Ok(total)
}

#[derive(Clone)]
pub struct DriverContext {
    pub storage_handle: storage::StorageHandle,
    pub net_handle: Option<NetHandle>,
    pub blob_handle: Option<BlobHandle>,
    pub metadata_handle: Option<MetadataHandle>,
    pub task_handle: Option<TaskHandle>,
    /// Enabled executor backends; `None` on nodes with no compute plane.
    pub compute_handle: Option<std::sync::Arc<ExecutorRegistry>>,
}

impl std::fmt::Debug for DriverContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverContext")
            .field("net_handle", &self.net_handle)
            .field("blob_handle", &self.blob_handle.is_some())
            .field("metadata_handle", &self.metadata_handle.is_some())
            .field("task_handle", &self.task_handle.is_some())
            .field("compute_handle", &self.compute_handle.is_some())
            .finish_non_exhaustive()
    }
}

const MAX_SUBOP_DEPTH: usize = 32;
const AUDIT_FANOUT_CONCURRENCY: usize = 8;
const AUDIT_FANOUT_DEADLINE: Duration = Duration::from_secs(30);

#[tracing::instrument(
    name = "operation.effect",
    level = "debug",
    skip(effect, context),
    fields(depth, effect = effect_kind(&effect))
)]
async fn dispatch_effect(effect: Effect, context: &DriverContext, depth: usize) -> Event {
    let effect_name = effect_kind(&effect);
    if depth == 0 {
        tracing::debug!(
            effect = effect_name,
            "Dispatching top-level operation effect"
        );
    }
    trace!(
        event = "operation.effect.dispatch",
        depth,
        effect = effect_name,
        "Dispatching operation effect"
    );

    let event = match effect {
        Effect::Blob(blob_effect) => {
            if let Some(blob_handle) = &context.blob_handle {
                Box::pin(blob_handle.send_blob_effect(blob_effect)).await
            } else {
                Event::Blob(BlobEvent::Error(BlobError::HandleMissing))
            }
        }
        Effect::StagingSource(staging_source_effect) => {
            if crate::native_reference::is_native_effect(&staging_source_effect) {
                Box::pin(crate::native_reference::send_native_effect(
                    staging_source_effect,
                    context,
                ))
                .await
            } else if let Some(blob_handle) = &context.blob_handle {
                Box::pin(blob_handle.send_staging_source_effect(staging_source_effect)).await
            } else {
                Event::StagingSource(aruna_core::events::StagingSourceEvent::Error {
                    error: aruna_core::errors::StagingSourceError::HandleMissing,
                })
            }
        }
        Effect::Storage(storage_effect) => {
            let realm_config_write = match &storage_effect {
                StorageEffect::Write {
                    key_space,
                    value,
                    txn_id: None,
                    ..
                } if key_space == REALM_CONFIG_KEYSPACE => Some(value.clone()),
                _ => None,
            };
            let refresh_after_commit =
                matches!(&storage_effect, StorageEffect::CommitTransaction { .. });
            let event = Box::pin(context.storage_handle.send_storage_effect(storage_effect)).await;
            if let Some(net_handle) = context.net_handle.as_ref() {
                match (&event, realm_config_write) {
                    (
                        Event::Storage(aruna_core::events::StorageEvent::WriteResult { .. }),
                        Some(bytes),
                    ) => {
                        if let Err(error) =
                            Box::pin(net_handle.refresh_realm_peers_from_bytes(&bytes)).await
                        {
                            warn!(error = %error, "Failed to refresh realm peers from written realm config");
                        }
                    }
                    (
                        Event::Storage(aruna_core::events::StorageEvent::TransactionCommitted {
                            ..
                        }),
                        _,
                    ) if refresh_after_commit => {
                        if let Err(error) = Box::pin(net_handle.reload_realm_peers()).await {
                            warn!(error = %error, "Failed to refresh realm peers after storage commit");
                        }
                    }
                    _ => {}
                }
            }
            event
        }
        // Job-control routing runs its frame I/O here, where the runner holds
        // the context; the net crate never sees this effect.
        Effect::Net(NetEffect::JobControl(job_control)) => {
            Box::pin(dispatch_job_control(*job_control, context)).await
        }
        // Audit fan-out runs its frame I/O here for the same reason.
        Effect::Net(NetEffect::AuditPage(audit)) => {
            Box::pin(dispatch_audit_page(*audit, context)).await
        }
        Effect::Net(net_effect) => {
            if let Some(net_handle) = &context.net_handle {
                Box::pin(net_handle.send_effect(Effect::Net(net_effect))).await
            } else {
                match net_effect {
                    aruna_core::effects::NetEffect::DocumentSync(
                        DocumentSyncEffect::PublishDocuments { documents, .. },
                    ) => Event::Net(NetEvent::DocumentSync(
                        DocumentSyncNetEvent::DocumentsPublished {
                            targets: documents
                                .into_iter()
                                .map(|document| document.target().clone())
                                .collect(),
                        },
                    )),
                    _ => Event::Net(NetEvent::Error(NetError::ChannelClosed)),
                }
            }
        }
        Effect::Metadata(metadata_effect) => {
            if let Some(metadata_handle) = &context.metadata_handle {
                Box::pin(metadata_handle.send_effect(Effect::Metadata(metadata_effect))).await
            } else {
                Event::Metadata(MetadataEvent::Error {
                    graph_iri: None,
                    error: MetadataError::HandleMissing,
                })
            }
        }
        Effect::SubOperation(sub_operation) => {
            if depth >= MAX_SUBOP_DEPTH {
                Event::SubOperation(SubOperationEvent::DepthLimitExceeded {
                    max_depth: MAX_SUBOP_DEPTH,
                })
            } else {
                let context = context.clone();
                tokio::spawn(
                    async move { drive_suboperation(sub_operation, &context, depth + 1).await },
                )
                .await
                .expect("suboperation task panicked or was cancelled")
            }
        }
        Effect::Task(task_effect) => {
            if let Err(message) = persist_task_effect(&context.storage_handle, &task_effect).await {
                return Event::Task(TaskEvent::Error {
                    key: task_effect_key(&task_effect),
                    message,
                });
            }
            if let Some(task_handle) = &context.task_handle {
                Box::pin(task_handle.send_effect(Effect::Task(task_effect))).await
            } else {
                Event::Task(TaskEvent::Error {
                    key: None,
                    message: "task handle unavailable".to_string(),
                })
            }
        }
        Effect::Search() => {
            tracing::warn!(
                depth,
                effect = effect_name,
                "Search effect is not handled by driver yet"
            );
            Event::Search()
        }
        Effect::Stream() => {
            tracing::warn!(
                depth,
                effect = effect_name,
                "Top-level stream effect is not handled by driver yet"
            );
            Event::Stream()
        }
    };

    trace!(
        event = "operation.effect.result",
        depth,
        effect = effect_name,
        result = event_kind(&event),
        "Received operation event"
    );
    if depth == 0 {
        tracing::debug!(
            effect = effect_name,
            result = event_kind(&event),
            "Received top-level operation event"
        );
    }

    event
}

/// Executes a job-control request by opening the frame stream and reading the
/// owner's reply; an unreachable owner is reported so the routing operation can
/// map it to `Unavailable` (503). The artifact body path stays out of band.
async fn dispatch_job_control(effect: JobControlEffect, context: &DriverContext) -> Event {
    let JobControlEffect { owner, request } = effect;
    let event = match crate::jobs::protocol::send_job_request(context, owner, request).await {
        Ok(reply) => JobControlEvent::Response(Box::new(reply.response)),
        Err(error) => JobControlEvent::Unavailable(error.to_string()),
    };
    Event::Net(NetEvent::JobControl(event))
}

/// Requests every node's local audit page over the metadata control transport,
/// concurrently so one unreachable node cannot spend the whole request deadline.
/// An unreachable or denied node is reported so the aggregator records it missing.
async fn dispatch_audit_page(effect: AuditPageEffect, context: &DriverContext) -> Event {
    let AuditPageEffect {
        nodes: input_nodes,
        request,
    } = effect;
    let mut remaining = BTreeSet::new();
    let nodes = input_nodes
        .into_iter()
        .filter(|node| remaining.insert(*node))
        .collect::<Vec<_>>();
    let mut pages = Vec::new();
    {
        let requests = stream::iter(nodes.into_iter().map(|node| {
            let request = request.clone();
            async move {
                let page = match crate::metadata::audit::send_audit_request(context, node, request)
                    .await
                {
                    Ok(response) => AuditPageEvent::Page {
                        node,
                        response: Box::new(response),
                    },
                    Err(error) => AuditPageEvent::Unavailable {
                        node,
                        message: format!("{error:?}"),
                    },
                };
                (node, page)
            }
        }))
        .buffer_unordered(AUDIT_FANOUT_CONCURRENCY);
        futures_util::pin_mut!(requests);
        let deadline = tokio::time::Instant::now() + AUDIT_FANOUT_DEADLINE;
        loop {
            let next = match tokio::time::timeout_at(deadline, requests.next()).await {
                Ok(next) => next,
                Err(_) => break,
            };
            let Some((node, page)) = next else {
                break;
            };
            remaining.remove(&node);
            pages.push(page);
        }
    }
    pages.extend(
        remaining
            .into_iter()
            .map(|node| AuditPageEvent::Unavailable {
                node,
                message: "audit fan-out deadline exceeded".to_string(),
            }),
    );
    Event::Net(NetEvent::AuditPages(pages))
}

/// Reserves every tenant backend an effect names for the rest of the operation.
/// The bytes land inside one effect, but the transaction naming them commits
/// after it returns, and that commit's rollback runs later still.
fn hold_backends(context: &DriverContext, effect: &Effect, holds: &mut Vec<GroupHold>) {
    let (Effect::Blob(blob_effect), Some(blob_handle)) = (effect, context.blob_handle.as_ref())
    else {
        return;
    };
    if let Ok(Some(hold)) = blob_handle.hold_backends(blob_effect) {
        holds.push(hold);
    }
}

fn task_effect_key(effect: &TaskEffect) -> Option<TaskKey> {
    match effect {
        TaskEffect::ResetTimer { key, .. }
        | TaskEffect::ShortenTimer { key, .. }
        | TaskEffect::CancelTimer { key }
        | TaskEffect::AbortRunningHandlers { key } => Some(key.clone()),
    }
}

fn note_transaction_effect(open_txn: &mut Option<TxnId>, effect: &Effect) {
    if let Effect::Storage(
        StorageEffect::CommitTransaction { txn_id } | StorageEffect::AbortTransaction { txn_id },
    ) = effect
        && *open_txn == Some(*txn_id)
    {
        *open_txn = None;
    }
}

fn note_transaction_event(open_txn: &mut Option<TxnId>, event: &Event) {
    if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
        *open_txn = Some(*txn_id);
    }
}

async fn abort_leaked_transaction(open_txn: Option<TxnId>, context: &DriverContext, depth: usize) {
    if let Some(txn_id) = open_txn {
        warn!(%txn_id, "Aborting a transaction left open by an operation");
        let _ = Box::pin(dispatch_effect(
            Effect::Storage(StorageEffect::AbortTransaction { txn_id }),
            context,
            depth,
        ))
        .await;
    }
}

fn drive_suboperation<'a>(
    mut operation: Box<dyn SubOperation>,
    context: &'a DriverContext,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Event> + Send + 'a>> {
    let operation_name = type_name_of_val(&*operation).to_string();
    Box::pin(async move {
        let span = debug_span!("suboperation", operation = %operation_name, depth);
        async move {
            trace!(
                event = "suboperation.started",
                operation = %operation_name,
                depth,
                "Starting suboperation"
            );
            let mut queue: VecDeque<_> = operation.start().into_iter().collect();
            let mut holds = Vec::new();
            let mut open_txn = None;

            while !operation.is_complete() {
                while let Some(effect) = queue.pop_front() {
                    note_transaction_effect(&mut open_txn, &effect);
                    hold_backends(context, &effect, &mut holds);
                    let event = dispatch_effect(effect, context, depth).await;
                    note_transaction_event(&mut open_txn, &event);
                    queue.extend(operation.step(event));
                }

                if queue.is_empty() && !operation.is_complete() {
                    queue.extend(operation.abort());
                    if queue.is_empty() {
                        break;
                    }
                }
            }

            abort_leaked_transaction(open_txn, context, depth).await;
            trace!(
                event = "suboperation.completed",
                operation = %operation_name,
                depth,
                "Completed suboperation"
            );
            operation.finalize()
        }
        .instrument(span)
        .await
    })
}

/// Drives an operation under one wall-clock deadline. On expiry the operation's
/// own `abort` runs and its cleanup effects are dispatched unbounded; racing a
/// timeout against `drive` instead drops the future and strands what it holds.
#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive_until<O: Operation>(
    mut operation: O,
    context: &DriverContext,
    deadline: tokio::time::Instant,
) -> Result<O::Output, O::Error> {
    let mut queue: VecDeque<_> = operation.start().into_iter().collect();
    let mut expired = false;
    let mut holds = Vec::new();
    let mut open_txn = None;

    while !operation.is_complete() {
        while let Some(effect) = queue.pop_front() {
            note_transaction_effect(&mut open_txn, &effect);
            hold_backends(context, &effect, &mut holds);
            let dispatch = Box::pin(dispatch_effect(effect, context, 0));
            let event = if expired {
                dispatch.await
            } else {
                match tokio::time::timeout_at(deadline, dispatch).await {
                    Ok(event) => event,
                    Err(_) => {
                        expired = true;
                        warn!(
                            operation = %type_name::<O>(),
                            "Operation deadline expired; running its abort path"
                        );
                        queue.clear();
                        queue.extend(operation.abort());
                        continue;
                    }
                }
            };
            note_transaction_event(&mut open_txn, &event);
            queue.extend(operation.step(event));
        }

        if queue.is_empty() && !operation.is_complete() {
            queue.extend(operation.abort());
            if queue.is_empty() {
                break;
            }
        }
    }
    abort_leaked_transaction(open_txn, context, 0).await;
    operation.finalize()
}

#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive<O: Operation>(
    mut operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    let operation_name = type_name::<O>();

    trace!(
        event = "operation.started",
        operation = %operation_name,
        "Starting operation"
    );

    let mut queue: VecDeque<_> = operation.start().into_iter().collect();
    let mut holds = Vec::new();
    let mut open_txn = None;

    while !operation.is_complete() {
        while let Some(effect) = queue.pop_front() {
            note_transaction_effect(&mut open_txn, &effect);
            hold_backends(context, &effect, &mut holds);
            let event = Box::pin(dispatch_effect(effect, context, 0)).await;
            note_transaction_event(&mut open_txn, &event);
            queue.extend(operation.step(event));
        }

        if queue.is_empty() && !operation.is_complete() {
            queue.extend(operation.abort());
            if queue.is_empty() {
                break;
            }
        }
    }
    abort_leaked_transaction(open_txn, context, 0).await;
    let result = operation.finalize();
    match &result {
        Ok(_) => trace!(
            event = "operation.completed",
            operation = %operation_name,
            "Completed operation"
        ),
        Err(error) if O::expected_error(error) => debug!(
            event = "operation.rejected",
            operation = %operation_name,
            error = ?error,
            "Operation rejected"
        ),
        Err(error) => error!(
            event = "operation.failed",
            operation = %operation_name,
            error = ?error,
            "Operation failed"
        ),
    }
    result
}

fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::Storage(_) => "storage",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
    }
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Blob(_) => "blob",
        Event::StagingSource(_) => "staging_source",
        Event::Storage(_) => "storage",
        Event::Net(_) => "net",
        Event::Metadata(_) => "metadata",
        Event::SubOperation(_) => "suboperation",
        Event::Task(_) => "task",
        Event::Search() => "search",
        Event::Stream() => "stream",
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use aruna_core::{
        effects::{Effect, StagingSourceEffect, StorageEffect},
        events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent},
        operation::{Operation, boxed_suboperation},
        structs::{ResolvedSourceAccess, SourceConnectorKind},
        task::{TaskEffect, TaskKey},
    };
    use aruna_storage::storage;
    use byteview::ByteView;
    use std::convert::Infallible;
    use tempfile::tempdir;

    #[tokio::test]
    async fn snapshot_reads_rules() {
        // The snapshot seam has to pick up both stored scopes, not stay empty.
        use crate::driver::{bucket_snapshot, routing_snapshot};
        use aruna_core::keyspaces::{GROUP_STORAGE_ROUTING_KEYSPACE, S3_BUCKET_KEYSPACE};
        use aruna_core::structs::{
            BucketInfo, GroupStorageRouting, RoutingTarget, StorageRoutingRule,
        };
        use std::time::SystemTime;

        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let group_id = ulid::Ulid::generate();
        let rule = StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTarget::Class("cold".to_string()),
        };
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            replication: None,
            storage_routing: vec![rule.clone()],
        };
        let record = GroupStorageRouting {
            group_id,
            default_target: Some(RoutingTarget::Class("archive".to_string())),
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: aruna_core::UserId::default(),
        };
        write_value(
            &context,
            S3_BUCKET_KEYSPACE,
            b"routed".to_vec(),
            info.to_bytes().unwrap(),
        )
        .await;
        write_value(
            &context,
            GROUP_STORAGE_ROUTING_KEYSPACE,
            group_id.to_bytes().to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;

        let snapshot = routing_snapshot(&context, group_id, "routed")
            .await
            .unwrap();
        assert_eq!(snapshot.bucket_rules, vec![rule.clone()]);
        assert_eq!(snapshot.group_default, record.default_target);

        let known = bucket_snapshot(&context, &info).await.unwrap();
        assert_eq!(known.bucket_rules, vec![rule]);
        assert_eq!(known.group_default, record.default_target);

        // An unwritten group and bucket are normal empty state, never an error.
        let absent = routing_snapshot(&context, ulid::Ulid::generate(), "missing")
            .await
            .unwrap();
        assert!(absent.bucket_rules.is_empty());
        assert_eq!(absent.group_default, None);
    }

    #[tokio::test]
    async fn sums_backend_shards() {
        // Fullness is measured over every shard of one backend, and only that one.
        use crate::driver::backend_used_bytes;
        use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
        use aruna_core::structs::{BackendRef, UsageCounters, usage_backend_key};

        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let counters = |bytes| UsageCounters {
            stored_bytes: bytes,
            ..Default::default()
        };
        for (backend, shard, bytes) in [
            (BackendRef::node_default(), 0, 10u64),
            (BackendRef::node_default(), 5, 7),
            (BackendRef::Node("cold".to_string()), 0, 100),
        ] {
            write_value(
                &context,
                USAGE_STATS_KEYSPACE,
                usage_backend_key(&backend, shard),
                counters(bytes).to_bytes().unwrap(),
            )
            .await;
        }

        assert_eq!(
            backend_used_bytes(&context, &BackendRef::node_default())
                .await
                .unwrap(),
            17
        );
        assert_eq!(
            backend_used_bytes(&context, &BackendRef::Node("gone".to_string()))
                .await
                .unwrap(),
            0
        );

        // One undecodable shard must fail the read, not read as zero usage.
        write_value(
            &context,
            USAGE_STATS_KEYSPACE,
            usage_backend_key(&BackendRef::node_default(), 1),
            vec![0xff; 8],
        )
        .await;
        assert!(
            backend_used_bytes(&context, &BackendRef::node_default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_fails_corrupt() {
        // A bucket record that will not decode must fail the write instead of
        // routing it to the node default.
        use crate::driver::routing_snapshot;
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;

        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        write_value(
            &context,
            S3_BUCKET_KEYSPACE,
            b"corrupt".to_vec(),
            vec![0xff; 8],
        )
        .await;

        let result = routing_snapshot(&context, ulid::Ulid::generate(), "corrupt").await;

        assert!(matches!(
            result,
            Err(crate::driver::RoutingInputsError::BucketRules(_))
        ));
    }

    async fn write_value(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[derive(Debug, PartialEq)]
    pub struct TestOperation {
        pub state: u8,
        pub txn_id: Option<aruna_core::types::TxnId>,
    }

    impl TestOperation {
        pub fn new() -> Self {
            TestOperation {
                state: 0,
                txn_id: None,
            }
        }
    }

    impl Operation for TestOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;

            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
            match (events, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.state = 2;
                    self.txn_id = Some(txn_id);
                    eprintln!("Transaction started with id {:?}", txn_id);
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        value: ByteView::from(*b"value1"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { key: _ }), 2) => {
                    self.state = 3;
                    eprintln!("Write completed, committing transaction.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.unwrap(),
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { txn_id: _ }), 3) => {
                    self.state = 4;
                    eprintln!("Transaction committed, reading back value.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        txn_id: None,
                    })]
                }
                (Event::Storage(StorageEvent::ReadResult { key, value }), 4) => {
                    self.state = 5;

                    eprintln!("Read key: {:?}, value: {:?}", key, value);
                    assert_eq!(key, ByteView::from(*b"key1"));
                    assert_eq!(value, Some(ByteView::from(*b"value1")));
                    self.state = 6;
                    smallvec::smallvec![]
                }

                a => {
                    eprintln!("Unexpected event/state combination {:?}", a);
                    smallvec::smallvec![]
                }
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 6
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[derive(Debug, PartialEq)]
    struct MarkerAbortOperation {
        state: u8,
        fail: bool,
    }

    impl Operation for MarkerAbortOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { .. }), 1) if self.fail => {
                    self.state = 3;
                    smallvec::smallvec![]
                }
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id,
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { .. }), 2) => {
                    self.state = 3;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 3
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            if self.fail { Err(()) } else { Ok(()) }
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"abort-marker"),
                value: ByteView::from(*b"ran"),
                txn_id: None,
            })]
        }
    }

    async fn marker_absent(context: &DriverContext) -> bool {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"abort-marker"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };
        value.is_none()
    }

    async fn transaction_reopens(context: &DriverContext) -> bool {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            return false;
        };
        matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted })
                if aborted == txn_id
        )
    }

    #[tokio::test]
    async fn drive_commit_safe() {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            MarkerAbortOperation {
                state: 0,
                fail: false,
            },
            &context,
        )
        .await;

        assert!(result.is_ok());
        assert!(marker_absent(&context).await);
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test]
    async fn drive_error_cleanup() {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            MarkerAbortOperation {
                state: 0,
                fail: true,
            },
            &context,
        )
        .await;

        assert!(result.is_err());
        assert!(marker_absent(&context).await);
        assert!(transaction_reopens(&context).await);
    }

    /// Never finishes on its own, so only the deadline can end it. The step cap
    /// keeps a failing test from spinning instead of hanging.
    #[derive(Debug, PartialEq)]
    struct StallingOperation {
        aborted: bool,
        steps: usize,
    }

    impl Operation for StallingOperation {
        type Output = bool;
        type Error = Infallible;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"stall"),
                txn_id: None,
            })]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.steps += 1;
            if self.is_complete() {
                return smallvec::smallvec![];
            }
            self.start()
        }

        fn is_complete(&self) -> bool {
            self.aborted || self.steps > 1_000
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.aborted)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.aborted = true;
            smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"cleanup"),
                value: ByteView::from(*b"done"),
                txn_id: None,
            })]
        }
    }

    #[tokio::test]
    async fn deadline_runs_abort() {
        // Racing a timeout against the whole drive would drop the abort path.
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let aborted = crate::driver::drive_until(
            StallingOperation {
                aborted: false,
                steps: 0,
            },
            &context,
            tokio::time::Instant::now(),
        )
        .await
        .unwrap();

        assert!(aborted, "the deadline must end the operation");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"cleanup"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event")
        };
        assert_eq!(value, Some(ByteView::from(*b"done")));
    }

    #[tokio::test]
    pub async fn test_driver() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let operation = TestOperation::new();
        let result = drive(operation, &context).await;
        assert!(result.is_ok());
    }

    #[derive(Debug, PartialEq)]
    struct EffectOrderOperation {
        observed: Vec<&'static str>,
    }

    impl EffectOrderOperation {
        fn new() -> Self {
            Self {
                observed: Vec::new(),
            }
        }
    }

    impl Operation for EffectOrderOperation {
        type Output = Vec<&'static str>;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![
                Effect::Task(TaskEffect::CancelTimer {
                    key: TaskKey::RealmPresence {
                        realm_id: aruna_core::structs::RealmId::from_bytes([0u8; 32]),
                        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                    },
                }),
                Effect::Search()
            ]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match event {
                Event::Task(_) => self.observed.push("task"),
                Event::Search() => self.observed.push("search"),
                _ => {}
            }
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.len() == 2
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_driver_preserves_effect_order_fifo() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let operation = EffectOrderOperation::new();
        let observed = drive(operation, &context)
            .await
            .expect("drive should succeed");
        assert_eq!(observed, vec!["task", "search"]);
    }

    #[derive(Debug, PartialEq)]
    struct StagingSourceDispatchOperation {
        observed_staging_source: bool,
    }

    impl StagingSourceDispatchOperation {
        fn new() -> Self {
            Self {
                observed_staging_source: false,
            }
        }
    }

    impl Operation for StagingSourceDispatchOperation {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::StagingSource(StagingSourceEffect::Head {
                access: ResolvedSourceAccess::OpenDal {
                    kind: SourceConnectorKind::Http,
                    config: std::collections::HashMap::from([(
                        "endpoint".to_string(),
                        "https://missing.example.org".to_string(),
                    )]),
                    path: "file.txt".to_string(),
                    version: None,
                },
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed_staging_source = matches!(
                event,
                Event::StagingSource(StagingSourceEvent::Error { .. })
            );
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed_staging_source
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed_staging_source)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_driver_dispatches_staging_source_effect_via_blob_handle() {
        let temp_dir = tempdir().unwrap();
        let temp_root = temp_dir.path().to_str().unwrap().to_string();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
        let net_handle =
            aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage_handle.clone())
                .await
                .unwrap();
        let blob_handle = aruna_blob::blob::BlobHandler::new(
            aruna_core::structs::BackendConfig {
                backend_type: aruna_core::structs::Backend::FileSystem,
                root: blob_root,
                service_config: std::collections::HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let observed = drive(StagingSourceDispatchOperation::new(), &context)
            .await
            .expect("staging source effect should be dispatched");
        assert!(observed);
    }

    #[derive(Debug, PartialEq)]
    struct RecursiveSubOperation {
        observed: Option<Event>,
    }

    impl RecursiveSubOperation {
        fn new() -> Self {
            Self { observed: None }
        }
    }

    impl Operation for RecursiveSubOperation {
        type Output = Event;
        type Error = Infallible;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::SubOperation(boxed_suboperation(
                RecursiveSubOperation::new(),
                |result| match result {
                    Ok(event) => event,
                    Err(never) => match never {},
                },
            ))]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed = Some(event);
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self
                .observed
                .expect("recursive suboperation should produce an event"))
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn test_suboperation_depth_limit_is_enforced() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let event = drive(RecursiveSubOperation::new(), &context)
            .await
            .expect("recursive suboperation should resolve to depth-limit event");

        assert!(matches!(
            event,
            Event::SubOperation(SubOperationEvent::DepthLimitExceeded { max_depth })
                if max_depth == super::MAX_SUBOP_DEPTH
        ));
    }
}

#[cfg(test)]
mod routing_tests {
    use super::{DriverContext, bucket_snapshot, routing_snapshot};
    use crate::staging::test_utils::setup_driver_context;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::GROUP_STORAGE_ROUTING_KEYSPACE;
    use aruna_core::structs::{
        BackendRef, BucketInfo, GroupBackendKind, GroupStorageBackend, GroupStorageRouting,
        ResolvedBackend, RoutingTarget, resolve_backend,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    async fn write(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn register(context: &DriverContext, group_id: Ulid) -> Ulid {
        let record = GroupStorageBackend {
            backend_id: Ulid::generate(),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        };
        for (key_space, key, value) in crate::group_backends::record_writes(&record).unwrap() {
            write(context, &key_space, key.to_vec(), value.to_vec()).await;
        }
        record.backend_id
    }

    async fn set_default(context: &DriverContext, group_id: Ulid, backend_id: Ulid) {
        let record = GroupStorageRouting {
            group_id,
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: Default::default(),
        };
        write(
            context,
            GROUP_STORAGE_ROUTING_KEYSPACE,
            group_id.to_bytes().to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
    }

    fn bucket(group_id: Ulid) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        }
    }

    #[tokio::test]
    async fn routes_group_backend() {
        // The catalog is built the way production builds it, so a group default
        // naming a registered backend has to resolve rather than fail.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let backend_id = register(&test.driver_context, group_id).await;
        set_default(&test.driver_context, group_id, backend_id).await;

        let snapshot = routing_snapshot(&test.driver_context, group_id, "b")
            .await
            .unwrap();

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Group(backend_id), None)
        );
    }

    #[tokio::test]
    async fn scopes_catalog() {
        // Another group's backend must never enter this group's catalog.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let foreign = register(&test.driver_context, Ulid::generate()).await;
        set_default(&test.driver_context, group_id, foreign).await;

        let snapshot = routing_snapshot(&test.driver_context, group_id, "b")
            .await
            .unwrap();

        assert!(resolve_backend(&snapshot, "b", "k").is_err());
    }

    #[tokio::test]
    async fn snapshot_loads_group() {
        // A caller holding the bucket record still has to pick up the group's
        // default target and backend ids.
        let test = setup_driver_context().await;
        let group_id = Ulid::generate();
        let backend_id = register(&test.driver_context, group_id).await;
        set_default(&test.driver_context, group_id, backend_id).await;

        let snapshot = bucket_snapshot(&test.driver_context, &bucket(group_id))
            .await
            .unwrap();

        assert_eq!(
            resolve_backend(&snapshot, "b", "k").unwrap(),
            ResolvedBackend::new(BackendRef::Group(backend_id), None)
        );
    }
}
