//! Builds storage driver contexts, raw storage reads and writes, and signed tokens for tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::{DocumentOutboxRecord, DocumentReconcileResult};
use aruna_core::document_apply::ApplyError;
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::auth::TokenClaims;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use crate::metadata::deletion_guard::DeletionGuardOperation;
use crate::sync::document_outbox::{
    OutboxPagePlan, OutboxReadBatch, OutboxTailsOperation, OutboxTimerOperation, ReadOutboxPlan,
};
use crate::sync::reconcile::SyncTopicsOperation;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_drivers::testing::test_context;

/// A storage driver context with a task plane, shared behind an `Arc`.
pub fn task_context(storage: StorageHandle) -> std::sync::Arc<DriverContext> {
    std::sync::Arc::new(DriverContext {
        task_handle: Some(TaskHandle::new()),
        ..test_context(storage)
    })
}

/// A storage-only driver context on a store opened at `root`.
pub fn path_context(root: &str) -> DriverContext {
    test_context(aruna_storage::FjallStorage::open(root).expect("open the test store"))
}

/// A storage-only driver context on a fresh temporary store.
pub fn temp_context() -> (tempfile::TempDir, DriverContext) {
    let dir = tempfile::tempdir().expect("temp dir");
    let context = path_context(dir.path().to_str().expect("temp path"));
    (dir, context)
}

/// Writes one value outside a transaction; panics on any other answer.
pub async fn write_value(
    storage: &StorageHandle,
    key_space: &str,
    key: impl Into<ByteView>,
    value: impl Into<ByteView>,
) {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(write).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// Writes `writes` in one batch outside a transaction; panics on any other answer.
pub async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// The stored value of `key`; panics on any answer but a read result.
pub async fn read_value(storage: &StorageHandle, keyspace: &str, key: Vec<u8>) -> Option<ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read event")
    };
    value
}

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

/// Arms the outbox drain on `tasks` as a restarting node does.
pub async fn restore_outbox_timers(storage: &StorageHandle, tasks: &TaskHandle) {
    let mut context = test_context(storage.clone());
    context.task_handle = Some(tasks.clone());
    drive(OutboxTimerOperation::new(), &context)
        .await
        .expect("outbox timer restores");
}

fn context_realm(context: &DriverContext) -> RealmId {
    *context
        .net_handle
        .as_ref()
        .expect("the context has a net handle")
        .realm_id()
}

/// Pulls `topics` from `peers` and reconciles them.
pub async fn sync_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::new(context_realm(context), topics, peers);
    drive(sync, context).await
}

/// Pulls one topic from `peers` and reconciles it; a topic that cannot be fetched fails.
pub async fn sync_topic(
    context: &DriverContext,
    topic: irokle::TopicId,
    peers: Vec<NodeId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let sync = SyncTopicsOperation::single(context_realm(context), topic, peers);
    drive(sync, context).await
}

/// Reconciles what `topics` already hold.
pub async fn reconcile_topics(
    context: &DriverContext,
    topics: Vec<irokle::TopicId>,
) -> Result<DocumentReconcileResult, ApplyError> {
    let reconcile = ReconcileTopicsOperation::new(context_realm(context), topics);
    drive(reconcile, context).await
}

/// An EdDSA bearer token of `claims` signed with `signing_key`.
pub fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

/// Replays the whole event log into the registry projection.
pub async fn replay_event_log(
    context: &DriverContext,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ReplayPageOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut after = None;
    let mut projected = 0usize;
    loop {
        let page = ReplayPageOperation::new(after, local_node, max_clock_skew());
        let (count, next) = aruna_drivers::driver::drive(page, context).await?;
        projected = projected.saturating_add(count);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(projected),
        }
    }
}

/// Runs one pass of the pending projection drain.
pub async fn drain_projection_queue(
    context: &DriverContext,
) -> Result<
    crate::metadata::projection::PendingProjectionResult,
    crate::metadata::projection::MetadataProjectionError,
> {
    use crate::metadata::projection::{DrainProjectionOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let drain = DrainProjectionOperation::new(local_node, max_clock_skew());
    aruna_drivers::driver::drive(drain, context).await
}

/// Projects carried events.
pub async fn project_create_events(
    context: &DriverContext,
    events: Vec<aruna_core::metadata::MetadataEventRecord>,
    local_node_id: Option<aruna_core::NodeId>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectEventsOperation, max_clock_skew};
    let project = ProjectEventsOperation::new(events, local_node_id, max_clock_skew());
    aruna_drivers::driver::drive(project, context).await
}

/// Projects logged events by their document and event ids.
pub async fn project_logged_events(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (ulid::Ulid, ulid::Ulid)>,
) -> Result<usize, crate::metadata::projection::MetadataProjectionError> {
    use crate::metadata::projection::{ProjectLoggedOperation, max_clock_skew};
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let project = ProjectLoggedOperation::new(targets, false, local_node, max_clock_skew());
    Ok(aruna_drivers::driver::drive(project, context)
        .await?
        .projected)
}

/// Projects one logged event.
pub async fn project_logged_event(
    context: &DriverContext,
    document_id: ulid::Ulid,
    event_id: ulid::Ulid,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    project_logged_events(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

/// Wakes the projection drain after `after`.
pub async fn schedule_projection_drain(
    context: &DriverContext,
    after: std::time::Duration,
) -> Result<(), crate::metadata::projection::MetadataProjectionError> {
    let wake = crate::metadata::projection::ProjectionWakeOperation::new(after);
    aruna_drivers::driver::drive(wake, context).await
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

pub struct RecordingTaskHandler {
    pub seen: tokio::sync::mpsc::Sender<aruna_core::task::TaskKey>,
}

#[async_trait::async_trait]
impl aruna_tasks::InboundTaskHandler for RecordingTaskHandler {
    async fn handle_timer(&self, key: aruna_core::task::TaskKey) {
        let _ = self.seen.send(key).await;
    }
}

/// The graph handle a test installed on its context.
pub fn installed_graph_handle(context: &DriverContext) -> &aruna_graph::GraphHandle {
    context
        .graph_handle
        .as_ref()
        .expect("graph handle installed")
}

/// Runs one metadata effect on this node's graph store and answers with its event; an effect
/// that names a graph skips the deletion guard here.
pub async fn send_metadata_effect(context: &DriverContext, effect: MetadataEffect) -> Event {
    match &context.graph_handle {
        Some(graph) => Event::Metadata(graph.run(effect).await),
        None => Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }),
    }
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

/// The stored user `user_id`; panics when the read answers anything else.
pub async fn read_user(
    context: &DriverContext,
    user_id: aruna_core::UserId,
) -> aruna_core::structs::identity::user::User {
    let key = user_id.to_bytes().to_vec();
    let value = read_value(
        &context.storage_handle,
        aruna_core::keyspaces::USER_KEYSPACE,
        key,
    )
    .await
    .expect("the user is stored");
    aruna_core::structs::identity::user::User::from_bytes(&value).unwrap()
}

/// Claims of `user_id` issued by `realm_id`, valid for ten minutes from now.
pub fn token_claims(realm_id: RealmId, user_id: aruna_core::UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: ulid::Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
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
