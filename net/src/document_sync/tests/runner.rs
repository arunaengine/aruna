//! Runs the document apply operations of core against a real service, as the runner does.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use aruna_core::document::{DocumentEffect, DocumentReconcileResult};
use aruna_core::document_apply::Step;
use aruna_core::document_apply::admin::{AdminFollowUp, AdminOperationPlan};
use aruna_core::document_apply::distribution::{DistributionPlan, Handled, RetryParkedPlan};
use aruna_core::document_apply::events::{PolicyChangePlan, WatchChangePlan};
use aruna_core::document_apply::overflow::{ReplayOverflowOperation, overflow_key};
use aruna_core::document_apply::reads::{
    dependency_available as dependency_available_in, reducer_state,
};
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::document_apply::registry::{
    DeleteRegistryPlan, DocumentLifecyclePlan, GraphLifecyclePlan, RegistryUpsertPlan,
};
use aruna_core::document_apply::validate::{ConfigSnapshot, ValidateAdminPlan};
use aruna_core::document_apply::validate::{validate_group_authority, validate_user_authority};
use aruna_core::document_apply::{ApplyError, PlacementOutcome};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::NetEvent;
use aruna_core::operation::{
    ByCalls, EffectOperation, Operation, Runnable, SubOperation, TransactionMode,
};
use aruna_core::row_plan::{PlanOperation, PlanRows, Planned, RowPlan};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Effects;
use aruna_core::types::TxnId;

use super::*;

impl From<ApplyError> for NetError {
    fn from(error: ApplyError) -> Self {
        match error {
            ApplyError::Invalid(reason) => Self::Bootstrap(reason),
            ApplyError::Deferred(reason) => Self::Deferred(reason),
            ApplyError::Storage(error) => Self::Storage(error),
            ApplyError::Failed(reason) => Self::Dht(reason),
        }
    }
}

/// What effects reach: storage always, the service when the operation reads topics.
#[derive(Clone, Copy)]
struct Backends<'a> {
    storage: &'a StorageHandle,
    service: Option<&'a DocumentSyncService>,
}

impl<'a> Backends<'a> {
    fn dispatch(self, effect: Effect) -> Pin<Box<dyn Future<Output = Event> + Send + 'a>> {
        Box::pin(async move {
            match effect {
                Effect::Storage(effect) => self.storage.send_storage_effect(effect).await,
                Effect::SubOperation(child) => self.run_child(child).await,
                // A retry backoff needs no real wait here.
                Effect::Delay(_) => {
                    tokio::task::yield_now().await;
                    Event::Delayed
                }
                Effect::Net(NetEffect::DocumentSync(effect)) => {
                    let service = self.service.expect("the operation needs the service");
                    Event::Net(NetEvent::DocumentSync(match effect {
                        DocumentEffect::ReconcileBatch {
                            topic,
                            stored_cursor,
                        } => service.reconcile_batch_event(topic, stored_cursor),
                        DocumentEffect::OverflowPage {
                            topic,
                            start,
                            applied,
                        } => service.overflow_page_event(topic, start, applied),
                        other => panic!("unexpected document effect: {other:?}"),
                    }))
                }
                other => panic!("unexpected effect: {other:?}"),
            }
        })
    }

    async fn run_child(self, mut child: Box<dyn SubOperation>) -> Event {
        child.stamp(unix_timestamp_millis());
        let mut effects: VecDeque<Effect> = child.start().into_iter().collect();
        while let Some(effect) = effects.pop_front() {
            let event = self.dispatch(effect).await;
            if !child.is_complete() {
                child.stamp(unix_timestamp_millis());
                let next: Effects = child.step(event);
                effects.extend(next);
            }
        }
        child.finalize()
    }

    async fn run<O>(self, operation: O) -> Result<O::Output>
    where
        O: Operation<Error = ApplyError> + 'static,
    {
        let mut machine = Runnable::<ByCalls>::into_machine(operation);
        machine.stamp(unix_timestamp_millis());
        let mut effects: VecDeque<Effect> = machine.start().into_iter().collect();
        while let Some(effect) = effects.pop_front() {
            let event = self.dispatch(effect).await;
            if !machine.is_complete() {
                machine.stamp(unix_timestamp_millis());
                effects.extend(machine.step(event));
            }
        }
        machine.finalize().map_err(NetError::from)
    }
}

/// Runs `plan` once in its transaction.
pub(crate) async fn run_plan<P>(storage: &StorageHandle, plan: P) -> Result<P::Output>
where
    P: RowPlan<Error = ApplyError> + 'static,
    P::Output: PartialEq,
{
    let backends = Backends {
        storage,
        service: None,
    };
    backends.run(PlanOperation::new(plan)).await
}

/// Runs `plan` and lets `during` finish right before its first change reaches storage.
pub(crate) async fn run_plan_around<P>(
    storage: &StorageHandle,
    plan: P,
    during: impl Future<Output = ()>,
) -> Result<P::Output>
where
    P: RowPlan<Error = ApplyError> + 'static,
    P::Output: PartialEq,
{
    let mut during = Some(Box::pin(during));
    let mut machine = Runnable::<ByCalls>::into_machine(PlanOperation::new(plan));
    machine.stamp(unix_timestamp_millis());
    let mut effects: VecDeque<Effect> = machine.start().into_iter().collect();
    while let Some(effect) = effects.pop_front() {
        let Effect::Storage(effect) = effect else {
            panic!("a row plan asked for other I/O: {effect:?}");
        };
        if matches!(
            effect,
            StorageEffect::BatchWrite { .. } | StorageEffect::BatchDelete { .. }
        ) && let Some(during) = during.take()
        {
            during.await;
        }
        let event = storage.send_storage_effect(effect).await;
        if !machine.is_complete() {
            effects.extend(machine.step(event));
        }
    }
    machine.finalize().map_err(NetError::from)
}

pub(crate) async fn start_storage_transaction(storage: &StorageHandle) -> Result<TxnId> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        other => panic!("unexpected transaction start: {other:?}"),
    }
}

pub(crate) async fn transaction_read(
    storage: &StorageHandle,
    key_space: String,
    key: ByteView,
    txn_id: Option<TxnId>,
) -> Result<Option<Value>> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space,
            key,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => panic!("unexpected read answer: {other:?}"),
    }
}

/// Writes `writes` in the transaction and commits it.
pub(crate) async fn replace_batch_in(
    storage: &StorageHandle,
    txn_id: TxnId,
    deletes: Vec<(String, ByteView)>,
    writes: Vec<(String, ByteView, Value)>,
) -> Result<()> {
    assert!(deletes.is_empty(), "tests only write here");
    let wrote = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await;
    assert!(matches!(
        wrote,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => panic!("unexpected commit answer: {other:?}"),
    }
}

pub(crate) async fn batch_delete_to(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<()> {
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => panic!("unexpected delete answer: {other:?}"),
    }
}

/// Decides one read over stored rows for a test.
#[derive(Debug, PartialEq)]
enum CheckPlan {
    Group(AdminDocumentEvent, Option<AdminDocumentState>),
    User(AdminDocumentEvent, Option<AdminDocumentState>),
}

impl RowPlan for CheckPlan {
    type Output = AdminEventValidation;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<AdminEventValidation>> {
        let validation = match self {
            Self::Group(event, state) => validate_group_authority(rows, event, state.as_ref())?,
            Self::User(event, state) => validate_user_authority(rows, event, state.as_ref())?,
        };
        Ok(Planned::output(validation))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

pub(crate) async fn check_group_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    state: Option<&AdminDocumentState>,
) -> Result<AdminEventValidation> {
    run_plan(storage, CheckPlan::Group(event.clone(), state.cloned())).await
}

pub(crate) async fn check_user_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    state: Option<&AdminDocumentState>,
) -> Result<AdminEventValidation> {
    run_plan(storage, CheckPlan::User(event.clone(), state.cloned())).await
}

#[derive(Debug, PartialEq)]
struct StatePlan(AdminDocumentTarget);

impl RowPlan for StatePlan {
    type Output = Option<AdminDocumentState>;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<Self::Output>> {
        Ok(Planned::output(reducer_state(rows, &self.0)?))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

pub(crate) async fn read_reducer_state(
    storage: &StorageHandle,
    target: &AdminDocumentTarget,
) -> Result<Option<AdminDocumentState>> {
    run_plan(storage, StatePlan(target.clone())).await
}

#[derive(Debug, PartialEq)]
struct DependencyPlan(DocumentSyncDependency);

impl RowPlan for DependencyPlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        Ok(Planned::output(dependency_available_in(rows, self.0)?))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

pub(crate) async fn dependency_available(
    storage: &StorageHandle,
    dependency: DocumentSyncDependency,
) -> Result<bool> {
    run_plan(storage, DependencyPlan(dependency)).await
}

/// Runs the plan `build` makes, again after each lost transaction conflict it allows.
async fn retry_plan<P>(storage: &StorageHandle, build: impl Fn() -> P) -> Result<P::Output>
where
    P: RowPlan<Error = ApplyError> + 'static,
    P::Output: PartialEq,
{
    for _ in 0..=P::conflict_retries() {
        tokio::task::yield_now().await;
        match run_plan(storage, build()).await {
            Err(NetError::Storage(StorageError::TransactionConflict)) => {}
            result => return result,
        }
    }
    Err(NetError::Dht("conflict retries exhausted".to_string()))
}

pub(crate) fn target_write_entry(
    target: DocumentTarget,
    value: Value,
) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

pub(crate) async fn apply_admin_operation(
    storage: &StorageHandle,
    target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    if matches!(event.target, AdminDocumentTarget::Distribution { .. })
        && matches!(target, DocumentTarget::RealmConfig { .. })
    {
        return apply_distribution_operation(storage, target, event)
            .await
            .map(|_| ());
    }
    let build = || AdminOperationPlan {
        target: target.clone(),
        event: event.clone(),
    };
    let AdminFollowUp { sync, retry_parked } = retry_plan(storage, build).await?;
    if sync {
        let finished = storage.send_storage_effect(StorageEffect::SyncAll).await;
        assert_eq!(finished, Event::Storage(StorageEvent::SyncAllFinished));
    }
    if let Some(realm_id) = retry_parked {
        retry_plan(storage, || RetryParkedPlan { realm_id }).await?;
    }
    Ok(())
}

pub(crate) use apply_admin_operation as apply_user_operation;

pub(crate) async fn apply_distribution_operation(
    storage: &StorageHandle,
    target: DocumentTarget,
    event: AdminDocumentEvent,
) -> Result<Handled> {
    let build = || DistributionPlan {
        target: target.clone(),
        event: event.clone(),
    };
    retry_plan(storage, build).await
}

/// The realm config snapshot shared by the admin events of one batch.
#[derive(Default)]
pub(crate) struct ConfigValidationCache {
    snapshot: Option<Arc<ConfigSnapshot>>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn validate_admin_event(
    storage: &StorageHandle,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    signature: &iroh::Signature,
    cache: &mut ConfigValidationCache,
) -> Result<AdminEventValidation> {
    let plan = ValidateAdminPlan {
        topic_id,
        actor_id,
        target: target.clone(),
        event: event.clone(),
        realm_id,
        placement: *placement,
        signature: *signature,
        cache: cache.snapshot.clone(),
    };
    let (validation, snapshot) = run_plan(storage, plan).await?;
    cache.snapshot = snapshot;
    Ok(validation)
}

pub(crate) async fn store_registry_upsert(
    storage: &StorageHandle,
    record: MetadataRegistryRecord,
    primary_bytes: Vec<u8>,
) -> Result<PlacementOutcome<()>> {
    let build = || RegistryUpsertPlan {
        record: record.clone(),
        primary_bytes: primary_bytes.clone(),
    };
    retry_plan(storage, build).await
}

pub(crate) async fn store_graph_lifecycle(
    storage: &StorageHandle,
    record: &GraphLifecycleRecord,
    primary_bytes: Vec<u8>,
) -> Result<bool> {
    let build = || GraphLifecyclePlan {
        record: record.clone(),
        primary_bytes: primary_bytes.clone(),
    };
    retry_plan(storage, build).await
}

pub(crate) async fn store_document_lifecycle(
    storage: &StorageHandle,
    record: &MetadataLifecycleRecord,
    change: DocumentChange,
) -> Result<bool> {
    let build = || DocumentLifecyclePlan {
        record: record.clone(),
        change,
    };
    retry_plan(storage, build).await
}

pub(crate) async fn delete_registry_record(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<()> {
    let build = || DeleteRegistryPlan {
        group_id,
        document_id,
    };
    retry_plan(storage, build).await
}

pub(crate) async fn store_watch_change(
    storage: &StorageHandle,
    target: DocumentTarget,
    bytes: Option<Vec<u8>>,
    change: DocumentChange,
) -> Result<bool> {
    let build = || WatchChangePlan {
        target: target.clone(),
        bytes: bytes.clone(),
        change,
    };
    retry_plan(storage, build).await
}

impl DocumentSyncService {
    pub(crate) fn document_topic_ids(&self) -> Result<Vec<::irokle::TopicId>> {
        let topics = self
            .node
            .list_topics()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(topics
            .into_iter()
            .filter(|topic| topic.event_type_id == DocumentEvent::TYPE_ID)
            .map(|topic| topic.topic_id)
            .collect())
    }

    pub(crate) fn document_events_after(
        &self,
        topic_id: ::irokle::TopicId,
        cursor: &::irokle::ActorClock,
    ) -> Result<Vec<(DocumentEvent, ::irokle::ActorId, u64)>> {
        Ok(self
            .document_event_batch(topic_id, cursor, FRAME_LEN_LIMIT)?
            .events)
    }

    /// One reconcile pass, holding the reconcile lock as the runner does.
    pub(crate) async fn reconcile_document_topics(
        &self,
        topics: impl IntoIterator<Item = ::irokle::TopicId>,
    ) -> Result<DocumentReconcileResult> {
        let _guard = self.hold_reconcile().await;
        let operation = ReconcileTopicsOperation::new(self.realm_id, topics.into_iter().collect());
        let backends = Backends {
            storage: &self.storage,
            service: Some(self),
        };
        backends.run(operation).await
    }

    /// Remembers where an overflowed batch started; an earlier checkpoint is kept.
    pub(crate) async fn record_overflow(
        &self,
        topic_id: ::irokle::TopicId,
        genesis: ::irokle::OpId,
        start: &::irokle::ActorClock,
    ) -> Result<()> {
        let key = overflow_key(topic_id);
        if self
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), key.clone())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let value = applied_cursor_value(self.node.storage(), topic_id, genesis, start)?;
        self.storage_write(APPLIED_OPS_KEYSPACE.to_string(), key, value)
            .await
    }

    pub(crate) async fn replay_overflow(&self) -> Result<()> {
        let backends = Backends {
            storage: &self.storage,
            service: Some(self),
        };
        backends
            .run(ReplayOverflowOperation::new(self.realm_id))
            .await
    }

    pub(crate) async fn storage_write(
        &self,
        key_space: String,
        key: ByteView,
        value: Value,
    ) -> Result<()> {
        batch_write_to(&self.storage, vec![(key_space, key, value)]).await
    }

    pub(crate) fn local_node_id(&self) -> Result<NodeId> {
        NodeId::from_bytes(self.node.peer_id().as_bytes())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    /// Admits one peer and adds it to the fan-out set, as a realm config would.
    pub(crate) fn add_peer_candidate(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_to_peer(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.node
            .add_peer_to_whitelist(peer_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.flush_database()?;
        self.default_peers.write().insert(peer_id);
        Ok(())
    }

    /// Signs one op carrying `payload` under the document-sync event type
    /// without encoding an event, so tests can deliver a payload no peer can
    /// decode. Returns the op's transport identity.
    pub(crate) fn publish_raw_event(
        &self,
        topic_id: ::irokle::TopicId,
        payload: Vec<u8>,
    ) -> Result<SyncQuarantineIdentity> {
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
        let envelope = EventEnvelope {
            type_id: DocumentEvent::TYPE_ID.to_string(),
            payload: payload.into(),
        };
        let op = self.publish_event_op(
            &oplog,
            topic_id,
            actor_id,
            envelope,
            &BTreeSet::new(),
            true,
            true,
            &mut 0,
            &mut 0,
        )?;
        self.flush_database()?;
        Ok(SyncQuarantineIdentity {
            topic: topic_id,
            actor: op.signed.body.actor_id,
            actor_seq: op.signed.body.actor_seq,
        })
    }

    pub(crate) async fn apply_policy_document(
        &self,
        document: &PlacementPolicyDocument,
        placement: PlacementRef,
    ) -> Result<PlacementOutcome<bool>> {
        let realm_id = self.realm_id;
        let build = || PolicyChangePlan {
            realm_id,
            document: document.clone(),
            placement,
        };
        retry_plan(&self.storage, build).await
    }
}
