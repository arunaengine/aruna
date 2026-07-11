use aruna_core::NodeId;
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
    DocumentSyncTarget, PendingDocumentPlacement,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    DOCUMENT_SYNC_REVISION_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE, SYNC_PLACEMENT_KEYSPACE,
};
use aruna_core::metadata::MetadataDocumentLifecycleRecord;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::storage_entries::{
    document_placement_write_entry, document_sync_revision_key, document_sync_revision_write_entry,
    metadata_document_key, metadata_document_lifecycle_revision_change,
    metadata_document_lifecycle_write_entry, metadata_registry_write_entries,
};
use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::read_effect;
use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::{PlacementResolutionContext, plan_target_placement};
use crate::sync_placement::{
    decode_placement, delete_placement_effect, new_placement_with_context, placement_prefix,
    placement_satisfied, schedule_placement_retry_after, sort_node_ids, write_placement_effect,
};
use std::time::Duration;
use tracing::warn;

const PENDING_PLACEMENT_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct PlacementConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    /// In-memory backoff duration the driver uses for the retry re-arm; not
    /// persisted anywhere.
    pub retry_after: Duration,
}

#[derive(Debug, PartialEq)]
pub struct ProcessPlacementsOperation {
    config: PlacementConfig,
    state: PlacementState,
    realm_config: Option<RealmConfigDocument>,
    records: Vec<PendingDocumentPlacement>,
    next_start_after: Option<Key>,
    current: Option<CurrentPlacement>,
    txn_id: Option<TxnId>,
    retry_needed: bool,
    metadata_refresh_queued: bool,
    rearmed: bool,
    output: Option<Result<(), PlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementState {
    Init,
    LoadRealmConfig,
    ListPending,
    Publish,
    StorePlacement,
    StartMetadataTransaction,
    ReadMetadataState,
    WriteMetadataState,
    CommitMetadataTransaction,
    ScheduleMetadataRefresh,
    ScheduleRetry,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
struct CurrentPlacement {
    planned: PendingDocumentPlacement,
    selected_holders_after_failure: Vec<NodeId>,
    newly_required_remote_holders: Vec<NodeId>,
    metadata_holders_changed: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum PlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("pending placement decode failed: {0}")]
    Decode(String),
    #[error("realm config document not found")]
    RealmConfigNotFound,
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("placement persistence failed: {0}")]
    Placement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl ProcessPlacementsOperation {
    pub fn new(config: PlacementConfig) -> Self {
        Self {
            config,
            state: PlacementState::Init,
            realm_config: None,
            records: Vec::new(),
            next_start_after: None,
            current: None,
            txn_id: None,
            retry_needed: false,
            metadata_refresh_queued: false,
            rearmed: false,
            output: None,
        }
    }

    fn fail(&mut self, error: PlacementError) -> Effects {
        let cleanup = self.abort();
        self.state = PlacementState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(PlacementError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_list_pending(&mut self) -> Effects {
        self.state = PlacementState::ListPending;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
            prefix: Some(placement_prefix(self.config.realm_id)),
            start: self.next_start_after.take().map(IterStart::After),
            limit: PENDING_PLACEMENT_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn emit_next_record(&mut self) -> Effects {
        let Some(record) = self.records.pop() else {
            if self.next_start_after.is_some() {
                return self.emit_list_pending();
            }
            self.state = PlacementState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };
        if record.realm_id != self.config.realm_id {
            warn!(
                record_realm_id = %record.realm_id,
                config_realm_id = %self.config.realm_id,
                "Skipping pending placement for a different realm"
            );
            return self.emit_next_record();
        }
        if record.target.is_admin_document() {
            // Admin documents never take placements; drain any stale row instead of
            // retrying it forever. The satisfied-delete path continues the sweep once
            // the delete result arrives.
            self.current = None;
            self.retry_needed = false;
            self.state = PlacementState::StorePlacement;
            return smallvec![delete_placement_effect(
                self.config.realm_id,
                &record.target
            )];
        }

        let context = PlacementResolutionContext {
            group_id: record.group_id,
            metadata_path: record.metadata_path.as_deref(),
        };
        let mut previously_selected = record.selected_holders.clone();
        sort_node_ids(&mut previously_selected);
        let (desired_holder_count, mut planned_holders, placement) = self
            .realm_config
            .as_ref()
            .and_then(|config| plan_target_placement(config, &record.target, context))
            .map(|plan| (plan.desired_count, plan.holders, plan.placement))
            .unwrap_or((record.desired_holder_count, Vec::new(), PlacementRef::NIL));
        sort_node_ids(&mut planned_holders);
        let is_metadata_lifecycle = matches!(
            &record.target,
            DocumentSyncTarget::MetadataDocumentLifecycle { .. }
        );
        let metadata_holders_changed =
            is_metadata_lifecycle && planned_holders != previously_selected;

        let selected_holders_after_failure: Vec<NodeId> = planned_holders
            .iter()
            .copied()
            .filter(|node_id| {
                previously_selected.contains(node_id)
                    || (*node_id == self.config.local_node_id && !is_metadata_lifecycle)
            })
            .collect();
        let newly_required_holders: Vec<NodeId> = planned_holders
            .iter()
            .copied()
            .filter(|node_id| !previously_selected.contains(node_id))
            .collect();
        let newly_required_remote_holders: Vec<NodeId> = planned_holders
            .iter()
            .copied()
            .filter(|node_id| {
                *node_id != self.config.local_node_id && !previously_selected.contains(node_id)
            })
            .collect();
        let planned = new_placement_with_context(
            self.config.realm_id,
            record.target.clone(),
            record.origin_node_id,
            record.group_id,
            record.metadata_path.clone(),
            desired_holder_count,
            planned_holders,
            placement,
        );
        self.current = Some(CurrentPlacement {
            planned,
            selected_holders_after_failure,
            newly_required_remote_holders: newly_required_remote_holders.clone(),
            metadata_holders_changed,
        });

        if (newly_required_remote_holders.is_empty() && !is_metadata_lifecycle)
            || newly_required_holders.is_empty()
        {
            return self.emit_placement_update(false);
        }

        self.state = PlacementState::Publish;
        // NodeInfo genesis is reserved for the explicit core-document bootstrap;
        // ordinary documents retain origin-derived genesis on placement retry.
        let allow_genesis = !matches!(&record.target, DocumentSyncTarget::NodeInfo { .. })
            && record.origin_node_id == self.config.local_node_id;
        let announce = if is_metadata_lifecycle {
            AnnounceTopicOperation::new_for_document_transfer_with_peers_and_placement(
                record.target.topic_id(),
                self.config.local_node_id,
                record.target,
                newly_required_remote_holders,
                placement,
                allow_genesis,
            )
        } else {
            AnnounceTopicOperation::new_for_document_with_peers_and_placement(
                record.target.topic_id(),
                self.config.local_node_id,
                Some(record.target),
                newly_required_remote_holders,
                placement,
                allow_genesis,
            )
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            announce,
            |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn emit_placement_update(&mut self, publication_failed: bool) -> Effects {
        let Some(mut current) = self.current.take() else {
            return self.emit_next_record();
        };
        if publication_failed {
            current.planned.selected_holders = current.selected_holders_after_failure.clone();
        }

        self.state = PlacementState::StorePlacement;
        self.retry_needed = publication_failed
            || !placement_satisfied(
                current.planned.selected_holders.len(),
                current.planned.desired_holder_count,
            );
        if current.metadata_holders_changed {
            self.current = Some(current);
            self.state = PlacementState::StartMetadataTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })];
        }
        match write_placement_effect(&current.planned) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(PlacementError::Placement(error.to_string())),
        }
    }

    fn emit_metadata_placement_update(
        &mut self,
        lifecycle: Option<aruna_core::types::Value>,
        lifecycle_revision: Option<aruna_core::types::Value>,
        registry: Option<MetadataRegistryRecord>,
    ) -> Effects {
        let Some(current) = self.current.take() else {
            return self.emit_next_record();
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(PlacementError::Placement(
                "missing metadata replan transaction".to_string(),
            ));
        };
        let mut writes = Vec::new();
        if let Some(mut registry) = registry {
            let DocumentSyncTarget::MetadataDocumentLifecycle { document_id } =
                &current.planned.target
            else {
                unreachable!("metadata holder changes only apply to lifecycle placements");
            };
            if registry.document_id != *document_id {
                return self.fail(PlacementError::DocumentSync(format!(
                    "metadata registry document {} does not match lifecycle placement {document_id}",
                    registry.document_id
                )));
            }
            registry.holder_node_ids = current.planned.selected_holders.clone();
            match metadata_registry_write_entries(&registry) {
                Ok(entries) => writes.extend(entries),
                Err(error) => return self.fail(error.into()),
            }

            if let Some(lifecycle) = lifecycle {
                let mut lifecycle: MetadataDocumentLifecycleRecord =
                    match postcard::from_bytes(&lifecycle) {
                        Ok(record) => record,
                        Err(error) => {
                            return self.fail(PlacementError::ConversionError(error.into()));
                        }
                    };
                if let MetadataDocumentLifecycleRecord::Upsert { event } = &mut lifecycle {
                    event.record.holder_node_ids = current.planned.selected_holders.clone();
                    let previous = match lifecycle_revision {
                        Some(value) => match postcard::from_bytes::<DocumentSyncChange>(&value) {
                            Ok(change) => change,
                            Err(error) => {
                                return self.fail(PlacementError::ConversionError(error.into()));
                            }
                        },
                        None => metadata_document_lifecycle_revision_change(
                            &lifecycle,
                            self.config.local_node_id,
                            current.planned.placement,
                        ),
                    };
                    if previous.kind != DocumentSyncChangeKind::Delete {
                        let refresh_id = ulid::Ulid::new();
                        let now = aruna_core::util::unix_timestamp_millis();
                        let change = DocumentSyncChange {
                            base: Some(previous.current),
                            current: DocumentSyncRevision {
                                generation: previous.current.generation.saturating_add(1).max(now),
                                event_id: refresh_id,
                                actor: self.config.local_node_id,
                                updated_at_ms: now,
                            },
                            kind: DocumentSyncChangeKind::Upsert,
                            placement: current.planned.placement,
                        };
                        let bytes = match postcard::to_allocvec(&lifecycle) {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                return self.fail(PlacementError::ConversionError(error.into()));
                            }
                        };
                        match metadata_document_lifecycle_write_entry(&lifecycle) {
                            Ok(entry) => writes.push(entry),
                            Err(error) => return self.fail(error.into()),
                        }
                        match document_sync_revision_write_entry(&current.planned.target, &change) {
                            Ok(entry) => writes.push(entry),
                            Err(error) => return self.fail(error.into()),
                        }
                        let peers: Vec<NodeId> = current
                            .planned
                            .selected_holders
                            .iter()
                            .copied()
                            .filter(|node_id| *node_id != self.config.local_node_id)
                            .collect();
                        if !peers.is_empty() {
                            let outbox = new_outbox_record_with_id(
                                refresh_id,
                                self.config.local_node_id,
                                current.planned.target.clone(),
                                peers,
                                DocumentSyncOutboxEvent::Upsert { bytes, change },
                                current.planned.origin_node_id == self.config.local_node_id,
                            );
                            match outbox_write_entry(&outbox) {
                                Ok(entry) => writes.push(entry),
                                Err(error) => {
                                    return self
                                        .fail(PlacementError::ConversionError(error.into()));
                                }
                            }
                            self.metadata_refresh_queued = true;
                        }
                    }
                }
            }
        }
        match document_placement_write_entry(&current.planned) {
            Ok(entry) => writes.push(entry),
            Err(error) => return self.fail(error.into()),
        }
        self.state = PlacementState::WriteMetadataState;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn emit_metadata_state_read(&mut self, txn_id: TxnId) -> Effects {
        let Some(current) = self.current.as_ref() else {
            return self.emit_next_record();
        };
        let document_id = match &current.planned.target {
            DocumentSyncTarget::MetadataDocumentLifecycle { document_id } => *document_id,
            _ => unreachable!("metadata holder changes only apply to lifecycle placements"),
        };
        let target = current.planned.target.clone();
        self.txn_id = Some(txn_id);
        self.state = PlacementState::ReadMetadataState;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (target.storage_keyspace().to_string(), target.storage_key()),
                (
                    DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
                    document_sync_revision_key(&target),
                ),
                (
                    METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
                    metadata_document_key(document_id),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn retry_metadata_transaction(&mut self) -> Effects {
        self.txn_id = None;
        self.current = None;
        self.metadata_refresh_queued = false;
        self.retry_needed = true;
        self.finish_placement_store()
    }

    fn finish_placement_store(&mut self) -> Effects {
        if self.retry_needed {
            self.rearmed = true;
            self.state = PlacementState::ScheduleRetry;
            smallvec![schedule_placement_retry_after(
                self.config.realm_id,
                self.config.local_node_id,
                self.config.retry_after,
            )]
        } else {
            self.emit_next_record()
        }
    }
}

impl Operation for ProcessPlacementsOperation {
    /// `true` when this tick re-armed the placement retry timer.
    type Output = bool;
    type Error = PlacementError;

    fn start(&mut self) -> Effects {
        self.state = PlacementState::LoadRealmConfig;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.config.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PlacementState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(PlacementError::RealmConfigNotFound);
                    };
                    let config = match RealmConfigDocument::from_bytes(&value) {
                        Ok(config) => config,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.realm_config = Some(config);
                    self.emit_list_pending()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            PlacementState::ListPending => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.next_start_after = next_start_after;
                    self.records.clear();
                    for (_, value) in values.into_iter().rev() {
                        let record = match decode_placement(&value) {
                            Ok(record) => record,
                            Err(error) => {
                                return self.fail(PlacementError::Decode(error.to_string()));
                            }
                        };
                        self.records.push(record);
                    }
                    self.emit_next_record()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("pending placement iter result", format!("{other:?}"))
                }
            },
            PlacementState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => self.emit_placement_update(false),
                        Err(error) => {
                            warn!(error = %error, "Document sync failed; keeping placement pending");
                            self.emit_placement_update(true)
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            PlacementState::StorePlacement => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::BatchWriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    if self.metadata_refresh_queued {
                        self.state = PlacementState::ScheduleMetadataRefresh;
                        smallvec![schedule_outbox_drain_effect()]
                    } else {
                        self.finish_placement_store()
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement storage result", format!("{other:?}")),
            },
            PlacementState::StartMetadataTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_metadata_state_read(txn_id)
                }
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) => self.retry_metadata_transaction(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata transaction start result", format!("{other:?}"))
                }
            },
            PlacementState::ReadMetadataState => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, lifecycle), (_, lifecycle_revision), (_, registry)] =
                        values.as_slice()
                    else {
                        return self.unexpected_event(
                            "metadata lifecycle, revision, and registry read results",
                            format!("{} batch values", values.len()),
                        );
                    };
                    let registry = match registry {
                        Some(value) => match postcard::from_bytes(value) {
                            Ok(record) => Some(record),
                            Err(error) => {
                                return self.fail(PlacementError::ConversionError(error.into()));
                            }
                        },
                        None => None,
                    };
                    self.emit_metadata_placement_update(
                        lifecycle.clone(),
                        lifecycle_revision.clone(),
                        registry,
                    )
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata state batch read result", format!("{other:?}"))
                }
            },
            PlacementState::WriteMetadataState => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(PlacementError::Placement(
                            "missing metadata replan transaction".to_string(),
                        ));
                    };
                    self.state = PlacementState::CommitMetadataTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata transaction write result", format!("{other:?}"))
                }
            },
            PlacementState::CommitMetadataTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    if self.metadata_refresh_queued {
                        self.state = PlacementState::ScheduleMetadataRefresh;
                        smallvec![schedule_outbox_drain_effect()]
                    } else {
                        self.finish_placement_store()
                    }
                }
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) => self.retry_metadata_transaction(),
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self
                    .unexpected_event("metadata transaction commit result", format!("{other:?}")),
            },
            PlacementState::ScheduleMetadataRefresh => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.metadata_refresh_queued = false;
                    self.finish_placement_store()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule metadata holder refresh; durable outbox remains retryable");
                    self.metadata_refresh_queued = false;
                    self.finish_placement_store()
                }
                other => self.unexpected_event(
                    "metadata holder refresh schedule result",
                    format!("{other:?}"),
                ),
            },
            PlacementState::ScheduleRetry => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.retry_needed = false;
                    self.emit_next_record()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule placement retry; pending placement remains durable");
                    self.retry_needed = false;
                    self.emit_next_record()
                }
                other => self.unexpected_event("task timer schedule result", format!("{other:?}")),
            },
            PlacementState::Init | PlacementState::Finish | PlacementState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PlacementState::Finish | PlacementState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Err(error)) => Err(error),
            _ => Ok(self.rearmed),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_placement::{SYNC_PLACEMENT_RETRY_AFTER, new_placement};
    use aruna_core::structs::{
        BindingScope, NodePlacementEntry, PlacementStrategy, RealmNodeKind, StrategyBinding,
    };
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn group_target(seed: u8) -> DocumentSyncTarget {
        DocumentSyncTarget::Group {
            group_id: Ulid::from_bytes([seed; 16]),
        }
    }

    fn metadata_target(seed: u8) -> DocumentSyncTarget {
        DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([seed; 16]),
        }
    }

    fn config_with(nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([8u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    fn finish_missing_metadata_state_read(
        operation: &mut ProcessPlacementsOperation,
        effects: Effects,
    ) -> Effects {
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        let txn_id = Ulid::from_bytes([14; 16]);
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(read_txn_id),
            })] if reads.len() == 3 && *read_txn_id == txn_id
        ));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(vec![1]), None),
                (Key::from(vec![2]), None),
                (Key::from(vec![3]), None),
            ],
        }))
    }

    fn placement_from_batch(effects: &Effects) -> PendingDocumentPlacement {
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected placement batch write, got {effects:?}");
        };
        writes
            .iter()
            .find(|(key_space, _, _)| key_space == SYNC_PLACEMENT_KEYSPACE)
            .and_then(|(_, _, value)| decode_placement(value).ok())
            .expect("placement update is present")
    }

    #[test]
    fn task_schedule_error_is_non_blocking_after_placement_write() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.state = PlacementState::ScheduleRetry;
        operation.retry_needed = true;

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "task handle unavailable".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, PlacementState::Finish);
        assert_eq!(operation.finalize(), Ok(false));
    }

    #[test]
    fn completed_inventory_record_is_rewritten_not_deleted() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let target = metadata_target(4);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[node(1), node(2), node(3)]));
        operation.records = vec![new_placement(
            realm_id,
            target,
            node(1),
            3,
            vec![node(1), node(2), node(3)],
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected retained placement write, got {effects:?}");
        };
        let record = decode_placement(value.as_ref()).expect("placement decodes");
        assert_eq!(record.selected_holders.len(), 3);
        assert!(!operation.retry_needed);
    }

    #[test]
    fn process_placement_may_select_origin_as_holder() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let origin = node(1);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[origin, node(2)]));
        operation.records = vec![new_placement(
            realm_id,
            metadata_target(5),
            origin,
            3,
            vec![node(2)],
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let current = operation.current.expect("placement is active");
        assert_eq!(current.planned.origin_node_id, origin);
        assert!(current.planned.selected_holders.contains(&origin));
        assert!(current.planned.selected_holders.contains(&node(2)));
        assert!(current.newly_required_remote_holders.contains(&origin));
        assert!(!current.newly_required_remote_holders.contains(&node(2)));
    }

    #[test]
    fn full_and_draining_holders_are_replaced_in_exact_record() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let target = metadata_target(6);
        let mut config = config_with(&[node(1), node(2), node(3), node(4)]);
        config.strategies[0].replica_count = Some(2);
        config.placement_map = vec![
            NodePlacementEntry {
                node_id: node(1),
                location: String::new(),
                weight: 100,
                full: true,
                draining: false,
                labels: BTreeMap::new(),
            },
            NodePlacementEntry {
                node_id: node(2),
                location: String::new(),
                weight: 100,
                full: false,
                draining: true,
                labels: BTreeMap::new(),
            },
        ];
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config);
        operation.records = vec![new_placement(
            realm_id,
            target,
            node(1),
            2,
            vec![node(1), node(2)],
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = operation.step(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
            result: Ok(()),
        }));
        let effects = finish_missing_metadata_state_read(&mut operation, effects);
        let record = placement_from_batch(&effects);
        assert_eq!(record.desired_holder_count, 2);
        assert_eq!(record.selected_holders.len(), 2);
        assert!(record.selected_holders.contains(&node(3)));
        assert!(record.selected_holders.contains(&node(4)));
        assert!(!record.selected_holders.contains(&node(1)));
        assert!(!record.selected_holders.contains(&node(2)));
        assert!(!operation.retry_needed);
    }

    #[test]
    fn metadata_replan_missing_source_and_transaction_conflict_remain_retryable() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let document_id = Ulid::from_bytes([12; 16]);
        let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
        let mut config = config_with(&[node(1), node(2), node(3)]);
        config.strategies[0].replica_count = Some(2);
        config.placement_map = vec![NodePlacementEntry {
            node_id: node(1),
            location: String::new(),
            weight: 100,
            full: true,
            draining: false,
            labels: BTreeMap::new(),
        }];
        let group_id = Ulid::from_bytes([11; 16]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config);
        operation.records = vec![new_placement_with_context(
            realm_id,
            target,
            node(9),
            Some(group_id),
            Some("datasets/replan".to_string()),
            2,
            vec![node(1), node(2)],
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();
        let Some(Effect::SubOperation(mut transfer)) = effects.into_iter().next() else {
            panic!("expected required transfer sub-operation");
        };
        assert!(matches!(
            transfer.start().as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        assert!(
            transfer
                .step(Event::Storage(StorageEvent::ReadResult {
                    key: Key::from(Vec::new()),
                    value: None,
                }))
                .is_empty()
        );
        assert!(transfer.is_complete());

        let effects = operation.step(transfer.finalize());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        let txn_id = Ulid::from_bytes([15; 16]);
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(read_txn_id),
            })] if reads.len() == 3 && *read_txn_id == txn_id
        ));

        let registry = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/replan".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/replan",
                document_id,
            ),
            holder_node_ids: vec![node(1), node(2)],
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: Ulid::from_bytes([10; 16]),
        };
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(vec![1]), None),
                (Key::from(vec![2]), None),
                (
                    Key::from(vec![3]),
                    Some(
                        postcard::to_allocvec(&registry)
                            .expect("registry serializes")
                            .into(),
                    ),
                ),
            ],
        }));
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected atomic registry and placement update, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);
        let registry = writes
            .iter()
            .find(|(key_space, _, _)| key_space == aruna_core::keyspaces::METADATA_INDEX_KEYSPACE)
            .and_then(|(_, _, value)| postcard::from_bytes::<MetadataRegistryRecord>(value).ok())
            .expect("registry update is present");
        assert_eq!(registry.holder_node_ids, vec![node(2)]);
        let placement = writes
            .iter()
            .find(|(key_space, _, _)| key_space == SYNC_PLACEMENT_KEYSPACE)
            .and_then(|(_, _, value)| decode_placement(value).ok())
            .expect("placement update is present");
        assert_eq!(placement.selected_holders, vec![node(2)]);
        assert!(operation.retry_needed);

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn_id })]
                if *commit_txn_id == txn_id
        ));
        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));
        assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
        assert!(operation.rearmed);
        assert!(operation.retry_needed);
        assert!(operation.txn_id.is_none());
        assert!(!operation.metadata_refresh_queued);
    }

    #[test]
    fn process_placement_does_not_implicitly_count_origin() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let origin = node(1);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[origin, node(2)]));
        operation.records = vec![new_placement(
            realm_id,
            metadata_target(7),
            origin,
            2,
            Vec::new(),
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let current = operation.current.expect("placement is active");
        assert_eq!(current.newly_required_remote_holders.len(), 2);
        assert!(current.newly_required_remote_holders.contains(&origin));
        assert!(current.newly_required_remote_holders.contains(&node(2)));
    }

    #[test]
    fn metadata_binding_and_replica_change_replace_count_ref_and_holders() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let target = metadata_target(8);
        let path_strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([10; 16]),
            name: "path".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
        };
        let mut config = config_with(&[node(1), node(2), node(3)]);
        config.strategies[0].replica_count = Some(1);
        config.strategies.push(path_strategy.clone());
        config.strategy_bindings.push(StrategyBinding {
            scope: BindingScope::MetadataPathPrefix("datasets/special".to_string()),
            strategy_id: path_strategy.strategy_id,
        });
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config);
        operation.records = vec![new_placement_with_context(
            realm_id,
            target,
            node(1),
            Some(Ulid::from_bytes([4; 16])),
            Some("datasets/special/object".to_string()),
            1,
            vec![node(1)],
            PlacementRef {
                strategy_id: Ulid::from_bytes([9; 16]),
                epoch: 0,
            },
        )];

        let effects = operation.emit_next_record();
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let current = operation.current.as_ref().expect("placement is active");
        assert_eq!(current.planned.desired_holder_count, 2);
        assert_eq!(current.planned.selected_holders.len(), 2);
        assert_eq!(
            current.planned.placement.strategy_id,
            path_strategy.strategy_id
        );
        assert_eq!(
            current.planned.metadata_path.as_deref(),
            Some("datasets/special/object")
        );

        let effects = operation.emit_placement_update(false);
        let effects = finish_missing_metadata_state_read(&mut operation, effects);
        let record = placement_from_batch(&effects);
        assert_eq!(record.desired_holder_count, 2);
        assert_eq!(record.selected_holders.len(), 2);
        assert_eq!(record.placement.strategy_id, path_strategy.strategy_id);
    }

    #[test]
    fn admin_target_placement_is_drained_not_retried() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.records = vec![new_placement(
            realm_id,
            group_target(4),
            node(1),
            3,
            Vec::new(),
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == SYNC_PLACEMENT_KEYSPACE
        ));
        assert!(!operation.retry_needed);
        assert_eq!(operation.state, PlacementState::StorePlacement);
        assert!(operation.current.is_none());
    }

    fn announce_outbox_record(
        effects: Effects,
        document_bytes: Vec<u8>,
    ) -> aruna_core::document::DocumentSyncOutboxRecord {
        let Some(Effect::SubOperation(mut sub)) = effects.into_iter().next() else {
            panic!("expected announce sub-operation");
        };
        let _read = sub.start();
        let write_effects = sub.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(vec![0u8]),
            value: Some(aruna_core::types::Value::from(document_bytes)),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = write_effects.as_slice() else {
            panic!("expected announce outbox write, got {write_effects:?}");
        };
        let record: aruna_core::document::DocumentSyncOutboxRecord =
            postcard::from_bytes(value.as_ref()).expect("outbox record decodes");
        record
    }

    fn announce_outbox_allow_genesis(effects: Effects, document_bytes: Vec<u8>) -> bool {
        announce_outbox_record(effects, document_bytes).allow_genesis
    }

    fn graph_lifecycle_fixture() -> (DocumentSyncTarget, Vec<u8>) {
        let record = aruna_core::metadata::MetadataGraphLifecycleRecord::deleted(
            "urn:graph:placement-test".to_string(),
            RealmId::from_bytes([8u8; 32]),
            aruna_core::types::GroupId::new(),
            Ulid::from_bytes([9u8; 16]),
            42,
        );
        let bytes = postcard::to_allocvec(&record).expect("lifecycle record serializes");
        let target = DocumentSyncTarget::MetadataGraphLifecycle {
            graph_iri: record.graph_iri.clone(),
        };
        (target, bytes)
    }

    #[test]
    fn process_local_holder_is_counted_but_not_a_network_peer() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        let remote = node(2);
        let (target, bytes) = graph_lifecycle_fixture();
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: local,
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[local, remote]));
        operation.records = vec![new_placement(
            realm_id,
            target,
            node(9),
            2,
            Vec::new(),
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        let current = operation.current.as_ref().expect("placement is active");
        assert!(current.planned.selected_holders.contains(&local));
        assert!(current.planned.selected_holders.contains(&remote));
        assert_eq!(current.selected_holders_after_failure, vec![local]);
        assert_eq!(current.newly_required_remote_holders, vec![remote]);
        let expected_placement = current.planned.placement;
        let outbox = announce_outbox_record(effects, bytes);
        assert_eq!(outbox.peers, vec![remote]);
        let aruna_core::document::DocumentSyncOutboxEvent::Upsert { change, .. } = outbox.event
        else {
            panic!("expected upsert outbox event");
        };
        assert_eq!(change.placement, expected_placement);
    }

    fn placement_announce_allow_genesis(
        local: NodeId,
        origin: NodeId,
        target: DocumentSyncTarget,
        document_bytes: Vec<u8>,
    ) -> bool {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: local,
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[node(1), node(2), node(3), local]));
        operation.records = vec![new_placement(
            realm_id,
            target,
            origin,
            3,
            Vec::new(),
            PlacementRef::NIL,
        )];
        announce_outbox_allow_genesis(operation.emit_next_record(), document_bytes)
    }

    #[test]
    fn ordinary_document_retry_allow_genesis_tracks_origin() {
        let local = node(1);
        let (target, bytes) = graph_lifecycle_fixture();
        assert!(
            placement_announce_allow_genesis(local, local, target.clone(), bytes.clone()),
            "local origin may mint genesis"
        );
        assert!(
            !placement_announce_allow_genesis(node(9), local, target, bytes),
            "non-origin publisher must not mint genesis"
        );
    }

    #[test]
    fn node_info_origin_retry_disallows_genesis() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        let target = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: local,
        };

        assert!(
            !placement_announce_allow_genesis(local, local, target, b"node info".to_vec()),
            "NodeInfo retries must not mint shared-topic genesis"
        );
    }
}
