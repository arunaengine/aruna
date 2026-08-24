use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState,
    overlay_realm_config_placement_reducer_materialization, realm_config_node_id_from_path,
    realm_config_node_path,
};
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::ADMIN_DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, BandPool, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_BANDS, HANDLE_RANGE_SIZE,
    HandleRange, PlacementBinding, PlacementScope, RealmConfigDocument, RealmNodeKind, band_start,
    coordinator_spans, owned_pools,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use aruna_core::util::unix_timestamp_millis;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;

#[derive(Debug, Clone, PartialEq)]
pub struct EnsureRealmConfigConfig {
    pub actor: Actor,
    pub target_node_id: NodeId,
    pub target_node_kind: RealmNodeKind,
    pub default_metadata_replication_factor: u32,
    pub realm_description: String,
    pub create_if_missing: bool,
    pub reject_kind_mismatch: bool,
}

#[derive(Debug, PartialEq)]
pub struct EnsureRealmConfigOperation {
    config: EnsureRealmConfigConfig,
    txn_id: Option<TxnId>,
    state: EnsureRealmConfigState,
    output: Option<Result<RealmConfigDocument, EnsureRealmConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum EnsureRealmConfigState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitNoop {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain {
        document: RealmConfigDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnsureRealmConfigError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("realm config node {node_id} already exists with a different kind")]
    NodeKindMismatch { node_id: NodeId },
    #[error("realm handle space is fully assigned")]
    HandleSpaceExhausted,
    #[error("onboarding coordinator {node_id} has no band pool")]
    CoordinatorPoolMissing { node_id: NodeId },
    #[error("realm config has no placement strategy for the job-control binding")]
    DefaultStrategyMissing,
    #[error("granted band start is not a valid placement handle")]
    InvalidBandStart,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl EnsureRealmConfigOperation {
    pub fn new(config: EnsureRealmConfigConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: EnsureRealmConfigState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = EnsureRealmConfigState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                    admin_document_reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write_document_and_admin_state(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, EnsureRealmConfigError> {
        let Some(txn_id) = self.txn_id else {
            return Err(EnsureRealmConfigError::MissingTransaction);
        };
        let fresh = document_value.is_none();
        let mut document = match document_value.as_deref() {
            Some(value) => RealmConfigDocument::from_bytes(value)?,
            None if self.config.create_if_missing => {
                let mut document = RealmConfigDocument::new(
                    self.config.actor.realm_id,
                    Vec::new(),
                    self.config.default_metadata_replication_factor,
                );
                document.description = self.config.realm_description.clone();
                // Seed default placement so no production path ever constructs a
                // strategy-less config (placement resolution needs strategies).
                document.seed_default_placement();
                document
            }
            None => return Err(EnsureRealmConfigError::RealmConfigNotFound),
        };

        if self.config.reject_kind_mismatch {
            let target_node_id = self.config.target_node_id.to_string();
            if document.nodes.iter().any(|node| {
                node.node_id == target_node_id && node.kind != self.config.target_node_kind
            }) {
                return Err(EnsureRealmConfigError::NodeKindMismatch {
                    node_id: self.config.target_node_id,
                });
            }
        }

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
                    value.as_ref(),
                )
                .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentReducerError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(target));
        overlay_realm_config_reducer_materialization(
            &mut document,
            &reducer_state,
            unix_timestamp_millis(),
        );

        let node_is_noop = previous_reducer_state.as_ref().is_some_and(|state| {
            realm_config_node_ensure_is_noop(
                &document,
                state,
                &self.config.target_node_id,
                &self.config.target_node_kind,
            )
        });
        // A fresh document seeds the creating coordinator with the whole space
        // as a self-issued root pool.
        let seed_pool = (fresh && document.band_pools.is_empty()).then(|| BandPool {
            pool_id: Ulid::generate(),
            parent: None,
            issuer: self.config.actor.node_id,
            owner: self.config.actor.node_id,
            start: FIRST_GRANTABLE_HANDLE,
            end: band_start(HANDLE_BANDS),
        });
        let mut pools = document.band_pools.clone();
        pools.extend(seed_pool);

        let directory = document.handle_range_directory();
        let usable_ranges = directory.granted_to(&self.config.target_node_id);
        // A usable grant is reused; otherwise the acting coordinator consumes
        // the lowest free band of its own disjoint pool, so two disconnected
        // coordinators can never mint colliding grants.
        let assigned_range = match usable_ranges.first() {
            Some(usable) => *usable,
            None => {
                let spans = coordinator_spans(&pools, &self.config.actor.node_id);
                if spans.is_empty() {
                    return Err(EnsureRealmConfigError::CoordinatorPoolMissing {
                        node_id: self.config.actor.node_id,
                    });
                }
                let (start, end) = directory
                    .free_band_in(&spans)
                    .ok_or(EnsureRealmConfigError::HandleSpaceExhausted)?;
                HandleRange {
                    range_id: Ulid::generate(),
                    owner: self.config.target_node_id,
                    start,
                    end,
                }
            }
        };
        let range_is_noop = reducer_state
            .materialized_handle_ranges()
            .get(&assigned_range.range_id)
            == Some(&assigned_range);
        // The band's first handle is the target's JobControl handle; the
        // immutable binding is appended at most once per handle.
        let job_handle = PlacementHandle::new(assigned_range.start)
            .map_err(|_| EnsureRealmConfigError::InvalidBandStart)?;
        let job_binding = if document
            .placement_bindings
            .iter()
            .any(|binding| binding.handle == job_handle)
        {
            None
        } else {
            let strategy_id = document
                .default_strategy_id
                .or_else(|| document.strategies.first().map(|s| s.strategy_id))
                .ok_or(EnsureRealmConfigError::DefaultStrategyMissing)?;
            Some(PlacementBinding {
                handle: job_handle,
                scope: PlacementScope::Realm(self.config.actor.realm_id),
                document_class: DocumentClass::JobControl,
                strategy_id,
                allocator_range_id: Some(assigned_range.range_id),
                allocated_by: Some(self.config.target_node_id),
                allocated_at_ms: Some(unix_timestamp_millis()),
            })
        };
        // A new management coordinator receives an unused tail slice of the
        // acting coordinator's pool so it can onboard nodes on its own.
        let transfer_pool = if self.config.target_node_kind == RealmNodeKind::Management
            && self.config.target_node_id != self.config.actor.node_id
            && coordinator_spans(&pools, &self.config.target_node_id).is_empty()
        {
            let spans = coordinator_spans(&pools, &self.config.actor.node_id);
            let mut consumed = document.placement_handle_ranges.clone();
            consumed.push(assigned_range);
            // The slice must sit inside one owned parent pool so lineage holds.
            let child = pool_transfer_slice(&spans, &consumed).and_then(|(start, end)| {
                owned_pools(&pools, &self.config.actor.node_id)
                    .into_iter()
                    .find(|pool| pool.start <= start && end <= pool.end)
                    .map(|parent| BandPool {
                        pool_id: Ulid::generate(),
                        parent: Some(parent.pool_id),
                        issuer: self.config.actor.node_id,
                        owner: self.config.target_node_id,
                        start,
                        end,
                    })
            });
            if child.is_none() {
                warn!(
                    target_node = %self.config.target_node_id,
                    "New coordinator gets no band pool: the acting pool cannot be split"
                );
            }
            child
        } else {
            None
        };
        if node_is_noop
            && range_is_noop
            && job_binding.is_none()
            && seed_pool.is_none()
            && transfer_pool.is_none()
        {
            self.output = Some(Ok(document.clone()));
            return Ok(self.emit_commit_noop(document));
        }

        let mut admin_events = Vec::with_capacity(5);
        if !node_is_noop {
            admin_events.push(apply_realm_config_node_ensure(
                &mut reducer_state,
                &self.config.actor,
                self.config.target_node_id,
                self.config.target_node_kind.clone(),
            )?);
        }
        if let Some(pool) = seed_pool {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigBandPoolAssigned { pool },
            )?);
        }
        if !range_is_noop {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigHandleRangeGranted {
                    range: assigned_range,
                },
            )?);
        }
        if let Some(binding) = job_binding {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
            )?);
        }
        if let Some(pool) = transfer_pool {
            admin_events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigBandPoolAssigned { pool },
            )?);
        }
        // A fresh document seeds its placement identity locally; without the
        // matching reducer events a later reducer-only rebuild loses them and
        // the sealed family routing with them.
        if fresh
            && reducer_state.materialized_family_strategy().is_none()
            && reducer_state
                .materialized_realm_config_placement_strategies()
                .is_empty()
        {
            admin_events.extend(crate::create_realm::seed_placement_events(
                &mut reducer_state,
                &self.config.actor,
                &document,
            )?);
        }
        overlay_realm_config_reducer_materialization(
            &mut document,
            &reducer_state,
            unix_timestamp_millis(),
        );

        let stale_conflict_deletes = stale_admin_document_conflict_delete_entries(
            previous_reducer_state.as_ref(),
            Some(&reducer_state),
        );
        let document_target = self.document_ref();
        let placement = placement_ref_for_target(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        for admin_event in admin_events {
            let record = new_outbox_record_with_id(
                admin_event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::admin(admin_event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = EnsureRealmConfigState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_noop(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EnsureRealmConfigError::MissingTransaction);
        };
        self.state = EnsureRealmConfigState::CommitNoop { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(EnsureRealmConfigError::MissingTransaction);
        };
        self.state = EnsureRealmConfigState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: EnsureRealmConfigError) -> Effects {
        let cleanup = self.abort();
        self.state = EnsureRealmConfigState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(EnsureRealmConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for EnsureRealmConfigOperation {
    type Output = RealmConfigDocument;
    type Error = EnsureRealmConfigError;

    fn start(&mut self) -> Effects {
        self.state = EnsureRealmConfigState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            EnsureRealmConfigState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            EnsureRealmConfigState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_write_document_and_admin_state(
                        document_value.clone(),
                        reducer_state_value.clone(),
                    ) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            EnsureRealmConfigState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(EnsureRealmConfigError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = EnsureRealmConfigState::DeleteStaleAdminConflicts { document };
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            EnsureRealmConfigState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            EnsureRealmConfigState::CommitNoop { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            EnsureRealmConfigState::CommitTransaction { document } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state =
                        EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { document };
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { .. } => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = EnsureRealmConfigState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            EnsureRealmConfigState::Finish
            | EnsureRealmConfigState::Error
            | EnsureRealmConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            EnsureRealmConfigState::Finish | EnsureRealmConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or_else(|| {
            Ok(RealmConfigDocument::default_for_realm(
                self.config.actor.realm_id,
                Vec::new(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Upper half of the largest free band run of `spans`; `None` when fewer than
/// two free bands remain (the new coordinator then starts without a pool).
fn pool_transfer_slice(spans: &[(u32, u32)], consumed: &[HandleRange]) -> Option<(u32, u32)> {
    let mut best: Option<(u32, u32)> = None;
    for (span_start, span_end) in spans {
        let mut run_start = None;
        let bands = span_end.saturating_sub(*span_start) / HANDLE_RANGE_SIZE;
        for band in 0..=bands {
            let start = span_start + band * HANDLE_RANGE_SIZE;
            let free = band < bands
                && !consumed
                    .iter()
                    .any(|range| range.start < start + HANDLE_RANGE_SIZE && start < range.end);
            match (free, run_start) {
                (true, None) => run_start = Some(start),
                (false, Some(from)) => {
                    if best
                        .is_none_or(|(best_start, best_end)| start - from > best_end - best_start)
                    {
                        best = Some((from, start));
                    }
                    run_start = None;
                }
                _ => {}
            }
        }
    }
    let (start, end) = best?;
    let bands = (end - start) / HANDLE_RANGE_SIZE;
    (bands >= 2).then(|| (start + bands.div_ceil(2) * HANDLE_RANGE_SIZE, end))
}

fn apply_realm_config_node_ensure(
    state: &mut AdminDocumentReducerState,
    actor: &Actor,
    node_id: NodeId,
    kind: RealmNodeKind,
) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::generate(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op: AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
    };
    state.apply(&event)?;
    Ok(event)
}

pub(crate) fn overlay_realm_config_reducer_materialization(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    now_ms: u64,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_node_id_from_path(path) {
            remove_realm_config_node(config, &node_id);
        }
    }

    for node_id in reducer_state.removed_config_nodes() {
        remove_realm_config_node(config, &node_id);
    }

    for (node_id, kind) in reducer_state.materialized_realm_config_nodes() {
        let path = realm_config_node_path(&node_id);
        if reducer_state.conflicts.contains_key(&path) {
            remove_realm_config_node(config, &node_id);
            continue;
        }
        config.ensure_node(node_id, kind);
    }

    overlay_realm_config_placement_reducer_materialization(config, reducer_state, now_ms);
}

fn realm_config_node_ensure_is_noop(
    document: &RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let path = realm_config_node_path(node_id);
    !reducer_state.conflicts.contains_key(&path)
        && reducer_state
            .materialized_realm_config_nodes()
            .get(node_id)
            .is_some_and(|materialized_kind| materialized_kind == kind)
        && realm_config_document_has_node_kind(document, node_id, kind)
}

fn realm_config_document_has_node_kind(
    document: &RealmConfigDocument,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let node_id = node_id.to_string();
    let mut matches = document.nodes.iter().filter(|node| node.node_id == node_id);
    matches.next().is_some_and(|node| node.kind == *kind) && matches.all(|node| node.kind == *kind)
}

fn remove_realm_config_node(config: &mut RealmConfigDocument, node_id: &NodeId) {
    let node_id = node_id.to_string();
    config.nodes.retain(|node| node.node_id != node_id);
}

#[cfg(test)]
mod tests {
    use aruna_core::admin_document_reducer::{
        AdminDocumentConflict, AdminDocumentConflictValue, AdminDocumentReducerState,
        REALM_CONFIG_DEFAULT_STRATEGY_PATH,
    };
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE,
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::admin_document_reducer_conflict_key;
    use aruna_core::structs::{
        Actor, BandPool, BindingScope, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_BANDS,
        HANDLE_RANGE_SIZE, HandleRange, NodePlacementEntry, PlacementOverride, PlacementStrategy,
        RealmConfigDocument, RealmId, RealmNodeKind, StrategyBinding, band_start,
        coordinator_spans,
    };
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{Effects, Key, KeySpace, TxnId, UserId, Value};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    use super::{
        EnsureRealmConfigConfig, EnsureRealmConfigError, EnsureRealmConfigOperation,
        EnsureRealmConfigState, overlay_realm_config_reducer_materialization,
        realm_config_node_path,
    };

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(seed: u8, realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(seed),
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor, factor: u32) -> EnsureRealmConfigConfig {
        EnsureRealmConfigConfig {
            target_node_id: actor.node_id,
            target_node_kind: RealmNodeKind::Management,
            actor,
            default_metadata_replication_factor: factor,
            realm_description: "Ensured Realm".to_string(),
            create_if_missing: true,
            reject_kind_mismatch: false,
        }
    }

    fn conflict(path: &str) -> AdminDocumentConflict {
        let value = |seed: u8, value: &str| AdminDocumentConflictValue {
            value: Some(value.to_string()),
            dot: AdminDocumentDot {
                event_id: Ulid::from_bytes([seed; 16]),
                origin_node_id: node(seed),
                origin_seq: 1,
            },
        };
        AdminDocumentConflict {
            path: path.to_string(),
            values: vec![value(3, "server"), value(4, "management")],
        }
    }

    fn batch_write(effects: Effects, txn_id: TxnId) -> Vec<(KeySpace, Key, Value)> {
        match effects.into_iter().next().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(id, Some(txn_id));
                writes
            }
            other => panic!("unexpected write effect: {other:?}"),
        }
    }

    fn write_value<'a>(writes: &'a [(KeySpace, Key, Value)], keyspace: &str) -> &'a Value {
        &writes
            .iter()
            .find(|(candidate, _, _)| candidate == keyspace)
            .expect("write exists")
            .2
    }

    #[test]
    fn writes_missing_config_reducer_state_outbox_and_stale_conflict_delete() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let path = realm_config_node_path(&actor.node_id);
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        for seed in [3, 4] {
            previous_state.clock.advance(node(seed), 1);
        }
        previous_state
            .conflicts
            .insert(path.clone(), conflict(&path));

        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 7));
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);
        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(
                    None,
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                )
                .unwrap(),
            txn_id,
        );

        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state: AdminDocumentReducerState =
            postcard::from_bytes(write_value(&writes, ADMIN_DOCUMENT_STATE_KEYSPACE)).unwrap();
        let outbox: DocumentSyncOutboxRecord =
            postcard::from_bytes(write_value(&writes, DOCUMENT_SYNC_OUTBOX_KEYSPACE)).unwrap();
        assert_eq!(stored.metadata_replication.default_replication_factor, 7);
        assert_eq!(stored.description, "Ensured Realm");
        assert!(stored.has_node(actor.node_id));
        let default_strategy = stored
            .default_strategy_id
            .and_then(|strategy_id| stored.strategy(&strategy_id))
            .expect("create-if-missing seeds a default placement strategy");
        assert_eq!(default_strategy.replica_count, Some(7));
        assert_eq!(
            state.materialized_realm_config_nodes()[&actor.node_id],
            RealmNodeKind::Management
        );
        assert_eq!(outbox.target, DocumentSyncTarget::RealmConfig { realm_id });
        assert!(matches!(
            &outbox.event,
            DocumentSyncOutboxEvent::AdminOperation { event, .. }
                if event.target == target
                    && matches!(event.op, AdminDocumentOperation::RealmConfigNodeEnsured { .. })
        ));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        assert_eq!(
            effects.first(),
            Some(&Effect::Storage(StorageEffect::BatchDelete {
                deletes: vec![(
                    ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                    admin_document_reducer_conflict_key(&target, &path),
                )],
                txn_id: Some(txn_id),
            }))
        );
    }

    #[test]
    fn fresh_seeds_bindings() {
        // A fresh bootstrap must materialize the placement identity the create
        // path does: whatever only the document carries a rebuild loses.
        let realm_id = RealmId::from_bytes([9; 32]);
        let mut operation = EnsureRealmConfigOperation::new(config(actor(1, realm_id), 3));
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);

        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(None, None)
                .unwrap(),
            txn_id,
        );

        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state: AdminDocumentReducerState =
            postcard::from_bytes(write_value(&writes, ADMIN_DOCUMENT_STATE_KEYSPACE)).unwrap();

        let bindings = state.materialized_realm_config_strategy_bindings();
        assert!(!bindings.is_empty());
        assert_eq!(bindings.len(), stored.strategy_bindings.len());
        for binding in &stored.strategy_bindings {
            assert!(
                bindings
                    .values()
                    .any(|materialized| materialized == binding)
            );
        }
        let placement = state.materialized_placement_bindings();
        for binding in &stored.placement_bindings {
            assert_eq!(placement.get(&binding.handle), Some(binding));
        }
        assert!(
            stored
                .placement_bindings
                .iter()
                .any(|binding| binding.document_class == DocumentClass::Metadata)
        );
        assert!(state.materialized_family_strategy().is_some());
    }

    fn pooled_document(realm_id: RealmId, pools: &[(u8, Actor, u32, u32)]) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.seed_default_placement();
        for (seed, owner, start_band, end_band) in pools {
            document.band_pools.push(BandPool {
                pool_id: Ulid::from_bytes([*seed; 16]),
                parent: None,
                issuer: owner.node_id,
                owner: owner.node_id,
                start: band_start(*start_band),
                end: band_start(*end_band),
            });
        }
        document
    }

    fn run_ensure(
        actor: &Actor,
        target: aruna_core::NodeId,
        kind: RealmNodeKind,
        document: &RealmConfigDocument,
    ) -> (RealmConfigDocument, Vec<u8>) {
        let mut operation = EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            target_node_id: target,
            target_node_kind: kind,
            ..config(actor.clone(), 3)
        });
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);
        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(
                    Some(document.to_bytes(actor).unwrap().into()),
                    None,
                )
                .unwrap(),
            txn_id,
        );
        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state = write_value(&writes, ADMIN_DOCUMENT_STATE_KEYSPACE).to_vec();
        (stored, state)
    }

    #[test]
    fn pools_stay_disjoint() {
        // Two disconnected coordinators onboard concurrently from the same base
        // document; disjoint pools make colliding grants impossible.
        let realm_id = RealmId::from_bytes([31; 32]);
        let actor_a = actor(31, realm_id);
        let actor_b = actor(32, realm_id);
        let joiner_a = node(41);
        let joiner_b = node(42);
        let mid = HANDLE_BANDS / 2;
        let document = pooled_document(
            realm_id,
            &[
                (1, actor_a.clone(), 0, mid),
                (2, actor_b.clone(), mid, HANDLE_BANDS),
            ],
        );

        let (after_a, state_a) = run_ensure(&actor_a, joiner_a, RealmNodeKind::Server, &document);
        let (after_b, _) = run_ensure(&actor_b, joiner_b, RealmNodeKind::Server, &document);
        let granted = |after: &RealmConfigDocument, owner: &aruna_core::NodeId| {
            let granted = after.handle_range_directory().granted_to(owner);
            assert_eq!(granted.len(), 1, "one usable grant per joiner");
            granted[0]
        };
        let granted_a = granted(&after_a, &joiner_a);
        let granted_b = granted(&after_b, &joiner_b);
        assert!(
            !granted_a.overlaps(&granted_b),
            "pool grants must be disjoint"
        );
        assert!(granted_a.start < band_start(mid), "A grants from A's pool");
        assert!(granted_b.start >= band_start(mid), "B grants from B's pool");

        // Each joiner's JobControl binding names the band's first handle.
        for (after, joiner, grant) in [
            (&after_a, joiner_a, granted_a),
            (&after_b, joiner_b, granted_b),
        ] {
            let handle = after.job_control_handle(&joiner).expect("binding appended");
            assert_eq!(handle.get(), grant.start);
        }

        // CRDT union of both rounds keeps both grants and bindings usable.
        let mut merged = after_a.clone();
        merged.placement_handle_ranges.push(granted_b);
        merged.placement_bindings.extend(
            after_b
                .placement_bindings
                .iter()
                .filter(|binding| binding.allocated_by == Some(joiner_b))
                .cloned(),
        );
        let directory = merged.handle_range_directory();
        assert_eq!(directory.conflicts(), 0);
        assert_eq!(directory.granted_to(&joiner_a), vec![granted_a]);
        assert_eq!(directory.granted_to(&joiner_b), vec![granted_b]);
        assert!(merged.job_control_handle(&joiner_a).is_some());
        assert!(merged.job_control_handle(&joiner_b).is_some());

        // Retrying the same joiner over converged state is a pure noop commit.
        let mut operation = EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            target_node_id: joiner_a,
            target_node_kind: RealmNodeKind::Server,
            ..config(actor_a.clone(), 3)
        });
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);
        let effects = operation
            .emit_write_document_and_admin_state(
                Some(merged.to_bytes(&actor_a).unwrap().into()),
                Some(state_a.into()),
            )
            .unwrap();
        assert_eq!(
            effects.first(),
            Some(&Effect::Storage(StorageEffect::CommitTransaction {
                txn_id
            }))
        );
    }

    #[test]
    fn transfer_splits_pool() {
        // A new management coordinator receives the tail half of the acting
        // coordinator's free pool and can grant on its own afterwards.
        let realm_id = RealmId::from_bytes([33; 32]);
        let actor_a = actor(33, realm_id);
        let coordinator = node(43);
        let document = pooled_document(realm_id, &[(1, actor_a.clone(), 0, HANDLE_BANDS)]);

        let (after, _) = run_ensure(&actor_a, coordinator, RealmNodeKind::Management, &document);
        let actor_spans = coordinator_spans(&after.band_pools, &actor_a.node_id);
        let joiner_spans = coordinator_spans(&after.band_pools, &coordinator);
        assert!(!joiner_spans.is_empty(), "new coordinator received a pool");
        for (start, end) in &joiner_spans {
            for (actor_start, actor_end) in &actor_spans {
                assert!(end <= actor_start || actor_end <= start, "pools overlap");
            }
        }
        // The joiner's node band was consumed from the actor's kept slice.
        let granted = after.handle_range_directory().granted_to(&coordinator);
        assert_eq!(granted.len(), 1);
        assert!(granted[0].start < joiner_spans[0].0);

        // The new coordinator can now grant a band from its own pool.
        let (after_second, _) = run_ensure(
            &Actor {
                node_id: coordinator,
                ..actor_a.clone()
            },
            node(44),
            RealmNodeKind::Server,
            &after,
        );
        let second = after_second.handle_range_directory().granted_to(&node(44));
        assert_eq!(second.len(), 1);
        assert!(second[0].start >= joiner_spans[0].0, "grant from own pool");
    }

    #[test]
    fn missing_pool_fails() {
        // A coordinator without a pool must fail closed, never self-admit.
        let realm_id = RealmId::from_bytes([34; 32]);
        let actor_a = actor(34, realm_id);
        let document = pooled_document(realm_id, &[]);
        let mut operation = EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            create_if_missing: false,
            ..config(actor_a.clone(), 3)
        });
        operation.txn_id = Some(TxnId::generate());
        let error = operation
            .emit_write_document_and_admin_state(
                Some(document.to_bytes(&actor_a).unwrap().into()),
                None,
            )
            .unwrap_err();
        assert_eq!(
            error,
            EnsureRealmConfigError::CoordinatorPoolMissing {
                node_id: actor_a.node_id
            }
        );
    }

    #[test]
    fn idempotent_same_node_ensure_does_not_duplicate_config_node() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let actor = actor(9, realm_id);
        let mut document = pooled_document(realm_id, &[(1, actor.clone(), 0, HANDLE_BANDS)]);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 3));
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);
        let writes = batch_write(
            operation
                .emit_write_document_and_admin_state(
                    Some(document.to_bytes(&actor).unwrap().into()),
                    None,
                )
                .unwrap(),
            txn_id,
        );
        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        assert_eq!(stored.nodes.len(), 1);
        assert!(stored.has_node(actor.node_id));
    }

    #[test]
    fn repeated_ensure_does_not_write_admin_outbox_event() {
        let realm_id = RealmId::from_bytes([10; 32]);
        let actor = actor(10, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut previous_state = AdminDocumentReducerState::new(target.clone());
        previous_state
            .apply(&AdminDocumentEvent {
                event_id: Ulid::from_bytes([10; 16]),
                target,
                origin_node_id: actor.node_id,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor.clone(),
                op: AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: actor.node_id,
                    kind: RealmNodeKind::Management,
                },
            })
            .unwrap();
        let range = HandleRange {
            range_id: Ulid::from_bytes([11; 16]),
            owner: actor.node_id,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        };
        previous_state
            .apply(&AdminDocumentEvent {
                event_id: Ulid::from_bytes([11; 16]),
                target: AdminDocumentTarget::RealmConfig { realm_id },
                origin_node_id: actor.node_id,
                origin_seq: 2,
                observed: AdminDocumentClock::default(),
                actor: actor.clone(),
                op: AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
            })
            .unwrap();
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.placement_handle_ranges.push(range);
        // The reserved JobControl binding already exists: nothing to append.
        document
            .placement_bindings
            .push(aruna_core::structs::PlacementBinding {
                handle: aruna_core::structured_id::PlacementHandle::new(range.start).unwrap(),
                scope: aruna_core::structs::PlacementScope::Realm(realm_id),
                document_class: DocumentClass::JobControl,
                strategy_id: Ulid::from_bytes([12; 16]),
                allocator_range_id: Some(range.range_id),
                allocated_by: Some(actor.node_id),
                allocated_at_ms: Some(1),
            });

        let mut operation = EnsureRealmConfigOperation::new(config(actor.clone(), 3));
        let txn_id = TxnId::generate();
        operation.txn_id = Some(txn_id);
        let effects = operation
            .emit_write_document_and_admin_state(
                Some(document.to_bytes(&actor).unwrap().into()),
                Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            )
            .unwrap();

        assert_eq!(
            effects.first(),
            Some(&Effect::Storage(StorageEffect::CommitTransaction {
                txn_id
            }))
        );
        assert!(!effects.iter().any(|effect| matches!(
            effect,
            Effect::Storage(StorageEffect::BatchWrite { writes, .. })
                if writes
                    .iter()
                    .any(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
        )));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize().unwrap(), document);
    }

    #[test]
    fn scheduled_admin_outbox_finishes_without_direct_replication() {
        let realm_id = RealmId::from_bytes([11; 32]);
        let actor = actor(11, realm_id);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let mut operation = EnsureRealmConfigOperation::new(config(actor, 3));
        operation.state = EnsureRealmConfigState::ScheduleDocumentSyncOutboxDrain { document };

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, EnsureRealmConfigState::Finish);
    }

    #[test]
    fn overlay_materializes_placement_fields_from_reducer_state() {
        let realm_id = RealmId::from_bytes([21; 32]);
        let actor = actor(21, realm_id);
        let mut state =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });

        let entry = NodePlacementEntry {
            node_id: actor.node_id,
            location: "eu-west".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([4; 16]),
            name: "default".to_string(),
            replica_count: Some(3),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        let binding = StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: strategy.strategy_id,
        };
        let record = PlacementOverride {
            subject: b"document-subject".to_vec(),
            pinned: vec![actor.node_id],
            excluded: Vec::new(),
            strategy_id: Some(strategy.strategy_id),
        };
        for op in [
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: entry.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
            AdminDocumentOperation::RealmConfigDefaultStrategySet {
                strategy_id: strategy.strategy_id,
            },
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
        ] {
            state.apply_operation(&actor, op).unwrap();
        }

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        overlay_realm_config_reducer_materialization(&mut config, &state, 0);

        assert_eq!(config.placement_map, vec![entry]);
        assert_eq!(config.strategies, vec![strategy.clone()]);
        assert_eq!(config.default_strategy_id, Some(strategy.strategy_id));
        assert_eq!(config.strategy_bindings, vec![binding]);
        assert_eq!(config.placement_overrides, vec![record]);
    }

    #[test]
    fn drops_removed_node() {
        // The stored document still carries a membership the reducer dropped,
        // so the overlay has to subtract it again.
        let realm_id = RealmId::from_bytes([24; 32]);
        let actor = actor(24, realm_id);
        let device = node(9);
        let mut state =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
        state
            .apply_operation(
                &actor,
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: device,
                    kind: RealmNodeKind::User {
                        owner: UserId::nil(realm_id),
                    },
                },
            )
            .unwrap();

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        overlay_realm_config_reducer_materialization(&mut config, &state, 0);
        assert!(
            config
                .nodes
                .iter()
                .any(|node| node.node_id == device.to_string())
        );

        state
            .apply_operation(
                &actor,
                AdminDocumentOperation::RealmConfigNodeRemoved { node_id: device },
            )
            .unwrap();
        overlay_realm_config_reducer_materialization(&mut config, &state, 0);
        assert!(
            config
                .nodes
                .iter()
                .all(|node| node.node_id != device.to_string())
        );
    }

    #[test]
    fn overlay_clears_prior_default_strategy_on_reducer_conflict() {
        let realm_id = RealmId::from_bytes([22; 32]);
        let actor_a = actor(22, realm_id);
        let actor_b = actor(23, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut state = AdminDocumentReducerState::new(target.clone());
        let prior_default = Ulid::from_bytes([5; 16]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.default_strategy_id = Some(prior_default);

        overlay_realm_config_reducer_materialization(&mut config, &state, 0);
        assert_eq!(config.default_strategy_id, None);

        for (event_id, actor, strategy_id) in [
            (
                Ulid::from_bytes([6; 16]),
                &actor_a,
                Ulid::from_bytes([7; 16]),
            ),
            (
                Ulid::from_bytes([8; 16]),
                &actor_b,
                Ulid::from_bytes([9; 16]),
            ),
        ] {
            state
                .apply(&AdminDocumentEvent {
                    event_id,
                    target: target.clone(),
                    origin_node_id: actor.node_id,
                    origin_seq: 1,
                    observed: AdminDocumentClock::default(),
                    actor: actor.clone(),
                    op: AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
                })
                .unwrap();
        }

        assert!(
            state
                .conflicts
                .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
        );
        assert_eq!(state.materialized_realm_config_default_strategy(), None);

        overlay_realm_config_reducer_materialization(&mut config, &state, 0);
        assert_eq!(config.default_strategy_id, None);
    }

    #[test]
    fn rejects_existing_node_kind_mismatch_when_configured() {
        let realm_id = RealmId::from_bytes([8; 32]);
        let actor = actor(8, realm_id);
        let target_node_id = node(7);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(target_node_id, RealmNodeKind::Management);

        let mut operation = EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            target_node_id,
            target_node_kind: RealmNodeKind::Server,
            reject_kind_mismatch: true,
            ..config(actor.clone(), 3)
        });
        operation.txn_id = Some(TxnId::generate());

        let error = operation
            .emit_write_document_and_admin_state(
                Some(document.to_bytes(&actor).unwrap().into()),
                None,
            )
            .unwrap_err();

        assert_eq!(
            error,
            EnsureRealmConfigError::NodeKindMismatch {
                node_id: target_node_id
            }
        );
    }
}
