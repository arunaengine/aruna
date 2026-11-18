//! Makes sure the realm config exists and lists the target node with its kind and handles.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{
    AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{
    AdminDocumentError, AdminDocumentState, config_node_path, overlay_placement, parse_config_node,
};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group_delete::MembershipFence;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::record::{
    BandPool, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_BANDS, HANDLE_RANGE_SIZE, HandleRange,
    PlacementBinding, PlacementScope, band_start, coordinator_spans, owned_pools,
};
use aruna_core::structured_id::PlacementHandle;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

#[derive(Debug, Clone, PartialEq)]
pub struct EnsureConfigParams {
    pub actor: Actor,
    pub target_node_id: NodeId,
    pub target_node_kind: RealmNodeKind,
    pub metadata_replication_factor: u32,
    pub realm_description: String,
    pub create_if_missing: bool,
    pub reject_kind_mismatch: bool,
}

/// Ensures the target node's entry, handle range and job-control binding in the realm config in
/// one write transaction; an unchanged config commits nothing. Callers rerun it after a lost
/// commit.
#[derive(Debug, PartialEq)]
pub struct EnsureConfigOperation {
    config: EnsureConfigParams,
    membership: MembershipFence,
    stage: Stage,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

struct EnsureResources {
    seed_pool: Option<BandPool>,
    assigned_range: HandleRange,
    range_is_noop: bool,
    job_binding: Option<PlacementBinding>,
    transfer_pool: Option<BandPool>,
}

/// What the ensure transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
}

type EnsureTurn = Result<Next<RealmConfigDocument>, EnsureConfigError>;

#[derive(Debug, Error, PartialEq)]
pub enum EnsureConfigError {
    #[error("realm membership is frozen while group deletion is pending")]
    DeletionPending,
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for EnsureConfigError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "EnsureConfig".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl EnsureConfigOperation {
    pub fn new(config: EnsureConfigParams) -> Self {
        Self {
            config,
            membership: MembershipFence::default(),
            stage: Stage::Read,
            document: None,
            deletes: Vec::new(),
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn plan_resources(
        &self,
        document: &RealmConfigDocument,
        reducer_state: &AdminDocumentState,
        fresh: bool,
    ) -> Result<EnsureResources, EnsureConfigError> {
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
        // Reuse a grant or consume the lowest free band from the coordinator's disjoint pool.
        let assigned_range = match usable_ranges.first() {
            Some(usable) => *usable,
            None => {
                let spans = coordinator_spans(&pools, &self.config.actor.node_id);
                if spans.is_empty() {
                    return Err(EnsureConfigError::CoordinatorPoolMissing {
                        node_id: self.config.actor.node_id,
                    });
                }
                let (start, end) = directory
                    .free_band_in(&spans)
                    .ok_or(EnsureConfigError::HandleSpaceExhausted)?;
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
        let job_handle = PlacementHandle::new(assigned_range.start)
            .map_err(|_| EnsureConfigError::InvalidBandStart)?;
        let job_binding = if document
            .placement_bindings
            .iter()
            .any(|binding| binding.handle == job_handle)
        {
            None
        } else {
            let strategy_id = document
                .default_strategy_id
                .or_else(|| {
                    document
                        .strategies
                        .first()
                        .map(|strategy| strategy.strategy_id)
                })
                .ok_or(EnsureConfigError::DefaultStrategyMissing)?;
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
        let transfer_pool = self.transfer_pool(document, &pools, assigned_range);
        Ok(EnsureResources {
            seed_pool,
            assigned_range,
            range_is_noop,
            job_binding,
            transfer_pool,
        })
    }

    fn transfer_pool(
        &self,
        document: &RealmConfigDocument,
        pools: &[BandPool],
        assigned_range: HandleRange,
    ) -> Option<BandPool> {
        if self.config.target_node_kind != RealmNodeKind::Management
            || self.config.target_node_id == self.config.actor.node_id
            || !coordinator_spans(pools, &self.config.target_node_id).is_empty()
        {
            return None;
        }
        let spans = coordinator_spans(pools, &self.config.actor.node_id);
        let mut consumed = document.placement_handle_ranges.clone();
        consumed.push(assigned_range);
        let child = pool_transfer_slice(&spans, &consumed).and_then(|(start, end)| {
            owned_pools(pools, &self.config.actor.node_id)
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
    }

    fn apply_config_events(
        &self,
        reducer_state: &mut AdminDocumentState,
        document: &RealmConfigDocument,
        fresh: bool,
        node_is_noop: bool,
        resources: EnsureResources,
    ) -> Result<Vec<AdminDocumentEvent>, EnsureConfigError> {
        let mut events = Vec::with_capacity(5);
        if !node_is_noop {
            events.push(apply_node_ensure(
                reducer_state,
                &self.config.actor,
                self.config.target_node_id,
                self.config.target_node_kind.clone(),
            )?);
        }
        if let Some(pool) = resources.seed_pool {
            events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::BandPoolAssigned { pool },
            )?);
        }
        if !resources.range_is_noop {
            events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::HandleRangeGranted {
                    range: resources.assigned_range,
                },
            )?);
        }
        if let Some(binding) = resources.job_binding {
            events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::PlacementBindingAppended { binding },
            )?);
        }
        if let Some(pool) = resources.transfer_pool {
            events.push(reducer_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::BandPoolAssigned { pool },
            )?);
        }
        // Reducer events preserve fresh placement identity and family routing during rebuilds.
        if fresh
            && reducer_state.materialized_family_strategy().is_none()
            && reducer_state.materialized_strategies().is_empty()
        {
            events.extend(crate::realm::create_realm::seed_placement_events(
                reducer_state,
                &self.config.actor,
                document,
            )?);
        }
        Ok(events)
    }

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> EnsureTurn {
        let fresh = document_value.is_none();
        let mut document = match document_value.as_deref() {
            Some(value) => RealmConfigDocument::from_bytes(value)?,
            None if self.config.create_if_missing => {
                let mut document = RealmConfigDocument::new(
                    self.config.actor.realm_id,
                    Vec::new(),
                    self.config.metadata_replication_factor,
                );
                document.description = self.config.realm_description.clone();
                // Seed default placement so no production path ever constructs a
                // strategy-less config (placement resolution needs strategies).
                document.seed_default_placement();
                document
            }
            None => return Err(EnsureConfigError::ConfigMissing),
        };

        if self.config.reject_kind_mismatch {
            let target_node_id = self.config.target_node_id.to_string();
            if document.nodes.iter().any(|node| {
                node.node_id == target_node_id && node.kind != self.config.target_node_kind
            }) {
                return Err(EnsureConfigError::NodeKindMismatch {
                    node_id: self.config.target_node_id,
                });
            }
        }

        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::reducer::decode_reducer_state(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }

        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        overlay_reducer_state(&mut document, &reducer_state, unix_timestamp_millis());

        let node_is_noop = previous_reducer_state.as_ref().is_some_and(|state| {
            node_ensure_noop(
                &document,
                state,
                &self.config.target_node_id,
                &self.config.target_node_kind,
            )
        });
        if !node_is_noop && !self.membership.pending.is_empty() {
            return Err(EnsureConfigError::DeletionPending);
        }
        let resources = self.plan_resources(&document, &reducer_state, fresh)?;
        if node_is_noop
            && resources.range_is_noop
            && resources.job_binding.is_none()
            && resources.seed_pool.is_none()
            && resources.transfer_pool.is_none()
        {
            return Ok(Next::Done(document));
        }

        let admin_events = self.apply_config_events(
            &mut reducer_state,
            &document,
            fresh,
            node_is_noop,
            resources,
        )?;
        overlay_reducer_state(&mut document, &reducer_state, unix_timestamp_millis());

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        for admin_event in admin_events {
            let record = new_identified_record(
                admin_event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(admin_event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.document = Some(document);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> EnsureTurn {
        let output = self.document.take().ok_or(EnsureConfigError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for EnsureConfigOperation {
    type Output = RealmConfigDocument;
    type Error = EnsureConfigError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> EnsureTurn {
        let document = self.document_ref();
        let target = self.admin_target();
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
            (
                aruna_core::keyspaces::GROUP_DELETE_KEYSPACE.to_string(),
                MembershipFence::key(self.config.actor.realm_id),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> EnsureTurn {
        match self.stage {
            Stage::Read => {
                let [document, reducer, membership] = values::<3>(result)?;
                self.membership = MembershipFence::from_value(membership.as_deref())?;
                self.emit_document_write(document, reducer)
            }
            Stage::Write if !self.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Stage::Write | Stage::Delete => {
                done(result)?;
                self.finish()
            }
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

fn apply_node_ensure(
    state: &mut AdminDocumentState,
    actor: &Actor,
    node_id: NodeId,
    kind: RealmNodeKind,
) -> Result<AdminDocumentEvent, AdminDocumentError> {
    let observed = state.clock.clone();
    let event = AdminDocumentEvent {
        event_id: Ulid::generate(),
        target: state.target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: observed.sequence_for(&actor.node_id) + 1,
        observed,
        actor: actor.clone(),
        op: AdminDocumentOperation::ConfigNodeEnsured { node_id, kind },
    };
    state.apply(&event)?;
    Ok(event)
}

pub(crate) fn overlay_reducer_state(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentState,
    now_ms: u64,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = parse_config_node(path) {
            remove_config_node(config, &node_id);
        }
    }

    for node_id in reducer_state.removed_config_nodes() {
        remove_config_node(config, &node_id);
    }

    for (node_id, kind) in reducer_state.materialized_config_nodes() {
        let path = config_node_path(&node_id);
        if reducer_state.conflicts.contains_key(&path) {
            remove_config_node(config, &node_id);
            continue;
        }
        config.ensure_node(node_id, kind);
    }

    overlay_placement(config, reducer_state, now_ms);
}

fn node_ensure_noop(
    document: &RealmConfigDocument,
    reducer_state: &AdminDocumentState,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let path = config_node_path(node_id);
    !reducer_state.conflicts.contains_key(&path)
        && reducer_state
            .materialized_config_nodes()
            .get(node_id)
            .is_some_and(|materialized_kind| materialized_kind == kind)
        && config_node_kind(document, node_id, kind)
}

fn config_node_kind(
    document: &RealmConfigDocument,
    node_id: &NodeId,
    kind: &RealmNodeKind,
) -> bool {
    let node_id = node_id.to_string();
    let mut matches = document.nodes.iter().filter(|node| node.node_id == node_id);
    matches.next().is_some_and(|node| node.kind == *kind) && matches.all(|node| node.kind == *kind)
}

fn remove_config_node(config: &mut RealmConfigDocument, node_id: &NodeId) {
    let node_id = node_id.to_string();
    config.nodes.retain(|node| node.node_id != node_id);
}

#[cfg(test)]
mod pure_tests {
    use aruna_core::UserId;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentTarget,
    };
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{
        DOCUMENT_CONFLICT_KEYSPACE, DOCUMENT_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE,
        SYNC_OUTBOX_KEYSPACE,
    };
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::reducer::{
        AdminConflict, AdminConflictValue, AdminDocumentState, CONFIG_STRATEGY_PATH,
    };
    use aruna_core::storage_entries::reducer_conflict_key;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::structs::placement::record::{
        BandPool, BindingScope, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_BANDS,
        HANDLE_RANGE_SIZE, HandleRange, NodePlacementEntry, PlacementOverride, PlacementStrategy,
        StrategyBinding, band_start, coordinator_spans,
    };
    use aruna_core::types::{Key, KeySpace, Value};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    use super::{
        EnsureConfigError, EnsureConfigOperation, EnsureConfigParams, config_node_path,
        overlay_reducer_state,
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

    fn config(actor: Actor, factor: u32) -> EnsureConfigParams {
        EnsureConfigParams {
            target_node_id: actor.node_id,
            target_node_kind: RealmNodeKind::Management,
            actor,
            metadata_replication_factor: factor,
            realm_description: "Ensured Realm".to_string(),
            create_if_missing: true,
            reject_kind_mismatch: false,
        }
    }

    fn conflict(path: &str) -> AdminConflict {
        let value = |seed: u8, value: &str| AdminConflictValue {
            value: Some(value.to_string()),
            dot: AdminDocumentDot {
                event_id: Ulid::from_bytes([seed; 16]),
                origin_node_id: node(seed),
                origin_seq: 1,
            },
        };
        AdminConflict {
            path: path.to_string(),
            values: vec![value(3, "server"), value(4, "management")],
        }
    }

    fn batch_write(turn: Next<RealmConfigDocument>) -> Vec<(KeySpace, Key, Value)> {
        match turn {
            Next::Call(IoCall::Write(writes)) => writes,
            _ => panic!("expected one write"),
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
    fn writes_missing_config() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let actor = actor(1, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let path = config_node_path(&actor.node_id);
        let mut previous_state = AdminDocumentState::new(target.clone());
        for seed in [3, 4] {
            previous_state.clock.advance(node(seed), 1);
        }
        previous_state
            .conflicts
            .insert(path.clone(), conflict(&path));

        let mut operation = EnsureConfigOperation::new(config(actor.clone(), 7));
        let writes = batch_write(
            operation
                .emit_document_write(
                    None,
                    Some(postcard::to_allocvec(&previous_state).unwrap().into()),
                )
                .unwrap(),
        );

        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state: AdminDocumentState =
            postcard::from_bytes(write_value(&writes, DOCUMENT_STATE_KEYSPACE)).unwrap();
        let outbox: DocumentOutboxRecord =
            postcard::from_bytes(write_value(&writes, SYNC_OUTBOX_KEYSPACE)).unwrap();
        assert_eq!(stored.metadata_replication.default_replication_factor, 7);
        assert_eq!(stored.description, "Ensured Realm");
        assert!(stored.has_node(actor.node_id));
        let default_strategy = stored
            .default_strategy_id
            .and_then(|strategy_id| stored.strategy(&strategy_id))
            .expect("create-if-missing seeds a default placement strategy");
        assert_eq!(default_strategy.replica_count, Some(7));
        assert_eq!(
            state.materialized_config_nodes()[&actor.node_id],
            RealmNodeKind::Management
        );
        assert_eq!(outbox.target, DocumentTarget::RealmConfig { realm_id });
        assert!(matches!(
            &outbox.event,
            DocumentOutboxEvent::AdminOperation { event, .. }
                if event.target == target
                    && matches!(event.op, AdminDocumentOperation::ConfigNodeEnsured { .. })
        ));

        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(vec![(
                DOCUMENT_CONFLICT_KEYSPACE.to_string(),
                reducer_conflict_key(&target, &path),
            )])))
        );
    }

    #[test]
    fn fresh_seeds_bindings() {
        // A fresh bootstrap must materialize the placement identity the create
        // path does: whatever only the document carries a rebuild loses.
        let realm_id = RealmId::from_bytes([9; 32]);
        let mut operation = EnsureConfigOperation::new(config(actor(1, realm_id), 3));

        let writes = batch_write(operation.emit_document_write(None, None).unwrap());

        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state: AdminDocumentState =
            postcard::from_bytes(write_value(&writes, DOCUMENT_STATE_KEYSPACE)).unwrap();

        let bindings = state.materialized_strategy_bindings();
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
        let mut operation = EnsureConfigOperation::new(EnsureConfigParams {
            target_node_id: target,
            target_node_kind: kind,
            ..config(actor.clone(), 3)
        });
        let writes = batch_write(
            operation
                .emit_document_write(Some(document.to_bytes(actor).unwrap().into()), None)
                .unwrap(),
        );
        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        let state = write_value(&writes, DOCUMENT_STATE_KEYSPACE).to_vec();
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
        let mut operation = EnsureConfigOperation::new(EnsureConfigParams {
            target_node_id: joiner_a,
            target_node_kind: RealmNodeKind::Server,
            ..config(actor_a.clone(), 3)
        });
        let turn = operation
            .emit_document_write(
                Some(merged.to_bytes(&actor_a).unwrap().into()),
                Some(state_a.into()),
            )
            .unwrap();
        assert!(matches!(turn, Next::Done(_)));
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
        let mut operation = EnsureConfigOperation::new(EnsureConfigParams {
            create_if_missing: false,
            ..config(actor_a.clone(), 3)
        });
        let error = operation
            .emit_document_write(Some(document.to_bytes(&actor_a).unwrap().into()), None)
            .unwrap_err();
        assert_eq!(
            error,
            EnsureConfigError::CoordinatorPoolMissing {
                node_id: actor_a.node_id
            }
        );
    }

    #[test]
    fn same_node_idempotent() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let actor = actor(9, realm_id);
        let mut document = pooled_document(realm_id, &[(1, actor.clone(), 0, HANDLE_BANDS)]);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut operation = EnsureConfigOperation::new(config(actor.clone(), 3));
        let writes = batch_write(
            operation
                .emit_document_write(Some(document.to_bytes(&actor).unwrap().into()), None)
                .unwrap(),
        );
        let stored =
            RealmConfigDocument::from_bytes(write_value(&writes, REALM_CONFIG_KEYSPACE)).unwrap();
        assert_eq!(stored.nodes.len(), 1);
        assert!(stored.has_node(actor.node_id));
    }

    #[test]
    fn repeated_ensure_quiet() {
        let realm_id = RealmId::from_bytes([10; 32]);
        let actor = actor(10, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut previous_state = AdminDocumentState::new(target.clone());
        previous_state
            .apply(&AdminDocumentEvent {
                event_id: Ulid::from_bytes([10; 16]),
                target,
                origin_node_id: actor.node_id,
                origin_seq: 1,
                observed: AdminDocumentClock::default(),
                actor: actor.clone(),
                op: AdminDocumentOperation::ConfigNodeEnsured {
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
                op: AdminDocumentOperation::HandleRangeGranted { range },
            })
            .unwrap();
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.placement_handle_ranges.push(range);
        // The reserved JobControl binding already exists: nothing to append.
        document.placement_bindings.push(
            aruna_core::structs::placement::record::PlacementBinding {
                handle: aruna_core::structured_id::PlacementHandle::new(range.start).unwrap(),
                scope: aruna_core::structs::placement::record::PlacementScope::Realm(realm_id),
                document_class: DocumentClass::JobControl,
                strategy_id: Ulid::from_bytes([12; 16]),
                allocator_range_id: Some(range.range_id),
                allocated_by: Some(actor.node_id),
                allocated_at_ms: Some(1),
            },
        );

        let mut operation = EnsureConfigOperation::new(config(actor.clone(), 3));
        let turn = operation
            .emit_document_write(
                Some(document.to_bytes(&actor).unwrap().into()),
                Some(postcard::to_allocvec(&previous_state).unwrap().into()),
            )
            .unwrap();

        // Nothing changed: the config answers without a write or an outbox row.
        assert_eq!(turn, Next::Done(document));
    }

    #[test]
    fn scheduled_outbox_finishes() {
        // A changed config comes out with the drain wake after the commit.
        let realm_id = RealmId::from_bytes([11; 32]);
        let mut operation = EnsureConfigOperation::new(config(actor(11, realm_id), 3));
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let written = operation.next(IoResult::Values(vec![None, None, None]));
        assert!(matches!(written, Ok(Next::Call(IoCall::Write(_)))));
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the config with the drain wake");
        };
        assert_eq!(output.description, "Ensured Realm");
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[test]
    fn overlay_materializes_placement() {
        let realm_id = RealmId::from_bytes([21; 32]);
        let actor = actor(21, realm_id);
        let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });

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
            AdminDocumentOperation::NodePlacementSet {
                entry: entry.clone(),
            },
            AdminDocumentOperation::PlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
            AdminDocumentOperation::ConfigStrategySet {
                strategy_id: strategy.strategy_id,
            },
            AdminDocumentOperation::StrategyBindingSet {
                binding: binding.clone(),
            },
            AdminDocumentOperation::PlacementOverrideSet {
                record: record.clone(),
            },
        ] {
            state.apply_operation(&actor, op).unwrap();
        }

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        overlay_reducer_state(&mut config, &state, 0);

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
        let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
        state
            .apply_operation(
                &actor,
                AdminDocumentOperation::ConfigNodeEnsured {
                    node_id: device,
                    kind: RealmNodeKind::User {
                        owner: UserId::nil(realm_id),
                    },
                },
            )
            .unwrap();

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        overlay_reducer_state(&mut config, &state, 0);
        assert!(
            config
                .nodes
                .iter()
                .any(|node| node.node_id == device.to_string())
        );

        state
            .apply_operation(
                &actor,
                AdminDocumentOperation::ConfigNodeRemoved {
                    node_id: device,
                    cutoffs: Vec::new(),
                },
            )
            .unwrap();
        overlay_reducer_state(&mut config, &state, 0);
        assert!(
            config
                .nodes
                .iter()
                .all(|node| node.node_id != device.to_string())
        );
    }

    #[test]
    fn overlay_clears_default() {
        let realm_id = RealmId::from_bytes([22; 32]);
        let actor_a = actor(22, realm_id);
        let actor_b = actor(23, realm_id);
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut state = AdminDocumentState::new(target.clone());
        let prior_default = Ulid::from_bytes([5; 16]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.default_strategy_id = Some(prior_default);

        overlay_reducer_state(&mut config, &state, 0);
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
                    op: AdminDocumentOperation::ConfigStrategySet { strategy_id },
                })
                .unwrap();
        }

        assert!(state.conflicts.contains_key(CONFIG_STRATEGY_PATH));
        assert_eq!(state.materialized_default_strategy(), None);

        overlay_reducer_state(&mut config, &state, 0);
        assert_eq!(config.default_strategy_id, None);
    }

    #[test]
    fn rejects_kind_mismatch() {
        let realm_id = RealmId::from_bytes([8; 32]);
        let actor = actor(8, realm_id);
        let target_node_id = node(7);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(target_node_id, RealmNodeKind::Management);

        let mut operation = EnsureConfigOperation::new(EnsureConfigParams {
            target_node_id,
            target_node_kind: RealmNodeKind::Server,
            reject_kind_mismatch: true,
            ..config(actor.clone(), 3)
        });

        let error = operation
            .emit_document_write(Some(document.to_bytes(&actor).unwrap().into()), None)
            .unwrap_err();

        assert_eq!(
            error,
            EnsureConfigError::NodeKindMismatch {
                node_id: target_node_id
            }
        );
    }
}
