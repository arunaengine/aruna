use aruna_core::NodeId;
use aruna_core::admin_document_reducer::{
    AdminDocumentReducerError, AdminDocumentReducerState,
    overlay_realm_config_placement_reducer_materialization,
};
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE, METADATA_INDEX_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::MetadataCreateEventRecord;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    admin_document_conflict_write_entries, admin_document_reducer_state_key,
    admin_document_reducer_state_write_entry, metadata_pending_projection_target,
    stale_admin_document_conflict_delete_entries,
};
use aruna_core::structs::{
    Actor, BindingError, BindingScope, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, HANDLE_RANGE_SIZE,
    HANDLE_SPACE_END, HandleRange, MetadataRegistryRecord, NodePlacementEntry, PlacementBinding,
    PlacementOverride, PlacementRef, PlacementScope, PlacementStrategy, RealmConfigDocument,
    StrategyBinding,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::placement::placement_ref_for_target;
use crate::sync_placement::schedule_placement_revalidation_effect;

const STRATEGY_REFERENCE_SCAN_PAGE_SIZE: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealmPlacementMutation {
    UpsertNode(NodePlacementEntry),
    RemoveNode(NodeId),
    UpsertStrategy(PlacementStrategy),
    RemoveStrategy(Ulid),
    SetDefaultStrategy(Ulid),
    SetBinding(StrategyBinding),
    RemoveBinding(BindingScope),
    SetOverride(PlacementOverride),
    RemoveOverride(Vec<u8>),
    AppendPlacementBinding(PlacementBinding),
    GrantHandleRange(HandleRange),
}

/// Builds the next disjoint handle range for `owner` from the realm's current
/// grant set. The range starts one past the highest end already carved out, so
/// sequential grants never overlap; concurrent grants from different
/// coordinators are caught by the overlap conflict after replication. `None` ⇒
/// the 20-bit handle space is fully granted.
pub fn next_handle_range(config: &RealmConfigDocument, owner: NodeId) -> Option<HandleRange> {
    let start = config.handle_range_directory().next_grantable_start();
    if start >= HANDLE_SPACE_END {
        return None;
    }
    let end = start.saturating_add(HANDLE_RANGE_SIZE).min(HANDLE_SPACE_END);
    Some(HandleRange {
        range_id: Ulid::r#gen(),
        owner,
        start,
        end,
    })
}

impl RealmPlacementMutation {
    fn admin_operation(&self) -> AdminDocumentOperation {
        match self {
            Self::UpsertNode(entry) => AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: entry.clone(),
            },
            Self::RemoveNode(node_id) => {
                AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id: *node_id }
            }
            Self::UpsertStrategy(strategy) => {
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: strategy.clone(),
                }
            }
            Self::RemoveStrategy(strategy_id) => {
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved {
                    strategy_id: *strategy_id,
                }
            }
            Self::SetDefaultStrategy(strategy_id) => {
                AdminDocumentOperation::RealmConfigDefaultStrategySet {
                    strategy_id: *strategy_id,
                }
            }
            Self::SetBinding(binding) => AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
            Self::RemoveBinding(scope) => {
                AdminDocumentOperation::RealmConfigStrategyBindingRemoved {
                    scope: scope.clone(),
                }
            }
            Self::SetOverride(record) => AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
            Self::RemoveOverride(subject) => {
                AdminDocumentOperation::RealmConfigPlacementOverrideRemoved {
                    subject: subject.clone(),
                }
            }
            Self::AppendPlacementBinding(binding) => {
                AdminDocumentOperation::RealmConfigPlacementBindingAppended {
                    binding: binding.clone(),
                }
            }
            Self::GrantHandleRange(range) => {
                AdminDocumentOperation::RealmConfigHandleRangeGranted { range: *range }
            }
        }
    }

    fn validate(&self, document: &RealmConfigDocument) -> Result<(), MutateRealmPlacementError> {
        match self {
            Self::UpsertNode(entry) if entry.draining => {
                let unchanged = if let Some(current) = document.placement_entry(entry.node_id) {
                    entry.effective_location() == current.effective_location()
                        && entry.weight == current.weight
                        && entry.full == current.full
                        && entry.labels == current.labels
                } else {
                    entry.effective_location() == DEFAULT_LOCATION
                        && entry.weight == DEFAULT_NODE_WEIGHT
                        && !entry.full
                        && entry.labels.is_empty()
                };
                if unchanged {
                    Ok(())
                } else {
                    Err(MutateRealmPlacementError::InvalidInput(
                        "draining freezes placement attributes until the node un-drains or is removed"
                            .to_string(),
                    ))
                }
            }
            Self::UpsertStrategy(strategy) if strategy.replica_count == Some(0) => {
                Err(MutateRealmPlacementError::InvalidInput(
                    "placement strategy replica_count must not be zero".to_string(),
                ))
            }
            Self::SetDefaultStrategy(strategy_id) => {
                require_strategy(document, strategy_id, "default strategy")
            }
            Self::SetBinding(binding) => {
                require_strategy(document, &binding.strategy_id, "binding")
            }
            Self::AppendPlacementBinding(binding) => {
                require_strategy(document, &binding.strategy_id, "placement binding")?;
                if matches!(
                    binding.scope,
                    PlacementScope::Realm(binding_realm_id)
                        if binding_realm_id != document.realm_id
                ) {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "placement binding realm does not match the realm config".to_string(),
                    ));
                }
                match document.binding_directory().resolve(binding.handle) {
                    Ok(existing) if existing != binding.tuple() => {
                        Err(MutateRealmPlacementError::InvalidInput(format!(
                            "placement binding handle {} is already bound to a different tuple",
                            binding.handle.get()
                        )))
                    }
                    Err(BindingError::Conflicted(_)) => {
                        Err(MutateRealmPlacementError::InvalidInput(format!(
                            "placement binding handle {} is conflicted",
                            binding.handle.get()
                        )))
                    }
                    Ok(_) | Err(_) => Ok(()),
                }
            }
            Self::GrantHandleRange(range) => {
                if !range.is_well_formed() {
                    return Err(MutateRealmPlacementError::InvalidInput(format!(
                        "handle range [{}, {}) is not a valid sub-interval of the handle space",
                        range.start, range.end
                    )));
                }
                // Overlaps are NOT rejected here: a concurrent grant from another
                // coordinator arrives via replication and must converge to a
                // fail-closed conflict, not be silently dropped. Only a same-id
                // re-grant with divergent bounds is a local error.
                if document
                    .placement_handle_ranges
                    .iter()
                    .any(|existing| existing.range_id == range.range_id && existing != range)
                {
                    return Err(MutateRealmPlacementError::InvalidInput(format!(
                        "handle range id {} is already granted with different bounds",
                        range.range_id
                    )));
                }
                Ok(())
            }
            Self::SetOverride(record) => match &record.strategy_id {
                Some(strategy_id) => require_strategy(document, strategy_id, "override"),
                None => Ok(()),
            },
            Self::RemoveStrategy(strategy_id) => {
                let referenced = document.default_strategy_id == Some(*strategy_id)
                    || document
                        .strategy_bindings
                        .iter()
                        .any(|binding| binding.strategy_id == *strategy_id)
                    || document
                        .placement_bindings
                        .iter()
                        .any(|binding| binding.strategy_id == *strategy_id)
                    || document
                        .placement_overrides
                        .iter()
                        .any(|record| record.strategy_id == Some(*strategy_id));
                if referenced {
                    Err(MutateRealmPlacementError::StrategyReferenced {
                        strategy_id: *strategy_id,
                    })
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

fn require_strategy(
    document: &RealmConfigDocument,
    strategy_id: &Ulid,
    reference: &str,
) -> Result<(), MutateRealmPlacementError> {
    if document.strategy(strategy_id).is_none() {
        return Err(MutateRealmPlacementError::InvalidInput(format!(
            "{reference} references missing strategy {strategy_id}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutateRealmPlacementConfig {
    pub actor: Actor,
    pub mutation: RealmPlacementMutation,
}

#[derive(Debug, PartialEq)]
pub struct MutateRealmPlacementOperation {
    config: MutateRealmPlacementConfig,
    txn_id: Option<TxnId>,
    state: MutateRealmPlacementState,
    output: Option<Result<RealmConfigDocument, MutateRealmPlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
struct StrategyRemovalCheck {
    document_value: Value,
    reducer_state_value: Option<Value>,
    strategy_id: Ulid,
}

#[derive(Debug, Clone, PartialEq)]
enum MutateRealmPlacementState {
    Init,
    StartTransaction,
    ReadCurrent,
    ReadRegistryReferences {
        check: StrategyRemovalCheck,
    },
    ReadPendingReferences {
        check: StrategyRemovalCheck,
    },
    ReadPendingEvents {
        check: StrategyRemovalCheck,
        next_start_after: Option<Key>,
    },
    WriteDocumentAndAdminState {
        document: RealmConfigDocument,
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteStaleAdminConflicts {
        document: RealmConfigDocument,
    },
    CommitTransaction {
        document: RealmConfigDocument,
    },
    ScheduleDocumentSyncOutboxDrain,
    SchedulePlacementRevalidation,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum MutateRealmPlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("realm config document missing")]
    RealmConfigNotFound,
    #[error("invalid placement mutation: {0}")]
    InvalidInput(String),
    #[error(
        "disjoint holder transition for strategy {strategy_id} shard {shard}: at least one current holder must remain until new holders verify"
    )]
    DisjointHolderTransition { strategy_id: Ulid, shard: u32 },
    #[error("placement leaves strategy {strategy_id} shard {shard} with no eligible holders")]
    EmptyShardHolders { strategy_id: Ulid, shard: u32 },
    #[error("placement strategy {strategy_id} is currently referenced")]
    StrategyReferenced { strategy_id: Ulid },
    #[error("realm handle space is fully granted: no disjoint range remains to grant")]
    RealmHandleSpaceExhausted,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl MutateRealmPlacementOperation {
    pub fn new(config: MutateRealmPlacementConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: MutateRealmPlacementState::Init,
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
        self.state = MutateRealmPlacementState::ReadCurrent;
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
    ) -> Result<Effects, MutateRealmPlacementError> {
        let Some(txn_id) = self.txn_id else {
            return Err(MutateRealmPlacementError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(MutateRealmPlacementError::RealmConfigNotFound);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        self.config.mutation.validate(&document)?;

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
        let admin_event = reducer_state
            .apply_operation(&self.config.actor, self.config.mutation.admin_operation())?;
        let pre_document = document.clone();
        overlay_realm_config_placement_reducer_materialization(&mut document, &reducer_state);

        if let Some((node_id, placement)) =
            crate::placement::first_draining_holder_set_change(&pre_document, &document)
        {
            return Err(MutateRealmPlacementError::InvalidInput(format!(
                "placement change alters drain-time holder set for node {node_id}, strategy {} shard {}",
                placement.strategy_id, placement.shard
            )));
        }
        if let Some(placement) =
            crate::placement::first_disjoint_shard_transition(&pre_document, &document)
        {
            return Err(MutateRealmPlacementError::DisjointHolderTransition {
                strategy_id: placement.strategy_id,
                shard: placement.shard,
            });
        }
        if let Some(placement) = crate::placement::first_empty_referenced_shard(&document) {
            return Err(MutateRealmPlacementError::EmptyShardHolders {
                strategy_id: placement.strategy_id,
                shard: placement.shard,
            });
        }

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
        let record = new_outbox_record_with_id(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentSyncOutboxEvent::AdminOperation {
                event: Box::new(admin_event),
            },
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(admin_document_conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = MutateRealmPlacementState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_reference_check_or_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, MutateRealmPlacementError> {
        let Some(document_value) = document_value else {
            return Err(MutateRealmPlacementError::RealmConfigNotFound);
        };
        let document = RealmConfigDocument::from_bytes(&document_value)?;
        self.config.mutation.validate(&document)?;
        let strategy_id = match &self.config.mutation {
            RealmPlacementMutation::RemoveStrategy(strategy_id) => *strategy_id,
            _ => {
                return self.emit_write_document_and_admin_state(
                    Some(document_value),
                    reducer_state_value,
                );
            }
        };
        let check = StrategyRemovalCheck {
            document_value,
            reducer_state_value,
            strategy_id,
        };
        Ok(self.emit_registry_reference_scan(check, None))
    }

    fn emit_registry_reference_scan(
        &mut self,
        check: StrategyRemovalCheck,
        start_after: Option<Key>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutateRealmPlacementError::MissingTransaction);
        };
        self.state = MutateRealmPlacementState::ReadRegistryReferences { check };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: STRATEGY_REFERENCE_SCAN_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    fn emit_pending_reference_scan(
        &mut self,
        check: StrategyRemovalCheck,
        start_after: Option<Key>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutateRealmPlacementError::MissingTransaction);
        };
        self.state = MutateRealmPlacementState::ReadPendingReferences { check };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: STRATEGY_REFERENCE_SCAN_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    fn reference_matches(&self, record: &MetadataRegistryRecord, strategy_id: Ulid) -> bool {
        record.realm_id == self.config.actor.realm_id
            && record.placement != PlacementRef::NIL
            && record.placement.strategy_id == strategy_id
    }

    fn emit_write_after_reference_check(&mut self, check: StrategyRemovalCheck) -> Effects {
        match self.emit_write_document_and_admin_state(
            Some(check.document_value),
            check.reducer_state_value,
        ) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutateRealmPlacementError::MissingTransaction);
        };
        self.state = MutateRealmPlacementState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: MutateRealmPlacementError) -> Effects {
        let cleanup = self.abort();
        self.state = MutateRealmPlacementState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(MutateRealmPlacementError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for MutateRealmPlacementOperation {
    type Output = RealmConfigDocument;
    type Error = MutateRealmPlacementError;

    fn start(&mut self) -> Effects {
        self.state = MutateRealmPlacementState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            MutateRealmPlacementState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            MutateRealmPlacementState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_reference_check_or_write(
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
            MutateRealmPlacementState::ReadRegistryReferences { check } => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (_, value) in values {
                        let record: MetadataRegistryRecord = match postcard::from_bytes(&value) {
                            Ok(record) => record,
                            Err(error) => return self.fail(ConversionError::from(error).into()),
                        };
                        if self.reference_matches(&record, check.strategy_id) {
                            return self.fail(MutateRealmPlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        }
                    }
                    match next_start_after {
                        Some(start_after) => {
                            self.emit_registry_reference_scan(check, Some(start_after))
                        }
                        None => self.emit_pending_reference_scan(check, None),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata registry scan result", format!("{other:?}"))
                }
            },
            MutateRealmPlacementState::ReadPendingReferences { check } => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    if values.is_empty() {
                        return match next_start_after {
                            Some(start_after) => {
                                self.emit_pending_reference_scan(check, Some(start_after))
                            }
                            None => self.emit_write_after_reference_check(check),
                        };
                    }
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(MutateRealmPlacementError::MissingTransaction);
                    };
                    self.state = MutateRealmPlacementState::ReadPendingEvents {
                        check,
                        next_start_after,
                    };
                    smallvec![Effect::Storage(StorageEffect::BatchRead {
                        reads: values
                            .into_iter()
                            .map(|(key, _)| (METADATA_EVENT_LOG_KEYSPACE.to_string(), key))
                            .collect(),
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("pending projection scan result", format!("{other:?}"))
                }
            },
            MutateRealmPlacementState::ReadPendingEvents {
                check,
                next_start_after,
            } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    for (key, value) in values {
                        let Some(value) = value else {
                            return self.fail(MutateRealmPlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        };
                        let event: MetadataCreateEventRecord = match postcard::from_bytes(&value) {
                            Ok(event) => event,
                            Err(_) => {
                                return self.fail(MutateRealmPlacementError::StrategyReferenced {
                                    strategy_id: check.strategy_id,
                                });
                            }
                        };
                        let valid_target = metadata_pending_projection_target(key.as_ref())
                            .is_some_and(|(document_id, event_id)| {
                                event.record.document_id == document_id
                                    && event.event_id == event_id
                            });
                        if !valid_target || self.reference_matches(&event.record, check.strategy_id)
                        {
                            return self.fail(MutateRealmPlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        }
                    }
                    match next_start_after {
                        Some(start_after) => {
                            self.emit_pending_reference_scan(check, Some(start_after))
                        }
                        None => self.emit_write_after_reference_check(check),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("pending create event reads", format!("{other:?}")),
            },
            MutateRealmPlacementState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(MutateRealmPlacementError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state =
                            MutateRealmPlacementState::DeleteStaleAdminConflicts { document };
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
            MutateRealmPlacementState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            MutateRealmPlacementState::CommitTransaction { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain;
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutateRealmPlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.config.actor.realm_id,
                        self.config.actor.node_id,
                    )]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = MutateRealmPlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.config.actor.realm_id,
                        self.config.actor.node_id,
                    )]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            MutateRealmPlacementState::SchedulePlacementRevalidation => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutateRealmPlacementState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule placement revalidation after realm placement mutation");
                    self.state = MutateRealmPlacementState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "placement revalidation timer schedule",
                    format!("{other:?}"),
                ),
            },
            MutateRealmPlacementState::Finish
            | MutateRealmPlacementState::Error
            | MutateRealmPlacementState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            MutateRealmPlacementState::Finish | MutateRealmPlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("realm placement mutation operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Drives a realm placement mutation, then — when it drains the local node —
/// flushes the local outbox synchronously (drain fence) so records this node
/// accepted before its holdership loss reach the shard topics right away, while
/// it is still a member, rather than waiting for the asynchronous drain timer. The
/// flush is best-effort: `classify_deferred_record` keeps any leftovers deliverable on the
/// retryable drain regardless, so a flush failure never strands a record.
pub async fn drive_realm_placement_mutation(
    config: MutateRealmPlacementConfig,
    context: &crate::driver::DriverContext,
) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
    let drains_node = matches!(
        &config.mutation,
        RealmPlacementMutation::UpsertNode(entry)
            if entry.draining
                && context.net_handle.as_ref().map(|net| net.node_id()) == Some(entry.node_id)
    );
    let outcome = crate::driver::drive(MutateRealmPlacementOperation::new(config), context).await;
    if outcome.is_ok() && drains_node && context.net_handle.is_some() {
        crate::task_incoming::drive_document_sync_outbox_drain(std::sync::Arc::new(
            context.clone(),
        ))
        .await;
    }
    outcome
}

/// Grants the next disjoint handle range to `owner`: reads the current realm
/// config, computes the next non-overlapping range from its grant cursor, and
/// replicates the grant as a Management-only admin op. This is the grant
/// MECHANISM onboarding (#342) drives; it does not itself decide when to grant.
pub async fn grant_next_handle_range(
    actor: Actor,
    owner: NodeId,
    context: &crate::driver::DriverContext,
) -> Result<HandleRange, MutateRealmPlacementError> {
    let document_target = DocumentSyncTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: document_target.storage_keyspace().to_string(),
            key: document_target.storage_key(),
            txn_id: None,
        })
        .await;
    let config = match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes)?,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        _ => return Err(MutateRealmPlacementError::RealmConfigNotFound),
    };
    let range = next_handle_range(&config, owner)
        .ok_or(MutateRealmPlacementError::RealmHandleSpaceExhausted)?;
    crate::driver::drive(
        MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor,
            mutation: RealmPlacementMutation::GrantHandleRange(range),
        }),
        context,
    )
    .await?;
    Ok(range)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aruna_core::admin_documents::AdminDocumentEvent;
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::events::StorageEvent;
    use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
    use aruna_core::storage_entries::{
        metadata_create_event_and_pending_projection_write_entries, metadata_registry_write_entries,
    };
    use aruna_core::structs::{
        AffinityEffect, AffinityRule, DEFAULT_NODE_WEIGHT, DEFAULT_SHARD_COUNT, DocumentClass,
        FIRST_HANDLE, HANDLE_SPACE_END, HandleRange, LabelMatch, MetadataRegistryRecord,
        PlacementBinding, PlacementRef, PlacementScope, RealmId, RealmNodeKind,
    };
    use aruna_core::structured_id::PlacementHandle;
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::types::UserId;
    use tempfile::tempdir;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn actor(realm_id: RealmId) -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
            realm_id,
        }
    }

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    async fn seed_config(context: &DriverContext, actor: &Actor) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.seed_default_placement();
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        document
    }

    async fn mutate(
        context: &DriverContext,
        actor: &Actor,
        mutation: RealmPlacementMutation,
    ) -> Result<RealmConfigDocument, MutateRealmPlacementError> {
        drive(
            MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
                actor: actor.clone(),
                mutation,
            }),
            context,
        )
        .await
    }

    fn strategy(strategy_id: Ulid) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id,
            name: "hot".to_string(),
            replica_count: Some(2),
            distinct_locations: true,
            affinity: Vec::new(),
            shard_count: 64,
        }
    }

    fn create_event(
        actor: &Actor,
        strategy_id: Ulid,
        document_seed: u8,
    ) -> MetadataCreateEventRecord {
        let document_id = Ulid::from_bytes([document_seed; 16]);
        let event_id = Ulid::from_bytes([document_seed.wrapping_add(1); 16]);
        MetadataCreateEventRecord {
            event_id,
            record: MetadataRegistryRecord {
                realm_id: actor.realm_id,
                group_id: Ulid::from_bytes([document_seed.wrapping_add(2); 16]),
                document_id,
                document_path: "datasets/referenced".to_string(),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: true,
                permission_path: "/referenced".to_string(),
                placement: PlacementRef {
                    strategy_id,
                    epoch: 0,
                    shard: 1,
                },
                holder_node_ids: vec![actor.node_id],
                created_at_ms: 1,
                updated_at_ms: 1,
                last_event_id: event_id,
            },
            user_id: actor.user_id,
            node_id: actor.node_id,
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Referenced".to_string(),
                description: "Strategy reference".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
            occurred_at_ms: 1,
        }
    }

    async fn write_entries(context: &DriverContext, writes: Vec<(String, Key, Value)>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn strategy_default_binding_and_override_lifecycle() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3; 32]);
        let actor = actor(realm_id);
        let initial = seed_config(&context, &actor).await;
        let initial_default = initial.default_strategy_id.unwrap();
        let strategy_id = Ulid::from_bytes([8; 16]);
        let scope = BindingScope::Class(DocumentClass::Metadata);
        let subject = vec![0xab, 0xcd];

        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetDefaultStrategy(strategy_id),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetBinding(StrategyBinding {
                scope: scope.clone(),
                strategy_id,
            }),
        )
        .await
        .unwrap();
        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: subject.clone(),
                pinned: vec![node(2)],
                excluded: vec![node(3)],
                strategy_id: Some(strategy_id),
            }),
        )
        .await
        .unwrap();

        assert_eq!(stored.default_strategy_id, Some(strategy_id));
        assert!(stored.strategy(&strategy_id).is_some());
        assert!(
            stored
                .strategy_bindings
                .iter()
                .any(|binding| { binding.scope == scope && binding.strategy_id == strategy_id })
        );
        assert!(stored.placement_overrides.iter().any(|record| {
            record.subject == subject && record.strategy_id == Some(strategy_id)
        }));

        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveOverride(subject),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveBinding(scope.clone()),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::SetDefaultStrategy(initial_default),
        )
        .await
        .unwrap();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id),
        )
        .await
        .unwrap();

        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        assert!(stored.strategy(&strategy_id).is_none());
        assert_eq!(stored.default_strategy_id, Some(initial_default));
        assert!(
            !stored
                .strategy_bindings
                .iter()
                .any(|binding| binding.scope == scope)
        );
        assert!(
            !stored
                .placement_overrides
                .iter()
                .any(|record| record.subject == vec![0xab, 0xcd])
        );
    }

    #[tokio::test]
    async fn node_placement_lifecycle() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([12; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: "eu-west".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        };

        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertNode(entry.clone()),
        )
        .await
        .unwrap();
        assert_eq!(stored.placement_entry(entry.node_id), Some(&entry));

        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveNode(entry.node_id),
        )
        .await
        .unwrap();
        assert!(stored.placement_entry(entry.node_id).is_none());
    }

    #[tokio::test]
    async fn node_placement_rejects_reserved_kind_label() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([13; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: String::new(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            labels: BTreeMap::from([(
                aruna_core::structs::KIND_LABEL_KEY.to_string(),
                "Server".to_string(),
            )]),
        };

        assert!(matches!(
            mutate(&context, &actor, RealmPlacementMutation::UpsertNode(entry)).await,
            Err(MutateRealmPlacementError::AdminDocumentReducerError(
                AdminDocumentReducerError::ReservedPlacementLabel
            ))
        ));
    }

    #[tokio::test]
    async fn local_validation_rejects_zero_and_dangling_references() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let missing = Ulid::from_bytes([9; 16]);

        let mut zero = strategy(missing);
        zero.replica_count = Some(0);
        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::UpsertStrategy(zero)
            )
            .await,
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("zero")
        ));

        for mutation in [
            RealmPlacementMutation::SetDefaultStrategy(missing),
            RealmPlacementMutation::SetBinding(StrategyBinding {
                scope: BindingScope::Realm,
                strategy_id: missing,
            }),
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: vec![1],
                pinned: Vec::new(),
                excluded: Vec::new(),
                strategy_id: Some(missing),
            }),
        ] {
            assert!(matches!(
                mutate(&context, &actor, mutation).await,
                Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
            ));
        }
    }

    #[tokio::test]
    async fn removing_a_referenced_strategy_is_a_conflict() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([5; 32]);
        let actor = actor(realm_id);
        let document = seed_config(&context, &actor).await;
        let strategy_id = document.default_strategy_id.unwrap();

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveStrategy(strategy_id)
            )
            .await,
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    // Removing a strategy still named by an immutable placement binding is a
    // StrategyReferenced conflict, like the other reference kinds above.
    #[test]
    fn binding_blocks_removal() {
        let realm_id = RealmId::from_bytes([14; 32]);
        let strategy_id = Ulid::from_bytes([14; 16]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.strategies.push(strategy(strategy_id));
        document.placement_bindings.push(PlacementBinding {
            handle: PlacementHandle::new(1).unwrap(),
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        });

        assert_eq!(
            RealmPlacementMutation::RemoveStrategy(strategy_id).validate(&document),
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    #[tokio::test]
    async fn materialized_reference_blocks() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([14; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let strategy_id = Ulid::from_bytes([14; 16]);
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
        )
        .await
        .unwrap();
        let event = create_event(&actor, strategy_id, 31);
        write_entries(
            &context,
            metadata_registry_write_entries(&event.record).unwrap(),
        )
        .await;

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveStrategy(strategy_id)
            )
            .await,
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    fn placement_binding(realm_id: RealmId, handle: u32, strategy_id: Ulid) -> PlacementBinding {
        PlacementBinding {
            handle: PlacementHandle::new(handle).unwrap(),
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::MetadataRegistry,
            strategy_id,
            allocator_range_id: Some(Ulid::from_bytes([44; 16])),
            allocated_by: None,
            allocated_at_ms: None,
        }
    }

    async fn read_reducer_state(
        context: &DriverContext,
        target: &AdminDocumentTarget,
    ) -> AdminDocumentReducerState {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
                key: admin_document_reducer_state_key(target),
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("unexpected reducer state read event: {event:?}");
        };
        aruna_core::admin_document_reducer::decode_admin_document_reducer_state(
            &value.expect("reducer state exists"),
        )
        .expect("reducer state decodes")
    }

    async fn read_outbox_admin_events(context: &DriverContext) -> Vec<AdminDocumentEvent> {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1_024,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            panic!("unexpected outbox iter event: {event:?}");
        };
        values
            .into_iter()
            .filter_map(|(_, value)| {
                let record: DocumentSyncOutboxRecord = postcard::from_bytes(&value).ok()?;
                match record.event {
                    DocumentSyncOutboxEvent::AdminOperation { event } => Some(*event),
                    _ => None,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn append_placement_binding_writes_and_enqueues() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([30; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let strategy_id = Ulid::from_bytes([30; 16]);
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
        )
        .await
        .unwrap();
        let binding = placement_binding(realm_id, 5, strategy_id);

        let stored = mutate(
            &context,
            &actor,
            RealmPlacementMutation::AppendPlacementBinding(binding.clone()),
        )
        .await
        .unwrap();
        assert!(
            stored
                .placement_bindings
                .iter()
                .any(|stored| stored.handle == binding.handle)
        );
        assert_eq!(
            stored.binding_directory().resolve(binding.handle).unwrap(),
            binding.tuple()
        );

        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let state = read_reducer_state(&context, &target).await;
        assert!(
            state
                .materialized_realm_config_placement_bindings()
                .contains_key(&binding.handle)
        );

        let events = read_outbox_admin_events(&context).await;
        assert!(events.iter().any(|event| matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding: appended }
                if appended.handle == binding.handle
        )));
    }

    #[tokio::test]
    async fn grant_writes_range_and_advances_cursor() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([50; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let owner = node(2);

        let range = grant_next_handle_range(actor.clone(), owner, &context)
            .await
            .unwrap();
        assert_eq!(range.owner, owner);
        assert_eq!(range.start, FIRST_HANDLE);
        assert_eq!(range.len(), aruna_core::structs::HANDLE_RANGE_SIZE);

        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        let ranges = stored.handle_range_directory();
        assert_eq!(ranges.granted_to(&owner), vec![range]);
        assert_eq!(ranges.next_grantable_start(), range.end);

        let events = read_outbox_admin_events(&context).await;
        assert!(events.iter().any(|event| matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigHandleRangeGranted { range: granted }
                if granted.range_id == range.range_id
        )));
    }

    #[tokio::test]
    async fn sequential_grants_do_not_overlap() {
        let temp = tempdir().unwrap();
        let context = context(temp.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([51; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;

        let first = grant_next_handle_range(actor.clone(), node(2), &context)
            .await
            .unwrap();
        let second = grant_next_handle_range(actor.clone(), node(3), &context)
            .await
            .unwrap();
        assert!(!first.overlaps(&second));
        assert_eq!(second.start, first.end);

        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        assert_eq!(stored.handle_range_directory().conflicts(), 0);
        assert_eq!(stored.placement_handle_ranges.len(), 2);
    }

    #[test]
    fn realm_space_exhaustion_yields_none() {
        let realm_id = RealmId::from_bytes([52; 32]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.placement_handle_ranges.push(HandleRange {
            range_id: Ulid::r#gen(),
            owner: node(2),
            start: FIRST_HANDLE,
            end: HANDLE_SPACE_END,
        });
        assert!(next_handle_range(&document, node(3)).is_none());
    }

    #[test]
    fn append_placement_binding_requires_strategy() {
        let realm_id = RealmId::from_bytes([31; 32]);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let binding = placement_binding(realm_id, 5, Ulid::from_bytes([9; 16]));
        assert!(matches!(
            RealmPlacementMutation::AppendPlacementBinding(binding).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
        ));
    }

    #[test]
    fn append_placement_binding_rejects_divergent_rebind() {
        let realm_id = RealmId::from_bytes([32; 32]);
        let strategy_a = Ulid::from_bytes([1; 16]);
        let strategy_b = Ulid::from_bytes([2; 16]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.strategies.push(strategy(strategy_a));
        document.strategies.push(strategy(strategy_b));
        document
            .placement_bindings
            .push(placement_binding(realm_id, 5, strategy_a));

        // A same-tuple re-append (differing only in provenance) stays allowed.
        let mut same = placement_binding(realm_id, 5, strategy_a);
        same.allocator_range_id = Some(Ulid::from_bytes([77; 16]));
        assert!(
            RealmPlacementMutation::AppendPlacementBinding(same)
                .validate(&document)
                .is_ok()
        );

        // A divergent tuple on the already-bound handle is rejected.
        let divergent = placement_binding(realm_id, 5, strategy_b);
        assert!(matches!(
            RealmPlacementMutation::AppendPlacementBinding(divergent).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("different tuple")
        ));

        let foreign = placement_binding(RealmId::from_bytes([33; 32]), 6, strategy_a);
        assert!(matches!(
            RealmPlacementMutation::AppendPlacementBinding(foreign).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("does not match")
        ));
    }

    #[tokio::test]
    async fn pending_reference_blocks() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([15; 32]);
        let actor = actor(realm_id);
        seed_config(&context, &actor).await;
        let strategy_id = Ulid::from_bytes([15; 16]);
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(strategy(strategy_id)),
        )
        .await
        .unwrap();
        let event = create_event(&actor, strategy_id, 41);
        write_entries(
            &context,
            metadata_create_event_and_pending_projection_write_entries(&event).unwrap(),
        )
        .await;

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveStrategy(strategy_id)
            )
            .await,
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    #[tokio::test]
    async fn missing_realm_config_is_not_found() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([11; 32]);
        let actor = actor(realm_id);

        assert_eq!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::RemoveOverride(Vec::new())
            )
            .await,
            Err(MutateRealmPlacementError::RealmConfigNotFound)
        );
    }

    #[test]
    fn successful_mutation_schedules_zero_delay_revalidation() {
        let realm_id = RealmId::from_bytes([6; 32]);
        let actor = actor(realm_id);
        let mut operation = MutateRealmPlacementOperation::new(MutateRealmPlacementConfig {
            actor: actor.clone(),
            mutation: RealmPlacementMutation::RemoveOverride(Vec::new()),
        });
        operation.state = MutateRealmPlacementState::ScheduleDocumentSyncOutboxDrain;

        let effects = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainDocumentSyncOutbox,
            after: std::time::Duration::ZERO,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::SyncPlacements {
                    realm_id: scheduled_realm,
                    node_id,
                },
                after,
            })] if *scheduled_realm == realm_id && *node_id == actor.node_id && after.is_zero()
        ));
    }

    async fn seed_placement_config(
        context: &DriverContext,
        actor: &Actor,
        nodes: &[aruna_core::NodeId],
        replica: Option<u32>,
    ) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.seed_default_placement();
        let default_id = document.default_strategy_id.unwrap();
        for strategy in document.strategies.iter_mut() {
            if strategy.strategy_id == default_id {
                strategy.replica_count = replica;
            }
        }
        for node_id in nodes {
            document.ensure_node(*node_id, RealmNodeKind::Server);
        }
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: actor.realm_id,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: document.to_bytes(actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        document
    }

    fn draining_entry(node_id: aruna_core::NodeId) -> NodePlacementEntry {
        NodePlacementEntry {
            node_id,
            location: String::new(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: true,
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn draining_change_rejected() {
        let node_id = node(1);
        // Selection inputs stay frozen on transition and later draining upserts.
        for already_draining in [false, true] {
            let mut document =
                RealmConfigDocument::new(RealmId::from_bytes([24; 32]), Vec::new(), 3);
            let mut current = draining_entry(node_id);
            current.draining = already_draining;
            document.placement_map.push(current);
            let mut changed = draining_entry(node_id);
            changed.weight = 0;

            assert!(matches!(
                RealmPlacementMutation::UpsertNode(changed).validate(&document),
                Err(MutateRealmPlacementError::InvalidInput(reason))
                    if reason.contains("draining freezes")
            ));
        }
    }

    #[test]
    fn unmapped_drain_allowed() {
        let document = RealmConfigDocument::new(RealmId::from_bytes([25; 32]), Vec::new(), 3);
        // Resolver defaults make a draining-only upsert valid for an unmapped node.
        assert!(
            RealmPlacementMutation::UpsertNode(draining_entry(node(1)))
                .validate(&document)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn disjoint_transition_rejected() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([21; 32]);
        let actor = actor(realm_id);
        // Single-replica: every shard has one holder, so draining a node moves its
        // shards to a disjoint holder.
        seed_placement_config(&context, &actor, &[node(1), node(2)], Some(1)).await;

        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::UpsertNode(draining_entry(node(1)))
            )
            .await,
            Err(MutateRealmPlacementError::DisjointHolderTransition { .. })
        ));
    }

    #[tokio::test]
    async fn overlapping_transition_allowed() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([22; 32]);
        let actor = actor(realm_id);
        // Replica two with two nodes: every shard holds both, so draining one
        // leaves the other as an overlapping holder.
        seed_placement_config(&context, &actor, &[node(1), node(2)], Some(2)).await;

        let result = mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertNode(draining_entry(node(1))),
        )
        .await;
        assert!(
            result.is_ok(),
            "overlap-preserving change rejected: {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_holder_config_rejected() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([23; 32]);
        let actor = actor(realm_id);
        let document = seed_placement_config(&context, &actor, &[node(1), node(2)], Some(2)).await;
        let default_id = document.default_strategy_id.unwrap();
        // Refilter the referenced default strategy onto a label no node carries:
        // its shards resolve to zero holders while both nodes stay usable.
        let filtered = PlacementStrategy {
            strategy_id: default_id,
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: vec![AffinityRule {
                matcher: LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: AffinityEffect::Filter,
            }],
            shard_count: DEFAULT_SHARD_COUNT,
        };

        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::UpsertStrategy(filtered.clone())
            )
            .await,
            Err(MutateRealmPlacementError::EmptyShardHolders { .. })
        ));

        let strategy_id = Ulid::from_bytes([23; 16]);
        let mut unreferenced = filtered;
        unreferenced.strategy_id = strategy_id;
        unreferenced.name = "binding-only".to_string();
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::UpsertStrategy(unreferenced),
        )
        .await
        .unwrap();

        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::AppendPlacementBinding(placement_binding(
                    realm_id,
                    23,
                    strategy_id,
                ))
            )
            .await,
            Err(MutateRealmPlacementError::EmptyShardHolders { .. })
        ));
    }

    #[test]
    fn override_without_strategy_is_valid() {
        let realm_id = RealmId::from_bytes([7; 32]);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert!(
            RealmPlacementMutation::SetOverride(PlacementOverride {
                subject: vec![1],
                pinned: Vec::new(),
                excluded: Vec::new(),
                strategy_id: None,
            })
            .validate(&document)
            .is_ok()
        );
    }

    #[test]
    fn affinity_data_is_not_changed_by_operation_input() {
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([10; 16]),
            name: "affinity".to_string(),
            replica_count: None,
            distinct_locations: false,
            shard_count: 64,
            affinity: vec![aruna_core::structs::AffinityRule {
                matcher: aruna_core::structs::LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: aruna_core::structs::AffinityEffect::Multiply { permille: 1500 },
            }],
        };
        let mutation = RealmPlacementMutation::UpsertStrategy(strategy.clone());
        assert!(matches!(
            mutation.admin_operation(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: stored }
                if stored == strategy
        ));
    }
}
