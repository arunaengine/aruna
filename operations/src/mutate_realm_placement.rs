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
    Actor, BindingError, BindingScope, BucketPlan, CandidatePlacementMap, CompletionProof,
    DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DocumentClass, MetadataRegistryRecord,
    NodePlacementEntry, PlacementBinding, PlacementOverride, PlacementRef, PlacementScope,
    PlacementStrategy, RealmConfigDocument, RealmNodeKind, StrategyBinding, TransitionPlan,
};
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
    PublishCandidateMap(CandidatePlacementMap),
    InitializeActivations {
        strategy_id: Ulid,
        candidate_map_epoch: u64,
    },
    StartTransition(TransitionPlan),
    ReportBarrier {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
        frontier: Vec<u8>,
    },
    SubmitCompletion {
        transition_id: Ulid,
        strategy_id: Ulid,
        proof: CompletionProof,
    },
    AbortTransition(Ulid),
    ForceFinalizeBucket {
        transition_id: Ulid,
        bucket: u32,
        at_risk_report: String,
    },
    ReportStall {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
        reason: String,
    },
    ReportDrained {
        transition_id: Ulid,
        bucket: u32,
        reported_by: NodeId,
    },
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
            Self::PublishCandidateMap(map) => {
                AdminDocumentOperation::RealmConfigCandidateMapPublished { map: map.clone() }
            }
            Self::InitializeActivations {
                strategy_id,
                candidate_map_epoch,
            } => AdminDocumentOperation::RealmConfigActivationsInitialized {
                strategy_id: *strategy_id,
                candidate_map_epoch: *candidate_map_epoch,
            },
            Self::StartTransition(plan) => {
                AdminDocumentOperation::RealmConfigTransitionStarted { plan: plan.clone() }
            }
            Self::ReportBarrier {
                transition_id,
                bucket,
                reported_by,
                frontier,
            } => AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                transition_id: *transition_id,
                bucket: *bucket,
                reported_by: *reported_by,
                frontier: frontier.clone(),
            },
            Self::SubmitCompletion {
                transition_id,
                strategy_id,
                proof,
            } => AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                transition_id: *transition_id,
                strategy_id: *strategy_id,
                proof: proof.clone(),
            },
            Self::AbortTransition(transition_id) => {
                AdminDocumentOperation::RealmConfigTransitionAborted {
                    transition_id: *transition_id,
                }
            }
            Self::ForceFinalizeBucket {
                transition_id,
                bucket,
                at_risk_report,
            } => AdminDocumentOperation::RealmConfigTransitionBucketForced {
                transition_id: *transition_id,
                bucket: *bucket,
                at_risk_report: at_risk_report.clone(),
            },
            Self::ReportStall {
                transition_id,
                bucket,
                reported_by,
                reason,
            } => AdminDocumentOperation::RealmConfigTransitionStallReported {
                transition_id: *transition_id,
                bucket: *bucket,
                reported_by: *reported_by,
                reason: reason.clone(),
            },
            Self::ReportDrained {
                transition_id,
                bucket,
                reported_by,
            } => AdminDocumentOperation::RealmConfigTransitionDrainReported {
                transition_id: *transition_id,
                bucket: *bucket,
                reported_by: *reported_by,
            },
        }
    }

    /// Local parity with the receiving side's admission: an authority-moving
    /// mutation needs a current Management node, a participant may only
    /// self-report the role the plan names it for, and a Server may append
    /// only a binding it allocated itself.
    fn authorize(
        &self,
        document: &RealmConfigDocument,
        actor: &Actor,
    ) -> Result<(), MutateRealmPlacementError> {
        let kind = node_kind(document, actor.node_id);
        let rejected = MutateRealmPlacementError::Unauthorized {
            node_id: actor.node_id,
        };
        if kind.is_none() {
            return Err(rejected);
        }
        let allowed = match self {
            Self::AppendPlacementBinding(binding) => {
                matches!(kind, Some(RealmNodeKind::Server))
                    && binding.allocated_by == Some(actor.node_id)
            }
            Self::ReportBarrier {
                transition_id,
                bucket,
                reported_by,
                ..
            } => bucket_plan(document, transition_id, *bucket).is_some_and(|plan| {
                *reported_by == actor.node_id && plan.old_holders.contains(reported_by)
            }),
            Self::SubmitCompletion {
                transition_id,
                proof,
                ..
            } => bucket_plan(document, transition_id, proof.bucket).is_some_and(|plan| {
                proof.holder == actor.node_id && plan.target_holders.contains(&proof.holder)
            }),
            Self::ReportStall {
                transition_id,
                bucket,
                reported_by,
                ..
            } => bucket_plan(document, transition_id, *bucket).is_some_and(|plan| {
                *reported_by == actor.node_id
                    && (plan.old_holders.contains(reported_by)
                        || plan.target_holders.contains(reported_by))
            }),
            Self::ReportDrained {
                transition_id,
                bucket,
                reported_by,
            } => bucket_plan(document, transition_id, *bucket).is_some_and(|plan| {
                *reported_by == actor.node_id
                    && plan.old_holders.contains(reported_by)
                    && !plan.target_holders.contains(reported_by)
            }),
            _ => false,
        };
        if matches!(
            self,
            Self::ReportBarrier { .. }
                | Self::SubmitCompletion { .. }
                | Self::ReportStall { .. }
                | Self::ReportDrained { .. }
        ) {
            return allowed.then_some(()).ok_or(rejected);
        }
        if matches!(kind, Some(RealmNodeKind::Management)) {
            return Ok(());
        }
        allowed.then_some(()).ok_or(rejected)
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
            // Per-shard activations cannot survive a bucket-space reshape.
            Self::UpsertStrategy(strategy)
                if document
                    .strategy(&strategy.strategy_id)
                    .is_some_and(|existing| existing.shard_count != strategy.shard_count)
                    && document
                        .placement_activations
                        .iter()
                        .any(|entry| entry.strategy_id == strategy.strategy_id) =>
            {
                Err(MutateRealmPlacementError::InvalidInput(
                    "shard_count cannot change while the strategy has activations".to_string(),
                ))
            }
            Self::SetDefaultStrategy(strategy_id) => {
                require_strategy(document, strategy_id, "default strategy")?;
                require_metadata_binding(
                    document,
                    PlacementScope::Realm(document.realm_id),
                    *strategy_id,
                )
            }
            Self::SetBinding(binding) => {
                require_strategy(document, &binding.strategy_id, "binding")?;
                let scope = match binding.scope {
                    BindingScope::Group(group_id) => Some(PlacementScope::Group(group_id)),
                    BindingScope::Realm
                    | BindingScope::MetadataPathPrefix(_)
                    | BindingScope::Class(DocumentClass::Metadata) => {
                        Some(PlacementScope::Realm(document.realm_id))
                    }
                    BindingScope::Class(_) => None,
                };
                match scope {
                    Some(scope) => require_metadata_binding(document, scope, binding.strategy_id),
                    None => Ok(()),
                }
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
                if !binding.has_valid_provenance(&document.handle_range_directory()) {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "placement binding provenance does not match an owned handle range"
                            .to_string(),
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
            Self::SetOverride(record) => match &record.strategy_id {
                Some(strategy_id) => require_strategy(document, strategy_id, "override"),
                None => Ok(()),
            },
            Self::PublishCandidateMap(map) => {
                if document.candidate_map(map.epoch).is_some()
                    || document
                        .candidate_maps
                        .iter()
                        .any(|known| known.epoch == map.epoch)
                {
                    return Err(MutateRealmPlacementError::InvalidInput(format!(
                        "candidate map epoch {} is already published",
                        map.epoch
                    )));
                }
                Ok(())
            }
            Self::InitializeActivations {
                strategy_id,
                candidate_map_epoch,
            } => {
                require_strategy(document, strategy_id, "activation")?;
                if document.candidate_map(*candidate_map_epoch).is_none() {
                    return Err(MutateRealmPlacementError::InvalidInput(format!(
                        "candidate map epoch {candidate_map_epoch} is missing or conflicted"
                    )));
                }
                Ok(())
            }
            Self::StartTransition(plan) => {
                require_strategy(document, &plan.strategy_id, "transition")?;
                if plan.limits.max_incomplete_buckets == 0 {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "a transition must allow at least one bucket in flight".to_string(),
                    ));
                }
                if let Some(existing) = document.placement_transitions.iter().find(|transition| {
                    transition.plan.strategy_id == plan.strategy_id && !transition.is_terminal()
                }) {
                    return Err(MutateRealmPlacementError::TransitionInFlight {
                        transition_id: existing.plan.transition_id,
                    });
                }
                // The plan restates derived holder sets, so admission re-derives
                // them: a plan naming sets this node disagrees with never enters.
                if !crate::placement::transition::plan_is_derivable(document, plan) {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "transition plan does not match the resolved holder sets".to_string(),
                    ));
                }
                Ok(())
            }
            Self::ReportBarrier {
                transition_id,
                bucket,
                ..
            }
            | Self::ReportStall {
                transition_id,
                bucket,
                ..
            }
            | Self::ReportDrained {
                transition_id,
                bucket,
                ..
            } => require_transition_bucket(document, transition_id, *bucket),
            Self::SubmitCompletion {
                transition_id,
                strategy_id,
                proof,
            } => {
                let transition = require_transition_bucket(document, transition_id, proof.bucket)
                    .and(document.transition(transition_id).ok_or(
                    MutateRealmPlacementError::UnknownTransition {
                        transition_id: *transition_id,
                    },
                ))?;
                if transition.plan.strategy_id != *strategy_id
                    || !proof.verify(document.realm_id, *transition_id, *strategy_id)
                {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "transition completion proof does not verify".to_string(),
                    ));
                }
                Ok(())
            }
            Self::AbortTransition(transition_id) => document
                .transition(transition_id)
                .map(|_| ())
                .ok_or(MutateRealmPlacementError::UnknownTransition {
                    transition_id: *transition_id,
                }),
            Self::ForceFinalizeBucket {
                transition_id,
                bucket,
                ..
            } => {
                let transition = require_transition_bucket(document, transition_id, *bucket).and(
                    document.transition(transition_id).ok_or(
                        MutateRealmPlacementError::UnknownTransition {
                            transition_id: *transition_id,
                        },
                    ),
                )?;
                // A forced cut still needs one verified copy on a target holder,
                // so the last verified copy is never the one cut away.
                if transition.proofs_for(*bucket).next().is_none() {
                    return Err(MutateRealmPlacementError::ForceWithoutProof {
                        transition_id: *transition_id,
                        bucket: *bucket,
                    });
                }
                Ok(())
            }
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

/// The configured kind of `node_id`, or `None` when the realm does not know it.
pub(crate) fn node_kind(document: &RealmConfigDocument, node_id: NodeId) -> Option<RealmNodeKind> {
    let node_id = node_id.to_string();
    document
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.kind.clone())
}

/// Only a management node's realm-config mutation passes inbound admission;
/// holder rank alone is not enough, because a rank-0 Server would apply
/// locally and then be rejected by every peer.
pub(crate) fn is_management(document: &RealmConfigDocument, node_id: NodeId) -> bool {
    matches!(
        node_kind(document, node_id),
        Some(RealmNodeKind::Management)
    )
}

fn bucket_plan<'a>(
    document: &'a RealmConfigDocument,
    transition_id: &Ulid,
    bucket: u32,
) -> Option<&'a BucketPlan> {
    document.transition(transition_id)?.plan.bucket_plan(bucket)
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

fn require_transition_bucket(
    document: &RealmConfigDocument,
    transition_id: &Ulid,
    bucket: u32,
) -> Result<(), MutateRealmPlacementError> {
    let transition =
        document
            .transition(transition_id)
            .ok_or(MutateRealmPlacementError::UnknownTransition {
                transition_id: *transition_id,
            })?;
    if !transition.plan.covers(bucket) {
        return Err(MutateRealmPlacementError::InvalidInput(format!(
            "transition {transition_id} does not cover bucket {bucket}"
        )));
    }
    Ok(())
}

fn require_metadata_binding(
    document: &RealmConfigDocument,
    scope: PlacementScope,
    strategy_id: Ulid,
) -> Result<(), MutateRealmPlacementError> {
    let directory = document.binding_directory();
    let exact = directory
        .handle_for(scope, DocumentClass::Metadata, strategy_id)
        .is_some();
    let realm_fallback = matches!(scope, PlacementScope::Group(_))
        && directory
            .handle_for(
                PlacementScope::Realm(document.realm_id),
                DocumentClass::Metadata,
                strategy_id,
            )
            .is_some();
    if !exact && !realm_fallback {
        return Err(MutateRealmPlacementError::InvalidInput(format!(
            "metadata policy strategy {strategy_id} has no binding for {scope:?}"
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
    actor: Actor,
    mutations: Vec<RealmPlacementMutation>,
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
    #[error("node {node_id} may not originate this placement mutation")]
    Unauthorized { node_id: NodeId },
    #[error("placement leaves strategy {strategy_id} shard {shard} with no eligible holders")]
    EmptyShardHolders { strategy_id: Ulid, shard: u32 },
    #[error("placement strategy {strategy_id} is currently referenced")]
    StrategyReferenced { strategy_id: Ulid },
    #[error("placement transition {transition_id} is still in flight")]
    TransitionInFlight { transition_id: Ulid },
    #[error("placement transition {transition_id} is unknown")]
    UnknownTransition { transition_id: Ulid },
    #[error(
        "forcing transition {transition_id} bucket {bucket} needs at least one verified completion proof"
    )]
    ForceWithoutProof { transition_id: Ulid, bucket: u32 },
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl MutateRealmPlacementOperation {
    pub fn new(config: MutateRealmPlacementConfig) -> Self {
        Self::batch(config.actor, vec![config.mutation])
    }

    /// One transaction, one reduced event per mutation, applied in order
    /// against the evolving document. The whole batch commits or none of it
    /// does. `RemoveStrategy` must be driven alone.
    pub fn batch(actor: Actor, mutations: Vec<RealmPlacementMutation>) -> Self {
        Self {
            actor,
            mutations,
            txn_id: None,
            state: MutateRealmPlacementState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: self.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.actor.realm_id,
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
        if self.mutations.is_empty() {
            return Err(MutateRealmPlacementError::InvalidInput(
                "empty placement mutation batch".to_string(),
            ));
        }
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;

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
        let pre_document = document.clone();
        let mut admin_events = Vec::with_capacity(self.mutations.len());
        for mutation in &self.mutations {
            mutation.authorize(&document, &self.actor)?;
            mutation.validate(&document)?;
            let admin_event =
                reducer_state.apply_operation(&self.actor, mutation.admin_operation())?;
            overlay_realm_config_placement_reducer_materialization(
                &mut document,
                &reducer_state,
                unix_timestamp_millis(),
            );
            admin_events.push(admin_event);
        }

        if let Some((node_id, placement)) =
            crate::placement::first_draining_holder_set_change(&pre_document, &document)
        {
            return Err(MutateRealmPlacementError::InvalidInput(format!(
                "placement change alters drain-time holder set for node {node_id}, strategy {} shard {}",
                placement.strategy_id, placement.shard
            )));
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
                document.to_bytes(&self.actor)?.into(),
            ),
            admin_document_reducer_state_write_entry(&reducer_state)?,
        ];
        for admin_event in admin_events {
            let record = new_outbox_record_with_id(
                admin_event.event_id,
                self.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(admin_event),
                },
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
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
        let strategy_id = match self.mutations.as_slice() {
            [RealmPlacementMutation::RemoveStrategy(strategy_id)] => {
                let document = RealmConfigDocument::from_bytes(&document_value)?;
                let removal = RealmPlacementMutation::RemoveStrategy(*strategy_id);
                removal.authorize(&document, &self.actor)?;
                removal.validate(&document)?;
                *strategy_id
            }
            mutations => {
                if mutations
                    .iter()
                    .any(|mutation| matches!(mutation, RealmPlacementMutation::RemoveStrategy(_)))
                {
                    return Err(MutateRealmPlacementError::InvalidInput(
                        "strategy removal cannot be batched".to_string(),
                    ));
                }
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
        record.realm_id == self.actor.realm_id
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
                        self.actor.realm_id,
                        self.actor.node_id,
                    )]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = MutateRealmPlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_placement_revalidation_effect(
                        self.actor.realm_id,
                        self.actor.node_id,
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
            .unwrap_or(Err(MutateRealmPlacementError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Drives a realm placement mutation, then — when it drains the local node —
/// kicks the installed outbox drain owner so records accepted before holdership
/// loss are retried without creating a second concurrent drainer or replacing a
/// persisted failure deadline.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::events::StorageEvent;
    use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
    use aruna_core::storage_entries::{
        metadata_create_event_and_pending_projection_write_entries, metadata_registry_write_entries,
    };
    use aruna_core::structs::{
        AffinityEffect, AffinityRule, DEFAULT_NODE_WEIGHT, DEFAULT_SHARD_COUNT, DocumentClass,
        FIRST_GRANTABLE_HANDLE, HandleRange, LabelMatch, MetadataRegistryRecord, PlacementBinding,
        PlacementRef, PlacementScope, RealmId, RealmNodeKind,
    };
    use aruna_core::structured_id::PlacementHandle;
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::types::UserId;
    use tempfile::tempdir;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::get_realm_config::GetRealmConfigOperation;
    use crate::placement::transition::{TransitionRequest, plan_transition};
    use aruna_core::structs::{PlacementTransition, ProofClaim, TransitionLimits};

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn node_secret(node_id: &aruna_core::NodeId) -> iroh::SecretKey {
        (1..=8u8)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]))
            .find(|secret| secret.public() == *node_id)
            .expect("fixture node keys are seeded")
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
            compute_handle: None,
        }
    }

    async fn seed_config(context: &DriverContext, actor: &Actor) -> RealmConfigDocument {
        let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
        document.seed_default_placement();
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
        document.placement_handle_ranges.push(HandleRange {
            range_id: Ulid::from_bytes([9; 16]),
            owner: actor.node_id,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + 1024,
        });
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
                    shard: 1,
                },
                holder_node_ids: vec![actor.node_id],
                created_at_ms: 1,
                updated_at_ms: 1,
                establishing_event_id: event_id,
                last_event_id: event_id,
            },
            user_id: actor.user_id,
            node_id: actor.node_id,
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Referenced".to_string(),
                description: "Strategy reference".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
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
        assert!(matches!(
            mutate(
                &context,
                &actor,
                RealmPlacementMutation::SetDefaultStrategy(strategy_id),
            )
            .await,
            Err(MutateRealmPlacementError::InvalidInput(reason))
                if reason.contains("has no binding")
        ));
        let range_id = initial.placement_handle_ranges[0].range_id;
        mutate(
            &context,
            &actor,
            RealmPlacementMutation::AppendPlacementBinding(PlacementBinding {
                handle: PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
                scope: PlacementScope::Realm(realm_id),
                document_class: DocumentClass::Metadata,
                strategy_id,
                allocator_range_id: Some(range_id),
                allocated_by: Some(actor.node_id),
                allocated_at_ms: Some(1),
            }),
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
        assert!(matches!(
            mutate(
            &context,
            &actor,
            RealmPlacementMutation::RemoveStrategy(strategy_id),
        )
            .await,
            Err(MutateRealmPlacementError::StrategyReferenced {
                strategy_id: referenced
            }) if referenced == strategy_id
        ));

        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .unwrap();
        assert!(stored.strategy(&strategy_id).is_some());
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
                AdminDocumentReducerError::ReservedPlacementLabel(_)
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

    #[test]
    fn shard_count_frozen() {
        // A bucket-space reshape would orphan per-shard activations.
        let realm_id = RealmId::from_bytes([15; 32]);
        let mut document = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        document.seed_default_placement();
        document.snapshot_candidate_map();
        let strategy_id = document.default_strategy_id.unwrap();
        let mut reshaped = document.strategy(&strategy_id).unwrap().clone();
        reshaped.shard_count *= 2;

        assert!(matches!(
            RealmPlacementMutation::UpsertStrategy(reshaped).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("shard_count")
        ));

        // Selector edits without a shard_count change stay allowed.
        let mut edited = document.strategy(&strategy_id).unwrap().clone();
        edited.replica_count = Some(1);
        assert_eq!(
            RealmPlacementMutation::UpsertStrategy(edited).validate(&document),
            Ok(())
        );
    }

    #[test]
    fn group_reuses_realm() {
        let realm_id = RealmId::from_bytes([14; 32]);
        let mut document = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        document.seed_default_placement();
        let strategy_id = document.default_strategy_id.unwrap();
        let mutation = RealmPlacementMutation::SetBinding(StrategyBinding {
            scope: BindingScope::Group(Ulid::generate()),
            strategy_id,
        });

        assert_eq!(mutation.validate(&document), Ok(()));
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
        // The issuing actor must be Management, or admission rejects it.
        document.ensure_node(actor.node_id, RealmNodeKind::Management);
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
                RealmPlacementMutation::UpsertStrategy(filtered)
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

    // Removing a strategy still named by an immutable placement binding is a
    // StrategyReferenced conflict, like the other reference kinds above.
    #[test]
    fn binding_blocks_removal() {
        let realm_id = RealmId::from_bytes([14; 32]);
        let strategy_id = Ulid::from_bytes([14; 16]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.strategies.push(strategy(strategy_id));
        document
            .placement_bindings
            .push(placement_binding(realm_id, 1, strategy_id));

        assert_eq!(
            RealmPlacementMutation::RemoveStrategy(strategy_id).validate(&document),
            Err(MutateRealmPlacementError::StrategyReferenced { strategy_id })
        );
    }

    #[test]
    fn binding_requires_strategy() {
        let realm_id = RealmId::from_bytes([31; 32]);
        let document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let binding = placement_binding(realm_id, 5, Ulid::from_bytes([9; 16]));
        assert!(matches!(
            RealmPlacementMutation::AppendPlacementBinding(binding).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("missing strategy")
        ));
    }

    #[test]
    fn rejects_divergent_rebind() {
        let realm_id = RealmId::from_bytes([32; 32]);
        let strategy_a = Ulid::from_bytes([1; 16]);
        let strategy_b = Ulid::from_bytes([2; 16]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.strategies.push(strategy(strategy_a));
        document.strategies.push(strategy(strategy_b));
        let handle = FIRST_GRANTABLE_HANDLE;
        let mut existing = placement_binding(realm_id, handle, strategy_a);
        existing.allocated_by = Some(node(4));
        existing.allocated_at_ms = Some(1);
        document.placement_handle_ranges.push(HandleRange {
            range_id: Ulid::from_bytes([44; 16]),
            owner: node(4),
            start: handle,
            end: handle + 1024,
        });
        document.placement_bindings.push(existing.clone());

        assert!(
            RealmPlacementMutation::AppendPlacementBinding(existing)
                .validate(&document)
                .is_ok()
        );
        let mut same = placement_binding(realm_id, handle, strategy_a);
        same.allocated_by = Some(node(4));
        same.allocated_at_ms = Some(1);
        same.allocator_range_id = Some(Ulid::from_bytes([77; 16]));
        assert!(matches!(
            RealmPlacementMutation::AppendPlacementBinding(same).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason)) if reason.contains("provenance")
        ));

        let mut divergent = placement_binding(realm_id, handle, strategy_b);
        divergent.allocated_by = Some(node(4));
        divergent.allocated_at_ms = Some(1);
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

    fn transition_document() -> (RealmConfigDocument, Ulid) {
        // Four nodes, one replica: every bucket moves to a disjoint holder when
        // the newest map adds a node, which is what a transition is for.
        let realm_id = RealmId::from_bytes([41; 32]);
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 1);
        let strategy_id = Ulid::from_bytes([42; 16]);
        document.strategies.push(PlacementStrategy {
            strategy_id,
            name: "moved".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 4,
        });
        document.default_strategy_id = Some(strategy_id);
        for seed in 1..=3u8 {
            document.ensure_node(node(seed), RealmNodeKind::Server);
        }
        document.snapshot_candidate_map();
        document.ensure_node(node(4), RealmNodeKind::Server);
        document.snapshot_candidate_map();
        (document, strategy_id)
    }

    fn transition_request(strategy_id: Ulid) -> TransitionRequest {
        TransitionRequest {
            transition_id: Ulid::from_bytes([43; 16]),
            strategy_id,
            buckets: Vec::new(),
            target_map_epoch: 2,
            limits: TransitionLimits::default(),
            created_by: node(1),
            created_at_ms: 1,
        }
    }

    #[test]
    fn transition_plan_must_match() {
        let (document, strategy_id) = transition_document();
        let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
        assert_eq!(plan.buckets.len(), 4);
        assert!(
            RealmPlacementMutation::StartTransition(plan.clone())
                .validate(&document)
                .is_ok()
        );

        // A plan naming a holder set this node does not derive never enters.
        let mut forged = plan.clone();
        forged.buckets[0].target_holders = vec![node(1)];
        assert!(matches!(
            RealmPlacementMutation::StartTransition(forged).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason))
                if reason.contains("does not match the resolved holder sets")
        ));

        // One transition per strategy at a time.
        let mut in_flight = document.clone();
        in_flight
            .placement_transitions
            .push(PlacementTransition::new(plan.clone()));
        let mut successor = plan.clone();
        successor.transition_id = Ulid::from_bytes([44; 16]);
        assert!(matches!(
            RealmPlacementMutation::StartTransition(successor).validate(&in_flight),
            Err(MutateRealmPlacementError::TransitionInFlight { transition_id })
                if transition_id == plan.transition_id
        ));
    }

    fn issuer(realm_id: RealmId, node_id: aruna_core::NodeId) -> Actor {
        Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        }
    }

    #[test]
    fn authority_needs_management() {
        // Server, Local, and unknown issuers must be rejected before any
        // reducer state is touched; Management keeps working.
        let (mut document, strategy_id) = transition_document();
        let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
        document
            .placement_transitions
            .push(PlacementTransition::new(plan.clone()));
        document.ensure_node(node(5), RealmNodeKind::Local);
        document.ensure_node(node(6), RealmNodeKind::Management);
        let realm_id = document.realm_id;

        for mutation in [
            RealmPlacementMutation::PublishCandidateMap(document.freeze_map(3)),
            RealmPlacementMutation::InitializeActivations {
                strategy_id,
                candidate_map_epoch: 2,
            },
            RealmPlacementMutation::StartTransition(plan.clone()),
            RealmPlacementMutation::AbortTransition(plan.transition_id),
            RealmPlacementMutation::ForceFinalizeBucket {
                transition_id: plan.transition_id,
                bucket: plan.buckets[0].bucket,
                at_risk_report: "old holders lost".to_string(),
            },
            RealmPlacementMutation::UpsertStrategy(strategy(Ulid::from_bytes([77; 16]))),
            RealmPlacementMutation::RemoveNode(node(3)),
        ] {
            for rejected in [node(1), node(5), node(9)] {
                assert!(
                    matches!(
                        mutation.authorize(&document, &issuer(realm_id, rejected)),
                        Err(MutateRealmPlacementError::Unauthorized { node_id })
                            if node_id == rejected
                    ),
                    "{rejected} must not originate an authority-moving mutation"
                );
            }
            assert_eq!(
                mutation.authorize(&document, &issuer(realm_id, node(6))),
                Ok(())
            );
        }
    }

    #[test]
    fn reports_need_planned_role() {
        // A non-Management participant may only report the role its bucket
        // plan names it for, and only for itself.
        let (mut document, strategy_id) = transition_document();
        let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
        let bucket = plan
            .buckets
            .iter()
            .find(|bucket| bucket.old_holders != bucket.target_holders)
            .expect("the fixture moves at least one bucket")
            .clone();
        document
            .placement_transitions
            .push(PlacementTransition::new(plan.clone()));
        let realm_id = document.realm_id;
        let old = bucket.old_holders[0];
        let target = bucket.target_holders[0];
        let transition_id = plan.transition_id;

        let barrier = |reported_by| RealmPlacementMutation::ReportBarrier {
            transition_id,
            bucket: bucket.bucket,
            reported_by,
            frontier: vec![1],
        };
        assert_eq!(
            barrier(old).authorize(&document, &issuer(realm_id, old)),
            Ok(())
        );
        assert!(
            barrier(target)
                .authorize(&document, &issuer(realm_id, target))
                .is_err()
        );
        // Reporting on another node's behalf is never a self-report.
        assert!(
            barrier(old)
                .authorize(&document, &issuer(realm_id, target))
                .is_err()
        );
        document.ensure_node(node(6), RealmNodeKind::Management);
        assert!(
            barrier(old)
                .authorize(&document, &issuer(realm_id, node(6)))
                .is_err()
        );

        let claim = ProofClaim {
            realm_id,
            transition_id,
            strategy_id,
            bucket: bucket.bucket,
            old_activation_epoch: 1,
            target_map_epoch: 2,
            barrier_digest: [0; 32],
            checkpoint_root: [1; 32],
            holder: target,
        };
        let completion = RealmPlacementMutation::SubmitCompletion {
            transition_id,
            strategy_id,
            proof: claim.sign(&node_secret(&target)),
        };
        assert_eq!(
            completion.authorize(&document, &issuer(realm_id, target)),
            Ok(())
        );
        assert!(
            completion
                .authorize(&document, &issuer(realm_id, old))
                .is_err()
        );

        let drained = |reported_by| RealmPlacementMutation::ReportDrained {
            transition_id,
            bucket: bucket.bucket,
            reported_by,
        };
        assert_eq!(
            drained(old).authorize(&document, &issuer(realm_id, old)),
            Ok(())
        );
        assert!(
            drained(target)
                .authorize(&document, &issuer(realm_id, target))
                .is_err()
        );

        let stall = |reported_by| RealmPlacementMutation::ReportStall {
            transition_id,
            bucket: bucket.bucket,
            reported_by,
            reason: "no source".to_string(),
        };
        for participant in [old, target] {
            assert_eq!(
                stall(participant).authorize(&document, &issuer(realm_id, participant)),
                Ok(())
            );
        }
        let outsider = (1..=9u8)
            .map(node)
            .find(|candidate| {
                !bucket.old_holders.contains(candidate)
                    && !bucket.target_holders.contains(candidate)
                    && node_kind(&document, *candidate).is_some()
            })
            .expect("the fixture has an uninvolved node");
        assert!(
            stall(outsider)
                .authorize(&document, &issuer(realm_id, outsider))
                .is_err()
        );
    }

    #[tokio::test]
    async fn unauthorized_writes_nothing() {
        let directory = tempdir().unwrap();
        let context = context(directory.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([46; 32]);
        let management = actor(realm_id);
        let seeded = seed_placement_config(&context, &management, &[node(2)], Some(2)).await;

        let result = mutate(
            &context,
            &issuer(realm_id, node(2)),
            RealmPlacementMutation::PublishCandidateMap(seeded.freeze_map(1)),
        )
        .await;

        assert!(matches!(
            result,
            Err(MutateRealmPlacementError::Unauthorized { node_id }) if node_id == node(2)
        ));
        let stored = drive(GetRealmConfigOperation::new(realm_id), &context)
            .await
            .expect("the realm config survives a rejected mutation");
        assert!(stored.candidate_maps.is_empty());
        assert!(
            crate::document_sync_outbox::read_outbox_tails(&context.storage_handle)
                .await
                .expect("outbox scan")
                .is_empty()
        );
    }

    #[test]
    fn force_needs_one_proof() {
        let (mut document, strategy_id) = transition_document();
        let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
        let bucket = plan.buckets[0].bucket;
        let holder = plan.buckets[0].target_holders[0];
        document
            .placement_transitions
            .push(PlacementTransition::new(plan.clone()));
        let force = RealmPlacementMutation::ForceFinalizeBucket {
            transition_id: plan.transition_id,
            bucket,
            at_risk_report: "old holders lost".to_string(),
        };

        assert!(matches!(
            force.validate(&document),
            Err(MutateRealmPlacementError::ForceWithoutProof { bucket: forced, .. })
                if forced == bucket
        ));

        let secret = node_secret(&holder);
        document.placement_transitions[0].proofs.push(
            ProofClaim {
                realm_id: document.realm_id,
                transition_id: plan.transition_id,
                strategy_id,
                bucket,
                old_activation_epoch: 1,
                target_map_epoch: 2,
                barrier_digest: [0; 32],
                checkpoint_root: [1; 32],
                holder,
            }
            .sign(&secret),
        );
        assert_eq!(force.validate(&document), Ok(()));
    }

    #[test]
    fn completion_must_verify() {
        let (mut document, strategy_id) = transition_document();
        let plan = plan_transition(&document, transition_request(strategy_id)).unwrap();
        let bucket = plan.buckets[0].bucket;
        let holder = plan.buckets[0].target_holders[0];
        document
            .placement_transitions
            .push(PlacementTransition::new(plan.clone()));
        let claim = ProofClaim {
            realm_id: document.realm_id,
            transition_id: plan.transition_id,
            strategy_id,
            bucket,
            old_activation_epoch: 1,
            target_map_epoch: 2,
            barrier_digest: [0; 32],
            checkpoint_root: [1; 32],
            holder,
        };
        let submit = |proof| RealmPlacementMutation::SubmitCompletion {
            transition_id: plan.transition_id,
            strategy_id,
            proof,
        };

        assert_eq!(
            submit(claim.sign(&node_secret(&holder))).validate(&document),
            Ok(())
        );

        // Signed by the wrong key, and aimed at a bucket the plan does not cover.
        let other = (1..=4u8)
            .map(node)
            .find(|candidate| *candidate != holder)
            .expect("the fixture has more than one node");
        assert!(matches!(
            submit(claim.sign(&node_secret(&other))).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason))
                if reason.contains("does not verify")
        ));
        let mut off_plan = claim;
        off_plan.bucket = 9;
        assert!(matches!(
            submit(off_plan.sign(&node_secret(&holder))).validate(&document),
            Err(MutateRealmPlacementError::InvalidInput(reason))
                if reason.contains("does not cover bucket")
        ));
    }
}
