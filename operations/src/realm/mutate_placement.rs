use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_STATE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE, METADATA_INDEX_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, overlay_placement};
use aruna_core::storage_entries::{
    conflict_write_entries, pending_projection_target, reducer_state_entry, reducer_state_key,
    stale_conflict_deletes,
};
use aruna_core::structs::{
    Actor, AuthContext, BindingError, BindingScope, BucketPlan, CandidatePlacementMap,
    CompletionProof, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DocumentClass, MetadataRegistryRecord,
    NodePlacementEntry, Permission, PlacementBinding, PlacementOverride, PlacementRef,
    PlacementScope, PlacementStrategy, RealmConfigDocument, RealmNodeKind, StrategyBinding,
    TransitionPlan, normalize_placement_input, policy_admin_path, reserved_label, storage_subject,
};
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::sync::shard_placement::schedule_revalidation;

pub(crate) const CONFLICT_ATTEMPTS: usize = 10;

const STRATEGY_REFERENCE_SCAN_PAGE_SIZE: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealmPlacementMutation {
    UpsertNode(NodePlacementEntry),
    RemoveNode(NodeId),
    /// Edits the placement attributes of a node the realm already knows. An
    /// absent field keeps its stored value; a changed one advances that node's
    /// storage subject, which revalidates its copies.
    SetNodeAttributes {
        node_id: NodeId,
        location: Option<String>,
        labels: Option<BTreeMap<String, String>>,
    },
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
    /// The replicated operation this mutation reduces to. An attribute edit is
    /// resolved against the current entry here, so peers replay the whole entry
    /// rather than a partial change against their own copy.
    fn admin_operation(
        &self,
        document: &RealmConfigDocument,
    ) -> Result<AdminDocumentOperation, MutatePlacementError> {
        Ok(match self {
            Self::UpsertNode(entry) => AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: entry.clone(),
            },
            Self::SetNodeAttributes {
                node_id,
                location,
                labels,
            } => AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: attributes_entry(document, *node_id, location.as_ref(), labels.as_ref())?,
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
        })
    }

    /// Local parity with the receiving side's admission: authority-moving mutations
    /// need a current Management node, participants may only self-report their named
    /// role, and a Server may append only a binding it allocated itself.
    fn authorize(
        &self,
        document: &RealmConfigDocument,
        actor: &Actor,
    ) -> Result<(), MutatePlacementError> {
        let kind = node_kind(document, actor.node_id);
        let rejected = MutatePlacementError::Unauthorized {
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

    fn validate(&self, document: &RealmConfigDocument) -> Result<(), MutatePlacementError> {
        match self {
            Self::SetNodeAttributes {
                node_id,
                location,
                labels,
            } => {
                let current = placement_entry(document, *node_id)?;
                if current.draining {
                    return Err(MutatePlacementError::InvalidInput(
                        "draining freezes placement attributes until the node un-drains or is removed"
                            .to_string(),
                    ));
                }
                if let Some(location) = location {
                    normalize_placement_input(Some(location), None)
                        .map_err(|error| MutatePlacementError::InvalidInput(error.to_string()))?;
                }
                if let Some(label) = labels.as_ref().and_then(reserved_label) {
                    return Err(MutatePlacementError::InvalidInput(format!(
                        "placement label {label} is derived and cannot be set"
                    )));
                }
                ensure_subject(&attributes_entry(
                    document,
                    *node_id,
                    location.as_ref(),
                    labels.as_ref(),
                )?)
            }
            Self::UpsertNode(entry) => {
                ensure_subject(entry)?;
                if !entry.draining {
                    return Ok(());
                }
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
                    Err(MutatePlacementError::InvalidInput(
                        "draining freezes placement attributes until the node un-drains or is removed"
                            .to_string(),
                    ))
                }
            }
            Self::UpsertStrategy(strategy) if strategy.replica_count == Some(0) => {
                Err(MutatePlacementError::InvalidInput(
                    "placement strategy replica_count must not be zero".to_string(),
                ))
            }
            // Every node derives family shards from this count, so a reshape
            // would silently re-route retained v1 family records.
            Self::UpsertStrategy(strategy)
                if document.job_family_strategy_id == strategy.strategy_id
                    && document
                        .strategy(&strategy.strategy_id)
                        .is_some_and(|existing| existing.shard_count != strategy.shard_count) =>
            {
                Err(MutatePlacementError::JobFamilyImmutable {
                    strategy_id: strategy.strategy_id,
                })
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
                Err(MutatePlacementError::InvalidInput(
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
                    return Err(MutatePlacementError::InvalidInput(
                        "placement binding realm does not match the realm config".to_string(),
                    ));
                }
                if !binding.has_valid_provenance(&document.handle_range_directory()) {
                    return Err(MutatePlacementError::InvalidInput(
                        "placement binding provenance does not match an owned handle range"
                            .to_string(),
                    ));
                }
                match document.binding_directory().resolve(binding.handle) {
                    Ok(existing) if existing != binding.tuple() => {
                        Err(MutatePlacementError::InvalidInput(format!(
                            "placement binding handle {} is already bound to a different tuple",
                            binding.handle.get()
                        )))
                    }
                    Err(BindingError::Conflicted(_)) => {
                        Err(MutatePlacementError::InvalidInput(format!(
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
                    return Err(MutatePlacementError::InvalidInput(format!(
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
                    return Err(MutatePlacementError::InvalidInput(format!(
                        "candidate map epoch {candidate_map_epoch} is missing or conflicted"
                    )));
                }
                Ok(())
            }
            Self::StartTransition(plan) => {
                require_strategy(document, &plan.strategy_id, "transition")?;
                if plan.limits.max_incomplete_buckets == 0 {
                    return Err(MutatePlacementError::InvalidInput(
                        "a transition must allow at least one bucket in flight".to_string(),
                    ));
                }
                if let Some(existing) = document.placement_transitions.iter().find(|transition| {
                    transition.plan.strategy_id == plan.strategy_id && !transition.is_terminal()
                }) {
                    return Err(MutatePlacementError::TransitionInFlight {
                        transition_id: existing.plan.transition_id,
                    });
                }
                // The plan restates derived holder sets, so admission re-derives
                // them: a plan naming sets this node disagrees with never enters.
                if !crate::placement::transition::plan_is_derivable(document, plan) {
                    return Err(MutatePlacementError::InvalidInput(
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
                    MutatePlacementError::UnknownTransition {
                        transition_id: *transition_id,
                    },
                ))?;
                if transition.plan.strategy_id != *strategy_id
                    || !proof.verify(document.realm_id, *transition_id, *strategy_id)
                {
                    return Err(MutatePlacementError::InvalidInput(
                        "transition completion proof does not verify".to_string(),
                    ));
                }
                Ok(())
            }
            Self::AbortTransition(transition_id) => document
                .transition(transition_id)
                .map(|_| ())
                .ok_or(MutatePlacementError::UnknownTransition {
                    transition_id: *transition_id,
                }),
            Self::ForceFinalizeBucket {
                transition_id,
                bucket,
                ..
            } => {
                let transition = require_transition_bucket(document, transition_id, *bucket).and(
                    document.transition(transition_id).ok_or(
                        MutatePlacementError::UnknownTransition {
                            transition_id: *transition_id,
                        },
                    ),
                )?;
                // A forced cut still needs one verified copy on a target holder,
                // so the last verified copy is never the one cut away.
                if transition.proofs_for(*bucket).next().is_none() {
                    return Err(MutatePlacementError::ForceWithoutProof {
                        transition_id: *transition_id,
                        bucket: *bucket,
                    });
                }
                Ok(())
            }
            Self::RemoveStrategy(strategy_id) => {
                if document.job_family_strategy_id == *strategy_id {
                    return Err(MutatePlacementError::JobFamilyImmutable {
                        strategy_id: *strategy_id,
                    });
                }
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
                    Err(MutatePlacementError::StrategyReferenced {
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

/// The placement entry of a node the realm already places. A node without one
/// holds no governed data, so editing its attributes is refused.
fn placement_entry(
    document: &RealmConfigDocument,
    node_id: NodeId,
) -> Result<&NodePlacementEntry, MutatePlacementError> {
    document.placement_entry(node_id).ok_or_else(|| {
        MutatePlacementError::InvalidInput(format!("node {node_id} has no placement entry"))
    })
}

/// The entry an attribute edit resolves to: the stored one with the requested
/// fields applied, so validation and reduction judge the same result.
fn attributes_entry(
    document: &RealmConfigDocument,
    node_id: NodeId,
    location: Option<&String>,
    labels: Option<&BTreeMap<String, String>>,
) -> Result<NodePlacementEntry, MutatePlacementError> {
    let mut entry = placement_entry(document, node_id)?.clone();
    if let Some(location) = location {
        entry.location = location.trim().to_string();
    }
    if let Some(labels) = labels {
        entry.labels = labels.clone();
    }
    Ok(entry)
}

/// Refuses attributes whose derived storage subject the node could never
/// advance to. The generation is not part of that validation.
fn ensure_subject(entry: &NodePlacementEntry) -> Result<(), MutatePlacementError> {
    storage_subject(entry, 1)
        .validate()
        .map_err(|error| MutatePlacementError::InvalidInput(error.to_string()))
}

fn require_strategy(
    document: &RealmConfigDocument,
    strategy_id: &Ulid,
    reference: &str,
) -> Result<(), MutatePlacementError> {
    if document.strategy(strategy_id).is_none() {
        return Err(MutatePlacementError::InvalidInput(format!(
            "{reference} references missing strategy {strategy_id}"
        )));
    }
    Ok(())
}

fn require_transition_bucket(
    document: &RealmConfigDocument,
    transition_id: &Ulid,
    bucket: u32,
) -> Result<(), MutatePlacementError> {
    let transition =
        document
            .transition(transition_id)
            .ok_or(MutatePlacementError::UnknownTransition {
                transition_id: *transition_id,
            })?;
    if !transition.plan.covers(bucket) {
        return Err(MutatePlacementError::InvalidInput(format!(
            "transition {transition_id} does not cover bucket {bucket}"
        )));
    }
    Ok(())
}

fn require_metadata_binding(
    document: &RealmConfigDocument,
    scope: PlacementScope,
    strategy_id: Ulid,
) -> Result<(), MutatePlacementError> {
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
        return Err(MutatePlacementError::InvalidInput(format!(
            "metadata policy strategy {strategy_id} has no binding for {scope:?}"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutatePlacementConfig {
    pub actor: Actor,
    pub mutation: RealmPlacementMutation,
}

#[derive(Debug, PartialEq)]
pub struct MutatePlacementOperation {
    actor: Actor,
    /// Set when a caller's token has to be authorized: the node-internal
    /// mutation paths originate their own changes and carry no token.
    auth_context: Option<AuthContext>,
    mutations: Vec<RealmPlacementMutation>,
    txn_id: Option<TxnId>,
    state: MutatePlacementState,
    output: Option<Result<RealmConfigDocument, MutatePlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
struct StrategyRemovalCheck {
    document_value: Value,
    reducer_state_value: Option<Value>,
    strategy_id: Ulid,
}

#[derive(Debug, Clone, PartialEq)]
enum MutatePlacementState {
    Init,
    Auth,
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
pub enum MutatePlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
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
    #[error("job family strategy {strategy_id} and its shard count are immutable")]
    JobFamilyImmutable { strategy_id: Ulid },
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

impl MutatePlacementOperation {
    /// Node-internal entry: the node originates the mutation itself, so only the
    /// per-mutation node authorization applies.
    pub fn new(config: MutatePlacementConfig) -> Self {
        Self::batch(config.actor, vec![config.mutation])
    }

    /// Caller-facing entry: the token must hold WRITE on the realm configuration
    /// admin path and only a management node may serve it.
    pub fn authorized(config: MutatePlacementConfig, auth_context: AuthContext) -> Self {
        Self {
            auth_context: Some(auth_context),
            ..Self::batch(config.actor, vec![config.mutation])
        }
    }

    /// One transaction, one reduced event per mutation, applied in order
    /// against the evolving document. The whole batch commits or none of it
    /// does. `RemoveStrategy` must be driven alone.
    pub fn batch(actor: Actor, mutations: Vec<RealmPlacementMutation>) -> Self {
        Self {
            actor,
            auth_context: None,
            mutations,
            txn_id: None,
            state: MutatePlacementState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
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
        self.state = MutatePlacementState::ReadCurrent;
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
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, MutatePlacementError> {
        let Some(txn_id) = self.txn_id else {
            return Err(MutatePlacementError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(MutatePlacementError::RealmConfigNotFound);
        };
        if self.mutations.is_empty() {
            return Err(MutatePlacementError::InvalidInput(
                "empty placement mutation batch".to_string(),
            ));
        }
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;

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
        let pre_document = document.clone();
        let mut admin_events = Vec::with_capacity(self.mutations.len());
        for mutation in &self.mutations {
            mutation.authorize(&document, &self.actor)?;
            mutation.validate(&document)?;
            let admin_event =
                reducer_state.apply_operation(&self.actor, mutation.admin_operation(&document)?)?;
            overlay_placement(&mut document, &reducer_state, unix_timestamp_millis());
            admin_events.push(admin_event);
        }

        if let Some((node_id, placement)) =
            crate::placement::first_draining_change(&pre_document, &document)
        {
            return Err(MutatePlacementError::InvalidInput(format!(
                "placement change alters drain-time holder set for node {node_id}, strategy {} shard {}",
                placement.strategy_id, placement.shard
            )));
        }
        if let Some(placement) = crate::placement::first_empty_shard(&document) {
            return Err(MutatePlacementError::EmptyShardHolders {
                strategy_id: placement.strategy_id,
                shard: placement.shard,
            });
        }

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        for admin_event in admin_events {
            let record = new_identified_record(
                admin_event.event_id,
                self.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(admin_event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(document.clone()));
        self.state = MutatePlacementState::WriteDocumentAndAdminState {
            document,
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_reference_check(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, MutatePlacementError> {
        let Some(document_value) = document_value else {
            return Err(MutatePlacementError::RealmConfigNotFound);
        };
        // A caller's request is served by management nodes only; a node's own
        // mutations stay governed by the per-mutation node authorization.
        if self.auth_context.is_some()
            && !is_management(
                &RealmConfigDocument::from_bytes(&document_value)?,
                self.actor.node_id,
            )
        {
            return Err(MutatePlacementError::Unauthorized {
                node_id: self.actor.node_id,
            });
        }
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
                    return Err(MutatePlacementError::InvalidInput(
                        "strategy removal cannot be batched".to_string(),
                    ));
                }
                return self.emit_document_write(Some(document_value), reducer_state_value);
            }
        };
        let check = StrategyRemovalCheck {
            document_value,
            reducer_state_value,
            strategy_id,
        };
        Ok(self.scan_registry_refs(check, None))
    }

    fn scan_registry_refs(
        &mut self,
        check: StrategyRemovalCheck,
        start_after: Option<Key>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutatePlacementError::MissingTransaction);
        };
        self.state = MutatePlacementState::ReadRegistryReferences { check };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: STRATEGY_REFERENCE_SCAN_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    fn scan_pending_refs(
        &mut self,
        check: StrategyRemovalCheck,
        start_after: Option<Key>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutatePlacementError::MissingTransaction);
        };
        self.state = MutatePlacementState::ReadPendingReferences { check };
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

    fn write_after_check(&mut self, check: StrategyRemovalCheck) -> Effects {
        match self.emit_document_write(Some(check.document_value), check.reducer_state_value) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn emit_commit_transaction(&mut self, document: RealmConfigDocument) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(MutatePlacementError::MissingTransaction);
        };
        self.state = MutatePlacementState::CommitTransaction { document };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: MutatePlacementError) -> Effects {
        let cleanup = self.abort();
        self.state = MutatePlacementState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(MutatePlacementError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for MutatePlacementOperation {
    type Output = RealmConfigDocument;
    type Error = MutatePlacementError;

    /// An SSI conflict is ordinary contention that every caller re-drives, so
    /// only an exhausted retry belongs on the error stream.
    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            MutatePlacementError::StorageError(StorageError::TransactionConflict)
        )
    }

    fn start(&mut self) -> Effects {
        let Some(auth_context) = self.auth_context.clone() else {
            self.state = MutatePlacementState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })];
        };
        if auth_context.realm_id != self.actor.realm_id {
            return self.fail(MutatePlacementError::Unauthorized {
                node_id: self.actor.node_id,
            });
        }
        self.state = MutatePlacementState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
                path: policy_admin_path(self.actor.realm_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            MutatePlacementState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = MutatePlacementState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false,
                            })]
                        }
                        Ok(false) => self.fail(MutatePlacementError::Unauthorized {
                            node_id: self.actor.node_id,
                        }),
                        Err(error) => {
                            warn!(error = %error, "Realm placement authorization check failed");
                            match error {
                                AuthorizationError::StorageError(error) => {
                                    self.fail(MutatePlacementError::StorageError(error))
                                }
                                _ => self.fail(MutatePlacementError::Unauthorized {
                                    node_id: self.actor.node_id,
                                }),
                            }
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            MutatePlacementState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            MutatePlacementState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self
                        .emit_reference_check(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            MutatePlacementState::ReadRegistryReferences { check } => match event {
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
                            return self.fail(MutatePlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        }
                    }
                    match next_start_after {
                        Some(start_after) => self.scan_registry_refs(check, Some(start_after)),
                        None => self.scan_pending_refs(check, None),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("metadata registry scan result", format!("{other:?}"))
                }
            },
            MutatePlacementState::ReadPendingReferences { check } => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    if values.is_empty() {
                        return match next_start_after {
                            Some(start_after) => self.scan_pending_refs(check, Some(start_after)),
                            None => self.write_after_check(check),
                        };
                    }
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(MutatePlacementError::MissingTransaction);
                    };
                    self.state = MutatePlacementState::ReadPendingEvents {
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
            MutatePlacementState::ReadPendingEvents {
                check,
                next_start_after,
            } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    for (key, value) in values {
                        let Some(value) = value else {
                            return self.fail(MutatePlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        };
                        let event: MetadataEventRecord = match postcard::from_bytes(&value) {
                            Ok(event) => event,
                            Err(_) => {
                                return self.fail(MutatePlacementError::StrategyReferenced {
                                    strategy_id: check.strategy_id,
                                });
                            }
                        };
                        let valid_target = pending_projection_target(key.as_ref()).is_some_and(
                            |(document_id, event_id)| {
                                event.record.document_id == document_id
                                    && event.event_id == event_id
                            },
                        );
                        if !valid_target || self.reference_matches(&event.record, check.strategy_id)
                        {
                            return self.fail(MutatePlacementError::StrategyReferenced {
                                strategy_id: check.strategy_id,
                            });
                        }
                    }
                    match next_start_after {
                        Some(start_after) => self.scan_pending_refs(check, Some(start_after)),
                        None => self.write_after_check(check),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("pending create event reads", format!("{other:?}")),
            },
            MutatePlacementState::WriteDocumentAndAdminState {
                document,
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(MutatePlacementError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = MutatePlacementState::DeleteStaleAdminConflicts { document };
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
            MutatePlacementState::DeleteStaleAdminConflicts { document } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction(document)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            MutatePlacementState::CommitTransaction { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = MutatePlacementState::ScheduleDocumentSyncOutboxDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            MutatePlacementState::ScheduleDocumentSyncOutboxDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutatePlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_revalidation(
                        self.actor.realm_id,
                        self.actor.node_id,
                    )]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = MutatePlacementState::SchedulePlacementRevalidation;
                    smallvec![schedule_revalidation(
                        self.actor.realm_id,
                        self.actor.node_id,
                    )]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            MutatePlacementState::SchedulePlacementRevalidation => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = MutatePlacementState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule placement revalidation after realm placement mutation");
                    self.state = MutatePlacementState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "placement revalidation timer schedule",
                    format!("{other:?}"),
                ),
            },
            MutatePlacementState::Finish
            | MutatePlacementState::Error
            | MutatePlacementState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            MutatePlacementState::Finish | MutatePlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(MutatePlacementError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Drives a realm placement mutation, then when it drains the local node kicks the
/// installed outbox drain owner so pre-holdership-loss records retry without a second
/// drainer or replacing a persisted deadline. `auth_context` is `None` for local origin.
pub async fn drive_placement_mutation(
    config: MutatePlacementConfig,
    auth_context: Option<AuthContext>,
    context: &crate::driver::DriverContext,
) -> Result<RealmConfigDocument, MutatePlacementError> {
    let drains_node = matches!(
        &config.mutation,
        RealmPlacementMutation::UpsertNode(entry)
            if entry.draining
                && context.net_handle.as_ref().map(|net| net.node_id()) == Some(entry.node_id)
    );
    let mut attempts = 0;
    let outcome = loop {
        let operation = match auth_context.clone() {
            Some(auth_context) => {
                MutatePlacementOperation::authorized(config.clone(), auth_context)
            }
            None => MutatePlacementOperation::new(config.clone()),
        };
        match crate::driver::drive(operation, context).await {
            Err(MutatePlacementError::StorageError(StorageError::TransactionConflict))
                if attempts < CONFLICT_ATTEMPTS =>
            {
                // Retrying with no wait spends every attempt in one contention window.
                tokio::time::sleep(crate::tasks::queue_backoff::conflict_backoff(
                    attempts,
                    config.actor.node_id.as_bytes(),
                ))
                .await;
                attempts += 1;
            }
            Err(MutatePlacementError::StorageError(StorageError::TransactionConflict)) => {
                warn!(attempts, "Realm placement mutation kept conflicting");
                break Err(MutatePlacementError::StorageError(
                    StorageError::TransactionConflict,
                ));
            }
            outcome => break outcome,
        }
    };
    if outcome.is_ok() && drains_node && context.net_handle.is_some() {
        crate::tasks::incoming::drive_sync_drain(std::sync::Arc::new(context.clone())).await;
    }
    outcome
}

#[cfg(test)]
#[path = "mutate_placement_tests.rs"]
mod tests;
