//! Applies realm config placement changes: nodes, strategies, bindings, maps and transitions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::{
    DOCUMENT_STATE_KEYSPACE, EVENT_LOG_KEYSPACE, METADATA_INDEX_KEYSPACE,
    PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, overlay_placement};
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, pending_projection_target, reducer_state_entry, reducer_state_key,
    stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::binding_directory::BindingError;
use aruna_core::structs::placement::node_subject::storage_subject;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::placement::record::{
    BindingScope, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DocumentClass, NodePlacementEntry,
    PlacementBinding, PlacementOverride, PlacementRef, PlacementScope, PlacementStrategy,
    StrategyBinding, normalize_placement_input,
};
use aruna_core::structs::placement::transition::{
    BucketPlan, CandidatePlacementMap, CompletionProof, TransitionPlan,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::node_info::reserved_label;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, Value};
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::sync::shard_placement::schedule_revalidation;
use aruna_core::placement::target_placement_ref;

pub(crate) const CONFLICT_ATTEMPTS: usize = 10;

const STRATEGY_PAGE_SIZE: usize = 8_192;

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
            Self::UpsertNode(entry) => AdminDocumentOperation::NodePlacementSet {
                entry: entry.clone(),
            },
            Self::SetNodeAttributes {
                node_id,
                location,
                labels,
            } => AdminDocumentOperation::NodePlacementSet {
                entry: attributes_entry(document, *node_id, location.as_ref(), labels.as_ref())?,
            },
            Self::RemoveNode(node_id) => {
                AdminDocumentOperation::NodePlacementRemoved { node_id: *node_id }
            }
            Self::UpsertStrategy(strategy) => AdminDocumentOperation::PlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
            Self::RemoveStrategy(strategy_id) => AdminDocumentOperation::PlacementStrategyRemoved {
                strategy_id: *strategy_id,
            },
            Self::SetDefaultStrategy(strategy_id) => AdminDocumentOperation::ConfigStrategySet {
                strategy_id: *strategy_id,
            },
            Self::SetBinding(binding) => AdminDocumentOperation::StrategyBindingSet {
                binding: binding.clone(),
            },
            Self::RemoveBinding(scope) => AdminDocumentOperation::StrategyBindingRemoved {
                scope: scope.clone(),
            },
            Self::SetOverride(record) => AdminDocumentOperation::PlacementOverrideSet {
                record: record.clone(),
            },
            Self::RemoveOverride(subject) => AdminDocumentOperation::PlacementOverrideRemoved {
                subject: subject.clone(),
            },
            Self::AppendPlacementBinding(binding) => {
                AdminDocumentOperation::PlacementBindingAppended {
                    binding: binding.clone(),
                }
            }
            Self::PublishCandidateMap(map) => {
                AdminDocumentOperation::CandidateMapPublished { map: map.clone() }
            }
            Self::InitializeActivations {
                strategy_id,
                candidate_map_epoch,
            } => AdminDocumentOperation::ConfigActivationsInitialized {
                strategy_id: *strategy_id,
                candidate_map_epoch: *candidate_map_epoch,
            },
            Self::StartTransition(plan) => {
                AdminDocumentOperation::ConfigTransitionStarted { plan: plan.clone() }
            }
            Self::ReportBarrier {
                transition_id,
                bucket,
                reported_by,
                frontier,
            } => AdminDocumentOperation::TransitionBarrierReported {
                transition_id: *transition_id,
                bucket: *bucket,
                reported_by: *reported_by,
                frontier: frontier.clone(),
            },
            Self::SubmitCompletion {
                transition_id,
                strategy_id,
                proof,
            } => AdminDocumentOperation::TransitionProofSubmitted {
                transition_id: *transition_id,
                strategy_id: *strategy_id,
                proof: proof.clone(),
            },
            Self::AbortTransition(transition_id) => {
                AdminDocumentOperation::ConfigTransitionAborted {
                    transition_id: *transition_id,
                }
            }
            Self::ForceFinalizeBucket {
                transition_id,
                bucket,
                at_risk_report,
            } => AdminDocumentOperation::TransitionBucketForced {
                transition_id: *transition_id,
                bucket: *bucket,
                at_risk_report: at_risk_report.clone(),
            },
            Self::ReportStall {
                transition_id,
                bucket,
                reported_by,
                reason,
            } => AdminDocumentOperation::TransitionStallReported {
                transition_id: *transition_id,
                bucket: *bucket,
                reported_by: *reported_by,
                reason: reason.clone(),
            },
            Self::ReportDrained {
                transition_id,
                bucket,
                reported_by,
            } => AdminDocumentOperation::TransitionDrainReported {
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
                if document.family_strategy_id == strategy.strategy_id
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
                if !aruna_core::placement::transition::plan_is_derivable(document, plan) {
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
                if document.family_strategy_id == *strategy_id {
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
pub fn node_kind(document: &RealmConfigDocument, node_id: NodeId) -> Option<RealmNodeKind> {
    document.node_kind(&node_id).cloned()
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

/// Checks a caller's config permission when there is one, then applies the mutations in one write
/// transaction. Callers rerun it after a lost commit.
#[derive(Debug, PartialEq)]
pub struct MutatePlacementOperation {
    actor: Actor,
    /// Set when a caller's token has to be authorized: the node-internal
    /// mutation paths originate their own changes and carry no token.
    auth_context: Option<AuthContext>,
    mutations: Vec<RealmPlacementMutation>,
    /// Whether the caller's check passed and the write runs.
    checked: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct StrategyRemovalCheck {
    document_value: Value,
    reducer_state_value: Option<Value>,
    strategy_id: Ulid,
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
    ConfigMissing,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for MutatePlacementError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "MutatePlacement".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type PlacementTurn = Result<Next<RealmConfigDocument>, MutatePlacementError>;

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
            checked: false,
        }
    }

    /// The write transaction of the mutations.
    fn write(&self) -> PlacementWrite {
        PlacementWrite {
            actor: self.actor.clone(),
            authorized: self.auth_context.is_some(),
            mutations: self.mutations.clone(),
            stage: Stage::Read,
            check: None,
            pages: None,
            pending: None,
            document: None,
            deletes: Vec::new(),
        }
    }

    fn run_write(&mut self) -> PlacementTurn {
        self.checked = true;
        Ok(Next::Call(IoCall::child(self.write())))
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

    fn first(&mut self) -> PlacementTurn {
        let Some(auth_context) = self.auth_context.clone() else {
            return self.run_write();
        };
        if auth_context.realm_id != self.actor.realm_id {
            return Err(MutatePlacementError::Unauthorized {
                node_id: self.actor.node_id,
            });
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context,
                path: policy_admin_path(self.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> PlacementTurn {
        if self.checked {
            return result
                .child::<RealmConfigDocument, MutatePlacementError>()?
                .map(Next::Done);
        }
        let unauthorized = MutatePlacementError::Unauthorized {
            node_id: self.actor.node_id,
        };
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => self.run_write(),
            Ok(false) => Err(unauthorized),
            Err(error) => {
                warn!(error = %error, "Realm placement authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => Err(error.into()),
                    _ => Err(unauthorized),
                }
            }
        }
    }
}

/// What the placement write transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    /// Scans the materialized metadata registry for references to a removed strategy.
    Registry,
    /// Scans the pending projections, then reads their create events.
    Pending,
    PendingEvents,
    Write,
    Delete,
}

/// Reads the realm config and its admin state, checks a strategy removal for references, then
/// stores the mutated config and its queued admin events in one write transaction.
#[derive(Debug, PartialEq)]
struct PlacementWrite {
    actor: Actor,
    /// A caller's request is served by management nodes only.
    authorized: bool,
    mutations: Vec<RealmPlacementMutation>,
    stage: Stage,
    check: Option<StrategyRemovalCheck>,
    pages: Option<PrefixPages>,
    pending: Option<Vec<Key>>,
    document: Option<RealmConfigDocument>,
    deletes: Vec<(KeySpace, Key)>,
}

impl PlacementWrite {
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

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> PlacementTurn {
        let Some(document_value) = document_value else {
            return Err(MutatePlacementError::ConfigMissing);
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
            aruna_core::placement::first_draining_change(&pre_document, &document)
        {
            return Err(MutatePlacementError::InvalidInput(format!(
                "placement change alters drain-time holder set for node {node_id}, strategy {} shard {}",
                placement.strategy_id, placement.shard
            )));
        }
        if let Some(placement) = aruna_core::placement::first_empty_shard(&document) {
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

        self.document = Some(document);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn emit_reference_check(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> PlacementTurn {
        let Some(document_value) = document_value else {
            return Err(MutatePlacementError::ConfigMissing);
        };
        // A caller's request is served by management nodes only; a node's own
        // mutations stay governed by the per-mutation node authorization.
        if self.authorized
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
        self.check = Some(StrategyRemovalCheck {
            document_value,
            reducer_state_value,
            strategy_id,
        });
        Ok(self.scan(Stage::Registry, METADATA_INDEX_KEYSPACE))
    }

    fn scan(&mut self, stage: Stage, space: &str) -> Next<RealmConfigDocument> {
        let pages = PrefixPages::new(space, Key::from(Vec::new()), STRATEGY_PAGE_SIZE);
        let call = pages.call();
        self.pages = Some(pages);
        self.stage = stage;
        Next::Call(call)
    }

    fn pages(&mut self) -> Result<&mut PrefixPages, MutatePlacementError> {
        self.pages.as_mut().ok_or(MutatePlacementError::NotFinished)
    }

    fn check(&self) -> Result<&StrategyRemovalCheck, MutatePlacementError> {
        self.check.as_ref().ok_or(MutatePlacementError::NotFinished)
    }

    fn referenced(&self) -> Result<MutatePlacementError, MutatePlacementError> {
        Ok(MutatePlacementError::StrategyReferenced {
            strategy_id: self.check()?.strategy_id,
        })
    }

    fn reference_matches(&self, record: &MetadataRegistryRecord, strategy_id: Ulid) -> bool {
        record.realm_id == self.actor.realm_id
            && record.placement != PlacementRef::NIL
            && record.placement.strategy_id == strategy_id
    }

    fn write_after_check(&mut self) -> PlacementTurn {
        let check = self.check.take().ok_or(MutatePlacementError::NotFinished)?;
        self.emit_document_write(Some(check.document_value), check.reducer_state_value)
    }

    fn registry_page(&mut self, rows: Vec<(Key, Value)>) -> PlacementTurn {
        let strategy_id = self.check()?.strategy_id;
        for (_, value) in rows {
            let record: MetadataRegistryRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            if self.reference_matches(&record, strategy_id) {
                return Err(self.referenced()?);
            }
        }
        match self.pages()?.done() {
            true => Ok(self.scan(Stage::Pending, PENDING_PROJECTION_KEYSPACE)),
            false => Ok(Next::Call(self.pages()?.call())),
        }
    }

    fn pending_page(&mut self, rows: Vec<(Key, Value)>) -> PlacementTurn {
        if rows.is_empty() {
            return match self.pages()?.done() {
                true => self.write_after_check(),
                false => Ok(Next::Call(self.pages()?.call())),
            };
        }
        let keys: Vec<Key> = rows.into_iter().map(|(key, _)| key).collect();
        let reads = (keys.iter())
            .map(|key| (EVENT_LOG_KEYSPACE.to_string(), key.clone()))
            .collect();
        self.pending = Some(keys);
        self.stage = Stage::PendingEvents;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn pending_events(&mut self, values: Vec<Option<Value>>) -> PlacementTurn {
        let strategy_id = self.check()?.strategy_id;
        let keys = self
            .pending
            .take()
            .ok_or(MutatePlacementError::NotFinished)?;
        for (key, value) in keys.into_iter().zip(values) {
            let Some(value) = value else {
                return Err(self.referenced()?);
            };
            let Ok(event) = postcard::from_bytes::<MetadataEventRecord>(&value) else {
                return Err(self.referenced()?);
            };
            let valid_target =
                pending_projection_target(key.as_ref()).is_some_and(|(document_id, event_id)| {
                    event.record.document_id == document_id && event.event_id == event_id
                });
            if !valid_target || self.reference_matches(&event.record, strategy_id) {
                return Err(self.referenced()?);
            }
        }
        match self.pages()?.done() {
            true => self.write_after_check(),
            false => {
                self.stage = Stage::Pending;
                Ok(Next::Call(self.pages()?.call()))
            }
        }
    }

    /// Failed drain or revalidation timers leave the outbox and the placement sync retryable.
    fn finish(&mut self) -> PlacementTurn {
        let output = self
            .document
            .take()
            .ok_or(MutatePlacementError::NotFinished)?;
        let calls = vec![
            IoCall::Effect(Box::new(schedule_drain_effect())),
            IoCall::Effect(Box::new(schedule_revalidation(
                self.actor.realm_id,
                self.actor.node_id,
            ))),
        ];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for PlacementWrite {
    type Output = RealmConfigDocument;
    type Error = MutatePlacementError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> PlacementTurn {
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
        ])))
    }

    fn next(&mut self, result: IoResult) -> PlacementTurn {
        match self.stage {
            Stage::Read => {
                let [document, reducer] = values::<2>(result)?;
                self.emit_reference_check(document, reducer)
            }
            Stage::Registry => {
                let rows = self.pages()?.take(result)?;
                self.registry_page(rows)
            }
            Stage::Pending => {
                let rows = self.pages()?.take(result)?;
                self.pending_page(rows)
            }
            Stage::PendingEvents => self.pending_events(result.values()?),
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

#[cfg(test)]
#[path = "mutate_placement_tests.rs"]
pub(crate) mod tests;
