//! Decides whether the origin of a replicated realm config event has the authority to make it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::NodeId;
use crate::admin_checks::{origin_may_publish, revocation_origin_known};
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget};
use crate::deferred_topics::DocumentSyncDependency;
use crate::reducer::{AdminDocumentState, overlay_placement};
use crate::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use crate::structs::placement::record::{
    FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, PoolAdmission, admit_band_pool, coordinator_spans,
};

/// The decision on one replicated admin event.
#[derive(Debug, PartialEq, Eq)]
pub enum AdminEventValidation {
    Accepted,
    Rejected(String),
    Deferred {
        dependency: Option<DocumentSyncDependency>,
        reason: String,
    },
}

/// Who a transition report claims to be, against the named plan's roles.
enum ReportParticipation {
    NotReport,
    Participant,
    UnknownPlan,
    Foreign,
}

/// Resolves the plan a report names (stored config first, reduced state as
/// the fallback) and checks the reporter holds the role the report claims:
/// barriers from old holders, proofs from targets, stalls from the union.
fn report_participation(
    op: &AdminDocumentOperation,
    current_config: Option<&RealmConfigDocument>,
    previous_state: Option<&AdminDocumentState>,
) -> ReportParticipation {
    enum Role {
        Old,
        Target,
        Union,
        Departing,
    }
    let (transition_id, bucket, reporter, role) = match op {
        AdminDocumentOperation::TransitionBarrierReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => (*transition_id, *bucket, *reported_by, Role::Old),
        AdminDocumentOperation::TransitionProofSubmitted {
            transition_id,
            proof,
            ..
        } => (*transition_id, proof.bucket, proof.holder, Role::Target),
        AdminDocumentOperation::TransitionStallReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => (*transition_id, *bucket, *reported_by, Role::Union),
        AdminDocumentOperation::TransitionDrainReported {
            transition_id,
            bucket,
            reported_by,
        } => (*transition_id, *bucket, *reported_by, Role::Departing),
        _ => return ReportParticipation::NotReport,
    };
    let from_config = current_config.and_then(|config| {
        config
            .placement_transitions
            .iter()
            .find(|transition| transition.plan.transition_id == transition_id)
            .map(|transition| transition.plan.clone())
    });
    let plan = match from_config {
        Some(plan) => plan,
        None => {
            let Some(plan) = previous_state
                .map(|state| state.materialized_transition_plans())
                .unwrap_or_default()
                .remove(&transition_id)
            else {
                return ReportParticipation::UnknownPlan;
            };
            plan
        }
    };
    let Some(bucket_plan) = plan.bucket_plan(bucket) else {
        return ReportParticipation::Foreign;
    };
    let allowed = match role {
        Role::Old => bucket_plan.old_holders.contains(&reporter),
        Role::Target => bucket_plan.target_holders.contains(&reporter),
        Role::Union => {
            bucket_plan.old_holders.contains(&reporter)
                || bucket_plan.target_holders.contains(&reporter)
        }
        Role::Departing => {
            bucket_plan.old_holders.contains(&reporter)
                && !bucket_plan.target_holders.contains(&reporter)
        }
    };
    if allowed {
        ReportParticipation::Participant
    } else {
        ReportParticipation::Foreign
    }
}

/// Admits a new merge coordinator only from the current one, so the old coordinator stops before
/// the new one starts. A realm without one, or whose coordinator left management, may name one.
fn merge_handoff(
    config: Option<&RealmConfigDocument>,
    previous_state: Option<&AdminDocumentState>,
    event: &AdminDocumentEvent,
    node_id: &NodeId,
) -> AdminEventValidation {
    let Some(config) = config else {
        return AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(config_realm(event))),
            reason: "current realm config is unavailable".to_string(),
        };
    };
    let management =
        |node: &NodeId| matches!(config.node_kind(node), Some(RealmNodeKind::Management));
    if !management(&event.origin_node_id) || !management(node_id) {
        return AdminEventValidation::Rejected(
            "a merge coordinator is named by and among management nodes".to_string(),
        );
    }
    let pinned = previous_state.and_then(AdminDocumentState::materialized_merge_coordinator);
    let left = |pinned: &NodeId| {
        previous_state.is_some_and(|state| state.removed_config_nodes().contains(pinned))
            || config
                .node_kind(pinned)
                .is_some_and(|kind| *kind != RealmNodeKind::Management)
    };
    match pinned {
        None => AdminEventValidation::Accepted,
        Some(pinned) if pinned == event.origin_node_id || left(&pinned) => {
            AdminEventValidation::Accepted
        }
        Some(pinned) if config.node_kind(&pinned).is_none() => AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(config_realm(event))),
            reason: "the current merge coordinator is not known here yet".to_string(),
        },
        Some(_) => AdminEventValidation::Rejected(
            "only the current merge coordinator hands the role on".to_string(),
        ),
    }
}

fn config_realm(event: &AdminDocumentEvent) -> RealmId {
    match event.target {
        AdminDocumentTarget::RealmConfig { realm_id } => realm_id,
        _ => event.actor.realm_id,
    }
}

/// Whether the origin of a realm config event may make it now: transition reports from planned
/// participants, revocations of known origins, a merge coordinator handoff, band pools and handle
/// grants in their lineage, and otherwise management nodes or the bootstrap of the first one.
pub fn validate_config_authority(
    current_config: Option<&RealmConfigDocument>,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
) -> AdminEventValidation {
    let AdminDocumentTarget::RealmConfig { realm_id } = event.target else {
        return AdminEventValidation::Rejected(
            "admin event target is not a realm config".to_string(),
        );
    };
    if current_config.is_some_and(|config| config.realm_id != realm_id) {
        return AdminEventValidation::Rejected(
            "stored realm config has the wrong realm".to_string(),
        );
    }
    // Reports are gated on participation whatever the origin's kind: a plan
    // names its finite holder sets, so nothing outside them may grow state.
    match report_participation(&event.op, current_config, previous_state) {
        ReportParticipation::NotReport | ReportParticipation::Participant => {}
        ReportParticipation::UnknownPlan => {
            // Same-topic retry only: an absent plan after the buffered run
            // flushes has no causal evidence and must not park the cursor.
            return AdminEventValidation::Deferred {
                dependency: None,
                reason: "transition plan is not yet materialized".to_string(),
            };
        }
        ReportParticipation::Foreign => {
            return AdminEventValidation::Rejected(
                "transition report does not come from a planned participant".to_string(),
            );
        }
    }
    if matches!(&event.op, AdminDocumentOperation::ConfigTokenRevoked { .. }) {
        if !revocation_origin_known(current_config, previous_state, event, realm_id) {
            return AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
                reason: if current_config.is_some() {
                    "revocation event origin onboarding is not yet materialized"
                } else {
                    "current realm config is unavailable"
                }
                .to_string(),
            };
        }
        return AdminEventValidation::Accepted;
    }
    if let AdminDocumentOperation::MergeCoordinatorSet { node_id } = &event.op {
        return merge_handoff(current_config, previous_state, event, node_id);
    }
    // Placement reducer state precedes config materialization; only band and
    // handle checks consult it, so the overlay is not paid for every op.
    let placement_config = matches!(
        &event.op,
        AdminDocumentOperation::BandPoolAssigned { .. }
            | AdminDocumentOperation::HandleRangeGranted { .. }
    )
    .then(|| {
        let mut placement_config = current_config
            .cloned()
            .unwrap_or_else(|| RealmConfigDocument::default_for_realm(realm_id, Vec::new()));
        if let Some(state) = previous_state {
            overlay_placement(&mut placement_config, state, 0);
        }
        placement_config
    });
    // Band pools form a causal delegation tree; reject a forged or
    // non-owning issuer, and defer a child until its parent replicates.
    if let (AdminDocumentOperation::BandPoolAssigned { pool }, Some(placement_config)) =
        (&event.op, placement_config.as_ref())
    {
        match admit_band_pool(&placement_config.band_pools, pool, &event.origin_node_id) {
            PoolAdmission::Reject => {
                return AdminEventValidation::Rejected("band pool lineage is invalid".to_string());
            }
            PoolAdmission::MissingParent => {
                return AdminEventValidation::Deferred {
                    dependency: None,
                    reason: "band pool parent is not yet replicated".to_string(),
                };
            }
            PoolAdmission::Accept => {}
        }
    }
    if let (AdminDocumentOperation::HandleRangeGranted { range }, Some(placement_config)) =
        (&event.op, placement_config.as_ref())
    {
        let canonical = range.len() == HANDLE_RANGE_SIZE
            && range
                .start
                .checked_sub(FIRST_GRANTABLE_HANDLE)
                .is_some_and(|offset| offset % HANDLE_RANGE_SIZE == 0)
            && range.start.checked_add(HANDLE_RANGE_SIZE) == Some(range.end);
        if !canonical {
            return AdminEventValidation::Rejected(
                "handle grant is not one canonical band".to_string(),
            );
        }
        let spans = coordinator_spans(&placement_config.band_pools, &event.origin_node_id);
        if spans.is_empty() {
            return AdminEventValidation::Deferred {
                dependency: None,
                reason: "coordinator band pool is not yet replicated".to_string(),
            };
        }
        if !spans
            .iter()
            .any(|(start, end)| *start <= range.start && range.end <= *end)
        {
            return AdminEventValidation::Rejected(
                "handle grant lies outside the coordinator band pool".to_string(),
            );
        }
    }
    if let Some(config) = current_config {
        let server_binding = match (config.node_kind(&event.origin_node_id), &event.op) {
            (
                Some(RealmNodeKind::Server),
                AdminDocumentOperation::PlacementBindingAppended { binding },
            ) => {
                binding.allocated_by == Some(event.origin_node_id)
                    && binding.has_valid_provenance(&config.handle_range_directory())
            }
            _ => false,
        };
        // Every holder acts. A barrier, proof, or stall names its own origin
        // and moves no authority on its own, so a holder may emit it.
        let self_report = matches!(
            &event.op,
            AdminDocumentOperation::TransitionBarrierReported { reported_by, .. }
            | AdminDocumentOperation::TransitionStallReported { reported_by, .. }
            | AdminDocumentOperation::TransitionDrainReported { reported_by, .. }
                if *reported_by == event.origin_node_id
        ) || matches!(
            &event.op,
            AdminDocumentOperation::TransitionProofSubmitted { proof, .. }
                if proof.holder == event.origin_node_id
        );
        if matches!(
            config.node_kind(&event.origin_node_id),
            Some(RealmNodeKind::Management)
        ) && matches!(
            &event.op,
            AdminDocumentOperation::PlacementBindingAppended { binding }
                if !binding.has_valid_provenance(&config.handle_range_directory())
        ) {
            // The granting range event may still be in flight in the same batch
            // (onboarding writes grant + JobControl binding back to back).
            return AdminEventValidation::Deferred {
                dependency: None,
                reason: "placement binding provenance is not yet valid".to_string(),
            };
        }
        return if matches!(
            config.node_kind(&event.origin_node_id),
            Some(RealmNodeKind::Management)
        ) || server_binding
            || (self_report && origin_may_publish(config, &event.origin_node_id))
        {
            AdminEventValidation::Accepted
        } else {
            AdminEventValidation::Rejected(
                "event origin is not a current management node".to_string(),
            )
        };
    }

    let bootstrap = previous_state.is_none()
        && matches!(
            &event.op,
            AdminDocumentOperation::ConfigNodeEnsured {
                node_id,
                kind: RealmNodeKind::Management,
            } if *node_id == event.origin_node_id
        );
    let continuing_bootstrap = previous_state.is_some_and(|state| {
        state
            .materialized_config_nodes()
            .get(&event.origin_node_id)
            .is_some_and(|kind| matches!(kind, RealmNodeKind::Management))
    });
    if bootstrap || continuing_bootstrap {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Deferred {
            dependency: None,
            reason: "realm config bootstrap must begin with a management node ensuring itself"
                .to_string(),
        }
    }
}
