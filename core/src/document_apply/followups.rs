//! Stores the follow-ups of each replicated apply in the transaction that applies it, so a
//! replay that skips an applied event still finds them queued for the node drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::admin::{AdminFollowUp, AdminOperationPlan, ConfigRunPlan};
use super::distribution::{DistributionPlan, Handled};
use super::events::{ApplyEventPlan, ApplyOutcome};
use super::registry::Row;
use super::{ApplyError, Step, invalid};
use crate::document::{
    AppliedStep, AppliedSync, DocumentReconcileResult, DocumentTarget, applied_sync_entry,
};
use crate::id::NodeId;
use crate::operation::TransactionMode;
use crate::row_plan::{PlanRows, Planned, RowPlan};
use crate::structs::identity::realm::RealmId;
use crate::structs::sync_quarantine::SyncQuarantineIdentity;

/// The node whose drain runs the follow-ups a reconcile queues, and the realm it serves.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FollowUpNode {
    pub node_id: NodeId,
    pub realm_id: RealmId,
}

/// The applied sync row of one apply. Its id comes from the event's sync identity, so a replay
/// that applies the event again writes the same row instead of a second one.
pub fn followup_row(
    node: &FollowUpNode,
    identity: &SyncQuarantineIdentity,
    result: DocumentReconcileResult,
) -> Result<Row, ApplyError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-applied-sync-v1\0");
    hasher.update(&identity.storage_key());
    let mut id = [0u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    let applied = AppliedSync {
        id: Ulid::from_bytes(id),
        node_id: node.node_id,
        realm_id: Some(node.realm_id),
        result,
        next: AppliedStep::Usage,
    };
    applied_sync_entry(&applied).map_err(invalid)
}

/// An apply plan that also writes its follow-up row for `node` when it changed rows, keyed by
/// `identity`, the event that applied (the last one of a config run).
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedApply<P> {
    pub plan: P,
    pub node: FollowUpNode,
    pub identity: SyncQuarantineIdentity,
}

impl<P> QueuedApply<P> {
    fn queue<T>(
        &self,
        mut planned: Planned<T>,
        result: DocumentReconcileResult,
    ) -> Step<Planned<T>> {
        planned
            .writes
            .push(followup_row(&self.node, &self.identity, result)?);
        Ok(planned)
    }

    /// An admin change counts as applied once it changed rows; a duplicate changes none.
    fn queue_target<T>(&self, planned: Planned<T>, target: &DocumentTarget) -> Step<Planned<T>> {
        if planned.writes.is_empty() && planned.deletes.is_empty() {
            return Ok(planned);
        }
        let result = DocumentReconcileResult {
            targets: vec![target.clone()],
            ..DocumentReconcileResult::default()
        };
        self.queue(planned, result)
    }
}

impl RowPlan for QueuedApply<ApplyEventPlan> {
    type Output = ApplyOutcome;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<ApplyOutcome>> {
        let planned = self.plan.plan(rows)?;
        let ApplyOutcome::Applied {
            target, tombstone, ..
        } = &planned.output
        else {
            return Ok(planned);
        };
        let result = DocumentReconcileResult {
            targets: vec![target.clone()],
            metadata_graph_tombstones: tombstone.iter().map(|record| (**record).clone()).collect(),
            ..DocumentReconcileResult::default()
        };
        self.queue(planned, result)
    }

    fn transaction(&self) -> TransactionMode {
        self.plan.transaction()
    }
}

impl RowPlan for QueuedApply<AdminOperationPlan> {
    type Output = AdminFollowUp;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<AdminFollowUp>> {
        let planned = self.plan.plan(rows)?;
        self.queue_target(planned, &self.plan.target)
    }

    fn conflict_retries() -> u32 {
        AdminOperationPlan::conflict_retries()
    }
}

impl RowPlan for QueuedApply<ConfigRunPlan> {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        let planned = self.plan.plan(rows)?;
        self.queue_target(planned, &self.plan.target)
    }

    fn conflict_retries() -> u32 {
        ConfigRunPlan::conflict_retries()
    }
}

impl RowPlan for QueuedApply<DistributionPlan> {
    type Output = Handled;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<Handled>> {
        let planned = self.plan.plan(rows)?;
        self.queue_target(planned, &self.plan.target)
    }

    fn conflict_retries() -> u32 {
        DistributionPlan::conflict_retries()
    }
}
