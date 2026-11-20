//! Stores the follow-ups of each replicated apply in the transaction that applies it, so a
//! replay that skips an applied event still finds them queued for the node drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;
use ulid::Ulid;

use super::admin::{AdminFollowUp, AdminOperationPlan, ConfigRunPlan};
use super::distribution::{DistributionPlan, Handled};
use super::events::{ApplyEventPlan, ApplyOutcome};
use super::registry::Row;
use super::{ApplyError, Step, invalid};
use crate::document::{
    AppliedStep, AppliedSync, DocumentReconcileResult, DocumentTarget, applied_sync_entry,
    order_counter_key, order_entry_key,
};
use crate::id::NodeId;
use crate::keyspaces::APPLIED_ORDER_KEYSPACE;
use crate::operation::TransactionMode;
use crate::row_plan::{PlanRows, Planned, RowPlan};
use crate::structs::identity::realm::RealmId;
use crate::structs::sync_quarantine::SyncQuarantineIdentity;

/// The node whose drain runs the follow-ups a reconcile queues, the realm it serves, and the
/// follow-up the drain starts at: a caller that runs the earlier ones itself queues from later.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FollowUpNode {
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub from: AppliedStep,
}

impl FollowUpNode {
    /// Whether `result` leaves the drain work from this node's first follow-up on.
    fn wants(&self, result: &DocumentReconcileResult) -> bool {
        let metadata = !result.metadata_create_events.is_empty()
            || !result.metadata_graph_tombstones.is_empty()
            || (result.targets.iter())
                .any(|target| matches!(target, DocumentTarget::MetadataCreateEvent { .. }));
        self.from != AppliedStep::Metadata || metadata
    }
}

/// The id of the applied sync row of the events one transaction applied: a hash of their sync
/// identities in apply order, so a replay that applies them again writes the same row.
pub fn followup_id(identities: &[SyncQuarantineIdentity]) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-applied-sync-v1\0");
    for identity in identities {
        hasher.update(&identity.storage_key());
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ulid::from_bytes(id)
}

/// The applied sync row of the events one transaction applied, under [`followup_id`].
pub fn followup_row(
    node: &FollowUpNode,
    identities: &[SyncQuarantineIdentity],
    result: DocumentReconcileResult,
) -> Result<Row, ApplyError> {
    let applied = AppliedSync {
        id: followup_id(identities),
        node_id: node.node_id,
        realm_id: Some(node.realm_id),
        result,
        next: node.from,
    };
    applied_sync_entry(&applied).map_err(invalid)
}

/// The rows that queue batch `id` after every earlier queued batch of each of `documents`, in
/// the transaction that accepts them: its entry under the document's next sequence, and the
/// advanced counter. The drain projects a document's batches in this order.
pub(super) fn order_rows(rows: &mut PlanRows, documents: &[Ulid], id: Ulid) -> Step<Vec<Row>> {
    for document_id in documents {
        rows.want(APPLIED_ORDER_KEYSPACE, order_counter_key(*document_id));
    }
    let mut writes = Vec::with_capacity(documents.len().saturating_mul(2));
    for document_id in documents {
        let counter = rows.read(APPLIED_ORDER_KEYSPACE, order_counter_key(*document_id))?;
        let next = match counter {
            Some(value) => u64::from_be_bytes(<[u8; 8]>::try_from(&value[..]).map_err(invalid)?),
            None => 0,
        };
        let entry = order_entry_key(*document_id, next);
        let id = ByteView::from(id.to_bytes().to_vec());
        writes.push((APPLIED_ORDER_KEYSPACE.to_string(), entry, id));
        let counted = ByteView::from(next.saturating_add(1).to_be_bytes().to_vec());
        let counter = order_counter_key(*document_id);
        writes.push((APPLIED_ORDER_KEYSPACE.to_string(), counter, counted));
    }
    Ok(writes)
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
        if !self.node.wants(&result) {
            return Ok(planned);
        }
        planned
            .writes
            .push(followup_row(&self.node, &[self.identity], result)?);
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

#[cfg(test)]
mod tests {
    use super::super::fixtures::{config_target, node, realm};
    use super::*;

    // A node that queues from the metadata follow-up stores rows only for metadata work; one that
    // queues every follow-up stores them all.
    #[test]
    fn metadata_wants_work() {
        let metadata = FollowUpNode {
            node_id: node(),
            realm_id: realm(),
            from: AppliedStep::Metadata,
        };
        let config = DocumentReconcileResult {
            targets: vec![config_target()],
            ..DocumentReconcileResult::default()
        };
        assert!(!metadata.wants(&config));
        let created = DocumentReconcileResult {
            targets: vec![DocumentTarget::MetadataCreateEvent {
                document_id: Ulid::from_parts(1, 1),
                event_id: Ulid::from_parts(2, 2),
            }],
            ..DocumentReconcileResult::default()
        };
        assert!(metadata.wants(&created));
        let every = FollowUpNode {
            from: AppliedStep::Usage,
            ..metadata
        };
        assert!(every.wants(&config));
    }
}
