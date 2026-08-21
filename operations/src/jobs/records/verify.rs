//! The local view and the retained evidence one job record is judged against.
//!
//! Every input here comes from this node's own replicated state: the realm
//! config it synchronized and the records it already stored as authentic. A
//! relaying peer supplies bytes, never authority and never evidence.

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::structs::{
    ExecutionReceipt, ExecutionUpdate, HolderView, JobFamilyId, JobFamilyRecord, JobId,
    JobRecordContext, JobRecordKey, JobRecordKind, LaunchIntent, LocalExecution, LogicalJobSpec,
    PlacementRef, RealmConfigDocument, RealmId, WitnessBudgetRecord,
};
use ulid::Ulid;

use super::keys::{budget_key, id_key};
use crate::placement::transition::activation_holders;

/// The authenticated local view records of one family are judged against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyView {
    pub realm_id: RealmId,
    pub family: JobFamilyId,
    pub placement: PlacementRef,
    members: Vec<NodeId>,
    holders: Vec<NodeId>,
}

impl FamilyView {
    /// Fail-closed derivation: `None` means this node cannot judge authority
    /// yet, because the family placement, its activation, or the membership is
    /// unavailable or conflicted. Verifying against an empty view would reject
    /// every holder-authored record instead of retrying it.
    pub fn resolve(
        config: &RealmConfigDocument,
        realm_id: RealmId,
        family: JobFamilyId,
    ) -> Option<Self> {
        let placement = config.family_placement(family.submission_id).ok()?;
        let strategy = config.strategy(&placement.strategy_id)?;
        let holders = activation_holders(config, strategy, &placement)?;
        let members = config.sync_eligible_node_ids().ok()?;
        if holders.is_empty() || members.is_empty() {
            return None;
        }
        Some(Self {
            realm_id,
            family,
            placement,
            members,
            holders,
        })
    }

    pub fn holders(&self) -> &[NodeId] {
        &self.holders
    }

    pub fn holds(&self, node_id: NodeId) -> bool {
        self.holders.contains(&node_id) && self.members.contains(&node_id)
    }

    pub fn is_member(&self, node_id: NodeId) -> bool {
        self.members.contains(&node_id)
    }

    pub fn context<'a>(
        &'a self,
        evidence: Evidence<'a>,
        local: Option<&'a LocalExecution>,
    ) -> JobRecordContext<'a> {
        JobRecordContext {
            realm_id: self.realm_id,
            family: self.family,
            placement: self.placement,
            view: HolderView {
                members: &self.members,
                holders: &self.holders,
            },
            spec: evidence.spec,
            budget: evidence.budget,
            launch: evidence.launch,
            receipt: evidence.receipt,
            previous_update: evidence.previous_update,
            local,
        }
    }
}

/// Predecessor records one candidate is verified against. Every field is a
/// record this node already stored as authentic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Evidence<'a> {
    pub spec: Option<&'a LogicalJobSpec>,
    pub budget: Option<&'a WitnessBudgetRecord>,
    pub launch: Option<&'a LaunchIntent>,
    pub receipt: Option<&'a ExecutionReceipt>,
    pub previous_update: Option<&'a ExecutionUpdate>,
}

/// The predecessor rows one set of candidates must be judged against.
///
/// A key is derived wherever the successor's own signed identity addresses its
/// predecessor exactly. A whole kind is scanned only where selection is by a
/// digest or an id no record key carries, and that scan must run to completion:
/// a truncated one proves nothing absent.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct EvidencePlan {
    pub keys: BTreeSet<JobRecordKey>,
    pub kinds: BTreeSet<JobRecordKind>,
}

impl EvidencePlan {
    /// Adds what one candidate needs. This mirrors [`EvidenceSet::select`]:
    /// every predecessor selection may consult must be loadable from here.
    pub fn extend(&mut self, record: &JobFamilyRecord) {
        let family = record.family();
        match record {
            JobFamilyRecord::Spec(_) => {}
            JobFamilyRecord::Claim(claim) => {
                self.keys.insert(id_key(
                    &family,
                    JobRecordKind::Spec,
                    claim.job_id.to_bytes(),
                    0,
                ));
            }
            JobFamilyRecord::Cancel(cancel) => {
                self.keys.insert(id_key(
                    &family,
                    JobRecordKind::Spec,
                    cancel.job_id.to_bytes(),
                    0,
                ));
            }
            // The spec is selected by its digest, which no record key carries.
            JobFamilyRecord::Budget(_) => {
                self.kinds.insert(JobRecordKind::Spec);
            }
            // A receipt is keyed by its execution, so the one that seals this
            // launch is found only by reading every receipt of the family.
            JobFamilyRecord::Launch(launch) => {
                self.kinds.insert(JobRecordKind::Spec);
                self.kinds.insert(JobRecordKind::Receipt);
                self.keys
                    .insert(budget_key(&family, launch.scheduler_node_id));
            }
            JobFamilyRecord::Receipt(receipt) => {
                self.keys.insert(id_key(
                    &family,
                    JobRecordKind::Launch,
                    receipt.launch_id.to_bytes(),
                    0,
                ));
            }
            JobFamilyRecord::Update(update) => {
                self.keys.insert(id_key(
                    &family,
                    JobRecordKind::Receipt,
                    update.execution_id.to_bytes(),
                    0,
                ));
                if let Some(sequence) = update.sequence.checked_sub(1) {
                    self.keys.insert(id_key(
                        &family,
                        JobRecordKind::Update,
                        update.execution_id.to_bytes(),
                        sequence,
                    ));
                }
            }
            JobFamilyRecord::Output(output) => {
                self.keys.insert(id_key(
                    &family,
                    JobRecordKind::Receipt,
                    output.execution_id.to_bytes(),
                    0,
                ));
            }
        }
    }
}

/// The authentic predecessor records of one family, indexed for selection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvidenceSet {
    specs: Vec<LogicalJobSpec>,
    budgets: Vec<WitnessBudgetRecord>,
    launches: Vec<LaunchIntent>,
    receipts: Vec<ExecutionReceipt>,
    updates: Vec<ExecutionUpdate>,
}

impl EvidenceSet {
    /// Retains one authentic record. Records of the dependent kinds prove
    /// nothing for another record and are not retained here.
    pub fn insert(&mut self, record: &JobFamilyRecord) {
        match record {
            JobFamilyRecord::Spec(spec) => self.specs.push(spec.as_ref().clone()),
            JobFamilyRecord::Budget(budget) => self.budgets.push(*budget),
            JobFamilyRecord::Launch(launch) => self.launches.push(launch.as_ref().clone()),
            JobFamilyRecord::Receipt(receipt) => self.receipts.push(receipt.as_ref().clone()),
            JobFamilyRecord::Update(update) => self.updates.push(update.as_ref().clone()),
            JobFamilyRecord::Claim(_) | JobFamilyRecord::Output(_) | JobFamilyRecord::Cancel(_) => {
            }
        }
    }

    pub fn select(&self, record: &JobFamilyRecord) -> Evidence<'_> {
        match record {
            JobFamilyRecord::Spec(_) => Evidence::default(),
            JobFamilyRecord::Claim(claim) => Evidence {
                spec: self.spec_by_job(claim.job_id),
                ..Evidence::default()
            },
            JobFamilyRecord::Budget(budget) => Evidence {
                spec: self.spec_by_digest(budget.source_spec_digest),
                ..Evidence::default()
            },
            JobFamilyRecord::Launch(launch) => Evidence {
                spec: self.spec_by_digest(launch.spec_digest),
                budget: self.budget_by_node(launch.scheduler_node_id),
                receipt: self.receipt_by_launch(launch.launch_id),
                ..Evidence::default()
            },
            JobFamilyRecord::Receipt(receipt) => Evidence {
                launch: self.launch_by_id(receipt.launch_id),
                ..Evidence::default()
            },
            JobFamilyRecord::Update(update) => Evidence {
                receipt: self.receipt_by_execution(update.execution_id),
                previous_update: update
                    .sequence
                    .checked_sub(1)
                    .and_then(|sequence| self.update_by_sequence(update.execution_id, sequence)),
                ..Evidence::default()
            },
            JobFamilyRecord::Output(output) => Evidence {
                receipt: self.receipt_by_execution(output.execution_id),
                ..Evidence::default()
            },
            JobFamilyRecord::Cancel(cancel) => Evidence {
                spec: self.spec_by_job(cancel.job_id),
                ..Evidence::default()
            },
        }
    }

    fn spec_by_job(&self, job_id: JobId) -> Option<&LogicalJobSpec> {
        self.specs.iter().find(|spec| spec.job_id == job_id)
    }

    fn spec_by_digest(&self, digest: [u8; 32]) -> Option<&LogicalJobSpec> {
        self.specs.iter().find(|spec| spec.spec_digest == digest)
    }

    fn budget_by_node(&self, node_id: NodeId) -> Option<&WitnessBudgetRecord> {
        self.budgets
            .iter()
            .find(|budget| budget.scheduler_node_id == node_id)
    }

    fn launch_by_id(&self, launch_id: Ulid) -> Option<&LaunchIntent> {
        self.launches
            .iter()
            .find(|launch| launch.launch_id == launch_id)
    }

    fn receipt_by_launch(&self, launch_id: Ulid) -> Option<&ExecutionReceipt> {
        self.receipts
            .iter()
            .find(|receipt| receipt.launch_id == launch_id)
    }

    fn receipt_by_execution(&self, execution_id: Ulid) -> Option<&ExecutionReceipt> {
        self.receipts
            .iter()
            .find(|receipt| receipt.execution_id == execution_id)
    }

    fn update_by_sequence(&self, execution_id: Ulid, sequence: u64) -> Option<&ExecutionUpdate> {
        self.updates
            .iter()
            .find(|update| update.execution_id == execution_id && update.sequence == sequence)
    }
}
