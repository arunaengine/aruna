//! Finds rows stored as alpha.206 stored them, before compute backends had names: job family
//! records, job and launch records, node info documents with their queued copies, plan explains.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::families::exact;
use super::removals::outbox_record;
use super::session_refs::{self, Layout, SESSION_REF};
use super::tombstones::Stated;
use crate::explorer::ExplorerError;
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, ComputeReservationSnapshot, JobReservationRecord,
};
use aruna_core::compute::{ExecutorAvailability, ResourceEnvelope};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::id::NodeId;
use aruna_core::scheduling::PlannedInput;
use aruna_core::scheduling::eligibility::RejectionVerdict;
use aruna_core::scheduling::inputs::TargetScore;
use aruna_core::structs::execution::job::family::{
    LogicalJobSpec, SubmissionClaim, SubmissionId, WitnessBudgetRecord,
};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::launch::LaunchRecord;
use aruna_core::structs::execution::job::local::JobRecord;
use aruna_core::structs::execution::job::pages::PageManifest;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PlacementSubject};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
};
use aruna_ops_work::jobs::lifecycle::witness::arm::WitnessExplain;
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

/// Structs that gained trailing fields since alpha.206, such as the backend name or a job's local
/// admission, with the field count alpha.206 stored, after any `$older` entries.
macro_rules! alpha_206 {
    ($($older:expr),*) => {
        &[
            $($older,)*
            ("ExecutionSpec", 16),
            ("AttemptIntent", 5),
            ("LaunchRecord", 7),
            ("JobReservationRecord", 7),
            ("JobRecord", 28),
        ]
    };
}

pub(super) const UNNAMED: Layout = alpha_206!();
/// Job rows whose sessions predate the linked login, or the display name too; those releases
/// stored the rest of a job as alpha.206 did.
pub(super) const LOGIN_JOBS: Layout = alpha_206!((SESSION_REF, 3));
pub(super) const NAME_JOBS: Layout = alpha_206!((SESSION_REF, 2));

/// Submission, request digest and job of an alpha.206 spec, launch or receipt, read with the
/// `Tail` its keyspace stores after the envelope.
pub(super) fn family_ids<Tail>(value: &[u8]) -> Option<(SubmissionId, [u8; 32], JobId)>
where
    Tail: Serialize + DeserializeOwned,
{
    let (envelope, _) = exact::<(Envelope, Tail)>(value)
        .or_else(|| session_refs::decode::<(Envelope, Tail)>(value, UNNAMED).ok())?;
    match envelope.record {
        Record::Spec(spec) => Some((spec.submission_id, spec.request_digest, spec.job_id)),
        Record::Launch(launch) => {
            Some((launch.submission_id, launch.request_digest, launch.job_id))
        }
        Record::Receipt(receipt) => Some((
            receipt.submission_id,
            receipt.request_digest,
            receipt.job_id,
        )),
        Record::Claim(_) | Record::Budget(_) => None,
    }
}

/// Physical job and execution an alpha.206 receipt names on this node.
pub(super) fn receipt_jobs(value: &[u8]) -> Option<(JobId, Ulid)> {
    match exact::<Envelope>(value)?.record {
        Record::Receipt(receipt) => Some((receipt.physical_job_id, receipt.execution_id)),
        _ => None,
    }
}

/// The time an alpha.206 spec, claim, launch or receipt states, and a spec's submitter.
pub(super) fn stated(value: &[u8]) -> Option<Stated> {
    let envelope = exact::<Envelope>(value)
        .or_else(|| session_refs::decode::<Envelope>(value, UNNAMED).ok())?;
    Some(match envelope.record {
        Record::Spec(spec) => (Some(spec.created_at_ms), Some(spec.created_by)),
        Record::Claim(claim) => (Some(claim.accepted_at_ms), None),
        Record::Budget(_) => (None, None),
        Record::Launch(launch) => (Some(launch.created_at_ms), None),
        Record::Receipt(receipt) => (Some(receipt.accepted_at_ms), None),
    })
}

/// The input page manifest an alpha.206 spec or launch names.
pub(super) fn input_pages(value: &[u8]) -> Option<PageManifest> {
    let envelope = exact::<Envelope>(value)
        .or_else(|| session_refs::decode::<Envelope>(value, UNNAMED).ok())?;
    match envelope.record {
        Record::Spec(spec) => spec.input_pages,
        Record::Launch(launch) => launch.input_pages,
        _ => None,
    }
}

/// A job record stored as alpha.206 stored it, read into the current shape.
pub(super) fn old_job(value: &[u8]) -> Option<JobRecord> {
    match exact::<JobRecord>(value) {
        Some(_) => None,
        None => session_refs::decode::<JobRecord>(value, UNNAMED).ok(),
    }
}

/// Whether a launch record reads as alpha.206 stored it and not in the current layout.
pub(super) fn old_launch(value: &[u8]) -> bool {
    exact::<LaunchRecord>(value).is_none()
        && session_refs::decode::<LaunchRecord>(value, UNNAMED).is_ok()
}

/// Whether a reservation reads as alpha.206 stored it and not in the current layout.
pub(super) fn old_reservation(value: &[u8]) -> bool {
    exact::<JobReservationRecord>(value).is_none()
        && session_refs::decode::<JobReservationRecord>(value, UNNAMED).is_ok()
}

/// Whether a node info document reads as alpha.206 stored it and not in the current layout.
/// A document without executors reads the same in both and stays.
pub(super) fn old_document(value: &[u8]) -> bool {
    exact::<Document>(value).is_some() && exact::<NodeInfoDocument>(value).is_none()
}

/// Keys of the rows `old` finds stored as alpha.206 stored them.
pub(super) fn old_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    old: fn(&[u8]) -> bool,
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let mut keys = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        if old(&value) {
            keys.push(key.to_vec());
        }
    }
    Ok(keys)
}

/// Whether a plan explain reads as alpha.206 stored it and not in the current layout.
pub(super) fn old_explain(value: &[u8]) -> bool {
    exact::<Explain>(value).is_some() && exact::<WitnessExplain>(value).is_none()
}

/// Queued node info publishes that carry an alpha.206 document. An unreadable entry is left to
/// the other outbox steps, which refuse it.
pub(super) fn old_outbox(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
) -> Result<Vec<Vec<u8>>, ExplorerError> {
    let mut keys = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        let Ok(record) = outbox_record(&value) else {
            continue;
        };
        if let (DocumentTarget::NodeInfo { .. }, DocumentOutboxEvent::Upsert { bytes, .. }) =
            (&record.target, &record.event)
            && old_document(bytes)
        {
            keys.push(key.to_vec());
        }
    }
    Ok(keys)
}

/// `ExecutionTargetId` as alpha.134 and alpha.206 stored it, before the backend name.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Target {
    pub(super) node_id: NodeId,
    pub(super) executor_kind: String,
}

/// `LaunchIntent` as alpha.206 stored it.
#[derive(Serialize, Deserialize)]
struct Launch {
    launch_id: Ulid,
    submission_id: SubmissionId,
    request_digest: [u8; 32],
    job_id: JobId,
    scheduler_node_id: NodeId,
    scheduler_seq: u32,
    witness_placement: PlacementRef,
    holder_generation: u64,
    target: Target,
    inputs: Vec<PlannedInput>,
    input_pages: Option<PageManifest>,
    output_policies: Vec<PlacementPolicyRef>,
    plan_digest: [u8; 32],
    spec_digest: [u8; 32],
    created_at_ms: u64,
}

/// `ExecutionReceipt` as alpha.206 stored it.
#[derive(Serialize, Deserialize)]
struct Receipt {
    execution_id: Ulid,
    physical_job_id: JobId,
    launch_id: Ulid,
    launch_digest: [u8; 32],
    submission_id: SubmissionId,
    request_digest: [u8; 32],
    job_id: JobId,
    executor_node_id: NodeId,
    target: Target,
    spec_digest: [u8; 32],
    membership_generation: u64,
    subject_generation: u64,
    subject_digest: [u8; 32],
    accepted_at_ms: u64,
}

/// `JobFamilyRecord` up to its receipt; only launches and receipts differ from the current one.
#[derive(Serialize, Deserialize)]
enum Record {
    Spec(Box<LogicalJobSpec>),
    Claim(SubmissionClaim),
    Budget(WitnessBudgetRecord),
    Launch(Box<Launch>),
    Receipt(Box<Receipt>),
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    realm_id: RealmId,
    record: Record,
    published_by: NodeId,
    signature: iroh::Signature,
}

/// `ExecutorCapability` as alpha.206 stored it, before the backend name.
#[derive(Clone, Serialize, Deserialize)]
struct Capability {
    kind: String,
    file_staging: bool,
    direct_s3: bool,
    s3_mount: bool,
    network_policy: bool,
    session: bool,
    subject: PlacementSubject,
    subject_digest: [u8; 32],
    policy_draining: bool,
    limits: ResourceEnvelope,
    availability: Option<ExecutorAvailability>,
    compute_priority: u32,
}

/// `NodeInfoDocument` as alpha.206 stored it.
#[derive(Clone, Serialize, Deserialize)]
struct Document {
    node_id: NodeId,
    executors: Vec<Capability>,
    labels: BTreeMap<String, String>,
    urls: NodeUrls,
    utilization: NodeUtilization,
    updated_at_ms: u64,
    epoch: AdvertisementEpoch,
    compute_draining: bool,
    leaving: bool,
    demand: ComputeDemandSnapshot,
    reservation: ComputeReservationSnapshot,
}

/// `Selection` as alpha.206 stored it.
#[derive(Serialize, Deserialize)]
struct Selection {
    target: Target,
    subject_digest: [u8; 32],
    subject_generation: u64,
    score: TargetScore,
    inputs: Vec<PlannedInput>,
    output_policies: Vec<PlacementPolicyRef>,
    plan_digest: [u8; 32],
}

/// `ExecutionPlan` as alpha.206 stored it.
#[derive(Serialize, Deserialize)]
struct Plan {
    selected: Option<Selection>,
    retryable: bool,
    alternatives: Vec<(Target, TargetScore)>,
    rejected: Vec<(Target, RejectionVerdict)>,
    omitted: u32,
}

/// `WitnessExplain` as alpha.206 stored it.
#[derive(Serialize, Deserialize)]
struct Explain {
    sequence: u32,
    plan: Plan,
    declined: Vec<Target>,
    overlapping: bool,
    stored_at_ms: u64,
}

#[cfg(test)]
#[path = "compute_tests.rs"]
mod tests;
