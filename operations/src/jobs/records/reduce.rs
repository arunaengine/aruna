//! The deterministic reducer.
//!
//! It reads only immutable authentic records. Arrival order, arrival batching,
//! duplication, the responder's clock, its local tasks, and its reachability are
//! never inputs, so every replica holding the same record set produces the same
//! projection.

use std::collections::BTreeMap;

use aruna_core::structs::{
    ExecutionOutputRecord, ExecutionReceipt, ExecutionRole, ExecutionUpdate, JobFamilyId,
    JobFamilyRecord, JobId, JobProjection, JobRecordBody, JobRecordEnvelope, JobRecordError,
    LogicalJobState, OutputSet, PhysicalExecutionState, ProjectedExecution, SubmissionClaim,
    canonical_execution_key, verify_update_chain,
};
use ulid::Ulid;

/// One physical execution as its own records describe it.
#[derive(Debug, Default)]
struct ExecutionRecords {
    receipt: Option<ExecutionReceipt>,
    updates: Vec<ExecutionUpdate>,
    output: Option<ExecutionOutputRecord>,
}

/// Reduces one request family to its single projection. `None` means the family
/// has no accepted alias yet, so there is no logical job to project.
pub fn reduce_family(
    family: JobFamilyId,
    records: &[JobRecordEnvelope],
) -> Result<Option<JobProjection>, JobRecordError> {
    let mut claims: Vec<SubmissionClaim> = Vec::new();
    let mut executions: BTreeMap<Ulid, ExecutionRecords> = BTreeMap::new();
    let mut cancelled = false;

    for envelope in records.iter().filter(|record| record.family() == family) {
        match &envelope.record {
            JobFamilyRecord::Claim(claim) => claims.push(*claim),
            JobFamilyRecord::Receipt(receipt) => {
                executions.entry(receipt.execution_id).or_default().receipt =
                    Some(receipt.as_ref().clone());
            }
            JobFamilyRecord::Update(update) => executions
                .entry(update.execution_id)
                .or_default()
                .updates
                .push(update.as_ref().clone()),
            JobFamilyRecord::Output(output) => {
                executions.entry(output.execution_id).or_default().output =
                    Some(output.as_ref().clone());
            }
            JobFamilyRecord::Cancel(_) => cancelled = true,
            JobFamilyRecord::Spec(_) | JobFamilyRecord::Budget(_) | JobFamilyRecord::Launch(_) => {}
        }
    }

    claims.sort_by_key(SubmissionClaim::order_key);
    claims.dedup_by_key(|claim| claim.job_id);
    let Some(canonical_claim) = claims.first() else {
        return Ok(None);
    };
    let aliases: Vec<JobId> = claims.iter().map(|claim| claim.job_id).collect();

    let mut projected: Vec<ProjectedExecution> = Vec::with_capacity(executions.len());
    let mut successes: Vec<(Ulid, [u8; 32])> = Vec::new();
    let mut failures: Vec<(Ulid, [u8; 32])> = Vec::new();
    let mut active = false;
    for (execution_id, collected) in &executions {
        let Some(receipt) = &collected.receipt else {
            // A receipt is the only proof that an execution exists at all.
            continue;
        };
        let output_digest = collected
            .output
            .as_ref()
            .map(|output| output.digest())
            .transpose()?;
        let chain = verify_update_chain(receipt.digest()?, output_digest, &collected.updates)?;
        let state = chain
            .last()
            .map_or(PhysicalExecutionState::Accepted, |update| update.state);
        let observed_at_ms = chain.last().map(|update| update.observed_at_ms);
        let result = chain.last().and_then(|update| update.result.clone());
        if !state.is_terminal() {
            active = true;
        }
        if state == PhysicalExecutionState::Succeeded {
            successes.push((
                *execution_id,
                canonical_execution_key(family.submission_id, family.request_digest, *execution_id),
            ));
        } else if state == PhysicalExecutionState::Failed {
            failures.push((
                *execution_id,
                canonical_execution_key(family.submission_id, family.request_digest, *execution_id),
            ));
        }
        projected.push(ProjectedExecution {
            execution_id: *execution_id,
            executor_node_id: receipt.executor_node_id,
            state,
            role: ExecutionRole::Redundant,
            observed_at_ms,
            result,
        });
    }

    successes.sort_by_key(|(_, key)| *key);
    failures.sort_by_key(|(_, key)| *key);
    let success = successes.first().map(|(execution_id, _)| *execution_id);
    let failure = failures.first().map(|(execution_id, _)| *execution_id);
    let canonical = success.or(failure);
    for execution in &mut projected {
        execution.role = match (canonical, execution.state) {
            (Some(id), _) if id == execution.execution_id => ExecutionRole::Canonical,
            (_, PhysicalExecutionState::Succeeded) => ExecutionRole::DuplicateSuccess,
            _ => ExecutionRole::Redundant,
        };
    }

    let outputs = success
        .and_then(|execution_id| executions.get(&execution_id))
        .and_then(|collected| collected.output.as_ref())
        .map(|output| output.outputs.clone())
        .map_or_else(|| OutputSet::new(Vec::new()), Ok)?;

    Ok(Some(JobProjection {
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        canonical_job_id: canonical_claim.job_id,
        aliases,
        state: logical_state(
            success.is_some(),
            failure.is_some(),
            cancelled,
            active,
            !projected.is_empty(),
        ),
        canonical_execution_id: canonical,
        executions: projected,
        outputs,
        cancel_requested: cancelled,
    }))
}

/// Failure needs a signed permanent execution result. Infrastructure errors and
/// absence stay indeterminate because neither proves a realm-wide outcome.
fn logical_state(
    succeeded: bool,
    failed: bool,
    cancelled: bool,
    active: bool,
    executed: bool,
) -> LogicalJobState {
    match (succeeded, failed, cancelled, active, executed) {
        (true, _, _, _, _) => LogicalJobState::Succeeded,
        (false, true, _, _, _) => LogicalJobState::Failed,
        (false, false, true, false, _) => LogicalJobState::Cancelled,
        (false, false, _, true, _) => LogicalJobState::Running,
        (false, false, _, false, true) => LogicalJobState::Indeterminate,
        (false, false, false, false, false) => LogicalJobState::Queued,
    }
}

/// The canonical idempotency binding of one submission: the family holding the
/// globally smallest claim key. A different request digest under the same
/// submission is an idempotency conflict and keeps its own projection.
pub fn canonical_binding(records: &[JobRecordEnvelope]) -> Option<JobFamilyId> {
    records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Claim(claim) => Some((claim.order_key(), envelope.family())),
            _ => None,
        })
        .min_by_key(|(order, _)| *order)
        .map(|(_, family)| family)
}

/// Every request family one submission has accepted a claim for, in claim-key
/// order: the canonical binding first, then the idempotency conflicts.
pub fn submission_families(records: &[JobRecordEnvelope]) -> Vec<JobFamilyId> {
    let mut ordered: Vec<([u8; 32], JobFamilyId)> = records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Claim(claim) => Some((claim.order_key(), envelope.family())),
            _ => None,
        })
        .collect();
    ordered.sort_by_key(|(order, _)| *order);
    let mut families: Vec<JobFamilyId> = Vec::new();
    for (_, family) in ordered {
        if !families.contains(&family) {
            families.push(family);
        }
    }
    families
}
