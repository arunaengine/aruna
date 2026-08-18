//! The monotonic execution chain one executor publishes.
//!
//! Only the node fenced by its own attempt control may advance an execution.
//! Each update chains by digest from the receipt, so a gap cannot silently skip
//! a state or forge a terminal result, and terminal success may only name an
//! output record that is already durable.

use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{
    ExecutionUpdate, JobFamilyId, JobFamilyRecord, JobId, JobRecord, JobRecordBody,
    JobRecordEnvelope, JobResultPayload, JobState, PhysicalExecutionResult, PhysicalExecutionState,
    ResultMessage,
};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;
use tracing::{debug, warn};
use ulid::Ulid;

use super::reservation::{ReleaseExecutionOperation, job_reservation};
use super::routing::family_of_alias;
use super::witness::load_family;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};

/// The replicated identity of one physical execution, read back from the
/// receipt that authorized it. Without it there is no distributed chain to
/// extend and every publication is skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionChain {
    pub family: JobFamilyId,
    pub execution_id: Ulid,
    pub executor_node_id: NodeId,
    pub spec_digest: [u8; 32],
    pub receipt_digest: [u8; 32],
    pub job_id: JobId,
}

/// Resolves the receipt chain of one local job, if a target admitted it here.
pub async fn execution_chain(context: &DriverContext, job_id: JobId) -> Option<ExecutionChain> {
    let execution_id = job_reservation(context, job_id).await?.execution_id;
    chain_for(context, job_id, execution_id).await
}

/// The chain of one exact execution id, used where the attempt control already
/// names it.
pub async fn chain_for(
    context: &DriverContext,
    job_id: JobId,
    execution_id: Ulid,
) -> Option<ExecutionChain> {
    let family = family_of_alias(context, job_id).await?;
    let records = load_family(context, family).await;
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Receipt(receipt) if receipt.execution_id == execution_id => {
            Some(ExecutionChain {
                family,
                execution_id,
                executor_node_id: receipt.executor_node_id,
                spec_digest: receipt.spec_digest,
                receipt_digest: receipt.digest().ok()?,
                job_id: receipt.job_id,
            })
        }
        _ => None,
    })
}

/// Publishes one state of the local execution. The sequence and the previous
/// digest are derived from the records already stored, so a replay of the same
/// state is idempotent and a lost publication never breaks the chain.
pub async fn publish_state(
    context: &DriverContext,
    chain: &ExecutionChain,
    state: PhysicalExecutionState,
    result: Option<PhysicalExecutionResult>,
) -> bool {
    let Some(net) = context.net_handle.as_ref() else {
        return false;
    };
    let local = net.node_id();
    if chain.executor_node_id != local {
        return false;
    }
    let records = load_family(context, chain.family).await;
    let mine: Vec<&ExecutionUpdate> = records
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Update(update) if update.execution_id == chain.execution_id => {
                Some(update.as_ref())
            }
            _ => None,
        })
        .collect();
    if mine.iter().any(|update| update.state == state) {
        return true;
    }
    let previous = mine
        .iter()
        .max_by_key(|update| update.sequence)
        .map(|update| update.digest().unwrap_or_default())
        .unwrap_or(chain.receipt_digest);
    let update = ExecutionUpdate {
        execution_id: chain.execution_id,
        submission_id: chain.family.submission_id,
        request_digest: chain.family.request_digest,
        executor_node_id: local,
        sequence: mine.len() as u64,
        previous_digest: previous,
        state,
        observed_at_ms: unix_timestamp_millis(),
        result,
    };
    let envelope = match JobRecordEnvelope::signed_with(
        *net.realm_id(),
        JobFamilyRecord::Update(Box::new(update)),
        local,
        |message| net.sign(message),
    ) {
        Ok(envelope) => envelope,
        Err(error) => {
            warn!(error = %error, "Execution update signing failed");
            return false;
        }
    };
    let Ok(frame) = JobRecordFrame::new(envelope) else {
        return false;
    };
    let appended = drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: *net.realm_id(),
            local_node_id: local,
            record: frame,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: unix_timestamp_millis(),
        }),
        context,
    )
    .await;
    match appended {
        Ok(_) => {
            debug!(state = state.name(), "Execution update published");
            super::outbox::kick(context).await;
            true
        }
        Err(error) => {
            warn!(error = %error, "Execution update append failed");
            false
        }
    }
}

/// Publishes a non-terminal state of a receipted execution. A job with no
/// receipt here is a purely local execution and publishes nothing.
pub async fn publish_progress(
    context: &DriverContext,
    job_id: JobId,
    state: PhysicalExecutionState,
) {
    let Some(chain) = execution_chain(context, job_id).await else {
        return;
    };
    publish_state(context, &chain, state, None).await;
}

/// Publishes the terminal state of a receipted execution and releases its
/// reservation. Success names the digest of the output record sealed before it,
/// so a success can never be projected without its exact outputs.
pub async fn publish_terminal(context: &DriverContext, record: &JobRecord) {
    let Some(state) = terminal_state(record.state) else {
        return;
    };
    let Some(chain) = execution_chain(context, record.job_id).await else {
        return;
    };
    let result = PhysicalExecutionResult {
        exit_code: exit_code(record.result.as_ref()),
        output_digest: output_digest(record.result.as_ref()),
        message: record
            .last_error
            .as_ref()
            .and_then(|error| ResultMessage::new(error.message.clone()).ok()),
    };
    publish_state(context, &chain, state, Some(result)).await;
    if let Err(error) = drive(ReleaseExecutionOperation::new(chain.execution_id), context).await {
        warn!(error = %error, "Execution reservation release failed");
    }
}

fn terminal_state(state: JobState) -> Option<PhysicalExecutionState> {
    match state {
        JobState::Succeeded => Some(PhysicalExecutionState::Succeeded),
        JobState::Failed => Some(PhysicalExecutionState::Failed),
        JobState::Cancelled => Some(PhysicalExecutionState::Cancelled),
        _ => None,
    }
}

fn exit_code(result: Option<&JobResultPayload>) -> Option<i32> {
    match result {
        Some(JobResultPayload::Execution { exit_code, .. }) => *exit_code,
        _ => None,
    }
}

fn output_digest(result: Option<&JobResultPayload>) -> Option<[u8; 32]> {
    match result {
        Some(JobResultPayload::Execution { output_digest, .. }) => *output_digest,
        _ => None,
    }
}
