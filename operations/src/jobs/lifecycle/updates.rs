//! The monotonic execution chain one executor publishes.
//!
//! Only the node fenced by its own attempt control may advance an execution.
//! Each update chains by digest from the receipt, so a gap cannot silently skip
//! a state or forge a terminal result, and terminal success may only name an
//! output record that is already durable.

use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{
    ExecutionReceipt, ExecutionUpdate, JobErrorKind, JobFamilyId, JobFamilyRecord, JobId,
    JobRecord, JobRecordBody, JobRecordEnvelope, JobRecordKind, JobResultPayload, JobState,
    PhysicalExecutionResult, PhysicalExecutionState, ResultMessage,
};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;
use tracing::{debug, warn};
use ulid::Ulid;

use super::reservation::{ReleaseExecutionOperation, held_reservations, job_reservation};
use super::routing::family_of_alias;
use super::witness::arm_family;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::{
    Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_kind_complete,
};
use crate::jobs::store::read_job_record;

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
/// A job with no reservation is purely local and has no chain at all.
pub async fn execution_chain(context: &DriverContext, job_id: JobId) -> Option<ExecutionChain> {
    let reservation = local_reservation(context, job_id).await?;
    chain_for(
        context,
        reservation.logical_job_id,
        reservation.execution_id,
    )
    .await
}

/// The chain of one local job's exact execution. The reservation is what maps
/// the local physical job row to the logical alias family records are keyed by,
/// so a caller holding only the physical id must resolve through it.
pub async fn chain_of_attempt(
    context: &DriverContext,
    job_id: JobId,
    execution_id: Ulid,
) -> Option<ExecutionChain> {
    let reservation = local_reservation(context, job_id).await?;
    if reservation.execution_id != execution_id {
        warn!(
            job_id = %job_id,
            execution_id = %execution_id,
            reserved = %reservation.execution_id,
            "Attempt does not hold the reserved execution; the chain stays unresolved"
        );
        return None;
    }
    chain_for(context, reservation.logical_job_id, execution_id).await
}

/// The reservation one local job holds. `None` without a warning is the purely
/// local case; a read that failed is reported instead of read as absence.
async fn local_reservation(
    context: &DriverContext,
    job_id: JobId,
) -> Option<super::reservation::ExecutionReservation> {
    match job_reservation(context, job_id).await {
        Ok(reservation) => reservation,
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Execution reservation read failed");
            None
        }
    }
}

/// The chain of one exact execution id, used where the attempt control already
/// names it. `job_id` is the logical alias, never a local physical job id.
pub async fn chain_for(
    context: &DriverContext,
    job_id: JobId,
    execution_id: Ulid,
) -> Option<ExecutionChain> {
    let family = match family_of_alias(context, job_id).await {
        Ok(Some(family)) => family,
        Ok(None) => {
            warn!(
                job_id = %job_id,
                execution_id = %execution_id,
                "No job family alias for this job; the execution chain is unresolvable"
            );
            return None;
        }
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Job family alias read failed");
            return None;
        }
    };
    let Some(receipt) = receipt_of(context, family, execution_id).await else {
        warn!(
            job_id = %job_id,
            execution_id = %execution_id,
            "No receipt for this execution in its family"
        );
        return None;
    };
    let digest = receipt
        .digest()
        .inspect_err(|error| warn!(error = %error, "Execution receipt digest failed"))
        .ok()?;
    Some(ExecutionChain {
        family,
        execution_id,
        executor_node_id: receipt.executor_node_id,
        spec_digest: receipt.spec_digest,
        receipt_digest: digest,
        job_id: receipt.job_id,
    })
}

/// The receipt one execution was admitted under, read from the receipt kind
/// alone so unrelated family history can never hide it behind the read bound.
async fn receipt_of(
    context: &DriverContext,
    family: JobFamilyId,
    execution_id: Ulid,
) -> Option<ExecutionReceipt> {
    let records = load_kind_complete(context, family, JobRecordKind::Receipt)
        .await
        .inspect_err(|error| warn!(error = %error, "Execution receipt read is incomplete"))
        .ok()?;
    records.iter().find_map(|envelope| match &envelope.record {
        JobFamilyRecord::Receipt(receipt) if receipt.execution_id == execution_id => {
            Some(receipt.as_ref().clone())
        }
        _ => None,
    })
}

/// The next sequence and predecessor digest of a chain proven contiguous from
/// `root`. `None` is a gap, a duplicate sequence, or a broken predecessor: the
/// chain cannot be extended without forging one of them.
fn chain_tip(updates: &[&ExecutionUpdate], root: [u8; 32]) -> Option<(u64, [u8; 32])> {
    let mut ordered: Vec<&ExecutionUpdate> = updates.to_vec();
    ordered.sort_by_key(|update| update.sequence);
    let mut previous = root;
    for (index, update) in ordered.iter().enumerate() {
        if update.sequence != index as u64 || update.previous_digest != previous {
            return None;
        }
        previous = update.digest().ok()?;
    }
    let next = ordered
        .last()
        .map_or(0, |update| update.sequence.saturating_add(1));
    Some((next, previous))
}

/// Publishes one state of the local execution. The sequence and the previous
/// digest are derived from the records already stored, so a replay of the same
/// state is idempotent and a lost publication never breaks the chain.
pub async fn publish_state(
    context: &DriverContext,
    chain: &ExecutionChain,
    state: PhysicalExecutionState,
    result: Option<PhysicalExecutionResult>,
    observed_at_ms: u64,
) -> bool {
    let Some(net) = context.net_handle.as_ref() else {
        warn!(
            job_id = %chain.job_id,
            execution_id = %chain.execution_id,
            "Execution update needs a net handle; publication abandoned"
        );
        return false;
    };
    let local = net.node_id();
    if chain.executor_node_id != local {
        warn!(
            job_id = %chain.job_id,
            execution_id = %chain.execution_id,
            executor = %chain.executor_node_id,
            "Only the receipted executor may publish this execution"
        );
        return false;
    }
    // The chain is extended only from evidence proven complete: the receipt
    // that authorized it, plus every stored update of this execution.
    let Some(receipt) = receipt_of(context, chain.family, chain.execution_id).await else {
        warn!(
            job_id = %chain.job_id,
            execution_id = %chain.execution_id,
            "Execution receipt is no longer readable; publication deferred"
        );
        return false;
    };
    let Ok(root) = receipt.digest() else {
        warn!(execution_id = %chain.execution_id, "Execution receipt digest failed");
        return false;
    };
    let records = match load_kind_complete(context, chain.family, JobRecordKind::Update).await {
        Ok(records) => records,
        Err(error) => {
            warn!(error = %error, "Execution update read is incomplete; publication deferred");
            return false;
        }
    };
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
    let Some((sequence, previous)) = chain_tip(&mine, root) else {
        warn!(
            job_id = %chain.job_id,
            execution_id = %chain.execution_id,
            "Execution update chain is not contiguous"
        );
        return false;
    };
    let update = ExecutionUpdate {
        execution_id: chain.execution_id,
        submission_id: chain.family.submission_id,
        request_digest: chain.family.request_digest,
        executor_node_id: local,
        sequence,
        previous_digest: previous,
        state,
        observed_at_ms,
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
        warn!(execution_id = %chain.execution_id, "Execution update exceeds the record bounds");
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
        Ok(outcome)
            if matches!(
                outcome.admission,
                Admission::Authentic | Admission::Duplicate
            ) =>
        {
            debug!(state = state.name(), "Execution update published");
            // An infrastructure error ends this execution without deciding the
            // job, so the family must be planned again from here too.
            if state == PhysicalExecutionState::Error {
                arm_family(context, chain.family, unix_timestamp_millis()).await;
            }
            super::outbox::kick(context).await;
            true
        }
        Ok(outcome) => {
            warn!(
                job_id = %chain.job_id,
                execution_id = %chain.execution_id,
                admission = ?outcome.admission,
                "Execution update was not admitted"
            );
            false
        }
        Err(error) => {
            warn!(
                job_id = %chain.job_id,
                execution_id = %chain.execution_id,
                error = %error,
                "Execution update append failed"
            );
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
    publish_state(context, &chain, state, None, unix_timestamp_millis()).await;
}

/// Publishes the terminal state of a receipted execution and releases its
/// reservation. Success names the digest of the output record sealed before it,
/// so a success can never be projected without its exact outputs.
pub async fn publish_terminal(context: &DriverContext, record: &JobRecord) -> bool {
    let Some(state) = terminal_state(record) else {
        return true;
    };
    let reservation = match job_reservation(context, record.job_id).await {
        Ok(Some(reservation)) => reservation,
        // No reservation is a purely local execution: it owes nothing here.
        Ok(None) => return true,
        Err(error) => {
            warn!(
                job_id = %record.job_id,
                error = %error,
                "Execution reservation read failed; terminal publication deferred"
            );
            return false;
        }
    };
    let Some(chain) = chain_for(
        context,
        reservation.logical_job_id,
        reservation.execution_id,
    )
    .await
    else {
        warn!(
            job_id = %record.job_id,
            execution_id = %reservation.execution_id,
            "Terminal publication deferred: the receipt chain is unresolved"
        );
        return false;
    };
    let result = PhysicalExecutionResult {
        exit_code: exit_code(record.result.as_ref()),
        output_digest: output_digest(record.result.as_ref()),
        message: record
            .last_error
            .as_ref()
            .and_then(|error| ResultMessage::new(error.message.clone()).ok()),
    };
    let observed_at_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
    if !publish_state(context, &chain, state, Some(result), observed_at_ms).await {
        return false;
    }
    if let Err(error) = drive(ReleaseExecutionOperation::new(chain.execution_id), context).await {
        warn!(error = %error, "Execution reservation release failed");
        return false;
    }
    true
}

/// Retries terminal publication and capacity release for every durable local
/// terminal execution that still has a reservation.
pub async fn settle_terminals(context: &DriverContext) -> Result<(), String> {
    for reservation in held_reservations(context).await? {
        let Some(record) =
            read_job_record(&context.storage_handle, reservation.job_id, None).await?
        else {
            continue;
        };
        if record.is_settled() && !publish_terminal(context, &record).await {
            return Err(format!(
                "terminal obligation for execution {} remains pending",
                reservation.execution_id
            ));
        }
    }
    Ok(())
}

fn terminal_state(record: &JobRecord) -> Option<PhysicalExecutionState> {
    match record.state {
        JobState::Succeeded => Some(PhysicalExecutionState::Succeeded),
        JobState::Failed
            if record
                .last_error
                .as_ref()
                .is_some_and(|error| error.kind == JobErrorKind::Permanent) =>
        {
            Some(PhysicalExecutionState::Failed)
        }
        JobState::Failed => Some(PhysicalExecutionState::Error),
        JobState::Cancelled => Some(PhysicalExecutionState::Cancelled),
        // Local exhaustion is an error the family may still resolve elsewhere.
        JobState::Indeterminate if record.locally_exhausted => Some(PhysicalExecutionState::Error),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::records::tests::fixture::{Family, node, payload, user};
    use aruna_core::structs::{JobError, JobPayload};

    fn receipt(family: &Family) -> ExecutionReceipt {
        let spec = family.spec();
        family.receipt(&family.launch(&spec, family.holder.public(), 0), 1)
    }

    fn terminal(error: JobError) -> JobRecord {
        let mut record = JobRecord::new(
            JobId::from_bytes([4u8; 16]),
            JobPayload::Execution(payload()),
            user(),
            node(1),
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Failed;
        record.last_error = Some(error);
        record
    }

    #[test]
    fn infra_failure_is_error() {
        // Only an authenticated permanent, job-specific failure may replicate as
        // `failed`; a retryable one stays an infrastructure `error`.
        assert_eq!(
            terminal_state(&terminal(JobError::permanent(
                "container exited with code 1"
            ))),
            Some(PhysicalExecutionState::Failed)
        );
        assert_eq!(
            terminal_state(&terminal(JobError::retryable(
                "backend infrastructure failure"
            ))),
            Some(PhysicalExecutionState::Error)
        );
    }

    #[test]
    fn extends_valid_chain() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let tip = first.digest().expect("update digest");

        assert_eq!(chain_tip(&[], root), Some((0, root)));
        assert_eq!(chain_tip(&[&first], root), Some((1, tip)));
    }

    #[test]
    fn rejects_sequence_gap() {
        // The next sequence must come from the proven chain, never from how many
        // updates happened to load.
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let third = family.update(
            &receipt,
            2,
            first.digest().expect("update digest"),
            PhysicalExecutionState::Succeeded,
            None,
        );

        assert_eq!(chain_tip(&[&first, &third], root), None);
    }

    #[test]
    fn rejects_duplicate_sequence() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let twin = family.update(&receipt, 0, root, PhysicalExecutionState::Cancelled, None);

        assert_eq!(chain_tip(&[&first, &twin], root), None);
    }

    #[test]
    fn rejects_broken_predecessor() {
        let family = Family::new([3u8; 32]);
        let receipt = receipt(&family);
        let root = receipt.digest().expect("receipt digest");
        let first = family.update(&receipt, 0, root, PhysicalExecutionState::Running, None);
        let second = family.update(
            &receipt,
            1,
            [9u8; 32],
            PhysicalExecutionState::Succeeded,
            None,
        );

        assert_eq!(chain_tip(&[&first, &second], root), None);
        assert_eq!(chain_tip(&[&first], [9u8; 32]), None);
    }
}
