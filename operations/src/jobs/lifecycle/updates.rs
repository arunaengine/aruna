//! Publishes the monotonic execution chain, each update chained by digest from the receipt.
//! Only the fenced node may advance a chain, and success may name only a durable output record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::structs::execution::job::{
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use tracing::{debug, warn};

use super::chain::{ExecutionChain, ExecutionChainOperation, receipt_in};
use super::held::HeldReservations;
use super::reservation::ReleaseExecutionOperation;
use crate::driver::{DriverContext, drive};
use crate::jobs::records::admit::Admission;

/// Spacing between retries of a terminal publication that stayed pending.
pub const SETTLE_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Asks the settle task to retry the terminal obligations still held here.
pub fn schedule_terminal_settle(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::SettleJobTerminals,
        after,
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

pub async fn publish_state(
    context: &DriverContext,
    chain: &ExecutionChain,
    state: PhysicalExecutionState,
    result: Option<PhysicalExecutionResult>,
    observed_at_ms: u64,
) -> bool {
}

/// Publishes a non-terminal state of a receipted execution. A job with no
/// receipt here is a purely local execution and publishes nothing.
pub async fn publish_progress(
    context: &DriverContext,
    job_id: JobId,
    state: PhysicalExecutionState,
) {
    let Ok(Some(chain)) = drive(ExecutionChainOperation::of_job(job_id), context).await else {
        return;
    };
    publish_state(context, &chain, state, None, unix_timestamp_millis()).await;
}

pub async fn publish_terminal(context: &DriverContext, record: &JobRecord) -> bool {
}

/// Retries terminal publication and capacity release for every durable local
/// terminal execution that still has a reservation. `true` means at least one
/// obligation stayed pending, so the caller must run this again.
pub async fn settle_terminals(context: &DriverContext) -> Result<bool, String> {
        }
    }
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

/// Bounded tails the replicated result carries, so every node answers a status
/// read with the logs instead of only the node that ran the container.
fn log_tails(result: Option<&JobResultPayload>) -> (Option<ResultMessage>, Option<ResultMessage>) {
    match result {
        Some(JobResultPayload::Execution { stdout, stderr, .. }) => {
            (ResultMessage::tail(stdout), ResultMessage::tail(stderr))
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::tests::records::{Family, node, payload, user};
    use aruna_core::structs::execution::job::{ExecutionReceipt, JobError, JobPayload};

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
    fn retryable_maps_error() {
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
