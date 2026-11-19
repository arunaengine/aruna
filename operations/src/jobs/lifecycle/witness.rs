//! Runs leaderless witness scheduling: every family holder ranks itself and may offer a launch.
//! No witness holds a lease, so a partition can produce one execution per witness.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{DEADLINE_INDEX_KEYSPACE, WITNESS_DEADLINE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::page;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::{JobFamilyId, SubmissionId};
use aruna_core::types::{Key, Value};
use tracing::warn;

use super::LifecycleError;
use super::witness_arm::{
    ChangeDeadlineOperation, DEADLINE_KEY_BYTES, DeadlineChange, WitnessDeadline,
    deadline_index_key, deadline_key,
};
pub use super::witness_round::{RoundOutcome, WitnessNode, WitnessRoundOperation};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::store::store_call::RowRead;

/// Families one drain pass considers.
const WITNESS_DRAIN_BATCH: usize = 64;
/// Spacing between drain passes while deadlines remain.
pub const WITNESS_RETRY_AFTER: Duration = Duration::from_secs(1);

fn deadline_family(key: &[u8]) -> Option<JobFamilyId> {
    if key.len() != DEADLINE_KEY_BYTES {
        return None;
    }
    let submission: [u8; 32] = key.get(8..40)?.try_into().ok()?;
    let request_digest: [u8; 32] = key.get(40..72)?.try_into().ok()?;
    Some(JobFamilyId {
        submission_id: SubmissionId(submission),
        request_digest,
    })
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    Scan,
    Index {
        family: JobFamilyId,
        key: Key,
    },
    Round {
        family: JobFamilyId,
        key: Key,
        deadline: WitnessDeadline,
    },
    /// A change whose failure leaves work behind when `required`.
    Change {
        required: bool,
    },
}

/// One bounded pass over the persisted deadlines. Answers true while rows remain, so the
/// caller re-arms instead of spinning.
#[derive(Debug, PartialEq)]
pub struct WitnessDrainOperation {
    node: Option<WitnessNode>,
    now_ms: u64,
    /// Deadline rows still to handle, the next one last.
    rows: Vec<(Key, Value)>,
    remaining: bool,
    step: DrainStep,
}

type DrainNext = Result<Next<bool>, IoError>;

impl WitnessDrainOperation {
    pub fn new(node: Option<WitnessNode>, now_ms: u64) -> Self {
        Self {
            node,
            now_ms,
            rows: Vec::new(),
            remaining: false,
            step: DrainStep::Scan,
        }
    }

    fn next_row(&mut self) -> DrainNext {
        while let Some((key, _)) = self.rows.pop() {
            let Some(family) = deadline_family(&key) else {
                continue;
            };
            let read = RowRead::new(
                DEADLINE_INDEX_KEYSPACE,
                deadline_index_key(&family),
                |row: Option<Value>| {
                    Ok(row.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok()))
                },
            );
            self.step = DrainStep::Index { family, key };
            return Ok(Next::Call(IoCall::child(read)));
        }
        Ok(Next::Done(self.remaining))
    }

    fn change(&mut self, family: JobFamilyId, change: DeadlineChange, required: bool) -> DrainNext {
        self.step = DrainStep::Change { required };
        let operation = ChangeDeadlineOperation::new(family, change);
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn indexed(
        &mut self,
        family: JobFamilyId,
        key: Key,
        deadline: Option<WitnessDeadline>,
    ) -> DrainNext {
        let Some(deadline) = deadline else {
            return self.change(family, DeadlineChange::Stale { key }, true);
        };
        if deadline_key(&family, deadline.due_at_ms) != key {
            return self.change(family, DeadlineChange::Stale { key }, true);
        }
        if deadline.due_at_ms > self.now_ms {
            self.remaining = true;
            return self.next_row();
        }
        self.step = DrainStep::Round {
            family,
            key,
            deadline,
        };
        let round = WitnessRoundOperation::new(family, self.node.clone(), self.now_ms);
        Ok(Next::Call(IoCall::child(round)))
    }

    fn rounded(
        &mut self,
        (family, key, deadline): (JobFamilyId, Key, WitnessDeadline),
        outcome: RoundOutcome,
    ) -> DrainNext {
        match outcome {
            RoundOutcome::Done => {
                let clear = DeadlineChange::Clear {
                    key,
                    expected: deadline,
                };
                self.change(family, clear, true)
            }
            RoundOutcome::Retry { after_ms } => {
                self.remaining = true;
                let next = WitnessDeadline {
                    due_at_ms: self.now_ms + after_ms,
                    rank: deadline.rank,
                };
                let replace = DeadlineChange::Replace {
                    expected: Some(deadline),
                    next,
                };
                self.change(family, replace, false)
            }
        }
    }
}

impl Operation for WitnessDrainOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> DrainNext {
        Ok(Next::Call(IoCall::Page {
            space: WITNESS_DEADLINE_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: WITNESS_DRAIN_BATCH,
        }))
    }

    fn next(&mut self, result: IoResult) -> DrainNext {
        match std::mem::replace(&mut self.step, DrainStep::Scan) {
            DrainStep::Scan => {
                let (mut rows, next) = page(result)?;
                rows.reverse();
                self.rows = rows;
                self.remaining = next.is_some();
                self.next_row()
            }
            DrainStep::Index { family, key } => {
                match result.child::<Option<WitnessDeadline>, JobMutationError>()? {
                    Ok(deadline) => self.indexed(family, key, deadline),
                    Err(_) => {
                        self.remaining = true;
                        self.next_row()
                    }
                }
            }
            DrainStep::Round {
                family,
                key,
                deadline,
            } => match result.child::<RoundOutcome, IoError>()? {
                Ok(outcome) => self.rounded((family, key, deadline), outcome),
                Err(error) => {
                    warn!(error = %error, "Witness round did not finish");
                    self.remaining = true;
                    self.next_row()
                }
            },
            DrainStep::Change { required } => {
                let applied = result.child::<bool, LifecycleError>()?.unwrap_or(false);
                self.remaining |= required && !applied;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::tests::records::{Family, context};
    use aruna_core::calls::ChildOutcome;
    use aruna_storage::scan::iter_prefix_page;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    async fn change_deadline(
        context: &DriverContext,
        family: &JobFamilyId,
        change: DeadlineChange,
    ) -> bool {
        let operation = ChangeDeadlineOperation::new(*family, change);
        drive(operation, context).await.unwrap_or(false)
    }

    async fn drain_witness_deadlines(context: &DriverContext, now_ms: u64) -> bool {
        drive(WitnessDrainOperation::new(None, now_ms), context)
            .await
            .unwrap_or(true)
    }

    async fn read_deadline_index(
        context: &DriverContext,
        family: &JobFamilyId,
    ) -> Result<Option<WitnessDeadline>, JobMutationError> {
        let read = RowRead::new(
            DEADLINE_INDEX_KEYSPACE,
            deadline_index_key(family),
            |row: Option<Value>| {
                Ok(row.and_then(|bytes| from_bytes::<WitnessDeadline>(&bytes).ok()))
            },
        );
        drive(read, context).await
    }

    fn family() -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([1u8; 32]),
            request_digest: [2u8; 32],
        }
    }

    fn deadline(due_at_ms: u64) -> WitnessDeadline {
        WitnessDeadline { due_at_ms, rank: 1 }
    }

    fn scanned(due_at_ms: u64) -> WitnessDrainOperation {
        let mut drain = WitnessDrainOperation::new(None, 10);
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let rows = vec![(deadline_key(&family(), due_at_ms), Value::from(Vec::new()))];
        let scan = IoResult::Page { rows, next: None };
        assert!(matches!(drain.next(scan), Ok(Next::Call(IoCall::Child(_)))));
        drain
    }

    fn indexed(row: Option<WitnessDeadline>) -> IoResult {
        child(Ok::<_, JobMutationError>(row))
    }

    // A row the index no longer names is dropped, and a failed drop stays owed.
    #[test]
    fn stale_row_dropped() {
        let mut drain = scanned(5);
        assert!(matches!(
            drain.next(indexed(None)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(drain.step, DrainStep::Change { required: true });
        let failed = child(Ok::<_, LifecycleError>(false));
        assert!(matches!(drain.next(failed), Ok(Next::Done(true))));
    }

    // A deadline still ahead stays for a later pass.
    #[test]
    fn future_deadline_kept() {
        let mut drain = scanned(50);
        let done = drain.next(indexed(Some(deadline(50))));
        assert!(matches!(done, Ok(Next::Done(true))));
    }

    // A finished round clears its deadline and leaves nothing behind.
    #[test]
    fn finished_round_clears() {
        let mut drain = scanned(5);
        assert!(matches!(
            drain.next(indexed(Some(deadline(5)))),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let finished = child(Ok::<_, IoError>(RoundOutcome::Done));
        assert!(matches!(
            drain.next(finished),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let cleared = child(Ok::<_, LifecycleError>(true));
        assert!(matches!(drain.next(cleared), Ok(Next::Done(false))));
    }

    // A round to repeat moves the deadline, and rows remain even if the move fails.
    #[test]
    fn retried_round_remains() {
        let mut drain = scanned(5);
        drain.next(indexed(Some(deadline(5)))).unwrap();
        let again = child(Ok::<_, IoError>(RoundOutcome::Retry { after_ms: 7 }));
        drain.next(again).unwrap();
        assert_eq!(drain.step, DrainStep::Change { required: false });
        let failed = child(Err::<bool, _>(LifecycleError::NotFinished));
        assert!(matches!(drain.next(failed), Ok(Next::Done(true))));
    }

    #[tokio::test]
    async fn keeps_later_deadlines() {
        // A full pass must re-arm the drain for rows on its next page.
        let fixture = Family::new([1u8; 32]);
        let (_dir, ctx) = context(&fixture.config, fixture.holder.public()).await;
        for index in 0..=WITNESS_DRAIN_BATCH {
            let family = JobFamilyId {
                submission_id: SubmissionId([index as u8; 32]),
                request_digest: [index as u8; 32],
            };
            let next = WitnessDeadline {
                due_at_ms: 0,
                rank: index as u32,
            };
            let arm = DeadlineChange::Replace {
                expected: None,
                next,
            };
            assert!(change_deadline(&ctx, &family, arm).await);
        }

        assert!(drain_witness_deadlines(&ctx, 1).await);
        let (rows, _) = iter_prefix_page(
            &ctx.storage_handle,
            WITNESS_DEADLINE_KEYSPACE,
            None,
            None,
            WITNESS_DRAIN_BATCH,
            None,
        )
        .await
        .expect("deadlines scan");
        assert_eq!(rows.len(), 1);
        assert!(!drain_witness_deadlines(&ctx, 1).await);
    }

    #[tokio::test]
    async fn moves_deadline_once() {
        let fixture = Family::new([2u8; 32]);
        let (_dir, ctx) = context(&fixture.config, fixture.holder.public()).await;
        let family = fixture.family();
        let first = WitnessDeadline {
            due_at_ms: 20,
            rank: 1,
        };
        let second = WitnessDeadline {
            due_at_ms: 10,
            rank: 1,
        };
        let arm = DeadlineChange::Replace {
            expected: None,
            next: first,
        };
        assert!(change_deadline(&ctx, &family, arm).await);
        let moved = DeadlineChange::Replace {
            expected: Some(first),
            next: second,
        };
        assert!(change_deadline(&ctx, &family, moved).await);
        assert_eq!(
            read_deadline_index(&ctx, &family)
                .await
                .expect("deadline index read"),
            Some(second)
        );
        let (rows, _) = iter_prefix_page(
            &ctx.storage_handle,
            WITNESS_DEADLINE_KEYSPACE,
            None,
            None,
            WITNESS_DRAIN_BATCH,
            None,
        )
        .await
        .expect("deadline scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, deadline_key(&family, second.due_at_ms));
    }
}
