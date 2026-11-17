//! Rechecks the events waiting on this receiver: a ready one becomes a queued run in the slot
//! it held, or is denied; one past its waiting expiry ends `expired` with a gap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::keyspaces::{PLUGIN_QUEUE_KEYSPACE, PLUGIN_WAITING_KEYSPACE};
use aruna_core::plugin_runs::GapReason;

use super::decide::{GapCount, Verdict, WaitingRow, seen_row};
use super::rows::{Answer, Ask, Plan, Row, StoreError, Turn, done};
use super::runs::{Admission, RunError};
use aruna_core::plugin_runs::Outcome;

/// Frees a waiting event's slot and decides it in one transaction, so the slot never counts
/// twice: a ready run takes the slot, a denial is recorded, an expired event is counted as a gap.
#[derive(Debug, PartialEq)]
pub struct SettleWaiting {
    key: Vec<u8>,
    waiting: Box<WaitingRow>,
    now_ms: u64,
    phase: SettlePhase,
}

#[derive(Debug, PartialEq)]
enum SettlePhase {
    Freeing(Box<Option<Verdict>>, GapCount),
    Admitting(Admission),
    Counting(GapCount),
    Writing,
}

impl SettleWaiting {
    pub(crate) fn new(
        key: Vec<u8>,
        waiting: WaitingRow,
        verdict: Option<Verdict>,
        (local, now_ms): (NodeId, u64),
    ) -> Self {
        let gap = GapCount::new(local, &waiting.row, GapReason::Waited);
        Self {
            key,
            waiting: Box::new(waiting),
            now_ms,
            phase: SettlePhase::Freeing(Box::new(verdict), gap),
        }
    }

    fn record(&mut self, outcome: Outcome, mut rows: Vec<Row>) -> Result<Turn<()>, StoreError> {
        rows.push(seen_row(&self.waiting.row, &outcome, self.now_ms)?);
        self.phase = SettlePhase::Writing;
        Ok(Turn::Ask(Ask::Write(rows)))
    }
}

impl Plan for SettleWaiting {
    type Output = ();
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<()>, StoreError> {
        Ok(Turn::Ask(Ask::Delete(vec![
            (PLUGIN_WAITING_KEYSPACE, self.key.clone()),
            (PLUGIN_QUEUE_KEYSPACE, self.key.clone()),
        ])))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<()>, StoreError> {
        match std::mem::replace(&mut self.phase, SettlePhase::Writing) {
            SettlePhase::Freeing(verdict, gap) => {
                done(answer)?;
                match *verdict {
                    Some(Verdict::Run(run)) => {
                        let admission = Admission::new(*run);
                        let ask = admission.ask();
                        self.phase = SettlePhase::Admitting(admission);
                        Ok(Turn::Ask(ask))
                    }
                    Some(Verdict::Denied(reason)) => self.record(Outcome::Denied(reason), vec![]),
                    _ => {
                        let ask = gap.ask();
                        self.phase = SettlePhase::Counting(gap);
                        Ok(Turn::Ask(ask))
                    }
                }
            }
            SettlePhase::Admitting(mut admission) => match admission.take(answer) {
                Ok(None) => {
                    let ask = admission.ask();
                    self.phase = SettlePhase::Admitting(admission);
                    Ok(Turn::Ask(ask))
                }
                Ok(Some(rows)) => self.record(Outcome::Queued, rows),
                Err(RunError::Store(error)) => Err(error),
                Err(error) => Err(StoreError(error.to_string())),
            },
            SettlePhase::Counting(mut gap) => {
                let counted = gap.take(answer)?;
                self.record(Outcome::Expired, counted.into_iter().collect())
            }
            SettlePhase::Writing => {
                done(answer)?;
                Ok(Turn::Finish(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "waiting_tests.rs"]
mod tests;
