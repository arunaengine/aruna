//! Rechecks the events waiting on this receiver: a ready one becomes a queued run in the slot
//! it held, or is denied; one past its waiting expiry ends `expired` with a gap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{PLUGIN_QUEUE_KEYSPACE, PLUGIN_WAITING_KEYSPACE};
use aruna_core::operation::{HoldScope, Operation, TransactionMode};
use aruna_core::plugin_runs::{GapReason, WAITING_EXPIRY_MS};
use aruna_core::structs::identity::realm::RealmConfigDocument;

use super::decide::{GapCount, Judge, Verdict, WaitingRow, seen_row};
use super::rows::{Row, Scan, StoreError, decode, delete_call, done};
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
    pub fn new(
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

    fn record(&mut self, outcome: Outcome, mut rows: Vec<Row>) -> Result<Next<()>, StoreError> {
        rows.push(seen_row(&self.waiting.row, &outcome, self.now_ms)?);
        self.phase = SettlePhase::Writing;
        Ok(Next::Call(IoCall::Write(rows)))
    }
}

impl Operation for SettleWaiting {
    type Output = ();
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, StoreError> {
        Ok(Next::Call(delete_call(vec![
            (PLUGIN_WAITING_KEYSPACE, self.key.clone()),
            (PLUGIN_QUEUE_KEYSPACE, self.key.clone()),
        ])))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<()>, StoreError> {
        match std::mem::replace(&mut self.phase, SettlePhase::Writing) {
            SettlePhase::Freeing(verdict, gap) => {
                done(answer)?;
                match *verdict {
                    Some(Verdict::Run(run)) => {
                        let admission = Admission::new(*run);
                        let ask = admission.ask();
                        self.phase = SettlePhase::Admitting(admission);
                        Ok(Next::Call(ask))
                    }
                    Some(Verdict::Denied(reason)) => self.record(Outcome::Denied(reason), vec![]),
                    _ => {
                        let ask = gap.ask();
                        self.phase = SettlePhase::Counting(gap);
                        Ok(Next::Call(ask))
                    }
                }
            }
            SettlePhase::Admitting(mut admission) => match admission.take(answer) {
                Ok(None) => {
                    let ask = admission.ask();
                    self.phase = SettlePhase::Admitting(admission);
                    Ok(Next::Call(ask))
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
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RecheckPhase {
    Scanning(Scan),
    Judging(Vec<u8>, Box<WaitingRow>),
    Settling,
}

/// Decides each waiting event that is ready or expired; answers how many left the waiting set.
///
#[derive(Debug, PartialEq)]
pub struct RecheckWaiting {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    now_ms: u64,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    left: usize,
    phase: RecheckPhase,
}

impl RecheckWaiting {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId, now_ms: u64) -> Self {
        let scan = Scan::new(PLUGIN_WAITING_KEYSPACE, Vec::new());
        Self {
            config,
            local,
            now_ms,
            rows: VecDeque::new(),
            left: 0,
            phase: RecheckPhase::Scanning(scan),
        }
    }

    fn settle(&mut self, key: Vec<u8>, waiting: WaitingRow, verdict: Option<Verdict>) -> IoCall {
        self.phase = RecheckPhase::Settling;
        let place = (self.local, self.now_ms);
        IoCall::child(SettleWaiting::new(key, waiting, verdict, place))
    }

    fn advance(&mut self) -> Result<Next<usize>, StoreError> {
        let Some((key, value)) = self.rows.pop_front() else {
            return Ok(Next::Done(self.left));
        };
        let waiting: WaitingRow = decode(&value)?;
        let expires = waiting.received_ms.saturating_add(WAITING_EXPIRY_MS);
        if self.now_ms > expires {
            return Ok(Next::Call(self.settle(key, waiting, None)));
        }
        let (config, row) = (self.config.clone(), waiting.row.clone());
        let judge = Judge::new(config, self.local, row, self.now_ms);
        self.phase = RecheckPhase::Judging(key, Box::new(waiting));
        Ok(Next::Call(IoCall::child(judge)))
    }
}

impl Operation for RecheckWaiting {
    type Output = usize;
    type Error = StoreError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginAdmissions)
    }

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        match &self.phase {
            RecheckPhase::Scanning(scan) => Ok(Next::Call(scan.ask())),
            _ => Err(StoreError("the recheck started twice".into())),
        }
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, RecheckPhase::Settling) {
            RecheckPhase::Scanning(mut scan) => match scan.take(answer)? {
                Some(rows) => {
                    self.rows = rows.into();
                    self.advance()
                }
                None => {
                    let call = scan.ask();
                    self.phase = RecheckPhase::Scanning(scan);
                    Ok(Next::Call(call))
                }
            },
            RecheckPhase::Judging(key, waiting) => {
                match answer.child::<Verdict, StoreError>()?? {
                    Verdict::Unready | Verdict::Unavailable => self.advance(),
                    verdict => Ok(Next::Call(self.settle(key, *waiting, Some(verdict)))),
                }
            }
            RecheckPhase::Settling => {
                answer.child::<(), StoreError>()??;
                self.left += 1;
                self.advance()
            }
        }
    }
}

#[cfg(test)]
#[path = "waiting_tests.rs"]
mod tests;
