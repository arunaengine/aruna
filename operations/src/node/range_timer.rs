//! Runs the range maintenance timer: one bounded pass over the range accounts, then the handed
//! imports that pass hands over, then one page of unverified data links, with every cursor in
//! the timer's memory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::NodeRealm;
use tracing::warn;

use crate::distribution::imports::KeptStart;
use crate::jobs::handed_imports::{StartKeptError, StartKeptOperation};
use crate::metadata::data_links::{DataLinkError, RetryUnverifiedOperation};
use crate::transfer::sharding::{MaintainPass, maintain_ranges, maintain_retry};
use crate::triggers::{TimerInput, TimerMemory, TimerPlan};

#[derive(Clone, Copy, Debug, PartialEq)]
enum RangeStage {
    Maintain,
    Start,
    Retry,
}

/// One range maintenance run. A pass the runner could not finish keeps its cursors.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeTimerOperation {
    ids: NodeRealm,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
    capacity: Option<u64>,
    now_ms: u64,
    memory: TimerMemory,
    after: Duration,
    starts: VecDeque<KeptStart>,
    stage: RangeStage,
}

type RangeTurn = Result<Next<TimerPlan>, IoError>;

impl RangeTimerOperation {
    pub fn new(input: &TimerInput) -> Self {
        Self {
            ids: input.facts.serving,
            fresh: input.heartbeats.clone(),
            capacity: input.routing.catalog.capacity(),
            now_ms: input.now_ms,
            memory: input.memory.clone(),
            after: maintain_retry(),
            starts: VecDeque::new(),
            stage: RangeStage::Maintain,
        }
    }

    /// Starts the next handed import, then retries one page of unverified data links.
    fn next_start(&mut self) -> RangeTurn {
        if let Some(start) = self.starts.pop_front() {
            self.stage = RangeStage::Start;
            let started = StartKeptOperation::new(start, self.ids, self.now_ms);
            return Ok(Next::Call(IoCall::child(started)));
        }
        self.stage = RangeStage::Retry;
        let retry = RetryUnverifiedOperation::new(self.ids, self.memory.cursor.clone());
        Ok(Next::Call(IoCall::child(retry)))
    }
}

impl Operation for RangeTimerOperation {
    type Output = TimerPlan;
    type Error = IoError;

    fn first(&mut self) -> RangeTurn {
        let cursors = self.memory.ranges.clone();
        let pass = maintain_ranges(self.ids, &self.fresh, self.capacity, cursors);
        Ok(Next::Call(IoCall::child(pass)))
    }

    fn next(&mut self, result: IoResult) -> RangeTurn {
        match self.stage {
            RangeStage::Maintain => {
                match result.child::<MaintainPass, IoError>()? {
                    Ok(pass) => {
                        let (after, imports, cursors) = pass.finish();
                        self.after = after;
                        self.memory.ranges = cursors;
                        self.starts = imports.into();
                    }
                    Err(error) => warn!(%error, "Range maintenance could not finish a pass"),
                }
                self.next_start()
            }
            RangeStage::Start => {
                if let Err(error) = result.child::<(), StartKeptError>()? {
                    warn!(%error, "Handed over imports could not start");
                }
                self.next_start()
            }
            // Version links metadata could not verify in a set are retried a page per pass.
            RangeStage::Retry => {
                match result.child::<Option<Vec<u8>>, DataLinkError>()? {
                    Ok(next) => {
                        if next.is_some() {
                            self.after = Duration::ZERO;
                        }
                        self.memory.cursor = next;
                    }
                    Err(error) => warn!(%error, "Unverified data links could not be retried"),
                }
                let memory = self.memory.clone();
                Ok(Next::Done(TimerPlan {
                    after: Some(self.after),
                    memory,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    // A pass that broke off still retries the unverified links; a further page of them runs the
    // timer again at once and keeps where it goes on.
    #[test]
    fn unverified_page_continues() {
        let mut input = TimerInput::new(1_000, None);
        input.memory.cursor = Some(b"kept".to_vec());
        let mut run = RangeTimerOperation::new(&input);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let broken = child(Err::<MaintainPass, _>(IoError::Unfinished));
        assert!(matches!(run.next(broken), Ok(Next::Call(IoCall::Child(_)))));
        let page = child(Ok::<_, DataLinkError>(Some(b"next".to_vec())));
        let Ok(Next::Done(plan)) = run.next(page) else {
            panic!("expected the next maintenance plan");
        };
        assert_eq!(plan.after, Some(Duration::ZERO));
        assert_eq!(plan.memory.cursor.as_deref(), Some(&b"next"[..]));
    }
}
