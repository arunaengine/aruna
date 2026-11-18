//! Runs a pure plan over stored rows: the plan names the rows it needs, is replayed once they are
//! known, and ends with the deletes and writes it decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::effects::{Effect, IterStart, StorageEffect};
use crate::events::{Event, StorageEvent};
use crate::operation::{Operation, TransactionMode};
use crate::rows::{all_values, done, event};
use crate::types::{Key, KeySpace, Value};

/// A plan asked for rows that are not known yet.
#[derive(Debug, PartialEq)]
pub struct NeedsRows;

/// Why a plan stopped: it needs rows that are not known yet, or it failed.
#[derive(Debug, PartialEq)]
pub enum PlanStop<E> {
    NeedsRows,
    Failed(E),
}

impl<E> From<NeedsRows> for PlanStop<E> {
    fn from(_: NeedsRows) -> Self {
        Self::NeedsRows
    }
}

impl From<IoError> for PlanStop<IoError> {
    fn from(error: IoError) -> Self {
        Self::Failed(error)
    }
}

/// One range read of a plan: every row below a prefix, up to a limit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ScanRequest {
    space: KeySpace,
    prefix: Option<Key>,
    after: Option<Key>,
    limit: usize,
}

/// The rows a plan has read so far and the time of its run.
#[derive(Debug, Default, PartialEq)]
pub struct PlanRows {
    known: BTreeMap<(KeySpace, Key), Option<Value>>,
    wanted: BTreeSet<(KeySpace, Key)>,
    scans: BTreeMap<ScanRequest, Vec<(Key, Value)>>,
    wanted_scan: Option<ScanRequest>,
    now_ms: u64,
}

impl PlanRows {
    /// Known rows and a time, for a plan run without storage.
    pub fn with_rows(
        rows: impl IntoIterator<Item = ((KeySpace, Key), Option<Value>)>,
        now_ms: u64,
    ) -> Self {
        Self {
            known: rows.into_iter().collect(),
            now_ms,
            ..Self::default()
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// The row, or a stop that asks for it and for every row wanted before.
    pub fn read(&mut self, space: &str, key: impl Into<Key>) -> Result<Option<Value>, NeedsRows> {
        let row = (space.to_string(), key.into());
        match self.known.get(&row) {
            Some(value) => Ok(value.clone()),
            None => {
                self.wanted.insert(row);
                Err(NeedsRows)
            }
        }
    }

    /// Asks for a row in the same read as the next stop, so a loop does not read one by one.
    pub fn want(&mut self, space: &str, key: impl Into<Key>) {
        let row = (space.to_string(), key.into());
        if !self.known.contains_key(&row) {
            self.wanted.insert(row);
        }
    }

    /// The rows below `prefix`, or the whole key space without one.
    pub fn scan(
        &mut self,
        space: &str,
        prefix: Option<Key>,
    ) -> Result<Vec<(Key, Value)>, NeedsRows> {
        self.scan_limited(space, prefix, usize::MAX)
    }

    /// At most `limit` rows below `prefix`.
    pub fn scan_limited(
        &mut self,
        space: &str,
        prefix: Option<Key>,
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, NeedsRows> {
        self.scan_after(space, prefix, None, limit)
    }

    /// At most `limit` rows below `prefix` that sort after `after`.
    pub fn scan_after(
        &mut self,
        space: &str,
        prefix: Option<Key>,
        after: Option<Key>,
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, NeedsRows> {
        let request = ScanRequest {
            space: space.to_string(),
            prefix,
            after,
            limit,
        };
        match self.scans.get(&request) {
            Some(rows) => Ok(rows.clone()),
            None => {
                self.wanted_scan = Some(request);
                Err(NeedsRows)
            }
        }
    }

    fn take_wanted(&mut self) -> Vec<(KeySpace, Key)> {
        std::mem::take(&mut self.wanted).into_iter().collect()
    }

    fn learn(&mut self, rows: Vec<(KeySpace, Key)>, values: Vec<Option<Value>>) {
        self.known.extend(rows.into_iter().zip(values));
    }
}

/// What a finished plan decided: its output and the rows to change.
#[derive(Debug, PartialEq)]
pub struct Planned<T> {
    pub output: T,
    pub deletes: Vec<(KeySpace, Key)>,
    pub writes: Vec<(KeySpace, Key, Value)>,
}

impl<T> Planned<T> {
    /// An output that changes no row.
    pub fn output(output: T) -> Self {
        Self::changes(output, Vec::new(), Vec::new())
    }

    pub fn changes(
        output: T,
        deletes: Vec<(KeySpace, Key)>,
        writes: Vec<(KeySpace, Key, Value)>,
    ) -> Self {
        Self {
            output,
            deletes,
            writes,
        }
    }
}

/// A decision over stored rows that may run again from the start, so it holds no state.
pub trait RowPlan: Send + std::fmt::Debug + PartialEq {
    type Output: Send + std::fmt::Debug;
    type Error: Send + std::fmt::Debug + PartialEq + From<IoError>;

    fn plan(&self, rows: &mut PlanRows) -> Result<Planned<Self::Output>, PlanStop<Self::Error>>;

    /// How the reads and changes run; by default in one write transaction.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// How often a lost transaction conflict reruns the plan.
    fn conflict_retries() -> u32
    where
        Self: Sized,
    {
        1
    }
}

#[derive(Debug, PartialEq)]
enum Phase<T> {
    Planning,
    Reading(Vec<(KeySpace, Key)>),
    Scanning(ScanRequest),
    /// The deletes ran; the writes come next.
    Deleting(Vec<(KeySpace, Key, Value)>, Option<T>),
    Writing(Option<T>),
}

/// Runs a [`RowPlan`]: reads what it names, replays it, then makes its changes.
#[derive(Debug, PartialEq)]
pub struct PlanOperation<P: RowPlan> {
    plan: P,
    rows: PlanRows,
    phase: Phase<P::Output>,
}

impl<P: RowPlan> PlanOperation<P>
where
    P::Output: PartialEq,
{
    pub fn new(plan: P) -> Self {
        Self {
            plan,
            rows: PlanRows::default(),
            phase: Phase::Planning,
        }
    }

    fn replay(&mut self) -> Result<Next<P::Output>, P::Error> {
        match self.plan.plan(&mut self.rows) {
            Ok(planned) => Ok(self.change(planned)),
            Err(PlanStop::Failed(error)) => Err(error),
            Err(PlanStop::NeedsRows) => Ok(self.ask()),
        }
    }

    fn ask(&mut self) -> Next<P::Output> {
        let rows = self.rows.take_wanted();
        if !rows.is_empty() {
            self.phase = Phase::Reading(rows.clone());
            return Next::Call(IoCall::Read(rows));
        }
        let Some(request) = self.rows.wanted_scan.take() else {
            // A plan that stops names what it needs; asking for nothing would never end.
            self.phase = Phase::Reading(Vec::new());
            return Next::Call(IoCall::Read(Vec::new()));
        };
        self.phase = Phase::Scanning(request.clone());
        let scan = StorageEffect::Iter {
            key_space: request.space,
            prefix: request.prefix,
            start: request.after.map(IterStart::After),
            limit: request.limit,
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(scan))))
    }

    fn change(&mut self, planned: Planned<P::Output>) -> Next<P::Output> {
        let Planned {
            output,
            deletes,
            writes,
        } = planned;
        if !deletes.is_empty() {
            self.phase = Phase::Deleting(writes, Some(output));
            return Next::Call(IoCall::Delete(deletes));
        }
        self.write(writes, output)
    }

    fn write(&mut self, writes: Vec<(KeySpace, Key, Value)>, output: P::Output) -> Next<P::Output> {
        if writes.is_empty() {
            return Next::Done(output);
        }
        self.phase = Phase::Writing(Some(output));
        Next::Call(IoCall::Write(writes))
    }
}

impl<P: RowPlan> Operation for PlanOperation<P>
where
    P::Output: PartialEq,
{
    type Output = P::Output;
    type Error = P::Error;

    fn first(&mut self) -> Result<Next<P::Output>, P::Error> {
        self.replay()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<P::Output>, P::Error> {
        let unfinished = || P::Error::from(IoError::Unfinished);
        match std::mem::replace(&mut self.phase, Phase::Planning) {
            Phase::Planning => Err(unfinished()),
            Phase::Reading(rows) => {
                let values = all_values(result)?;
                self.rows.learn(rows, values);
                self.replay()
            }
            Phase::Scanning(request) => {
                let values = match event(result)? {
                    Event::Storage(StorageEvent::IterResult { values, .. }) => values,
                    Event::Storage(StorageEvent::Error { error }) => {
                        return Err(IoError::Storage(error).into());
                    }
                    other => {
                        return Err(IoError::Unexpected {
                            expected: "StorageEvent::IterResult",
                            got: other.label(),
                        }
                        .into());
                    }
                };
                self.rows.scans.insert(request, values);
                self.replay()
            }
            Phase::Deleting(writes, output) => {
                done(result)?;
                Ok(self.write(writes, output.ok_or_else(unfinished)?))
            }
            Phase::Writing(output) => {
                done(result)?;
                Ok(Next::Done(output.ok_or_else(unfinished)?))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        self.plan.transaction()
    }

    fn conflict_retries() -> u32 {
        P::conflict_retries()
    }

    fn stamp(&mut self, now_ms: u64) {
        if self.rows.now_ms == 0 {
            self.rows.now_ms = now_ms;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct CopyPlan;

    impl RowPlan for CopyPlan {
        type Output = bool;
        type Error = IoError;

        fn plan(&self, rows: &mut PlanRows) -> Result<Planned<bool>, PlanStop<IoError>> {
            rows.want("rows", key("b"));
            let Some(first) = rows.read("rows", key("a"))? else {
                return Ok(Planned::output(false));
            };
            let second = rows.read("rows", key("b"))?;
            let deletes = vec![("rows".to_string(), key("a"))];
            let value = second.unwrap_or(first);
            let writes = vec![("rows".to_string(), key("c"), value)];
            Ok(Planned::changes(true, deletes, writes))
        }
    }

    fn key(text: &str) -> Key {
        Key::from(text.as_bytes())
    }

    fn reads() -> Vec<(KeySpace, Key)> {
        vec![
            ("rows".to_string(), key("a")),
            ("rows".to_string(), key("b")),
        ]
    }

    #[test]
    fn replays_after_reads() {
        let mut operation = PlanOperation::new(CopyPlan);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads()))));
        let values = IoResult::Values(vec![Some(key("1")), None]);
        assert_eq!(
            operation.next(values),
            Ok(Next::Call(IoCall::Delete(vec![(
                "rows".to_string(),
                key("a")
            )])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(vec![(
                "rows".to_string(),
                key("c"),
                key("1")
            )])))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn missing_row_stops() {
        let mut operation = PlanOperation::new(CopyPlan);
        operation.first().unwrap();
        let values = IoResult::Values(vec![None, None]);
        assert_eq!(operation.next(values), Ok(Next::Done(false)));
    }

    #[test]
    fn refuses_other_results() {
        let mut operation = PlanOperation::new(CopyPlan);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(IoError::Unexpected { .. })
        ));
    }

    #[test]
    fn keeps_first_stamp() {
        let mut operation = PlanOperation::new(CopyPlan);
        operation.stamp(5);
        operation.stamp(9);
        assert_eq!(operation.rows.now_ms(), 5);
    }
}
