//! The call forms of the job store: a store change that retries lost conflicts, and plain row
//! reads and writes. Each is an operation, so callers drive it or run it as a typed child.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::{ByCalls, Operation};
use aruna_core::rows::{done, read_row, value};
use aruna_core::types::{Key, KeySpace, Value};

use super::JobMutationError;
use crate::jobs::MUTATE_MAX_ATTEMPTS;

type StartCall = Box<dyn FnMut() -> IoCall + Send>;
type FinishCall<T> =
    Box<dyn FnMut(IoResult) -> Result<Result<T, JobMutationError>, IoError> + Send>;

/// Runs one store change as a child and runs it again, after a short growing pause, while it
/// loses a transaction conflict. The last lost attempt ends with the `exhausted` error.
pub struct StoreCall<T> {
    start: StartCall,
    finish: FinishCall<T>,
    refused: Option<JobMutationError>,
    exhausted: &'static str,
    attempt: u32,
    waiting: bool,
}

impl<T> std::fmt::Debug for StoreCall<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreCall")
            .field("exhausted", &self.exhausted)
            .field("attempt", &self.attempt)
            .field("waiting", &self.waiting)
            .finish_non_exhaustive()
    }
}

/// Two calls are equal when they stand at the same place; the closures are not compared.
impl<T> PartialEq for StoreCall<T> {
    fn eq(&self, other: &Self) -> bool {
        self.exhausted == other.exhausted
            && self.attempt == other.attempt
            && self.waiting == other.waiting
            && self.refused == other.refused
    }
}

impl<T: 'static> StoreCall<T> {
    /// Each attempt runs a fresh `build()`, and `map` turns its output into the answer.
    pub(crate) fn of<O, B, M>(mut build: B, mut map: M, exhausted: &'static str) -> Self
    where
        O: Operation<Error = JobMutationError> + 'static,
        O::Output: PartialEq + 'static,
        B: FnMut() -> O + Send + 'static,
        M: FnMut(O::Output) -> Result<T, JobMutationError> + Send + 'static,
    {
        Self {
            start: Box::new(move || IoCall::child::<O, ByCalls>(build())),
            finish: Box::new(move |result| {
                let output = result.child::<O::Output, JobMutationError>()?;
                Ok(output.and_then(&mut map))
            }),
            refused: None,
            exhausted,
            attempt: 0,
            waiting: false,
        }
    }

    /// A call that was refused before it could start.
    pub(crate) fn refused(error: JobMutationError) -> Self {
        Self {
            start: Box::new(|| IoCall::Read(Vec::new())),
            finish: Box::new(|_| Err(IoError::Unfinished)),
            refused: Some(error),
            exhausted: "",
            attempt: 0,
            waiting: false,
        }
    }
}

impl<T: Send + std::fmt::Debug> Operation for StoreCall<T> {
    type Output = T;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<T>, JobMutationError> {
        if let Some(error) = self.refused.take() {
            return Err(error);
        }
        Ok(Next::Call((self.start)()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, JobMutationError> {
        if self.waiting {
            self.waiting = false;
            return Ok(Next::Call((self.start)()));
        }
        match (self.finish)(result)? {
            Err(JobMutationError::Conflict) if self.attempt + 1 < MUTATE_MAX_ATTEMPTS => {
                let pause = Duration::from_millis(1 << self.attempt.min(6));
                self.attempt += 1;
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(pause)))
            }
            Err(JobMutationError::Conflict) => {
                Err(JobMutationError::Storage(self.exhausted.to_string()))
            }
            outcome => outcome.map(Next::Done),
        }
    }
}

type Decode<T> = Box<dyn FnMut(Option<Value>) -> Result<T, JobMutationError> + Send>;

/// Reads one row and decodes it. Without a row to read it decodes nothing at all.
pub struct RowRead<T> {
    row: Option<(KeySpace, Key)>,
    decode: Decode<T>,
}

impl<T> std::fmt::Debug for RowRead<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowRead")
            .field("row", &self.row)
            .finish_non_exhaustive()
    }
}

impl<T> PartialEq for RowRead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl<T> RowRead<T> {
    pub(crate) fn new<D>(key_space: &str, key: Key, decode: D) -> Self
    where
        D: FnMut(Option<Value>) -> Result<T, JobMutationError> + Send + 'static,
    {
        Self {
            row: Some((key_space.to_string(), key)),
            decode: Box::new(decode),
        }
    }

    /// The answer `decode` gives for an absent row, without reading anything.
    pub(crate) fn absent<D>(decode: D) -> Self
    where
        D: FnMut(Option<Value>) -> Result<T, JobMutationError> + Send + 'static,
    {
        Self {
            row: None,
            decode: Box::new(decode),
        }
    }
}

impl<T: Send + std::fmt::Debug> Operation for RowRead<T> {
    type Output = T;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<T>, JobMutationError> {
        match self.row.take() {
            Some((space, key)) => Ok(Next::Call(read_row(&space, key))),
            None => (self.decode)(None).map(Next::Done),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, JobMutationError> {
        (self.decode)(value(result)?).map(Next::Done)
    }
}

/// Writes rows, or deletes them, in one batch; nothing to do ends at once.
#[derive(Debug, PartialEq)]
pub struct RowBatch {
    call: Result<Option<IoCall>, JobMutationError>,
}

impl RowBatch {
    pub(crate) fn write(rows: Result<Vec<(KeySpace, Key, Value)>, JobMutationError>) -> Self {
        let call = rows.map(|rows| (!rows.is_empty()).then_some(IoCall::Write(rows)));
        Self { call }
    }

    pub(crate) fn delete(rows: Vec<(KeySpace, Key)>) -> Self {
        let call = Ok((!rows.is_empty()).then_some(IoCall::Delete(rows)));
        Self { call }
    }
}

impl Operation for RowBatch {
    type Output = ();
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        let call = std::mem::replace(&mut self.call, Ok(None))?;
        Ok(call.map_or(Next::Done(()), Next::Call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    #[derive(Debug, PartialEq)]
    struct Step;

    impl Operation for Step {
        type Output = u8;
        type Error = JobMutationError;

        fn first(&mut self) -> Result<Next<u8>, JobMutationError> {
            Ok(Next::Done(1))
        }

        fn next(&mut self, _: IoResult) -> Result<Next<u8>, JobMutationError> {
            Ok(Next::Done(1))
        }
    }

    fn call() -> StoreCall<u16> {
        StoreCall::of(|| Step, |output| Ok(u16::from(output) + 1), "gave up")
    }

    fn answer(result: Result<u8, JobMutationError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // The output of the child is mapped into the answer.
    #[test]
    fn maps_child_output() {
        let mut store = call();
        assert!(matches!(store.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(store.next(answer(Ok(1))), Ok(Next::Done(2)));
    }

    // A lost conflict pauses, runs a fresh child, and the pause grows.
    #[test]
    fn conflict_pauses_then_retries() {
        let mut store = call();
        store.first().expect("the first attempt starts");
        for pause in [1, 2, 4] {
            assert_eq!(
                store.next(answer(Err(JobMutationError::Conflict))),
                Ok(Next::Call(IoCall::Delay(Duration::from_millis(pause))))
            );
            assert!(matches!(
                store.next(IoResult::Woke),
                Ok(Next::Call(IoCall::Child(_)))
            ));
        }
    }

    // The last lost attempt ends with the named error instead of another run.
    #[test]
    fn conflicts_end_exhausted() {
        let mut store = call();
        store.first().expect("the first attempt starts");
        for _ in 1..MUTATE_MAX_ATTEMPTS {
            store
                .next(answer(Err(JobMutationError::Conflict)))
                .expect("pause");
            store.next(IoResult::Woke).expect("retry");
        }
        assert_eq!(
            store.next(answer(Err(JobMutationError::Conflict))),
            Err(JobMutationError::Storage("gave up".to_string()))
        );
    }

    // Any other failure ends the call as it is.
    #[test]
    fn other_errors_end() {
        let mut store = call();
        store.first().expect("the first attempt starts");
        let failed = store.next(answer(Err(JobMutationError::NotFound)));
        assert_eq!(failed, Err(JobMutationError::NotFound));
    }

    // A refused call never starts a child.
    #[test]
    fn refused_never_starts() {
        let mut store = StoreCall::<u16>::refused(JobMutationError::NotFound);
        assert_eq!(store.first(), Err(JobMutationError::NotFound));
    }

    // A read decodes its row, and without a row it asks for nothing.
    #[test]
    fn row_read_decodes() {
        let decode = |row: Option<Value>| Ok(row.is_some());
        let mut read = RowRead::new("space", Key::from(vec![1]), decode);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Read(_)))));
        let row = IoResult::Values(vec![Some(Value::from(vec![2]))]);
        assert_eq!(read.next(row), Ok(Next::Done(true)));
        let mut none = RowRead::absent(decode);
        assert_eq!(none.first(), Ok(Next::Done(false)));
    }

    // An empty batch ends at once; a failed encoding never writes.
    #[test]
    fn empty_batch_ends() {
        assert_eq!(RowBatch::write(Ok(Vec::new())).first(), Ok(Next::Done(())));
        assert_eq!(RowBatch::delete(Vec::new()).first(), Ok(Next::Done(())));
        let failed = RowBatch::write(Err(JobMutationError::NotFound)).first();
        assert_eq!(failed, Err(JobMutationError::NotFound));
    }
}
