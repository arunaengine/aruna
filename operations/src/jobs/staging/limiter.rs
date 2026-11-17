//! Sends the source requests of table imports again after transient answers, with backoff. The
//! blob adapter paces every request it sends, these included, and honours `Retry-After`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::StagingJobSpec;
use rand::RngExt;

/// Attempts of one request while its answers stay transient.
const ATTEMPTS: u32 = 5;
/// The first backoff; each further attempt doubles it.
const BACKOFF: Duration = Duration::from_millis(500);
/// The longest backoff between two attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// The wait before attempt `attempt`, with up to half of it again as jitter from `seed`.
fn backoff(attempt: u32, seed: u64) -> Duration {
    let base = capped_backoff(BACKOFF, MAX_BACKOFF, attempt, 0);
    let half = u64::try_from(base.as_millis() / 2).unwrap_or(u64::MAX);
    let jitter = seed.rotate_left(attempt) % half.saturating_add(1);
    base + Duration::from_millis(jitter)
}

/// Runs one source request as a child and, for a table import, runs it again after a
/// transient error until it answers or the attempts run out.
#[derive(Debug)]
pub(super) struct Retried<I, O: Operation> {
    input: I,
    build: fn(I) -> O,
    transient: fn(&O::Error) -> bool,
    table: bool,
    attempt: u32,
    seed: u64,
    waiting: bool,
}

/// Two retries are equal when they stand at the same place; the functions are not compared.
impl<I: PartialEq, O: Operation> PartialEq for Retried<I, O> {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.table == other.table
            && self.attempt == other.attempt
            && self.seed == other.seed
            && self.waiting == other.waiting
    }
}

impl<I: Clone, O: Operation> Retried<I, O> {
    pub(super) fn new(
        spec: &StagingJobSpec,
        input: I,
        build: fn(I) -> O,
        transient: fn(&O::Error) -> bool,
    ) -> Self {
        Self {
            input,
            build,
            transient,
            table: spec.table.is_some(),
            attempt: 0,
            seed: rand::rng().random(),
            waiting: false,
        }
    }
}

impl<I, O> Operation for Retried<I, O>
where
    I: Clone + Debug + PartialEq + Send + 'static,
    O: Operation + 'static,
    O::Output: PartialEq + 'static,
    O::Error: 'static,
{
    type Output = O::Output;
    type Error = O::Error;

    fn first(&mut self) -> Result<Next<O::Output>, O::Error> {
        Ok(Next::Call(IoCall::child((self.build)(self.input.clone()))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<O::Output>, O::Error> {
        if self.waiting {
            let IoResult::Woke = result else {
                return Err(IoError::Unexpected {
                    expected: "IoResult::Woke",
                    got: result.label(),
                }
                .into());
            };
            self.waiting = false;
            return self.first();
        }
        match result.child::<O::Output, O::Error>()? {
            Ok(output) => Ok(Next::Done(output)),
            Err(error) if self.table && (self.transient)(&error) => {
                self.attempt += 1;
                if self.attempt >= ATTEMPTS {
                    return Err(error);
                }
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(backoff(self.attempt, self.seed))))
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::table::{TableImport, TableSource};

    /// A request that always answers its input.
    #[derive(Debug, PartialEq)]
    struct Echo(u8);

    impl Operation for Echo {
        type Output = u8;
        type Error = IoError;

        fn first(&mut self) -> Result<Next<u8>, IoError> {
            Ok(Next::Done(self.0))
        }

        fn next(&mut self, _result: IoResult) -> Result<Next<u8>, IoError> {
            Ok(Next::Done(self.0))
        }
    }

    fn spec(table: bool) -> StagingJobSpec {
        let mut spec = crate::jobs::staging::pure_tests::test_spec();
        let rows = serde_json::json!({ "kind": "file", "source": "{a}", "target": "{a}" });
        spec.table = table.then(|| TableImport {
            source: TableSource::Connector("list.csv".to_string()),
            format: Default::default(),
            rows: serde_json::from_value(rows).unwrap(),
            rate: Some(10),
        });
        spec
    }

    fn answer(result: Result<u8, IoError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn retried(table: bool) -> Retried<u8, Echo> {
        Retried::new(&spec(table), 1, Echo, |error| *error == IoError::Unfinished)
    }

    // A transient answer is sent again after a backoff; a lasting one returns at once.
    #[test]
    fn transient_answers_retried() {
        let mut operation = retried(true);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Call(IoCall::Delay(wait))) = operation.next(answer(Err(IoError::Unfinished)))
        else {
            panic!("a transient answer waits");
        };
        assert!(wait >= BACKOFF * 2);
        assert!(matches!(
            operation.next(IoResult::Woke),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.next(answer(Ok(1))), Ok(Next::Done(1)));

        let mut lasting = retried(true);
        lasting.first().expect("request call");
        assert_eq!(
            lasting.next(answer(Err(IoError::Aborted))),
            Err(IoError::Aborted)
        );
    }

    // Only table imports send again, and only until the attempts run out.
    #[test]
    fn retries_are_bounded() {
        let mut plain = retried(false);
        plain.first().expect("request call");
        assert_eq!(
            plain.next(answer(Err(IoError::Unfinished))),
            Err(IoError::Unfinished)
        );

        let mut operation = retried(true);
        operation.first().expect("request call");
        for _ in 1..ATTEMPTS {
            let waited = operation.next(answer(Err(IoError::Unfinished)));
            assert!(matches!(waited, Ok(Next::Call(IoCall::Delay(_)))));
            operation.next(IoResult::Woke).expect("request call");
        }
        assert_eq!(
            operation.next(answer(Err(IoError::Unfinished))),
            Err(IoError::Unfinished)
        );
    }
}
