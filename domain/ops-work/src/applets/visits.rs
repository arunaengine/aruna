//! Counts what visitors and guests do with an applet and refuses what goes past their caps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::usage::{Caps, Window, usage_count, usage_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::APPLETS_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::AppletError;

/// Counts one action of `visitor` (a pseudonym) in one write transaction, so two edges never
/// both take the last allowed action.
#[derive(Debug, PartialEq)]
pub struct CountVisitOperation {
    applet_id: Ulid,
    visitor: String,
    caps: Caps,
    now_ms: u64,
    rows: Vec<Key>,
}

impl CountVisitOperation {
    pub fn new(applet_id: Ulid, visitor: &str, caps: Caps) -> Self {
        Self {
            applet_id,
            visitor: visitor.to_string(),
            caps,
            now_ms: 0,
            rows: Vec::new(),
        }
    }
}

impl Operation for CountVisitOperation {
    type Output = ();
    type Error = AppletError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, AppletError> {
        let (id, now) = (self.applet_id, self.now_ms);
        let hour = (Window::Hour, Window::Hour.start(now));
        let day = (Window::Day, Window::Day.start(now));
        self.rows = vec![
            usage_key(id, hour, Some(&self.visitor)),
            usage_key(id, day, Some(&self.visitor)),
            usage_key(id, day, None),
        ];
        let reads = (self.rows.iter())
            .map(|row| (APPLETS_KEYSPACE.to_string(), row.clone()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AppletError> {
        if self.rows.is_empty() {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let counts: Vec<u32> = (result.values()?.iter())
            .map(|value| usage_count(value.as_deref()))
            .collect();
        let caps = [
            (self.caps.hourly, "this hour"),
            (self.caps.daily, "today"),
            (self.caps.applet_daily, "today for the whole applet"),
        ];
        for ((cap, span), count) in caps.into_iter().zip(&counts) {
            if cap.is_some_and(|cap| *count >= cap) {
                return Err(AppletError::Limited(format!(
                    "the limit for {span} is used up"
                )));
            }
        }
        let writes = (std::mem::take(&mut self.rows).into_iter())
            .zip(counts)
            .map(|(row, count)| {
                let value = Value::from((count + 1).to_be_bytes().to_vec());
                (APPLETS_KEYSPACE.to_string(), row, value)
            })
            .collect();
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AppletError) -> bool {
        matches!(error, AppletError::Limited(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(caps: Caps, stored: [u32; 3]) -> Result<Next<()>, AppletError> {
        let mut count = CountVisitOperation::new(Ulid::from_bytes([1; 16]), "ab", caps);
        count.stamp(5);
        count.first()?;
        let values = (stored.iter())
            .map(|count| Some(Value::from(count.to_be_bytes().to_vec())))
            .collect();
        count.next(IoResult::Values(values))
    }

    // Each cap refuses once its window is used up; no cap counts without limit.
    #[test]
    fn caps_refuse_last() {
        let caps = Caps {
            hourly: Some(2),
            daily: Some(3),
            applet_daily: Some(10),
        };
        assert!(matches!(
            turn(caps, [1, 2, 9]),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        for stored in [[2, 2, 0], [1, 3, 0], [0, 0, 10]] {
            assert!(matches!(turn(caps, stored), Err(AppletError::Limited(_))));
        }
        let open = Caps::default();
        assert!(matches!(
            turn(open, [99, 99, 99]),
            Ok(Next::Call(IoCall::Write(_)))
        ));
    }
}
