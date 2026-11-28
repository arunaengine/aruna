//! Transactional hourly/daily firing counters and daily copy byte limits for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::scope::Limits;
use aruna_core::automation::usage::{CounterKind, Window, count_u64, counter_key};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::AUTOMATIONS_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::{AutoError, pinned_retry_after};

/// Atomically checks and increments one or more usage counter rows in `AUTOMATIONS_KEYSPACE`.
#[derive(Clone, Debug, PartialEq)]
pub struct CountUsageOperation {
    id: Ulid,
    checks: Vec<(CounterKind, Window, u64, u64)>,
    now_ms: u64,
    window_ms: u64,
    rows: Vec<(Key, Window, u64, u64)>,
}

impl CountUsageOperation {
    /// Checks hourly and daily firing rate limits and increments both by 1.
    pub fn firing(id: Ulid, limits: &Limits) -> Self {
        Self {
            id,
            checks: vec![
                (
                    CounterKind::Firings,
                    Window::Hour,
                    1,
                    u64::from(limits.hour_limit()),
                ),
                (
                    CounterKind::Firings,
                    Window::Day,
                    1,
                    u64::from(limits.day_limit()),
                ),
            ],
            now_ms: 0,
            window_ms: 0,
            rows: Vec::new(),
        }
    }

    /// Checks the daily copied bytes cap and adds `bytes`.
    pub fn copy_bytes(id: Ulid, bytes: u64, limits: &Limits) -> Self {
        Self {
            id,
            checks: vec![(
                CounterKind::CopyBytes,
                Window::Day,
                bytes,
                limits.copy_bytes_day(),
            )],
            now_ms: 0,
            window_ms: 0,
            rows: Vec::new(),
        }
    }
}

impl Operation for CountUsageOperation {
    type Output = ();
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, AutoError> {
        self.window_ms = self.now_ms;
        self.rows = self
            .checks
            .iter()
            .map(|&(kind, window, add, cap)| {
                let key = counter_key(self.id, kind, (window, window.start(self.window_ms)));
                (key, window, add, cap)
            })
            .collect();
        let reads = self
            .rows
            .iter()
            .map(|(key, _, _, _)| (AUTOMATIONS_KEYSPACE.to_string(), key.clone()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AutoError> {
        if self.rows.is_empty() {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let values = result.values()?;
        let mut writes = Vec::with_capacity(self.rows.len());
        let mut retry_after: Option<u64> = None;
        for ((key, window, add, cap), stored) in
            std::mem::take(&mut self.rows).into_iter().zip(values)
        {
            let next = count_u64(stored.as_deref()).saturating_add(add);
            if next > cap {
                let wait = pinned_retry_after(window, self.window_ms, self.now_ms);
                retry_after = Some(retry_after.map_or(wait, |prev| prev.max(wait)));
            }
            writes.push((
                AUTOMATIONS_KEYSPACE.to_string(),
                key,
                Value::from(next.to_be_bytes().to_vec()),
            ));
        }
        if let Some(wait) = retry_after {
            return Err(AutoError::limited(
                "automation rate or byte cap reached",
                wait,
            ));
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::Limited { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_retry_window() {
        let auto_id = Ulid::from_bytes([17; 16]);
        let utc_midnight = 1_764_374_400_000u64;
        let limits = Limits {
            per_hour: Some(5),
            per_day: Some(10),
            bytes_per_day: Some(100),
            ..Limits::default()
        };

        // Crossing midnight UTC between first() and next() floors the expired window at 1s.
        let mut bytes_op = CountUsageOperation::copy_bytes(auto_id, 20, &limits);
        bytes_op.stamp(utc_midnight - 1_000);
        bytes_op.first().unwrap();
        bytes_op.stamp(utc_midnight + 1_000);
        let bytes_err = bytes_op.next(IoResult::Values(vec![Some(
            90u64.to_be_bytes().to_vec().into(),
        )]));
        assert!(matches!(
            bytes_err,
            Err(AutoError::Limited { retry_after: 1, .. })
        ));

        let mut firing_op = CountUsageOperation::firing(auto_id, &limits);
        firing_op.stamp(utc_midnight - 1_000);
        firing_op.first().unwrap();
        firing_op.stamp(utc_midnight + 1_000);
        let firing_err = firing_op.next(IoResult::Values(vec![
            None,
            Some(10u64.to_be_bytes().to_vec().into()),
        ]));
        assert!(matches!(
            firing_err,
            Err(AutoError::Limited { retry_after: 1, .. })
        ));

        // Exceeding both caps reports the longer daily expiry; hourly-only reports 1,800s.
        let mid_morning = utc_midnight + 10 * 3_600_000 + 30 * 60_000;
        let mut both_op = CountUsageOperation::firing(auto_id, &limits);
        both_op.stamp(mid_morning);
        both_op.first().unwrap();
        let both_err = both_op.next(IoResult::Values(vec![
            Some(5u64.to_be_bytes().to_vec().into()),
            Some(10u64.to_be_bytes().to_vec().into()),
        ]));
        assert!(matches!(
            both_err,
            Err(AutoError::Limited {
                retry_after: 48_600,
                ..
            })
        ));

        let mut hour_op = CountUsageOperation::firing(auto_id, &limits);
        hour_op.stamp(mid_morning);
        hour_op.first().unwrap();
        let hour_err = hour_op.next(IoResult::Values(vec![
            Some(5u64.to_be_bytes().to_vec().into()),
            Some(5u64.to_be_bytes().to_vec().into()),
        ]));
        assert!(matches!(
            hour_err,
            Err(AutoError::Limited {
                retry_after: 1_800,
                ..
            })
        ));
    }
}
