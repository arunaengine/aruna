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

use super::AutoError;

/// Atomically checks and increments one or more usage counter rows in `AUTOMATIONS_KEYSPACE`.
#[derive(Clone, Debug, PartialEq)]
pub struct CountUsageOperation {
    id: Ulid,
    checks: Vec<(CounterKind, Window, u64, u64)>,
    now_ms: u64,
    rows: Vec<(Key, u64, u64)>,
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
        self.rows = self
            .checks
            .iter()
            .map(|&(kind, window, add, cap)| {
                let key = counter_key(self.id, kind, (window, window.start(self.now_ms)));
                (key, add, cap)
            })
            .collect();
        let reads = self
            .rows
            .iter()
            .map(|(key, _, _)| (AUTOMATIONS_KEYSPACE.to_string(), key.clone()))
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
        for ((key, add, cap), stored) in std::mem::take(&mut self.rows).into_iter().zip(values) {
            let next = count_u64(stored.as_deref()).saturating_add(add);
            if next > cap {
                return Err(AutoError::Limited(
                    "automation rate or byte cap reached".into(),
                ));
            }
            writes.push((
                AUTOMATIONS_KEYSPACE.to_string(),
                key,
                Value::from(next.to_be_bytes().to_vec()),
            ));
        }
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::Limited(_))
    }
}
