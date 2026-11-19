//! Computes retry delays for queued task work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::backoff::capped_backoff;

pub const RETRY_BASE_MS: u64 = 250;
pub const RETRY_MAX_MS: u64 = 30_000;

pub fn retry_delay_ms(attempts: u32) -> u64 {
    let base = Duration::from_millis(RETRY_BASE_MS);
    let delay = capped_backoff(base, Duration::from_millis(RETRY_MAX_MS), attempts, 0);
    u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
}

pub fn min_due_at(current: Option<u64>, due_at_ms: u64) -> Option<u64> {
    Some(current.map_or(due_at_ms, |current| current.min(due_at_ms)))
}

pub fn due_after(now_ms: u64, due_at_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn retry_backoff_values() {
        let expected = [
            (0, 250),
            (1, 500),
            (2, 1_000),
            (3, 2_000),
            (4, 4_000),
            (5, 8_000),
            (6, 16_000),
            (7, 30_000),
            (8, 30_000),
            (u32::MAX, 30_000),
        ];

        for (attempts, expected_ms) in expected {
            assert_eq!(retry_delay_ms(attempts), expected_ms);
        }
    }
}
