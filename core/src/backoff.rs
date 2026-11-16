//! Computes capped exponential retry delays with optional seeded jitter.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

/// The delay before retry `attempt`: `base` doubled once per attempt and capped at `cap`, plus
/// `seed` modulo that delay in whole milliseconds as jitter. A `seed` of 0 adds no jitter.
pub fn capped_backoff(base: Duration, cap: Duration, attempt: u32, seed: u64) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    let delay = base.saturating_mul(factor).min(cap);
    match u64::try_from(delay.as_millis()).unwrap_or(u64::MAX) {
        0 => delay,
        millis => delay.saturating_add(Duration::from_millis(seed % millis)),
    }
}

#[cfg(test)]
mod tests {
    use super::capped_backoff;
    use std::time::Duration;

    const BASE: Duration = Duration::from_millis(250);
    const CAP: Duration = Duration::from_secs(30);

    #[test]
    fn doubles_until_cap() {
        let delays: Vec<u128> = [0, 1, 2, 6, 7, 31, 32, u32::MAX]
            .into_iter()
            .map(|attempt| capped_backoff(BASE, CAP, attempt, 0).as_millis())
            .collect();
        assert_eq!(
            delays,
            [250, 500, 1_000, 16_000, 30_000, 30_000, 30_000, 30_000]
        );
    }

    #[test]
    fn seed_adds_jitter() {
        let base = Duration::from_millis(25);
        let cap = Duration::from_millis(250);
        let seed = u64::from_le_bytes([7; 8]);
        let first = capped_backoff(base, cap, 0, seed);
        assert_eq!(
            first,
            capped_backoff(base, cap, 0, seed),
            "same seed, same delay"
        );
        assert!(
            first >= base && first < base * 2,
            "jitter stays below the delay: {first:?}"
        );
        assert_eq!(capped_backoff(base, cap, 0, 0), base, "seed 0 adds nothing");
    }
}
