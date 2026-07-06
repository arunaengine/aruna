pub(crate) const QUEUE_RETRY_BASE_MS: u64 = 250;
pub(crate) const QUEUE_RETRY_MAX_MS: u64 = 30_000;

pub(crate) fn queue_retry_after_ms(attempts: u32) -> u64 {
    retry_after_ms(attempts, QUEUE_RETRY_BASE_MS, QUEUE_RETRY_MAX_MS)
}

pub(crate) fn retry_after_ms(attempts: u32, base_ms: u64, max_ms: u64) -> u64 {
    let shift = attempts.min(7);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    base_ms.saturating_mul(multiplier).min(max_ms)
}

pub(crate) const TIMER_RETRY_BASE_SECS: u64 = 30;
pub(crate) const TIMER_RETRY_MAX_SECS: u64 = 300;

/// Timer-scale backoff for the in-memory placement-retry and outbox-drain re-arm
/// counters: 30s base, doubling, capped at 300s.
pub(crate) const fn timer_retry_after_secs(attempts: u32) -> u64 {
    let shift = if attempts < 7 { attempts } else { 7 };
    let multiplier = match 1u64.checked_shl(shift) {
        Some(value) => value,
        None => u64::MAX,
    };
    let scaled = TIMER_RETRY_BASE_SECS.saturating_mul(multiplier);
    if scaled < TIMER_RETRY_MAX_SECS {
        scaled
    } else {
        TIMER_RETRY_MAX_SECS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_preserves_queue_policy_values() {
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
            assert_eq!(queue_retry_after_ms(attempts), expected_ms);
        }
    }

    #[test]
    fn timer_backoff_doubles_from_base_to_cap() {
        let expected = [
            (0, 30),
            (1, 60),
            (2, 120),
            (3, 240),
            (4, 300),
            (5, 300),
            (7, 300),
            (u32::MAX, 300),
        ];

        for (attempts, expected_secs) in expected {
            assert_eq!(timer_retry_after_secs(attempts), expected_secs);
        }
    }
}
