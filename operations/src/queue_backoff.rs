use std::time::Duration;

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

// Deterministic jitter from the caller's own id decorrelates the retries of
// drivers that conflict on one record, and the wait lets the winner commit.
pub(crate) fn conflict_backoff(attempt: usize, seed: &[u8]) -> Duration {
    let base = retry_after_ms(attempt as u32, 25, 250);
    let mut head = [0u8; 8];
    head.copy_from_slice(&seed[..8]);
    let jitter = u64::from_le_bytes(head) % base;
    Duration::from_millis(base.saturating_add(jitter))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_jitters() {
        // Backoff is deterministic per seed and stays within [base, 2*base).
        let seed = [7u8; 16];
        let base = retry_after_ms(0, 25, 250);
        let first = conflict_backoff(0, &seed);
        assert_eq!(first, conflict_backoff(0, &seed));
        let ms = first.as_millis() as u64;
        assert!(ms >= base && ms < base.saturating_mul(2));
    }

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
}
