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
}
