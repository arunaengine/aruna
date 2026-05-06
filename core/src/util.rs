// core/src/util.rs

/// Get current unix timestamp in seconds.
/// Returns 0 if system time is before UNIX epoch (should never happen in practice).
#[inline]
pub fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Get current unix timestamp in milliseconds.
#[inline]
pub fn unix_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Compute XOR distance between two 32-byte values.
#[inline]
pub fn xor_distance_32(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for (i, byte) in result.iter_mut().enumerate() {
        *byte = a[i] ^ b[i];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_timestamp_secs() {
        let ts = unix_timestamp_secs();
        // Should be after 2020 (1577836800)
        assert!(ts > 1577836800);
    }

    #[test]
    fn test_xor_distance_32() {
        let a = [0xAA; 32];
        let b = [0x0F; 32];
        let dist = xor_distance_32(&a, &b);
        assert_eq!(dist, [0xA5; 32]);
    }
}
