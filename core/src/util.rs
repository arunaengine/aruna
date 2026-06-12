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

/// Smallest key strictly greater than every key starting with `prefix`,
/// or `None` if no such key exists (prefix is all `0xFF`).
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] = upper[idx].saturating_add(1);
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_upper_bound() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
    }

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
