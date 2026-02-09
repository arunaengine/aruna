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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_timestamp_secs() {
        let ts = unix_timestamp_secs();
        // Should be after 2020 (1577836800)
        assert!(ts > 1577836800);
    }
}
