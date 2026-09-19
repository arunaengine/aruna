//! Reads the current Unix time in seconds and in milliseconds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Returns 0 if system time is before UNIX epoch (should never happen in practice).
#[inline]
pub fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[inline]
pub fn unix_timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::unix_timestamp_secs;

    #[test]
    fn timestamp_after_epoch() {
        let ts = unix_timestamp_secs();
        // Should be after 2020 (1577836800)
        assert!(ts > 1577836800);
    }
}
