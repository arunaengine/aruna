//! Host calls per install on its receiver: a token bucket that refills at a steady rate, so a
//! busy install waits without slowing the others.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use ulid::Ulid;

/// Host calls per second one install makes on average, and at most at once.
pub const CALL_RATE: f64 = 50.0;
pub const CALL_BURST: f64 = 200.0;

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

static BUCKETS: LazyLock<Mutex<HashMap<Ulid, Bucket>>> = LazyLock::new(Default::default);

/// Takes one call of `install_id` at `now`; `false` when the install is over its rate.
pub fn take_call(install_id: Ulid, now: Instant) -> bool {
    let mut buckets = BUCKETS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let bucket = buckets.entry(install_id).or_insert(Bucket {
        tokens: CALL_BURST,
        at: now,
    });
    let elapsed = now.saturating_duration_since(bucket.at).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * CALL_RATE).min(CALL_BURST);
    bucket.at = bucket.at.max(now);
    match bucket.tokens >= 1.0 {
        true => {
            bucket.tokens -= 1.0;
            true
        }
        false => false,
    }
}

/// Forgets an install's bucket, after it was removed.
pub fn forget_calls(install_id: Ulid) {
    let mut buckets = BUCKETS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    buckets.remove(&install_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn refills_steadily() {
        let (install, other) = (Ulid::from_parts(77, 1), Ulid::from_parts(77, 2));
        let start = Instant::now();
        let taken = (0..1_000).filter(|_| take_call(install, start)).count();
        assert_eq!(taken, CALL_BURST as usize);
        assert!(take_call(other, start));
        let later = start + Duration::from_secs(1);
        let refilled = (0..1_000).filter(|_| take_call(install, later)).count();
        assert_eq!(refilled, CALL_RATE as usize);
        forget_calls(install);
        assert!(take_call(install, later));
    }
}
