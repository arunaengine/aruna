//! Host calls and webhook requests per install on its receiver: token buckets that refill at a
//! steady rate, so a busy install waits without slowing the others.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use ulid::Ulid;

/// Host calls per second one install makes on average, and at most at once.
pub const CALL_RATE: f64 = 50.0;
pub const CALL_BURST: f64 = 200.0;
/// Webhook requests one install takes per minute, and at most at once.
pub const HOOKS_PER_MINUTE: f64 = 60.0;

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    at_ms: u64,
}

type Buckets = LazyLock<Mutex<HashMap<Ulid, Bucket>>>;

static BUCKETS: Buckets = LazyLock::new(Default::default);
static HOOK_BUCKETS: Buckets = LazyLock::new(Default::default);

/// Takes one call of `install_id` at `now_ms`; `false` when the install is over its rate.
pub(crate) fn take_call(install_id: Ulid, now_ms: u64) -> bool {
    take(&BUCKETS, install_id, now_ms, (CALL_RATE, CALL_BURST))
}

/// Takes one webhook request of `install_id` at `now_ms`; `false` when it is over its rate.
pub(crate) fn take_hook(install_id: Ulid, now_ms: u64) -> bool {
    let rate = (HOOKS_PER_MINUTE / 60.0, HOOKS_PER_MINUTE);
    take(&HOOK_BUCKETS, install_id, now_ms, rate)
}

fn take(buckets: &Buckets, install_id: Ulid, now_ms: u64, (rate, burst): (f64, f64)) -> bool {
    let mut buckets = buckets
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let bucket = buckets.entry(install_id).or_insert(Bucket {
        tokens: burst,
        at_ms: now_ms,
    });
    let elapsed = now_ms.saturating_sub(bucket.at_ms) as f64 / 1000.0;
    bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
    bucket.at_ms = bucket.at_ms.max(now_ms);
    match bucket.tokens >= 1.0 {
        true => {
            bucket.tokens -= 1.0;
            true
        }
        false => false,
    }
}

/// Forgets an install's buckets, after it was removed.
pub fn forget_calls(install_id: Ulid) {
    for buckets in [&BUCKETS, &HOOK_BUCKETS] {
        let mut buckets = buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buckets.remove(&install_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refills_steadily() {
        let (install, other) = (Ulid::from_parts(77, 1), Ulid::from_parts(77, 2));
        let start = 1_000_000;
        let taken = (0..1_000).filter(|_| take_call(install, start)).count();
        assert_eq!(taken, CALL_BURST as usize);
        assert!(take_call(other, start));
        let later = start + 1_000;
        let refilled = (0..1_000).filter(|_| take_call(install, later)).count();
        assert_eq!(refilled, CALL_RATE as usize);
        forget_calls(install);
        assert!(take_call(install, later));
        let hooks = (0..1_000).filter(|_| take_hook(install, start)).count();
        assert_eq!(hooks, HOOKS_PER_MINUTE as usize);
        assert!(take_hook(install, start + 1_000));
    }
}
