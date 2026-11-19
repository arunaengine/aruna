//! The plugin host's node-wide state, kept for the life of the node process: the host call and
//! webhook rates per install, the memory budget of artifact copies, and when each account no
//! install names was first seen. Operations reach it through the runner's plugin effect.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};

/// Host calls per second one install makes on average, and at most at once.
pub const CALL_RATE: f64 = 50.0;
pub const CALL_BURST: f64 = 200.0;
/// Webhook requests one install takes per minute, and at most at once.
pub const HOOKS_PER_MINUTE: f64 = 60.0;
/// Memory one node spends at most on artifact uploads and assemblies, in MiB.
pub const MEMORY_MIB: u32 = 256;

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    at_ms: u64,
}

type Buckets = LazyLock<Mutex<HashMap<u128, Bucket>>>;

static BUCKETS: Buckets = LazyLock::new(Default::default);
static HOOK_BUCKETS: Buckets = LazyLock::new(Default::default);
/// MiB of the memory budget reserved now.
static RESERVED: AtomicU32 = AtomicU32::new(0);
/// When this node first saw each unnamed account; a restart starts the grace again.
static SEEN: LazyLock<Mutex<HashMap<String, u64>>> = LazyLock::new(Default::default);

/// Takes one host call of install `install` at `now_ms`; `false` when it is over its rate.
pub fn take_call(install: u128, now_ms: u64) -> bool {
    take(&BUCKETS, install, now_ms, (CALL_RATE, CALL_BURST))
}

/// Takes one webhook request of install `install` at `now_ms`; `false` when over its rate.
pub fn take_hook(install: u128, now_ms: u64) -> bool {
    let rate = (HOOKS_PER_MINUTE / 60.0, HOOKS_PER_MINUTE);
    take(&HOOK_BUCKETS, install, now_ms, rate)
}

fn take(buckets: &Buckets, install: u128, now_ms: u64, (rate, burst): (f64, f64)) -> bool {
    let mut buckets = buckets.lock().unwrap_or_else(PoisonError::into_inner);
    let bucket = buckets.entry(install).or_insert(Bucket {
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

/// Reserves memory for `bytes` and answers the MiB it holds, or `None` while the node spends
/// its budget elsewhere. [`release`] gives them back.
pub fn reserve(bytes: u64) -> Option<u32> {
    let mib = u32::try_from(bytes.div_ceil(1 << 20).max(1)).ok()?;
    let fits = |reserved: u32| {
        reserved
            .checked_add(mib)
            .filter(|total| *total <= MEMORY_MIB)
    };
    RESERVED
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, fits)
        .ok()?;
    Some(mib)
}

/// Gives back `mib` that [`reserve`] answered.
pub fn release(mib: u32) {
    RESERVED.fetch_sub(mib, Ordering::AcqRel);
}

/// Forgets the `named` accounts and answers when each `unnamed` one was first seen, `now_ms`
/// for one not seen before.
pub fn first_seen(named: &[String], unnamed: &[String], now_ms: u64) -> Vec<u64> {
    let mut seen = SEEN.lock().unwrap_or_else(PoisonError::into_inner);
    for account in named {
        seen.remove(account);
    }
    (unnamed.iter())
        .map(|account| *seen.entry(account.clone()).or_insert(now_ms))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refills_steadily() {
        let (install, other) = (77 << 64 | 1, 77 << 64 | 2);
        let start = 1_000_000;
        let taken = (0..1_000).filter(|_| take_call(install, start)).count();
        assert_eq!(taken, CALL_BURST as usize);
        assert!(take_call(other, start));
        let later = start + 1_000;
        let refilled = (0..1_000).filter(|_| take_call(install, later)).count();
        assert_eq!(refilled, CALL_RATE as usize);
        let hooks = (0..1_000).filter(|_| take_hook(install, start)).count();
        assert_eq!(hooks, HOOKS_PER_MINUTE as usize);
        assert!(take_hook(install, start + 1_000));
    }

    #[test]
    fn budget_given_back() {
        let whole = u64::from(MEMORY_MIB) << 20;
        let held = reserve(whole).unwrap();
        assert_eq!(held, MEMORY_MIB);
        assert_eq!(reserve(1), None);
        release(held);
        assert_eq!(reserve(1), Some(1));
        release(1);
    }

    #[test]
    fn named_forgotten() {
        let (unnamed, again) = ("seen-a".to_string(), "seen-b".to_string());
        assert_eq!(first_seen(&[], std::slice::from_ref(&unnamed), 5), vec![5]);
        assert_eq!(
            first_seen(&[], &[unnamed.clone(), again.clone()], 9),
            vec![5, 9]
        );
        // An account named again starts its grace anew once it is unnamed later.
        assert_eq!(first_seen(&[unnamed.clone()], &[], 12), Vec::<u64>::new());
        assert_eq!(first_seen(&[], &[unnamed], 20), vec![20]);
    }
}
