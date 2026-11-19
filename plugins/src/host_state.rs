//! The plugin host's node-wide process state: call and webhook rates, the artifact memory budget,
//! when unnamed accounts were first seen, and run secrets. Operations reach it through effects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};

use zeroize::Zeroizing;

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
/// The secret fields each broker of a running run holds, by run, broker and slot, per value id.
static HELD: LazyLock<Mutex<HashMap<(Holder, String), Held>>> = LazyLock::new(Default::default);

/// A run and one broker instance of it; a later attempt's broker is another holder.
pub type Holder = (u128, u128);
type Held = BTreeMap<u128, Zeroizing<Vec<u8>>>;

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
        .try_update(Ordering::AcqRel, Ordering::Acquire, fits)
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

/// The newest value write id whose fields `holder` holds for `slot`.
pub fn held_value(holder: Holder, slot: &str) -> Option<u128> {
    let held = HELD.lock().unwrap_or_else(PoisonError::into_inner);
    let values = held.get(&(holder, slot.to_string()))?;
    values.keys().next_back().copied()
}

/// Keeps `fields` as value `value` of the holder's `slot` when given, and answers the fields the
/// holder keeps for that value; `None` when it keeps none.
pub fn hold_value(
    holder: Holder,
    slot: &str,
    value: u128,
    fields: Option<Vec<u8>>,
) -> Option<Vec<u8>> {
    let mut held = HELD.lock().unwrap_or_else(PoisonError::into_inner);
    let values = held.entry((holder, slot.to_string())).or_default();
    if let Some(fields) = fields {
        values.insert(value, Zeroizing::new(fields));
    }
    values.get(&value).map(|fields| fields.to_vec())
}

/// Forgets every secret `holder` holds, once its broker is gone; other holders keep theirs.
pub fn forget_held(holder: Holder) {
    let mut held = HELD.lock().unwrap_or_else(PoisonError::into_inner);
    held.retain(|(kept, _), _| *kept != holder);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_while_unchanged() {
        // One slot through the uses of a run, as the release of each use asks and answers.
        let holder = (92 << 64 | 1, 1);
        let (one, two) = (|| Some(b"one".to_vec()), || Some(b"two".to_vec()));
        assert_eq!(held_value(holder, "a"), None, "the first use names no value");
        assert_eq!(hold_value(holder, "a", 1, one()), one());
        assert_eq!(held_value(holder, "a"), Some(1));
        // The check passed again for the same value: the held fields are used.
        assert_eq!(hold_value(holder, "a", 1, None), one());
        // A rotated value arrives with its fields, and later uses name it.
        assert_eq!(hold_value(holder, "a", 2, two()), two());
        assert_eq!(held_value(holder, "a"), Some(2));
        assert_eq!(hold_value(holder, "a", 2, None), two());
        // A value the run never received has no fields here.
        assert_eq!(hold_value(holder, "a", 3, None), None);
        forget_held(holder);
    }

    #[test]
    fn holds_per_broker() {
        let run = 91 << 64 | 1;
        let (holder, other, later) = ((run, 1), (run + 1, 1), (run, 2));
        assert_eq!(held_value(holder, "token"), None);
        assert_eq!(hold_value(holder, "token", 4, None), None);
        assert_eq!(hold_value(holder, "token", 4, Some(vec![1])), Some(vec![1]));
        assert_eq!(hold_value(holder, "token", 7, Some(vec![2])), Some(vec![2]));
        assert_eq!(held_value(holder, "token"), Some(7));
        // An older value stays held, so a call that checked it still finds its fields.
        assert_eq!(hold_value(holder, "token", 4, None), Some(vec![1]));
        assert_eq!(held_value(other, "token"), None);
        // A later broker of the same run keeps its fields when the older one is gone.
        assert_eq!(hold_value(later, "token", 7, Some(vec![3])), Some(vec![3]));
        forget_held(holder);
        assert_eq!(held_value(holder, "token"), None);
        assert_eq!(held_value(later, "token"), Some(7));
        forget_held(later);
    }

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
        assert_eq!(
            first_seen(std::slice::from_ref(&unnamed), &[], 12),
            Vec::<u64>::new()
        );
        assert_eq!(first_seen(&[], &[unnamed], 20), vec![20]);
    }
}
