//! The boot-fenced nonrepeating change counter of files and their parent folders, the head
//! change observer feeding it, and the NFS write verifier.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::vfs::BucketScope;
use aruna_core::vfs_effects::{VfsEvent, VfsFailure};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Answer, VfsHandle};

/// A change value keeps the boot generation above this many local sequence bits.
const LOCAL_BITS: u32 = 40;
const LOCAL_LIMIT: u64 = 1 << LOCAL_BITS;
/// The highest boot generation stays free to mark an unconfigured counter.
const BOOT_LIMIT: u64 = (1 << (u64::BITS - LOCAL_BITS)) - 1;
const UNSET: u64 = u64::MAX;
/// Keys whose change values stay cached; a later miss takes a fresh value.
pub(crate) const CACHE_LIMIT: usize = 65_536;

pub(crate) struct Changes {
    /// Boot generation and local sequence in one word, so no allocation sees half a reboot.
    counter: AtomicU64,
    sequence: AtomicU64,
    cache: Mutex<ChangeCache>,
    verifier: Mutex<Option<Verifier>>,
}

#[derive(Default)]
struct ChangeCache {
    values: HashMap<(String, String), u64>,
    order: VecDeque<(String, String)>,
}

#[derive(Clone, Copy)]
struct Verifier {
    base: u64,
    step: u64,
}

impl ChangeCache {
    fn put(&mut self, bucket: &str, key: &str, value: u64) {
        let name = (bucket.to_string(), key.to_string());
        if self.values.insert(name.clone(), value).is_none() {
            self.order.push_back(name);
        }
        while self.values.len() > CACHE_LIMIT {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.values.remove(&oldest);
        }
    }

    fn forget(&mut self, bucket: &str, key: &str) {
        self.values.remove(&(bucket.to_string(), key.to_string()));
    }
}

/// `key` and every folder prefix above it, the bucket root `""` included.
pub(crate) fn with_ancestors(key: &str) -> Vec<&str> {
    let mut names = vec![""];
    for (index, _) in key.match_indices('/') {
        if index + 1 < key.len() {
            names.push(&key[..=index]);
        }
    }
    if !key.is_empty() {
        names.push(key);
    }
    names
}

impl Changes {
    pub(crate) fn new() -> Self {
        Self {
            counter: AtomicU64::new(UNSET),
            sequence: AtomicU64::new(0),
            cache: Mutex::new(ChangeCache::default()),
            verifier: Mutex::new(None),
        }
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    /// The next value: boot generation in the high bits, local sequence in the low bits.
    fn allocate(&self) -> Result<u64, VfsFailure> {
        let step = |value: u64| {
            let local = value & (LOCAL_LIMIT - 1);
            (value != UNSET && local + 1 < LOCAL_LIMIT).then(|| value + 1)
        };
        match self
            .counter
            .try_update(Ordering::AcqRel, Ordering::Acquire, step)
        {
            Ok(previous) => Ok(previous + 1),
            Err(UNSET) => Err(VfsFailure::Unconfigured),
            Err(_) => Err(VfsFailure::CounterExhausted),
        }
    }

    /// Advances `key`, and its parent folders when `ancestors` is set, to one fresh value.
    pub(crate) fn touch(
        &self,
        bucket: &str,
        key: &str,
        ancestors: bool,
    ) -> Result<u64, VfsFailure> {
        // Allocating under the cache lock keeps every cached value rising.
        let mut cache = self.cache.lock();
        let value = self.allocate()?;
        match ancestors {
            true => {
                for name in with_ancestors(key) {
                    cache.put(bucket, name, value);
                }
            }
            false => cache.put(bucket, key, value),
        }
        Ok(value)
    }

    /// The cached value of `key`, or a fresh one that is kept for later reads.
    pub(crate) fn read(&self, bucket: &str, key: &str) -> Result<u64, VfsFailure> {
        let mut cache = self.cache.lock();
        let name = (bucket.to_string(), key.to_string());
        if let Some(value) = cache.values.get(&name) {
            return Ok(*value);
        }
        let value = self.allocate()?;
        cache.put(bucket, key, value);
        Ok(value)
    }

    /// Called from the storage worker: short locks and atomics only, never a wait.
    pub(crate) fn observe(&self, bucket: &str, key: &str) {
        self.sequence.fetch_add(1, Ordering::AcqRel);
        if self.touch(bucket, key, true).is_err() {
            // Without a fresh value the next read must miss and fail closed.
            let mut cache = self.cache.lock();
            for name in with_ancestors(key) {
                cache.forget(bucket, name);
            }
        }
    }

    pub(crate) fn configure(&self, boot_generation: u64, verifier: [u8; 8]) -> Answer {
        if boot_generation >= BOOT_LIMIT {
            return Err(VfsFailure::CounterExhausted);
        }
        let start = boot_generation << LOCAL_BITS;
        let newer =
            |value: u64| (value == UNSET || value >> LOCAL_BITS < boot_generation).then_some(start);
        let installed = self
            .counter
            .try_update(Ordering::AcqRel, Ordering::Acquire, newer);
        // The same boot configured again keeps its sequence; an older boot never returns.
        if let Err(current) = installed
            && current >> LOCAL_BITS != boot_generation
        {
            return Err(VfsFailure::CounterExhausted);
        }
        // A repeated call for the running boot keeps its advances, so no verifier comes back.
        let mut held = self.verifier.lock();
        if installed.is_ok() || held.is_none() {
            *held = Some(Verifier {
                base: u64::from_be_bytes(verifier),
                step: 0,
            });
        }
        Ok(VfsEvent::Done)
    }

    /// The active verifier: the configured boot verifier plus the advances of this boot.
    pub(crate) fn verifier(&self) -> Answer {
        let verifier = (*self.verifier.lock()).ok_or(VfsFailure::Unconfigured)?;
        let value = verifier.base.checked_add(verifier.step);
        let value = value.ok_or(VfsFailure::CounterExhausted)?;
        Ok(VfsEvent::Verifier {
            verifier: value.to_be_bytes(),
        })
    }

    pub(crate) fn advance_verifier(&self) -> Answer {
        let mut held = self.verifier.lock();
        let verifier = held.as_mut().ok_or(VfsFailure::Unconfigured)?;
        let step = verifier
            .step
            .checked_add(1)
            .ok_or(VfsFailure::CounterExhausted)?;
        let value = verifier.base.checked_add(step);
        let value = value.ok_or(VfsFailure::CounterExhausted)?;
        verifier.step = step;
        Ok(VfsEvent::Verifier {
            verifier: value.to_be_bytes(),
        })
    }
}

impl VfsHandle {
    pub(crate) fn next_change(&self, scope: &BucketScope, key: Option<&str>) -> Answer {
        let value = self
            .shared
            .changes
            .touch(&scope.bucket, key.unwrap_or(""), true)?;
        Ok(VfsEvent::ChangeValue { value })
    }

    /// Samples the observer sequence first, so a caller comparing two samples sees any change.
    pub(crate) fn read_change(&self, scope: &BucketScope, keys: &[String]) -> Answer {
        let sequence = self.shared.changes.sequence();
        let values = keys
            .iter()
            .map(|key| self.shared.changes.read(&scope.bucket, key))
            .collect::<Result<Vec<u64>, VfsFailure>>()?;
        Ok(VfsEvent::ChangeSnapshot { sequence, values })
    }
}
