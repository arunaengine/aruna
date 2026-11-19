//! The memory whole artifact copies take on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicU32, Ordering};

/// Memory one node spends at most on artifact uploads and assemblies, in MiB.
const MEMORY_MIB: u32 = 256;

/// MiB of the budget reserved now.
static RESERVED: AtomicU32 = AtomicU32::new(0);

/// Memory reserved for one artifact upload or assembly, released when dropped.
#[derive(Debug)]
pub struct MemoryPermit {
    mib: u32,
}

impl MemoryPermit {
    /// MiB this reservation holds of the node's budget.
    pub fn mib(&self) -> u32 {
        self.mib
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        RESERVED.fetch_sub(self.mib, Ordering::AcqRel);
    }
}

/// Reserves memory for `bytes`, or `None` while the node spends its budget elsewhere.
pub fn reserve(bytes: usize) -> Option<MemoryPermit> {
    let mib = u32::try_from(bytes.div_ceil(1 << 20).max(1)).ok()?;
    let fits = |reserved: u32| {
        reserved
            .checked_add(mib)
            .filter(|total| *total <= MEMORY_MIB)
    };
    RESERVED
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, fits)
        .ok()?;
    Some(MemoryPermit { mib })
}

/// A whole artifact in memory together with the memory reserved for it; dropping it releases
/// the reservation, so whoever consumes the bytes holds it until done.
pub struct Assembled {
    pub bytes: Vec<u8>,
    memory: MemoryPermit,
}

impl Assembled {
    pub fn new(bytes: Vec<u8>, memory: MemoryPermit) -> Self {
        Self { bytes, memory }
    }

    /// MiB this artifact holds of the node's budget.
    pub fn reserved_mib(&self) -> u32 {
        self.memory.mib()
    }
}

impl PartialEq for Assembled {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl std::fmt::Debug for Assembled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Assembled({} bytes)", self.bytes.len())
    }
}

#[cfg(test)]
#[path = "copies_tests.rs"]
mod tests;
