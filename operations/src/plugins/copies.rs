//! The memory whole artifact copies take on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use tokio::sync::{Semaphore, SemaphorePermit};

/// Memory one node spends at most on artifact uploads and assemblies, in MiB.
const MEMORY_MIB: u32 = 256;

static MEMORY: Semaphore = Semaphore::const_new(MEMORY_MIB as usize);

/// Memory reserved for one artifact upload or assembly, released when dropped.
pub type MemoryPermit = SemaphorePermit<'static>;

/// Reserves memory for `bytes`, or `None` while the node spends its budget elsewhere.
pub fn reserve(bytes: usize) -> Option<MemoryPermit> {
    let mib = u32::try_from(bytes.div_ceil(1 << 20).max(1)).ok()?;
    MEMORY.try_acquire_many(mib).ok()
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
        self.memory.num_permits() as u32
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
