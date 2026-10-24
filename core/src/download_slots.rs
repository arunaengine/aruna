//! Counts a node's DRS download slots for its heartbeat and keeps the holders a choosing node
//! skips for a while after they failed. Both live in memory only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::NodeId;

/// Downloads one node serves at once.
pub const DOWNLOAD_SLOTS: u32 = 256;
/// How long a failed holder is skipped.
pub const AVOID_MS: u64 = 30_000;
/// At most this many holders are skipped at once; the one avoided longest goes first.
const MAX_AVOIDED: usize = 256;

/// The download slot use a heartbeat reports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DownloadSlots {
    pub active: u32,
    pub max: u32,
    /// Downloads refused for lack of a slot since the previous heartbeat.
    pub refused: u32,
}

impl DownloadSlots {
    pub fn free(&self) -> u32 {
        self.max.saturating_sub(self.active)
    }
}

/// The node-wide download slots and the refusals since the last heartbeat.
#[derive(Debug)]
pub struct DownloadGauge {
    slots: Arc<Semaphore>,
    max: u32,
    refused: AtomicU32,
}

impl Default for DownloadGauge {
    fn default() -> Self {
        Self::new(DOWNLOAD_SLOTS)
    }
}

impl DownloadGauge {
    pub fn new(max: u32) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(max as usize)),
            max,
            refused: AtomicU32::new(0),
        }
    }

    /// Takes one slot, or counts a refusal when none is free.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        let permit = self.slots.clone().try_acquire_owned().ok();
        if permit.is_none() {
            self.refused.fetch_add(1, Ordering::Relaxed);
        }
        permit
    }

    /// The current use; the refusal count starts again from zero.
    pub fn snapshot(&self) -> DownloadSlots {
        let free = self.slots.available_permits() as u32;
        DownloadSlots {
            active: self.max.saturating_sub(free),
            max: self.max,
            refused: self.refused.swap(0, Ordering::Relaxed),
        }
    }
}

/// Holders this node skips until the stored time (Unix milliseconds).
#[derive(Debug, Default)]
pub struct AvoidList {
    until: Mutex<HashMap<NodeId, u64>>,
}

impl AvoidList {
    /// Skips `node_id` for [`AVOID_MS`] from `now_ms`.
    pub fn avoid(&self, node_id: NodeId, now_ms: u64) {
        let Ok(mut until) = self.until.lock() else {
            return;
        };
        until.retain(|_, ends| *ends > now_ms);
        if until.len() >= MAX_AVOIDED
            && !until.contains_key(&node_id)
            && let Some(first) = until
                .iter()
                .min_by_key(|(_, ends)| **ends)
                .map(|(id, _)| *id)
        {
            until.remove(&first);
        }
        until.insert(node_id, now_ms.saturating_add(AVOID_MS));
    }

    pub fn avoided(&self, node_id: &NodeId, now_ms: u64) -> bool {
        self.until
            .lock()
            .is_ok_and(|until| until.get(node_id).is_some_and(|ends| *ends > now_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn gauge_counts_refusals() {
        let gauge = DownloadGauge::new(1);
        let held = gauge.try_acquire().unwrap();
        assert!(gauge.try_acquire().is_none());
        let slots = gauge.snapshot();
        assert_eq!((slots.active, slots.max, slots.refused), (1, 1, 1));
        assert_eq!(slots.free(), 0);
        // Refusals count per heartbeat; a released slot is free again.
        drop(held);
        let slots = gauge.snapshot();
        assert_eq!((slots.active, slots.refused), (0, 0));
    }

    #[test]
    fn avoidance_expires() {
        let list = AvoidList::default();
        list.avoid(node(1), 1_000);
        assert!(list.avoided(&node(1), 1_000 + AVOID_MS - 1));
        assert!(!list.avoided(&node(1), 1_000 + AVOID_MS));
        assert!(!list.avoided(&node(2), 1_000));
    }

    #[test]
    fn avoid_list_bounded() {
        let list = AvoidList::default();
        for seed in 0..=MAX_AVOIDED as u16 {
            let mut bytes = [0u8; 32];
            bytes[..2].copy_from_slice(&seed.to_be_bytes());
            let id = iroh::SecretKey::from_bytes(&bytes).public();
            list.avoid(id, 10 + u64::from(seed));
        }
        assert_eq!(list.until.lock().unwrap().len(), MAX_AVOIDED);
    }
}
