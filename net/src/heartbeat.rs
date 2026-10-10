//! Holds the latest heartbeat of each sync peer in memory and bounds inbound heartbeat streams.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::heartbeat::{HeldHeartbeat, NodeHeartbeat};
use parking_lot::Mutex;

/// Shortest gap between two accepted heartbeats of one peer.
const MIN_HEARTBEAT_GAP: Duration = Duration::from_secs(10);
/// Inbound heartbeat streams read at once, per peer and overall.
const PEER_STREAMS: usize = 2;
const TOTAL_STREAMS: usize = 64;

#[derive(Debug)]
pub(crate) struct HeartbeatTable {
    state: Mutex<TableState>,
    started: Instant,
}

impl Default for HeartbeatTable {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            started: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
struct TableState {
    held: BTreeMap<NodeId, Held>,
    reading: BTreeMap<NodeId, usize>,
    total: usize,
}

#[derive(Debug)]
struct Held {
    heartbeat: NodeHeartbeat,
    received: Instant,
    received_at_ms: u64,
}

/// One admitted inbound heartbeat stream; dropping it frees the slot.
#[derive(Debug)]
pub struct HeartbeatSlot {
    table: Arc<HeartbeatTable>,
    node: NodeId,
}

impl HeartbeatTable {
    /// A slot for reading one heartbeat of `node`, or none past the stream limits.
    pub(crate) fn slot(self: &Arc<Self>, node: NodeId) -> Option<HeartbeatSlot> {
        let mut state = self.state.lock();
        let reading = state.reading.get(&node).copied().unwrap_or(0);
        if reading >= PEER_STREAMS || state.total >= TOTAL_STREAMS {
            return None;
        }
        state.reading.insert(node, reading + 1);
        state.total += 1;
        Some(HeartbeatSlot {
            table: Arc::clone(self),
            node,
        })
    }

    /// Stores `heartbeat` when `node` is a member, the heartbeat orders after the held one, and
    /// the peer's last accepted heartbeat is old enough. Membership is read under the table lock.
    pub(crate) fn record(
        &self,
        node: NodeId,
        heartbeat: NodeHeartbeat,
        member: impl FnOnce() -> bool,
    ) -> bool {
        let mut state = self.state.lock();
        if !member() {
            state.held.remove(&node);
            return false;
        }
        if let Some(held) = state.held.get(&node)
            && (held.heartbeat.order() >= heartbeat.order()
                || held.received.elapsed() < MIN_HEARTBEAT_GAP)
        {
            return false;
        }
        state.held.insert(
            node,
            Held {
                heartbeat,
                received: Instant::now(),
                received_at_ms: aruna_core::time::unix_timestamp_millis(),
            },
        );
        true
    }

    /// Stores this node's own heartbeat; it orders like a peer's but has no rate limit.
    pub(crate) fn record_own(&self, node: NodeId, heartbeat: NodeHeartbeat) {
        let mut state = self.state.lock();
        if state
            .held
            .get(&node)
            .is_some_and(|held| held.heartbeat.order() >= heartbeat.order())
        {
            return;
        }
        state.held.insert(
            node,
            Held {
                heartbeat,
                received: Instant::now(),
                received_at_ms: aruna_core::time::unix_timestamp_millis(),
            },
        );
    }

    pub(crate) fn held(&self) -> Vec<HeldHeartbeat> {
        let state = self.state.lock();
        state
            .held
            .iter()
            .map(|(node_id, held)| HeldHeartbeat {
                node_id: *node_id,
                heartbeat: held.heartbeat.clone(),
                age_ms: u64::try_from(held.received.elapsed().as_millis()).unwrap_or(u64::MAX),
                received_at_ms: held.received_at_ms,
            })
            .collect()
    }

    /// Time since the last held heartbeat of `node`, or since this table started without one.
    pub(crate) fn silence(&self, node: &NodeId) -> Duration {
        let state = self.state.lock();
        state
            .held
            .get(node)
            .map_or(self.started, |held| held.received)
            .elapsed()
    }

    /// Drops the heartbeats of nodes that are no longer sync peers.
    pub(crate) fn retain(&self, keep: impl Fn(&NodeId) -> bool) {
        self.state.lock().held.retain(|node, _| keep(node));
    }
}

impl Drop for HeartbeatSlot {
    fn drop(&mut self) {
        let mut state = self.table.state.lock();
        state.total = state.total.saturating_sub(1);
        if let Some(reading) = state.reading.get_mut(&self.node) {
            *reading = reading.saturating_sub(1);
            if *reading == 0 {
                state.reading.remove(&self.node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::node_info::{AdvertisementEpoch, NodeUtilization};

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn heartbeat(publisher_generation: u64, sequence: u64) -> NodeHeartbeat {
        let epoch = AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation,
            observed_at_ms: 0,
        };
        NodeHeartbeat {
            realm_id: RealmId([1; 32]),
            epoch,
            sequence,
            utilization: NodeUtilization {
                storage_bytes_used: 0,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 0,
            },
            availability: Vec::new(),
            reservation: ComputeReservationSnapshot {
                epoch,
                ..ComputeReservationSnapshot::default()
            },
            demand: ComputeDemandSnapshot {
                epoch,
                ..ComputeDemandSnapshot::default()
            },
        }
    }

    /// An older or equal heartbeat never replaces a newer one, and a peer that is no longer a
    /// member is dropped instead of stored.
    #[test]
    fn record_keeps_newest() {
        let table = HeartbeatTable::default();
        assert!(table.record(node(1), heartbeat(2, 1), || true));
        assert!(!table.record(node(1), heartbeat(1, 9), || true));
        assert!(!table.record(node(1), heartbeat(2, 1), || true));
        assert!(!table.record(node(2), heartbeat(1, 1), || false));
        assert_eq!(table.held().len(), 1);
        assert!(!table.record(node(1), heartbeat(3, 1), || false));
        assert!(table.held().is_empty());
    }

    /// A peer cannot replace its heartbeat faster than the minimum gap, even with newer ones.
    #[test]
    fn record_limits_rate() {
        let table = HeartbeatTable::default();
        assert!(table.record(node(1), heartbeat(1, 1), || true));
        assert!(!table.record(node(1), heartbeat(1, 2), || true));
        table.record_own(node(9), heartbeat(1, 1));
        table.record_own(node(9), heartbeat(1, 2));
        let own = table
            .held()
            .into_iter()
            .find(|held| held.node_id == node(9));
        assert_eq!(own.map(|held| held.heartbeat.sequence), Some(2));
    }

    #[test]
    fn slots_bound_streams() {
        let table = Arc::new(HeartbeatTable::default());
        let first = table.slot(node(1)).expect("first slot");
        let second = table.slot(node(1)).expect("second slot");
        assert!(table.slot(node(1)).is_none());
        drop(first);
        assert!(table.slot(node(1)).is_some());
        drop(second);
    }

    #[test]
    fn silence_from_start() {
        let table = HeartbeatTable::default();
        let before = table.silence(&node(1));
        table.record(node(1), heartbeat(1, 1), || true);
        assert!(table.silence(&node(1)) <= table.silence(&node(2)));
        assert!(table.silence(&node(2)) >= before);
    }

    #[test]
    fn retain_prunes_departed() {
        let table = HeartbeatTable::default();
        table.record(node(1), heartbeat(1, 1), || true);
        table.record(node(2), heartbeat(1, 1), || true);
        table.retain(|node_id| *node_id == node(2));
        let held = table.held();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].node_id, node(2));
    }
}
