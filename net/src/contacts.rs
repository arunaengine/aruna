//! Remembers when each authenticated realm peer last reached this node.
//! It is kept in memory only, never replicated, and is the only liveness signal for devices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use aruna_core::NodeId;
use aruna_core::heartbeat::seen_recently;

/// Peers remembered before stale entries are swept. A realm configuration holds
/// far fewer, so the sweep only ever answers an unexpected peer set.
const PEER_CONTACT_CAPACITY: usize = 4096;

/// Last contact per authenticated peer, in unix milliseconds.
#[derive(Clone, Debug, Default)]
pub struct PeerContacts(Arc<Mutex<HashMap<NodeId, u64>>>);

impl PeerContacts {
    /// Records that an authenticated peer reached this node.
    pub fn note(&self, peer: NodeId, now_ms: u64) {
        let mut seen = self.entries();
        seen.insert(peer, now_ms);
        if seen.len() > PEER_CONTACT_CAPACITY {
            seen.retain(|_, at| seen_recently(*at, now_ms));
        }
    }

    /// When this node last saw each of `peers` it has seen.
    pub fn last_seen_of(&self, peers: &[NodeId]) -> BTreeMap<NodeId, u64> {
        let seen = self.entries();
        (peers.iter())
            .filter_map(|peer| seen.get(peer).map(|at| (*peer, *at)))
            .collect()
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<NodeId, u64>> {
        self.0.lock().unwrap_or_else(|lock| lock.into_inner())
    }
}

#[cfg(test)]
mod pure_tests {
    use super::PeerContacts;
    use aruna_core::NodeId;
    use aruna_core::heartbeat::{PEER_CONTACT_WINDOW, seen_recently};
    use std::collections::BTreeMap;

    fn peer(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn window_edge_holds() {
        // The last millisecond inside the window still counts as seen.
        let contacts = PeerContacts::default();
        let device = peer(43);
        let window = PEER_CONTACT_WINDOW.as_millis() as u64;
        contacts.note(device, 1_000);

        let seen = contacts.last_seen_of(&[device, peer(44)]);
        assert_eq!(seen, BTreeMap::from([(device, 1_000)]));
        assert!(seen_recently(1_000, 1_000 + window));
        assert!(!seen_recently(1_000, 1_001 + window));
    }

    #[test]
    fn note_replaces_contact() {
        let contacts = PeerContacts::default();
        let device = peer(45);
        contacts.note(device, 1_000);
        contacts.note(device, 5_000);

        assert_eq!(
            contacts.last_seen_of(&[device]),
            BTreeMap::from([(device, 5_000)])
        );
    }
}
