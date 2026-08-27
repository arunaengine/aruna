//! When each realm peer last reached this node.
//!
//! A device publishes no realm presence, so the only liveness signal a realm
//! node holds about one is that the device itself reached it. This records that
//! observation: node-local, in memory, never replicated and never published.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use aruna_core::NodeId;

/// How recently a peer must have reached this node to count as seen. A device
/// beats every 60 seconds and shuffles which node it asks, so three beats keep
/// one skipped turn from reading as gone.
pub const PEER_CONTACT_WINDOW: Duration = Duration::from_secs(180);

/// Peers remembered before stale entries are swept. A realm configuration holds
/// far fewer, so the sweep only ever answers an unexpected peer set.
const PEER_CONTACT_CAPACITY: usize = 4096;

/// Last contact per authenticated peer, in unix milliseconds.
#[derive(Clone, Default)]
pub struct PeerContacts(Arc<Mutex<HashMap<NodeId, u64>>>);

impl PeerContacts {
    /// Records that an authenticated peer reached this node.
    pub fn note(&self, peer: NodeId, now_ms: u64) {
        let mut seen = self.entries();
        seen.insert(peer, now_ms);
        if seen.len() > PEER_CONTACT_CAPACITY {
            seen.retain(|_, at| within_window(*at, now_ms));
        }
    }

    /// When this node last saw the peer, however long ago that was.
    pub fn last_seen(&self, peer: &NodeId) -> Option<u64> {
        self.entries().get(peer).copied()
    }

    /// Whether the peer reached this node within [`PEER_CONTACT_WINDOW`].
    pub fn seen_recently(&self, peer: &NodeId, now_ms: u64) -> bool {
        self.last_seen(peer)
            .is_some_and(|at| within_window(at, now_ms))
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<NodeId, u64>> {
        self.0.lock().unwrap_or_else(|lock| lock.into_inner())
    }
}

fn within_window(seen_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(seen_ms) <= PEER_CONTACT_WINDOW.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{PEER_CONTACT_WINDOW, PeerContacts};
    use aruna_core::NodeId;

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

        assert_eq!(contacts.last_seen(&device), Some(1_000));
        assert!(contacts.seen_recently(&device, 1_000 + window));
        assert!(!contacts.seen_recently(&device, 1_001 + window));
        assert_eq!(contacts.last_seen(&peer(44)), None);
    }

    #[test]
    fn note_replaces_contact() {
        let contacts = PeerContacts::default();
        let device = peer(45);
        contacts.note(device, 1_000);
        contacts.note(device, 5_000);

        assert_eq!(contacts.last_seen(&device), Some(5_000));
    }
}
