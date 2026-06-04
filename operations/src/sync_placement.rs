use std::cmp::Ordering;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingTopicPlacement};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::RealmId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_secs;
use byteview::ByteView;

const SELECTOR_DOMAIN: &[u8] = b"aruna-sync-peer-v1";
pub const DEFAULT_DOCUMENT_PEER_COUNT: usize = 3;
pub const DOCUMENT_SYNC_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const SYNC_PLACEMENT_RETRY_AFTER: Duration = Duration::from_secs(30);

pub fn desired_peer_count(target: &DocumentSyncTarget) -> usize {
    match target {
        DocumentSyncTarget::MetadataRegistry { .. } => 0,
        _ => DEFAULT_DOCUMENT_PEER_COUNT,
    }
}

pub fn desired_remote_peer_count(desired_peer_count: usize) -> usize {
    desired_peer_count.saturating_sub(1)
}

pub fn select_sync_peers(
    target: &DocumentSyncTarget,
    local_node_id: NodeId,
    candidates: &[NodeId],
    excluded: &[NodeId],
    desired_count: usize,
) -> Vec<NodeId> {
    if desired_count == 0 {
        return Vec::new();
    }

    let topic_id = target.irokle_topic_id().to_string();
    let mut candidates = candidates
        .iter()
        .copied()
        .filter(|node_id| *node_id != local_node_id)
        .filter(|node_id| !excluded.contains(node_id))
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    candidates.dedup();
    candidates.sort_unstable_by(|left, right| {
        let left_score = selector_score(topic_id.as_bytes(), local_node_id, *left);
        let right_score = selector_score(topic_id.as_bytes(), local_node_id, *right);
        left_score
            .cmp(&right_score)
            .then_with(|| left.as_bytes().cmp(right.as_bytes()))
    });
    candidates.truncate(desired_count);
    candidates
}

pub fn placement_prefix(realm_id: RealmId) -> Key {
    ByteView::from(realm_id.as_bytes().to_vec())
}

pub fn placement_key(realm_id: RealmId, target: &DocumentSyncTarget) -> Key {
    let mut bytes = realm_id.as_bytes().to_vec();
    bytes.extend_from_slice(target.irokle_topic_id().to_string().as_bytes());
    ByteView::from(bytes)
}

pub fn new_placement(
    realm_id: RealmId,
    target: DocumentSyncTarget,
    desired_peer_count: usize,
    mut selected_peers: Vec<NodeId>,
) -> PendingTopicPlacement {
    selected_peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    selected_peers.dedup();
    PendingTopicPlacement {
        realm_id,
        target,
        desired_peer_count,
        selected_peers,
        updated_at: unix_timestamp_secs(),
    }
}

pub fn missing_peer_count(record: &PendingTopicPlacement) -> usize {
    record
        .desired_peer_count
        .saturating_sub(record.selected_peers.len().saturating_add(1))
}

pub fn placement_satisfied(selected_peer_count: usize, desired_peer_count: usize) -> bool {
    selected_peer_count.saturating_add(1) >= desired_peer_count
}

pub fn schedule_document_sync_effect(
    node_id: NodeId,
    target: DocumentSyncTarget,
    mut peers: Vec<NodeId>,
) -> Effect {
    sort_node_ids(&mut peers);
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::SyncDocument {
            node_id,
            target,
            peers,
        },
        after: Duration::ZERO,
    })
}

pub fn write_placement_effect(record: &PendingTopicPlacement) -> Result<Effect, postcard::Error> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
        key: placement_key(record.realm_id, &record.target),
        value: ByteView::from(postcard::to_allocvec(record)?),
        txn_id: None,
    }))
}

pub fn delete_placement_effect(realm_id: RealmId, target: &DocumentSyncTarget) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
        key: placement_key(realm_id, target),
        txn_id: None,
    })
}

pub fn schedule_placement_retry_effect(realm_id: RealmId, local_node_id: NodeId) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::SyncPlacements {
            realm_id,
            node_id: local_node_id,
        },
        after: SYNC_PLACEMENT_RETRY_AFTER,
    })
}

pub fn decode_placement(value: &[u8]) -> Result<PendingTopicPlacement, postcard::Error> {
    postcard::from_bytes(value)
}

fn selector_score(topic_id: &[u8], local_node_id: NodeId, candidate_node_id: NodeId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SELECTOR_DOMAIN);
    hasher.update(topic_id);
    hasher.update(local_node_id.as_bytes());
    hasher.update(candidate_node_id.as_bytes());
    *hasher.finalize().as_bytes()
}

pub fn sort_node_ids(nodes: &mut Vec<NodeId>) {
    nodes.sort_unstable_by(compare_node_ids);
    nodes.dedup();
}

fn compare_node_ids(left: &NodeId, right: &NodeId) -> Ordering {
    left.as_bytes().cmp(right.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn target() -> DocumentSyncTarget {
        DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
        }
    }

    #[test]
    fn selector_is_deterministic() {
        let candidates = vec![node(4), node(2), node(3), node(1)];
        let first = select_sync_peers(&target(), node(9), &candidates, &[], 3);
        let second = select_sync_peers(&target(), node(9), &candidates, &[], 3);

        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
    }

    #[test]
    fn selector_excludes_local_and_explicit_nodes() {
        let local = node(1);
        let excluded = node(3);
        let selected = select_sync_peers(
            &target(),
            local,
            &[local, node(2), excluded, node(4)],
            &[excluded],
            3,
        );

        assert!(!selected.contains(&local));
        assert!(!selected.contains(&excluded));
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn selector_returns_available_candidates_when_under_capacity() {
        let selected = select_sync_peers(&target(), node(1), &[node(2)], &[], 3);

        assert_eq!(selected, vec![node(2)]);
    }

    #[test]
    fn placement_key_is_realm_scoped() {
        let target = target();
        let first_realm = RealmId::from_bytes([1u8; 32]);
        let second_realm = RealmId::from_bytes([2u8; 32]);

        let first_key = placement_key(first_realm, &target);
        let second_key = placement_key(second_realm, &target);

        assert_ne!(first_key, second_key);
        assert!(first_key.as_ref().starts_with(first_realm.as_bytes()));
        assert_eq!(
            placement_prefix(first_realm).as_ref(),
            first_realm.as_bytes()
        );
    }

    #[test]
    fn placement_deduplicates_peers_and_computes_missing_count() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let peer = node(5);
        let placement = new_placement(realm_id, target(), 3, vec![peer, peer]);

        assert_eq!(placement.realm_id, realm_id);
        assert_eq!(placement.selected_peers, vec![peer]);
        assert_eq!(missing_peer_count(&placement), 1);
    }

    #[test]
    fn placement_counts_local_node_toward_desired_peer_count() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let placement = new_placement(realm_id, target(), 3, vec![node(5), node(6)]);

        assert_eq!(desired_remote_peer_count(DEFAULT_DOCUMENT_PEER_COUNT), 2);
        assert_eq!(missing_peer_count(&placement), 0);
        assert!(placement_satisfied(
            placement.selected_peers.len(),
            placement.desired_peer_count
        ));
    }
}
