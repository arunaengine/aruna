use std::cmp::Ordering;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::PendingShardPlacement;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_secs;
use byteview::ByteView;

pub const DOCUMENT_SYNC_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const SYNC_PLACEMENT_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Retry interval for outbox records deferred on a missing shard-topic
/// genesis. Short: the genesis is usually one gossip push away (the rank-0
/// holder creates it eagerly on config apply).
pub const DOCUMENT_SYNC_DEFER_RETRY_AFTER: Duration = Duration::from_secs(1);

pub fn placement_prefix(realm_id: RealmId) -> Key {
    ByteView::from(realm_id.as_bytes().to_vec())
}

/// Shard-scoped record key: `realm(32) ‖ strategy(16) ‖ epoch(8, le) ‖
/// shard(4, be)`. One record per shard the local node authoritatively holds.
pub fn placement_key(realm_id: RealmId, placement: &PlacementRef) -> Key {
    let mut bytes = realm_id.as_bytes().to_vec();
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.epoch.to_le_bytes());
    bytes.extend_from_slice(&placement.shard.to_be_bytes());
    ByteView::from(bytes)
}

pub fn new_placement(
    realm_id: RealmId,
    placement: PlacementRef,
    authoritative_node_id: NodeId,
    mut selected_peers: Vec<NodeId>,
) -> PendingShardPlacement {
    selected_peers.retain(|node_id| *node_id != authoritative_node_id);
    sort_node_ids(&mut selected_peers);
    PendingShardPlacement {
        realm_id,
        placement,
        selected_peers,
        updated_at: unix_timestamp_secs(),
        authoritative_node_id,
    }
}

/// Co-holders still to add to a shard to reach `desired_peer_count` total
/// holders (the authoritative node counts as one).
pub fn missing_peer_count(record: &PendingShardPlacement, desired_peer_count: usize) -> usize {
    let mut selected_peers = record.selected_peers.clone();
    selected_peers.retain(|node_id| *node_id != record.authoritative_node_id);
    sort_node_ids(&mut selected_peers);
    desired_peer_count.saturating_sub(selected_peers.len().saturating_add(1))
}

pub fn placement_satisfied(selected_peer_count: usize, desired_peer_count: usize) -> bool {
    selected_peer_count.saturating_add(1) >= desired_peer_count
}

pub fn write_placement_effect(record: &PendingShardPlacement) -> Result<Effect, postcard::Error> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
        key: placement_key(record.realm_id, &record.placement),
        value: ByteView::from(postcard::to_allocvec(record)?),
        txn_id: None,
    }))
}

pub fn delete_placement_effect(realm_id: RealmId, placement: &PlacementRef) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
        key: placement_key(realm_id, placement),
        txn_id: None,
    })
}

pub fn schedule_placement_retry_effect(realm_id: RealmId, local_node_id: NodeId) -> Effect {
    schedule_placement_retry_after(realm_id, local_node_id, SYNC_PLACEMENT_RETRY_AFTER)
}

pub fn schedule_placement_revalidation_effect(realm_id: RealmId, local_node_id: NodeId) -> Effect {
    schedule_placement_retry_after(realm_id, local_node_id, Duration::ZERO)
}

pub fn schedule_placement_retry_after(
    realm_id: RealmId,
    local_node_id: NodeId,
    after: Duration,
) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::SyncPlacements {
            realm_id,
            node_id: local_node_id,
        },
        after,
    })
}

pub fn decode_placement(value: &[u8]) -> Result<PendingShardPlacement, postcard::Error> {
    postcard::from_bytes(value)
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
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            epoch: 0,
            shard,
        }
    }

    #[test]
    fn placement_key_is_realm_and_shard_scoped() {
        let first_realm = RealmId::from_bytes([1u8; 32]);
        let second_realm = RealmId::from_bytes([2u8; 32]);

        let first_key = placement_key(first_realm, &placement(3));
        let second_key = placement_key(second_realm, &placement(3));
        let other_shard = placement_key(first_realm, &placement(4));

        assert_ne!(first_key, second_key);
        assert_ne!(first_key, other_shard);
        assert!(first_key.as_ref().starts_with(first_realm.as_bytes()));
        // Layout: realm(32) ‖ strategy(16) ‖ epoch(8) ‖ shard(4) = 60 bytes.
        assert_eq!(first_key.as_ref().len(), 60);
        assert_eq!(
            placement_prefix(first_realm).as_ref(),
            first_realm.as_bytes()
        );
    }

    #[test]
    fn placement_deduplicates_peers_and_computes_missing_count() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let authoritative = node(1);
        let peer = node(5);
        let record = new_placement(realm_id, placement(2), authoritative, vec![peer, peer]);

        assert_eq!(record.realm_id, realm_id);
        assert_eq!(record.authoritative_node_id, authoritative);
        assert_eq!(record.selected_peers, vec![peer]);
        assert_eq!(missing_peer_count(&record, 3), 1);
    }

    #[test]
    fn placement_counts_authoritative_node_toward_desired_peer_count() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let record = new_placement(realm_id, placement(2), node(1), vec![node(5), node(6)]);

        assert_eq!(missing_peer_count(&record, 3), 0);
        assert!(placement_satisfied(record.selected_peers.len(), 3));
    }

    #[test]
    fn placement_records_authoritative_holder_explicitly() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let authoritative = node(7);

        let record = new_placement(realm_id, placement(2), authoritative, vec![node(8)]);

        assert_eq!(record.authoritative_node_id, authoritative);
    }

    #[test]
    fn placement_deduplicates_selected_peers_and_excludes_authoritative() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let authoritative = node(7);

        let record = new_placement(
            realm_id,
            placement(2),
            authoritative,
            vec![node(8), authoritative, node(8), node(9)],
        );

        assert_eq!(record.selected_peers, vec![node(9), node(8)]);
        assert_eq!(missing_peer_count(&record, 3), 0);
    }
}
