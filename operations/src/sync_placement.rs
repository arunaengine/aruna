use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingDocumentPlacement};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_secs;
use byteview::ByteView;

use crate::placement::{PlacementResolutionContext, rank_eligible_holders};

pub const DOCUMENT_SYNC_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const SYNC_PLACEMENT_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Monotonic top-up: keeps every `existing_holders` entry and appends
/// resolver-ranked eligible nodes (best-first, skipping already-held ones)
/// until `desired_holder_count` is reached. The stored set is node-id sorted so
/// any two nodes materialise an identical holder set from an identical record.
pub fn complete_authoritative_holders(
    config: &RealmConfigDocument,
    target: &DocumentSyncTarget,
    context: PlacementResolutionContext<'_>,
    existing_holders: &[NodeId],
    desired_holder_count: usize,
) -> Vec<NodeId> {
    let mut holders = existing_holders.to_vec();
    sort_node_ids(&mut holders);
    if holders.len() >= desired_holder_count {
        return holders;
    }

    let held: HashSet<NodeId> = holders.iter().copied().collect();
    for candidate in rank_eligible_holders(config, target, context) {
        if holders.len() >= desired_holder_count {
            break;
        }
        if !held.contains(&candidate) {
            holders.push(candidate);
        }
    }
    sort_node_ids(&mut holders);
    holders
}

pub fn placement_prefix(realm_id: RealmId) -> Key {
    ByteView::from(realm_id.as_bytes().to_vec())
}

pub fn placement_key(realm_id: RealmId, target: &DocumentSyncTarget) -> Key {
    let mut bytes = realm_id.as_bytes().to_vec();
    bytes.extend_from_slice(target.sync_topic_id().to_string().as_bytes());
    ByteView::from(bytes)
}

pub fn new_placement(
    realm_id: RealmId,
    target: DocumentSyncTarget,
    origin_node_id: NodeId,
    desired_holder_count: usize,
    mut selected_holders: Vec<NodeId>,
    placement: PlacementRef,
) -> PendingDocumentPlacement {
    sort_node_ids(&mut selected_holders);
    PendingDocumentPlacement {
        realm_id,
        target,
        desired_holder_count,
        selected_holders,
        updated_at: unix_timestamp_secs(),
        origin_node_id,
        placement,
    }
}

pub fn missing_holder_count(record: &PendingDocumentPlacement) -> usize {
    let mut selected_holders = record.selected_holders.clone();
    sort_node_ids(&mut selected_holders);
    record
        .desired_holder_count
        .saturating_sub(selected_holders.len())
}

pub fn placement_satisfied(selected_holder_count: usize, desired_holder_count: usize) -> bool {
    selected_holder_count >= desired_holder_count
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

pub fn write_placement_effect(
    record: &PendingDocumentPlacement,
) -> Result<Effect, postcard::Error> {
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
    schedule_placement_retry_after(realm_id, local_node_id, SYNC_PLACEMENT_RETRY_AFTER)
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

pub fn decode_placement(value: &[u8]) -> Result<PendingDocumentPlacement, postcard::Error> {
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
    use aruna_core::structs::{RealmConfigDocument, RealmId, RealmNodeKind};
    use ulid::Ulid;

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

    fn config_with(nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = aruna_core::structs::PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "test".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    #[test]
    fn authoritative_holder_completion_is_monotonic() {
        let existing = vec![node(1), node(3), node(3)];
        let config = config_with(&[node(1), node(2), node(3), node(4), node(5)]);

        let holders = complete_authoritative_holders(
            &config,
            &target(),
            PlacementResolutionContext::default(),
            &existing,
            4,
        );

        assert!(holders.contains(&node(1)));
        assert!(holders.contains(&node(3)));
        assert_eq!(holders.len(), 4);
    }

    #[test]
    fn authoritative_holder_completion_is_cross_node_identical() {
        let config = config_with(&[node(1), node(2), node(3), node(4), node(5)]);
        let existing = vec![node(2)];

        let first = complete_authoritative_holders(
            &config,
            &target(),
            PlacementResolutionContext::default(),
            &existing,
            3,
        );
        // A node observing the members in a different order derives the same set.
        let reversed = config_with(&[node(5), node(4), node(3), node(2), node(1)]);
        let second = complete_authoritative_holders(
            &reversed,
            &target(),
            PlacementResolutionContext::default(),
            &existing,
            3,
        );

        assert_eq!(first, second);
        assert_eq!(first.len(), 3);
        assert!(first.contains(&node(2)));
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
    fn placement_deduplicates_holders_and_computes_missing_count() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let origin = node(1);
        let holder = node(5);
        let placement = new_placement(
            realm_id,
            target(),
            origin,
            3,
            vec![holder, holder],
            PlacementRef::NIL,
        );

        assert_eq!(placement.realm_id, realm_id);
        assert_eq!(placement.origin_node_id, origin);
        assert_eq!(placement.selected_holders, vec![holder]);
        assert_eq!(missing_holder_count(&placement), 2);
    }

    #[test]
    fn placement_does_not_count_origin_implicitly() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let placement = new_placement(
            realm_id,
            target(),
            node(1),
            3,
            vec![node(5), node(6)],
            PlacementRef::NIL,
        );

        assert_eq!(missing_holder_count(&placement), 1);
        assert!(!placement_satisfied(
            placement.selected_holders.len(),
            placement.desired_holder_count
        ));
    }

    #[test]
    fn placement_records_origin_separately() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let origin = node(7);

        let placement = new_placement(
            realm_id,
            target(),
            origin,
            3,
            vec![node(8)],
            PlacementRef::NIL,
        );

        assert_eq!(placement.origin_node_id, origin);
        assert!(!placement.selected_holders.contains(&origin));
    }

    #[test]
    fn placement_counts_origin_only_when_selected_as_holder() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let origin = node(7);

        let placement = new_placement(
            realm_id,
            target(),
            origin,
            3,
            vec![node(8), origin, node(8), node(9)],
            PlacementRef::NIL,
        );

        assert_eq!(placement.selected_holders, vec![origin, node(9), node(8)]);
        assert_eq!(missing_holder_count(&placement), 0);
        assert!(placement_satisfied(
            placement.selected_holders.len(),
            placement.desired_holder_count
        ));
    }
}
