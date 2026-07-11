use std::cmp::Ordering;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingDocumentPlacement};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::storage_entries::{
    document_placement_delete_entry, document_placement_key, document_placement_write_entry,
};
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key, TxnId};
use aruna_core::util::unix_timestamp_secs;
use byteview::ByteView;

pub const DOCUMENT_SYNC_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const SYNC_PLACEMENT_RETRY_AFTER: Duration = Duration::from_secs(30);

pub fn placement_prefix(realm_id: RealmId) -> Key {
    ByteView::from(realm_id.as_bytes().to_vec())
}

pub fn placement_key(realm_id: RealmId, target: &DocumentSyncTarget) -> Key {
    document_placement_key(realm_id, target)
}

pub fn new_placement(
    realm_id: RealmId,
    target: DocumentSyncTarget,
    origin_node_id: NodeId,
    desired_holder_count: usize,
    selected_holders: Vec<NodeId>,
    placement: PlacementRef,
) -> PendingDocumentPlacement {
    new_placement_with_context(
        realm_id,
        target,
        origin_node_id,
        None,
        None,
        desired_holder_count,
        selected_holders,
        placement,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn new_placement_with_context(
    realm_id: RealmId,
    target: DocumentSyncTarget,
    origin_node_id: NodeId,
    group_id: Option<GroupId>,
    metadata_path: Option<String>,
    desired_holder_count: usize,
    mut selected_holders: Vec<NodeId>,
    placement: PlacementRef,
) -> PendingDocumentPlacement {
    sort_node_ids(&mut selected_holders);
    PendingDocumentPlacement {
        realm_id,
        target,
        group_id,
        metadata_path,
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
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = document_placement_write_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id: None,
    }))
}

pub fn delete_placement_effect(realm_id: RealmId, target: &DocumentSyncTarget) -> Effect {
    delete_placement_effect_with_txn(realm_id, target, None)
}

pub fn delete_placement_effect_with_txn(
    realm_id: RealmId,
    target: &DocumentSyncTarget,
    txn_id: Option<TxnId>,
) -> Effect {
    let (key_space, key) = document_placement_delete_entry(realm_id, target);
    Effect::Storage(StorageEffect::Delete {
        key_space,
        key,
        txn_id,
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

    #[test]
    fn metadata_resolution_context_survives_placement_roundtrip() {
        let group_id = GroupId::from_bytes([8; 16]);
        let placement = new_placement_with_context(
            RealmId::from_bytes([7; 32]),
            DocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: GroupId::from_bytes([9; 16]),
            },
            node(1),
            Some(group_id),
            Some("datasets/context/object".to_string()),
            2,
            vec![node(1), node(2)],
            PlacementRef::NIL,
        );

        let bytes = postcard::to_allocvec(&placement).expect("placement serializes");
        let decoded = decode_placement(&bytes).expect("placement decodes");

        assert_eq!(decoded.group_id, Some(group_id));
        assert_eq!(
            decoded.metadata_path.as_deref(),
            Some("datasets/context/object")
        );
        assert_eq!(decoded, placement);
    }
}
