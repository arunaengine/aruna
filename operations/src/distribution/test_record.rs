//! Builds a signed record of a two-shard distributed bucket for tests of writers and guards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::presets::shard_layout;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionConfig, DistributionRequest, DistributionSettings,
    DistributionSnapshot, DistributionView, SetRoot, SignedSnapshot, Statement, next_snapshot,
};
use ulid::Ulid;

pub(crate) fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

pub(crate) fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

pub(crate) fn member(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// Two shards split at "m"; member 1 coordinates the lower one. Both admitted with
/// incarnation 5; `active` decides whether the set was activated.
pub(crate) fn record(active: bool) -> (DistributionSnapshot, Vec<u8>) {
    replicated(active, 0)
}

/// The two shards of [`record`], each with `replicas` copies on the other member.
pub(crate) fn replicated(active: bool, replicas: usize) -> (DistributionSnapshot, Vec<u8>) {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (member(1), secret(1).public()),
        (member(2), secret(2).public()),
    ];
    let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], replicas).unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root.owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    let mut head = next_snapshot(realm(), None, &create).unwrap();
    for seed in [1, 2] {
        let intent = head.member(member(seed)).unwrap().intent;
        let statement = Statement::admit(realm(), head.set_id(), member(seed), 5, intent);
        let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))));
        head = next_snapshot(realm(), Some(&head), &request).unwrap();
    }
    if active {
        head = next_snapshot(realm(), Some(&head), &DistributionRequest::Activate).unwrap();
    }
    let view = DistributionView {
        head: SignedSnapshot::sign(head.clone(), realm(), &secret(1)).unwrap(),
        conflict: None,
    };
    (head, view.to_bytes().unwrap())
}

pub(crate) fn binding(
    snapshot: &DistributionSnapshot,
    seed: u8,
    incarnation: u128,
) -> DistributionBinding {
    DistributionBinding {
        set_id: snapshot.set_id(),
        member_id: member(seed),
        incarnation,
        revision: snapshot.revision(),
    }
}

/// Stores record bytes as this node's view of their set.
pub(crate) async fn store_record(
    handle: &aruna_storage::storage::StorageHandle,
    set_id: Ulid,
    bytes: Vec<u8>,
) {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    let write = StorageEffect::Write {
        key_space: aruna_core::keyspaces::DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        value: bytes.into(),
        txn_id: None,
    };
    assert!(matches!(
        handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}
