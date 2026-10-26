//! Tests forwarded configuration requests: retry identity, publisher errors and token refusal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::presets::replica_layout;
use aruna_core::structs::storage::distribution::{
    DistributionConfig, DistributionError, DistributionSettings, SetRoot, next_snapshot,
};

use super::*;

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn snapshot() -> DistributionSnapshot {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
    ];
    let layout = replica_layout(root.set_id(), &nodes).unwrap();
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
    next_snapshot(RealmId::from_bytes([9; 32]), None, &create).unwrap()
}

fn bucket(set_id: Ulid, member_id: Ulid, incarnation: u128) -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([90; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::nil(RealmId::from_bytes([9; 32])),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(DistributionBinding {
            set_id,
            member_id,
            incarnation,
            revision: Default::default(),
        }),
    }
}

#[test]
fn retries_bind_identity() {
    let snapshot = snapshot();
    let mut member = snapshot.config.members[1].clone();
    let set_id = snapshot.set_id();
    let own = bucket(set_id, member.member_id, 5);
    assert!(same_member(&own, &snapshot, &member, "spire").is_ok());
    let other_set = bucket(Ulid::from_bytes([3; 16]), member.member_id, 5);
    assert!(same_member(&other_set, &snapshot, &member, "spire").is_err());
    let other_member = bucket(set_id, Ulid::from_bytes([1; 16]), 5);
    assert!(same_member(&other_member, &snapshot, &member, "spire").is_err());
    let mut ordinary = own.clone();
    ordinary.distribution = None;
    assert!(same_member(&ordinary, &snapshot, &member, "spire").is_err());
    let mut foreign_group = own.clone();
    foreign_group.group_id = Ulid::from_bytes([4; 16]);
    assert!(same_member(&foreign_group, &snapshot, &member, "spire").is_err());
    // Once admitted, only the admitted incarnation passes; a recreated bucket does not.
    member.incarnation = Some(5);
    assert!(same_member(&own, &snapshot, &member, "spire").is_ok());
    member.incarnation = Some(6);
    assert!(same_member(&own, &snapshot, &member, "spire").is_err());
}

#[test]
fn publisher_errors_map() {
    assert_eq!(
        PublishRefusal::from(PublishError::Unauthorized),
        PublishRefusal::Forbidden
    );
    let replayed = DistributionError::Replayed(Ulid::nil());
    assert_eq!(
        PublishRefusal::from(PublishError::Distribution(replayed)),
        PublishRefusal::Replayed
    );
    assert!(matches!(
        PublishRefusal::from(PublishError::Storage(StorageError::TransactionConflict)),
        PublishRefusal::Unavailable(_)
    ));
    assert!(matches!(
        PublishRefusal::from(PublishError::NotPublisher),
        PublishRefusal::Rejected(_)
    ));
}

#[test]
fn internal_tokens_refused() {
    let realm = RealmId::from_bytes([9; 32]);
    let internal = AuthToken::internal(AuthContext {
        user_id: UserId::nil(realm),
        realm_id: realm,
        path_restrictions: None,
        session: None,
    });
    assert!(!bearer_only(&internal));
    assert!(bearer_only(&AuthToken::bearer("token").unwrap()));
}
