//! Builds the active two-member distribution snapshot the distribution tests start from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::structs::identity::realm::RealmId;
use crate::structs::storage::distribution::presets::shard_layout;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, DistributionSnapshot, SetRoot,
    Statement, next_snapshot,
};

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn member(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// A bucket split at `m` over members 1 and 2, both admitted, then activated.
pub(crate) fn active() -> DistributionSnapshot {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (member(1), secret(1).public()),
        (member(2), secret(2).public()),
    ];
    let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 0).unwrap();
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
    next_snapshot(realm(), Some(&head), &DistributionRequest::Activate).unwrap()
}
