//! Tests range splits of a set's record: who may split, refusals, ancestors and open uploads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::structs::storage::distribution::presets::shard_layout;
use crate::structs::storage::distribution::snapshot::OpenTransfer;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, SetRoot, TransferRole,
    next_snapshot,
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

/// Two shards split at "m", member 1 coordinates the lower one, both admitted and active.
fn active() -> DistributionSnapshot {
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

fn split(head: &DistributionSnapshot, signer: u8, range: usize, key: &str) -> DistributionRequest {
    let member = head.member(member(signer)).unwrap();
    let range = &head.config.ranges[range];
    let statement = Statement::split(realm(), head.set_id(), member, head.number, range, key);
    DistributionRequest::Statement(Box::new(statement.sign(&secret(signer))))
}

#[test]
fn coordinator_splits_range() {
    let head = active();
    let parent = head.config.ranges[0].clone();
    let next = next_snapshot(realm(), Some(&head), &split(&head, 1, 0, "f/")).unwrap();
    let ranges = &next.config.ranges;
    assert_eq!(ranges.len(), 3);
    assert_eq!(
        (ranges[0].lower.as_str(), ranges[0].upper.as_deref()),
        ("", Some("f/"))
    );
    assert_eq!(
        (ranges[1].lower.as_str(), ranges[1].upper.as_deref()),
        ("f/", Some("m"))
    );
    for child in &ranges[..2] {
        assert_eq!(child.lineage, Some(parent.range_id));
        assert_eq!(
            (child.coordinator, child.epoch),
            (parent.coordinator, parent.epoch)
        );
    }
    assert_eq!(next.retired_range(parent.range_id).unwrap().split_key, "f/");
    assert_eq!(
        next.resolve(parent.range_id, "g").unwrap().range_id,
        ranges[1].range_id
    );
    assert!(
        next.statements
            .iter()
            .any(|signed| matches!(signed.statement.kind, StatementKind::Split { .. }))
    );
    let settings = DistributionRequest::ChangeSettings {
        settings: DistributionSettings::default(),
    };
    let later = next_snapshot(realm(), Some(&next), &settings).unwrap();
    assert!(
        later
            .statements
            .iter()
            .all(|signed| !matches!(signed.statement.kind, StatementKind::Split { .. })),
        "the split statement stays only in the revision that applies it"
    );
}

#[test]
fn splits_refused() {
    let head = active();
    assert!(next_snapshot(realm(), Some(&head), &split(&head, 2, 0, "f")).is_err());
    assert!(next_snapshot(realm(), Some(&head), &split(&head, 1, 0, "")).is_err());
    assert!(next_snapshot(realm(), Some(&head), &split(&head, 1, 0, "n")).is_err());
    let mut moving = head.clone();
    moving.lifecycle.transfers.push(OpenTransfer {
        transfer_id: Ulid::from_bytes([3; 16]),
        range_id: head.config.ranges[0].range_id,
        from: member(1),
        to: member(2),
        epoch: 2,
        role: TransferRole::Replica,
        fenced: None,
        completed: None,
    });
    assert!(next_snapshot(realm(), Some(&moving), &split(&moving, 1, 0, "f")).is_err());
    let request = split(&head, 1, 0, "f");
    let next = next_snapshot(realm(), Some(&head), &request).unwrap();
    assert!(
        next_snapshot(realm(), Some(&next), &request).is_err(),
        "a replayed split finds its range retired"
    );
}

#[test]
fn ancestors_resolve_deeply() {
    let head = active();
    let root_range = head.config.ranges[0].range_id;
    let once = next_snapshot(realm(), Some(&head), &split(&head, 1, 0, "f")).unwrap();
    let twice = next_snapshot(realm(), Some(&once), &split(&once, 1, 0, "c")).unwrap();
    let leaf = twice.resolve(root_range, "a").unwrap();
    assert_eq!(leaf.upper.as_deref(), Some("c"));
    assert!(twice.descends(leaf, once.config.ranges[0].range_id));
    assert_eq!(
        twice.resolve(twice.config.ranges.last().unwrap().range_id, "a"),
        Err(RouteError::StaleRange(
            twice.config.ranges.last().unwrap().range_id
        ))
    );
    assert!(twice.validate(realm()).is_ok());
}

#[test]
fn uploads_survive_split() {
    use crate::structs::storage::distribution::upload::UploadTag;
    let head = active();
    let range = &head.config.ranges[0];
    let tag = UploadTag {
        upload_id: Ulid::from_bytes([4; 16]),
        set_id: head.set_id(),
        range_id: range.range_id,
        epoch: range.epoch,
    };
    let next = next_snapshot(realm(), Some(&head), &split(&head, 1, 0, "f")).unwrap();
    assert_eq!(next.upload_authority(member(1), "g", &tag), Ok(()));
    assert!(next.upload_authority(member(2), "g", &tag).is_err());
    let moved = UploadTag { epoch: 9, ..tag };
    assert!(next.upload_authority(member(1), "g", &moved).is_err());
}
