// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::structs::storage::distribution::presets::replica_layout;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, RouteError, SetRoot, WriteRoute,
    next_snapshot,
};

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn member_id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn step(head: &DistributionSnapshot, request: &DistributionRequest) -> DistributionSnapshot {
    next_snapshot(realm(), Some(head), request).unwrap()
}

fn signed(
    head: &DistributionSnapshot,
    seed: u8,
    subject: &[u8],
    kind: StatementKind,
) -> DistributionRequest {
    let member = head.member(member_id(seed)).unwrap();
    let statement = Statement::lifecycle(realm(), head.set_id(), member, subject, kind);
    DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))))
}

/// Member 1 coordinates the one range, members 2 and 3 keep copies; all admitted and active.
fn active() -> DistributionSnapshot {
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes: Vec<_> = (1..=3)
        .map(|seed| (member_id(seed), secret(seed).public()))
        .collect();
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
    let mut head = next_snapshot(realm(), None, &create).unwrap();
    for seed in 1..=3 {
        let intent = head.member(member_id(seed)).unwrap().intent;
        let statement = Statement::admit(realm(), head.set_id(), member_id(seed), 7, intent);
        head = step(
            &head,
            &DistributionRequest::Statement(Box::new(statement.sign(&secret(seed)))),
        );
    }
    step(&head, &DistributionRequest::Activate)
}

fn prepare(head: &DistributionSnapshot, to: u8, role: TransferRole) -> DistributionRequest {
    DistributionRequest::TransferPrepare {
        transfer_id: Ulid::from_bytes([50; 16]),
        range_id: head.config.ranges[0].range_id,
        to: member_id(to),
        epoch: 0,
        role,
    }
}

fn fenced(head: &DistributionSnapshot, seed: u8) -> DistributionRequest {
    let open = &head.lifecycle.transfers[0];
    let kind = StatementKind::TransferFenced {
        transfer_id: open.transfer_id,
        range_id: open.range_id,
        epoch: head.config.ranges[0].epoch,
        next_epoch: open.epoch,
        watermark: vec![1, 2],
        import_cursor: None,
        refresh_cursor: None,
        open_uploads: 0,
    };
    signed(head, seed, &open.transfer_id.to_bytes(), kind)
}

fn complete(head: &DistributionSnapshot, seed: u8, watermark: Vec<u8>) -> DistributionRequest {
    let open = &head.lifecycle.transfers[0];
    let kind = StatementKind::TransferComplete {
        transfer_id: open.transfer_id,
        range_id: open.range_id,
        fenced: open.fenced.unwrap(),
        watermark,
    };
    signed(head, seed, &open.transfer_id.to_bytes(), kind)
}

#[test]
fn transfer_moves_coordinator() {
    // Prepare takes a fresh epoch, the fence stops writes, the completion activates the move.
    let head = active();
    let range_id = head.config.ranges[0].range_id;
    let prepared = step(&head, &prepare(&head, 2, TransferRole::Replica));
    assert_eq!(prepared.lifecycle.transfers[0].epoch, 2);
    assert!(prepared.lifecycle_open());
    let fence = step(&prepared, &fenced(&prepared, 1));
    assert_eq!(
        fence.write_route(member_id(1), "a"),
        Err(RouteError::Moving(range_id))
    );
    let done = step(&fence, &complete(&fence, 2, vec![1, 2]));
    let range = &done.config.ranges[0];
    assert_eq!((range.coordinator, range.epoch), (member_id(2), 2));
    assert!(range.replicas.contains(&member_id(1)));
    assert!(!range.replicas.contains(&member_id(2)));
    assert!(done.lifecycle.transfers.is_empty());
    assert!(
        done.statements
            .iter()
            .all(|s| s.statement.kind.name() == "admit")
    );
    assert_eq!(
        done.write_route(member_id(2), "a"),
        Ok(WriteRoute::Local {
            range_id,
            epoch: range.original_epoch(),
        })
    );
}

#[test]
fn transfer_needs_ready_copy_replica() {
    let head = active();
    let to_self = next_snapshot(
        realm(),
        Some(&head),
        &prepare(&head, 1, TransferRole::Leave),
    );
    assert!(matches!(to_self, Err(DistributionError::InvalidChange(_))));
    let prepared = step(&head, &prepare(&head, 2, TransferRole::Leave));
    let mut again = prepare(&prepared, 3, TransferRole::Leave);
    if let DistributionRequest::TransferPrepare { transfer_id, .. } = &mut again {
        *transfer_id = Ulid::from_bytes([51; 16]);
    }
    assert_eq!(
        next_snapshot(realm(), Some(&prepared), &again),
        Err(DistributionError::TransferOpen(
            head.config.ranges[0].range_id
        ))
    );
}

#[test]
fn transfer_targets_are_ready_copies() {
    // Only ready copy replicas other than the coordinator may take the range.
    let mut head = active();
    let ids = |head: &DistributionSnapshot| -> Vec<Ulid> {
        let range = &head.config.ranges[0];
        (head.transfer_targets(range).iter())
            .map(|m| m.member_id)
            .collect()
    };
    assert_eq!(ids(&head), vec![member_id(2), member_id(3)]);
    head.config.members[2].state = MemberState::CatchingUp;
    head.config.members[1].strategy = DataStrategy::CacheOnUse;
    assert!(ids(&head).is_empty());
    let refused = next_snapshot(
        realm(),
        Some(&head),
        &prepare(&head, 3, TransferRole::Leave),
    );
    assert!(matches!(refused, Err(DistributionError::InvalidChange(_))));
}

#[test]
fn range_ready_needs_every_replica() {
    // A former coordinator reclaims only while the range keeps all its listed copies.
    let mut head = active();
    let range = head.config.ranges[0].clone();
    assert!(head.range_ready(&range));
    head.config.members[2].state = MemberState::CatchingUp;
    assert!(!head.range_ready(&range));
    head.config.members[2].state = MemberState::Ready;
    head.config.members[0].state = MemberState::Leaving;
    assert!(
        !head.range_ready(&range),
        "the coordinator must be ready too"
    );
}

#[test]
fn transfer_binds_its_statements() {
    let head = active();
    let prepared = step(&head, &prepare(&head, 2, TransferRole::Leave));
    // Only the former coordinator fences, only the new one completes, at the fenced watermark.
    assert!(next_snapshot(realm(), Some(&prepared), &fenced(&prepared, 3)).is_err());
    let fence = step(&prepared, &fenced(&prepared, 1));
    assert!(next_snapshot(realm(), Some(&fence), &complete(&fence, 3, vec![1, 2])).is_err());
    assert!(next_snapshot(realm(), Some(&fence), &complete(&fence, 2, vec![9])).is_err());
    let done = step(&fence, &complete(&fence, 2, vec![1, 2]));
    // A former coordinator with `all` coverage keeps its copy even with role "leave".
    assert!(done.config.ranges[0].replicas.contains(&member_id(1)));
    let mut assigned = fence.clone();
    assigned.config.members[0].coverage = MetadataCoverage::Assigned;
    let done = step(&assigned, &complete(&assigned, 2, vec![1, 2]));
    // An assigned former coordinator keeps no copy of the range and leaves the set.
    assert!(!done.config.ranges[0].replicas.contains(&member_id(1)));
    assert_eq!(
        done.member(member_id(1)).unwrap().state,
        MemberState::Leaving
    );
}

#[test]
fn deletion_commits_after_fences() {
    let head = active();
    let members: Vec<_> = (1..=3).map(|seed| (member_id(seed), 7)).collect();
    let wrong = DistributionRequest::DeletePrepare {
        members: members[..2].to_vec(),
    };
    assert!(next_snapshot(realm(), Some(&head), &wrong).is_err());
    let prepared = step(&head, &DistributionRequest::DeletePrepare { members });
    assert_eq!(
        prepared.write_route(member_id(1), "a"),
        Err(RouteError::Deleting(prepared.set_id()))
    );
    assert_eq!(
        next_snapshot(realm(), Some(&prepared), &DistributionRequest::DeleteCommit),
        Err(DistributionError::NotFenced(member_id(1)))
    );
    let identity = prepared
        .lifecycle
        .deletion
        .as_ref()
        .unwrap()
        .identity(prepared.set_id());
    let fence = |seed| {
        signed(
            &prepared,
            seed,
            &identity,
            StatementKind::DeleteFenced { deletion: identity },
        )
    };
    // A replica fences its incoming publications on its own; only coordinators state it.
    assert!(next_snapshot(realm(), Some(&prepared), &fence(2)).is_err());
    let committed = step(&prepared, &fence(1));
    let deletion = committed.lifecycle.deletion.as_ref().unwrap();
    assert!(deletion.committed);
    assert!((1..=3).all(|seed| committed.tombstoned(member_id(seed), 7)));
    assert!(
        next_snapshot(
            realm(),
            Some(&committed),
            &DistributionRequest::DeleteCommit
        )
        .is_err()
    );
    assert!(
        committed
            .range_deliveries(member_id(1), &committed.config.ranges[0])
            .is_empty()
    );
}
