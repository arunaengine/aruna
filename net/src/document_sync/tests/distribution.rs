//! Tests that receiving nodes admit distribution snapshots only from the set's publisher, with
//! valid embedded statements, and keep the highest signed view without role rechecks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::structs::storage::distribution::presets::replica_layout;
use aruna_core::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, DistributionSnapshot,
    DistributionView, MemberState, PublisherCutoff, SetRoot, SignedSnapshot, Statement,
    StatementKind, next_snapshot,
};

const PUBLISHER: u8 = 41;
const MEMBER: u8 = 42;
const OTHER: u8 = 43;

fn realm() -> RealmId {
    RealmId::from_bytes([84; 32])
}

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn actor(seed: u8) -> Actor {
    test_actor(
        seed,
        UserId::local(Ulid::from_parts(841, u128::from(seed)), realm()),
        realm(),
    )
}

fn root() -> SetRoot {
    SetRoot {
        publisher: node(PUBLISHER),
        owner_group: Ulid::from_parts(842, 1),
        nonce: [5; 16],
    }
}

fn created() -> DistributionSnapshot {
    let nodes = [
        (Ulid::from_parts(843, 1), node(PUBLISHER)),
        (Ulid::from_parts(843, 2), node(MEMBER)),
    ];
    let layout = replica_layout(root().set_id(), &nodes).unwrap();
    let config = DistributionConfig {
        bucket: "spire".to_string(),
        owner_group: root().owner_group,
        node_set: None,
        members: layout.members,
        ranges: layout.ranges,
        settings: DistributionSettings::default(),
    };
    let create = DistributionRequest::Create {
        root: root(),
        config,
    };
    next_snapshot(realm(), None, &create).unwrap()
}

fn admitted(head: &DistributionSnapshot, signer: u8) -> DistributionSnapshot {
    let statement = Statement {
        realm_id: realm(),
        set_id: root().set_id(),
        statement_id: Ulid::from_parts(844, 1),
        member_id: Ulid::from_parts(843, 2),
        incarnation: 6,
        intent: head.config.members[1].intent,
        kind: StatementKind::Admit,
    }
    .sign(&secret(signer));
    let mut next = head.clone();
    next.number += 1;
    next.previous = Some(head.digest());
    next.config.members[1].incarnation = Some(6);
    next.config.members[1].state = MemberState::Ready;
    next.statements.push(statement);
    next
}

fn published(origin: u8, snapshot: DistributionSnapshot, signer: u8) -> AdminDocumentEvent {
    let signed = SignedSnapshot::sign(snapshot, realm(), &secret(signer)).unwrap();
    let target = AdminDocumentTarget::Distribution {
        realm_id: realm(),
        set_id: signed.snapshot.set_id(),
    };
    let op = AdminDocumentOperation::DistributionPublished {
        snapshot: Box::new(signed),
    };
    test_admin_event(Ulid::generate(), target, &actor(origin), 1, op)
}

async fn receive(storage: &StorageHandle, event: AdminDocumentEvent) -> AdminEventValidation {
    let target = DocumentTarget::RealmConfig { realm_id: realm() };
    let placement = admin_test_placement();
    let topic_id = target.sync_topic_id(realm(), &placement);
    let outcome = validate_admin_event(
        storage,
        topic_id,
        ::irokle::actor_id_for(topic_id, node_to_peer(&event.origin_node_id)),
        &target,
        &event,
        realm(),
        &placement,
        &sign_as_origin(&event, &placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs");
    if outcome == AdminEventValidation::Accepted {
        apply_admin_operation(storage, target, event)
            .await
            .expect("accepted event applies");
    }
    outcome
}

async fn stored(storage: &StorageHandle) -> Option<DistributionView> {
    storage_read_from(
        storage,
        DISTRIBUTION_KEYSPACE.to_string(),
        root().set_id().to_bytes().to_vec().into(),
    )
    .await
    .unwrap()
    .map(|value| DistributionView::from_bytes(&value).unwrap())
}

/// A realm with three server nodes; nobody holds a role, since none is rechecked.
async fn realm_nodes(storage: &StorageHandle) {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    for seed in [PUBLISHER, MEMBER, OTHER] {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    batch_write_to(
        storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id: realm() },
            config.to_bytes(&actor(PUBLISHER)).unwrap().into(),
        )],
    )
    .await
    .unwrap();
}

fn rejected(outcome: AdminEventValidation) -> bool {
    matches!(outcome, AdminEventValidation::Rejected(_))
}

#[tokio::test]
async fn admits_publisher_snapshots() {
    let (_dir, storage) = test_storage();
    realm_nodes(&storage).await;
    let first = created();
    assert!(rejected(
        receive(&storage, published(OTHER, first.clone(), OTHER)).await
    ));
    assert!(rejected(
        receive(&storage, published(OTHER, first.clone(), PUBLISHER)).await
    ));
    assert!(rejected(
        receive(&storage, published(PUBLISHER, first.clone(), OTHER)).await
    ));
    assert_eq!(stored(&storage).await, None);

    let accepted = receive(&storage, published(PUBLISHER, first.clone(), PUBLISHER)).await;
    assert_eq!(accepted, AdminEventValidation::Accepted);
    let forged = admitted(&first, OTHER);
    assert!(rejected(
        receive(&storage, published(PUBLISHER, forged, PUBLISHER)).await
    ));
    let second = admitted(&first, MEMBER);
    let accepted = receive(&storage, published(PUBLISHER, second.clone(), PUBLISHER)).await;
    assert_eq!(accepted, AdminEventValidation::Accepted);
    // An older snapshot is no gap and no error: it is simply not installed.
    let older = receive(&storage, published(PUBLISHER, first, PUBLISHER)).await;
    assert_eq!(older, AdminEventValidation::Accepted);
    let view = stored(&storage).await.expect("signed view stored");
    assert_eq!(view.head.snapshot, second);
    assert!(view.head.publisher_signed(realm()));
}

#[tokio::test]
async fn missing_config_defers() {
    let (_dir, storage) = test_storage();
    let outcome = receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await;
    assert!(matches!(outcome, AdminEventValidation::Deferred { .. }));
}

#[tokio::test]
async fn gap_recovers_by_snapshot() {
    // A node that missed revisions installs the latest snapshot without replaying them.
    let (_dir, storage) = test_storage();
    realm_nodes(&storage).await;
    let first = created();
    let second = admitted(&first, MEMBER);
    let settings = DistributionRequest::ChangeSettings {
        settings: DistributionSettings {
            public: true,
            ..DistributionSettings::default()
        },
    };
    let third = next_snapshot(realm(), Some(&second), &settings).unwrap();
    let outcome = receive(&storage, published(PUBLISHER, third.clone(), PUBLISHER)).await;
    assert_eq!(outcome, AdminEventValidation::Accepted);
    let view = stored(&storage).await.expect("latest snapshot stored");
    assert_eq!(view.head.snapshot, third);
    assert_eq!(view.head.snapshot.number, 3);
}

#[tokio::test]
async fn membership_gaps_defer() {
    // A snapshot that arrives before its publisher's membership waits instead of failing.
    let (_dir, storage) = test_storage();
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(OTHER), RealmNodeKind::Server);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id: realm() },
            config.to_bytes(&actor(OTHER)).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let early = receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await;
    assert!(matches!(early, AdminEventValidation::Deferred { .. }));

    // After the publisher's removal, what it signed as a member stays relayable.
    let target = AdminDocumentTarget::RealmConfig { realm_id: realm() };
    let mut state = AdminDocumentState::new(target.clone());
    let ensure = AdminDocumentOperation::ConfigNodeEnsured {
        node_id: node(PUBLISHER),
        kind: RealmNodeKind::Server,
    };
    let removed = AdminDocumentOperation::ConfigNodeRemoved {
        node_id: node(PUBLISHER),
        cutoffs: vec![PublisherCutoff {
            set_id: root().set_id(),
            revision: created().revision(),
        }],
    };
    state.apply_operation(&actor(OTHER), ensure).unwrap();
    state.apply_operation(&actor(OTHER), removed).unwrap();
    batch_write_to(&storage, vec![reducer_state_entry(&state).unwrap()])
        .await
        .unwrap();
    let relayed = receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await;
    assert_eq!(relayed, AdminEventValidation::Accepted);
    // Anything it signs beyond its cutoff carries no authority any more.
    let later = admitted(&created(), MEMBER);
    let beyond = receive(&storage, published(PUBLISHER, later, PUBLISHER)).await;
    assert!(rejected(beyond));
    let mut forked = created();
    forked.config.settings.public = true;
    let fork = receive(&storage, published(PUBLISHER, forked, PUBLISHER)).await;
    assert!(rejected(fork));

    // A device can never publish a set's revisions.
    config.ensure_node(
        node(MEMBER),
        RealmNodeKind::User {
            owner: actor(MEMBER).user_id,
        },
    );
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id: realm() },
            config.to_bytes(&actor(OTHER)).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let mut device_root = root();
    device_root.publisher = node(MEMBER);
    let mut snapshot = created();
    snapshot.root = device_root;
    let device = receive(&storage, published(MEMBER, snapshot, MEMBER)).await;
    assert!(rejected(device));
}

#[tokio::test]
async fn federated_admin_published() {
    // A federated admin's accepted request still reaches every node as the publisher's revision.
    let (_dir, storage) = test_storage();
    realm_nodes(&storage).await;
    let mut event = published(PUBLISHER, created(), PUBLISHER);
    event.actor.user_id = UserId::new(Ulid::from_parts(845, 1), RealmId::from_bytes([85; 32]));
    assert_eq!(
        receive(&storage, event).await,
        AdminEventValidation::Accepted
    );
    let mut forged = published(OTHER, created(), PUBLISHER);
    forged.actor.user_id = UserId::new(Ulid::from_parts(845, 1), RealmId::from_bytes([85; 32]));
    assert!(rejected(receive(&storage, forged).await));
}
