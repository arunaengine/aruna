//! Tests that receiving nodes admit distribution snapshots only from the set's publisher, with
//! valid embedded statements, and keep the highest signed view without role rechecks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, PARKED_INDEX_KEYSPACE};
use aruna_core::structs::storage::distribution::presets::replica_layout;
use aruna_core::structs::storage::distribution::publisher_key;
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

/// The revision number parked for the test set's publisher, if any.
async fn parked(storage: &StorageHandle) -> Option<u64> {
    let key = publisher_key(&node(PUBLISHER), root().set_id());
    storage_read_from(storage, PARKED_INDEX_KEYSPACE.to_string(), key.into())
        .await
        .unwrap()
        .map(|value| u64::from_be_bytes(<[u8; 8]>::try_from(value.as_ref()).unwrap()))
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
async fn missing_config_parks() {
    let (_dir, storage) = test_storage();
    let outcome = receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await;
    assert_eq!(outcome, AdminEventValidation::Accepted);
    assert_eq!(stored(&storage).await, None);
    assert_eq!(parked(&storage).await, Some(1));
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
    assert_eq!(early, AdminEventValidation::Accepted);
    assert_eq!(stored(&storage).await, None);

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

#[tokio::test]
async fn revocation_drops_parked() {
    // A removal that lands while a snapshot waits ends the publisher's authority at its cutoff.
    let (_dir, storage) = test_storage();
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(OTHER), RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id: realm() },
            config.to_bytes(&actor(OTHER)).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let later = admitted(&created(), MEMBER);
    let outcome = receive(&storage, published(PUBLISHER, later, PUBLISHER)).await;
    assert_eq!(outcome, AdminEventValidation::Accepted);
    assert_eq!(parked(&storage).await, Some(2));

    let removal = test_admin_event(
        Ulid::from_parts(846, 1),
        AdminDocumentTarget::RealmConfig { realm_id: realm() },
        &actor(OTHER),
        1,
        AdminDocumentOperation::ConfigNodeRemoved {
            node_id: node(PUBLISHER),
            cutoffs: vec![PublisherCutoff {
                set_id: root().set_id(),
                revision: created().revision(),
            }],
        },
    );
    apply_admin_operation(
        &storage,
        DocumentTarget::RealmConfig { realm_id: realm() },
        removal,
    )
    .await
    .unwrap();
    assert_eq!(parked(&storage).await, None);
    assert_eq!(stored(&storage).await, None);
    let first = receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await;
    assert_eq!(first, AdminEventValidation::Accepted);
    assert_eq!(stored(&storage).await.unwrap().head.snapshot, created());
}

#[tokio::test]
async fn parked_cursor_recovers() {
    // Through the real batch and cursor: the cursor passes a snapshot whose publisher is not
    // known yet, and the later membership update installs it.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    const LOCAL: u8 = 44;
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(LOCAL).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm(),
    )
    .expect("document sync service opens");
    let target = DocumentTarget::RealmConfig { realm_id: realm() };
    let topic_id = target.sync_topic_id(realm(), &PlacementRef::NIL);
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(LOCAL), RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            target.clone(),
            config.to_bytes(&actor(LOCAL)).unwrap().into(),
        )],
    )
    .await
    .unwrap();

    let snapshot = published(PUBLISHER, created(), PUBLISHER);
    let signature = secret(PUBLISHER).sign(&snapshot.signing_bytes(&PlacementRef::NIL).unwrap());
    let publish = |event: AdminDocumentEvent, origin_signature| {
        service.publish_documents(
            vec![DocumentSyncPublish::AdminOperation {
                target: target.clone(),
                event: Box::new(event),
                placement: PlacementRef::NIL,
                allow_genesis: true,
                origin_signature,
            }],
            Vec::new(),
        )
    };
    assert!(matches!(
        publish(snapshot, Some(signature)).await,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(&service, topic_id).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile runs");
    let cursor = read_test_cursor(&storage, topic_id).await.expect("cursor");
    let clock = service.node().storage().actor_clock(&topic_id).unwrap();
    assert!(
        cursor.dominates(&clock),
        "the cursor moves past the parked snapshot"
    );
    assert_eq!(parked(&storage).await, Some(1));
    assert_eq!(stored(&storage).await, None);

    let ensure = test_admin_event(
        Ulid::from_parts(847, 1),
        AdminDocumentTarget::RealmConfig { realm_id: realm() },
        &actor(LOCAL),
        1,
        AdminDocumentOperation::ConfigNodeEnsured {
            node_id: node(PUBLISHER),
            kind: RealmNodeKind::Server,
        },
    );
    assert!(matches!(
        publish(ensure, None).await,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    // A local publication counts as applied here, so replay the topic as a receiver would.
    reset_test_cursor(&service, topic_id).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile runs");
    assert_eq!(parked(&storage).await, None);
    assert_eq!(stored(&storage).await.unwrap().head.snapshot, created());
    service.shutdown().await;
}

#[tokio::test]
async fn cutoff_is_exact() {
    // After removal only the exact cutoff snapshot carries authority: older revisions and
    // lower-number forks are refused, and receivers jump to the cutoff.
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
    let cutoff = admitted(&created(), MEMBER);
    let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id: realm() });
    let removed = AdminDocumentOperation::ConfigNodeRemoved {
        node_id: node(PUBLISHER),
        cutoffs: vec![PublisherCutoff {
            set_id: root().set_id(),
            revision: cutoff.revision(),
        }],
    };
    state.apply_operation(&actor(OTHER), removed).unwrap();
    batch_write_to(&storage, vec![reducer_state_entry(&state).unwrap()])
        .await
        .unwrap();
    assert!(rejected(
        receive(&storage, published(PUBLISHER, created(), PUBLISHER)).await
    ));
    let mut fork = created();
    fork.config.settings.search_index = true;
    assert!(rejected(
        receive(&storage, published(PUBLISHER, fork, PUBLISHER)).await
    ));
    let exact = receive(&storage, published(PUBLISHER, cutoff.clone(), PUBLISHER)).await;
    assert_eq!(exact, AdminEventValidation::Accepted);
    assert_eq!(stored(&storage).await.unwrap().head.snapshot, cutoff);
}

#[tokio::test]
async fn parked_forks_conflict() {
    // Two snapshots of one number wait together and install as a conflict, not a winner.
    let (_dir, storage) = test_storage();
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.ensure_node(node(OTHER), RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id: realm() },
            config.to_bytes(&actor(OTHER)).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let admitted = admitted(&created(), MEMBER);
    let mut fork = admitted.clone();
    fork.config.settings.public = true;
    for snapshot in [admitted.clone(), fork] {
        let outcome = receive(&storage, published(PUBLISHER, snapshot, PUBLISHER)).await;
        assert_eq!(outcome, AdminEventValidation::Accepted);
    }
    assert_eq!(stored(&storage).await, None);
    let ensure = test_admin_event(
        Ulid::from_parts(848, 1),
        AdminDocumentTarget::RealmConfig { realm_id: realm() },
        &actor(OTHER),
        1,
        AdminDocumentOperation::ConfigNodeEnsured {
            node_id: node(PUBLISHER),
            kind: RealmNodeKind::Server,
        },
    );
    apply_admin_operation(
        &storage,
        DocumentTarget::RealmConfig { realm_id: realm() },
        ensure,
    )
    .await
    .unwrap();
    assert_eq!(parked(&storage).await, None);
    let view = stored(&storage).await.expect("installed");
    assert_eq!(view.head.snapshot, admitted);
    assert!(view.conflicted());
}
