//! Builds the realm, group, conflict and outbox fixtures the group and role tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::AdminDocumentDot;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
use aruna_core::reducer::{AdminConflict, AdminConflictValue};
use aruna_core::structs::execution::notification::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementStrategy;
use aruna_core::user::id::UserId;
use aruna_p2p::NetHandle;
use aruna_runner::driver::{DriverContext, drive};
use aruna_scheduler::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use crate::tests::calls::node;

/// A storage context with a loopback net and a task plane, its net handle and its directory.
pub(crate) async fn net_context() -> (DriverContext, NetHandle, TempDir) {
    let (dir, context) = aruna_runner::testing::temp_context();
    let net_handle = NetHandle::new(
        aruna_runner::testing::local_net(),
        context.storage_handle.clone(),
    )
    .await
    .unwrap();
    let context = DriverContext {
        net_handle: Some(net_handle.clone()),
        task_handle: Some(TaskHandle::new()),
        ..context
    };
    (context, net_handle, dir)
}

/// A new realm with a fresh user on node 1, and a group that user created.
pub(crate) async fn setup_group(
    context: &DriverContext,
) -> (Actor, Group, GroupAuthorizationDocument) {
    let realm_id = RealmId([0u8; 32]);
    let actor = Actor {
        node_id: node(1),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    let realm = CreateRealmOperation::new(CreateRealmConfig {
        actor: actor.clone(),
        realm_description: "Test realm".to_string(),
        oidc_providers: Vec::new(),
        node_location: None,
        node_weight: None,
        node_labels: Default::default(),
    });
    drive(realm, context).await.unwrap();
    let group = CreateGroupOperation::new(CreateGroupConfig {
        actor: actor.clone(),
        display_name: "Test group".to_string(),
        owner_cap: None,
    });
    let (group, auth_doc) = drive(group, context).await.unwrap();
    (actor, group, auth_doc)
}

/// Every notification in the outbox.
pub(crate) async fn read_notification_outbox(context: &DriverContext) -> Vec<NotificationRecord> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1024,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| {
                postcard::from_bytes::<NotificationOutboxRecord>(&value)
                    .unwrap()
                    .record
            })
            .collect(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// A realm config with the actor's node as a server and one default strategy.
pub(crate) fn activated_config(actor: &Actor) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    config.strategies.push(PlacementStrategy {
        strategy_id: Ulid::from_bytes([5; 16]),
        name: "default".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 16,
    });
    config.default_strategy_id = Some(config.strategies[0].strategy_id);
    config.snapshot_candidate_map();
    config
}

/// An admin document dot of node `seed`.
pub(crate) fn dot(seed: u8) -> AdminDocumentDot {
    AdminDocumentDot {
        event_id: Ulid::from_bytes([seed; 16]),
        origin_node_id: node(seed),
        origin_seq: u64::from(seed),
    }
}

/// A conflict at `path` between adding `user_id` at `add_seed` and removing it at `remove_seed`.
pub(crate) fn assignment_conflict(
    path: String,
    user_id: UserId,
    add_seed: u8,
    remove_seed: u8,
) -> AdminConflict {
    AdminConflict {
        path,
        values: vec![
            AdminConflictValue {
                value: Some(user_id.to_string()),
                dot: dot(add_seed),
            },
            AdminConflictValue {
                value: None,
                dot: dot(remove_seed),
            },
        ],
    }
}
