//! Stores realm and group documents so permission and policy checks run against storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use ulid::Ulid;

/// The group owner may write everywhere in the group except below `denied`.
pub(crate) async fn seed_access(
    context: &DriverContext,
    (realm_id, group_id, node_id): (RealmId, GroupId, NodeId),
    denied: &str,
) -> AuthContext {
    let owner = UserId::local(Ulid::from_parts(5, 5), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut document = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    for role in document.roles.values_mut() {
        role.permissions
            .insert(denied.to_string(), Permission::DENY);
    }
    let group = Group {
        display_name: "Lab".to_string(),
        group_id,
        realm_id,
        roles: document.roles.keys().copied().collect(),
        owner,
    };
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let (realm_key, group_key) = (realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec());
    let rows = [
        (
            REALM_CONFIG_KEYSPACE,
            realm_key.clone(),
            config.to_bytes(&actor),
        ),
        (AUTH_KEYSPACE, realm_key, realm_doc.to_bytes(&actor)),
        (GROUP_KEYSPACE, group_key.clone(), group.to_bytes(&actor)),
        (AUTH_KEYSPACE, group_key, document.to_bytes(&actor)),
    ];
    let writes = rows
        .into_iter()
        .map(|(space, key, value)| (space.to_string(), key.into(), value.unwrap().into()))
        .collect();
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}
