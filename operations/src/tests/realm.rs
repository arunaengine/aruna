//! Seeds the realm config and realm admin rows the realm setting tests start from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_drivers::driver::DriverContext;

use aruna_drivers::testing::write_value;

/// Stores `document` as the realm config, signed by `actor`.
pub(crate) async fn seed_config(
    ctx: &DriverContext,
    actor: &Actor,
    document: &RealmConfigDocument,
) {
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let value = document.to_bytes(actor).unwrap();
    write_value(
        &ctx.storage_handle,
        target.storage_keyspace(),
        target.storage_key(),
        value,
    )
    .await;
}

/// Stores the default realm authorization with `actor` in every role.
pub(crate) async fn seed_realm_admin(ctx: &DriverContext, actor: &Actor) {
    let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in document.roles.values_mut() {
        role.assigned_users.insert(actor.user_id);
    }
    let key = actor.realm_id.as_bytes().to_vec();
    write_value(
        &ctx.storage_handle,
        AUTH_KEYSPACE,
        key,
        document.to_bytes(actor).unwrap(),
    )
    .await;
}

/// A realm with a management node, a server and a device of the nil user, and its signing key.
pub(crate) fn promotion_realm(
    nodes: [aruna_core::NodeId; 3],
) -> (
    ed25519_dalek::SigningKey,
    aruna_core::structs::identity::realm::RealmId,
    RealmConfigDocument,
) {
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    let key = aruna_core::keys::generate_signing_key();
    let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.ensure_node(nodes[0], RealmNodeKind::Management);
    document.ensure_node(nodes[1], RealmNodeKind::Server);
    let owner = aruna_core::UserId::nil(realm_id);
    document.ensure_node(nodes[2], RealmNodeKind::User { owner });
    (key, realm_id, document)
}

/// A user of `realm_id` with an id from `seed` and the display name `name`.
pub(crate) fn named_user(
    realm_id: aruna_core::structs::identity::realm::RealmId,
    seed: u8,
    name: &str,
) -> aruna_core::structs::identity::user::User {
    aruna_core::structs::identity::user::User {
        user_id: aruna_core::UserId::local(ulid::Ulid::from_bytes([seed; 16]), realm_id),
        name: name.to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    }
}
