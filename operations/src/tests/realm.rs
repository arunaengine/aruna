//! Seeds the realm config and realm admin rows the realm setting tests start from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_drivers::driver::DriverContext;

use crate::testing::write_value;

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
