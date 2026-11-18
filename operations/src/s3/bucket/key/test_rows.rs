//! Builds the stored rows tests answer a read of a bucket's key authority with.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{Key, Value};

/// The rows of `authority_keys`, where `admins` hold the group admin role.
pub(crate) fn authority_rows(
    info: &BucketInfo,
    settings: Option<&BucketEncryption>,
    admins: &[UserId],
) -> Vec<(Key, Option<Value>)> {
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    let realm_id = info.created_by.realm_id;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: info.created_by,
        realm_id,
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    };
    let admin = Role {
        role_id: ulid::Ulid::from_bytes([1; 16]),
        name: "keepers".to_string(),
        permissions: [(group_admin_path(realm_id, info.group_id), Permission::WRITE)].into(),
        assigned_users: admins.iter().copied().collect(),
    };
    let group = GroupAuthorizationDocument {
        group_id: info.group_id,
        roles: [(admin.role_id, admin)].into(),
        policies: Vec::new(),
    };
    let row = |value: Vec<u8>| (Key::from(Vec::new()), Some(Value::from(value)));
    let settings = settings.map(|settings| Value::from(settings.to_bytes().unwrap()));
    vec![
        row(info.to_bytes().unwrap()),
        (Key::from(Vec::new()), settings),
        row(realm.to_bytes(&actor).unwrap()),
        row(group.to_bytes(&actor).unwrap()),
    ]
}
