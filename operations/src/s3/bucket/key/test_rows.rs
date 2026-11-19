//! Builds the stored rows, key lookups and turn checks the bucket key tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use aruna_core::vault_format::key_fingerprint;
use ulid::Ulid;

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

/// One stored key of `user_id` without a recovery copy.
pub(crate) fn keys(user_id: UserId) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        realm_id: user_id.realm_id,
        user_id,
        record_id: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: false,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
        signature: None,
    }])
}

/// The stored encryption settings of `bucket_id` at key generation 2.
pub(crate) fn settings(mode: EncryptionMode, bucket_id: Ulid) -> Vec<u8> {
    let settings = BucketEncryption {
        mode,
        bucket_id: Some(bucket_id),
        key_generation: 2,
        ..Default::default()
    };
    settings.to_bytes().unwrap()
}

/// A last page with `values` under empty keys.
pub(crate) fn rows(values: Vec<Vec<u8>>) -> IoResult {
    IoResult::Page {
        rows: values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.into()))
            .collect(),
        next: None,
    }
}

/// The output of a turn that also wakes the compression migration.
pub(crate) fn woke<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> T {
    match turn {
        Ok(Next::Then { output, calls }) => {
            let wake = Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::MigrateCompression,
                after: std::time::Duration::ZERO,
            });
            assert_eq!(calls, vec![IoCall::from(wake)]);
            output
        }
        other => panic!("expected the transition wake, got {other:?}"),
    }
}
