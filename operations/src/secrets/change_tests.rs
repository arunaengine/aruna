//! Tests the shared create, replace, rotate and delete rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::compute::Secret;
use aruna_core::secrets::{Delivery, ResponseExposure, SecretConsumer};
use aruna_core::structs::identity::realm::RealmId;

const CANARY: &str = "canary-5b71";

pub(super) fn actor() -> UserId {
    UserId::local(Ulid::from_bytes([2; 16]), RealmId::from_bytes([1; 32]))
}

pub(super) fn name() -> SecretName {
    SecretName::try_from("catalog-key".to_string()).unwrap()
}

pub(super) fn token(value: &str) -> SecretFields {
    SecretFields::new([("token".to_string(), Secret::new(value))].into())
}

pub(super) fn create() -> SecretChange {
    SecretChange::Create {
        name: name(),
        kind: SecretKind::parse("bearer").unwrap(),
        description: "Catalog API".to_string(),
        settings: SecretSettings::new(),
        uses: Vec::new(),
        fields: token(CANARY),
    }
}

fn id(byte: u8) -> Ulid {
    Ulid::from_bytes([byte; 16])
}

fn created() -> SecretRecord {
    let applied = apply_change(SecretScope::Node, None, create(), actor(), 10, id(10)).unwrap();
    applied.info.unwrap()
}

#[test]
fn create_once() {
    let applied = apply_change(SecretScope::Node, None, create(), actor(), 10, id(10)).unwrap();
    let info = applied.info.unwrap();
    assert_eq!((info.revision, info.value_id), (id(10), id(10)));
    assert_eq!(info.fields, ["token"]);
    assert_eq!(applied.audit.action, AdminAuditAction::SecretCreated);
    assert!(applied.value.is_some() && applied.retired.is_none());
    let again = apply_change(
        SecretScope::Node,
        Some(&info),
        create(),
        actor(),
        11,
        id(11),
    );
    assert_eq!(again.unwrap_err(), ChangeError::Exists);
}

#[test]
fn replace_keeps_value() {
    let current = created();
    let change = SecretChange::Replace {
        name: name(),
        description: "New".to_string(),
        settings: SecretSettings::new(),
        uses: Vec::new(),
        expected: Some(current.revision_token()),
    };
    let applied = apply_change(
        SecretScope::Node,
        Some(&current),
        change,
        actor(),
        11,
        id(11),
    );
    let applied = applied.unwrap();
    let info = applied.info.unwrap();
    assert_eq!((info.revision, info.value_id), (id(11), id(10)));
    assert_eq!((info.description.as_str(), info.created_at_ms), ("New", 10));
    assert!(applied.value.is_none() && applied.retired.is_none());
    assert_eq!(applied.audit.action, AdminAuditAction::SecretReplaced);
}

#[test]
fn rotate_retires_value() {
    let current = created();
    let change = SecretChange::Rotate {
        name: name(),
        fields: token("canary-5b72"),
        expected: None,
    };
    let applied = apply_change(
        SecretScope::Node,
        Some(&current),
        change,
        actor(),
        12,
        id(12),
    );
    let applied = applied.unwrap();
    assert_eq!(applied.info.unwrap().value_id, id(12));
    assert_eq!(applied.retired, Some(id(10)));
    let wrong = SecretChange::Rotate {
        name: name(),
        fields: SecretFields::new([("key".to_string(), Secret::new("x"))].into()),
        expected: None,
    };
    let refused = apply_change(
        SecretScope::Node,
        Some(&current),
        wrong,
        actor(),
        12,
        id(12),
    );
    assert_eq!(
        refused.unwrap_err(),
        ChangeError::Invalid(SecretError::Fields)
    );
}

#[test]
fn stale_token_refused() {
    let current = created();
    let delete = |expected: &str| SecretChange::Delete {
        name: name(),
        expected: Some(expected.to_string()),
    };
    let stale = apply_change(
        SecretScope::Node,
        Some(&current),
        delete("x"),
        actor(),
        13,
        id(13),
    );
    assert_eq!(stale.unwrap_err(), ChangeError::Stale);
    let token = current.revision_token();
    let applied = apply_change(
        SecretScope::Node,
        Some(&current),
        delete(&token),
        actor(),
        13,
        id(13),
    );
    let applied = applied.unwrap();
    assert_eq!((applied.info, applied.retired), (None, Some(id(10))));
    let missing = apply_change(SecretScope::Node, None, delete(&token), actor(), 13, id(13));
    assert_eq!(missing.unwrap_err(), ChangeError::NotFound);
}

#[test]
fn scope_checks_rules() {
    let rule = UseRule {
        consumer: SecretConsumer::Plugin {
            plugin_id: "announcer".to_string(),
            install_id: id(3),
        },
        delivery: Delivery::Inject,
        origins: vec!["https://api.example.org".to_string()],
        paths: Vec::new(),
        methods: Vec::new(),
        response: ResponseExposure::Status,
        groups: Vec::new(),
    };
    let SecretChange::Create {
        name,
        kind,
        description,
        settings,
        fields,
        ..
    } = create()
    else {
        unreachable!("create() builds a create change");
    };
    let change = SecretChange::Create {
        name,
        kind,
        description,
        settings,
        uses: vec![rule],
        fields,
    };
    // A node secret names the groups whose installs may use it.
    let refused = apply_change(SecretScope::Node, None, change, actor(), 10, id(10));
    assert_eq!(
        refused.unwrap_err(),
        ChangeError::Invalid(SecretError::Rule)
    );
}

#[test]
fn audit_hides_value() {
    let applied = apply_change(SecretScope::Node, None, create(), actor(), 10, id(10)).unwrap();
    let shown = format!("{applied:?}{:?}", applied.audit.to_bytes().unwrap());
    assert!(!shown.contains(CANARY));
    assert!(
        !(applied.audit.to_bytes().unwrap())
            .windows(CANARY.len())
            .any(|w| w == CANARY.as_bytes())
    );
}
