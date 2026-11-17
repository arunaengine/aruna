//! Tests the node side of a promotion: who may hand the key, the stored key and the check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};
use aruna_core::types::Key;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;

use super::*;
use crate::tests::policy::realm_view;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// Node 1 manages, node 2 serves, node 3 is a device.
fn realm() -> (SigningKey, RealmId, RealmConfigDocument) {
    let key = generate_signing_key();
    let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
    let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    document.ensure_node(node(1), RealmNodeKind::Management);
    document.ensure_node(node(2), RealmNodeKind::Server);
    let owner = UserId::nil(realm_id);
    document.ensure_node(node(3), RealmNodeKind::User { owner });
    (key, realm_id, document)
}

fn pem(key: &SigningKey) -> RealmKeyPem {
    RealmKeyPem(key.to_pkcs8_pem(LineEnding::default()).unwrap().to_string())
}

fn stored(value: Option<Value>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value,
    })
}

fn config_row(document: &RealmConfigDocument) -> Event {
    stored(Some(realm_view(document, UserId::nil(document.realm_id)).0))
}

#[test]
fn handoff_checks_sender() {
    let (key, realm_id, document) = realm();
    let key = pem(&key);
    assert_eq!(
        check_handoff(&document, node(1), node(2), realm_id, &key),
        Ok(())
    );
    assert_eq!(
        check_handoff(&document, node(2), node(2), realm_id, &key),
        Err(MetadataReadError::Forbidden),
        "a server node may not hand a key"
    );
    assert_eq!(
        check_handoff(&document, node(1), node(3), realm_id, &key),
        Err(MetadataReadError::Forbidden),
        "a device never stores the realm key"
    );
    assert_eq!(
        check_handoff(&document, node(1), node(9), realm_id, &key),
        Err(MetadataReadError::Forbidden)
    );
}

#[test]
fn handoff_rejects_foreign() {
    let (_, realm_id, document) = realm();
    let foreign = pem(&generate_signing_key());
    assert_eq!(
        check_handoff(&document, node(1), node(2), realm_id, &foreign),
        Err(MetadataReadError::Unauthorized)
    );
    let garbage = RealmKeyPem("not a key".to_string());
    assert_eq!(
        check_handoff(&document, node(1), node(2), realm_id, &garbage),
        Err(MetadataReadError::Unauthorized)
    );
}

#[test]
fn stores_handed_key() {
    // A server node stores the realm key a management node hands it.
    let (key, realm_id, document) = realm();
    let handed = pem(&key);
    let request = PromotionRequest::HandKey(handed.clone());
    let mut operation = ServePromotionOperation::new(realm_id, node(2), node(1), request);
    let write = Effect::Storage(StorageEffect::Write {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: ByteView::from(PROMOTED_KEY_ROW),
        value: ByteView::from(handed.0.as_bytes()),
        txn_id: None,
    });
    let read = read_effect(&DocumentTarget::RealmConfig { realm_id }, None);
    check_trace(
        &mut operation,
        &[read],
        vec![
            (config_row(&document), vec![write]),
            (
                Event::Storage(StorageEvent::WriteResult {
                    key: Key::from(Vec::new()),
                }),
                vec![],
            ),
        ],
    );
    assert_eq!(operation.finalize(), Ok(Ok(true)));
}

#[test]
fn check_reports_ready() {
    // Ready only for a management node that holds a key; a non-manager asker is refused.
    let (_, realm_id, document) = realm();
    let check = |local, peer| {
        let mut operation =
            ServePromotionOperation::new(realm_id, local, peer, PromotionRequest::Check);
        operation.start();
        operation
    };
    let mut held = check(node(1), node(1));
    assert_eq!(held.step(config_row(&document)).len(), 1);
    held.step(stored(Some(Value::from(b"key".to_vec()))));
    assert_eq!(held.finalize(), Ok(Ok(true)));
    let mut server = check(node(2), node(1));
    assert!(server.step(config_row(&document)).is_empty());
    assert_eq!(server.finalize(), Ok(Ok(false)));
    let mut refused = check(node(1), node(2));
    assert!(refused.step(config_row(&document)).is_empty());
    assert_eq!(refused.finalize(), Ok(Err(MetadataReadError::Forbidden)));
}

#[test]
fn rejects_unexpected_events() {
    // Before start every event fails the serve; a missing config answers unavailable.
    let (_, realm_id, _) = realm();
    let make = || ServePromotionOperation::new(realm_id, node(1), node(1), PromotionRequest::Check);
    for (label, effects, error) in rejects_events(make, sample_events, |_| false) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(error, MetadataReadError::Unavailable, "{label}");
    }
    let mut missing = make();
    missing.start();
    missing.step(stored(None));
    assert_eq!(missing.finalize(), Ok(Err(MetadataReadError::Unavailable)));
}

#[test]
fn abort_and_finalize() {
    // Serving needs no cleanup, and an early finalize fails.
    let (_, realm_id, _) = realm();
    let mut operation =
        ServePromotionOperation::new(realm_id, node(1), node(1), PromotionRequest::Check);
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}
