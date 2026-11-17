//! Tests the node side of a promotion: who may hand the key, the stored key and the check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
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

fn stored(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

fn config_row(document: &RealmConfigDocument) -> IoResult {
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
    let read = read_row(&DocumentTarget::RealmConfig { realm_id });
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![read]))));
    let row = (
        NODE_STATE_KEYSPACE.to_string(),
        ByteView::from(PROMOTED_KEY_ROW),
        ByteView::from(handed.0.as_bytes()),
    );
    let write = Next::Call(IoCall::Write(vec![row]));
    assert_eq!(operation.next(config_row(&document)), Ok(write));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(Ok(true))));
}

#[test]
fn check_reports_ready() {
    // Ready only for a management node that holds a key; a non-manager asker is refused.
    let (_, realm_id, document) = realm();
    let check = |local, peer| {
        let mut operation =
            ServePromotionOperation::new(realm_id, local, peer, PromotionRequest::Check);
        operation.first().unwrap();
        operation
    };
    let mut held = check(node(1), node(1));
    let read = (
        NODE_STATE_KEYSPACE.to_string(),
        ByteView::from(PROMOTED_KEY_ROW),
    );
    let key_read = Ok(Next::Call(IoCall::Read(vec![read])));
    assert_eq!(held.next(config_row(&document)), key_read);
    let answer = held.next(stored(Some(Value::from(b"key".to_vec()))));
    assert_eq!(answer, Ok(Next::Done(Ok(true))));
    let mut server = check(node(2), node(1));
    assert_eq!(
        server.next(config_row(&document)),
        Ok(Next::Done(Ok(false)))
    );
    let mut refused = check(node(1), node(2));
    let forbidden = Ok(Next::Done(Err(MetadataReadError::Forbidden)));
    assert_eq!(refused.next(config_row(&document)), forbidden);
}

#[test]
fn unavailable_answers() {
    // A missing config is answered unavailable; a wrong answer fails the serve.
    let (_, realm_id, _) = realm();
    let make = || ServePromotionOperation::new(realm_id, node(1), node(1), PromotionRequest::Check);
    let mut missing = make();
    missing.first().unwrap();
    let unavailable = Ok(Next::Done(Err(MetadataReadError::Unavailable)));
    assert_eq!(missing.next(stored(None)), unavailable);
    let mut wrong = make();
    wrong.first().unwrap();
    assert_eq!(
        wrong.next(IoResult::Done),
        Err(MetadataReadError::Unavailable)
    );
}
