//! Tests of the import intent and export grant checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::identity::auth::NodeCapabilities;
use ed25519_dalek::SigningKey;

const NOW: u64 = 10_000;
const SECRET: &[u8] = b"browser secret";

fn key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn realm(seed: u8) -> RealmId {
    RealmId::from_bytes(key(seed).verifying_key().to_bytes())
}

fn capabilities(seed: u8) -> NodeCapabilities {
    NodeCapabilities::management_node(key(seed)).unwrap()
}

fn descriptor(seed: u8, issued_at: u64) -> Signed<RealmDescriptor> {
    let url = Url::parse(&format!("https://realm{seed}.example.org")).unwrap();
    let descriptor = RealmDescriptor {
        realm_id: realm(seed),
        name: format!("Realm {seed}"),
        description: String::new(),
        api_url: url.clone(),
        portal_url: url,
        issued_at,
    };
    Signed::sign(descriptor, &capabilities(seed)).unwrap()
}

fn destination() -> ImportDestination {
    ImportDestination {
        group_id: Ulid::from_bytes([1; 16]),
        bucket: "lab".to_string(),
        prefix: "imports".to_string(),
        metadata_path: "datasets/run".to_string(),
    }
}

/// An intent of destination realm 2, signed by `signer`.
fn signed_intent(signer: u8, issued_at: u64, expires_at: u64) -> Signed<ImportIntent> {
    let intent = ImportIntent {
        realm_id: realm(2),
        descriptor_digest: descriptor_digest(&descriptor(2, 1)).unwrap(),
        principal: UserId::new(Ulid::from_bytes([3; 16]), realm(1)),
        destination: destination(),
        max_bytes: 1_000,
        nonce: secret_nonce(SECRET),
        issued_at,
        expires_at,
        intent_id: Ulid::from_bytes([4; 16]),
    };
    Signed::sign(intent, &capabilities(signer)).unwrap()
}

/// A grant of source realm 1 for `intent`.
fn grant(intent: &Signed<ImportIntent>, size: u64) -> Signed<ExportGrant> {
    let grant = ExportGrant {
        source: realm(1),
        audience: realm(2),
        intent_digest: intent_digest(intent).unwrap(),
        export_job_id: Ulid::from_bytes([5; 16]),
        document_id: Ulid::from_bytes([6; 16]),
        source_revision: Ulid::from_bytes([7; 16]),
        dataset_digest: "aa".repeat(32),
        selection_digest: selection_digest(Ulid::from_bytes([7; 16]), &[]).unwrap(),
        artifact_url: Url::parse("https://realm1.example.org/artifact").unwrap(),
        artifact_blake3: "bb".repeat(32),
        artifact_size: size,
        issued_at: NOW,
        expires_at: NOW + MAX_TRANSFER_SECS,
    };
    Signed::sign(grant, &capabilities(1)).unwrap()
}

fn valid_intent() -> Signed<ImportIntent> {
    signed_intent(2, NOW, NOW + MAX_TRANSFER_SECS)
}

#[test]
fn intent_admitted() {
    let intent = valid_intent();
    let current = descriptor(2, 1);
    assert_eq!(
        check_intent(&intent, &realm(2), &current, Some(SECRET), NOW),
        Ok(())
    );
    // The push path has no browser secret; every other binding still holds.
    assert_eq!(
        check_intent(&intent, &realm(2), &current, None, NOW),
        Ok(())
    );
    assert_eq!(check_remote(&intent, &current, &realm(1), NOW), Ok(()));
}

#[test]
fn intent_bindings_rejected() {
    let intent = valid_intent();
    let current = descriptor(2, 1);
    let local = realm(2);
    assert_eq!(
        check_intent(&intent, &local, &current, Some(b"other"), NOW),
        Err(TransferError::WrongSecret)
    );
    // A superseded descriptor no longer admits intents issued for it.
    assert_eq!(
        check_intent(&intent, &local, &descriptor(2, 2), Some(SECRET), NOW),
        Err(TransferError::StaleDescriptor)
    );
    assert_eq!(
        check_intent(&intent, &realm(1), &current, Some(SECRET), NOW),
        Err(TransferError::Signature(FederationError::RealmMismatch))
    );
    // Signed by another realm's key while naming realm 2.
    let forged = signed_intent(3, NOW, NOW + 60);
    assert!(matches!(
        check_intent(&forged, &local, &current, Some(SECRET), NOW),
        Err(TransferError::Signature(_))
    ));
    assert!(matches!(
        check_remote(&forged, &current, &realm(1), NOW),
        Err(TransferError::Signature(_))
    ));
    // The source never accepts an intent of its own realm.
    let own = descriptor(1, 1);
    assert_eq!(
        check_remote(&intent, &own, &realm(1), NOW),
        Err(TransferError::WrongRealm)
    );
}

#[test]
fn intent_expiry_enforced() {
    let current = descriptor(2, 1);
    let local = realm(2);
    let expired = valid_intent();
    let later = NOW + MAX_TRANSFER_SECS;
    assert_eq!(
        check_intent(&expired, &local, &current, None, later),
        Err(TransferError::BadLifetime)
    );
    let too_long = signed_intent(2, NOW, NOW + MAX_TRANSFER_SECS + 1);
    assert_eq!(
        check_intent(&too_long, &local, &current, None, NOW),
        Err(TransferError::BadLifetime)
    );
    let future = signed_intent(2, NOW + 120, NOW + 180);
    assert_eq!(
        check_intent(&future, &local, &current, None, NOW),
        Err(TransferError::BadLifetime)
    );
}

#[test]
fn grant_binding_enforced() {
    let intent = valid_intent();
    let granted = grant(&intent, 1_000);
    assert_eq!(check_grant(&granted, &intent, NOW), Ok(()));
    assert_eq!(
        check_issued(&granted, &realm(1), Ulid::from_bytes([5; 16]), NOW),
        Ok(())
    );
    // The grant works only for its own artifact.
    assert_eq!(
        check_issued(&granted, &realm(1), Ulid::from_bytes([8; 16]), NOW),
        Err(TransferError::Unbound)
    );
    let other = signed_intent(2, NOW, NOW + 60);
    assert_eq!(
        check_grant(&granted, &other, NOW),
        Err(TransferError::Unbound)
    );
    assert_eq!(
        check_grant(&grant(&intent, 1_001), &intent, NOW),
        Err(TransferError::TooLarge)
    );
    assert_eq!(
        check_grant(&granted, &intent, NOW + MAX_TRANSFER_SECS),
        Err(TransferError::BadLifetime)
    );
    let mut tampered = granted.clone();
    tampered.payload.artifact_blake3 = "cc".repeat(32);
    assert!(matches!(
        check_grant(&tampered, &intent, NOW),
        Err(TransferError::Signature(_))
    ));
}

#[test]
fn import_key_identity() {
    // Retries map to one key; another selection or destination does not.
    let intent = valid_intent();
    let granted = grant(&intent, 10).payload;
    let key = import_key(&granted, &destination()).unwrap();
    assert_eq!(key, import_key(&granted, &destination()).unwrap());
    let mut moved = destination();
    moved.prefix = "elsewhere".to_string();
    assert_ne!(key, import_key(&granted, &moved).unwrap());
    let mut reselected = granted.clone();
    reselected.selection_digest = "dd".repeat(32);
    assert_ne!(key, import_key(&reselected, &destination()).unwrap());
}

#[test]
fn selection_order_free() {
    let version = |seed: u8| SelectedVersion {
        version_id: Ulid::from_bytes([seed; 16]),
        blake3: [seed; 32],
        size: u64::from(seed),
    };
    let revision = Ulid::from_bytes([9; 16]);
    let forward = selection_digest(revision, &[version(1), version(2)]).unwrap();
    let backward = selection_digest(revision, &[version(2), version(1)]).unwrap();
    assert_eq!(forward, backward);
    let mut changed = version(2);
    changed.size = 99;
    assert_ne!(
        forward,
        selection_digest(revision, &[version(1), changed]).unwrap()
    );
}
