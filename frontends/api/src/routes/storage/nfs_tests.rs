//! Tests the NFS administration transport: proof of possession, fingerprints and error mapping.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signer, SigningKey};

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

/// A key proof for `user`, signed by a fixed host key, and that key's SPKI fingerprint.
fn signed_proof(user: &UserId) -> (KeyProof, [u8; 32]) {
    let key = SigningKey::from_bytes(&[9; 32]);
    let spki = key.verifying_key().to_public_key_der().unwrap();
    let fingerprint = Hasher::new_with_bytes(spki.as_bytes()).finalize().sha256;
    let pem = key
        .verifying_key()
        .to_public_key_pem(LineEnding::LF)
        .unwrap();
    let signature = key.sign(enroll_challenge(user, &fingerprint).as_bytes());
    let proof = KeyProof {
        public_key_pem: Some(pem),
        certificate_pem: None,
        proof: STANDARD.encode(signature.to_bytes()),
    };
    (proof, fingerprint)
}

#[test]
fn proof_names_key() {
    let owner = user(2);
    let (proof, fingerprint) = signed_proof(&owner);
    assert_eq!(proven_key(&owner, &proof).ok(), Some(fingerprint));
}

#[test]
fn proof_binds_caller() {
    // A proof signed for one user never enrolls a host for another.
    let (proof, _) = signed_proof(&user(2));
    assert!(matches!(
        proven_key(&user(3), &proof),
        Err(ServerError::Forbidden)
    ));
    let mut both = proof.clone();
    both.certificate_pem = both.public_key_pem.clone();
    assert!(matches!(
        proven_key(&user(2), &both),
        Err(ServerError::BadRequestReason(_))
    ));
    let mut garbled = proof;
    garbled.proof = "not base64!".to_string();
    assert!(matches!(
        proven_key(&user(2), &garbled),
        Err(ServerError::BadRequestReason(_))
    ));
}

#[test]
fn parses_fingerprints() {
    let digest = "ab".repeat(32);
    assert_eq!(parse_fingerprints(&[digest]).unwrap(), vec![[0xab; 32]]);
    assert!(parse_fingerprints(&[]).is_err());
    assert!(parse_fingerprints(&["ab".repeat(31)]).is_err());
    assert!(parse_fingerprints(&["zz".repeat(32)]).is_err());
}

#[test]
fn maps_vfs_errors() {
    let status = |error| nfs_refusal(error).status_code();
    assert_eq!(status(VfsError::InvalidName), StatusCode::BAD_REQUEST);
    assert_eq!(status(VfsError::Denied), StatusCode::FORBIDDEN);
    assert_eq!(status(VfsError::VaultLocked), StatusCode::FORBIDDEN);
    assert_eq!(status(VfsError::StaleHandle), StatusCode::NOT_FOUND);
    assert_eq!(status(VfsError::Conflict), StatusCode::CONFLICT);
    assert_eq!(status(VfsError::LockConflict), StatusCode::CONFLICT);
    assert_eq!(
        status(VfsError::QuotaExceeded),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(status(VfsError::NoSpace), StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(
        status(VfsError::Distributed),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let unavailable = VfsError::Unavailable("down".to_string());
    assert_eq!(status(unavailable), StatusCode::SERVICE_UNAVAILABLE);
    let code = nfs_refusal(VfsError::ShareViolation).response_body().code;
    assert_eq!(code.as_deref(), Some("share_violation"));
}
