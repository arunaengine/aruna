//! Unit tests for the preauthentication hash, key derivation, signing and GCM transforms.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::wire::{FLAG_RESPONSE, Header};

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex digit"))
        .collect()
}

fn sample_message(flags: u32, command: u16) -> Vec<u8> {
    let header = Header {
        command,
        flags,
        message_id: 0x0102_0304,
        session_id: 99,
        ..Header::default()
    };
    let mut out = Vec::new();
    header.encode(&mut out);
    out.extend_from_slice(&[0x5a; 40]);
    out
}

#[test]
fn cmac_matches_rfc4493() {
    let key = hex("2b7e151628aed2a6abf7158809cf4f3c");
    assert_eq!(
        signature_of(Signing::AesCmac, &key, &[])
            .expect("cmac")
            .to_vec(),
        hex("bb1d6929e95937287fa37d129b756746")
    );
    let block = hex("6bc1bee22e409f96e93d7e117393172a");
    assert_eq!(
        signature_of(Signing::AesCmac, &key, &block)
            .expect("cmac")
            .to_vec(),
        hex("070a16b46b4d4144f79bdd9dd04a287c")
    );
}

#[test]
fn preauth_chains_messages() {
    let mut chain = PreauthHash::default();
    chain.update(b"negotiate request");
    chain.update(b"negotiate response");
    let mut first = Sha512::new();
    first.update([0u8; 64]);
    first.update(b"negotiate request");
    let first = first.finalize();
    let mut second = Sha512::new();
    second.update(first);
    second.update(b"negotiate response");
    assert_eq!(chain.value().as_slice(), second.finalize().as_slice());
    let mut swapped = PreauthHash::default();
    swapped.update(b"negotiate response");
    swapped.update(b"negotiate request");
    assert_ne!(swapped, chain, "the chain depends on message order");
}

#[test]
fn derivation_matches_formula() {
    let key = [0x11u8; 16];
    let context = [0x22u8; 64];
    let derived = derive_key(&key, SIGNING_LABEL, &context, 128).expect("derivation");
    let mut mac = <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(&key).expect("hmac key");
    mac.update(&1u32.to_be_bytes());
    mac.update(b"SMBSigningKey\0");
    mac.update(&[0]);
    mac.update(&context);
    mac.update(&128u32.to_be_bytes());
    let expected = mac.finalize().into_bytes();
    assert_eq!(derived.as_slice(), &expected[..16]);
    let longer = derive_key(&key, SIGNING_LABEL, &context, 256).expect("derivation");
    assert_eq!(longer.len(), 32);
    assert_ne!(&longer[..16], derived.as_slice(), "L is part of the input");
}

#[test]
fn session_keys_differ() {
    let preauth = [0x33u8; 64];
    let keys = SessionKeys::derive(&[0x44u8; 16], &preauth, Cipher::Aes128Gcm).expect("keys");
    assert_eq!(keys.signing.len(), 16);
    assert_eq!(keys.encryption.len(), 16);
    assert_ne!(keys.encryption.as_slice(), keys.decryption.as_slice());
    assert_ne!(keys.signing.as_slice(), keys.application.as_slice());
    let wide = SessionKeys::derive(&[0x44u8; 16], &preauth, Cipher::Aes256Gcm).expect("keys");
    assert_eq!(wide.encryption.len(), 32);
    assert_eq!(wide.decryption.len(), 32);
    assert_eq!(wide.signing.as_slice(), keys.signing.as_slice());
    let other = SessionKeys::derive(&[0x44u8; 16], &[0x34u8; 64], Cipher::Aes128Gcm).expect("keys");
    assert_ne!(
        other.signing.as_slice(),
        keys.signing.as_slice(),
        "bound to the transcript"
    );
}

#[test]
fn signatures_detect_tampering() {
    let key = [0x66u8; 16];
    for algorithm in [Signing::AesCmac, Signing::AesGmac] {
        let mut message = sample_message(FLAG_RESPONSE, 1);
        sign(algorithm, &key, &mut message).expect("sign");
        assert_ne!(&message[48..64], &[0u8; 16]);
        assert_ne!(message[16] & 0x08, 0, "the signed flag is set");
        assert_eq!(verify(algorithm, &key, &message), Ok(true));
        let mut tampered = message.clone();
        tampered[HEADER_SIZE + 3] ^= 1;
        assert_eq!(verify(algorithm, &key, &tampered), Ok(false));
        assert_eq!(verify(algorithm, &[0x67u8; 16], &message), Ok(false));
    }
}

#[test]
fn gmac_nonce_layout() {
    let key = [0x77u8; 16];
    let mut response = sample_message(FLAG_RESPONSE, 1);
    sign(Signing::AesGmac, &key, &mut response).expect("sign");
    let mut zeroed = response.clone();
    zeroed[48..64].fill(0);
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(&0x0102_0304u64.to_le_bytes());
    nonce[8..].copy_from_slice(&1u32.to_le_bytes());
    let expected = gcm_seal(Cipher::Aes128Gcm, &key, nonce, &zeroed, &mut []).expect("gmac");
    assert_eq!(
        &response[48..64],
        &expected,
        "MessageId then the server role bit"
    );
    let mut cancel = sample_message(0, 0x0C);
    sign(Signing::AesGmac, &key, &mut cancel).expect("sign");
    let mut zeroed = cancel.clone();
    zeroed[48..64].fill(0);
    nonce[8..].copy_from_slice(&2u32.to_le_bytes());
    let expected = gcm_seal(Cipher::Aes128Gcm, &key, nonce, &zeroed, &mut []).expect("gmac");
    assert_eq!(
        &cancel[48..64],
        &expected,
        "a CANCEL request sets the second bit"
    );
}

#[test]
fn transform_round_trip() {
    for (cipher, size) in [(Cipher::Aes128Gcm, 16), (Cipher::Aes256Gcm, 32)] {
        let key = Zeroizing::new(vec![0x5cu8; size]);
        let mut sealer = Sealer::new(cipher, key.clone());
        let message = sample_message(FLAG_RESPONSE, 8);
        let first = sealer.seal(7, &message).expect("seal");
        let second = sealer.seal(7, &message).expect("seal");
        assert_ne!(first, second, "each message gets a fresh nonce");
        assert_eq!(&first[20..28], &0u64.to_le_bytes());
        assert_eq!(&second[20..28], &1u64.to_le_bytes());
        assert_eq!(unseal(cipher, &key, &first), Ok(message.clone()));
        let mut body = first.clone();
        body[TRANSFORM_SIZE + 5] ^= 1;
        assert_eq!(unseal(cipher, &key, &body), Err(CryptoError::Open));
        let mut session = first.clone();
        session[44] ^= 1;
        assert_eq!(unseal(cipher, &key, &session), Err(CryptoError::Open));
        let wrong = vec![0x5du8; size];
        assert_eq!(unseal(cipher, &wrong, &first), Err(CryptoError::Open));
    }
}

#[test]
fn transform_rejects_layout() {
    let key = Zeroizing::new(vec![1u8; 16]);
    let mut sealer = Sealer::new(Cipher::Aes128Gcm, key.clone());
    let packet = sealer.seal(3, &sample_message(0, 8)).expect("seal");
    let mut flags = packet.clone();
    flags[42] = 2;
    assert!(matches!(
        unseal(Cipher::Aes128Gcm, &key, &flags),
        Err(CryptoError::Transform(_))
    ));
    let mut size = packet.clone();
    size[36] ^= 1;
    assert!(matches!(
        unseal(Cipher::Aes128Gcm, &key, &size),
        Err(CryptoError::Transform(_))
    ));
    assert!(unseal(Cipher::Aes128Gcm, &key, &packet[..40]).is_err());
}

#[test]
fn nonce_counter_stops() {
    let key = Zeroizing::new(vec![2u8; 16]);
    let mut sealer = Sealer::starting(Cipher::Aes128Gcm, key, u64::MAX - 1);
    let message = sample_message(0, 8);
    let last = sealer.seal(1, &message).expect("the last nonce is usable");
    assert_eq!(&last[20..28], &(u64::MAX - 1).to_le_bytes());
    assert_eq!(sealer.seal(1, &message), Err(CryptoError::NonceExhausted));
    assert_eq!(sealer.seal(1, &message), Err(CryptoError::NonceExhausted));
}

#[test]
fn ccm_is_refused() {
    assert_eq!(Cipher::from_code(CIPHER_AES128_CCM), None);
    assert_eq!(Cipher::from_code(CIPHER_AES256_CCM), None);
    assert_eq!(
        Cipher::from_code(CIPHER_AES128_GCM),
        Some(Cipher::Aes128Gcm)
    );
    assert_eq!(Signing::from_code(SIGNING_HMAC), None);
}
