//! Unit tests for the NTLMv2 acceptor: MS-NLMP 4.2.4 vectors, MIC policy, refused logons
//! and the session security used for the SPNEGO mechListMIC.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::testing::{AuthOptions, ntlm_authenticate, ntlm_negotiate};

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex digit"))
        .collect()
}

fn names() -> TargetNames {
    TargetNames {
        domain: "ARUNA".to_owned(),
        computer: "NODE".to_owned(),
    }
}

fn answered() -> Challenge {
    Challenge::answer(
        &ntlm_negotiate(),
        [0x01; 8],
        0x01D9_0000_0000_0000,
        &names(),
    )
    .expect("challenge")
}

fn options(secret: &str, mic: bool) -> AuthOptions<'_> {
    AuthOptions {
        user: "AKIA-tester",
        secret,
        mic,
        legacy: false,
    }
}

#[test]
fn matches_nlmp_vectors() {
    let hash = nt_hash("Password");
    assert_eq!(hash.to_vec(), hex("a4f49c406510bdcab6824ee7c30fd852"));
    let key = response_key(&hash, "User", "Domain").expect("NTOWFv2");
    assert_eq!(key.to_vec(), hex("0c868a403bfd7a93a3001ef22ef02e3f"));
    let server_challenge = hex("0123456789abcdef");
    let temp = hex(concat!(
        "0101000000000000",
        "0000000000000000",
        "aaaaaaaaaaaaaaaa",
        "00000000",
        "02000c0044006f006d00610069006e00",
        "01000c00530065007200760065007200",
        "00000000",
        "00000000",
    ));
    let proof = hmac_md5(key.as_slice(), &[&server_challenge, &temp]).expect("proof");
    assert_eq!(proof.to_vec(), hex("68cd0ab851e51c96aabc927bebef6a1c"));
    let base = hmac_md5(key.as_slice(), &[&proof]).expect("base key");
    assert_eq!(base.to_vec(), hex("8de40ccadbc14a82f15cb0ad0de95ca3"));
    let mut exported = [0x55u8; 16];
    rc4_apply(&base, &mut exported).expect("rc4");
    assert_eq!(exported.to_vec(), hex("c5dad2544fc9799094ce1ce90bc9d03e"));
}

#[test]
fn challenge_carries_timestamp() {
    let challenge = answered();
    let message = challenge.message();
    assert_eq!(&message[..8], SIGNATURE);
    assert_eq!(read_u32(message, 8), Ok(2));
    let flags = read_u32(message, 20).expect("flags");
    assert_eq!(flags & REQUIRED, REQUIRED);
    assert_ne!(flags & TARGET_INFO, 0);
    let (info, _) = field_at(message, 40).expect("target info");
    let pairs = av_pairs(info).expect("pairs");
    assert!(
        pairs
            .iter()
            .any(|(id, value)| *id == AV_TIMESTAMP && value.len() == 8)
    );
    assert!(pairs.iter().any(|(id, _)| *id == AV_COMPUTER));
}

#[test]
fn verifies_with_mic() {
    let challenge = answered();
    let (bytes, exported) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", true),
    );
    let auth = parse_authenticate(&bytes).expect("parses");
    assert_eq!(auth.user, "AKIA-tester");
    let verified = challenge
        .verify(&auth, "secret", MicPolicy::Require)
        .expect("verifies");
    assert!(verified.mic_present);
    assert_eq!(*verified.session_key, exported);
}

#[test]
fn verifies_without_exchange() {
    let mut negotiate = ntlm_negotiate();
    let flags = read_u32(&negotiate, 12).expect("flags") & !KEY_EXCHANGE;
    negotiate[12..16].copy_from_slice(&flags.to_le_bytes());
    let challenge = Challenge::answer(&negotiate, [0x01; 8], 0x01D9_0000_0000_0000, &names())
        .expect("challenge");
    let message_flags = read_u32(challenge.message(), 20).expect("flags");
    assert_eq!(
        message_flags & KEY_EXCHANGE,
        0,
        "no key exchange is offered"
    );
    let (bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", false),
    );
    let auth = parse_authenticate(&bytes).expect("parses");
    let hash = nt_hash("secret");
    let key = response_key(&hash, "AKIA-tester", "WORKGROUP").expect("NTOWFv2");
    let base = hmac_md5(key.as_slice(), &[&auth.nt_response[..16]]).expect("base key");
    let verified = challenge
        .verify(&auth, "secret", MicPolicy::AllowMissing)
        .expect("verifies");
    assert_eq!(*verified.session_key, base, "the base key is exported");
}

#[test]
fn rejects_wrong_secret() {
    let challenge = answered();
    let (bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", true),
    );
    let auth = parse_authenticate(&bytes).expect("parses");
    assert_eq!(
        challenge
            .verify(&auth, "Secret", MicPolicy::Require)
            .map(|_| ()),
        Err(NtlmError::Failed)
    );
}

#[test]
fn rejects_tampered_mic() {
    let challenge = answered();
    let (mut bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", true),
    );
    bytes[MIC_OFFSET] ^= 0x80;
    let auth = parse_authenticate(&bytes).expect("parses");
    assert_eq!(
        challenge
            .verify(&auth, "secret", MicPolicy::Require)
            .map(|_| ()),
        Err(NtlmError::Failed)
    );
    let other = Challenge::answer(&ntlm_negotiate(), [0x02; 8], 7, &names()).expect("challenge");
    let (bytes, _) = ntlm_authenticate(&other.negotiate, other.message(), options("secret", true));
    let auth = parse_authenticate(&bytes).expect("parses");
    assert_eq!(
        challenge
            .verify(&auth, "secret", MicPolicy::Require)
            .map(|_| ()),
        Err(NtlmError::Failed),
        "a proof for another challenge fails"
    );
}

#[test]
fn missing_mic_policy() {
    let challenge = answered();
    let (bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", false),
    );
    let auth = parse_authenticate(&bytes).expect("parses");
    assert_eq!(
        challenge
            .verify(&auth, "secret", MicPolicy::Require)
            .map(|_| ()),
        Err(NtlmError::MissingMic)
    );
    let verified = challenge
        .verify(&auth, "secret", MicPolicy::AllowMissing)
        .expect("legacy clients pass when allowed");
    assert!(!verified.mic_present);
}

#[test]
fn refuses_ntlm_v1() {
    let challenge = answered();
    let (bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        AuthOptions {
            legacy: true,
            ..options("secret", false)
        },
    );
    assert_eq!(
        parse_authenticate(&bytes).map(|_| ()),
        Err(NtlmError::Version)
    );
}

#[test]
fn refuses_anonymous_logons() {
    let challenge = answered();
    for user in ["", "guest", "GUEST"] {
        let (bytes, _) = ntlm_authenticate(
            &challenge.negotiate,
            challenge.message(),
            AuthOptions {
                user,
                ..options("secret", true)
            },
        );
        assert_eq!(
            parse_authenticate(&bytes).map(|_| ()),
            Err(NtlmError::Anonymous),
            "{user:?}"
        );
    }
    let (mut bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", true),
    );
    let flags = read_u32(&bytes, 60).expect("flags") | ANONYMOUS;
    bytes[60..64].copy_from_slice(&flags.to_le_bytes());
    assert_eq!(
        parse_authenticate(&bytes).map(|_| ()),
        Err(NtlmError::Anonymous)
    );
}

#[test]
fn requires_negotiate_flags() {
    for missing in [EXTENDED_SECURITY, NEGOTIATE_128, UNICODE] {
        let mut negotiate = ntlm_negotiate();
        let flags = read_u32(&negotiate, 12).expect("flags") & !missing;
        negotiate[12..16].copy_from_slice(&flags.to_le_bytes());
        assert_eq!(
            Challenge::answer(&negotiate, [0; 8], 0, &names()).map(|_| ()),
            Err(NtlmError::Flags),
            "flag {missing:#x}"
        );
    }
    assert!(Challenge::answer(b"NTLMSSP\0\x03\0\0\0", [0; 8], 0, &names()).is_err());
}

#[test]
fn rejects_bad_fields() {
    let challenge = answered();
    let (mut bytes, _) = ntlm_authenticate(
        &challenge.negotiate,
        challenge.message(),
        options("secret", true),
    );
    bytes[24..28].copy_from_slice(&0xFFFF_FF00u32.to_le_bytes());
    assert!(matches!(
        parse_authenticate(&bytes),
        Err(NtlmError::Malformed(_))
    ));
    assert!(parse_authenticate(&bytes[..40]).is_err());
}

#[test]
fn mechanism_mic_directions() {
    let key = [0x55u8; 16];
    let list = crate::spnego::ntlm_list();
    let mut client = SessionSecurity::new(&key).expect("security");
    let mut server = SessionSecurity::new(&key).expect("security");
    let from_client = client.client_mic(0, &list).expect("mic");
    assert_eq!(server.client_mic(0, &list).expect("mic"), from_client);
    let from_server = server.server_mic(0, &list).expect("mic");
    assert_ne!(from_client, from_server, "each direction has its own keys");
    assert_eq!(&from_client[..4], &1u32.to_le_bytes());
    assert_eq!(&from_client[12..], &0u32.to_le_bytes());
    let other = SessionSecurity::new(&[0x56u8; 16])
        .expect("security")
        .client_mic(0, &list)
        .expect("mic");
    assert_ne!(other, from_client);
}
