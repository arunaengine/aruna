// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn issuer() -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[2; 32])
}

fn target() -> NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn ticket(issued_at: u64, expires_at: u64) -> Ticket {
    Ticket {
        realm_id: realm(),
        issuer: issuer().public(),
        target: target(),
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm()),
        grant: TokenGrant {
            token_hash: "hash".to_string(),
            issuer: realm(),
            issued_at_secs: 1,
            expires_at_secs: expires_at,
            via: None,
        },
        restrictions: None,
        session: None,
        object_id: "/realm/version".to_string(),
        nonce: [5; 16],
        issued_at,
        expires_at,
    }
}

fn signed(ticket: Ticket) -> SignedTicket {
    let signature = issuer().sign(&ticket.signing_bytes().unwrap());
    SignedTicket { ticket, signature }
}

#[test]
fn expiry_follows_credential() {
    assert_eq!(ticket_expiry(100, u64::MAX), 100 + TICKET_SECS);
    assert_eq!(ticket_expiry(100, 103), 103);
}

#[test]
fn verifies_round_trip() {
    let signed = signed(ticket(100, 105));
    let decoded = SignedTicket::decode(&signed.encode().unwrap()).unwrap();
    assert_eq!(decoded, signed);
    assert!(
        decoded
            .verify(realm(), target(), "/realm/version", 104)
            .is_ok()
    );
}

#[test]
fn refuses_other_requests() {
    let signed = signed(ticket(100, 105));
    assert_eq!(
        signed.verify(realm(), issuer().public(), "/realm/version", 101),
        Err(TicketError::WrongTarget)
    );
    assert_eq!(
        signed.verify(realm(), target(), "/realm/other", 101),
        Err(TicketError::ObjectMismatch)
    );
    assert_eq!(
        signed.verify(realm(), target(), "/realm/version", 106),
        Err(TicketError::Expired)
    );
    // An issuer may not stretch a ticket past the bound.
    let long = self::signed(ticket(100, 100 + TICKET_SECS + 1));
    assert_eq!(
        long.verify(realm(), target(), "/realm/version", 101),
        Err(TicketError::Expired)
    );
    let mut forged = signed.clone();
    forged.ticket.expires_at = 104;
    assert_eq!(
        forged.verify(realm(), target(), "/realm/version", 101),
        Err(TicketError::Signature)
    );
}

#[test]
fn debug_hides_signature() {
    let signed = signed(ticket(100, 105));
    let encoded = signed.encode().unwrap();
    let printed = format!("{signed:?}");
    assert!(!printed.contains(&encoded));
    assert!(!printed.contains("signature"));
}
