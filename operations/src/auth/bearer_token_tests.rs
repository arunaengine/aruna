//! Tests the bearer token checks as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::create_token::{CreateTokenConfig, mint_token};
use aruna_core::calls::ChildOutcome;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, SessionKind, SessionRef};
use aruna_core::structs::identity::realm::TokenRevocation;
use ed25519_dalek::SigningKey;

const FIXED_NOW: u64 = 1_800_000_000;

fn pubkey_b64(key: &SigningKey) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes())
}

#[test]
fn evicts_beyond_capacity() {
    let cache = IssuerKeyCache::with_limits(2, KEY_CACHE_TTL);
    for _ in 0..5 {
        cache
            .get_or_insert(&pubkey_b64(&generate_signing_key()))
            .unwrap();
    }
    assert_eq!(cache.len(), 2);
}

fn backdate_entry(cache: &IssuerKeyCache, pubkey: &str, age: Duration) -> Instant {
    let inserted_at = Instant::now().checked_sub(age).unwrap();
    cache.entries().peek_mut(pubkey).unwrap().inserted_at = inserted_at;
    inserted_at
}

fn entry_inserted_at(cache: &IssuerKeyCache, pubkey: &str) -> Instant {
    cache.entries().peek(pubkey).unwrap().inserted_at
}

#[test]
fn refreshes_expired_entries() {
    let ttl = Duration::from_secs(3600);
    let cache = IssuerKeyCache::with_limits(4, ttl);
    let pubkey = pubkey_b64(&generate_signing_key());
    cache.get_or_insert(&pubkey).unwrap();

    // Just inside the TTL: served from cache, entry untouched.
    let inside = backdate_entry(&cache, &pubkey, ttl - Duration::from_secs(60));
    cache.get_or_insert(&pubkey).unwrap();
    assert_eq!(entry_inserted_at(&cache, &pubkey), inside);

    // Just past the TTL: refreshed in place without accumulating.
    let expired = backdate_entry(&cache, &pubkey, ttl + Duration::from_secs(1));
    cache.get_or_insert(&pubkey).unwrap();
    assert!(entry_inserted_at(&cache, &pubkey) > expired);
    assert_eq!(cache.len(), 1);
}

#[test]
fn rejects_invalid_pubkey() {
    let cache = IssuerKeyCache::new();
    assert!(cache.get_or_insert("not-base64!!").is_err());
    assert!(cache.is_empty());
}

fn actor(realm_id: RealmId) -> Actor {
    Actor {
        node_id: iroh::SecretKey::from_bytes(&[9; 32]).public(),
        user_id: UserId::nil(realm_id),
        realm_id,
    }
}

fn config_row(config: &RealmConfigDocument) -> IoResult {
    let bytes = config.to_bytes(&actor(config.realm_id)).unwrap();
    IoResult::Values(vec![Some(bytes.into())])
}

fn absent() -> IoResult {
    IoResult::Values(vec![None])
}

fn revoked_answer(row: IoResult) -> Result<Next<bool>, ArunaBearerError> {
    let mut operation = TokenRevokedOperation::new(RealmId::from_bytes([7; 32]), "token-hash");
    operation.stamp(FIXED_NOW * 1000);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    operation.next(row)
}

#[test]
fn missing_config_denies() {
    assert!(matches!(revoked_answer(absent()), Ok(Next::Done(true))));
}

#[test]
fn unreadable_config_errors() {
    // An undecodable document is a failed read, not an absent one: it must
    // surface as unavailable instead of silently meaning "revoked".
    let garbage = IoResult::Values(vec![Some(vec![0xff; 8].into())]);
    assert!(matches!(
        revoked_answer(garbage),
        Err(ArunaBearerError::RevocationUnavailable)
    ));
}

#[test]
fn config_recovery_allows() {
    let config = RealmConfigDocument::default_for_realm(RealmId::from_bytes([7; 32]), Vec::new());
    assert!(matches!(
        revoked_answer(config_row(&config)),
        Ok(Next::Done(false))
    ));
}

fn grant(realm: RealmId, via: Option<UserId>, expires_at_secs: u64) -> TokenGrant {
    TokenGrant {
        token_hash: "hash".to_string(),
        issuer: realm,
        issued_at_secs: 1,
        expires_at_secs,
        via,
    }
}

#[test]
fn checks_saved_grant() {
    // A realm without its config denies, like a revoked token; an expired grant too.
    let realm = RealmId::from_bytes([1; 32]);
    let user = UserId::local(ulid::Ulid::from_parts(5, 5), realm);
    let mut live = GrantValidOperation::new(&grant(realm, None, u64::MAX), &user);
    live.stamp(FIXED_NOW * 1000);
    live.first().unwrap();
    assert!(matches!(live.next(absent()), Ok(Next::Done(false))));
    let config = RealmConfigDocument::new(realm, Vec::new(), 2);
    let mut valid = GrantValidOperation::new(&grant(realm, None, u64::MAX), &user);
    valid.stamp(FIXED_NOW * 1000);
    valid.first().unwrap();
    assert!(matches!(
        valid.next(config_row(&config)),
        Ok(Next::Done(true))
    ));
    let mut expired = GrantValidOperation::new(&grant(realm, None, 1), &user);
    expired.stamp(FIXED_NOW * 1000);
    assert!(matches!(expired.first(), Ok(Next::Done(false))));
}

#[test]
fn linked_grant_resolves() {
    // A grant through a linked login holds only while the link names its user.
    let realm = RealmId::from_bytes([1; 32]);
    let user = UserId::local(ulid::Ulid::from_parts(5, 5), realm);
    let via = UserId::new(ulid::Ulid::from_parts(6, 6), RealmId::from_bytes([2; 32]));
    let config = RealmConfigDocument::new(realm, Vec::new(), 2);
    for (owner, valid) in [(Some(user), true), (None, false)] {
        let mut operation = GrantValidOperation::new(&grant(realm, Some(via), u64::MAX), &user);
        operation.stamp(FIXED_NOW * 1000);
        operation.first().unwrap();
        let turn = operation.next(config_row(&config));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let answer: Result<Option<UserId>, ArunaBearerError> = Ok(owner);
        let done = operation.next(IoResult::Child(ChildOutcome::new(answer)));
        assert!(matches!(done, Ok(Next::Done(found)) if found == valid));
    }
}

fn lifetime_claims(iat: u64, exp: u64) -> TokenClaims {
    TokenClaims {
        sub: UserId::nil(RealmId::from_bytes([12; 32])).to_string(),
        iss: RealmId::from_bytes([12; 32]).to_base64(),
        iat,
        exp,
        jti: "token-id".to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    }
}

fn judged(claims: &TokenClaims) -> Result<(), ArunaBearerError> {
    let trusted = HashSet::from([RealmId::from_bytes([12; 32])]);
    validate_bearer_claims(&trusted, FIXED_NOW, claims)
}

#[test]
fn rejects_aged_token() {
    // An overlong token must stay rejected as it ages: the bound is the
    // signed lifetime, not the validity still remaining.
    let max = aruna_core::auth::MAX_TOKEN_LIFETIME;
    assert!(matches!(
        judged(&lifetime_claims(FIXED_NOW - max, FIXED_NOW + 600)),
        Err(ArunaBearerError::LifetimeTooLong)
    ));
    // A token signed within the bound still validates.
    judged(&lifetime_claims(FIXED_NOW, FIXED_NOW + 600)).unwrap();
}

#[test]
fn rejects_future_issuance() {
    let skewed = FIXED_NOW + aruna_core::auth::REVOCATION_GRACE_SECS + 60;
    assert!(matches!(
        judged(&lifetime_claims(skewed, skewed + 600)),
        Err(ArunaBearerError::LifetimeTooLong)
    ));
}

#[test]
fn rejects_expired_claims() {
    // Expiry is decided by the given clock, not by real time passing.
    assert!(matches!(
        judged(&lifetime_claims(FIXED_NOW - 1200, FIXED_NOW - 600)),
        Err(ArunaBearerError::Expired)
    ));
}

struct Minted {
    token: String,
    realm_id: RealmId,
    local: UserId,
    via: UserId,
    now: u64,
}

fn linked_token() -> Minted {
    let key = SigningKey::from_bytes(&[13; 32]);
    let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
    let local = UserId::local(ulid::Ulid::from_bytes([1; 16]), realm_id);
    let via = UserId::new(
        ulid::Ulid::from_bytes([2; 16]),
        RealmId::from_bytes([14; 32]),
    );
    let now = FIXED_NOW;
    let token = mint_token(&CreateTokenConfig {
        time: now,
        expiry: Some(now + 600),
        user_id: local,
        realm_id,
        node_capabilities: NodeCapabilities::management_node(key).unwrap(),
        session: Some(SessionRef {
            sid: ulid::Ulid::from_bytes([3; 16]).to_string(),
            kind: SessionKind::Federated,
            name: None,
            via: Some(via),
        }),
        restrictions: None,
    })
    .unwrap();
    Minted {
        token,
        realm_id,
        local,
        via,
        now,
    }
}

fn decoding(minted: &Minted, revocation: RevocationCheck) -> DecodeTokenOperation {
    let checks = TokenChecks {
        trust: IssuerTrust::Listed(HashSet::from([minted.realm_id])),
        revocation,
        linked_realm: Some(minted.realm_id),
        keys: None,
    };
    let mut operation = DecodeTokenOperation::new(&minted.token, checks);
    operation.stamp(minted.now * 1000);
    operation
}

fn owner_answer(owner: Option<UserId>) -> IoResult {
    let answer: Result<Option<UserId>, ArunaBearerError> = Ok(owner);
    IoResult::Child(ChildOutcome::new(answer))
}

#[test]
fn linked_login_checked() {
    // A linked session lives only while its link resolves to the token's user.
    let minted = linked_token();
    let mut linked = decoding(&minted, RevocationCheck::Skipped);
    assert!(matches!(linked.first(), Ok(Next::Call(IoCall::Child(_)))));
    let Ok(Next::Done(claims)) = linked.next(owner_answer(Some(minted.local))) else {
        panic!("expected the claims");
    };
    assert_eq!(claims.via, Some(minted.via.to_string()));
    let mut unlinked = decoding(&minted, RevocationCheck::Skipped);
    unlinked.first().unwrap();
    assert!(matches!(
        unlinked.next(owner_answer(None)),
        Err(ArunaBearerError::TokenRevoked)
    ));
}

#[test]
fn cutoff_denies_ids() {
    // A cutoff of the user or of the linked login ends the session.
    let minted = linked_token();
    for cut in [minted.via, minted.local] {
        let mut config = RealmConfigDocument::new(minted.realm_id, Vec::new(), 2);
        config.revoked_tokens.push(TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&cut),
            expires_at: aruna_core::auth::user_cutoff_expiry(minted.now + 1),
        });
        let mut operation = decoding(&minted, RevocationCheck::Realm);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        assert!(matches!(
            operation.next(config_row(&config)),
            Err(ArunaBearerError::TokenRevoked)
        ));
    }
}

#[test]
fn listed_hash_revoked() {
    let minted = linked_token();
    let hashes = BTreeSet::from([bearer_token_hash(&minted.token)]);
    let mut operation = decoding(&minted, RevocationCheck::Listed(hashes));
    assert!(matches!(
        operation.first(),
        Err(ArunaBearerError::TokenRevoked)
    ));
}

#[test]
fn foreign_link_refused() {
    // Only the realm this node serves can resolve a linked login.
    let minted = linked_token();
    let mut operation = decoding(&minted, RevocationCheck::Skipped);
    operation.checks.linked_realm = None;
    assert!(matches!(
        operation.first(),
        Err(ArunaBearerError::TokenRevoked)
    ));
}

#[test]
fn stored_trust_read() {
    // Without a readable list no realm is trusted.
    let minted = linked_token();
    let mut operation = decoding(&minted, RevocationCheck::Skipped);
    operation.checks.trust = IssuerTrust::Stored;
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert!(matches!(
        operation.next(absent()),
        Err(ArunaBearerError::RealmNotTrusted)
    ));
}

#[test]
fn debug_hides_token() {
    let minted = linked_token();
    let printed = format!("{:?}", decoding(&minted, RevocationCheck::Skipped));
    assert!(!printed.contains(&minted.token));
}
