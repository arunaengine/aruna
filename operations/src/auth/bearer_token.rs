//! Decodes and validates bearer tokens, checking revocation and caching issuer keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::ArunaBearerError;
use crate::document_repository::read_row;
use aruna_core::UserId;
use aruna_core::auth::{
    REALMS_LIST_KEY, REVOCATION_GRACE_SECS, bearer_token_hash, valid_token_lifetime,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, FEDERATION_KEYSPACE, USER_KEYSPACE};
use aruna_core::link::{alias_claims_key, alias_owner};
use aruna_core::operation::Operation;
use aruna_core::rows::value;
use aruna_core::structs::identity::auth::{AuthContext, TokenClaims};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::storage::backend_import::TokenGrant;
use aruna_core::types::Value;
use base64::Engine;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use lru::LruCache;
use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use tracing::warn;

impl From<IoError> for ArunaBearerError {
    fn from(_: IoError) -> Self {
        Self::RevocationUnavailable
    }
}

fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::Read(vec![read_row(&DocumentTarget::RealmConfig { realm_id })])
}

/// The realm config of a read; `None` when the realm has none here.
fn read_config(value: Option<Value>) -> Result<Option<RealmConfigDocument>, ArunaBearerError> {
    value
        .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
        .transpose()
        .map_err(|error| {
            warn!(error = %error, "Failed to decode realm config for token checks");
            ArunaBearerError::RevocationUnavailable
        })
}

fn now_secs(now_ms: u64) -> u64 {
    now_ms / 1000
}

/// Whether the realm's replicated revocation set denies this token hash. This is the realm-wide
/// authority; a realm without a config document denies, a failed read is unavailable.
#[derive(Debug, PartialEq)]
pub struct TokenRevokedOperation {
    realm_id: RealmId,
    token_hash: String,
    now_ms: u64,
}

impl TokenRevokedOperation {
    pub fn new(realm_id: RealmId, token_hash: &str) -> Self {
        Self {
            realm_id,
            token_hash: token_hash.to_string(),
            now_ms: 0,
        }
    }
}

impl Operation for TokenRevokedOperation {
    type Output = bool;
    type Error = ArunaBearerError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<bool>, ArunaBearerError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ArunaBearerError> {
        let revoked = read_config(value(result)?)?
            .is_none_or(|config| config.token_revoked(&self.token_hash, now_secs(self.now_ms)));
        Ok(Next::Done(revoked))
    }
}

/// The user's cutoff in the realm's replicated revocation set, in Unix seconds.
#[derive(Debug, PartialEq)]
pub struct UserCutoffOperation {
    realm_id: RealmId,
    user_id: UserId,
    now_ms: u64,
}

impl UserCutoffOperation {
    pub fn new(realm_id: RealmId, user_id: &UserId) -> Self {
        Self {
            realm_id,
            user_id: *user_id,
            now_ms: 0,
        }
    }
}

impl Operation for UserCutoffOperation {
    type Output = Option<u64>;
    type Error = ArunaBearerError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Option<u64>>, ArunaBearerError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, ArunaBearerError> {
        let cutoff = read_config(value(result)?)?
            .and_then(|config| config.user_cutoff(&self.user_id, now_secs(self.now_ms)));
        Ok(Next::Done(cutoff))
    }
}

/// Whether this realm's replicated record of the account marks it deactivated; an account
/// without a record here, such as a federated one, is not.
#[derive(Debug, PartialEq)]
pub struct AccountDeactivatedOperation {
    user_id: UserId,
}

impl AccountDeactivatedOperation {
    pub fn new(user_id: &UserId) -> Self {
        Self { user_id: *user_id }
    }
}

impl Operation for AccountDeactivatedOperation {
    type Output = bool;
    type Error = ArunaBearerError;

    fn first(&mut self) -> Result<Next<bool>, ArunaBearerError> {
        Ok(Next::Call(aruna_core::rows::read_row(
            USER_KEYSPACE,
            self.user_id.to_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ArunaBearerError> {
        let user = value(result)?
            .map(|bytes| User::from_bytes(&bytes))
            .transpose()
            .map_err(|_| ArunaBearerError::RevocationUnavailable)?;
        Ok(Next::Done(user.is_some_and(|user| user.is_deactivated())))
    }
}

/// The local account `via` is linked to: exactly one claim, by an active account that still
/// lists the link without a second login of that realm.
#[derive(Debug, PartialEq)]
struct LinkedOwnerOperation {
    via: UserId,
    owner: Option<UserId>,
}

impl LinkedOwnerOperation {
    fn new(via: UserId) -> Self {
        Self { via, owner: None }
    }
}

impl Operation for LinkedOwnerOperation {
    type Output = Option<UserId>;
    type Error = ArunaBearerError;

    fn first(&mut self) -> Result<Next<Option<UserId>>, ArunaBearerError> {
        Ok(Next::Call(aruna_core::rows::read_row(
            FEDERATION_KEYSPACE,
            alias_claims_key(&self.via),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<UserId>>, ArunaBearerError> {
        let row = value(result)?;
        let unreadable = |_| ArunaBearerError::RevocationUnavailable;
        let Some(owner) = self.owner else {
            let claims = row
                .map(|bytes| postcard::from_bytes::<BTreeSet<UserId>>(&bytes))
                .transpose()
                .map_err(|_| ArunaBearerError::RevocationUnavailable)?
                .unwrap_or_default();
            let Some(owner) = alias_owner(&claims) else {
                return Ok(Next::Done(None));
            };
            self.owner = Some(owner);
            return Ok(Next::Call(aruna_core::rows::read_row(
                USER_KEYSPACE,
                owner.to_bytes(),
            )));
        };
        let user = row
            .map(|bytes| User::from_bytes(&bytes))
            .transpose()
            .map_err(unreadable)?;
        let active =
            user.is_some_and(|user| !user.is_deactivated() && user.linked_login(&self.via));
        Ok(Next::Done(active.then_some(owner)))
    }
}

fn linked_owner(result: IoResult) -> Result<Option<UserId>, ArunaBearerError> {
    result.child::<Option<UserId>, ArunaBearerError>()?
}

/// Whether a token still authorizes `user_id`, as the request checks did: not expired, not
/// revoked, issued after any cutoff of the user or of the linked login, and that link still
/// resolving to the user. A failed read is reported as unavailable, never as a verdict.
#[derive(Debug, PartialEq)]
pub struct GrantValidOperation {
    grant: TokenGrant,
    user_id: UserId,
    now_ms: u64,
    via_valid: bool,
}

impl GrantValidOperation {
    pub fn new(grant: &TokenGrant, user_id: &UserId) -> Self {
        Self {
            grant: grant.clone(),
            user_id: *user_id,
            now_ms: 0,
            via_valid: false,
        }
    }
}

impl Operation for GrantValidOperation {
    type Output = bool;
    type Error = ArunaBearerError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<bool>, ArunaBearerError> {
        if now_secs(self.now_ms) > self.grant.expires_at_secs {
            return Ok(Next::Done(false));
        }
        Ok(Next::Call(config_call(self.grant.issuer)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ArunaBearerError> {
        if self.via_valid {
            let owner = linked_owner(result)?;
            return Ok(Next::Done(owner == Some(self.user_id)));
        }
        let Some(config) = read_config(value(result)?)? else {
            return Ok(Next::Done(false));
        };
        let now = now_secs(self.now_ms);
        let issued = self.grant.issued_at_secs;
        let cut = |user: &UserId| config.user_cutoff(user, now).is_some_and(|at| issued < at);
        if config.token_revoked(&self.grant.token_hash, now) || cut(&self.user_id) {
            return Ok(Next::Done(false));
        }
        let Some(via) = self.grant.via else {
            return Ok(Next::Done(true));
        };
        if cut(&via) {
            return Ok(Next::Done(false));
        }
        self.via_valid = true;
        Ok(Next::Call(IoCall::child(LinkedOwnerOperation::new(via))))
    }
}

/// Which realms' tokens this node accepts.
#[derive(Clone, Debug, PartialEq)]
pub enum IssuerTrust {
    Listed(HashSet<RealmId>),
    /// The trusted realm list stored on this node; an unreadable list trusts no realm.
    Stored,
}

/// How a token's revocation state is judged.
#[derive(Clone, Debug, PartialEq)]
pub enum RevocationCheck {
    /// The issuing realm's replicated config decides, with its user cutoffs.
    Realm,
    /// These token hashes are revoked; no cutoffs apply.
    Listed(BTreeSet<String>),
    /// Nothing counts as revoked, so a revocation can still read the token's claims.
    Skipped,
}

/// What a node checks a bearer token against.
#[derive(Clone, Debug, PartialEq)]
pub struct TokenChecks {
    pub trust: IssuerTrust,
    pub revocation: RevocationCheck,
    /// The realm whose linked logins this node can resolve.
    pub linked_realm: Option<RealmId>,
    pub keys: Option<Arc<IssuerKeyCache>>,
}

#[derive(PartialEq)]
enum DecodeStage {
    Trust,
    Config,
    Link,
}

/// Verifies a bearer token and answers its claims when it is still valid. The operation never
/// prints the token.
#[derive(PartialEq)]
pub struct DecodeTokenOperation {
    token: String,
    checks: TokenChecks,
    now_ms: u64,
    stage: DecodeStage,
    claims: Option<TokenClaims>,
}

impl std::fmt::Debug for DecodeTokenOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodeTokenOperation")
            .finish_non_exhaustive()
    }
}

type DecodeTurn = Result<Next<TokenClaims>, ArunaBearerError>;

impl DecodeTokenOperation {
    pub fn new(token: &str, checks: TokenChecks) -> Self {
        Self {
            token: token.to_string(),
            checks,
            now_ms: 0,
            stage: DecodeStage::Trust,
            claims: None,
        }
    }

    /// Checks the signature and the claims, then the revocation state.
    fn verify(&mut self, trusted: &HashSet<RealmId>) -> DecodeTurn {
        let unverified = insecure_decode::<TokenClaims>(&self.token)?.claims;
        let issuer = match (
            unverified.issuer_pubkey.as_deref(),
            unverified.delegation_signature.is_some(),
        ) {
            (Some(issuer_pubkey), true) => issuer_pubkey,
            _ => &unverified.iss,
        };
        // Only trusted realm or delegated issuers may populate the bounded cache;
        // untrusted issuers are verified with an ephemeral key that is discarded.
        let key = match (&self.checks.keys, issuer_trusted(trusted, &unverified)) {
            (Some(keys), Ok(())) => keys.get_or_insert(issuer)?,
            _ => bearer_decoding_key(issuer)?,
        };
        let validation = Validation::new(Algorithm::EdDSA);
        let claims = decode::<TokenClaims>(&self.token, &key, &validation)?.claims;
        validate_bearer_claims(trusted, now_secs(self.now_ms), &claims)?;
        // The issuing realm from the verified claims, so a foreign token is judged
        // by its origin realm's revocation set and not by the serving node's.
        let issuer_realm = issuer_realm(&claims)?;
        let turn = match &self.checks.revocation {
            RevocationCheck::Realm => {
                self.claims = Some(claims);
                self.stage = DecodeStage::Config;
                return Ok(Next::Call(config_call(issuer_realm)));
            }
            RevocationCheck::Listed(hashes) if hashes.contains(&bearer_token_hash(&self.token)) => {
                Err(ArunaBearerError::TokenRevoked)
            }
            _ => self.check_link(claims),
        };
        turn
    }

    /// Applies the issuing realm's revocations and the cutoffs of both ids.
    fn config_read(&mut self, config: Option<RealmConfigDocument>) -> DecodeTurn {
        let claims = self
            .claims
            .take()
            .ok_or(ArunaBearerError::RevocationUnavailable)?;
        let config = config.ok_or(ArunaBearerError::TokenRevoked)?;
        let now = now_secs(self.now_ms);
        if config.token_revoked(&bearer_token_hash(&self.token), now) {
            return Err(ArunaBearerError::TokenRevoked);
        }
        let cut = |user: &UserId| {
            (config.user_cutoff(user, now)).is_some_and(|cutoff| claims.iat < cutoff)
        };
        let via = claims.via.as_deref().map(UserId::from_string).transpose()?;
        if cut(&UserId::from_string(&claims.sub)?) || via.as_ref().is_some_and(cut) {
            return Err(ArunaBearerError::TokenRevoked);
        }
        self.check_link(claims)
    }

    /// A linked login lives only while its link still resolves to the token's user.
    fn check_link(&mut self, claims: TokenClaims) -> DecodeTurn {
        UserId::from_string(&claims.sub)?;
        let Some(via) = claims.via.as_deref() else {
            return Ok(Next::Done(claims));
        };
        let via = UserId::from_string(via)?;
        if self.checks.linked_realm != Some(issuer_realm(&claims)?) {
            return Err(ArunaBearerError::TokenRevoked);
        }
        self.claims = Some(claims);
        self.stage = DecodeStage::Link;
        Ok(Next::Call(IoCall::child(LinkedOwnerOperation::new(via))))
    }
}

impl Operation for DecodeTokenOperation {
    type Output = TokenClaims;
    type Error = ArunaBearerError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> DecodeTurn {
        match self.checks.trust.clone() {
            IssuerTrust::Listed(trusted) => self.verify(&trusted),
            IssuerTrust::Stored => Ok(Next::Call(aruna_core::rows::read_row(
                API_STATE_KEYSPACE,
                REALMS_LIST_KEY,
            ))),
        }
    }

    fn next(&mut self, result: IoResult) -> DecodeTurn {
        match self.stage {
            DecodeStage::Trust => {
                let trusted = stored_realms(value(result));
                self.verify(&trusted)
            }
            DecodeStage::Config => self.config_read(read_config(value(result)?)?),
            DecodeStage::Link => {
                let owner = linked_owner(result)?;
                let claims = self.claims.take().ok_or(ArunaBearerError::TokenRevoked)?;
                match owner == Some(UserId::from_string(&claims.sub)?) {
                    true => Ok(Next::Done(claims)),
                    false => Err(ArunaBearerError::TokenRevoked),
                }
            }
        }
    }
}

/// The stored trusted realm list; a failed read or an undecodable list trusts no realm.
fn stored_realms(row: Result<Option<Value>, IoError>) -> HashSet<RealmId> {
    let realms = row.map_err(|error| error.to_string()).and_then(|row| {
        row.map(|bytes| postcard::from_bytes(&bytes).map_err(|error| error.to_string()))
            .transpose()
    });
    match realms {
        Ok(realms) => realms.unwrap_or_default(),
        Err(error) => {
            warn!(error = %error, "Failed to read metadata trusted realms state");
            HashSet::new()
        }
    }
}

fn issuer_realm(claims: &TokenClaims) -> Result<RealmId, ArunaBearerError> {
    RealmId::from_base64(&claims.iss).map_err(|_| ArunaBearerError::InvalidIssuerKey)
}

/// The auth context of validated claims.
pub fn claims_auth(claims: TokenClaims) -> Result<AuthContext, ArunaBearerError> {
    Ok(claims.try_into()?)
}

fn issuer_trusted(
    trusted: &HashSet<RealmId>,
    claims: &TokenClaims,
) -> Result<(), ArunaBearerError> {
    if !trusted.contains(&issuer_realm(claims)?) {
        return Err(ArunaBearerError::RealmNotTrusted);
    }
    match (&claims.delegation_signature, &claims.issuer_pubkey) {
        (Some(delegation_signature), Some(issuer_pubkey)) => {
            verify_realm_delegation(&claims.iss, issuer_pubkey, delegation_signature)
        }
        (None, None) => Ok(()),
        (_, _) => Err(ArunaBearerError::InvalidServerToken),
    }
}

/// Judges verified claims at `now`: their lifetime, and that a trusted realm issued them.
pub fn validate_bearer_claims(
    trusted: &HashSet<RealmId>,
    now: u64,
    claims: &TokenClaims,
) -> Result<(), ArunaBearerError> {
    if now > claims.exp {
        return Err(ArunaBearerError::Expired);
    }
    // Judge the signed lifetime so ageing and a future `iat` cannot evade the revocation bound.
    if !valid_token_lifetime(claims.iat, claims.exp)
        || claims.iat.saturating_sub(now) > REVOCATION_GRACE_SECS
    {
        return Err(ArunaBearerError::LifetimeTooLong);
    }
    issuer_trusted(trusted, claims)
}

fn verify_realm_delegation(
    realm_iss: &str,
    issuer_pubkey: &str,
    delegation_signature: &str,
) -> Result<(), ArunaBearerError> {
    let realm_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(realm_iss)?;
    let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
    let signature = Signature::from_str(delegation_signature)?;
    realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
    Ok(())
}

pub fn bearer_decoding_key(issuer_pubkey: &str) -> Result<DecodingKey, ArunaBearerError> {
    let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(issuer_pubkey)?
        .try_into()
        .map_err(|_| ArunaBearerError::InvalidIssuerKey)?;
    let public_key = VerifyingKey::from_bytes(&issuer_pubkey)?;
    let public_key_pem = public_key.to_public_key_pem(LineEnding::default())?;
    Ok(DecodingKey::from_ed_pem(public_key_pem.as_bytes())?)
}

/// Maximum number of issuer decoding keys retained in the process cache.
pub const KEY_CACHE_CAPACITY: usize = 1024;
/// Time an issuer decoding key is retained before it is refreshed.
pub const KEY_CACHE_TTL: Duration = Duration::from_secs(3600);

/// Bounded, TTL + LRU cache of issuer decoding keys keyed by base64 public key.
pub struct IssuerKeyCache {
    entries: Mutex<LruCache<String, CachedIssuerKey>>,
    ttl: Duration,
}

#[derive(Clone)]
struct CachedIssuerKey {
    key: DecodingKey,
    inserted_at: Instant,
}

impl std::fmt::Debug for IssuerKeyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuerKeyCache").finish_non_exhaustive()
    }
}

/// Two caches are the same only when they are one cache.
impl PartialEq for IssuerKeyCache {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Default for IssuerKeyCache {
    fn default() -> Self {
        Self::new()
    }
}

impl IssuerKeyCache {
    pub fn new() -> Self {
        Self::with_limits(KEY_CACHE_CAPACITY, KEY_CACHE_TTL)
    }

    pub fn with_limits(capacity: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
            ttl,
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, LruCache<String, CachedIssuerKey>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn get_or_insert(&self, issuer_pubkey: &str) -> Result<DecodingKey, ArunaBearerError> {
        let mut entries = self.entries();
        if let Some(entry) = entries.get(issuer_pubkey).cloned() {
            if entry.inserted_at.elapsed() < self.ttl {
                return Ok(entry.key);
            }
            entries.pop(issuer_pubkey);
        }
        let key = bearer_decoding_key(issuer_pubkey)?;
        entries.put(
            issuer_pubkey.to_string(),
            CachedIssuerKey {
                key: key.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(key)
    }

    pub fn len(&self) -> usize {
        self.entries().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries().is_empty()
    }
}

#[cfg(test)]
#[path = "bearer_token_tests.rs"]
mod tests;
