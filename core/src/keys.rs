use ed25519_dalek::SigningKey;

use crate::id::{DhtKeyId, NodeId};
use crate::structs::RealmId;

/// Fresh Ed25519 signing key from operating-system randomness.
///
/// `SigningKey::generate` needs an infallible `rand_core` generator, and no
/// `OsRng` implements that trait, so the entropy is drawn directly. A failing
/// system generator is not recoverable.
#[must_use]
pub fn generate_signing_key() -> SigningKey {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("operating system random number generator failed");
    SigningKey::from_bytes(&bytes)
}

/// Derive a DHT key from arbitrary bytes using BLAKE3.
#[must_use]
#[inline]
pub fn dht_key_from_bytes(input: &[u8]) -> DhtKeyId {
    DhtKeyId::from_data(input)
}

/// Domain-separated key derivation using BLAKE3 keyed hash.
#[inline]
fn derive_key_with_domain(domain: &[u8], input: &[u8]) -> [u8; 32] {
    let domain_key = blake3::hash(domain);
    let mut hasher = blake3::Hasher::new_keyed(domain_key.as_bytes());
    hasher.update(input);
    *hasher.finalize().as_bytes()
}

/// Derive a DHT key with domain separation.
#[must_use]
#[inline]
pub fn dht_key_from_domain(domain: &[u8], input: &[u8]) -> DhtKeyId {
    DhtKeyId::from_bytes(derive_key_with_domain(domain, input))
}

/// Domain-separated storage key for an OIDC identity. Keeps the issuer and
/// subject out of the plaintext keyspace while staying collision resistant.
#[must_use]
#[inline]
pub fn oidc_subject_key(issuer: &str, subject_id: &str) -> [u8; 32] {
    let mut input = Vec::with_capacity(issuer.len() + subject_id.len() + 1);
    input.extend_from_slice(issuer.as_bytes());
    input.push(0);
    input.extend_from_slice(subject_id.as_bytes());
    derive_key_with_domain(b"oidc-subject-v1", &input)
}

/// Derive a DHT key for active realm node presence announcements.
#[must_use]
#[inline]
pub fn realm_presence_key(realm_id: &RealmId) -> DhtKeyId {
    dht_key_from_domain(b"realm-presence", realm_id.as_bytes())
}

/// Derive a DHT key for a node's realm-scoped endpoint announcement.
#[must_use]
#[inline]
pub fn realm_endpoint_key(realm_id: &RealmId, node_id: &NodeId) -> DhtKeyId {
    let mut input = Vec::with_capacity(64);
    input.extend_from_slice(realm_id.as_bytes());
    input.extend_from_slice(node_id.as_bytes());
    dht_key_from_domain(b"realm-endpoint-v1", &input)
}
