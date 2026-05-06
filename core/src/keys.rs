use crate::id::{DhtKeyId, TopicId};
use crate::structs::RealmId;

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

/// Derive a DHT key for finding gossip peers for a topic.
#[must_use]
#[inline]
pub fn gossip_peer_key(topic: &TopicId) -> DhtKeyId {
    dht_key_from_domain(b"gossip", &topic.to_bytes())
}

/// Derive a DHT key for active realm node presence announcements.
#[must_use]
#[inline]
pub fn realm_presence_key(realm_id: &RealmId) -> DhtKeyId {
    dht_key_from_domain(b"realm-presence", realm_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn test_gossip_peer_key() {
        let topic = TopicId::group(Ulid::new());
        let key1 = gossip_peer_key(&topic);
        let key2 = gossip_peer_key(&topic);
        assert_eq!(key1, key2);
    }
}
