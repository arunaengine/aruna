// core/src/id.rs
use crate::util::xor_distance_32;
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// NodeId is simply an iroh public key - no wrapper needed
pub type NodeId = iroh::PublicKey;

/// Extension trait for NodeId (iroh::PublicKey) to add Kademlia routing methods
pub trait NodeIdExt {
    /// XOR distance to another NodeId (for Kademlia routing)
    fn xor_distance(&self, other: &NodeId) -> [u8; 32];

    /// Returns the index of the first differing bit (0-255), or 256 if equal.
    /// Used to determine which k-bucket a node belongs to.
    fn bucket_index(&self, other: &NodeId) -> usize;
}

impl NodeIdExt for NodeId {
    #[inline]
    fn xor_distance(&self, other: &NodeId) -> [u8; 32] {
        xor_distance_32(self.as_bytes(), other.as_bytes())
    }

    #[inline]
    fn bucket_index(&self, other: &NodeId) -> usize {
        let distance = self.xor_distance(other);
        for (byte_idx, &byte) in distance.iter().enumerate() {
            if byte != 0 {
                return byte_idx * 8 + byte.leading_zeros() as usize;
            }
        }
        256 // Equal nodes
    }
}

/// A 32-byte DHT key for storing/retrieving values.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DhtKeyId([u8; 32]);

impl DhtKeyId {
    #[inline]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash arbitrary data to create a DhtKeyId
    #[inline]
    pub fn from_data(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    /// XOR distance to a NodeId (for finding closest nodes)
    #[inline]
    pub fn xor_distance_to_node(&self, node: &NodeId) -> [u8; 32] {
        xor_distance_32(self.as_bytes(), node.as_bytes())
    }
}

impl fmt::Debug for DhtKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DhtKeyId({})", hex::encode(&self.0[..8]))
    }
}

impl fmt::Display for DhtKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

impl From<[u8; 32]> for DhtKeyId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Topic identifier for gossip pub/sub with semantic prefixes.
/// Format: prefix byte + payload, hashed to 32 bytes for network use.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum TopicId {
    /// Realm-scoped topic (prefix 'r')
    Realm(Ulid),
    /// Node-specific topic (prefix 'n')
    Node(NodeId),
    /// Group-scoped topic (prefix 'g')
    Group(Ulid),
    /// Metadata document topic (prefix 'm')
    MetadataDocument(Ulid),
    /// Generic Automerge document topic (prefix 'a')
    AutomergeDocument(DhtKeyId),
}

/// Prefix bytes for TopicId variants
const PREFIX_REALM: u8 = b'r';
const PREFIX_NODE: u8 = b'n';
const PREFIX_GROUP: u8 = b'g';
const PREFIX_METADATA_DOCUMENT: u8 = b'm';
const PREFIX_AUTOMERGE_DOCUMENT: u8 = b'a';

impl TopicId {
    /// Create a realm-scoped topic
    #[inline]
    pub fn realm(id: Ulid) -> Self {
        Self::Realm(id)
    }

    /// Create a node-specific topic
    #[inline]
    pub fn node(id: NodeId) -> Self {
        Self::Node(id)
    }

    /// Create a group-scoped topic
    #[inline]
    pub fn group(id: Ulid) -> Self {
        Self::Group(id)
    }

    /// Create a metadata document topic
    #[inline]
    pub fn metadata_document(id: Ulid) -> Self {
        Self::MetadataDocument(id)
    }

    /// Create a generic automerge document topic
    #[inline]
    pub fn automerge_document(id: DhtKeyId) -> Self {
        Self::AutomergeDocument(id)
    }

    /// Serialize to bytes (prefix + payload)
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Realm(ulid) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(PREFIX_REALM);
                buf.extend_from_slice(&ulid.to_bytes());
                buf
            }
            Self::Node(pubkey) => {
                let mut buf = Vec::with_capacity(33);
                buf.push(PREFIX_NODE);
                buf.extend_from_slice(pubkey.as_bytes());
                buf
            }
            Self::Group(ulid) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(PREFIX_GROUP);
                buf.extend_from_slice(&ulid.to_bytes());
                buf
            }
            Self::MetadataDocument(ulid) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(PREFIX_METADATA_DOCUMENT);
                buf.extend_from_slice(&ulid.to_bytes());
                buf
            }
            Self::AutomergeDocument(id) => {
                let mut buf = Vec::with_capacity(33);
                buf.push(PREFIX_AUTOMERGE_DOCUMENT);
                buf.extend_from_slice(id.as_bytes());
                buf
            }
        }
    }

    /// Deserialize from bytes (prefix + payload)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        let prefix = bytes[0];
        let payload = &bytes[1..];

        match prefix {
            PREFIX_REALM => {
                if payload.len() != 16 {
                    return None;
                }
                let bytes: [u8; 16] = payload.try_into().ok()?;
                Some(Self::Realm(Ulid::from_bytes(bytes)))
            }
            PREFIX_NODE => {
                if payload.len() != 32 {
                    return None;
                }
                let bytes: [u8; 32] = payload.try_into().ok()?;
                Some(Self::Node(NodeId::from_bytes(&bytes).ok()?))
            }
            PREFIX_GROUP => {
                if payload.len() != 16 {
                    return None;
                }
                let bytes: [u8; 16] = payload.try_into().ok()?;
                Some(Self::Group(Ulid::from_bytes(bytes)))
            }
            PREFIX_METADATA_DOCUMENT => {
                if payload.len() != 16 {
                    return None;
                }
                let bytes: [u8; 16] = payload.try_into().ok()?;
                Some(Self::MetadataDocument(Ulid::from_bytes(bytes)))
            }
            PREFIX_AUTOMERGE_DOCUMENT => {
                if payload.len() != 32 {
                    return None;
                }
                let bytes: [u8; 32] = payload.try_into().ok()?;
                Some(Self::AutomergeDocument(DhtKeyId::from_bytes(bytes)))
            }
            _ => None,
        }
    }

    /// Convert to iroh-gossip TopicId (hashes the serialized form to 32 bytes)
    #[inline]
    pub fn to_iroh_topic(&self) -> iroh_gossip::TopicId {
        let bytes = self.to_bytes();
        let hash = blake3::hash(&bytes);
        (*hash.as_bytes()).into()
    }
}

impl fmt::Debug for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Realm(id) => write!(f, "TopicId::Realm({})", id),
            Self::Node(id) => write!(f, "TopicId::Node({})", id),
            Self::Group(id) => write!(f, "TopicId::Group({})", id),
            Self::MetadataDocument(id) => write!(f, "TopicId::MetadataDocument({})", id),
            Self::AutomergeDocument(id) => write!(f, "TopicId::AutomergeDocument({})", id),
        }
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Realm(id) => write!(f, "r:{}", id),
            Self::Node(id) => write!(f, "n:{}", id),
            Self::Group(id) => write!(f, "g:{}", id),
            Self::MetadataDocument(id) => write!(f, "m:{}", id),
            Self::AutomergeDocument(id) => write!(f, "a:{}", id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(seed: u8) -> NodeId {
        // Generate deterministic keys from seed
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        let secret = iroh::SecretKey::from_bytes(&seed_bytes);
        secret.public()
    }

    #[test]
    fn test_node_id_xor_distance() {
        let a = make_node_id(1);
        let b = make_node_id(2);
        // XOR distance should be symmetric
        assert_eq!(a.xor_distance(&b), b.xor_distance(&a));
        // XOR with self should be zero
        assert_eq!(a.xor_distance(&a), [0x00; 32]);
    }

    #[test]
    fn test_bucket_index() {
        let a = make_node_id(1);
        let b = make_node_id(2);
        let c = make_node_id(3);

        // Different nodes should have different bucket indices (usually)
        // Equal nodes have index 256
        assert_eq!(a.bucket_index(&a), 256);

        // Different nodes should have index < 256
        let idx_ab = a.bucket_index(&b);
        let idx_ac = a.bucket_index(&c);
        assert!(idx_ab < 256);
        assert!(idx_ac < 256);
    }

    #[test]
    fn test_dht_key_from_data() {
        let key1 = DhtKeyId::from_data(b"hello");
        let key2 = DhtKeyId::from_data(b"hello");
        let key3 = DhtKeyId::from_data(b"world");
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_topic_id_roundtrip() {
        let realm_id = Ulid::new();
        let topic = TopicId::realm(realm_id);
        let bytes = topic.to_bytes();
        let parsed = TopicId::from_bytes(&bytes).unwrap();
        assert_eq!(topic, parsed);
    }

    #[test]
    fn test_topic_id_group() {
        let topic = TopicId::group(Ulid::new());
        let bytes = topic.to_bytes();
        assert_eq!(bytes[0], PREFIX_GROUP);
        let parsed = TopicId::from_bytes(&bytes).unwrap();
        assert_eq!(topic, parsed);
    }

    #[test]
    fn test_topic_id_display() {
        let realm_id = Ulid::new();
        let topic = TopicId::realm(realm_id);
        let display = format!("{}", topic);
        assert!(display.starts_with("r:"));
    }
}
