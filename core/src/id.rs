use crate::util::xor_distance_32;
use crate::{structs::RealmId, types::GroupId};
use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

pub type NodeId = iroh::PublicKey;

pub trait NodeIdExt {
    fn xor_distance(&self, other: &NodeId) -> [u8; 32];
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
        256
    }
}

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

    #[inline]
    pub fn from_data(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

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

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum TopicId {
    Realm(RealmId),
    Node(NodeId),
    Group(GroupId),
    Metadata(Ulid),
}

const PREFIX_REALM: u8 = b'r';
const PREFIX_NODE: u8 = b'n';
const PREFIX_GROUP: u8 = b'g';
const PREFIX_METADATA: u8 = b'm';

impl TopicId {
    #[inline]
    pub fn realm(id: RealmId) -> Self {
        Self::Realm(id)
    }

    #[inline]
    pub fn node(id: NodeId) -> Self {
        Self::Node(id)
    }

    #[inline]
    pub fn group(id: GroupId) -> Self {
        Self::Group(id)
    }

    #[inline]
    pub fn metadata(id: Ulid) -> Self {
        Self::Metadata(id)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Realm(realm_id) => {
                let mut buf = Vec::with_capacity(33);
                buf.push(PREFIX_REALM);
                buf.extend_from_slice(realm_id.as_bytes());
                buf
            }
            Self::Node(pubkey) => {
                let mut buf = Vec::with_capacity(33);
                buf.push(PREFIX_NODE);
                buf.extend_from_slice(pubkey.as_bytes());
                buf
            }
            Self::Group(group_id) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(PREFIX_GROUP);
                buf.extend_from_slice(&group_id.to_bytes());
                buf
            }
            Self::Metadata(document_id) => {
                let mut buf = Vec::with_capacity(17);
                buf.push(PREFIX_METADATA);
                buf.extend_from_slice(&document_id.to_bytes());
                buf
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        let prefix = bytes[0];
        let payload = &bytes[1..];

        match prefix {
            PREFIX_REALM => {
                if payload.len() != 32 {
                    return None;
                }
                let bytes: [u8; 32] = payload.try_into().ok()?;
                Some(Self::Realm(RealmId::from_bytes(bytes)))
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
                Some(Self::Group(GroupId::from_bytes(bytes)))
            }
            PREFIX_METADATA => {
                if payload.len() != 16 {
                    return None;
                }
                let bytes: [u8; 16] = payload.try_into().ok()?;
                Some(Self::Metadata(Ulid::from_bytes(bytes)))
            }
            _ => None,
        }
    }

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
            Self::Realm(id) => write!(f, "TopicId::Realm({id})"),
            Self::Node(id) => write!(f, "TopicId::Node({id})"),
            Self::Group(id) => write!(f, "TopicId::Group({id})"),
            Self::Metadata(id) => write!(f, "TopicId::Metadata({id})"),
        }
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Realm(id) => write!(f, "r:{id}"),
            Self::Node(id) => write!(f, "n:{id}"),
            Self::Group(id) => write!(f, "g:{id}"),
            Self::Metadata(id) => write!(f, "m:{id}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        let secret = iroh::SecretKey::from_bytes(&seed_bytes);
        secret.public()
    }

    #[test]
    fn test_node_id_xor_distance() {
        let a = make_node_id(1);
        let b = make_node_id(2);
        assert_eq!(a.xor_distance(&b), b.xor_distance(&a));
        assert_eq!(a.xor_distance(&a), [0x00; 32]);
    }

    #[test]
    fn test_bucket_index() {
        let a = make_node_id(1);
        let b = make_node_id(2);
        let c = make_node_id(3);

        assert_eq!(a.bucket_index(&a), 256);

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
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let topic = TopicId::realm(realm_id);
        let bytes = topic.to_bytes();
        let parsed = TopicId::from_bytes(&bytes).unwrap();
        assert_eq!(topic, parsed);
    }

    #[test]
    fn test_topic_id_group() {
        let topic = TopicId::group(GroupId::new());
        let bytes = topic.to_bytes();
        assert_eq!(bytes[0], PREFIX_GROUP);
        let parsed = TopicId::from_bytes(&bytes).unwrap();
        assert_eq!(topic, parsed);
    }

    #[test]
    fn test_topic_id_metadata() {
        let topic = TopicId::metadata(Ulid::from_bytes([9u8; 16]));
        let bytes = topic.to_bytes();
        assert_eq!(bytes[0], PREFIX_METADATA);
        let parsed = TopicId::from_bytes(&bytes).unwrap();
        assert_eq!(topic, parsed);
    }

    #[test]
    fn test_topic_id_display() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let topic = TopicId::realm(realm_id);
        let display = format!("{topic}");
        assert!(display.starts_with("r:"));
    }
}
