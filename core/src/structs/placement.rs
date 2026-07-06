use crate::NodeId;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ulid::Ulid;

pub const DEFAULT_LOCATION: &str = "default";
pub const DEFAULT_NODE_WEIGHT: u32 = 100;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodePlacementEntry {
    pub node_id: NodeId,
    /// Empty string ⇒ [`DEFAULT_LOCATION`].
    pub location: String,
    /// Weight 0 ⇒ the node is never selected.
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
    pub label_overrides: BTreeMap<String, String>,
}

impl NodePlacementEntry {
    pub fn effective_location(&self) -> &str {
        if self.location.is_empty() {
            DEFAULT_LOCATION
        } else {
            &self.location
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementStrategy {
    pub strategy_id: Ulid,
    pub name: String,
    /// `None` ⇒ all sync-eligible nodes.
    pub replica_count: Option<u32>,
    pub distinct_locations: bool,
    pub affinity: Vec<AffinityRule>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AffinityRule {
    pub matcher: LabelMatch,
    pub effect: AffinityEffect,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct LabelMatch {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum AffinityEffect {
    Filter,
    Multiply { permille: u32 },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementOverride {
    pub subject: Vec<u8>,
    pub pinned: Vec<NodeId>,
    pub excluded: Vec<NodeId>,
    pub strategy_id: Option<Ulid>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DocumentClass {
    Admin,
    Group,
    User,
    Metadata,
    MetadataRegistry,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum BindingScope {
    Realm,
    Group(GroupId),
    Class(DocumentClass),
    MetadataPathPrefix(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct StrategyBinding {
    pub scope: BindingScope,
    pub strategy_id: Ulid,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementRef {
    pub strategy_id: Ulid,
    pub epoch: u64,
}

impl PlacementRef {
    /// Zero-valued reference used when no strategy governs a change yet (early
    /// bootstrap / generic re-announce). The single named fallback so no
    /// ad-hoc `PlacementRef` literals scatter across producers.
    pub const NIL: PlacementRef = PlacementRef {
        strategy_id: Ulid::nil(),
        epoch: 0,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn effective_location_falls_back_to_default() {
        let mut entry = NodePlacementEntry {
            node_id: node(1),
            location: String::new(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            label_overrides: BTreeMap::new(),
        };
        assert_eq!(entry.effective_location(), DEFAULT_LOCATION);

        entry.location = "eu-west".to_string();
        assert_eq!(entry.effective_location(), "eu-west");
    }

    #[test]
    fn placement_entry_round_trips() {
        let entry = NodePlacementEntry {
            node_id: node(2),
            location: "eu-west".to_string(),
            weight: 250,
            full: true,
            draining: false,
            label_overrides: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        };
        let bytes = postcard::to_allocvec(&entry).unwrap();
        assert_eq!(
            postcard::from_bytes::<NodePlacementEntry>(&bytes).unwrap(),
            entry
        );
    }

    #[test]
    fn placement_strategy_round_trips() {
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([3u8; 16]),
            name: "default".to_string(),
            replica_count: Some(3),
            distinct_locations: true,
            affinity: vec![
                AffinityRule {
                    matcher: LabelMatch {
                        key: "aruna.io/kind".to_string(),
                        value: "Server".to_string(),
                    },
                    effect: AffinityEffect::Filter,
                },
                AffinityRule {
                    matcher: LabelMatch {
                        key: "tier".to_string(),
                        value: "hot".to_string(),
                    },
                    effect: AffinityEffect::Multiply { permille: 1500 },
                },
            ],
        };
        let bytes = postcard::to_allocvec(&strategy).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementStrategy>(&bytes).unwrap(),
            strategy
        );
    }

    #[test]
    fn placement_override_round_trips() {
        let over = PlacementOverride {
            subject: b"document-subject".to_vec(),
            pinned: vec![node(4)],
            excluded: vec![node(5)],
            strategy_id: Some(Ulid::from_bytes([6u8; 16])),
        };
        let bytes = postcard::to_allocvec(&over).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementOverride>(&bytes).unwrap(),
            over
        );
    }

    #[test]
    fn strategy_binding_round_trips() {
        for scope in [
            BindingScope::Realm,
            BindingScope::Group(Ulid::from_bytes([7u8; 16])),
            BindingScope::Class(DocumentClass::MetadataRegistry),
            BindingScope::MetadataPathPrefix("/datasets".to_string()),
        ] {
            let binding = StrategyBinding {
                scope,
                strategy_id: Ulid::from_bytes([8u8; 16]),
            };
            let bytes = postcard::to_allocvec(&binding).unwrap();
            assert_eq!(
                postcard::from_bytes::<StrategyBinding>(&bytes).unwrap(),
                binding
            );
        }
    }

    #[test]
    fn placement_ref_round_trips() {
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            epoch: 0,
        };
        let bytes = postcard::to_allocvec(&placement).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementRef>(&bytes).unwrap(),
            placement
        );
    }
}
