use crate::NodeId;
use crate::structs::RealmId;
use crate::structured_id::PlacementHandle;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;
use ulid::Ulid;

pub const DEFAULT_LOCATION: &str = "default";
pub const DEFAULT_NODE_WEIGHT: u32 = 100;
/// Default per-strategy shard fan-out. Power of two so `shard_for_subject`
/// can mask with `shard_count - 1`.
pub const DEFAULT_SHARD_COUNT: u32 = 64;
/// Maximum per-strategy shard fan-out accepted from placement config. Aliased
/// to the id codec's 12-bit bucket cap so the two can never drift: a strategy
/// whose `shard_count` exceeded the bucket field would mint ids whose bucket
/// silently truncates.
pub const MAX_PLACEMENT_SHARD_COUNT: u32 = crate::structured_id::MAX_BUCKET_COUNT as u32;
/// Upper bound for a configurable node weight; onboarding/config inputs clamp
/// present values into `1..=MAX_NODE_WEIGHT`.
pub const MAX_NODE_WEIGHT: u32 = 10_000;
/// Maximum accepted placement location length (bytes, after trim).
pub const MAX_NODE_LOCATION_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NodePlacementInputError {
    #[error("placement location must be at most {MAX_NODE_LOCATION_LEN} characters")]
    LocationTooLong,
}

/// Normalizes onboarding/config-sourced placement inputs: trims the location
/// (empty-after-trim ⇒ unset), rejects locations longer than
/// [`MAX_NODE_LOCATION_LEN`], clamps a present weight into `1..=MAX_NODE_WEIGHT`,
/// and defaults an absent weight to [`DEFAULT_NODE_WEIGHT`].
pub fn normalize_node_placement_input(
    location: Option<&str>,
    weight: Option<u32>,
) -> Result<(String, u32), NodePlacementInputError> {
    let location = match location {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.len() > MAX_NODE_LOCATION_LEN {
                return Err(NodePlacementInputError::LocationTooLong);
            }
            trimmed.to_string()
        }
        None => String::new(),
    };
    let weight = weight
        .map(|weight| weight.clamp(1, MAX_NODE_WEIGHT))
        .unwrap_or(DEFAULT_NODE_WEIGHT);
    Ok((location, weight))
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodePlacementEntry {
    pub node_id: NodeId,
    /// Empty string ⇒ [`DEFAULT_LOCATION`].
    pub location: String,
    /// Weight 0 ⇒ the node is never selected.
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
    pub labels: BTreeMap<String, String>,
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
    /// Number of sync shards subjects hash into. Power of two so the topic
    /// derivation is a pure mask; validated on upsert.
    pub shard_count: u32,
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    pub shard: u32,
}

impl PlacementRef {
    /// Zero-valued reference used when no strategy governs a change yet (early
    /// bootstrap / generic re-announce). The single named fallback so no
    /// ad-hoc `PlacementRef` literals scatter across producers.
    pub const NIL: PlacementRef = PlacementRef {
        strategy_id: Ulid::nil(),
        epoch: 0,
        shard: 0,
    };
}

/// Shard a subject hashes into for `shard_count` shards. Blake3 of a domain
/// tag concatenated with the subject, masked into `0..shard_count`. All
/// records of one logical document share a subject (see `subject_bytes`) and so
/// land in one shard.
pub fn shard_for_subject(subject: &[u8], shard_count: u32) -> u32 {
    debug_assert!(shard_count.is_power_of_two());
    let mut input = b"aruna-shard-v1".to_vec();
    input.extend_from_slice(subject);
    let hash = blake3::hash(&input);
    let mut head = [0u8; 4];
    head.copy_from_slice(&hash.as_bytes()[..4]);
    u32::from_be_bytes(head) & (shard_count - 1)
}

/// Discriminant of a placement binding's scope (spec 6.3.4).
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PlacementScopeKind {
    Realm,
    Group,
}

/// A placement binding's scope: the `scope_kind`/`scope_id` pair of the spec
/// record unified into one sum type so an impossible (kind, id) combination is
/// unrepresentable. `JobControl` bindings are realm-scoped; `GroupBulk`
/// bindings name the group (spec 6.3.4).
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PlacementScope {
    Realm(RealmId),
    Group(GroupId),
}

impl PlacementScope {
    pub fn kind(&self) -> PlacementScopeKind {
        match self {
            PlacementScope::Realm(_) => PlacementScopeKind::Realm,
            PlacementScope::Group(_) => PlacementScopeKind::Group,
        }
    }
}

/// The immutable identity a placement handle maps to
/// (`scope_kind, scope_id, document_class, strategy_id`). This is exactly the
/// value compared for conflict/alias detection: it carries no handle, no
/// provenance, no bucket, and no holder ids (REQ-META-PLACEMENT-BINDING-001).
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BindingTuple {
    pub scope: PlacementScope,
    pub document_class: DocumentClass,
    pub strategy_id: Ulid,
}

/// An immutable, append-only Placement Binding record
/// (REQ-META-PLACEMENT-BINDING-001/002). It maps one handle to its base
/// placement tuple plus allocation provenance. It MUST NOT carry a bucket (the
/// bucket travels in the id) or any holder node ids.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementBinding {
    pub handle: PlacementHandle,
    pub scope: PlacementScope,
    pub document_class: DocumentClass,
    pub strategy_id: Ulid,
    pub allocator_range_id: Option<Ulid>,
    pub allocated_by: Option<NodeId>,
    pub allocated_at_ms: Option<u64>,
}

impl PlacementBinding {
    /// The identity tuple used for conflict/alias detection; allocation
    /// provenance is deliberately excluded.
    pub fn tuple(&self) -> BindingTuple {
        BindingTuple {
            scope: self.scope,
            document_class: self.document_class,
            strategy_id: self.strategy_id,
        }
    }
}

/// First allocatable handle: handle zero is reserved by the id codec, so every
/// grant starts at one.
pub const FIRST_HANDLE: u32 = 1;
/// Exclusive upper bound of the 20-bit handle space (one past the highest
/// allocatable handle).
pub const HANDLE_SPACE_END: u32 = crate::structured_id::MAX_PLACEMENT_HANDLE + 1;
/// Handles carved into each coordinator grant. 1024 keeps ~1023 disjoint ranges
/// across the 20-bit space (so ~1000 nodes can each be onboarded with their own
/// range) while still handing every grant 1024 distinct
/// `(scope, class, strategy)` handles — far above the number of distinct scopes
/// a single node originates in practice (one binding is reused by every document
/// and bucket of a scope).
pub const HANDLE_RANGE_SIZE: u32 = 1024;

/// A durable, non-overlapping slice of the 20-bit handle space granted to one
/// node by a coordinator. The owner mints handles from `[start, end)` offline
/// (DEC-ONBOARD); `end` is exclusive. Two grants whose intervals intersect are a
/// fail-closed conflict resolved in the derived [`HandleRangeDirectory`].
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HandleRange {
    pub range_id: Ulid,
    pub owner: NodeId,
    /// Inclusive lower bound.
    pub start: u32,
    /// Exclusive upper bound.
    pub end: u32,
}

impl HandleRange {
    pub fn contains(&self, handle: u32) -> bool {
        self.start <= handle && handle < self.end
    }

    /// Half-open interval intersection (both ends exclusive of the other's).
    pub fn overlaps(&self, other: &HandleRange) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Number of handles the range covers.
    pub fn len(&self) -> u32 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// A range is well-formed when it is a non-empty sub-interval of the
    /// allocatable handle space (`[FIRST_HANDLE, HANDLE_SPACE_END)`).
    pub fn is_well_formed(&self) -> bool {
        self.start >= FIRST_HANDLE && self.start < self.end && self.end <= HANDLE_SPACE_END
    }
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
            labels: BTreeMap::new(),
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
            labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
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
            shard_count: 64,
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
    fn normalize_placement_input_clamps_and_validates() {
        assert_eq!(
            normalize_node_placement_input(None, None).unwrap(),
            (String::new(), DEFAULT_NODE_WEIGHT)
        );
        assert_eq!(normalize_node_placement_input(None, Some(0)).unwrap().1, 1);
        assert_eq!(
            normalize_node_placement_input(None, Some(50_000))
                .unwrap()
                .1,
            MAX_NODE_WEIGHT
        );
        assert_eq!(
            normalize_node_placement_input(None, Some(250)).unwrap().1,
            250
        );
        assert_eq!(
            normalize_node_placement_input(Some("  eu-west  "), None)
                .unwrap()
                .0,
            "eu-west"
        );
        assert_eq!(
            normalize_node_placement_input(Some("   "), None).unwrap().0,
            ""
        );
        let long = "x".repeat(MAX_NODE_LOCATION_LEN + 1);
        assert_eq!(
            normalize_node_placement_input(Some(&long), None),
            Err(NodePlacementInputError::LocationTooLong)
        );
        let at_limit = "y".repeat(MAX_NODE_LOCATION_LEN);
        assert!(normalize_node_placement_input(Some(&at_limit), None).is_ok());
    }

    #[test]
    fn placement_ref_round_trips() {
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            epoch: 0,
            shard: 7,
        };
        let bytes = postcard::to_allocvec(&placement).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementRef>(&bytes).unwrap(),
            placement
        );
    }

    #[test]
    fn shard_for_subject_matches_golden_vectors() {
        // Fixed subjects → fixed shards. These are the stage-2 cross-node
        // canaries: a change here means a document would remap topics.
        assert_eq!(shard_for_subject(b"", 64), 30);
        assert_eq!(shard_for_subject(b"aruna", 64), 20);
        assert_eq!(shard_for_subject(b"aruna", 128), 84);
        assert_eq!(shard_for_subject(&[0u8; 16], 64), 4);
        assert_eq!(shard_for_subject(&[0u8; 16], 128), 4);
    }

    #[test]
    fn shard_for_subject_stays_in_range() {
        for count in [1u32, 2, 4, 64, 128, 1024] {
            for seed in 0u32..256 {
                let shard = shard_for_subject(&seed.to_be_bytes(), count);
                assert!(shard < count, "shard {shard} out of range for {count}");
            }
        }
    }

    #[test]
    fn shard_for_subject_distributes_evenly() {
        let shard_count = 64u32;
        let samples = 10_000u32;
        let mut counts = vec![0u32; shard_count as usize];
        let mut cursor = [0u8; 32];
        for _ in 0..samples {
            cursor = *blake3::hash(&cursor).as_bytes();
            let shard = shard_for_subject(&cursor, shard_count);
            counts[shard as usize] += 1;
        }
        let expected = samples / shard_count; // ~156
        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        // Generous band around the mean; a broken mask would collapse or spike.
        assert!(min > expected / 2, "under-filled shard: {min} < {expected}");
        assert!(max < expected * 2, "over-filled shard: {max} > {expected}");
    }

    #[test]
    fn binding_round_trips() {
        use crate::structs::RealmId;
        use crate::structured_id::PlacementHandle;

        let binding = PlacementBinding {
            handle: PlacementHandle::new(0x1234).unwrap(),
            scope: PlacementScope::Group(Ulid::from_bytes([7u8; 16])),
            document_class: DocumentClass::Metadata,
            strategy_id: Ulid::from_bytes([8u8; 16]),
            allocator_range_id: Some(Ulid::from_bytes([9u8; 16])),
            allocated_by: Some(node(1)),
            allocated_at_ms: Some(1_700_000_000_000),
        };
        let bytes = postcard::to_allocvec(&binding).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementBinding>(&bytes).unwrap(),
            binding
        );

        // The tuple identity drops provenance; realm scope also round-trips.
        let realm = PlacementBinding {
            scope: PlacementScope::Realm(RealmId([3u8; 32])),
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
            ..binding.clone()
        };
        assert_ne!(realm.scope, binding.scope);
        assert_eq!(realm.tuple().document_class, DocumentClass::Metadata);
        assert_eq!(binding.scope.kind(), PlacementScopeKind::Group);
        assert_eq!(realm.scope.kind(), PlacementScopeKind::Realm);
    }

    #[test]
    fn shard_count_cap_tracks_bucket_cap() {
        // Single source of truth: raising the codec bucket cap raises the
        // placement shard cap in lockstep, so a strategy can never declare more
        // shards than the id's bucket field can encode.
        assert_eq!(
            MAX_PLACEMENT_SHARD_COUNT,
            crate::structured_id::MAX_BUCKET_COUNT as u32
        );
        assert_eq!(MAX_PLACEMENT_SHARD_COUNT, 4096);
    }

    #[test]
    fn handle_range_geometry() {
        let a = HandleRange {
            range_id: Ulid::from_bytes([1; 16]),
            owner: node(1),
            start: FIRST_HANDLE,
            end: FIRST_HANDLE + HANDLE_RANGE_SIZE,
        };
        assert!(a.is_well_formed());
        assert_eq!(a.len(), HANDLE_RANGE_SIZE);
        assert!(a.contains(FIRST_HANDLE));
        assert!(!a.contains(a.end));

        let adjacent = HandleRange {
            start: a.end,
            end: a.end + HANDLE_RANGE_SIZE,
            ..a
        };
        assert!(!a.overlaps(&adjacent));
        let straddling = HandleRange {
            start: a.end - 1,
            end: a.end + 1,
            ..a
        };
        assert!(a.overlaps(&straddling));

        let zero = HandleRange {
            start: 0,
            end: 4,
            ..a
        };
        assert!(!zero.is_well_formed());
        let past_end = HandleRange {
            start: HANDLE_SPACE_END - 1,
            end: HANDLE_SPACE_END + 1,
            ..a
        };
        assert!(!past_end.is_well_formed());
    }

    #[test]
    fn binding_no_bucket() {
        // REQ-META-PLACEMENT-BINDING-001: a binding must not carry a bucket
        // or holders. The exhaustive pattern below has no rest pattern, so
        // adding any field to PlacementBinding breaks this test's compilation
        // until the addition is reviewed against the invariant.
        use crate::structured_id::PlacementHandle;

        let binding = PlacementBinding {
            handle: PlacementHandle::new(1).unwrap(),
            scope: PlacementScope::Group(Ulid::from_bytes([1u8; 16])),
            document_class: DocumentClass::Metadata,
            strategy_id: Ulid::from_bytes([2u8; 16]),
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        };
        let PlacementBinding {
            handle: _,
            scope: _,
            document_class: _,
            strategy_id: _,
            allocator_range_id: _,
            allocated_by: _,
            allocated_at_ms: _,
        } = binding;
    }
}
