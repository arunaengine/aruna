use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use crate::MetaResourceId;

use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::{NotificationKind, PathRestriction, RealmId};
use crate::types::{GroupId, Key, UserId};

pub const NOTIFICATION_WATCH_PER_USER_CAP: usize = 50;
pub const NOTIFICATION_WATCH_MAX_PREFIX_LEN: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataWatchResourcePath<'a> {
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub bucket: &'a str,
    pub key_prefix: &'a str,
}

pub fn data_watch_resource_path(
    group_id: GroupId,
    node_id: NodeId,
    bucket: &str,
    key: &str,
) -> String {
    format!("s3/{group_id}/{node_id}/{bucket}/{key}")
}

pub fn parse_data_watch_resource_path(path: &str) -> Option<DataWatchResourcePath<'_>> {
    let mut segments = path.strip_prefix("s3/")?.splitn(4, '/');
    let raw_group_id = segments.next()?;
    let raw_node_id = segments.next()?;
    let bucket = segments.next()?;
    let key_prefix = segments.next()?;
    let group_id = Ulid::from_str(raw_group_id).ok()?;
    let node_id = NodeId::from_str(raw_node_id).ok()?;
    if group_id.is_nil()
        || raw_group_id != group_id.to_string()
        || raw_node_id != node_id.to_string()
        || bucket.is_empty()
    {
        return None;
    }
    Some(DataWatchResourcePath {
        group_id,
        node_id,
        bucket,
        key_prefix,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEventKind {
    MetadataCreated,
    DataUploaded,
}

impl WatchEventKind {
    /// Stable machine-readable variant name for API payloads.
    pub fn name(&self) -> &'static str {
        match self {
            WatchEventKind::MetadataCreated => "metadata_created",
            WatchEventKind::DataUploaded => "data_uploaded",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "metadata_created" => Some(WatchEventKind::MetadataCreated),
            "data_uploaded" => Some(WatchEventKind::DataUploaded),
            _ => None,
        }
    }

    const fn bit(self) -> u32 {
        match self {
            WatchEventKind::MetadataCreated => WatchEventMask::METADATA_CREATED,
            WatchEventKind::DataUploaded => WatchEventMask::DATA_UPLOADED,
        }
    }
}

/// Hand-rolled bitset over [`WatchEventKind`]; serializes transparently as its
/// backing `u32`, so new event bits are additive and never renumber old ones.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEventMask(u32);

impl WatchEventMask {
    pub const METADATA_CREATED: u32 = 1;
    pub const DATA_UPLOADED: u32 = 2;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub fn from_kinds(kinds: impl IntoIterator<Item = WatchEventKind>) -> Self {
        let mut mask = Self::empty();
        for kind in kinds {
            mask.insert(kind);
        }
        mask
    }

    pub fn contains(self, kind: WatchEventKind) -> bool {
        self.0 & kind.bit() != 0
    }

    pub fn insert(&mut self, kind: WatchEventKind) {
        self.0 |= kind.bit();
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Bitwise union of two masks; used to merge subscriptions that share a
    /// path prefix into a single interest entry.
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub fn kinds(self) -> Vec<WatchEventKind> {
        let mut kinds = Vec::new();
        if self.contains(WatchEventKind::MetadataCreated) {
            kinds.push(WatchEventKind::MetadataCreated);
        }
        if self.contains(WatchEventKind::DataUploaded) {
            kinds.push(WatchEventKind::DataUploaded);
        }
        kinds
    }
}

/// Raw origin-plane watch event that crosses the wire from the node where a
/// mutation committed to every interested inbox-holder node. `event_id` is minted
/// exactly once at the origin and is the idempotency root: holder-side expansion
/// derives a deterministic per-subscription record id from it, so a redelivered
/// event re-expands to the same records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchEvent {
    pub event_id: Ulid,
    pub realm_id: RealmId,
    pub kind: WatchEventKind,
    pub path: String,
    pub actor: UserId,
    pub occurred_at_ms: u64,
    pub detail: WatchEventDetail,
}

/// Kind-specific payload carried by a [`WatchEvent`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEventDetail {
    MetadataCreated {
        group_id: GroupId,
        document_id: MetaResourceId,
    },
    DataUploaded {
        group_id: GroupId,
        node_id: NodeId,
        bucket: String,
        key: String,
        size_bytes: u64,
    },
}

impl WatchEvent {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Maps this event to the recipient-facing notification kind. The event's
    /// `kind` selects matching; the `detail` supplies the deep-link fields.
    pub fn notification_kind(&self) -> NotificationKind {
        match &self.detail {
            WatchEventDetail::MetadataCreated {
                group_id,
                document_id,
            } => NotificationKind::MetadataCreated {
                path: self.path.clone(),
                group_id: *group_id,
                document_id: *document_id,
                actor_user_id: self.actor,
            },
            WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket,
                key,
                size_bytes,
            } => NotificationKind::DataUploaded {
                path: self.path.clone(),
                group_id: *group_id,
                node_id: *node_id,
                bucket: bucket.clone(),
                key: key.clone(),
                size_bytes: *size_bytes,
                actor_user_id: self.actor,
            },
        }
    }
}

/// Domain separator for the deterministic per-subscription notification id.
pub const WATCH_NOTIFICATION_ID_DOMAIN: &[u8] = b"aruna-watch-notification-v1\0";

/// Deterministic notification-record id for the `(event, subscription)` pair.
/// Because it is a pure function of the origin-minted `event_id` and the
/// `watch_id`, re-expanding a redelivered event mints the same id, so the
/// holder-side idempotent upsert collapses duplicate deliveries.
pub fn watch_notification_id(event_id: Ulid, watch_id: Ulid) -> Ulid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(WATCH_NOTIFICATION_ID_DOMAIN);
    hasher.update(&event_id.to_bytes());
    hasher.update(&watch_id.to_bytes());
    let hash = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Ulid::from_bytes(bytes)
}

/// Durable watch subscription row on the owner's inbox-holder node. Placed and
/// proxied exactly like a notification inbox record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchAuthorizationBinding {
    pub token_hash: String,
    pub expires_at_secs: u64,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub watch_path_prefix: String,
}

impl WatchAuthorizationBinding {
    pub fn is_valid(&self) -> bool {
        self.expires_at_secs > 0
            && self.token_hash.len() == 64
            && self
                .token_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            && self
                .path_restrictions
                .iter()
                .flatten()
                .all(|restriction| globset::Glob::new(&restriction.pattern).is_ok())
    }
}

impl Default for WatchAuthorizationBinding {
    fn default() -> Self {
        Self {
            token_hash: blake3::hash(b"internal-watch").to_hex().to_string(),
            expires_at_secs: u64::MAX,
            path_restrictions: None,
            watch_path_prefix: String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchSubscription {
    pub watch_id: Ulid,
    pub owner: UserId,
    pub path_prefix: String,
    pub event_mask: WatchEventMask,
    pub created_at_ms: u64,
    pub authorization: WatchAuthorizationBinding,
}

impl WatchSubscription {
    pub fn new(
        owner: UserId,
        path_prefix: String,
        event_mask: WatchEventMask,
        created_at_ms: u64,
    ) -> Self {
        Self::new_with_authorization(
            owner,
            path_prefix,
            event_mask,
            created_at_ms,
            WatchAuthorizationBinding::default(),
        )
    }

    pub fn new_with_authorization(
        owner: UserId,
        path_prefix: String,
        event_mask: WatchEventMask,
        created_at_ms: u64,
        mut authorization: WatchAuthorizationBinding,
    ) -> Self {
        authorization.watch_path_prefix = path_prefix.clone();
        Self {
            watch_id: Ulid::r#gen(),
            owner,
            path_prefix,
            event_mask,
            created_at_ms,
            authorization,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

pub fn watch_subscription_key(owner: UserId, watch_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(&owner.to_storage_key());
    bytes.extend_from_slice(&watch_id.to_bytes());
    ByteView::from(bytes)
}

pub fn watch_subscription_prefix(owner: UserId) -> Key {
    ByteView::from(owner.to_storage_key())
}

pub fn parse_watch_subscription_key(key: &[u8]) -> Result<(UserId, Ulid), ConversionError> {
    if key.len() != 64 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 64-byte watch subscription key, got {} bytes",
            key.len()
        )));
    }
    let owner = UserId::from_storage_key(&key[..48])?;
    let watch_id = Ulid::from_bytes(key[48..64].try_into()?);
    Ok((owner, watch_id))
}

/// Keys in the watch-interest keyspace. Per-node digests use fixed-length keys
/// so their prefixes are unambiguous: `n/` + realm id + node id (realm first so
/// all nodes' digests for one realm form a single scan range). Local-only
/// markers use text prefixes that never collide with the binary digest prefix.
pub const WATCH_INTEREST_NODE_PREFIX: &[u8] = b"n/";
pub const WATCH_INTEREST_DIRTY_PREFIX: &[u8] = b"dirty/";
const WATCH_INTEREST_PENDING_PREFIX: &[u8] = b"pending/";

/// One coalesced interest entry: the union of every subscription a node holds
/// for a realm that shares this path prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchInterestEntry {
    pub path_prefix: String,
    pub event_mask: WatchEventMask,
}

/// A single node's realm-wide watch interest distributed over the sync layer.
/// Single writer per key (each node writes only its own digest per realm), so
/// ingest is last-write-wins. The owning realm is carried by the storage key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchInterestDigest {
    pub node_id: NodeId,
    pub entries: Vec<WatchInterestEntry>,
}

impl WatchInterestDigest {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Builds a deterministic digest from a node's subscriptions: identical
    /// prefixes are deduped by OR-ing their masks, and entries are sorted by
    /// prefix so the encoded bytes are stable regardless of scan order.
    pub fn from_subscriptions(
        node_id: NodeId,
        subscriptions: impl IntoIterator<Item = (String, WatchEventMask)>,
    ) -> Self {
        let mut merged: BTreeMap<String, WatchEventMask> = BTreeMap::new();
        for (path_prefix, mask) in subscriptions {
            let slot = merged
                .entry(path_prefix)
                .or_insert_with(WatchEventMask::empty);
            *slot = slot.union(mask);
        }
        let entries = merged
            .into_iter()
            .map(|(path_prefix, event_mask)| WatchInterestEntry {
                path_prefix,
                event_mask,
            })
            .collect();
        Self { node_id, entries }
    }
}

pub fn watch_interest_node_key(realm_id: RealmId, node_id: NodeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(WATCH_INTEREST_NODE_PREFIX.len() + 64);
    key.extend_from_slice(WATCH_INTEREST_NODE_PREFIX);
    key.extend_from_slice(realm_id.as_bytes());
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Scan prefix over every node's digest, ordered by realm.
pub fn watch_interest_node_prefix() -> Vec<u8> {
    WATCH_INTEREST_NODE_PREFIX.to_vec()
}

/// Scan prefix over one realm's digests (all nodes, contiguous range).
pub fn watch_interest_realm_prefix(realm_id: RealmId) -> Vec<u8> {
    let mut key = Vec::with_capacity(WATCH_INTEREST_NODE_PREFIX.len() + 32);
    key.extend_from_slice(WATCH_INTEREST_NODE_PREFIX);
    key.extend_from_slice(realm_id.as_bytes());
    key
}

/// Recovers the realm id from a `n/<realm><node>` digest key.
pub fn watch_interest_key_realm_id(key: &[u8]) -> Option<RealmId> {
    let tail = key.strip_prefix(WATCH_INTEREST_NODE_PREFIX)?;
    let bytes: [u8; 32] = tail.get(..32)?.try_into().ok()?;
    Some(RealmId::from_bytes(bytes))
}

/// Recovers the node id from a `n/<realm><node>` digest key.
pub fn watch_interest_key_node_id(key: &[u8]) -> Option<NodeId> {
    let tail = key.strip_prefix(WATCH_INTEREST_NODE_PREFIX)?;
    let bytes: [u8; 32] = tail.get(32..64)?.try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

pub fn watch_interest_dirty_key(realm_id: RealmId) -> Vec<u8> {
    let mut key = Vec::with_capacity(WATCH_INTEREST_DIRTY_PREFIX.len() + 32);
    key.extend_from_slice(WATCH_INTEREST_DIRTY_PREFIX);
    key.extend_from_slice(realm_id.as_bytes());
    key
}

pub fn watch_interest_pending_key(realm_id: RealmId) -> Vec<u8> {
    let mut key = Vec::with_capacity(WATCH_INTEREST_PENDING_PREFIX.len() + 32);
    key.extend_from_slice(WATCH_INTEREST_PENDING_PREFIX);
    key.extend_from_slice(realm_id.as_bytes());
    key
}

/// Recovers the realm id from a `dirty/<realm>` marker key.
pub fn watch_interest_dirty_realm_id(key: &[u8]) -> Option<RealmId> {
    let tail = key.strip_prefix(WATCH_INTEREST_DIRTY_PREFIX)?;
    let bytes: [u8; 32] = tail.try_into().ok()?;
    Some(RealmId::from_bytes(bytes))
}

/// In-memory realm -> node -> interest entries cache rebuilt from the replicated
/// digests. Consumed by origin nodes to match events against realm-wide watch
/// interest without a per-event storage read. Only nodes with live watches are
/// retained, so an empty node map means no held interest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WatchInterestTable {
    realms: HashMap<RealmId, HashMap<NodeId, Vec<WatchInterestEntry>>>,
}

impl WatchInterestTable {
    pub fn is_empty(&self) -> bool {
        self.realms.is_empty()
    }

    /// Records a node's interest entries for a realm. An empty entry set drops
    /// the node (and the realm once its last node is gone) so the table only
    /// ever holds nodes with live watches.
    pub fn insert(&mut self, realm_id: RealmId, node_id: NodeId, entries: Vec<WatchInterestEntry>) {
        if entries.is_empty() {
            if let Some(nodes) = self.realms.get_mut(&realm_id) {
                nodes.remove(&node_id);
                if nodes.is_empty() {
                    self.realms.remove(&realm_id);
                }
            }
            return;
        }
        self.realms
            .entry(realm_id)
            .or_default()
            .insert(node_id, entries);
    }

    /// Replaces the entire node map for a realm, dropping empty digests and the
    /// realm entry itself when nothing remains.
    pub fn set_realm(
        &mut self,
        realm_id: RealmId,
        nodes: HashMap<NodeId, Vec<WatchInterestEntry>>,
    ) {
        let nodes: HashMap<NodeId, Vec<WatchInterestEntry>> = nodes
            .into_iter()
            .filter(|(_, entries)| !entries.is_empty())
            .collect();
        if nodes.is_empty() {
            self.realms.remove(&realm_id);
        } else {
            self.realms.insert(realm_id, nodes);
        }
    }

    pub fn nodes(&self, realm_id: RealmId) -> Option<&HashMap<NodeId, Vec<WatchInterestEntry>>> {
        self.realms.get(&realm_id)
    }

    /// Holder nodes in `realm_id` whose interest covers `path` for `kind`: an
    /// entry matches when its mask selects the kind and its prefix is a prefix
    /// of the event path. Result is sorted for a deterministic fan-out order.
    pub fn matching_nodes(
        &self,
        realm_id: RealmId,
        path: &str,
        kind: WatchEventKind,
    ) -> Vec<NodeId> {
        let Some(nodes) = self.realms.get(&realm_id) else {
            return Vec::new();
        };
        let mut matched: Vec<NodeId> = nodes
            .iter()
            .filter(|(_, entries)| {
                entries.iter().any(|entry| {
                    entry.event_mask.contains(kind) && path.starts_with(entry.path_prefix.as_str())
                })
            })
            .map(|(node_id, _)| *node_id)
            .collect();
        matched.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        matched
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspaces::NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE;
    use crate::storage_entries::{watch_subscription_delete_entry, watch_subscription_write_entry};
    use crate::structs::RealmId;

    fn user(realm: u8, user_byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([user_byte; 16]), RealmId([realm; 32]))
    }

    fn subscription(owner: UserId) -> WatchSubscription {
        WatchSubscription::new(
            owner,
            "/bucket/prefix".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            1_700_000_000_000,
        )
    }

    #[test]
    fn subscription_roundtrips_through_postcard() {
        let owner = user(1, 2);
        for mask in [
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            WatchEventMask::from_kinds([
                WatchEventKind::MetadataCreated,
                WatchEventKind::DataUploaded,
            ]),
        ] {
            let record = WatchSubscription::new(owner, "/data".to_string(), mask, 42);
            let bytes = record.to_bytes().unwrap();
            assert_eq!(WatchSubscription::from_bytes(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn keys_order_by_watch_id_within_owner() {
        let owner = user(1, 2);
        let a = Ulid::from_parts(1, 0);
        let b = Ulid::from_parts(2, 0);
        assert!(watch_subscription_key(owner, a) < watch_subscription_key(owner, b));
    }

    #[test]
    fn keys_are_owner_isolated() {
        let a = user(1, 2);
        let b = user(1, 3);
        let watch_id = Ulid::r#gen();
        let ka = watch_subscription_key(a, watch_id);
        let kb = watch_subscription_key(b, watch_id);
        assert_ne!(&ka[..48], &kb[..48]);
        assert_eq!(&ka[..48], watch_subscription_prefix(a).as_ref());
        assert!(!ka.starts_with(watch_subscription_prefix(b).as_ref()));
    }

    #[test]
    fn key_roundtrips_through_parser() {
        let owner = user(5, 9);
        let watch_id = Ulid::r#gen();
        let key = watch_subscription_key(owner, watch_id);
        assert_eq!(
            parse_watch_subscription_key(&key).unwrap(),
            (owner, watch_id)
        );
        assert!(matches!(
            parse_watch_subscription_key(&key[..63]),
            Err(ConversionError::InvalidLength(_))
        ));
        let mut long = key.to_vec();
        long.push(0);
        assert!(matches!(
            parse_watch_subscription_key(&long),
            Err(ConversionError::InvalidLength(_))
        ));
    }

    #[test]
    fn mask_semantics() {
        let mut mask = WatchEventMask::empty();
        assert!(mask.is_empty());
        assert!(!mask.contains(WatchEventKind::MetadataCreated));

        mask.insert(WatchEventKind::DataUploaded);
        assert!(!mask.is_empty());
        assert!(mask.contains(WatchEventKind::DataUploaded));
        assert!(!mask.contains(WatchEventKind::MetadataCreated));
        assert_eq!(mask.bits(), WatchEventMask::DATA_UPLOADED);

        mask.insert(WatchEventKind::MetadataCreated);
        assert_eq!(
            mask.kinds(),
            vec![
                WatchEventKind::MetadataCreated,
                WatchEventKind::DataUploaded
            ]
        );
        assert_eq!(
            mask.bits(),
            WatchEventMask::METADATA_CREATED | WatchEventMask::DATA_UPLOADED
        );
    }

    fn metadata_event(actor: UserId) -> WatchEvent {
        let group_id = Ulid::from_bytes([3u8; 16]);
        WatchEvent {
            event_id: Ulid::from_bytes([9u8; 16]),
            realm_id: RealmId([1u8; 32]),
            kind: WatchEventKind::MetadataCreated,
            path: format!("meta/{group_id}/datasets/project/run-42"),
            actor,
            occurred_at_ms: 1_700_000_000_000,
            detail: WatchEventDetail::MetadataCreated {
                group_id,
                document_id: MetaResourceId::from_bytes([4u8; 16]).unwrap(),
            },
        }
    }

    #[test]
    fn watch_event_roundtrips_through_postcard() {
        let actor = user(1, 2);
        let metadata = metadata_event(actor);
        assert_eq!(
            WatchEvent::from_bytes(&metadata.to_bytes().unwrap()).unwrap(),
            metadata
        );

        let group_id = Ulid::from_bytes([5u8; 16]);
        let node_id = node(6);
        let uploaded = WatchEvent {
            event_id: Ulid::from_bytes([10u8; 16]),
            realm_id: RealmId([1u8; 32]),
            kind: WatchEventKind::DataUploaded,
            path: data_watch_resource_path(group_id, node_id, "bucket", "object"),
            actor,
            occurred_at_ms: 42,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 8192,
            },
        };
        assert_eq!(
            WatchEvent::from_bytes(&uploaded.to_bytes().unwrap()).unwrap(),
            uploaded
        );
    }

    #[test]
    fn watch_event_maps_to_notification_kind() {
        let actor = user(1, 2);
        match metadata_event(actor).notification_kind() {
            NotificationKind::MetadataCreated {
                path,
                group_id,
                document_id,
                actor_user_id,
            } => {
                assert_eq!(
                    path,
                    format!(
                        "meta/{}/datasets/project/run-42",
                        Ulid::from_bytes([3u8; 16])
                    )
                );
                assert_eq!(group_id, Ulid::from_bytes([3u8; 16]));
                assert_eq!(document_id, MetaResourceId::from_bytes([4u8; 16]).unwrap());
                assert_eq!(actor_user_id, actor);
            }
            other => panic!("unexpected kind: {other:?}"),
        }

        let group_id = Ulid::from_bytes([5u8; 16]);
        let node_id = node(6);
        let uploaded = WatchEvent {
            event_id: Ulid::r#gen(),
            realm_id: RealmId([1u8; 32]),
            kind: WatchEventKind::DataUploaded,
            path: data_watch_resource_path(group_id, node_id, "bucket", "object"),
            actor,
            occurred_at_ms: 1,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 5,
            },
        };
        match uploaded.notification_kind() {
            NotificationKind::DataUploaded {
                path,
                group_id: event_group_id,
                node_id: event_node_id,
                bucket,
                key,
                size_bytes,
                actor_user_id,
            } => {
                assert_eq!(
                    path,
                    data_watch_resource_path(group_id, node_id, "bucket", "object")
                );
                assert_eq!(event_group_id, group_id);
                assert_eq!(event_node_id, node_id);
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "object");
                assert_eq!(size_bytes, 5);
                assert_eq!(actor_user_id, actor);
            }
            other => panic!("unexpected kind: {other:?}"),
        }
    }

    #[test]
    fn data_resource_path_is_node_disambiguated_and_roundtrips() {
        let group_id = Ulid::from_bytes([5u8; 16]);
        let first_node = node(6);
        let second_node = node(7);
        let first = data_watch_resource_path(group_id, first_node, "bucket", "reports/q3");
        let second = data_watch_resource_path(group_id, second_node, "bucket", "reports/q3");

        assert_ne!(first, second);
        assert_eq!(
            parse_data_watch_resource_path(&first),
            Some(DataWatchResourcePath {
                group_id,
                node_id: first_node,
                bucket: "bucket",
                key_prefix: "reports/q3",
            })
        );
    }

    #[test]
    fn watch_notification_id_is_deterministic_per_event_and_watch() {
        let event_id = Ulid::from_bytes([1u8; 16]);
        let watch_id = Ulid::from_bytes([2u8; 16]);
        assert_eq!(
            watch_notification_id(event_id, watch_id),
            watch_notification_id(event_id, watch_id)
        );
        assert_ne!(
            watch_notification_id(event_id, watch_id),
            watch_notification_id(event_id, Ulid::from_bytes([3u8; 16]))
        );
        assert_ne!(
            watch_notification_id(event_id, watch_id),
            watch_notification_id(Ulid::from_bytes([9u8; 16]), watch_id)
        );
    }

    #[test]
    fn watch_authorization_hash_must_be_canonical() {
        let mut binding = WatchAuthorizationBinding::default();
        assert!(binding.is_valid());
        binding.token_hash.make_ascii_uppercase();
        assert!(!binding.is_valid());
    }

    #[test]
    fn event_kind_names_are_stable() {
        assert_eq!(WatchEventKind::MetadataCreated.name(), "metadata_created");
        assert_eq!(WatchEventKind::DataUploaded.name(), "data_uploaded");
        assert_eq!(
            WatchEventKind::from_name("metadata_created"),
            Some(WatchEventKind::MetadataCreated)
        );
        assert_eq!(
            WatchEventKind::from_name("data_uploaded"),
            Some(WatchEventKind::DataUploaded)
        );
        assert_eq!(WatchEventKind::from_name("nope"), None);
    }

    #[test]
    fn write_and_delete_entries_target_the_keyspace() {
        let owner = user(1, 2);
        let record = subscription(owner);

        let write = watch_subscription_write_entry(&record).unwrap();
        assert_eq!(write.0, NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE);
        assert_eq!(write.1, watch_subscription_key(owner, record.watch_id));
        assert_eq!(WatchSubscription::from_bytes(&write.2).unwrap(), record);

        let delete = watch_subscription_delete_entry(owner, record.watch_id);
        assert_eq!(delete.0, NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE);
        assert_eq!(delete.1, watch_subscription_key(owner, record.watch_id));
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn digest_roundtrips_through_postcard() {
        let digest = WatchInterestDigest {
            node_id: node(3),
            entries: vec![
                WatchInterestEntry {
                    path_prefix: "/a".to_string(),
                    event_mask: WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
                },
                WatchInterestEntry {
                    path_prefix: "/b".to_string(),
                    event_mask: WatchEventMask::from_kinds([
                        WatchEventKind::MetadataCreated,
                        WatchEventKind::DataUploaded,
                    ]),
                },
            ],
        };
        let bytes = digest.to_bytes().unwrap();
        assert_eq!(WatchInterestDigest::from_bytes(&bytes).unwrap(), digest);
    }

    #[test]
    fn digest_dedupes_prefixes_and_sorts_deterministically() {
        let node_id = node(4);
        let metadata = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
        let data = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);

        let first = WatchInterestDigest::from_subscriptions(
            node_id,
            [
                ("/z".to_string(), data),
                ("/a".to_string(), metadata),
                ("/a".to_string(), data),
            ],
        );
        // Same subscriptions in a different order must encode identically.
        let second = WatchInterestDigest::from_subscriptions(
            node_id,
            [
                ("/a".to_string(), data),
                ("/z".to_string(), data),
                ("/a".to_string(), metadata),
            ],
        );

        assert_eq!(first, second);
        assert_eq!(first.to_bytes().unwrap(), second.to_bytes().unwrap());
        assert_eq!(
            first.entries,
            vec![
                WatchInterestEntry {
                    path_prefix: "/a".to_string(),
                    event_mask: metadata.union(data),
                },
                WatchInterestEntry {
                    path_prefix: "/z".to_string(),
                    event_mask: data,
                },
            ]
        );
    }

    #[test]
    fn interest_keys_recover_realm_and_node() {
        let realm_id = RealmId([7u8; 32]);
        let node_id = node(9);

        let key = watch_interest_node_key(realm_id, node_id);
        assert!(key.starts_with(WATCH_INTEREST_NODE_PREFIX));
        assert!(key.starts_with(&watch_interest_realm_prefix(realm_id)));
        assert!(key.starts_with(&watch_interest_node_prefix()));
        assert_eq!(watch_interest_key_realm_id(&key), Some(realm_id));
        assert_eq!(watch_interest_key_node_id(&key), Some(node_id));

        let dirty = watch_interest_dirty_key(realm_id);
        assert_eq!(watch_interest_dirty_realm_id(&dirty), Some(realm_id));
        // Marker and digest prefixes never collide.
        assert_eq!(watch_interest_key_realm_id(&dirty), None);
        assert_eq!(watch_interest_dirty_realm_id(&key), None);
    }

    #[test]
    fn interest_table_matches_by_prefix_and_mask() {
        let realm_id = RealmId([1u8; 32]);
        let other_realm = RealmId([2u8; 32]);
        let holder = node(10);
        let mut table = WatchInterestTable::default();
        table.insert(
            realm_id,
            holder,
            vec![WatchInterestEntry {
                path_prefix: "/bucket/".to_string(),
                event_mask: WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            }],
        );

        assert_eq!(
            table.matching_nodes(realm_id, "/bucket/object", WatchEventKind::MetadataCreated),
            vec![holder]
        );
        // Prefix mismatch and mask mismatch both filter the holder out.
        assert!(
            table
                .matching_nodes(realm_id, "/other", WatchEventKind::MetadataCreated)
                .is_empty()
        );
        assert!(
            table
                .matching_nodes(realm_id, "/bucket/object", WatchEventKind::DataUploaded)
                .is_empty()
        );
        // Realm isolation.
        assert!(
            table
                .matching_nodes(
                    other_realm,
                    "/bucket/object",
                    WatchEventKind::MetadataCreated
                )
                .is_empty()
        );

        // An empty entry set drops the holder, and the realm once empty.
        table.insert(realm_id, holder, Vec::new());
        assert!(table.nodes(realm_id).is_none());
        assert!(table.is_empty());
    }
}
