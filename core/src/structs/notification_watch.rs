use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::types::{Key, UserId};

pub const NOTIFICATION_WATCH_PER_USER_CAP: usize = 50;
pub const NOTIFICATION_WATCH_MAX_PREFIX_LEN: usize = 1024;

/// Append-only postcard enum: postcard encodes the variant index, so existing
/// variants must never be removed, reordered, or have fields changed; new
/// variants are appended at the end only.
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

/// Durable watch subscription row on the owner's inbox-holder node. Placed and
/// proxied exactly like a notification inbox record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchSubscription {
    pub watch_id: Ulid,
    pub owner: UserId,
    pub path_prefix: String,
    pub event_mask: WatchEventMask,
    pub created_at_ms: u64,
}

impl WatchSubscription {
    pub fn new(
        owner: UserId,
        path_prefix: String,
        event_mask: WatchEventMask,
        created_at_ms: u64,
    ) -> Self {
        Self {
            watch_id: Ulid::new(),
            owner,
            path_prefix,
            event_mask,
            created_at_ms,
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
        let watch_id = Ulid::new();
        let ka = watch_subscription_key(a, watch_id);
        let kb = watch_subscription_key(b, watch_id);
        assert_ne!(&ka[..48], &kb[..48]);
        assert_eq!(&ka[..48], watch_subscription_prefix(a).as_ref());
        assert!(!ka.starts_with(watch_subscription_prefix(b).as_ref()));
    }

    #[test]
    fn key_roundtrips_through_parser() {
        let owner = user(5, 9);
        let watch_id = Ulid::new();
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
}
