use byteview::ByteView;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::RealmId;
use crate::types::{GroupId, Key, UserId};

pub const NOTIFICATION_DIRECT_TTL_MS: u64 = 90 * 24 * 60 * 60 * 1000;
pub const NOTIFICATION_TRANSIENT_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1000;
pub const NOTIFICATION_TRANSIENT_PER_USER_CAP: usize = 500;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationClass {
    Direct,
    Transient,
}

impl NotificationClass {
    pub fn ttl_ms(&self) -> u64 {
        match self {
            NotificationClass::Direct => NOTIFICATION_DIRECT_TTL_MS,
            NotificationClass::Transient => NOTIFICATION_TRANSIENT_TTL_MS,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationKind {
    AddedToGroup {
        group_id: GroupId,
        actor_user_id: UserId,
    },
    RemovedFromGroup {
        group_id: GroupId,
        actor_user_id: UserId,
    },
    GroupMemberAdded {
        group_id: GroupId,
        member_user_id: UserId,
        actor_user_id: UserId,
    },
    NodeOnboarded {
        realm_id: RealmId,
        node_id: NodeId,
    },
    MetadataCreated {
        path: String,
        group_id: GroupId,
        document_id: Ulid,
        actor_user_id: UserId,
    },
    DataUploaded {
        path: String,
        group_id: GroupId,
        node_id: NodeId,
        bucket: String,
        key: String,
        size_bytes: u64,
        actor_user_id: UserId,
    },
}

impl NotificationKind {
    /// Stable category string for per-kind preferences (#288). Never change an
    /// existing mapping.
    pub fn category(&self) -> &'static str {
        match self {
            NotificationKind::AddedToGroup { .. }
            | NotificationKind::RemovedFromGroup { .. }
            | NotificationKind::GroupMemberAdded { .. } => "group.membership",
            NotificationKind::NodeOnboarded { .. } => "node.onboarding",
            NotificationKind::MetadataCreated { .. } | NotificationKind::DataUploaded { .. } => {
                "resource.watch"
            }
        }
    }

    /// Stable machine-readable variant name for API payloads.
    pub fn name(&self) -> &'static str {
        match self {
            NotificationKind::AddedToGroup { .. } => "added_to_group",
            NotificationKind::RemovedFromGroup { .. } => "removed_from_group",
            NotificationKind::GroupMemberAdded { .. } => "group_member_added",
            NotificationKind::NodeOnboarded { .. } => "node_onboarded",
            NotificationKind::MetadataCreated { .. } => "metadata_created",
            NotificationKind::DataUploaded { .. } => "data_uploaded",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationRecord {
    pub notification_id: Ulid,
    pub recipient: UserId,
    pub class: NotificationClass,
    pub kind: NotificationKind,
    pub created_at_ms: u64,
    pub read_at_ms: Option<u64>,
}

impl NotificationRecord {
    pub fn new(
        recipient: UserId,
        class: NotificationClass,
        kind: NotificationKind,
        created_at_ms: u64,
    ) -> Self {
        Self {
            notification_id: Ulid::r#gen(),
            recipient,
            class,
            kind,
            created_at_ms,
            read_at_ms: None,
        }
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.created_at_ms.saturating_add(self.class.ttl_ms())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Typed post-commit event emitted by mutating operations (event plane). Never
/// persisted raw; consumed by routing (operations crate) only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceEvent {
    GroupMemberAdded {
        group_id: GroupId,
        affected_user: UserId,
        actor_user_id: UserId,
    },
    GroupMemberRemoved {
        group_id: GroupId,
        affected_user: UserId,
        actor_user_id: UserId,
    },
    NodeOnboarded {
        realm_id: RealmId,
        node_id: NodeId,
    },
}

/// Durable origin-node outbox row (transport plane). Holder is NOT stored: it is
/// re-resolved on every drain so deliveries re-rank when the eligible set changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationOutboxRecord {
    pub outbox_id: Ulid,
    pub record: NotificationRecord,
}

pub fn invert_timestamp_ms(timestamp_ms: u64) -> u64 {
    u64::MAX - timestamp_ms
}

pub fn notification_inbox_key(recipient: UserId, created_at_ms: u64, notification_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&recipient.to_storage_key());
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&notification_id.to_bytes());
    ByteView::from(bytes)
}

pub fn notification_inbox_prefix(recipient: UserId) -> Key {
    ByteView::from(recipient.to_storage_key())
}

pub fn notification_inbox_cursor(created_at_ms: u64, notification_id: Ulid) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(&invert_timestamp_ms(created_at_ms).to_be_bytes());
    bytes.extend_from_slice(&notification_id.to_bytes());
    bytes
}

pub fn parse_notification_inbox_key(key: &[u8]) -> Result<(UserId, u64, Ulid), ConversionError> {
    if key.len() != 72 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 72-byte notification inbox key, got {} bytes",
            key.len()
        )));
    }
    let recipient = UserId::from_storage_key(&key[..48])?;
    let created_at_ms = invert_timestamp_ms(u64::from_be_bytes(key[48..56].try_into()?));
    let notification_id = Ulid::from_bytes(key[56..72].try_into()?);
    Ok((recipient, created_at_ms, notification_id))
}

pub fn notification_prune_index_key(record: &NotificationRecord) -> Key {
    let mut bytes = Vec::with_capacity(72);
    bytes.extend_from_slice(&record.expires_at_ms().to_be_bytes());
    bytes.extend_from_slice(&record.recipient.to_storage_key());
    bytes.extend_from_slice(&record.notification_id.to_bytes());
    ByteView::from(bytes)
}

pub fn parse_notification_prune_index_key(
    key: &[u8],
) -> Result<(u64, UserId, Ulid), ConversionError> {
    if key.len() != 72 {
        return Err(ConversionError::InvalidLength(format!(
            "expected 72-byte notification prune index key, got {} bytes",
            key.len()
        )));
    }
    let expires_at_ms = u64::from_be_bytes(key[..8].try_into()?);
    let recipient = UserId::from_storage_key(&key[8..56])?;
    let notification_id = Ulid::from_bytes(key[56..72].try_into()?);
    Ok((expires_at_ms, recipient, notification_id))
}

pub fn notification_outbox_key(outbox_id: Ulid) -> Key {
    ByteView::from(outbox_id.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE};
    use crate::storage_entries::{
        notification_inbox_delete_entries, notification_inbox_update_entry,
        notification_inbox_write_entries,
    };

    fn make_node_id(seed: u8) -> NodeId {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes).public()
    }

    fn user(realm: u8, user_byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([user_byte; 16]), RealmId([realm; 32]))
    }

    fn added(actor: u8) -> NotificationKind {
        NotificationKind::AddedToGroup {
            group_id: Ulid::r#gen(),
            actor_user_id: user(1, actor),
        }
    }

    #[test]
    fn notification_record_roundtrips_through_postcard() {
        let recipient = user(1, 2);
        let metadata_group_id = Ulid::r#gen();
        let data_group_id = Ulid::r#gen();
        let data_node_id = make_node_id(8);
        for kind in [
            NotificationKind::AddedToGroup {
                group_id: Ulid::r#gen(),
                actor_user_id: user(1, 3),
            },
            NotificationKind::RemovedFromGroup {
                group_id: Ulid::r#gen(),
                actor_user_id: user(1, 3),
            },
            NotificationKind::GroupMemberAdded {
                group_id: Ulid::r#gen(),
                member_user_id: user(1, 4),
                actor_user_id: user(1, 3),
            },
            NotificationKind::NodeOnboarded {
                realm_id: RealmId([1; 32]),
                node_id: make_node_id(7),
            },
            NotificationKind::MetadataCreated {
                path: format!("meta/{metadata_group_id}/datasets/project/run-42"),
                group_id: metadata_group_id,
                document_id: Ulid::r#gen(),
                actor_user_id: user(1, 5),
            },
            NotificationKind::DataUploaded {
                path: crate::structs::data_watch_resource_path(
                    data_group_id,
                    data_node_id,
                    "bucket",
                    "key",
                ),
                group_id: data_group_id,
                node_id: data_node_id,
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                size_bytes: 4096,
                actor_user_id: user(1, 6),
            },
        ] {
            let record = NotificationRecord::new(recipient, NotificationClass::Direct, kind, 1234);
            let bytes = record.to_bytes().unwrap();
            assert_eq!(NotificationRecord::from_bytes(&bytes).unwrap(), record);
        }
    }

    #[test]
    fn inbox_keys_order_newest_first_within_recipient() {
        let r = user(1, 2);
        let id = Ulid::r#gen();
        assert!(notification_inbox_key(r, 2000, id) < notification_inbox_key(r, 1000, id));

        let id_a = Ulid::from_bytes([0u8; 16]);
        let id_b = Ulid::from_bytes([1u8; 16]);
        let ka = notification_inbox_key(r, 1000, id_a);
        let kb = notification_inbox_key(r, 1000, id_b);
        assert_ne!(ka, kb);
        assert!(ka < kb);
    }

    #[test]
    fn inbox_key_is_recipient_prefixed() {
        let a = user(1, 2);
        let b = user(1, 3);
        let id = Ulid::r#gen();
        let ka = notification_inbox_key(a, 1000, id);
        let kb = notification_inbox_key(b, 1000, id);
        assert_ne!(&ka[..48], &kb[..48]);
        assert_eq!(&ka[..48], notification_inbox_prefix(a).as_ref());
        assert!(!ka.starts_with(notification_inbox_prefix(b).as_ref()));
    }

    #[test]
    fn inbox_key_roundtrips_through_parser() {
        let r = user(5, 9);
        let ts = 1_700_000_000_000u64;
        let id = Ulid::r#gen();
        let key = notification_inbox_key(r, ts, id);
        assert_eq!(parse_notification_inbox_key(&key).unwrap(), (r, ts, id));
        assert!(matches!(
            parse_notification_inbox_key(&key[..71]),
            Err(ConversionError::InvalidLength(_))
        ));
        let mut long = key.to_vec();
        long.push(0);
        assert!(matches!(
            parse_notification_inbox_key(&long),
            Err(ConversionError::InvalidLength(_))
        ));
    }

    #[test]
    fn prune_index_key_orders_by_expiry() {
        let r = user(1, 2);
        let t = 1_000_000u64;
        let transient = NotificationRecord::new(r, NotificationClass::Transient, added(3), t);
        let direct = NotificationRecord::new(r, NotificationClass::Direct, added(3), t);
        let kt = notification_prune_index_key(&transient);
        let kd = notification_prune_index_key(&direct);
        assert!(kt < kd);
        assert_eq!(
            parse_notification_prune_index_key(&kt).unwrap(),
            (transient.expires_at_ms(), r, transient.notification_id)
        );
    }

    #[test]
    fn outbox_keys_are_fifo_by_ulid() {
        assert!(
            notification_outbox_key(Ulid::from_parts(1, 0))
                < notification_outbox_key(Ulid::from_parts(2, 0))
        );
    }

    #[test]
    fn cursor_is_key_suffix() {
        let r = user(1, 2);
        let ts = 42u64;
        let id = Ulid::r#gen();
        let cursor = notification_inbox_cursor(ts, id);
        assert_eq!(cursor.len(), 24);
        assert_eq!(
            cursor.as_slice(),
            &notification_inbox_key(r, ts, id)[48..72]
        );
    }

    #[test]
    fn write_entries_pair_primary_and_index() {
        let r = user(1, 2);
        let record = NotificationRecord::new(r, NotificationClass::Direct, added(3), 1000);

        let writes = notification_inbox_write_entries(&record).unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].0, NOTIFICATION_INBOX_KEYSPACE);
        assert_eq!(
            writes[0].1,
            notification_inbox_key(r, record.created_at_ms, record.notification_id)
        );
        assert_eq!(writes[1].0, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE);
        assert_eq!(writes[1].1, notification_prune_index_key(&record));
        assert!(writes[1].2.is_empty());

        let deletes = notification_inbox_delete_entries(&record);
        assert_eq!(deletes.len(), 2);
        assert_eq!(deletes[0].0, NOTIFICATION_INBOX_KEYSPACE);
        assert_eq!(
            deletes[0].1,
            notification_inbox_key(r, record.created_at_ms, record.notification_id)
        );
        assert_eq!(deletes[1].0, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE);
        assert_eq!(deletes[1].1, notification_prune_index_key(&record));

        let update = notification_inbox_update_entry(&record).unwrap();
        assert_eq!(update.0, NOTIFICATION_INBOX_KEYSPACE);
        assert_eq!(
            update.1,
            notification_inbox_key(r, record.created_at_ms, record.notification_id)
        );
    }

    #[test]
    fn expires_at_saturates() {
        let record = NotificationRecord::new(
            user(1, 2),
            NotificationClass::Direct,
            NotificationKind::NodeOnboarded {
                realm_id: RealmId([1; 32]),
                node_id: make_node_id(1),
            },
            u64::MAX,
        );
        assert_eq!(record.expires_at_ms(), u64::MAX);
    }

    #[test]
    fn kind_categories_are_stable() {
        let added = NotificationKind::AddedToGroup {
            group_id: Ulid::r#gen(),
            actor_user_id: user(1, 3),
        };
        let removed = NotificationKind::RemovedFromGroup {
            group_id: Ulid::r#gen(),
            actor_user_id: user(1, 3),
        };
        let member = NotificationKind::GroupMemberAdded {
            group_id: Ulid::r#gen(),
            member_user_id: user(1, 4),
            actor_user_id: user(1, 3),
        };
        let onboarded = NotificationKind::NodeOnboarded {
            realm_id: RealmId([1; 32]),
            node_id: make_node_id(1),
        };
        let metadata_group_id = Ulid::r#gen();
        let metadata_created = NotificationKind::MetadataCreated {
            path: format!("meta/{metadata_group_id}/datasets/project/run-42"),
            group_id: metadata_group_id,
            document_id: Ulid::r#gen(),
            actor_user_id: user(1, 5),
        };
        let data_group_id = Ulid::r#gen();
        let data_node_id = make_node_id(2);
        let data_uploaded = NotificationKind::DataUploaded {
            path: crate::structs::data_watch_resource_path(
                data_group_id,
                data_node_id,
                "bucket",
                "key",
            ),
            group_id: data_group_id,
            node_id: data_node_id,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            size_bytes: 1,
            actor_user_id: user(1, 6),
        };
        assert_eq!(added.category(), "group.membership");
        assert_eq!(removed.category(), "group.membership");
        assert_eq!(member.category(), "group.membership");
        assert_eq!(onboarded.category(), "node.onboarding");
        assert_eq!(metadata_created.category(), "resource.watch");
        assert_eq!(data_uploaded.category(), "resource.watch");
        assert_eq!(added.name(), "added_to_group");
        assert_eq!(removed.name(), "removed_from_group");
        assert_eq!(member.name(), "group_member_added");
        assert_eq!(onboarded.name(), "node_onboarded");
        assert_eq!(metadata_created.name(), "metadata_created");
        assert_eq!(data_uploaded.name(), "data_uploaded");
    }
}
