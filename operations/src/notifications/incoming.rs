use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    NotificationClass, NotificationKind, NotificationRecord, RealmConfigDocument, RealmId,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use byteview::ByteView;
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive};
use crate::notifications::client::{
    close_stream, drain_request_stream, read_message, write_message,
};
use crate::notifications::inbox::upsert_inbox_records;
use crate::notifications::list::{ListNotificationsInput, ListNotificationsOperation};
use crate::notifications::mark_read::{MARK_READ_MAX_IDS, MarkReadInput, MarkReadOperation};
use crate::notifications::outbox::NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE;
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::protocol::{NotificationTransportMessage, notification_message_kind};
use crate::notifications::unread::{UnreadCountInput, UnreadCountOperation};
use crate::notifications::watch::subscriptions::{
    create_watch_subscription, delete_watch_subscription, list_watch_subscriptions,
};

const NOTIFICATION_MAX_FUTURE_SKEW_MS: u64 = 5 * 60 * 1000;

#[tracing::instrument(
    name = "notifications.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub async fn handle_notification_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(peer = %peer, "Dropping inbound notification stream without net handle");
        return;
    };

    let message = match read_message(&mut stream).await {
        Ok(message) => message,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read notification message");
            return;
        }
    };
    debug!(peer = %peer, message = notification_message_kind(&message), "Received notification message");

    let response = build_response(context, net_handle, peer, message).await;

    if let Err(error) = drain_request_stream(&mut stream).await {
        warn!(peer = %peer, error = %error, "Failed to drain notification request stream");
    }
    if let Err(error) = write_message(&mut stream, &response).await {
        warn!(peer = %peer, error = %error, "Failed to write notification response");
    }
    close_stream(&mut stream).await;
}

async fn build_response(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    message: NotificationTransportMessage,
) -> NotificationTransportMessage {
    let realm_id = match message_realm(&message) {
        Ok(realm_id) => realm_id,
        Err(reason) => return NotificationTransportMessage::Reject(reason),
    };
    let realm_config = match authorize_peer(context, net_handle, peer, realm_id).await {
        Ok(config) => config,
        Err(reason) => return NotificationTransportMessage::Reject(reason),
    };
    let local_node_id = net_handle.node_id();

    match message {
        NotificationTransportMessage::DeliverBatch { records } => {
            if let Err(reason) = validate_inbound_batch(&records, unix_timestamp_millis()) {
                return NotificationTransportMessage::Reject(reason);
            }
            if let Err(reason) = verify_batch_local_holder(&records, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }
            match upsert_inbox_records(&context.storage_handle, &records).await {
                Ok(written) => NotificationTransportMessage::DeliverAck {
                    written: written as u32,
                },
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::List {
            recipient,
            cursor,
            limit,
        } => {
            if let Err(reason) =
                verify_recipient_local_holder(&recipient, &realm_config, local_node_id)
            {
                return NotificationTransportMessage::Reject(reason);
            }
            match drive(
                ListNotificationsOperation::new(ListNotificationsInput {
                    recipient,
                    cursor,
                    limit: limit as usize,
                }),
                context,
            )
            .await
            {
                Ok(output) => NotificationTransportMessage::ListResult {
                    records: output.records,
                    next_cursor: output.next_cursor,
                },
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::UnreadCount { recipient } => {
            if let Err(reason) =
                verify_recipient_local_holder(&recipient, &realm_config, local_node_id)
            {
                return NotificationTransportMessage::Reject(reason);
            }
            match drive(
                UnreadCountOperation::new(UnreadCountInput { recipient }),
                context,
            )
            .await
            {
                Ok(output) => NotificationTransportMessage::UnreadCountResult {
                    count: output.count as u32,
                    capped: output.capped,
                },
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::MarkRead {
            recipient,
            ids,
            up_to_ms,
        } => {
            if ids.len() > MARK_READ_MAX_IDS {
                return NotificationTransportMessage::Reject(format!(
                    "mark read id count {} exceeds cap {MARK_READ_MAX_IDS}",
                    ids.len()
                ));
            }
            if let Err(reason) =
                verify_recipient_local_holder(&recipient, &realm_config, local_node_id)
            {
                return NotificationTransportMessage::Reject(reason);
            }
            match drive(
                MarkReadOperation::new(MarkReadInput {
                    recipient,
                    ids,
                    up_to_ms,
                    now_ms: unix_timestamp_millis(),
                }),
                context,
            )
            .await
            {
                Ok(output) => NotificationTransportMessage::MarkReadResult {
                    marked: output.marked as u32,
                },
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::CreateWatch {
            owner,
            path_prefix,
            event_mask,
        } => match create_watch_subscription(
            &context.storage_handle,
            owner,
            path_prefix,
            event_mask,
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(subscription) => NotificationTransportMessage::WatchCreated { subscription },
            Err(error) => NotificationTransportMessage::Reject(error.to_string()),
        },
        NotificationTransportMessage::DeleteWatch { owner, watch_id } => {
            match delete_watch_subscription(&context.storage_handle, owner, watch_id).await {
                Ok(()) => NotificationTransportMessage::WatchDeleted,
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::ListWatches { owner } => {
            match list_watch_subscriptions(&context.storage_handle, owner).await {
                Ok(subscriptions) => NotificationTransportMessage::WatchList { subscriptions },
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::DeliverAck { .. }
        | NotificationTransportMessage::ListResult { .. }
        | NotificationTransportMessage::UnreadCountResult { .. }
        | NotificationTransportMessage::MarkReadResult { .. }
        | NotificationTransportMessage::Reject(_)
        | NotificationTransportMessage::WatchCreated { .. }
        | NotificationTransportMessage::WatchDeleted
        | NotificationTransportMessage::WatchList { .. } => NotificationTransportMessage::Reject(
            "unexpected notification control message".to_string(),
        ),
    }
}

fn validate_inbound_batch(records: &[NotificationRecord], now_ms: u64) -> Result<(), String> {
    let mut direct_count = 0usize;
    for record in records {
        validate_inbound_record(record, now_ms)?;
        if record.class == NotificationClass::Direct {
            direct_count = direct_count.saturating_add(1);
            if direct_count > NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE {
                return Err(format!(
                    "direct notification batch count {direct_count} exceeds cap {NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_inbound_record(record: &NotificationRecord, now_ms: u64) -> Result<(), String> {
    if record.read_at_ms.is_some() {
        return Err("delivered notification records must be unread".to_string());
    }
    if record.created_at_ms > now_ms.saturating_add(NOTIFICATION_MAX_FUTURE_SKEW_MS) {
        return Err(format!(
            "notification created_at_ms {} is too far in the future",
            record.created_at_ms
        ));
    }
    if record.notification_id.is_nil() {
        return Err("notification record has empty notification_id".to_string());
    }
    if record.recipient.is_nil() {
        return Err("notification record has empty recipient".to_string());
    }
    validate_inbound_kind(&record.kind, record.recipient.realm_id)
}

fn validate_inbound_kind(kind: &NotificationKind, recipient_realm: RealmId) -> Result<(), String> {
    match kind {
        NotificationKind::AddedToGroup {
            group_id,
            actor_user_id,
        }
        | NotificationKind::RemovedFromGroup {
            group_id,
            actor_user_id,
        } => {
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
        NotificationKind::GroupMemberAdded {
            group_id,
            member_user_id,
            actor_user_id,
            ..
        } => {
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            validate_kind_user("member_user_id", member_user_id, recipient_realm)?;
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
        NotificationKind::NodeOnboarded { realm_id, .. } => {
            if *realm_id != recipient_realm {
                return Err(
                    "node onboarding notification realm must match recipient realm".to_string(),
                );
            }
        }
    }
    Ok(())
}

fn validate_kind_user(
    field: &str,
    user_id: &UserId,
    recipient_realm: RealmId,
) -> Result<(), String> {
    if user_id.is_nil() {
        return Err(format!("notification record has empty {field}"));
    }
    if user_id.realm_id != recipient_realm {
        return Err(format!(
            "notification record {field} realm must match recipient realm"
        ));
    }
    Ok(())
}

fn verify_batch_local_holder(
    records: &[NotificationRecord],
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    for record in records {
        verify_recipient_local_holder(&record.recipient, realm_config, local_node_id)?;
    }
    Ok(())
}

fn verify_recipient_local_holder(
    recipient: &UserId,
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    match resolve_inbox_holder(recipient, realm_config).map_err(|error| error.to_string())? {
        Some(holder) if holder == local_node_id => Ok(()),
        Some(holder) => Err(format!(
            "notification inbox recipient `{recipient}` is held by `{holder}`, not local node `{local_node_id}`"
        )),
        None => Err(format!(
            "no eligible notification inbox holder for recipient `{recipient}`"
        )),
    }
}

// The recipient realm is peer-asserted; an empty DeliverBatch has no record to
// derive it from and must be rejected before any indexing.
fn message_realm(message: &NotificationTransportMessage) -> Result<RealmId, String> {
    match message {
        NotificationTransportMessage::DeliverBatch { records } => {
            let Some(first) = records.first() else {
                return Err("empty batch".to_string());
            };
            let realm_id = first.recipient.realm_id;
            if records
                .iter()
                .any(|record| record.recipient.realm_id != realm_id)
            {
                return Err("mixed-realm batch".to_string());
            }
            Ok(realm_id)
        }
        NotificationTransportMessage::List { recipient, .. }
        | NotificationTransportMessage::UnreadCount { recipient }
        | NotificationTransportMessage::MarkRead { recipient, .. } => Ok(recipient.realm_id),
        NotificationTransportMessage::CreateWatch { owner, .. }
        | NotificationTransportMessage::DeleteWatch { owner, .. }
        | NotificationTransportMessage::ListWatches { owner } => Ok(owner.realm_id),
        NotificationTransportMessage::DeliverAck { .. }
        | NotificationTransportMessage::ListResult { .. }
        | NotificationTransportMessage::UnreadCountResult { .. }
        | NotificationTransportMessage::MarkReadResult { .. }
        | NotificationTransportMessage::Reject(_)
        | NotificationTransportMessage::WatchCreated { .. }
        | NotificationTransportMessage::WatchDeleted
        | NotificationTransportMessage::WatchList { .. } => {
            Err("unexpected notification control message".to_string())
        }
    }
}

// Deliberately stricter than the metadata `has_node` gate: only sync-eligible
// (server-class) realm nodes are trusted to assert recipient identity.
async fn authorize_peer(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, String> {
    if realm_id != *net_handle.realm_id() {
        return Err(format!(
            "notification peer `{peer}` addressed foreign realm `{realm_id}`"
        ));
    }
    let Some(config) = read_realm_config(context, realm_id).await? else {
        return Err(format!("realm `{realm_id}` config unavailable"));
    };
    let eligible = config
        .sync_eligible_node_ids()
        .map_err(|error| error.to_string())?;
    if eligible.contains(&peer) {
        Ok(config)
    } else {
        Err(format!(
            "notification peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
        ))
    }
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incoming::initialize_net_incoming;
    use crate::notifications::client::{
        create_watch_remote, delete_watch_remote, deliver_remote, list_remote, list_watches_remote,
        mark_read_remote, send_notification_request, unread_count_remote,
    };
    use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
    use aruna_core::structs::{
        Actor, NotificationClass, NotificationKind, NotificationRecord, RealmNodeKind,
        WatchEventKind, WatchEventMask,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
    use aruna_storage::FjallStorage;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct Node {
        _dir: TempDir,
        net: NetHandle,
        context: Arc<DriverContext>,
    }

    async fn spawn(realm_id: RealmId, secret: [u8; 32]) -> Node {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        initialize_net_incoming(context.clone());
        Node {
            _dir: dir,
            net,
            context,
        }
    }

    async fn connect(from: &Node, to: &Node) {
        from.net.add_peer_addr(to.net.endpoint_addr()).await;
    }

    async fn install_config(
        node: &Node,
        realm_id: RealmId,
        members: &[(NodeId, RealmNodeKind)],
    ) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for (node_id, kind) in members {
            config.ensure_node(*node_id, kind.clone());
        }
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        match node
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write event: {other:?}"),
        }
        config
    }

    fn recipient_for_holder(
        config: &RealmConfigDocument,
        holder: NodeId,
        realm_id: RealmId,
    ) -> UserId {
        for seed in 1..50_000u128 {
            let candidate = UserId::new(Ulid::from_bytes(seed.to_be_bytes()), realm_id);
            if resolve_inbox_holder(&candidate, config).expect("resolve holder") == Some(holder) {
                return candidate;
            }
        }
        panic!("no recipient resolved to holder {holder}");
    }

    async fn read_inbox(node: &Node) -> Vec<NotificationRecord> {
        match node
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| NotificationRecord::from_bytes(&value).expect("record decodes"))
                .collect(),
            other => panic!("unexpected inbox iter event: {other:?}"),
        }
    }

    fn record(recipient: UserId, seed: u8) -> NotificationRecord {
        NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::from_bytes([seed; 16]),
                actor_user_id: recipient,
            },
            1_700_000_000_000 + seed as u64,
        )
    }

    async fn delivery_pair(realm_seed: u8) -> (Node, Node, UserId) {
        let realm_id = RealmId::from_bytes([realm_seed; 32]);
        let a = spawn(realm_id, [realm_seed; 32]).await;
        let b = spawn(realm_id, [realm_seed.wrapping_add(1); 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;
        let recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        (a, b, recipient)
    }

    #[tokio::test]
    async fn deliver_remote_upserts_on_holder() {
        let realm_id = RealmId::from_bytes([40u8; 32]);
        let a = spawn(realm_id, [40u8; 32]).await;
        let b = spawn(realm_id, [41u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        let mut records = vec![record(recipient, 1), record(recipient, 2)];
        let written = deliver_remote(&a.net, b.net.node_id(), records.clone())
            .await
            .expect("delivery succeeds");
        assert_eq!(written, records.len() as u32);

        let mut inbox = read_inbox(&b).await;
        inbox.sort_by_key(|record| record.notification_id);
        records.sort_by_key(|record| record.notification_id);
        assert_eq!(inbox, records);
    }

    #[tokio::test]
    async fn duplicate_remote_delivery_is_idempotent() {
        let realm_id = RealmId::from_bytes([42u8; 32]);
        let a = spawn(realm_id, [42u8; 32]).await;
        let b = spawn(realm_id, [43u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        let records = vec![record(recipient, 1), record(recipient, 2)];

        let first = deliver_remote(&a.net, b.net.node_id(), records.clone())
            .await
            .expect("first delivery succeeds");
        assert_eq!(first, records.len() as u32);
        let second = deliver_remote(&a.net, b.net.node_id(), records.clone())
            .await
            .expect("second delivery succeeds");
        assert_eq!(second, 0);

        assert_eq!(read_inbox(&b).await.len(), records.len());
    }

    #[tokio::test]
    async fn unknown_peer_is_rejected() {
        let realm_id = RealmId::from_bytes([44u8; 32]);
        let a = spawn(realm_id, [44u8; 32]).await;
        let b = spawn(realm_id, [45u8; 32]).await;
        let c = spawn(realm_id, [46u8; 32]).await;
        connect(&c, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = UserId::new(Ulid::new(), realm_id);
        let error = deliver_remote(&c.net, b.net.node_id(), vec![record(recipient, 1)])
            .await
            .expect_err("unknown peer must be rejected");
        assert!(
            error.contains("not a sync-eligible node"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn mixed_realm_batch_is_rejected() {
        let realm_id = RealmId::from_bytes([47u8; 32]);
        let other_realm = RealmId::from_bytes([48u8; 32]);
        let a = spawn(realm_id, [47u8; 32]).await;
        let b = spawn(realm_id, [49u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let records = vec![
            record(UserId::new(Ulid::new(), realm_id), 1),
            record(UserId::new(Ulid::new(), other_realm), 2),
        ];
        let error = deliver_remote(&a.net, b.net.node_id(), records)
            .await
            .expect_err("mixed-realm batch must be rejected");
        assert!(
            error.contains("mixed-realm batch"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn batch_for_non_holder_is_rejected_without_partial_write() {
        let realm_id = RealmId::from_bytes([64u8; 32]);
        let a = spawn(realm_id, [64u8; 32]).await;
        let b = spawn(realm_id, [65u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let local_recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        let remote_recipient = recipient_for_holder(&config, a.net.node_id(), realm_id);
        let error = deliver_remote(
            &a.net,
            b.net.node_id(),
            vec![record(local_recipient, 1), record(remote_recipient, 2)],
        )
        .await
        .expect_err("batch containing non-local recipient must be rejected");
        assert!(
            error.contains("not local node"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn batch_with_read_record_is_rejected_without_partial_write() {
        let (a, b, recipient) = delivery_pair(70).await;
        let valid = record(recipient, 1);
        let mut read = record(recipient, 2);
        read.read_at_ms = Some(1_700_000_000_999);

        let error = deliver_remote(&a.net, b.net.node_id(), vec![valid, read])
            .await
            .expect_err("batch containing read record must be rejected");
        assert!(
            error.contains("must be unread"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn batch_with_future_created_at_is_rejected_without_partial_write() {
        let (a, b, recipient) = delivery_pair(72).await;
        let valid = record(recipient, 1);
        let mut future = record(recipient, 2);
        future.created_at_ms = unix_timestamp_millis()
            .saturating_add(NOTIFICATION_MAX_FUTURE_SKEW_MS)
            .saturating_add(60_000);

        let error = deliver_remote(&a.net, b.net.node_id(), vec![valid, future])
            .await
            .expect_err("batch containing future record must be rejected");
        assert!(
            error.contains("too far in the future"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn batch_with_invalid_kind_user_is_rejected_without_partial_write() {
        let (a, b, recipient) = delivery_pair(73).await;
        let valid = record(recipient, 1);
        let mut invalid = record(recipient, 2);
        invalid.kind = NotificationKind::AddedToGroup {
            group_id: Ulid::from_bytes([2u8; 16]),
            actor_user_id: UserId::nil(recipient.realm_id),
        };

        let error = deliver_remote(&a.net, b.net.node_id(), vec![valid, invalid])
            .await
            .expect_err("batch containing invalid kind user must be rejected");
        assert!(
            error.contains("empty actor_user_id"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn direct_cap_rejects_whole_batch_without_partial_write() {
        let (a, b, recipient) = delivery_pair(74).await;
        let records: Vec<_> = (0..=NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE)
            .map(|index| record(recipient, (index % 255 + 1) as u8))
            .collect();

        let error = deliver_remote(&a.net, b.net.node_id(), records)
            .await
            .expect_err("batch exceeding direct cap must be rejected");
        assert!(
            error.contains("exceeds cap"),
            "unexpected reject reason: {error}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn empty_batch_is_rejected() {
        let realm_id = RealmId::from_bytes([50u8; 32]);
        let a = spawn(realm_id, [50u8; 32]).await;
        let b = spawn(realm_id, [51u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let response = send_notification_request(
            &a.net,
            b.net.node_id(),
            NotificationTransportMessage::DeliverBatch { records: vec![] },
        )
        .await
        .expect("request completes");
        assert!(
            matches!(&response, NotificationTransportMessage::Reject(reason) if reason.contains("empty batch")),
            "unexpected response: {response:?}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    #[tokio::test]
    async fn user_kind_peer_is_rejected() {
        let realm_id = RealmId::from_bytes([52u8; 32]);
        let b = spawn(realm_id, [53u8; 32]).await;
        let c = spawn(realm_id, [54u8; 32]).await;
        connect(&c, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (b.net.node_id(), RealmNodeKind::Server),
                (c.net.node_id(), RealmNodeKind::User),
            ],
        )
        .await;

        let recipient = UserId::new(Ulid::new(), realm_id);
        let deliver_error = deliver_remote(&c.net, b.net.node_id(), vec![record(recipient, 1)])
            .await
            .expect_err("user-kind peer must be rejected");
        assert!(
            deliver_error.contains("not a sync-eligible node"),
            "unexpected reject reason: {deliver_error}"
        );

        let list = send_notification_request(
            &c.net,
            b.net.node_id(),
            NotificationTransportMessage::UnreadCount { recipient },
        )
        .await
        .expect("request completes");
        assert!(
            matches!(&list, NotificationTransportMessage::Reject(reason) if reason.contains("not a sync-eligible node")),
            "unexpected response: {list:?}"
        );
        assert!(read_inbox(&b).await.is_empty());
    }

    async fn seed_inbox(node: &Node, records: &[NotificationRecord]) {
        assert_eq!(
            upsert_inbox_records(&node.context.storage_handle, records).await,
            Ok(records.len())
        );
    }

    #[tokio::test]
    async fn rpc_list_roundtrip() {
        let realm_id = RealmId::from_bytes([55u8; 32]);
        let a = spawn(realm_id, [55u8; 32]).await;
        let b = spawn(realm_id, [56u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        let records = vec![
            record(recipient, 1),
            record(recipient, 2),
            record(recipient, 3),
        ];
        seed_inbox(&b, &records).await;

        let (page1, cursor) = list_remote(&a.net, b.net.node_id(), recipient, None, 2)
            .await
            .expect("first page");
        let cursor = cursor.expect("cursor for second page");
        let (page2, next) = list_remote(&a.net, b.net.node_id(), recipient, Some(cursor), 2)
            .await
            .expect("second page");
        assert_eq!(next, None);

        let seen: Vec<u64> = page1
            .iter()
            .chain(page2.iter())
            .map(|record| record.created_at_ms)
            .collect();
        assert_eq!(
            seen,
            vec![
                1_700_000_000_000 + 3,
                1_700_000_000_000 + 2,
                1_700_000_000_000 + 1,
            ]
        );
    }

    #[tokio::test]
    async fn rpc_unread_and_mark_read_roundtrip() {
        let realm_id = RealmId::from_bytes([59u8; 32]);
        let a = spawn(realm_id, [59u8; 32]).await;
        let b = spawn(realm_id, [60u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = recipient_for_holder(&config, b.net.node_id(), realm_id);
        let records = vec![
            record(recipient, 1),
            record(recipient, 2),
            record(recipient, 3),
        ];
        seed_inbox(&b, &records).await;

        assert_eq!(
            unread_count_remote(&a.net, b.net.node_id(), recipient)
                .await
                .expect("unread count"),
            (3, false)
        );

        let ids: Vec<Ulid> = records
            .iter()
            .map(|record| record.notification_id)
            .collect();
        assert_eq!(
            mark_read_remote(&a.net, b.net.node_id(), recipient, ids.clone(), None)
                .await
                .expect("mark read"),
            3
        );
        assert_eq!(
            mark_read_remote(&a.net, b.net.node_id(), recipient, ids, None)
                .await
                .expect("mark read again"),
            0
        );
        assert_eq!(
            unread_count_remote(&a.net, b.net.node_id(), recipient)
                .await
                .expect("unread count after"),
            (0, false)
        );
    }

    #[tokio::test]
    async fn rpc_mark_read_rejects_too_many_ids() {
        let (a, b, recipient) = delivery_pair(75).await;
        let ids = (0..=MARK_READ_MAX_IDS).map(|_| Ulid::new()).collect();

        let error = mark_read_remote(&a.net, b.net.node_id(), recipient, ids, None)
            .await
            .expect_err("too many ids must be rejected");
        assert!(
            error.contains("exceeds cap"),
            "unexpected reject reason: {error}"
        );
    }

    #[tokio::test]
    async fn rpc_read_path_still_gated() {
        let realm_id = RealmId::from_bytes([61u8; 32]);
        let a = spawn(realm_id, [61u8; 32]).await;
        let b = spawn(realm_id, [62u8; 32]).await;
        let c = spawn(realm_id, [63u8; 32]).await;
        connect(&c, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = UserId::new(Ulid::new(), realm_id);
        let error = list_remote(&c.net, b.net.node_id(), recipient, None, 10)
            .await
            .expect_err("unknown peer must be rejected on the read path");
        assert!(
            error.contains("not a sync-eligible node"),
            "unexpected reject reason: {error}"
        );
    }

    #[tokio::test]
    async fn rpc_inbox_ops_reject_non_local_holder() {
        let realm_id = RealmId::from_bytes([66u8; 32]);
        let a = spawn(realm_id, [66u8; 32]).await;
        let b = spawn(realm_id, [67u8; 32]).await;
        connect(&a, &b).await;
        let config = install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = recipient_for_holder(&config, a.net.node_id(), realm_id);
        let seeded = record(recipient, 1);
        seed_inbox(&b, std::slice::from_ref(&seeded)).await;

        for error in [
            list_remote(&a.net, b.net.node_id(), recipient, None, 10)
                .await
                .expect_err("list on non-holder must be rejected"),
            unread_count_remote(&a.net, b.net.node_id(), recipient)
                .await
                .expect_err("unread count on non-holder must be rejected"),
            mark_read_remote(
                &a.net,
                b.net.node_id(),
                recipient,
                vec![seeded.notification_id],
                None,
            )
            .await
            .expect_err("mark read on non-holder must be rejected"),
        ] {
            assert!(
                error.contains("not local node"),
                "unexpected reject reason: {error}"
            );
        }

        let inbox = read_inbox(&b).await;
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].read_at_ms, None);
    }

    #[tokio::test]
    async fn oversized_message_is_refused() {
        let realm_id = RealmId::from_bytes([57u8; 32]);
        let a = spawn(realm_id, [57u8; 32]).await;
        let b = spawn(realm_id, [58u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let recipient = UserId::new(Ulid::new(), realm_id);
        let sample = record(recipient, 1);
        let per_record = postcard::to_allocvec(&NotificationTransportMessage::DeliverBatch {
            records: vec![sample.clone(), sample.clone()],
        })
        .expect("encodes")
        .len()
            - postcard::to_allocvec(&NotificationTransportMessage::DeliverBatch {
                records: vec![sample.clone()],
            })
            .expect("encodes")
            .len();
        let count = crate::notifications::protocol::NOTIFICATION_MAX_MESSAGE_SIZE
            / per_record.max(1)
            + 1_000;
        let records = vec![sample; count];

        let error = deliver_remote(&a.net, b.net.node_id(), records)
            .await
            .expect_err("oversized message must be refused");
        assert!(
            error.contains("exceeds maximum size"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rpc_watch_crud_roundtrip() {
        let realm_id = RealmId::from_bytes([64u8; 32]);
        let a = spawn(realm_id, [64u8; 32]).await;
        let b = spawn(realm_id, [65u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let owner = UserId::new(Ulid::new(), realm_id);
        let mask = WatchEventMask::from_kinds([
            WatchEventKind::MetadataCreated,
            WatchEventKind::DataUploaded,
        ]);
        let created = create_watch_remote(
            &a.net,
            b.net.node_id(),
            owner,
            "/bucket/prefix".to_string(),
            mask,
        )
        .await
        .expect("create succeeds");
        assert_eq!(created.owner, owner);
        assert_eq!(created.path_prefix, "/bucket/prefix");
        assert_eq!(created.event_mask, mask);

        let listed = list_watches_remote(&a.net, b.net.node_id(), owner)
            .await
            .expect("list succeeds");
        assert_eq!(listed, vec![created.clone()]);

        delete_watch_remote(&a.net, b.net.node_id(), owner, created.watch_id)
            .await
            .expect("delete succeeds");
        assert!(
            list_watches_remote(&a.net, b.net.node_id(), owner)
                .await
                .expect("list after delete succeeds")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn watch_path_is_trust_gated() {
        let realm_id = RealmId::from_bytes([66u8; 32]);
        let b = spawn(realm_id, [67u8; 32]).await;
        let c = spawn(realm_id, [68u8; 32]).await;
        connect(&c, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (b.net.node_id(), RealmNodeKind::Server),
                (c.net.node_id(), RealmNodeKind::User),
            ],
        )
        .await;

        let owner = UserId::new(Ulid::new(), realm_id);
        let error = create_watch_remote(
            &c.net,
            b.net.node_id(),
            owner,
            "/bucket".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )
        .await
        .expect_err("non-eligible peer must be rejected");
        assert!(
            error.contains("not a sync-eligible node"),
            "unexpected reject reason: {error}"
        );
        assert!(
            list_watch_subscriptions(&b.context.storage_handle, owner)
                .await
                .expect("list succeeds")
                .is_empty()
        );
    }
}
