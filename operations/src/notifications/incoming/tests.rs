use super::*;
use crate::notifications::client::{
    create_watch_remote, delete_watch_remote, deliver_events_remote, deliver_remote, list_remote,
    list_watches_remote, mark_read_remote, send_notification_request, unread_count_remote,
};
use crate::notifications::inbox::upsert_inbox_records;
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_UNAUTHORIZED, create_local_watch, list_watch_subscriptions,
};
use crate::sync::incoming::initialize_net_incoming_for_tests;
use aruna_core::UserId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE,
    NOTIFICATION_WATCH_INTEREST_KEYSPACE,
};
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, NotificationClass, NotificationKind,
    NotificationRecord, PathRestriction, Permission, RealmAuthorizationDocument, RealmNodeKind,
    TokenRevocation, WatchAuthorizationBinding, WatchEvent, WatchEventDetail, WatchEventKind,
    WatchEventMask, interest_dirty_key, object_permission_path, watch_resource_path,
};
use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_storage::FjallStorage;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
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
        compute_handle: None,
    });
    initialize_net_incoming_for_tests(context.clone());
    Node {
        _dir: dir,
        net,
        context,
    }
}

async fn connect(from: &Node, to: &Node) {
    from.net.add_peer_addr(to.net.endpoint_addr()).await;
}

fn data_group_id() -> Ulid {
    Ulid::from_bytes([31u8; 16])
}

fn data_node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[31u8; 32]).public()
}

fn data_path(key: &str) -> String {
    watch_resource_path(data_group_id(), data_node_id(), "bucket", key)
}

/// Adds the record's creating token to the holder's replicated revocation set.
async fn revoke_watch_token(
    node: &Node,
    config: &RealmConfigDocument,
    record: &NotificationRecord,
) {
    let mut config = config.clone();
    config.revoked_tokens.push(TokenRevocation {
        token_hash: record
            .watch_authorization
            .as_ref()
            .expect("watch binding")
            .token_hash
            .clone(),
        expires_at: aruna_core::time::unix_timestamp_secs() + 600,
    });
    write_config(node, config.realm_id, &config).await;
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
    write_config(node, realm_id, &config).await;
    config
}

async fn write_config(node: &Node, realm_id: RealmId, config: &RealmConfigDocument) {
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
}

/// Registers the realm config with net admission, as production reloads do.
/// Without this, the first in-request reload closes provisional sessions.
async fn admit_peers(node: &Node, config: &RealmConfigDocument) {
    node.net
        .refresh_document_peers(config)
        .await
        .expect("refresh realm peers");
}

async fn install_watch_authorization(
    node: &Node,
    realm_id: RealmId,
    owner: UserId,
    readers: &[UserId],
) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let mut group_auth =
        GroupAuthorizationDocument::default_group_doc(owner, realm_id, data_group_id());
    group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer")
        .unwrap()
        .assigned_users
        .extend(readers.iter().copied());
    // Policy loading resolves the group record before group policies apply.
    let group = Group {
        display_name: "watch".to_string(),
        group_id: data_group_id(),
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    for (key_space, key, value) in [
        (
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        ),
        (
            AUTH_KEYSPACE,
            data_group_id().to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        ),
        (
            GROUP_KEYSPACE,
            data_group_id().to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
    ] {
        assert!(matches!(
            node.context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}

fn recipient_for_holder(config: &RealmConfigDocument, holder: NodeId, realm_id: RealmId) -> UserId {
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

fn watch_record(recipient: UserId, seed: u8) -> NotificationRecord {
    let key = format!("object-{seed}");
    let mut record = NotificationRecord::new(
        recipient,
        NotificationClass::Transient,
        NotificationKind::DataUploaded {
            path: data_path(&key),
            group_id: data_group_id(),
            node_id: data_node_id(),
            bucket: "bucket".to_string(),
            key,
            size_bytes: seed as u64,
            actor_user_id: recipient,
        },
        1_700_000_000_000 + seed as u64,
    );
    let authorization = WatchAuthorizationBinding {
        watch_path_prefix: data_path(""),
        ..Default::default()
    };
    record.watch_authorization = Some(authorization);
    record
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
async fn remote_upserts_deliver() {
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
async fn remote_delivery_idempotent() {
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
async fn unknown_peer_rejected() {
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

    let recipient = UserId::new(Ulid::generate(), realm_id);
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
async fn mixed_realm_rejected() {
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
        record(UserId::new(Ulid::generate(), realm_id), 1),
        record(UserId::new(Ulid::generate(), other_realm), 2),
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
async fn nonholder_batch_rejected() {
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
async fn read_batch_rejected() {
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
async fn future_batch_rejected() {
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
async fn invalid_user_rejected() {
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
async fn watch_batch_rejected() {
    let (a, b, recipient) = delivery_pair(83).await;
    let valid = record(recipient, 1);
    let mut invalid = record(recipient, 2);
    let group_id = Ulid::from_bytes([3u8; 16]);
    let node_id = data_node_id();
    invalid.kind = NotificationKind::DataUploaded {
        path: watch_resource_path(group_id, node_id, "bucket", "object"),
        group_id,
        node_id,
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        size_bytes: 0,
        actor_user_id: recipient,
    };

    let error = deliver_remote(&a.net, b.net.node_id(), vec![valid, invalid])
        .await
        .expect_err("generic batch containing watch data must be rejected");
    assert!(
        error.contains("must use watch event delivery"),
        "unexpected reject reason: {error}"
    );
    assert!(read_inbox(&b).await.is_empty());
}

#[tokio::test]
async fn direct_cap_rejected() {
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

#[test]
fn join_payload_validates() {
    let realm_id = RealmId::from_bytes([7; 32]);
    let actor_user_id = UserId::local(Ulid::from_bytes([1; 16]), realm_id);
    let group_id = Ulid::from_bytes([2; 16]);
    let valid = NotificationKind::GroupJoinRequested {
        group_id,
        request_id: Ulid::from_bytes([3; 16]),
        actor_user_id,
    };
    assert!(validate_inbound_kind(&valid, realm_id).is_ok());
    assert!(validate_inbound_kind(&valid, RealmId::from_bytes([8; 32])).is_err());
    let invalid = NotificationKind::GroupJoinRequested {
        group_id,
        request_id: Ulid::nil(),
        actor_user_id,
    };
    assert!(validate_inbound_kind(&invalid, realm_id).is_err());
}

#[test]
fn transient_batch_cap() {
    let realm_id = RealmId::from_bytes([75u8; 32]);
    let recipient = UserId::new(Ulid::generate(), realm_id);
    let mut records: Vec<_> = (0..=NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE)
        .map(|index| {
            let mut record = record(recipient, (index % 255 + 1) as u8);
            record.class = NotificationClass::Transient;
            record
        })
        .collect();

    let error = validate_inbound_batch(&records, unix_timestamp_millis())
        .expect_err("transient-only batch must use the total cap");
    assert!(error.contains("notification batch count"));
    records.truncate(NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE);
    assert!(validate_inbound_batch(&records, unix_timestamp_millis()).is_ok());
}

#[tokio::test]
async fn empty_batch_rejected() {
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
async fn user_peer_rejected() {
    let realm_id = RealmId::from_bytes([52u8; 32]);
    let b = spawn(realm_id, [53u8; 32]).await;
    let c = spawn(realm_id, [54u8; 32]).await;
    connect(&c, &b).await;
    install_config(
        &b,
        realm_id,
        &[
            (b.net.node_id(), RealmNodeKind::Server),
            (
                c.net.node_id(),
                RealmNodeKind::User {
                    owner: UserId::nil(realm_id),
                },
            ),
        ],
    )
    .await;

    let recipient = UserId::new(Ulid::generate(), realm_id);
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
async fn list_skips_revoked() {
    let realm_id = RealmId::from_bytes([83u8; 32]);
    let a = spawn(realm_id, [83u8; 32]).await;
    let b = spawn(realm_id, [84u8; 32]).await;
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
    install_watch_authorization(&b, realm_id, recipient, &[]).await;
    admit_peers(&b, &config).await;
    let direct = record(recipient, 1);
    let watch_two = watch_record(recipient, 2);
    let watch_three = watch_record(recipient, 3);
    seed_inbox(
        &b,
        &[direct.clone(), watch_two.clone(), watch_three.clone()],
    )
    .await;

    assert_eq!(
        list_remote(&a.net, b.net.node_id(), recipient, None, 10)
            .await
            .expect("authorized list")
            .0
            .len(),
        3
    );
    assert_eq!(
        unread_count_remote(&a.net, b.net.node_id(), recipient)
            .await
            .expect("authorized unread count"),
        (3, false)
    );
    let (first_page, retry_cursor) = list_remote(&a.net, b.net.node_id(), recipient, None, 1)
        .await
        .expect("first authorized page");
    assert_eq!(first_page, vec![watch_three]);
    let retry_cursor = retry_cursor.expect("cursor after first authorized page");
    install_watch_authorization(&b, realm_id, UserId::new(Ulid::generate(), realm_id), &[]).await;

    let (listed, next_cursor) = list_remote(&a.net, b.net.node_id(), recipient, None, 1)
        .await
        .expect("list after revocation");
    assert_eq!(listed, vec![direct]);
    assert_eq!(next_cursor, None);
    assert_eq!(
        unread_count_remote(&a.net, b.net.node_id(), recipient)
            .await
            .expect("unread count after revocation"),
        (1, false)
    );

    assert!(matches!(
        b.context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: data_group_id().to_bytes().to_vec().into(),
                value: vec![0xff].into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    list_remote(
        &a.net,
        b.net.node_id(),
        recipient,
        Some(retry_cursor.clone()),
        1,
    )
    .await
    .expect_err("authorization decode failure must reject the page");
    install_watch_authorization(&b, realm_id, recipient, &[]).await;
    assert_eq!(
        list_remote(&a.net, b.net.node_id(), recipient, Some(retry_cursor), 1,)
            .await
            .expect("same cursor retries after authorization recovers")
            .0,
        vec![watch_two]
    );
}

#[tokio::test]
async fn revoked_token_ignored() {
    let realm_id = RealmId::from_bytes([82u8; 32]);
    let a = spawn(realm_id, [82u8; 32]).await;
    let b = spawn(realm_id, [83u8; 32]).await;
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
    install_watch_authorization(&b, realm_id, recipient, &[]).await;
    admit_peers(&b, &config).await;
    let direct = record(recipient, 1);
    let watch = watch_record(recipient, 2);
    seed_inbox(&b, &[direct.clone(), watch.clone()]).await;

    revoke_watch_token(&b, &config, &watch).await;

    let listed = list_remote(&a.net, b.net.node_id(), recipient, None, 10)
        .await
        .expect("list after token revocation")
        .0;
    assert_eq!(listed, vec![watch.clone(), direct]);
    assert_eq!(
        unread_count_remote(&a.net, b.net.node_id(), recipient)
            .await
            .expect("unread count after token revocation"),
        (2, false)
    );
    assert_eq!(
        mark_read_remote(
            &a.net,
            b.net.node_id(),
            recipient,
            vec![watch.notification_id],
            None,
        )
        .await
        .expect("mark watch record"),
        1
    );
    assert!(
        read_inbox(&b)
            .await
            .into_iter()
            .find(|record| record.notification_id == watch.notification_id)
            .expect("stored watch record")
            .read_at_ms
            .is_some()
    );
}

#[tokio::test]
async fn restricted_binding_hidden() {
    let (a, b, recipient) = delivery_pair(80).await;
    install_watch_authorization(&b, recipient.realm_id, recipient, &[]).await;
    let mut watch = watch_record(recipient, 2);
    watch
        .watch_authorization
        .as_mut()
        .unwrap()
        .path_restrictions = Some(vec![PathRestriction {
        pattern: object_permission_path(
            recipient.realm_id,
            data_group_id(),
            data_node_id(),
            "bucket",
            "object-2",
        ),
        permission: Permission::READ,
    }]);
    seed_inbox(&b, &[watch.clone()]).await;

    assert!(
        list_remote(&a.net, b.net.node_id(), recipient, None, 10)
            .await
            .expect("list with restricted binding")
            .0
            .is_empty()
    );
}

#[tokio::test]
async fn unread_mark_roundtrip() {
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
async fn mark_limit_rejected() {
    let (a, b, recipient) = delivery_pair(75).await;
    let ids = (0..=MARK_READ_MAX_IDS).map(|_| Ulid::generate()).collect();

    let error = mark_read_remote(&a.net, b.net.node_id(), recipient, ids, None)
        .await
        .expect_err("too many ids must be rejected");
    assert!(
        error.contains("exceeds cap"),
        "unexpected reject reason: {error}"
    );
}

#[tokio::test]
async fn read_path_gated() {
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

    let recipient = UserId::new(Ulid::generate(), realm_id);
    let error = list_remote(&c.net, b.net.node_id(), recipient, None, 10)
        .await
        .expect_err("unknown peer must be rejected on the read path");
    assert!(
        error.contains("not a sync-eligible node"),
        "unexpected reject reason: {error}"
    );
}

#[tokio::test]
async fn inbox_nonholder_rejected() {
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
async fn oversized_message_refused() {
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

    let recipient = UserId::new(Ulid::generate(), realm_id);
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
    let count =
        crate::notifications::protocol::NOTIFICATION_MAX_MESSAGE_SIZE / per_record.max(1) + 1_000;
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
async fn watch_operations_roundtrip() {
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

    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    install_watch_authorization(&b, realm_id, owner, &[]).await;
    admit_peers(&b, &config).await;
    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    let prefix = data_path("prefix");
    let created = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        prefix.clone(),
        mask,
        WatchAuthorizationBinding {
            watch_path_prefix: prefix.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("create succeeds");
    assert_eq!(created.owner, owner);
    assert_eq!(created.path_prefix, prefix);
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
async fn watch_nonholder_rejected() {
    let realm_id = RealmId::from_bytes([68u8; 32]);
    let a = spawn(realm_id, [68u8; 32]).await;
    let b = spawn(realm_id, [69u8; 32]).await;
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

    let owner = recipient_for_holder(&config, a.net.node_id(), realm_id);
    let create_error = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding::default(),
    )
    .await
    .expect_err("create on non-holder must be rejected");
    assert!(
        create_error.contains("not local node"),
        "unexpected reject reason: {create_error}"
    );

    let list_error = list_watches_remote(&a.net, b.net.node_id(), owner)
        .await
        .expect_err("list on non-holder must be rejected");
    assert!(
        list_error.contains("not local node"),
        "unexpected reject reason: {list_error}"
    );

    let delete_error = delete_watch_remote(&a.net, b.net.node_id(), owner, Ulid::generate())
        .await
        .expect_err("delete on non-holder must be rejected");
    assert!(
        delete_error.contains("not local node"),
        "unexpected reject reason: {delete_error}"
    );
    assert!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("list succeeds")
            .is_empty()
    );
}

// A proxy cannot authorize a watch: the holder rechecks READ before persisting.
// An unauthorized owner is refused without a durable write.
#[tokio::test]
async fn create_requires_read() {
    let realm_id = RealmId::from_bytes([86u8; 32]);
    let a = spawn(realm_id, [86u8; 32]).await;
    let b = spawn(realm_id, [87u8; 32]).await;
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

    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    // The watched group exists and is readable, just not by `owner`.
    install_watch_authorization(&b, realm_id, UserId::new(Ulid::generate(), realm_id), &[]).await;
    admit_peers(&b, &config).await;

    let error = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        data_path("prefix"),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding::default(),
    )
    .await
    .expect_err("an owner without READ must be refused by the holder");
    assert_eq!(
        error,
        format!("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: permission_denied")
    );
    assert!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("list succeeds")
            .is_empty(),
        "an unauthorized create must not persist watch state"
    );
}

#[tokio::test]
async fn policy_denies_watch() {
    let realm_id = RealmId::from_bytes([88u8; 32]);
    let a = spawn(realm_id, [88u8; 32]).await;
    let b = spawn(realm_id, [89u8; 32]).await;
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
    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    install_watch_authorization(&b, realm_id, owner, &[]).await;
    admit_peers(&b, &config).await;
    let existing_prefix = data_path("existing");
    let existing = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        existing_prefix.clone(),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding {
            watch_path_prefix: existing_prefix,
            ..Default::default()
        },
    )
    .await
    .expect("existing watch creates");

    let mut denied = config;
    denied.request_policies.push(RequestPolicy {
        policy_id: Ulid::from_bytes([88u8; 16]),
        name: "deny-watches".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: "operation == 'notifications.create_watch'".to_string(),
        enabled: true,
    });
    write_config(&b, realm_id, &denied).await;

    let prefix = data_path("policy");
    let error = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        prefix.clone(),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding {
            watch_path_prefix: prefix,
            ..Default::default()
        },
    )
    .await
    .expect_err("the holder policy must deny forwarded create");
    assert_eq!(
        error,
        format!("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: permission_denied")
    );
    assert_eq!(
        list_watches_remote(&a.net, b.net.node_id(), owner)
            .await
            .expect("list remains available"),
        vec![existing.clone()]
    );
    delete_watch_remote(&a.net, b.net.node_id(), owner, existing.watch_id)
        .await
        .expect("delete remains available");
    assert!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("list succeeds")
            .is_empty()
    );
}

#[tokio::test]
async fn binding_path_rejected() {
    let realm_id = RealmId::from_bytes([89u8; 32]);
    let a = spawn(realm_id, [90u8; 32]).await;
    let b = spawn(realm_id, [91u8; 32]).await;
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
    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    install_watch_authorization(&b, realm_id, owner, &[]).await;

    let prefix = data_path("requested");
    let error = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        prefix,
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding {
            watch_path_prefix: data_path("bound"),
            ..Default::default()
        },
    )
    .await
    .expect_err("a binding for another path must be rejected");
    assert_eq!(
        error,
        format!("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: invalid_state")
    );
    assert!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("list succeeds")
            .is_empty()
    );
}

// Enumeration shares the delivery authorization result: once READ is revoked
// only an opaque cleanup id remains visible for the stored row.
#[tokio::test]
async fn revoked_watch_unlisted() {
    let realm_id = RealmId::from_bytes([84u8; 32]);
    let a = spawn(realm_id, [84u8; 32]).await;
    let b = spawn(realm_id, [85u8; 32]).await;
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

    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    install_watch_authorization(&b, realm_id, owner, &[]).await;
    admit_peers(&b, &config).await;
    let created = create_watch_remote(
        &a.net,
        b.net.node_id(),
        owner,
        data_path("prefix"),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        WatchAuthorizationBinding {
            watch_path_prefix: data_path("prefix"),
            ..Default::default()
        },
    )
    .await
    .expect("authorized create succeeds");
    assert_eq!(
        list_watches_remote(&a.net, b.net.node_id(), owner)
            .await
            .expect("list succeeds"),
        vec![created.clone()]
    );

    // Re-owning the group drops the original owner's READ.
    install_watch_authorization(&b, realm_id, UserId::new(Ulid::generate(), realm_id), &[]).await;

    let listed = list_watches_remote(&a.net, b.net.node_id(), owner)
        .await
        .expect("list after revocation succeeds");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].watch_id, created.watch_id);
    assert_eq!(listed[0].owner, owner);
    assert!(listed[0].path_prefix.is_empty());
    assert!(listed[0].event_mask.is_empty());
    assert_eq!(listed[0].created_at_ms, 0);
    assert_eq!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("stored row survives")
            .len(),
        1,
        "the row is filtered at the surface, not silently deleted"
    );
    delete_watch_remote(&a.net, b.net.node_id(), owner, listed[0].watch_id)
        .await
        .expect("redacted watch remains deletable");
    assert!(
        list_watch_subscriptions(&b.context.storage_handle, owner)
            .await
            .expect("list after delete succeeds")
            .is_empty()
    );
}

fn upload_event(realm_id: RealmId, actor: UserId, path: &str) -> WatchEvent {
    let (_, key) = path.split_once('/').expect("bucket/key fixture");
    WatchEvent {
        event_id: Ulid::from_bytes([7u8; 16]),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path: data_path(key),
        actor,
        occurred_at_ms: 1_700_000_000_000,
        detail: WatchEventDetail::DataUploaded {
            group_id: data_group_id(),
            node_id: data_node_id(),
            bucket: "bucket".to_string(),
            key: key.to_string(),
            size_bytes: 16,
        },
    }
}

#[test]
fn metadata_path_accepted() {
    let realm_id = RealmId::from_bytes([79u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let document_id = Ulid::generate();
    let mut event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::MetadataCreated,
        path: format!("meta/{group_id}/datasets/project/runs/run-42"),
        actor,
        occurred_at_ms: 1_000,
        detail: WatchEventDetail::MetadataCreated {
            group_id,
            document_id,
        },
    };

    assert!(validate_watch_event(&event, realm_id, 1_000).is_ok());

    event.path = format!("meta/{group_id}/datasets/project/");
    assert_eq!(
        validate_watch_event(&event, realm_id, 1_000),
        Err("watch event metadata path is not canonical".to_string())
    );

    event.path = format!("meta/{}/datasets/project", Ulid::generate());
    assert_eq!(
        validate_watch_event(&event, realm_id, 1_000),
        Err("watch event metadata path does not match detail".to_string())
    );
}

#[test]
fn watch_batch_caps() {
    let realm_id = RealmId::from_bytes([80u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let events = vec![
        upload_event(realm_id, actor, "bucket/object");
        NOTIFICATION_WATCH_EVENT_BATCH_SIZE + 1
    ];

    assert!(
        validate_watch_events(&events, realm_id, 1_700_000_000_000)
            .expect_err("oversized watch batch must be rejected")
            .contains("exceeds cap")
    );
}

#[test]
fn data_identity_required() {
    let realm_id = RealmId::from_bytes([78u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let mut event = upload_event(realm_id, actor, "bucket/object");

    assert!(
        validate_watch_event(&event, realm_id, event.occurred_at_ms).is_ok(),
        "canonical detail matches its path"
    );

    if let WatchEventDetail::DataUploaded { node_id, .. } = &mut event.detail {
        *node_id = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
    } else {
        panic!("canonical data detail expected");
    }
    assert_eq!(
        validate_watch_event(&event, realm_id, event.occurred_at_ms),
        Err("watch event data path does not match detail".to_string())
    );
}

#[test]
fn validates_sync_watch() {
    let realm_id = RealmId::from_bytes([77u8; 32]);
    let actor = UserId::new(Ulid::generate(), realm_id);
    let group_id = data_group_id();
    let node_id = data_node_id();
    let mut event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::SyncCompleted,
        path: watch_resource_path(group_id, node_id, "bucket", "prefix/"),
        actor,
        occurred_at_ms: 1_000,
        detail: WatchEventDetail::SyncCompleted {
            group_id,
            node_id,
            bucket: "bucket".to_string(),
            relationship_id: Ulid::generate(),
            versions_synced: 2,
        },
    };

    assert!(validate_watch_event(&event, realm_id, 1_000).is_ok());
    if let WatchEventDetail::SyncCompleted {
        relationship_id, ..
    } = &mut event.detail
    {
        *relationship_id = Ulid::nil();
    }
    assert!(validate_watch_event(&event, realm_id, 1_000).is_err());
}

#[tokio::test]
async fn events_expand_idempotently() {
    let realm_id = RealmId::from_bytes([70u8; 32]);
    let a = spawn(realm_id, [70u8; 32]).await;
    let b = spawn(realm_id, [71u8; 32]).await;
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

    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    let actor = UserId::new(Ulid::generate(), realm_id);
    install_watch_authorization(&b, realm_id, owner, &[]).await;
    admit_peers(&b, &config).await;
    create_local_watch(
        &b.context.storage_handle,
        owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    )
    .await
    .expect("holder subscription");

    let events = vec![upload_event(realm_id, actor, "bucket/object")];
    let first = deliver_events_remote(&a.net, b.net.node_id(), events.clone())
        .await
        .expect("first delivery succeeds");
    assert_eq!(first, 1);
    let second = deliver_events_remote(&a.net, b.net.node_id(), events)
        .await
        .expect("redelivery succeeds");
    assert_eq!(second, 0);

    let inbox = read_inbox(&b).await;
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].recipient, owner);
    assert!(matches!(
        inbox[0].kind,
        NotificationKind::DataUploaded { .. }
    ));
}

#[tokio::test]
async fn stale_subscription_skipped() {
    let realm_id = RealmId::from_bytes([71u8; 32]);
    let a = spawn(realm_id, [72u8; 32]).await;
    let b = spawn(realm_id, [73u8; 32]).await;
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

    let local_owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    let stale_owner = recipient_for_holder(&config, a.net.node_id(), realm_id);
    install_watch_authorization(&b, realm_id, local_owner, &[stale_owner]).await;
    // Before the new member joined, both owners were held locally.
    install_config(&b, realm_id, &[(b.net.node_id(), RealmNodeKind::Server)]).await;
    create_local_watch(
        &b.context.storage_handle,
        local_owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    )
    .await
    .expect("local subscription fixture");
    create_local_watch(
        &b.context.storage_handle,
        stale_owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    )
    .await
    .expect("stale subscription fixture");
    let dirty_key = interest_dirty_key(realm_id);
    match b
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: dirty_key.clone().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        other => panic!("unexpected dirty marker delete result: {other:?}"),
    }
    // Adding node A re-ranks only `stale_owner` away from node B.
    let config = install_config(
        &b,
        realm_id,
        &[
            (a.net.node_id(), RealmNodeKind::Server),
            (b.net.node_id(), RealmNodeKind::Server),
        ],
    )
    .await;
    admit_peers(&b, &config).await;

    let actor = UserId::new(Ulid::generate(), realm_id);
    let written = deliver_events_remote(
        &a.net,
        b.net.node_id(),
        vec![upload_event(realm_id, actor, "bucket/object")],
    )
    .await
    .expect("valid local subscription must still expand");
    assert_eq!(written, 1);
    let inbox = read_inbox(&b).await;
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].recipient, local_owner);
    match b
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: dirty_key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
        other => panic!("stale subscription did not re-dirty interest: {other:?}"),
    }
}

#[tokio::test]
async fn kind_mismatch_rejected() {
    let realm_id = RealmId::from_bytes([73u8; 32]);
    let a = spawn(realm_id, [74u8; 32]).await;
    let b = spawn(realm_id, [75u8; 32]).await;
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

    let owner = recipient_for_holder(&config, b.net.node_id(), realm_id);
    create_local_watch(
        &b.context.storage_handle,
        owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    )
    .await
    .expect("holder subscription");

    let actor = UserId::new(Ulid::generate(), realm_id);
    let mut event = upload_event(realm_id, actor, "bucket/object");
    event.kind = WatchEventKind::MetadataCreated;
    let error = deliver_events_remote(&a.net, b.net.node_id(), vec![event])
        .await
        .expect_err("kind/detail mismatch must be rejected");
    assert!(
        error.contains("kind does not match detail"),
        "unexpected reject reason: {error}"
    );
    assert!(read_inbox(&b).await.is_empty());
}

#[tokio::test]
async fn delivery_trust_gated() {
    let realm_id = RealmId::from_bytes([72u8; 32]);
    let b = spawn(realm_id, [73u8; 32]).await;
    let c = spawn(realm_id, [74u8; 32]).await;
    connect(&c, &b).await;
    install_config(
        &b,
        realm_id,
        &[
            (b.net.node_id(), RealmNodeKind::Server),
            (
                c.net.node_id(),
                RealmNodeKind::User {
                    owner: UserId::nil(realm_id),
                },
            ),
        ],
    )
    .await;

    let owner = UserId::new(Ulid::generate(), realm_id);
    create_local_watch(
        &b.context.storage_handle,
        owner,
        data_path(""),
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        1,
    )
    .await
    .expect("holder subscription");

    let actor = UserId::new(Ulid::generate(), realm_id);
    let error = deliver_events_remote(
        &c.net,
        b.net.node_id(),
        vec![upload_event(realm_id, actor, "bucket/object")],
    )
    .await
    .expect_err("non-eligible peer must be rejected");
    assert!(
        error.contains("not a sync-eligible node"),
        "unexpected reject reason: {error}"
    );
    assert!(read_inbox(&b).await.is_empty());
}

#[tokio::test]
async fn invalid_events_rejected() {
    let realm_id = RealmId::from_bytes([75u8; 32]);
    let other_realm = RealmId::from_bytes([76u8; 32]);
    let a = spawn(realm_id, [75u8; 32]).await;
    let b = spawn(realm_id, [77u8; 32]).await;
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

    let empty = send_notification_request(
        &a.net,
        b.net.node_id(),
        NotificationTransportMessage::DeliverWatchEvents { events: vec![] },
    )
    .await
    .expect("request completes");
    assert!(
        matches!(&empty, NotificationTransportMessage::Reject(reason) if reason.contains("empty batch")),
        "unexpected response: {empty:?}"
    );

    let actor = UserId::new(Ulid::generate(), realm_id);
    let mixed = deliver_events_remote(
        &a.net,
        b.net.node_id(),
        vec![
            upload_event(realm_id, actor, "bucket/object"),
            upload_event(other_realm, actor, "bucket/object"),
        ],
    )
    .await
    .expect_err("mixed-realm batch must be rejected");
    assert!(
        mixed.contains("mixed-realm batch"),
        "unexpected reject reason: {mixed}"
    );
}

#[tokio::test]
async fn path_trust_gated() {
    let realm_id = RealmId::from_bytes([66u8; 32]);
    let b = spawn(realm_id, [67u8; 32]).await;
    let c = spawn(realm_id, [68u8; 32]).await;
    connect(&c, &b).await;
    install_config(
        &b,
        realm_id,
        &[
            (b.net.node_id(), RealmNodeKind::Server),
            (
                c.net.node_id(),
                RealmNodeKind::User {
                    owner: UserId::nil(realm_id),
                },
            ),
        ],
    )
    .await;

    let owner = UserId::new(Ulid::generate(), realm_id);
    let error = create_watch_remote(
        &c.net,
        b.net.node_id(),
        owner,
        "bucket".to_string(),
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        WatchAuthorizationBinding::default(),
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

#[tokio::test]
async fn fresh_delivery_wakes() {
    let realm_id = RealmId::from_bytes([80u8; 32]);
    let a = spawn(realm_id, [80u8; 32]).await;
    let b = spawn(realm_id, [81u8; 32]).await;
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
    let batch = vec![record(recipient, 1)];
    let mut wakes = b.net.subscribe_notification_wakes();

    deliver_remote(&a.net, b.net.node_id(), batch.clone())
        .await
        .expect("first delivery succeeds");
    let woken = timeout(Duration::from_secs(1), wakes.recv())
        .await
        .expect("wake arrives")
        .expect("channel open");
    assert_eq!(woken, recipient);

    deliver_remote(&a.net, b.net.node_id(), batch.clone())
        .await
        .expect("redelivery succeeds");
    // The holder wakes before writing the ack, so the awaited reply orders
    // this emptiness check after any wake the redelivery could have sent.
    assert!(wakes.is_empty(), "redelivery must not wake");
}

#[tokio::test]
async fn changed_count_wakes() {
    let realm_id = RealmId::from_bytes([82u8; 32]);
    let a = spawn(realm_id, [82u8; 32]).await;
    let b = spawn(realm_id, [83u8; 32]).await;
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
    seed_inbox(&b, &records).await;
    let ids: Vec<Ulid> = records
        .iter()
        .map(|record| record.notification_id)
        .collect();
    let mut wakes = b.net.subscribe_notification_wakes();

    assert_eq!(
        mark_read_remote(&a.net, b.net.node_id(), recipient, ids.clone(), None)
            .await
            .expect("mark read"),
        2
    );
    let woken = timeout(Duration::from_secs(1), wakes.recv())
        .await
        .expect("wake arrives")
        .expect("channel open");
    assert_eq!(woken, recipient);

    assert_eq!(
        mark_read_remote(&a.net, b.net.node_id(), recipient, ids, None)
            .await
            .expect("mark read again"),
        0
    );
    // The holder wakes before writing the ack, so the awaited reply orders
    // this emptiness check after any wake the no-op mark-read could send.
    assert!(wakes.is_empty(), "no-op mark-read must not wake");
}
