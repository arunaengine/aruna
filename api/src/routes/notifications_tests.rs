use super::*;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::metrics::NodeMetrics;
use aruna_core::structs::{
    Actor, BucketInfo, GroupAuthorizationDocument, NodeCapabilities, PathRestriction, Permission,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind, WatchEvent,
    WatchEventDetail, watch_resource_path,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::notifications::inbox::upsert_inbox_records;
use aruna_operations::notifications::routing::route_watch_event;
use aruna_operations::notifications::watch::subscriptions::list_watch_subscriptions;
use aruna_storage::storage::FjallStorage;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use byteview::ByteView;
use std::time::SystemTime;
use tempfile::TempDir;
use tokio::time::timeout;

fn node(seed: u8) -> NodeId {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    iroh::SecretKey::from_bytes(&bytes).public()
}

fn realm_id(seed: u8) -> RealmId {
    RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .as_bytes(),
    )
}

async fn build_state(realm_id: RealmId, node_id: NodeId) -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let state = ServerState::new(
        ctx,
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).expect("capabilities"),
        false,
        None,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}

async fn build_network_state(
    realm_id: RealmId,
    secret: [u8; 32],
) -> (TempDir, Arc<ServerState>, NetHandle) {
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
    let node_id = net.node_id();
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let state = ServerState::new(
        ctx,
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).expect("capabilities"),
        false,
        None,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state), net)
}

async fn install_holder_config(state: &ServerState, realm_id: RealmId, holder: NodeId) {
    install_holder_policies(state, realm_id, holder, Vec::new()).await;
}

async fn install_holder_policies(
    state: &ServerState,
    realm_id: RealmId,
    holder: NodeId,
    policies: Vec<aruna_core::request_policy::RequestPolicy>,
) {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(holder, RealmNodeKind::Server);
    config.seed_job_control(holder, 0);
    config.request_policies = policies;
    let actor = Actor {
        node_id: holder,
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    match state
        .get_ctx()
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

async fn write_fixture(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected fixture write event: {other:?}"),
    }
}

async fn install_group_authorization(
    state: &ServerState,
    realm_id: RealmId,
    group_id: Ulid,
    owner: UserId,
    readers: &[UserId],
) {
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: owner,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let viewer = group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer")
        .expect("default viewer role");
    viewer.assigned_users.extend(readers.iter().copied());

    write_fixture(
        state,
        AUTH_KEYSPACE,
        ByteView::from(realm_id.as_bytes().to_vec()),
        ByteView::from(realm_auth.to_bytes(&actor).expect("realm auth serializes")),
    )
    .await;
    write_fixture(
        state,
        AUTH_KEYSPACE,
        ByteView::from(group_id.to_bytes().to_vec()),
        ByteView::from(group_auth.to_bytes(&actor).expect("group auth serializes")),
    )
    .await;
    // Policy loading resolves the group record before group policies apply.
    let group = aruna_core::structs::Group {
        display_name: "watch-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    write_fixture(
        state,
        aruna_core::keyspaces::GROUP_KEYSPACE,
        ByteView::from(group_id.to_bytes().to_vec()),
        ByteView::from(group.to_bytes(&actor).expect("group serializes")),
    )
    .await;
}

async fn install_bucket(state: &ServerState, bucket: &str, group_id: Ulid, created_by: UserId) {
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::now(),
        created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_fixture(
        state,
        S3_BUCKET_KEYSPACE,
        ByteView::from(bucket.as_bytes().to_vec()),
        ByteView::from(info.to_bytes().expect("bucket serializes")),
    )
    .await;
}

fn direct_record(recipient: UserId, seed: u8) -> NotificationRecord {
    NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::from_bytes([seed; 16]),
            actor_user_id: UserId::new(Ulid::from_bytes([200u8; 16]), recipient.realm_id),
        },
        1_700_000_000_000 + seed as u64,
    )
}

fn auth_for(user_id: UserId, realm_id: RealmId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

fn bearer() -> Extension<Option<ValidatedBearer>> {
    Extension(Some(ValidatedBearer::new_for_test(
        "notification-watch-test-token",
    )))
}

#[tokio::test]
async fn list_requires_auth() {
    let realm_id = realm_id(1);
    let (_dir, state) = build_state(realm_id, node(1)).await;
    let error = list_notifications(
        State(state),
        Extension(None),
        Query(ListNotificationsQuery::default()),
    )
    .await
    .expect_err("missing auth must be rejected");
    assert!(matches!(error, ServerError::Unauthorized));
}

#[tokio::test]
async fn restricted_token_rejected() {
    let realm_id = realm_id(5);
    let (_dir, state) = build_state(realm_id, node(5)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let mut auth = auth_for(user_id, realm_id);
    auth.path_restrictions = Some(vec![PathRestriction {
        pattern: "/bucket/**".to_string(),
        permission: Permission::READ,
    }]);

    let list_err = list_notifications(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Query(ListNotificationsQuery::default()),
    )
    .await
    .expect_err("delegated token must be rejected");
    assert!(matches!(list_err, ServerError::Forbidden));

    let unread_err = unread_count(State(state.clone()), Extension(Some(auth.clone())))
        .await
        .expect_err("delegated token must be rejected");
    assert!(matches!(unread_err, ServerError::Forbidden));

    let mark_err = mark_read(
        State(state),
        Extension(Some(auth)),
        Json(MarkReadRequest {
            ids: Vec::new(),
            up_to_ms: None,
        }),
    )
    .await
    .expect_err("delegated token must be rejected");
    assert!(matches!(mark_err, ServerError::Forbidden));
}

#[test]
fn cursor_rejects_garbage() {
    assert!(matches!(
        decode_cursor(Some("not base64 !!")),
        Err(ServerError::BadRequest)
    ));
    let short = URL_SAFE_NO_PAD.encode([0u8; 23]);
    assert!(matches!(
        decode_cursor(Some(&short)),
        Err(ServerError::BadRequest)
    ));
    assert_eq!(decode_cursor(None).unwrap(), None);

    let raw = vec![7u8; 24];
    let encoded = encode_cursor(Some(raw.clone()));
    assert_eq!(decode_cursor(encoded.as_deref()).unwrap(), Some(raw));
}

#[test]
fn missing_ids_default() {
    let request: MarkReadRequest =
        serde_json::from_str(r#"{"up_to_ms":123}"#).expect("request deserializes");

    assert!(request.ids.is_empty());
    assert_eq!(request.up_to_ms, Some(123));
}

#[tokio::test]
async fn bad_ids_rejected() {
    let realm_id = realm_id(2);
    let (_dir, state) = build_state(realm_id, node(2)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let error = mark_read(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        Json(MarkReadRequest {
            ids: vec!["not-a-ulid".to_string()],
            up_to_ms: None,
        }),
    )
    .await
    .expect_err("bad id must be rejected");
    assert!(matches!(error, ServerError::BadRequest));
}

#[tokio::test]
async fn excess_ids_rejected() {
    let realm_id = realm_id(6);
    let (_dir, state) = build_state(realm_id, node(6)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let error = mark_read(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        Json(MarkReadRequest {
            ids: (0..=MARK_READ_MAX_IDS)
                .map(|_| Ulid::generate().to_string())
                .collect(),
            up_to_ms: None,
        }),
    )
    .await
    .expect_err("too many ids must be rejected");
    assert!(matches!(error, ServerError::BadRequest));
}

#[tokio::test]
async fn local_path_serves() {
    let realm_id = realm_id(3);
    let holder = node(3);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;

    let user_id = UserId::new(Ulid::generate(), realm_id);
    let records = vec![
        direct_record(user_id, 1),
        direct_record(user_id, 2),
        direct_record(user_id, 3),
    ];
    upsert_inbox_records(&state.get_ctx().storage_handle, &records)
        .await
        .expect("seed inbox");

    let (_, listed) = list_notifications(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        Query(ListNotificationsQuery::default()),
    )
    .await
    .expect("list succeeds");
    assert_eq!(listed.notifications.len(), 3);
    assert_eq!(listed.notifications[0].created_at_ms, 1_700_000_000_000 + 3);
    assert!(listed.notifications.iter().all(|n| !n.read));

    let (_, unread) = unread_count(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
    )
    .await
    .expect("unread succeeds");
    assert_eq!(unread.count, 3);
    assert!(!unread.capped);

    let ids = listed
        .notifications
        .iter()
        .map(|n| n.id.clone())
        .collect::<Vec<_>>();
    let (_, marked) = mark_read(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        Json(MarkReadRequest {
            ids,
            up_to_ms: None,
        }),
    )
    .await
    .expect("mark read succeeds");
    assert_eq!(marked.marked, 3);

    let (_, unread_after) =
        unread_count(State(state), Extension(Some(auth_for(user_id, realm_id))))
            .await
            .expect("unread after succeeds");
    assert_eq!(unread_after.count, 0);
}

#[tokio::test]
async fn local_stream_emits() {
    let realm_id = realm_id(11);
    let (_dir, state, net) = build_network_state(realm_id, [11u8; 32]).await;
    install_holder_config(&state, realm_id, state.get_node_id()).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);

    let mut events = Box::pin(unread_count_stream(
        state.get_ctx(),
        state.get_node_id(),
        user_id,
        UnreadStreamMode::Local(net.subscribe_notification_wakes()),
        CancellationToken::new(),
        Duration::from_secs(5),
        // Long recheck so this test exercises only the wake path.
        Duration::from_secs(60),
    ));

    let (initial, initial_capped) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("initial event arrives")
        .expect("stream open");
    assert_eq!(initial, 0);
    assert!(!initial_capped);

    upsert_inbox_records(
        &state.get_ctx().storage_handle,
        &[direct_record(user_id, 1)],
    )
    .await
    .expect("seed inbox");
    net.notify_inbox_activity(user_id);

    let (after_wake, after_wake_capped) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("wake event arrives")
        .expect("stream open");
    assert_eq!(after_wake, 1);
    assert!(!after_wake_capped);
}

#[tokio::test]
async fn state_stream_initial() {
    let (_changes, revisions) = watch::channel(3);
    let unread = stream::iter([(4, false)]).chain(stream::pending());
    let mut states = Box::pin(notification_state_stream(
        unread,
        "test-epoch".to_string(),
        revisions,
        Duration::from_secs(20),
    ));

    assert_eq!(
        states.next().await,
        Some(NotificationStreamResponse {
            epoch: "test-epoch".to_string(),
            revision: 3,
            unread: UnreadCountResponse {
                count: 4,
                capped: false,
            },
        })
    );
}

#[tokio::test]
async fn state_stream_updates() {
    let (changes, revisions) = watch::channel(3);
    let unread = stream::iter([(4, false), (5, true)]).chain(stream::pending());
    let mut states = Box::pin(notification_state_stream(
        unread,
        "test-epoch".to_string(),
        revisions,
        Duration::from_secs(20),
    ));

    assert_eq!(states.next().await.expect("initial state").revision, 3);
    assert_eq!(
        states.next().await.expect("unread state").unread,
        UnreadCountResponse {
            count: 5,
            capped: true,
        }
    );
    changes.send_replace(4);
    assert_eq!(states.next().await.expect("dashboard state").revision, 4);
}

#[tokio::test]
async fn state_stream_closes() {
    let (_changes, revisions) = watch::channel(3);
    let unread = stream::iter([(4, false)]);
    let mut states = Box::pin(notification_state_stream(
        unread,
        "test-epoch".to_string(),
        revisions,
        Duration::from_secs(20),
    ));

    assert!(states.next().await.is_some());
    assert_eq!(states.next().await, None);
}

#[tokio::test]
async fn wake_emits_unchanged() {
    // A wake repeats the aggregate at the unread cap; the frame must still fire.
    let (_changes, revisions) = watch::channel(3);
    let unread = stream::iter([(100, true), (100, true)]).chain(stream::pending());
    let mut states = Box::pin(notification_state_stream(
        unread,
        "test-epoch".to_string(),
        revisions,
        Duration::from_secs(20),
    ));

    let capped = UnreadCountResponse {
        count: 100,
        capped: true,
    };
    assert_eq!(states.next().await.expect("initial state").unread, capped);
    assert_eq!(states.next().await.expect("wake state").unread, capped);
}

#[tokio::test]
async fn state_stream_periodic() {
    tokio::time::pause();
    let (_changes, revisions) = watch::channel(3);
    let unread = stream::iter([(4, false)]).chain(stream::pending());
    let mut states = Box::pin(notification_state_stream(
        unread,
        "test-epoch".to_string(),
        revisions,
        Duration::from_secs(20),
    ));
    let initial = states.next().await.expect("initial state");

    tokio::time::advance(Duration::from_secs(20)).await;

    assert_eq!(states.next().await, Some(initial));
}

#[tokio::test]
async fn state_event_shape() {
    use axum::response::IntoResponse;

    let event = state_event(NotificationStreamResponse {
        epoch: "test-epoch".to_string(),
        revision: 3,
        unread: UnreadCountResponse {
            count: 4,
            capped: false,
        },
    });
    let response = Sse::new(stream::iter([Ok::<_, Infallible>(event)])).into_response();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("SSE body");

    assert_eq!(
            body.as_ref(),
            b"event: state\ndata: {\"epoch\":\"test-epoch\",\"revision\":3,\"unread\":{\"count\":4,\"capped\":false}}\n\n"
        );
}

// This single-node case verifies missed-wake refetch; multi-node tests cover holder moves.
#[tokio::test]
async fn local_recheck_refetches() {
    let realm_id = realm_id(12);
    let (_dir, state, net) = build_network_state(realm_id, [12u8; 32]).await;
    install_holder_config(&state, realm_id, state.get_node_id()).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);

    let mut events = Box::pin(unread_count_stream(
        state.get_ctx(),
        state.get_node_id(),
        user_id,
        UnreadStreamMode::Local(net.subscribe_notification_wakes()),
        CancellationToken::new(),
        Duration::from_secs(60),
        // Short recheck so the backstop refetch fires promptly.
        Duration::from_millis(150),
    ));

    let (initial, _) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("initial event arrives")
        .expect("stream open");
    assert_eq!(initial, 0);

    // Seed a record but deliberately fire no wake: only the recheck backstop
    // can surface it.
    upsert_inbox_records(
        &state.get_ctx().storage_handle,
        &[direct_record(user_id, 1)],
    )
    .await
    .expect("seed inbox");

    let (after_recheck, _) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("recheck backstop emits without a wake")
        .expect("stream open");
    assert_eq!(after_recheck, 1);
}

#[tokio::test]
async fn expired_recheck_wins() {
    let realm_id = realm_id(15);
    let recipient = UserId::new(Ulid::generate(), realm_id);
    let unrelated = UserId::new(Ulid::generate(), realm_id);
    let (tx, mut rx) = tokio::sync::broadcast::channel(4);
    tx.send(unrelated).expect("receiver is open");

    let step = next_local_step(
        &mut rx,
        recipient,
        Instant::now() - Duration::from_millis(1),
    )
    .await;

    assert_eq!(step, StreamStep::Recheck);
}

// The remote arm polls the holder and emits only on change: the initial poll
// reports the current count, a later poll reports it again once it moved.
#[tokio::test]
async fn remote_stream_emits() {
    let realm_id = realm_id(13);
    let (_dir, state, _net) = build_network_state(realm_id, [13u8; 32]).await;
    install_holder_config(&state, realm_id, state.get_node_id()).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);

    let mut events = Box::pin(unread_count_stream(
        state.get_ctx(),
        state.get_node_id(),
        user_id,
        UnreadStreamMode::Remote,
        CancellationToken::new(),
        Duration::from_millis(100),
        Duration::from_secs(60),
    ));

    let (initial, _) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("initial event arrives")
        .expect("stream open");
    assert_eq!(initial, 0);

    upsert_inbox_records(
        &state.get_ctx().storage_handle,
        &[direct_record(user_id, 1)],
    )
    .await
    .expect("seed inbox");

    let (changed, _) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("poll emits once the count changed")
        .expect("stream open");
    assert_eq!(changed, 1);
}

// The stream must end promptly once node shutdown begins: an open SSE
// response otherwise pins the ingress drain until the client disconnects.
#[tokio::test]
async fn shutdown_ends_stream() {
    let realm_id = realm_id(16);
    let (_dir, state, net) = build_network_state(realm_id, [16u8; 32]).await;
    install_holder_config(&state, realm_id, state.get_node_id()).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let shutdown = CancellationToken::new();

    let mut events = Box::pin(unread_count_stream(
        state.get_ctx(),
        state.get_node_id(),
        user_id,
        UnreadStreamMode::Local(net.subscribe_notification_wakes()),
        shutdown.clone(),
        // Both waits are far longer than the test: only the token can end it.
        Duration::from_secs(60),
        Duration::from_secs(60),
    ));

    let (initial, _) = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("initial event arrives")
        .expect("stream open");
    assert_eq!(initial, 0);

    shutdown.cancel();

    let ended = timeout(Duration::from_secs(2), events.next())
        .await
        .expect("stream reacts to shutdown promptly");
    assert!(ended.is_none(), "stream must end, got {ended:?}");
}

// A poll that cannot reach the holder is skipped silently: the stream neither
// emits nor ends, it just keeps polling.
#[tokio::test]
async fn remote_poll_skips() {
    let realm_id = realm_id(14);
    let (_dir, state, _net) = build_network_state(realm_id, [14u8; 32]).await;
    // The holder is a node that is in no mesh, so every remote poll fails.
    install_holder_config(&state, realm_id, node(200)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);

    let mut events = Box::pin(unread_count_stream(
        state.get_ctx(),
        state.get_node_id(),
        user_id,
        UnreadStreamMode::Remote,
        CancellationToken::new(),
        Duration::from_millis(100),
        Duration::from_secs(60),
    ));

    // Nothing is ever emitted and the stream stays open (times out) rather than
    // ending on the unreachable-holder errors.
    let outcome = timeout(Duration::from_millis(800), events.next()).await;
    assert!(
        outcome.is_err(),
        "a failing poll must be skipped silently, not emitted or ended: {outcome:?}"
    );
}

#[test]
fn response_maps_links() {
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let recipient = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let actor = UserId::new(Ulid::generate(), realm_id);
    let member = UserId::new(Ulid::generate(), realm_id);
    let onboarded_node = node(9);

    let request_id = Ulid::generate();
    let joined = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::GroupJoinRequested {
            group_id,
            request_id,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(joined.kind, "group_join_requested");
    assert_eq!(joined.category, "group.membership");
    assert_eq!(joined.request_id, Some(request_id.to_string()));
    assert_eq!(joined.group_id, Some(group_id.to_string()));
    assert_eq!(joined.actor_user_id, Some(actor.to_string()));
    let added = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(added.group_id, Some(group_id.to_string()));
    assert_eq!(added.actor_user_id, Some(actor.to_string()));
    assert_eq!(added.member_user_id, None);

    let removed = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::RemovedFromGroup {
            group_id,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(removed.group_id, Some(group_id.to_string()));
    assert_eq!(removed.actor_user_id, Some(actor.to_string()));

    let member_added = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::GroupMemberAdded {
            group_id,
            member_user_id: member,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(member_added.group_id, Some(group_id.to_string()));
    assert_eq!(member_added.member_user_id, Some(member.to_string()));
    assert_eq!(member_added.actor_user_id, Some(actor.to_string()));

    let onboarded = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::NodeOnboarded {
            realm_id,
            node_id: onboarded_node,
        },
        1,
    ));
    assert_eq!(onboarded.realm_id, Some(realm_id.to_string()));
    assert_eq!(onboarded.node_id, Some(onboarded_node.to_string()));
    assert_eq!(onboarded.group_id, None);

    let document_id = Ulid::generate();
    let metadata_created = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Transient,
        NotificationKind::MetadataCreated {
            path: format!("meta/{group_id}/datasets/project/run-42"),
            group_id,
            document_id,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(metadata_created.kind, "metadata_created");
    assert_eq!(metadata_created.category, "resource.watch");
    assert_eq!(
        metadata_created.path,
        Some(format!("meta/{group_id}/datasets/project/run-42"))
    );
    assert_eq!(metadata_created.group_id, Some(group_id.to_string()));
    assert_eq!(metadata_created.document_id, Some(document_id.to_string()));
    assert_eq!(metadata_created.actor_user_id, Some(actor.to_string()));

    let data_uploaded = notification_response(&NotificationRecord::new(
        recipient,
        NotificationClass::Transient,
        NotificationKind::DataUploaded {
            path: watch_resource_path(group_id, onboarded_node, "bucket", "object"),
            group_id,
            node_id: onboarded_node,
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            size_bytes: 4096,
            actor_user_id: actor,
        },
        1,
    ));
    assert_eq!(data_uploaded.kind, "data_uploaded");
    assert_eq!(data_uploaded.category, "resource.watch");
    assert_eq!(
        data_uploaded.path,
        Some(watch_resource_path(
            group_id,
            onboarded_node,
            "bucket",
            "object"
        ))
    );
    assert_eq!(data_uploaded.group_id, Some(group_id.to_string()));
    assert_eq!(data_uploaded.node_id, Some(onboarded_node.to_string()));
    assert_eq!(data_uploaded.bucket, Some("bucket".to_string()));
    assert_eq!(data_uploaded.key, Some("object".to_string()));
    assert_eq!(data_uploaded.size_bytes, Some(4096));
    assert_eq!(data_uploaded.actor_user_id, Some(actor.to_string()));
}

#[tokio::test]
async fn local_watch_crud() {
    let realm_id = realm_id(6);
    let holder = node(6);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, group_id, user_id, &[]).await;
    let path_prefix = watch_resource_path(group_id, node(60), "bucket", "prefix");

    let (status, created) = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: path_prefix.clone(),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect("create succeeds");
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created.path_prefix, path_prefix);
    assert_eq!(created.events, vec!["data_uploaded"]);

    let (_, listed) = list_watches(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
    )
    .await
    .expect("list succeeds");
    assert_eq!(listed.watches.len(), 1);
    assert_eq!(listed.watches[0].id, created.id);

    let status = delete_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        Path(created.id.clone()),
    )
    .await
    .expect("delete succeeds");
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, empty) = list_watches(State(state), Extension(Some(auth_for(user_id, realm_id))))
        .await
        .expect("list after delete succeeds");
    assert!(empty.watches.is_empty());
}

#[tokio::test]
async fn lists_unauthorized_watch() {
    // Losing READ stops delivery; the row stays listed so its owner can see
    // and delete it, with its details withheld.
    let realm_id = realm_id(31);
    let holder = node(31);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let owner = UserId::new(Ulid::generate(), realm_id);
    let reader = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, group_id, owner, &[reader]).await;

    let (status, created) = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(reader, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: format!("meta/{group_id}/"),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect("group wide watch is accepted");
    assert_eq!(status, StatusCode::CREATED);
    assert!(created.authorized);

    let (_, listed) = list_watches(
        State(state.clone()),
        Extension(Some(auth_for(reader, realm_id))),
    )
    .await
    .expect("list succeeds");
    assert_eq!(listed.watches.len(), 1);
    assert!(listed.watches[0].authorized);

    install_group_authorization(&state, realm_id, group_id, owner, &[]).await;

    let (_, listed) = list_watches(
        State(state.clone()),
        Extension(Some(auth_for(reader, realm_id))),
    )
    .await
    .expect("list after revocation succeeds");
    assert_eq!(listed.watches.len(), 1);
    assert_eq!(listed.watches[0].id, created.id);
    assert!(!listed.watches[0].authorized);
    assert!(listed.watches[0].path_prefix.is_empty());
}

#[tokio::test]
async fn policy_denies_watches() {
    // The realm request policies must reach watch creation, which only
    // ordinary RBAC used to gate.
    let realm_id = realm_id(21);
    let holder = node(21);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, group_id, user_id, &[]).await;
    let path_prefix = watch_resource_path(group_id, node(60), "bucket", "prefix");
    let request = || CreateWatchRequest {
        path_prefix: path_prefix.clone(),
        events: vec!["data_uploaded".to_string()],
    };

    let (status, _) = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(request()),
    )
    .await
    .expect("a group reader may create a watch");
    assert_eq!(status, StatusCode::CREATED);

    install_holder_policies(
        &state,
        realm_id,
        holder,
        vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "deny-watches".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "operation == 'notifications.create_watch'".to_string(),
            enabled: true,
        }],
    )
    .await;

    let denied = create_watch(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(request()),
    )
    .await
    .expect_err("the realm policy must deny watch creation");
    assert!(matches!(denied, ServerError::Forbidden));
}

#[tokio::test]
async fn watch_requires_read() {
    let realm_id = realm_id(15);
    let (_dir, state, net) = build_network_state(realm_id, [15u8; 32]).await;
    let holder = net.node_id();
    let metrics = NodeMetrics::new();
    net.notification_watch_metrics().register(&metrics).await;
    install_holder_config(&state, realm_id, holder).await;
    let authorized = UserId::new(Ulid::generate(), realm_id);
    let unauthorized = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, group_id, authorized, &[]).await;
    let path_prefix = format!("meta/{group_id}/datasets/proteomics");

    let error = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(unauthorized, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: path_prefix.clone(),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("user without metadata READ must be rejected");
    assert!(matches!(error, ServerError::Forbidden));
    assert!(metrics.render().await.contains(
        "aruna_notification_watch_creation_denials_total{reason=\"permission_denied\"} 1"
    ));

    let (status, created) = create_watch(
        State(state),
        Extension(Some(auth_for(authorized, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: path_prefix.clone(),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect("metadata reader may create a watch");
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created.path_prefix, path_prefix);
}

#[tokio::test]
async fn watch_uses_canonical() {
    let realm_id = realm_id(16);
    let holder = node(16);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let authorized = UserId::new(Ulid::generate(), realm_id);
    let unauthorized = UserId::new(Ulid::generate(), realm_id);
    let bucket_group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, bucket_group_id, authorized, &[]).await;
    let remote_bucket_node = node(99);
    let path_prefix =
        watch_resource_path(bucket_group_id, remote_bucket_node, "reports", "quarterly");

    let error = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(unauthorized, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: path_prefix.clone(),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect_err("user without bucket READ must be rejected");
    assert!(matches!(error, ServerError::Forbidden));

    let (status, created) = create_watch(
        State(state),
        Extension(Some(auth_for(authorized, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: path_prefix.clone(),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect("bucket reader may create a watch");
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created.path_prefix, path_prefix);
}

#[tokio::test]
async fn cross_group_watch() {
    let realm_id = realm_id(17);
    let holder = node(17);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let watcher = UserId::new(Ulid::generate(), realm_id);
    let uploader = UserId::new(Ulid::generate(), realm_id);
    let credential_group = Ulid::generate();
    let bucket_group = Ulid::generate();
    let bucket_node = holder;
    install_group_authorization(&state, realm_id, bucket_group, watcher, &[uploader]).await;
    install_bucket(&state, "reports", bucket_group, watcher).await;

    let (_, created) = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(watcher, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: watch_resource_path(
                credential_group,
                bucket_node,
                "reports",
                "quarterly/",
            ),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect("bucket reader may register through a foreign credential group");

    let canonical = watch_resource_path(bucket_group, bucket_node, "reports", "quarterly/");
    assert_eq!(created.path_prefix, canonical);
    let subscriptions = list_watch_subscriptions(&state.get_ctx().storage_handle, watcher)
        .await
        .expect("watch lists");
    let event_path =
        watch_resource_path(bucket_group, bucket_node, "reports", "quarterly/result.csv");
    let event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path: event_path,
        actor: uploader,
        occurred_at_ms: created.created_at_ms + 1,
        detail: WatchEventDetail::DataUploaded {
            group_id: bucket_group,
            node_id: bucket_node,
            bucket: "reports".to_string(),
            key: "quarterly/result.csv".to_string(),
            size_bytes: 1,
        },
    };
    assert_eq!(route_watch_event(&event, &subscriptions).len(), 1);
}

// A group that does not exist and a group the caller may not read answer
// identically, so creating a watch is never an existence oracle.
#[tokio::test]
async fn missing_group_forbidden() {
    let realm_id = realm_id(18);
    let holder = node(18);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let caller = UserId::new(Ulid::generate(), realm_id);
    let existing_group = Ulid::generate();
    install_group_authorization(
        &state,
        realm_id,
        existing_group,
        UserId::new(Ulid::generate(), realm_id),
        &[],
    )
    .await;

    let missing = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(caller, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: format!("meta/{}/datasets", Ulid::generate()),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("a watch on a group that does not exist must be refused");
    assert!(matches!(missing, ServerError::Forbidden));

    let unreadable = create_watch(
        State(state),
        Extension(Some(auth_for(caller, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: format!("meta/{existing_group}/datasets"),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("a watch on an existing unreadable group must be refused");
    assert!(matches!(unreadable, ServerError::Forbidden));
}

#[tokio::test]
async fn mixed_events_rejected() {
    let realm_id = realm_id(17);
    let holder = node(17);
    let (_dir, state) = build_state(realm_id, holder).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let metadata_group_id = Ulid::generate();
    let path_prefix = format!("meta/{metadata_group_id}/datasets/shared");
    let events = vec!["metadata_created".to_string(), "data_uploaded".to_string()];

    let error = create_watch(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix,
            events,
        }),
    )
    .await
    .expect_err("one prefix cannot represent both canonical namespaces");
    assert!(matches!(error, ServerError::BadRequest));
}

#[tokio::test]
async fn watch_validates_input() {
    let realm_id = realm_id(7);
    let (_dir, state) = build_state(realm_id, node(7)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);

    let empty_prefix = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: String::new(),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("empty prefix must be rejected");
    assert!(matches!(empty_prefix, ServerError::BadRequest));

    let leading_slash = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: "/bucket".to_string(),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("leading-slash prefix must be rejected");
    assert!(matches!(leading_slash, ServerError::BadRequest));

    let empty_events = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: "bucket".to_string(),
            events: Vec::new(),
        }),
    )
    .await
    .expect_err("empty events must be rejected");
    assert!(matches!(empty_events, ServerError::BadRequest));

    let unknown_event = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: "bucket".to_string(),
            events: vec!["not_an_event".to_string()],
        }),
    )
    .await
    .expect_err("unknown event must be rejected");
    assert!(matches!(unknown_event, ServerError::BadRequest));

    let unscoped_data = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: "reports".to_string(),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect_err("a prefix without a bucket boundary is ambiguous");
    assert!(matches!(unscoped_data, ServerError::BadRequest));

    let group_id = Ulid::generate();
    let unscoped_metadata = create_watch(
        State(state.clone()),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: format!("meta/{group_id}"),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("a metadata prefix without the group boundary is ambiguous");
    assert!(matches!(unscoped_metadata, ServerError::BadRequest));

    let noncanonical_metadata = create_watch(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: format!("meta/{group_id}/datasets/proteomics/"),
            events: vec!["metadata_created".to_string()],
        }),
    )
    .await
    .expect_err("metadata prefixes must use normalized document paths");
    assert!(matches!(noncanonical_metadata, ServerError::BadRequest));
}

#[tokio::test]
async fn delete_rejects_id() {
    let realm_id = realm_id(8);
    let (_dir, state) = build_state(realm_id, node(8)).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let error = delete_watch(
        State(state),
        Extension(Some(auth_for(user_id, realm_id))),
        Path("not-a-ulid".to_string()),
    )
    .await
    .expect_err("bad id must be rejected");
    assert!(matches!(error, ServerError::BadRequest));
}

#[tokio::test]
async fn rejects_restricted_watch() {
    let realm_id = realm_id(9);
    let holder = node(9);
    let (_dir, state) = build_state(realm_id, holder).await;
    install_holder_config(&state, realm_id, holder).await;
    let user_id = UserId::new(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    install_group_authorization(&state, realm_id, group_id, user_id, &[]).await;
    let mut auth = auth_for(user_id, realm_id);
    auth.path_restrictions = Some(vec![PathRestriction {
        pattern: format!("/{realm_id}/g/{group_id}/data/{holder}/bucket/allowed/**"),
        permission: Permission::READ,
    }]);

    let list_err = list_watches(State(state.clone()), Extension(Some(auth.clone())))
        .await
        .expect_err("delegated token must be rejected");
    assert!(matches!(list_err, ServerError::Forbidden));

    let create_err = create_watch(
        State(state.clone()),
        Extension(Some(auth.clone())),
        bearer(),
        Json(CreateWatchRequest {
            path_prefix: watch_resource_path(group_id, holder, "bucket", "allowed/"),
            events: vec!["data_uploaded".to_string()],
        }),
    )
    .await
    .expect_err("delegated token must be rejected");
    assert!(matches!(create_err, ServerError::Forbidden));

    let delete_err = delete_watch(
        State(state),
        Extension(Some(auth)),
        Path(Ulid::generate().to_string()),
    )
    .await
    .expect_err("delegated token must be rejected");
    assert!(matches!(delete_err, ServerError::Forbidden));
}
