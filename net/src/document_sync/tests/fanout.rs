use super::*;

#[test]
fn budget_caps_streams() {
    // Per-peer and global caps hold, and dropping a permit restores both.
    let budget = Arc::new(InboundSyncBudget::default());
    let mut held = Vec::new();
    for _ in 0..DOCUMENT_SYNC_INBOUND_PEER_STREAMS {
        held.push(budget.acquire(peer(1)).expect("within per-peer budget"));
    }
    assert!(budget.acquire(peer(1)).is_none());
    assert!(budget.acquire(peer(2)).is_some());

    held.pop();
    assert!(budget.acquire(peer(1)).is_some());

    let mut fill = Vec::new();
    for seed in 10..u8::MAX {
        match budget.acquire(peer(seed)) {
            Some(permit) => fill.push(permit),
            None => break,
        }
    }
    assert!(budget.acquire(peer(3)).is_none());
    fill.clear();
    assert!(budget.acquire(peer(3)).is_some());
}

#[test]
fn inbound_timeout_order() {
    assert!(DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT < DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT);
}

#[test]
fn sync_peers_bounded() {
    let selection = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);

    assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
    assert!(selection.truncated);
    assert!(!selection.peers.contains(&peer(0)));
}

#[test]
fn sync_peers_dedup() {
    let candidates = (1u8..=9).map(peer).collect::<Vec<_>>();
    let selection = select_sync_peers(
        candidates.iter().copied().chain(candidates.iter().copied()),
        peer(0),
        b"document-sync-subject",
        0,
    );

    assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
    assert!(selection.truncated);
}

#[test]
fn sync_peers_cover() {
    let candidates = (1u8..=17).map(peer).collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let rounds = candidates.len().div_ceil(DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
    for round in 0..rounds as u64 {
        seen.extend(
            select_sync_peers(
                candidates.iter().copied(),
                peer(0),
                b"document-sync-subject",
                round,
            )
            .peers,
        );
    }

    assert_eq!(seen, candidates);
}

#[test]
fn fanout_cursor_restart() {
    let root = TempDir::new().expect("fanout cursor tempdir");
    let topic_id = topic(42);
    {
        let db = fjall::OptimisticTxDatabase::builder(root.path())
            .manual_journal_persist(true)
            .open()
            .expect("fanout cursor database");
        let cursors = db
            .keyspace(
                DOCUMENT_SYNC_FANOUT_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .expect("fanout cursor keyspace");
        assert_eq!(
            current_cursor(&cursors, topic_id, test_genesis(1)).expect("first cursor"),
            0
        );
        advance_cursor(&cursors, topic_id, test_genesis(1), 0).expect("advance cursor");
        db.persist(fjall::PersistMode::SyncAll)
            .expect("persist fanout cursor");
    }
    let db = fjall::OptimisticTxDatabase::builder(root.path())
        .manual_journal_persist(true)
        .open()
        .expect("reopen fanout cursor database");
    let cursors = db
        .keyspace(
            DOCUMENT_SYNC_FANOUT_KEYSPACE,
            fjall::KeyspaceCreateOptions::default,
        )
        .expect("reopen fanout cursor keyspace");
    assert_eq!(
        current_cursor(&cursors, topic_id, test_genesis(1)).expect("restarted cursor"),
        1
    );
    // A replaced genesis must restart peer selection instead of inheriting
    // the losing chain's rotation.
    assert_eq!(
        current_cursor(&cursors, topic_id, test_genesis(2)).expect("replaced cursor"),
        0
    );
    advance_cursor(&cursors, topic_id, test_genesis(2), 0).expect("rebind cursor");
    assert_eq!(
        current_cursor(&cursors, topic_id, test_genesis(2)).expect("rebound cursor"),
        1
    );
}

#[test]
fn fanout_cursor_clear() {
    // A topic reset can remove stale fan-out progress before re-emission.
    let selection = select_sync_peers((1u8..=9).map(peer), peer(0), b"missing-topic", 0);
    assert_eq!(selection.peers.len(), DOCUMENT_SYNC_OUTBOUND_PEER_LIMIT);
    assert!(selection.truncated);
    let root = TempDir::new().expect("fanout cursor clear tempdir");
    let topic_id = topic(43);
    {
        let db = fjall::OptimisticTxDatabase::builder(root.path())
            .manual_journal_persist(true)
            .open()
            .expect("fanout cursor clear database");
        let cursors = db
            .keyspace(
                DOCUMENT_SYNC_FANOUT_KEYSPACE,
                fjall::KeyspaceCreateOptions::default,
            )
            .expect("fanout cursor clear keyspace");
        advance_cursor(&cursors, topic_id, test_genesis(3), 0).expect("create fanout cursor");
        assert!(
            cursors
                .contains_key(topic_cursor_key(topic_id))
                .expect("find fanout cursor")
        );
        remove_cursor(&cursors, topic_id).expect("clear fanout cursor");
        db.persist(fjall::PersistMode::SyncAll)
            .expect("persist cleared fanout cursor");
    }
    let db = fjall::OptimisticTxDatabase::builder(root.path())
        .manual_journal_persist(true)
        .open()
        .expect("reopen fanout cursor clear database");
    let cursors = db
        .keyspace(
            DOCUMENT_SYNC_FANOUT_KEYSPACE,
            fjall::KeyspaceCreateOptions::default,
        )
        .expect("reopen fanout cursor clear keyspace");
    assert!(
        !cursors
            .contains_key(topic_cursor_key(topic_id))
            .expect("find cleared fanout cursor")
    );
}

#[test]
fn sync_peers_permutation() {
    let forward = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);
    let reverse = select_sync_peers(
        (1u8..=32).rev().map(peer),
        peer(0),
        b"document-sync-subject",
        0,
    );

    assert_eq!(forward.peers, reverse.peers);
    assert_ne!(forward.peers, (1u8..=8).map(peer).collect::<BTreeSet<_>>());
}

#[test]
fn sync_peers_rotate() {
    let first = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 0);
    let second = select_sync_peers((1u8..=32).map(peer), peer(0), b"document-sync-subject", 1);

    assert!(second.peers.iter().any(|peer| !first.peers.contains(peer)));
}

#[test]
fn sync_topics_grouped() {
    let topics = [topic(1), topic(2), topic(3)];
    let groups = group_sync_topics(&topics, |topic_id| {
        let selected = if topic_id == topics[1] {
            peer(2)
        } else {
            peer(1)
        };
        PeerSelection {
            peers: BTreeSet::from([selected]),
            truncated: false,
            round: 0,
        }
    });

    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups
            .values()
            .map(|(_, topics)| topics.len())
            .sum::<usize>(),
        topics.len()
    );
    assert!(
        groups
            .values()
            .any(|(_, grouped)| grouped == &vec![topics[1]])
    );
    assert!(
        groups
            .values()
            .any(|(_, grouped)| grouped == &vec![topics[0], topics[2]])
    );
}

#[test]
fn write_permission_scope() {
    let realm_id = RealmId::from_bytes([74; 32]);
    let group_id = Ulid::from_parts(1_700, 1);
    let user_id = UserId::local(Ulid::from_parts(1_701, 1), realm_id);
    let role = Role {
        role_id: Ulid::from_parts(1_702, 1),
        name: "group-admin".to_string(),
        permissions: HashMap::from([(format!("/{realm_id}/g/{group_id}/*"), Permission::WRITE)]),
        assigned_users: HashSet::from([user_id]),
    };

    assert!(has_write_permission(
        user_id,
        &format!("/{realm_id}/g/{group_id}/admin"),
        [&role],
    ));
    assert!(!has_write_permission(
        user_id,
        &format!("/{realm_id}/g/{group_id}/admin/config"),
        [&role],
    ));

    let anonymous = UserId::nil(realm_id);
    let public_role = Role {
        assigned_users: HashSet::from([anonymous]),
        ..role.clone()
    };
    assert!(!has_write_permission(
        anonymous,
        &format!("/{realm_id}/g/{group_id}/admin"),
        [&public_role],
    ));
}

#[test]
fn revocation_expiry_bound() {
    // The shared admission window bounds replicated reducer retention.
    let now = 1_000;
    let bound = now + MAX_BEARER_TOKEN_LIFETIME_SECS + REVOCATION_GRACE_SECS;

    assert!(valid_revocation_expiry(bound, now));
    assert!(!valid_revocation_expiry(bound + 1, now));
    assert!(!valid_revocation_expiry(u64::MAX, now));
}

#[test]
fn index_skip_expiry() {
    let realm_id = RealmId::from_bytes([76; 32]);
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let actor = test_actor(
        45,
        UserId::local(Ulid::from_parts(1_810, 1), realm_id),
        realm_id,
    );
    let mut state = AdminDocumentReducerState::new(target.clone());
    state
        .apply(&test_admin_event(
            Ulid::from_parts(1_811, 1),
            target,
            &actor,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: aruna_core::auth::bearer_token_hash("scheduled"),
                expires_at: 2_000,
                token_owner: actor.user_id,
            },
        ))
        .expect("revocation applies");

    assert!(!needs_revocation_index(false, true, &state, 2_000));
    assert!(needs_revocation_index(
        false,
        true,
        &state,
        2_000 + REVOCATION_GRACE_SECS + 1
    ));
    assert!(needs_revocation_index(true, true, &state, 2_000));
    assert!(needs_revocation_index(false, false, &state, 2_000));
}

#[test]
fn floor_stays_monotonic() {
    // A clock rollback must not resurrect compacted revocation paths.
    let realm_id = RealmId::from_bytes([75; 32]);
    let actor = test_actor(
        44,
        UserId::local(Ulid::from_parts(1_800, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let token_hash = aruna_core::auth::bearer_token_hash("floor-token");
    let mut state = AdminDocumentReducerState::new(target.clone());
    state
        .apply(&test_admin_event(
            Ulid::from_parts(1_801, 1),
            target,
            &actor,
            1,
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: token_hash.clone(),
                expires_at: 10_000,
                token_owner: actor.user_id,
            },
        ))
        .expect("revocation applies");

    state.compact_revocations(10_000);
    state.compact_revocations(9_999);

    assert_eq!(state.revocation_floor, 10_000);
    assert!(
        state
            .materialized_revoked_tokens()
            .contains_key(&token_hash)
    );
}

#[tokio::test]
async fn malformed_state_aborts() {
    // A decode failure after transaction start must release the snapshot.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([76; 32]);
    let actor = test_actor(
        45,
        UserId::local(Ulid::from_parts(1_810, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    batch_write_to(
        &storage,
        vec![(
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&target),
            vec![0xff].into(),
        )],
    )
    .await
    .expect("malformed state writes");

    assert!(
        apply_admin_operation(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_811, 1),
                target.clone(),
                &actor,
                1,
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: test_discovery(27, "https://abort.example:443"),
                },
            ),
        )
        .await
        .is_err()
    );
    batch_delete_to(
        &storage,
        vec![(
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&target),
        )],
    )
    .await
    .expect("malformed state deletes");

    apply_admin_operation(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_812, 1),
            target,
            &actor,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(27, "https://abort.example:443"),
            },
        ),
    )
    .await
    .expect("valid state applies after abort");
}

#[tokio::test]
async fn admits_known_peers() {
    // An inbound sync stream is refused before any read unless the pusher
    // is a configured realm peer.
    let root = TempDir::new().expect("tempdir");
    let service = open_restart_service(root.path(), "storage").await;
    let stranger = iroh::SecretKey::from_bytes(&[41u8; 32]).public();

    assert!(service.admit_inbound(stranger).is_err());
    service.add_peer_candidate(stranger).expect("peer added");
    let permit = service.admit_inbound(stranger);
    assert!(permit.is_ok());
}

#[test]
fn budget_caps_bytes() {
    // Per-peer and global byte ceilings hold, and release restores both.
    let budget = Arc::new(InboundSyncBudget::default());
    assert!(budget.reserve_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
    assert!(!budget.reserve_bytes(peer(1), 1));
    budget.release_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES);
    assert!(budget.reserve_bytes(peer(1), 1));
    budget.release_bytes(peer(1), 1);

    let mut reserved = 0usize;
    for seed in 10..u8::MAX {
        if budget.reserve_bytes(peer(seed), DOCUMENT_SYNC_INBOUND_PEER_BYTES) {
            reserved = reserved.saturating_add(DOCUMENT_SYNC_INBOUND_PEER_BYTES);
        } else {
            break;
        }
    }
    assert!(reserved <= DOCUMENT_SYNC_INBOUND_GLOBAL_BYTES);
    assert!(!budget.reserve_bytes(peer(9), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
}

#[test]
fn drop_releases_reservation() {
    // A stream's reservation is returned to the budget when it drops.
    let budget = Arc::new(InboundSyncBudget::default());
    {
        let mut reservation = InboundByteReservation::new(budget.clone(), peer(1));
        reservation
            .reserve(DOCUMENT_SYNC_INBOUND_PEER_BYTES)
            .expect("first reservation fits");
        assert!(reservation.reserve(1).is_err());
    }
    assert!(budget.reserve_bytes(peer(1), DOCUMENT_SYNC_INBOUND_PEER_BYTES));
}

#[tokio::test]
async fn removed_peer_denied() {
    // A startup peer admits during bootstrap, then loses admission once
    // realm config materializes without it, with no restart.
    let root = TempDir::new().expect("tempdir");
    let startup = node(51);
    let current = node(52);
    let service = DocumentSyncService::open_with_policy(
        restart_endpoint().await,
        storage_at(&root.path().join("storage")),
        root.path().join("document-sync"),
        &[startup],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        restart_realm(),
    )
    .expect("service opens");

    assert!(service.admit_inbound(startup).is_ok());

    service
        .refresh_peer_candidates([current])
        .expect("refresh applies");

    assert!(matches!(
        service.admit_inbound(startup),
        Err(NetError::AdmissionRejected(_))
    ));
    assert!(service.admit_inbound(current).is_ok());
}
