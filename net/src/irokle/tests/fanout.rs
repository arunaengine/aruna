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
