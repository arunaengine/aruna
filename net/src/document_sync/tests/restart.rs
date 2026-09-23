//! Tests sync summary handling, buffered publish after restart and fan-out failure reports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn sync_summary(
    event_type_id: Option<String>,
    heads: BTreeSet<::irokle::OpId>,
) -> ::irokle::sync::SyncSummary {
    summary_for(topic(9), event_type_id, heads)
}

fn summary_for(
    topic_id: ::irokle::TopicId,
    event_type_id: Option<String>,
    heads: BTreeSet<::irokle::OpId>,
) -> ::irokle::sync::SyncSummary {
    ::irokle::sync::SyncSummary {
        topic_id,
        event_type_id,
        fingerprint: [0; 32],
        heads,
        actor_clock: ::irokle::ActorClock::default(),
        actor_tips: BTreeMap::new(),
        genesis: None,
        staged: None,
    }
}

#[test]
fn empty_confirms_unknown() {
    let wanted = BTreeSet::from([topic(1), topic(2), topic(3)]);
    let responses = vec![
        // Empty summary: positive confirmation the peer has no genesis.
        SyncMessage::Summary(summary_for(topic(1), None, BTreeSet::new())),
        // Typed summary: the peer holds a genesis.
        SyncMessage::Summary(summary_for(
            topic(2),
            Some(DocumentEvent::TYPE_ID.to_string()),
            BTreeSet::new(),
        )),
        // topic(3) omitted entirely: refused (held, prober not a member).
    ];
    let probe = classify_probe_responses(&wanted, &responses);
    assert_eq!(probe.confirmed_unknown, BTreeSet::from([topic(1)]));
    assert_eq!(probe.known, BTreeSet::from([topic(2)]));
    assert!(!probe.confirmed_unknown.contains(&topic(3)));
    assert!(!probe.known.contains(&topic(3)));
}

#[test]
fn unwanted_summaries_ignored() {
    let wanted = BTreeSet::from([topic(1)]);
    let responses = vec![SyncMessage::Summary(summary_for(
        topic(5),
        None,
        BTreeSet::new(),
    ))];
    assert_eq!(
        classify_probe_responses(&wanted, &responses),
        PeerTopicProbe::default()
    );
}

#[test]
fn buffered_publish_child() {
    let Ok(root) = env::var(CHILD_PATH_ENV) else {
        return;
    };
    let root = PathBuf::from(root);
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    runtime.block_on(async {
        let service = open_restart_service(&root, "child-storage").await;
        let target = restart_target();
        // Shard topics are join-only at publish time; this node plays the
        // shard's rank-0 holder and creates the genesis eagerly first.
        service
            .ensure_sync_topics(&[restart_topic()], Vec::new())
            .expect("restart shard topic genesis");
        let event = service
            .publish_documents(
                vec![DocumentSyncPublish::Upsert {
                    event_id: restart_event_id(),
                    target: target.clone(),
                    bytes: restart_payload(),
                    change: revision_change(),
                    allow_genesis: true,
                }],
                Vec::new(),
            )
            .await;

        assert_eq!(
            event,
            DocumentNetEvent::DocumentsPublished {
                targets: vec![target]
            }
        );
    });

    // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
    std::process::exit(0);
}

#[test]
fn event_envelope_roundtrip() {
    let event = DocumentEvent::Upsert {
        event_id: restart_event_id(),
        target: restart_target(),
        bytes: restart_payload(),
        change: revision_change(),
    };
    let envelope = EventEnvelope::encode_event(&event).expect("event encodes");
    let decoded = envelope
        .decode_event::<DocumentEvent>()
        .expect("event decodes");

    assert_eq!(decoded, event);
}

#[tokio::test]
async fn buffered_publish_survives() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    let target = restart_target();

    run_restart_child(root);

    let service = open_restart_service(root, "parent-storage").await;
    let topic = service
        .node()
        .open_topic::<DocumentEvent>(restart_topic())
        .expect("published topic reopens after restart");
    let history = topic
        .history(::irokle::history::HistoryOrder::OldestFirst)
        .expect("published history reads after restart");

    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].event,
        DocumentEvent::Upsert {
            event_id: restart_event_id(),
            target,
            bytes: restart_payload(),
            change: revision_change(),
        }
    );

    service.shutdown().await;
}

#[tokio::test]
async fn fanout_propagates_failure() {
    let ok_peer = peer(1);
    let failed_peer = peer(2);
    let selection = PeerSelection {
        peers: BTreeSet::from([ok_peer, failed_peer]),
        truncated: false,
        round: 0,
    };

    let error = DocumentSyncService::sync_peer_fanout(
        selection,
        "test document sync".to_string(),
        move |peer| async move {
            if peer == ok_peer {
                Ok(())
            } else {
                Err(NetError::Bootstrap("offline peer".to_string()))
            }
        },
    )
    .await
    .expect_err("partial peer fan-out must fail");

    let NetError::Bootstrap(message) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(message.contains("only 1/2 peers synced"));
    assert!(message.contains("offline peer"));
}

#[test]
fn known_failure_rejected() {
    let results = BTreeMap::from([
        (topic(3), Ok(())),
        (topic(4), Err(std::io::Error::other("peer refused"))),
    ]);

    let error = finish_batch_sync(peer(5), &results)
        .expect_err("partial topic failure must fail the batch");

    let NetError::Bootstrap(message) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(message.contains("1/2 document sync batch topics failed"));
}

#[test]
fn clean_batch_succeeds() {
    let results = BTreeMap::from([(topic(6), Ok(())), (topic(7), Ok(()))]);

    finish_batch_sync(peer(8), &results).expect("batch with no failed topics should succeed");
}

#[test]
fn paged_batch_succeeds() {
    // Irokle reports WouldBlock when it scheduled the pages left after its budget.
    let results = BTreeMap::from([(
        topic(6),
        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
    )]);

    finish_batch_sync(peer(8), &results).expect("scheduled pages are progress, not failure");
}

#[test]
fn headless_summary_empty() {
    assert!(summary_is_empty(&sync_summary(None, BTreeSet::new())));
    assert!(!summary_is_empty(&sync_summary(
        Some(DocumentEvent::TYPE_ID.to_string()),
        BTreeSet::new()
    )));
    assert!(!summary_is_empty(&sync_summary(
        None,
        BTreeSet::from([::irokle::OpId::hash(b"head")])
    )));
}
