use super::*;

fn sync_summary(
    event_type_id: Option<String>,
    heads: BTreeSet<irokle_crate::OpId>,
) -> irokle_crate::sync::SyncSummary {
    summary_for(topic(9), event_type_id, heads)
}

fn summary_for(
    topic_id: irokle_crate::TopicId,
    event_type_id: Option<String>,
    heads: BTreeSet<irokle_crate::OpId>,
) -> irokle_crate::sync::SyncSummary {
    irokle_crate::sync::SyncSummary {
        topic_id,
        event_type_id,
        fingerprint: [0; 32],
        heads,
        actor_clock: irokle_crate::ActorClock::default(),
        actor_tips: BTreeMap::new(),
    }
}

#[test]
fn classify_probe_buckets_empty_summary_as_confirmed_unknown() {
    let wanted = BTreeSet::from([topic(1), topic(2), topic(3)]);
    let responses = vec![
        // Empty summary: positive confirmation the peer has no genesis.
        SyncMessage::Summary(summary_for(topic(1), None, BTreeSet::new())),
        // Typed summary: the peer holds a genesis.
        SyncMessage::Summary(summary_for(
            topic(2),
            Some(DocumentSyncEvent::TYPE_ID.to_string()),
            BTreeSet::new(),
        )),
        // topic(3) omitted entirely: refused (held, prober not a member).
    ];
    let probe = classify_probe_responses(&wanted, responses);
    assert_eq!(probe.confirmed_unknown, BTreeSet::from([topic(1)]));
    assert_eq!(probe.known, BTreeSet::from([topic(2)]));
    assert!(!probe.confirmed_unknown.contains(&topic(3)));
    assert!(!probe.known.contains(&topic(3)));
}

#[test]
fn classify_probe_ignores_summaries_for_unwanted_topics() {
    let wanted = BTreeSet::from([topic(1)]);
    let responses = vec![SyncMessage::Summary(summary_for(
        topic(5),
        None,
        BTreeSet::new(),
    ))];
    assert_eq!(
        classify_probe_responses(&wanted, responses),
        PeerTopicProbe::default()
    );
}

#[test]
fn buffered_document_sync_publish_restart_child_process() {
    let Ok(root) = env::var(DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV) else {
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
            .ensure_document_sync_topics(&[restart_topic()], Vec::new())
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
            DocumentSyncNetEvent::DocumentsPublished {
                targets: vec![target]
            }
        );
    });

    // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
    std::process::exit(0);
}

#[test]
fn upsert_event_envelope_round_trips() {
    let event = DocumentSyncEvent::Upsert {
        event_id: restart_event_id(),
        target: restart_target(),
        bytes: restart_payload(),
        change: revision_change(),
    };
    let envelope = EventEnvelope::encode_event(&event).expect("event encodes");
    let decoded = envelope
        .decode_event::<DocumentSyncEvent>()
        .expect("event decodes");

    assert_eq!(decoded, event);
}

#[tokio::test]
async fn acknowledged_document_sync_publish_survives_buffered_process_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path();
    let target = restart_target();

    run_document_sync_restart_child(root);

    let service = open_restart_service(root, "parent-storage").await;
    let topic = service
        .node()
        .open_topic::<DocumentSyncEvent>(restart_topic())
        .expect("published topic reopens after restart");
    let history = topic
        .history(irokle_crate::history::HistoryOrder::OldestFirst)
        .expect("published history reads after restart");

    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].event,
        DocumentSyncEvent::Upsert {
            event_id: restart_event_id(),
            target,
            bytes: restart_payload(),
            change: revision_change(),
        }
    );

    service.shutdown().await;
}

#[tokio::test]
async fn fan_out_peer_syncs_fails_when_any_peer_failed() {
    let ok_peer = peer(1);
    let failed_peer = peer(2);
    let selection = PeerSelection {
        peers: BTreeSet::from([ok_peer, failed_peer]),
        truncated: false,
        round: 0,
    };

    let error = DocumentSyncService::fan_out_peer_syncs(
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

fn batch_test_node(seed: u8) -> (TempDir, irokle_crate::Irokle<irokle_crate::FjallStorage>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .open()
        .expect("fjall database opens");
    let node = irokle_crate::Irokle::builder()
        .with_iroh_secret_key(&iroh::SecretKey::from_bytes(&[seed; 32]))
        .with_fjall_database_and_persist_mode(db, fjall::PersistMode::Buffer)
        .expect("fjall storage")
        .build()
        .expect("irokle node builds");
    (dir, node)
}

#[test]
fn batch_failure_marks_only_its_topic() {
    // A per-topic failure frame must not abort the batch: the topics after
    // it in the same response still get negotiated.
    let (_dir, node) = batch_test_node(21);
    let known_topics = BTreeSet::from([topic(1), topic(2)]);
    let local_fingerprints = BTreeMap::from([(topic(2), [0; 32])]);
    let responses = vec![
        SyncMessage::Failure(irokle_crate::sync::SyncFailure {
            topic_id: topic(1),
            code: irokle_crate::sync::SyncFailureCode::Fingerprint,
        }),
        SyncMessage::Fingerprint(irokle_crate::sync::SyncFingerprint {
            topic_id: topic(2),
            fingerprint: [0; 32],
        }),
    ];

    let (responded, failed, _messages) = process_batch_summary_responses(
        &node,
        peer(9),
        &known_topics,
        &local_fingerprints,
        responses,
    )
    .expect("a per-topic failure must not fail the whole batch");

    assert_eq!(responded, known_topics);
    assert_eq!(failed, BTreeSet::from([topic(1)]));
}

#[test]
fn batch_failure_for_unknown_topic_errors() {
    let (_dir, node) = batch_test_node(22);
    let known_topics = BTreeSet::from([topic(1)]);
    let responses = vec![SyncMessage::Failure(irokle_crate::sync::SyncFailure {
        topic_id: topic(7),
        code: irokle_crate::sync::SyncFailureCode::Open,
    })];

    process_batch_summary_responses(&node, peer(9), &known_topics, &BTreeMap::new(), responses)
        .expect_err("a failure for an unrequested topic indicts the peer");
}

#[test]
fn finish_batch_sync_fails_when_any_known_topic_failed() {
    let known_topics = BTreeSet::from([topic(3), topic(4)]);
    let failed_topics = BTreeSet::from([topic(4)]);

    let error = finish_batch_sync(peer(5), &known_topics, &failed_topics)
        .expect_err("partial topic failure must fail the batch");

    let NetError::Bootstrap(message) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert!(message.contains("1/2 document sync batch topics failed"));
}

#[test]
fn finish_batch_sync_succeeds_when_no_topics_failed() {
    let known_topics = BTreeSet::from([topic(6), topic(7)]);
    let failed_topics = BTreeSet::new();

    finish_batch_sync(peer(8), &known_topics, &failed_topics)
        .expect("batch with no failed topics should succeed");
}

#[test]
fn remote_summary_is_empty_only_for_untyped_headless_topics() {
    assert!(remote_summary_is_empty(&sync_summary(
        None,
        BTreeSet::new()
    )));
    assert!(!remote_summary_is_empty(&sync_summary(
        Some(DocumentSyncEvent::TYPE_ID.to_string()),
        BTreeSet::new()
    )));
    assert!(!remote_summary_is_empty(&sync_summary(
        None,
        BTreeSet::from([irokle_crate::OpId::hash(b"head")])
    )));
}
