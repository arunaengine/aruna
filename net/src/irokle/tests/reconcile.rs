use super::*;

// The eager cursor delete is an optimization, not the invariant. A crash or
// a failed delete leaves the replaced chain's cursor behind, so reconcile
// must detect the lineage change itself and replay the winner from one.
#[tokio::test]
async fn applied_cursor_lineage() {
    // A cursor is trusted only while it still describes the topic's current
    // history: another genesis, a position rebuilt under the same genesis
    // (an orphan quarantine), and any unreadable value all restart replay.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([81; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(81).await,
        storage,
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let local_actor = test_actor(81, UserId::nil(realm_id), realm_id);
    let event = test_admin_event(
        Ulid::from_parts(1_810, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        1,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "cursor lineage".to_string(),
        },
    );
    assert!(matches!(
        service
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target,
                    event: Box::new(event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                }],
                Vec::new(),
            )
            .await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    let node = service.node();
    let store = node.storage();
    let genesis = service
        .topic_genesis(topic_id)
        .expect("topic genesis")
        .expect("topic exists");
    let clock = store.actor_clock(&topic_id).expect("topic clock");
    let encoded = applied_cursor_value(store, topic_id, genesis, &clock).expect("cursor encodes");
    let read = |value: Option<Value>, genesis| {
        applied_cursor_clock(store, topic_id, genesis, value).expect("cursor reads")
    };

    assert_eq!(read(Some(encoded.clone()), genesis), clock);
    assert_eq!(
        read(Some(encoded.clone()), test_genesis(2)),
        irokle_crate::ActorClock::default(),
        "another genesis is another history"
    );
    assert_eq!(
        read(
            Some(
                postcard::to_allocvec(&clock)
                    .expect("bare clock serializes")
                    .into()
            ),
            genesis
        ),
        irokle_crate::ActorClock::default(),
        "a cursor without a lineage is untrusted"
    );
    assert_eq!(
        read(None, genesis),
        irokle_crate::ActorClock::default(),
        "an absent cursor replays from one"
    );

    // An orphan quarantine keeps the genesis and rebuilds the chain, so the
    // recorded positions hold different ops than the cursor remembers.
    let mut rebuilt: AppliedCursor =
        postcard::from_bytes(encoded.as_ref()).expect("cursor decodes");
    for mark in rebuilt.marks.values_mut() {
        *mark = test_genesis(9);
    }
    assert_eq!(
        read(
            Some(
                postcard::to_allocvec(&rebuilt)
                    .expect("rebuilt cursor serializes")
                    .into()
            ),
            genesis
        ),
        irokle_crate::ActorClock::default(),
        "a rebuilt position must not count as applied"
    );

    service.shutdown().await;
}
