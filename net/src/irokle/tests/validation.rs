use super::*;

#[test]
fn validate_node_usage_upsert_accepts_owner_and_rejects_forgeries() {
    use aruna_core::structs::UsageCounters;

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let group_id = Ulid::from_bytes([4u8; 16]);
    let global = DocumentSyncTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: None,
    };
    let group = DocumentSyncTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: Some(group_id),
    };

    // The owning node's own snapshot validates as global and per-group.
    let owned = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters {
            buckets: 3,
            ..Default::default()
        },
    };
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_node_usage_upsert(&global, &owned_bytes).is_ok());
    assert!(validate_node_usage_upsert(&group, &owned_bytes).is_ok());

    // Zero-counter snapshots (stale-group cleanup) are legitimate upserts.
    let zero = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters::default(),
    };
    assert!(validate_node_usage_upsert(&global, &zero.to_bytes().unwrap()).is_ok());

    // A snapshot whose embedded node id is a different node is rejected.
    let misattributed = NodeUsageSnapshot {
        node_id: node(9),
        counters: UsageCounters {
            buckets: 99,
            ..Default::default()
        },
    };
    assert!(validate_node_usage_upsert(&global, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non node-usage targets are rejected.
    assert!(validate_node_usage_upsert(&global, b"not-a-snapshot").is_err());
    assert!(
        validate_node_usage_upsert(&DocumentSyncTarget::RealmConfig { realm_id }, &owned_bytes)
            .is_err()
    );
}

#[test]
fn watch_interest_validation() {
    use aruna_core::structs::{WatchEventKind, WatchEventMask};

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let target = DocumentSyncTarget::WatchInterest { realm_id, node_id };

    // The owning node's own digest validates.
    let owned = WatchInterestDigest::from_subscriptions(
        node_id,
        [(
            "/owned/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )],
    );
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_watch_interest(&target, &owned_bytes).is_ok());

    // Empty digests are legitimate upserts that clear a node's interest.
    let empty = WatchInterestDigest {
        node_id,
        entries: Vec::new(),
    };
    assert!(validate_watch_interest(&target, &empty.to_bytes().unwrap()).is_ok());

    let too_many = WatchInterestDigest::from_subscriptions(
        node_id,
        (0..=NOTIFICATION_WATCH_INTEREST_ENTRY_CAP).map(|index| {
            (
                format!("/entry/{index}"),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )
        }),
    );
    assert!(validate_watch_interest(&target, &too_many.to_bytes().unwrap()).is_err());
    assert!(
        validate_watch_interest(&target, &vec![0; NOTIFICATION_WATCH_INTEREST_BYTES_CAP + 1],)
            .is_err()
    );

    // A digest whose embedded node id is a different node is rejected.
    let misattributed = WatchInterestDigest::from_subscriptions(
        node(9),
        [(
            "/forged/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )],
    );
    assert!(validate_watch_interest(&target, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non watch-interest targets are rejected.
    assert!(validate_watch_interest(&target, b"not-a-digest").is_err());
    assert!(
        validate_watch_interest(&DocumentSyncTarget::RealmConfig { realm_id }, &owned_bytes,)
            .is_err()
    );
}

#[tokio::test]
async fn watch_origins_converge() {
    // Independent origins must not lose a replica to arrival-order admission.
    use aruna_core::structs::{WatchEventKind, WatchEventMask};

    let (_left_dir, left) = test_storage();
    let (_right_dir, right) = test_storage();
    let realm_id = RealmId::from_bytes([57u8; 32]);
    let make = |seed: u8, event_mask: WatchEventMask| {
        let owner = UserId::local(Ulid::from_parts(seed as u64, 1), realm_id);
        let watch_id = Ulid::from_parts(seed as u64, 2);
        let mut subscription =
            WatchSubscription::new(owner, format!("watch/{seed}"), event_mask, 1);
        subscription.watch_id = watch_id;
        let change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(seed as u64, 3),
                actor: node(seed),
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        (
            DocumentSyncTarget::WatchSubscription { owner, watch_id },
            subscription.to_bytes().expect("subscription serializes"),
            change,
        )
    };
    let first = make(
        1,
        WatchEventMask::from_kinds([WatchEventKind::SyncCompleted]),
    );
    let second = make(2, WatchEventMask::from_kinds([WatchEventKind::SyncFailed]));

    assert!(validate_watch_subscription_upsert(&first.0, &first.1, &first.2).is_ok());
    assert!(
        apply_watch_subscription_change_to_storage(
            &left,
            first.0.clone(),
            Some(first.1.clone()),
            first.2,
        )
        .await
        .expect("first origin applies")
    );
    assert!(validate_watch_subscription_upsert(&second.0, &second.1, &second.2).is_ok());
    assert!(
        apply_watch_subscription_change_to_storage(
            &left,
            second.0.clone(),
            Some(second.1.clone()),
            second.2,
        )
        .await
        .expect("second origin applies")
    );
    assert!(validate_watch_subscription_upsert(&second.0, &second.1, &second.2).is_ok());
    assert!(
        apply_watch_subscription_change_to_storage(
            &right,
            second.0.clone(),
            Some(second.1.clone()),
            second.2,
        )
        .await
        .expect("second origin applies in reverse order")
    );
    assert!(validate_watch_subscription_upsert(&first.0, &first.1, &first.2).is_ok());
    assert!(
        apply_watch_subscription_change_to_storage(
            &right,
            first.0.clone(),
            Some(first.1.clone()),
            first.2,
        )
        .await
        .expect("first origin applies in reverse order")
    );

    for storage in [&left, &right] {
        assert!(
            read_storage_value(storage, first.0.storage_keyspace(), first.0.storage_key(),)
                .await
                .is_some()
        );
        assert!(
            read_storage_value(storage, second.0.storage_keyspace(), second.0.storage_key(),)
                .await
                .is_some()
        );
    }
    let unknown_mask: WatchEventMask =
        postcard::from_bytes(&postcard::to_allocvec(&16u32).expect("unknown mask serializes"))
            .expect("unknown mask decodes");
    let (target, bytes, change) = make(3, unknown_mask);
    assert!(validate_watch_subscription_upsert(&target, &bytes, &change).is_err());
}
