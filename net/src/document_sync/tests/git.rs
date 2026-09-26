//! Tests that replicated Git records keep the rows a shard handover proves them by.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::git::{GitChange, GitRecord, git_record_entry, record_change};
use aruna_core::keyspaces::{GIT_RECORD_KEYSPACE, SHARD_MANIFEST_KEYSPACE, SYNC_REVISION_KEYSPACE};
use aruna_core::storage_entries::{shard_manifest_key, sync_revision_key};

#[tokio::test]
async fn git_record_manifest() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([81; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(81).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let owner = service.local_node_id().expect("local node id");
    let actor = test_actor(
        81,
        UserId::local(Ulid::from_parts(5_004, 1), realm_id),
        realm_id,
    );

    let strategy_id = Ulid::from_parts(5_005, 1);
    let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(owner, RealmNodeKind::Management);
    config.placement_bindings.push(PlacementBinding {
        handle,
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::Metadata,
        strategy_id,
        allocator_range_id: None,
        allocated_by: None,
        allocated_at_ms: None,
    });
    config.strategies.push(PlacementStrategy {
        strategy_id,
        name: "placed".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    });
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id },
            config
                .to_bytes(&actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    let document_id = MetaResourceId::from_parts(5_010, handle, BucketId::new(4).unwrap(), 1)
        .unwrap()
        .as_ulid();
    let placement = PlacementRef {
        strategy_id,
        shard: 4,
    };
    let record = GitRecord {
        event_id: Ulid::from_parts(5_020, 1),
        realm_id,
        group_id: Ulid::from_parts(5_021, 1),
        document_id,
        placement,
        user_id: UserId::local(Ulid::from_parts(5_022, 1), realm_id),
        node_id: owner,
        occurred_at_ms: 1,
        change: GitChange::Lock {
            id: Ulid::from_parts(5_023, 1),
            path: "data/x.bin".into(),
        },
    };
    let target = DocumentTarget::GitRecord {
        document_id,
        event_id: record.event_id,
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);
    service
        .ensure_sync_topics(&[topic_id], Vec::new())
        .expect("Git shard topic genesis");
    let (_, _, bytes) = git_record_entry(&record).expect("record serializes");
    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id: Ulid::from_parts(5_030, 1),
                target: target.clone(),
                bytes: bytes.to_vec(),
                change: record_change(&record),
                allow_genesis: true,
            }],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(&service, topic_id).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("Git record reconciles");

    assert!(
        read_storage_value(&storage, GIT_RECORD_KEYSPACE, target.storage_key())
            .await
            .is_some()
    );
    assert!(
        read_storage_value(&storage, SYNC_REVISION_KEYSPACE, sync_revision_key(&target))
            .await
            .is_some(),
        "a handover proves the record by its revision"
    );
    let manifest = read_storage_value(
        &storage,
        SHARD_MANIFEST_KEYSPACE,
        shard_manifest_key(&placement, &target),
    )
    .await
    .expect("a handover proves the record by its manifest row");
    let entry: aruna_core::document::ShardManifestEntry = postcard::from_bytes(&manifest).unwrap();
    assert_eq!(entry.revision, record_change(&record).current);

    service.shutdown().await;
}
