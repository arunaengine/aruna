//! Tests that replicated repository links keep owner order, refuse forgeries and stay deleted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{REPOSITORY_LINK_KEYSPACE, SHARD_MANIFEST_KEYSPACE};
use aruna_core::repository::{LinkRemote, LinkStatus, RepositoryLink, link_key};
use aruna_core::storage_entries::shard_manifest_key;
use aruna_core::structs::execution::job::RoCrateLimits;

fn link(document_id: Ulid, owner: NodeId, realm_id: RealmId, generation: u64) -> RepositoryLink {
    RepositoryLink {
        link_id: Ulid::from_parts(4_000, 9),
        document_id,
        group_id: Ulid::from_parts(4_001, 1),
        connector_id: Ulid::from_parts(4_002, 1),
        endpoint: "https://zenodo.org/api/".into(),
        owner_node: owner,
        owner_node_url: "https://owner.example/api/v1".into(),
        created_by: UserId::local(Ulid::from_parts(4_003, 1), realm_id),
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: format!("{{\"generation\":{generation}}}"),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: generation,
        limits: RoCrateLimits::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        generation,
        warning: None,
        direction: aruna_core::repository::LinkDirection::Push,
        kind: aruna_core::structs::execution::harvest::RepositoryConnectorKind::Invenio,
    }
}

#[tokio::test]
async fn link_replication_order() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([78; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(78).await,
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
        78,
        UserId::local(Ulid::from_parts(4_004, 1), realm_id),
        realm_id,
    );

    let strategy_id = Ulid::from_parts(4_005, 1);
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

    let document_id = MetaResourceId::from_parts(4_010, handle, BucketId::new(4).unwrap(), 1)
        .unwrap()
        .as_ulid();
    let placement = PlacementRef {
        strategy_id,
        shard: 4,
    };
    let first = link(document_id, owner, realm_id, 10);
    let second = link(document_id, owner, realm_id, 20);
    let forged = link(document_id, node(79), realm_id, 30);
    let target = first.target();
    let topic_id = target.sync_topic_id(realm_id, &placement);
    service
        .ensure_sync_topics(&[topic_id], Vec::new())
        .expect("link shard topic genesis");
    let upsert = |link: &RepositoryLink, event_id: u64| DocumentSyncPublish::Upsert {
        event_id: Ulid::from_parts(event_id, 1),
        target: link.target(),
        bytes: link.to_bytes().expect("link serializes"),
        change: link.sync_change(placement),
        allow_genesis: true,
    };
    let stored = || async {
        read_storage_value(
            &storage,
            REPOSITORY_LINK_KEYSPACE,
            ByteView::from(link_key(document_id, first.link_id)),
        )
        .await
    };
    let reconcile = || async {
        reset_test_cursor(&service, topic_id).await;
        service
            .reconcile_document_topics([topic_id])
            .await
            .expect("link events reconcile")
    };

    // Newer owner revisions win; a row the owner did not publish is refused.
    let published = service
        .publish_documents(
            vec![
                upsert(&second, 4_020),
                upsert(&first, 4_021),
                upsert(&forged, 4_022),
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reconcile().await;
    assert_eq!(
        stored().await.as_deref(),
        Some(second.to_bytes().unwrap().as_slice())
    );

    let delete = second.delete_change(placement);
    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Delete {
                event_id: Ulid::from_parts(4_030, 1),
                target: target.clone(),
                change: delete,
                allow_genesis: true,
            }],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reconcile().await;
    assert!(stored().await.is_none(), "the delete removed the row");
    let manifest = read_storage_value(
        &storage,
        SHARD_MANIFEST_KEYSPACE,
        shard_manifest_key(&placement, &target),
    )
    .await
    .expect("the delete keeps a manifest tombstone");
    let entry: aruna_core::document::ShardManifestEntry = postcard::from_bytes(&manifest).unwrap();
    assert_eq!(entry.revision, delete.current);

    // A later replay of an upsert cannot bring the deleted link back.
    let late = link(document_id, owner, realm_id, 40);
    let published = service
        .publish_documents(vec![upsert(&late, 4_040)], Vec::new())
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reconcile().await;
    assert!(
        stored().await.is_none(),
        "a tombstone keeps the link deleted"
    );

    service.shutdown().await;
}
