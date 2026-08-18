//! The quarantine store's hard capacity against the replication path that fills
//! it (#338, fixed choice D-4).
//!
//! Evidence is written by the sync apply path and reclaimed by the admin
//! surface, so the fail-closed rule only holds if both halves agree: while
//! unacknowledged evidence fills the store, a rejected event must neither be
//! dropped nor advance its topic cursor, and reclaiming capacity must let the
//! redelivered event persist.

use aruna_core::alpn::Alpn;
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncEvent, DocumentSyncNetEvent,
    DocumentSyncPublish, DocumentSyncRevision, DocumentSyncTarget,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE, SYNC_QUARANTINE_KEYSPACE};
use aruna_core::structs::{
    NodeInfoDocument, NodeUrls, NodeUtilization, PlacementRef, RealmId,
    SYNC_QUARANTINE_MAX_RECORDS, SyncQuarantineEvidence, SyncQuarantineIdentity,
    SyncQuarantineRecord, SyncQuarantineUsage, quarantine_row_entry, quarantine_usage_entry,
};
use aruna_core::types::Value;
use aruna_net::document_sync::DocumentSyncService;
use aruna_operations::driver::DriverContext;
use aruna_operations::sync_quarantine::{
    QuarantinePageRequest, acknowledge_quarantine_row, list_quarantine_records,
    prune_quarantine_records, read_quarantine_record, read_quarantine_usage,
};
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use byteview::ByteView;
use ulid::Ulid;

fn context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn write_batch(storage: &StorageHandle, writes: Vec<(String, ByteView, Value)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// Mirrors the applied-ops cursor key of the sync path: publishing marks its
/// own ops applied, so a fresh peer's delivery has to be simulated.
async fn reset_cursor(storage: &StorageHandle, topic: &[u8]) {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic);
    write_batch(
        storage,
        vec![(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            ByteView::from(key),
            ByteView::from(
                postcard::to_allocvec(&irokle::ActorClock::default()).expect("clock serializes"),
            ),
        )],
    )
    .await;
}

fn seed_event(index: u64) -> DocumentSyncEvent {
    DocumentSyncEvent::Upsert {
        event_id: Ulid::from_parts(index, 1),
        target: DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([9; 32]),
        },
        bytes: vec![7; 16],
        change: DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(index, 1),
                actor: iroh::SecretKey::from_bytes(&[1; 32]).public(),
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        },
    }
}

/// Fill the store to its record capacity with unacknowledged evidence.
async fn fill_store(storage: &StorageHandle) -> SyncQuarantineUsage {
    let mut usage = SyncQuarantineUsage::default();
    let mut writes = Vec::new();
    for index in 0..SYNC_QUARANTINE_MAX_RECORDS {
        let record = SyncQuarantineRecord::new(
            SyncQuarantineIdentity::from_parts([7; 32], [8; 32], index + 1),
            SyncQuarantineEvidence::from_event(&seed_event(index)),
            "seeded evidence",
            index,
        );
        let entry = quarantine_row_entry(&record).expect("row entry");
        usage.records += 1;
        usage.bytes += entry.2.len() as u64;
        writes.push(entry);
        if writes.len() == 512 {
            write_batch(storage, std::mem::take(&mut writes)).await;
        }
    }
    writes.push(quarantine_usage_entry(usage).expect("usage entry"));
    write_batch(storage, writes).await;
    usage
}

fn node_info_bytes(node_id: aruna_core::NodeId) -> Vec<u8> {
    NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: std::collections::BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 5,
        },
        updated_at_ms: 5,
        epoch: aruna_core::structs::AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 5,
        },
        compute_draining: false,
        leaving: false,
    }
    .to_bytes()
    .expect("node info serializes")
}

// capacity blocks, then releases
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_blocks_releases() {
    let storage_dir = tempfile::tempdir().expect("storage dir");
    let storage = aruna_storage::FjallStorage::open(storage_dir.path().to_str().expect("path"))
        .expect("storage opens");
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([31; 32]);
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(iroh::SecretKey::from_bytes(&[31; 32]))
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![Alpn::DocumentSync.as_bytes().to_vec()])
        .bind_addr(
            "127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .expect("bind address"),
        )
        .expect("bind address configures")
        .bind()
        .await
        .expect("endpoint binds");
    let service = DocumentSyncService::open_with_persist_policy(
        endpoint,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        Default::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let ctx = context(storage.clone());
    let local_node = iroh::SecretKey::from_bytes(&[31; 32]).public();

    let seeded = fill_store(&storage).await;
    assert_eq!(read_quarantine_usage(&ctx).await.unwrap(), seeded);

    // A node info document that claims another node is a permanent reject.
    let target = DocumentSyncTarget::NodeInfo {
        realm_id,
        node_id: local_node,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let event_id = Ulid::from_parts(9_001, 1);
    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id,
                target: target.clone(),
                bytes: node_info_bytes(iroh::SecretKey::from_bytes(&[5; 32]).public()),
                change: DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: 1,
                        event_id,
                        actor: local_node,
                        updated_at_ms: 1,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: PlacementRef::NIL,
                },
                allow_genesis: true,
            }],
            Vec::new(),
        )
        .await;
    assert!(
        matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "publish failed: {published:?}"
    );

    // Evidence rows are keyed by transport identity, so the tested topic's
    // rows are exactly the ones under its own key prefix.
    let topic_page = || QuarantinePageRequest {
        start_after: None,
        topic: Some(topic_id.as_bytes().to_vec()),
        limit: Some(4),
    };
    reset_cursor(&storage, topic_id.as_bytes()).await;
    service.reconcile_documents_event().await;
    assert!(
        list_quarantine_records(&ctx, topic_page())
            .await
            .unwrap()
            .records
            .is_empty(),
        "a full store must not write evidence"
    );
    assert_eq!(read_quarantine_usage(&ctx).await.unwrap(), seeded);

    // Reclaim one slot: only acknowledged rows are prunable.
    let seeded_page = || QuarantinePageRequest {
        start_after: None,
        topic: Some(vec![7u8; 32]),
        limit: Some(1),
    };
    let page = list_quarantine_records(&ctx, seeded_page()).await.unwrap();
    let oldest = page.records.first().expect("seeded evidence").storage_key();
    let pruned = prune_quarantine_records(&ctx, seeded_page()).await.unwrap();
    assert_eq!(pruned.pruned, 0, "unacknowledged evidence is never pruned");
    acknowledge_quarantine_row(&ctx, &oldest)
        .await
        .unwrap()
        .expect("row is acknowledged");
    let pruned = prune_quarantine_records(&ctx, seeded_page()).await.unwrap();
    assert_eq!(pruned.pruned, 1);
    assert_eq!(pruned.usage.records, SYNC_QUARANTINE_MAX_RECORDS - 1);

    // The event was never applied and its cursor never advanced, so the next
    // reconcile redelivers it and the reclaimed slot takes the evidence.
    service.reconcile_documents_event().await;
    let redelivered = list_quarantine_records(&ctx, topic_page()).await.unwrap();
    let record = redelivered
        .records
        .first()
        .expect("redelivered evidence persists");
    assert_eq!(redelivered.records.len(), 1);
    assert_eq!(record.event_id(), Some(event_id));
    assert!(record.reason.starts_with("invalid node info document:"));
    assert_eq!(record.target(), Some(&target));
    assert_eq!(
        read_quarantine_record(&ctx, &record.storage_key())
            .await
            .unwrap()
            .as_ref(),
        Some(record)
    );
    assert_eq!(
        read_quarantine_usage(&ctx).await.unwrap().records,
        SYNC_QUARANTINE_MAX_RECORDS
    );
    assert!(
        matches!(
            storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
                    key: ByteView::from(oldest.clone()),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ),
        "the pruned row stays gone"
    );

    service.shutdown().await;
}
