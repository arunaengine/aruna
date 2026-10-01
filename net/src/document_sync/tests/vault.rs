//! Tests that holders keep concurrent vault saves as heads and never revive a replaced one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{VAULT_RETIRED_KEYSPACE, VAULT_REVISION_KEYSPACE};
use aruna_core::structs::identity::user::vault::{VaultRevision, record_rows, user_record_key};

struct Holder {
    _storage_dir: TempDir,
    _doc_dir: TempDir,
    storage: StorageHandle,
    service: DocumentSyncService,
    owner: NodeId,
    user_id: UserId,
    placement: PlacementRef,
}

impl Holder {
    async fn open() -> Self {
        let (storage_dir, storage) = test_storage();
        let doc_dir = tempfile::tempdir().expect("doc dir");
        let realm_id = RealmId::from_bytes([82; 32]);
        let service = DocumentSyncService::open_with_policy(
            test_endpoint(82).await,
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
        Self {
            _storage_dir: storage_dir,
            _doc_dir: doc_dir,
            storage,
            service,
            owner,
            user_id: UserId::local(Ulid::from_parts(6_002, 1), realm_id),
            placement: PlacementRef {
                strategy_id: Ulid::from_parts(6_001, 1),
                shard: 3,
            },
        }
    }

    fn revision(&self, id: u64, predecessors: &[u64]) -> VaultRevision {
        VaultRevision {
            user_id: self.user_id,
            revision_id: Ulid::from_parts(id, 1),
            predecessors: predecessors
                .iter()
                .map(|id| Ulid::from_parts(*id, 1))
                .collect(),
            payload: Some(format!("sealed {id}")),
            node_id: self.owner,
            placement: self.placement,
            created_at_ms: id,
        }
    }

    /// Publishes the records on the user's shard topic and applies them like a holder.
    async fn apply(&self, records: &[VaultRevision], first_event: u64) {
        let topic_id = records[0]
            .target()
            .sync_topic_id(self.user_id.realm_id, &self.placement);
        self.service
            .ensure_sync_topics(&[topic_id], Vec::new())
            .expect("vault shard topic genesis");
        let events = records
            .iter()
            .zip(first_event..)
            .map(|(record, event)| DocumentSyncPublish::Upsert {
                event_id: Ulid::from_parts(event, 1),
                target: record.target(),
                bytes: record.to_bytes().expect("revision serializes"),
                change: record.sync_change(),
                allow_genesis: true,
            })
            .collect();
        let published = self.service.publish_documents(events, Vec::new()).await;
        assert!(matches!(
            published,
            DocumentNetEvent::DocumentsPublished { .. }
        ));
        reset_test_cursor(&self.service, topic_id).await;
        self.service
            .reconcile_document_topics([topic_id])
            .await
            .expect("vault records reconcile");
    }

    async fn stored(&self, id: u64) -> bool {
        let key = user_record_key(self.user_id, Ulid::from_parts(id, 1));
        read_storage_value(&self.storage, VAULT_REVISION_KEYSPACE, key)
            .await
            .is_some()
    }
}

#[tokio::test]
async fn keeps_concurrent_heads() {
    let holder = Holder::open().await;
    // Two saves on the same predecessor both stay as heads for the portal to merge.
    let first = [
        holder.revision(1, &[]),
        holder.revision(2, &[1]),
        holder.revision(3, &[1]),
    ];
    holder.apply(&first, 7_000).await;
    assert!(!holder.stored(1).await, "a replaced save is no head");
    assert!(holder.stored(2).await && holder.stored(3).await);

    // The merge retires both heads; a replay of the old save stays retired.
    holder
        .apply(
            &[holder.revision(4, &[2, 3]), holder.revision(1, &[])],
            7_010,
        )
        .await;
    for retired in [1, 2, 3] {
        assert!(
            !holder.stored(retired).await,
            "revision {retired} is retired"
        );
    }
    assert!(holder.stored(4).await);

    // A record must come from the node that admitted it.
    let mut forged = holder.revision(5, &[4]);
    forged.node_id = iroh::SecretKey::from_bytes(&[9; 32]).public();
    holder.apply(&[forged], 7_020).await;
    assert!(!holder.stored(5).await, "a relayed author is refused");
    assert!(holder.stored(4).await);

    holder.service.shutdown().await;
}

#[tokio::test]
async fn shuffled_retirement_replays() {
    for deleted in [2, 3] {
        for order in [[1, 2, 3], [1, 3, 2], [3, 2, 1]] {
            let holder = Holder::open().await;
            let mut records = [
                holder.revision(1, &[]),
                holder.revision(2, &[1]),
                holder.revision(3, &[2]),
            ];
            records[deleted - 1].payload = None;
            for replay in [false, true] {
                let sequence = if replay { [2, 1, 3] } else { order };
                for (event, id) in sequence.into_iter().enumerate() {
                    holder
                        .apply(
                            &[records[id - 1].clone()],
                            8_000 + u64::from(replay) * 10 + event as u64,
                        )
                        .await;
                }
                for id in [1, 2] {
                    assert!(!holder.stored(id).await, "revision {id} is retired");
                    assert!(
                        read_storage_value(
                            &holder.storage,
                            VAULT_RETIRED_KEYSPACE,
                            user_record_key(holder.user_id, Ulid::from_parts(id, 1)),
                        )
                        .await
                        .is_some()
                    );
                }
                assert!(holder.stored(3).await);
                for record in &records {
                    for (keyspace, key, expected) in record_rows(
                        &record.target(),
                        &record.to_bytes().unwrap(),
                        &record.sync_change(),
                    )
                    .unwrap()
                    {
                        if keyspace == VAULT_REVISION_KEYSPACE
                            && record.revision_id != records[2].revision_id
                        {
                            continue;
                        }
                        assert_eq!(
                            read_storage_value(&holder.storage, &keyspace, key).await,
                            Some(expected),
                            "revision {} retains its {keyspace} row",
                            record.revision_id
                        );
                    }
                }
            }
            holder.service.shutdown().await;
        }
    }
}
