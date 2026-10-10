//! Deletes the old heartbeat-carrying node info topics with their sync cursors, quarantine rows and
//! queued publishes. Each node publishes its info again on start, into a fresh `/node-info` topic.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna::identity::PersistedNodeState;
use aruna_core::document::{DocumentOutboxRecord, DocumentTarget};
use aruna_core::id::TopicId as DocumentTopic;
use aruna_core::keyspaces::{NODE_STATE_KEY, NODE_STATE_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::{QUARANTINE_USAGE_KEY, SyncQuarantineUsage};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use irokle::{FjallStorage, Storage, TopicId};
use std::path::Path;

/// Ops one purge transaction deletes at most.
const PURGE_STEP: usize = 10_000;
/// Fan-out cursors of the document sync store.
const FANOUT_KEYSPACE: &str = "document-sync-fanout";

/// Rows of the node database to delete, and the topics to purge after they are gone. Empty when
/// the node no longer holds `/node-info-v2`, so a repeated migration keeps the new topic.
#[derive(Default)]
pub(super) struct InfoCleanup {
    pub(super) topics: Vec<TopicId>,
    pub(super) cursors: Vec<Vec<u8>>,
    pub(super) quarantine: Vec<Vec<u8>>,
    pub(super) usage: Option<Vec<u8>>,
    pub(super) outbox: Vec<Vec<u8>>,
}

/// The node database keyspaces the cleanup reads and writes.
pub(super) struct InfoRows<'a> {
    pub(super) applied: &'a OptimisticTxKeyspace,
    pub(super) quarantine: &'a OptimisticTxKeyspace,
    pub(super) usage: &'a OptimisticTxKeyspace,
    pub(super) outbox: &'a OptimisticTxKeyspace,
}

/// The current `/node-info` topic, which has the id of the first one, and `/node-info-v2`.
fn info_topics(realm_id: RealmId) -> [TopicId; 2] {
    [
        shared_topic(realm_id, b"/node-info"),
        shared_topic(realm_id, b"/node-info-v2"),
    ]
}

/// The realm-shared topic derivation of `DocumentTarget::sync_topic_id`.
fn shared_topic(realm_id: RealmId, suffix: &[u8]) -> TopicId {
    let mut bytes = b"aruna-document-topic-v1".to_vec();
    bytes.extend_from_slice(&DocumentTopic::realm(realm_id).to_bytes());
    bytes.extend_from_slice(suffix);
    TopicId::hash(bytes)
}

fn cursor_key(topic_id: &TopicId) -> Vec<u8> {
    [b"topic-cursor/".as_slice(), topic_id.as_bytes()].concat()
}

pub(super) fn info_cleanup(
    db: &OptimisticTxDatabase,
    sync_path: &Path,
    rows: InfoRows<'_>,
) -> Result<InfoCleanup, ExplorerError> {
    let states = db.keyspace(NODE_STATE_KEYSPACE, KeyspaceCreateOptions::default)?;
    let read = db.read_tx();
    let Some(state) = read.get(&states, NODE_STATE_KEY)? else {
        return Ok(InfoCleanup::default());
    };
    let realm_id = postcard::from_bytes::<PersistedNodeState>(&state)
        .map_err(|error| ExplorerError::Decode(error.to_string()))?
        .realm_id;
    let topics = info_topics(realm_id);
    if !sync_path.exists()
        || FjallStorage::open(sync_path)?
            .topic_state(&topics[1])?
            .is_none()
    {
        return Ok(InfoCleanup::default());
    }
    let mut cleanup = InfoCleanup {
        topics: topics.to_vec(),
        ..InfoCleanup::default()
    };
    let mut usage = match read.get(rows.usage, QUARANTINE_USAGE_KEY)? {
        Some(value) => SyncQuarantineUsage::from_bytes(&value)
            .map_err(|error| ExplorerError::Decode(error.to_string()))?,
        None => SyncQuarantineUsage::default(),
    };
    for topic_id in &topics {
        if read.get(rows.applied, cursor_key(topic_id))?.is_some() {
            cleanup.cursors.push(cursor_key(topic_id));
        }
        for entry in read.prefix(rows.quarantine, topic_id.as_bytes()) {
            let (key, value) = entry.into_inner()?;
            usage.records = usage.records.saturating_sub(1);
            usage.bytes = usage.bytes.saturating_sub(value.len() as u64);
            cleanup.quarantine.push(key.to_vec());
        }
    }
    if !cleanup.quarantine.is_empty() {
        cleanup.usage = Some(
            usage
                .to_bytes()
                .map_err(|error| ExplorerError::Decode(error.to_string()))?,
        );
    }
    for entry in read.iter(rows.outbox) {
        let (key, value) = entry.into_inner()?;
        let node_info = postcard::from_bytes::<DocumentOutboxRecord>(&value)
            .is_ok_and(|record| matches!(record.target, DocumentTarget::NodeInfo { .. }));
        if node_info {
            cleanup.outbox.push(key.to_vec());
        }
    }
    Ok(cleanup)
}

/// Purges `topics` and their fan-out cursors from the document sync store, in order, so the
/// `/node-info-v2` topic that marks pending work goes last. Returns ops and cursors deleted.
pub(super) fn purge_topics(
    sync_path: &Path,
    topics: &[TopicId],
) -> Result<(usize, usize), ExplorerError> {
    if topics.is_empty() {
        return Ok((0, 0));
    }
    let db = OptimisticTxDatabase::builder(sync_path).open()?;
    let fanout = db.keyspace(FANOUT_KEYSPACE, KeyspaceCreateOptions::default)?;
    let storage = FjallStorage::from_database(db.clone())?;
    let (mut ops, mut cursors) = (0, 0);
    for topic_id in topics {
        for provisional in storage.provisional_topics()? {
            if provisional.topic_id == *topic_id {
                storage.discard_provisional(&provisional)?;
            }
        }
        if fanout.get(cursor_key(topic_id))?.is_some() {
            fanout.remove(cursor_key(topic_id))?;
            cursors += 1;
        }
        ops += storage.purge_topic(topic_id, PURGE_STEP)?;
    }
    Ok((ops, cursors))
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{node_state, read, write};
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision,
    };
    use aruna_core::keyspaces::{
        APPLIED_OPS_KEYSPACE, QUARANTINE_USAGE_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
        SYNC_QUARANTINE_KEYSPACE,
    };
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_operations::sync::document_outbox::new_outbox_record;
    use irokle::oplog::Oplog;
    use irokle::{Ed25519Signer, ReplicationPolicy, Signer, TopicGenesis, actor_id_for};
    use tempfile::tempdir;
    use ulid::Ulid;

    const REALM: RealmId = RealmId([3u8; 32]);

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn genesis(storage: &FjallStorage, topic_id: TopicId) {
        let signer = Ed25519Signer::from_bytes(&[5u8; 32]);
        let genesis = TopicGenesis {
            event_type_id: "test".to_string(),
            initial_peers: Default::default(),
            replication_policy: ReplicationPolicy::default(),
        };
        Oplog::with_storage(storage.clone())
            .create_topic_genesis(
                topic_id,
                actor_id_for(topic_id, signer.peer_id()),
                genesis,
                &signer,
            )
            .unwrap();
    }

    fn outbox(target: DocumentTarget) -> Vec<u8> {
        let change = DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_bytes([1u8; 16]),
                actor: node(1),
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let event = DocumentOutboxEvent::Upsert {
            bytes: vec![1],
            change,
        };
        let record =
            new_outbox_record(node(1), target, Vec::new(), event, PlacementRef::NIL, false);
        postcard::to_allocvec(&record).unwrap()
    }

    #[test]
    fn topic_matches_target() {
        let target = DocumentTarget::NodeInfo {
            realm_id: REALM,
            node_id: node(2),
        };
        assert_eq!(
            info_topics(REALM)[0],
            target.sync_topic_id(REALM, &PlacementRef::NIL)
        );
    }

    /// Both node info topics, their cursors, quarantine rows and queued publishes go, while
    /// other topics stay; a repeat keeps the new `/node-info` topic the node created since.
    #[test]
    fn deletes_info_topics() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let sync = path.join("document-sync");
        node_state(&path);
        let [current, former] = info_topics(REALM);
        let other = shared_topic(REALM, b"/node-usage");
        {
            let db = OptimisticTxDatabase::builder(&sync).open().unwrap();
            let storage = FjallStorage::from_database(db.clone()).unwrap();
            for topic_id in [current, former, other] {
                genesis(&storage, topic_id);
            }
            let fanout = db
                .keyspace(FANOUT_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            fanout.insert(cursor_key(&former), b"cursor").unwrap();
        }
        let info = DocumentTarget::NodeInfo {
            realm_id: REALM,
            node_id: node(1),
        };
        let usage = DocumentTarget::NodeUsage {
            realm_id: REALM,
            node_id: node(1),
            group_id: None,
        };
        write(
            &path,
            SYNC_OUTBOX_KEYSPACE,
            vec![
                (b"info".as_slice(), outbox(info)),
                (b"usage".as_slice(), outbox(usage)),
            ],
        );
        let quarantined = [former.as_bytes().as_slice(), b"actor"].concat();
        let kept = [other.as_bytes().as_slice(), b"actor"].concat();
        write(
            &path,
            SYNC_QUARANTINE_KEYSPACE,
            vec![
                (quarantined.as_slice(), vec![0; 10]),
                (kept.as_slice(), vec![0; 6]),
            ],
        );
        let total = SyncQuarantineUsage {
            records: 2,
            bytes: 16,
        };
        write(
            &path,
            QUARANTINE_USAGE_KEYSPACE,
            vec![(QUARANTINE_USAGE_KEY, total.to_bytes().unwrap())],
        );
        let cursor = cursor_key(&current);
        write(
            &path,
            APPLIED_OPS_KEYSPACE,
            vec![(cursor.as_slice(), b"cursor".to_vec())],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(
            (output.info_ops_deleted, output.info_cursors_deleted),
            (2, 2)
        );
        assert_eq!(
            (output.info_outbox_deleted, output.info_quarantine_deleted),
            (1, 1)
        );
        let outbox_rows = read(&path, SYNC_OUTBOX_KEYSPACE);
        assert_eq!(
            outbox_rows.into_keys().collect::<Vec<_>>(),
            vec![b"usage".to_vec()]
        );
        let quarantine_rows = read(&path, SYNC_QUARANTINE_KEYSPACE);
        assert_eq!(quarantine_rows.into_keys().collect::<Vec<_>>(), vec![kept]);
        let usage_row = &read(&path, QUARANTINE_USAGE_KEYSPACE)[QUARANTINE_USAGE_KEY];
        let left = SyncQuarantineUsage::from_bytes(usage_row).unwrap();
        assert_eq!((left.records, left.bytes), (1, 6));
        assert!(read(&path, APPLIED_OPS_KEYSPACE).is_empty());
        {
            let storage = FjallStorage::open(&sync).unwrap();
            assert!(storage.topic_state(&current).unwrap().is_none());
            assert!(storage.topic_state(&former).unwrap().is_none());
            assert!(storage.topic_state(&other).unwrap().is_some());
            genesis(&storage, current);
        }

        let repeat = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(repeat.info_ops_deleted, 0);
        let storage = FjallStorage::open(&sync).unwrap();
        assert!(storage.topic_state(&current).unwrap().is_some());
    }
}
