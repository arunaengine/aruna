//! Applies node info an older version quarantined but this version reads, and drops those rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::sync_quarantine::{
    QUARANTINE_USAGE_KEY, SyncQuarantineRecord, SyncQuarantineUsage,
};
use aruna_core::sync_checks::{node_info_supersedes, quarantined_node_info};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use std::collections::BTreeMap;

/// Node info rows to write, quarantine rows to delete and the usage row that releases them.
#[derive(Default)]
pub(super) struct InfoRecovery {
    /// Quarantined node info upserts this version accepts.
    pub(super) scanned: usize,
    pub(super) documents: BTreeMap<Vec<u8>, Vec<u8>>,
    pub(super) removed: Vec<Vec<u8>>,
    pub(super) usage: Option<Vec<u8>>,
}

/// The keyspaces the recovery reads.
pub(super) struct InfoStores<'a> {
    pub(super) quarantine: &'a OptimisticTxKeyspace,
    pub(super) nodes: &'a OptimisticTxKeyspace,
    pub(super) usage: &'a OptimisticTxKeyspace,
}

/// What this migration already does to those rows: quarantine rows it deletes with the usage
/// they leave, node info rows it rewrites and node info rows it deletes.
pub(super) struct InfoChanges<'a> {
    pub(super) cleared: &'a [Vec<u8>],
    pub(super) usage: Option<&'a [u8]>,
    pub(super) relaid: &'a [(Vec<u8>, Vec<u8>)],
    pub(super) deleted: &'a [Vec<u8>],
}

/// Every quarantined node info upsert of the local realm `realm_id` that now validates leaves the
/// quarantine; one newer than the stored document is written, so a peer's advertisement an older
/// version could not read is not lost. Without a node state nothing is recovered.
pub(super) fn recovered_infos(
    db: &OptimisticTxDatabase,
    realm_id: Option<RealmId>,
    stores: InfoStores<'_>,
    changes: InfoChanges<'_>,
) -> Result<InfoRecovery, ExplorerError> {
    let Some(realm_id) = realm_id else {
        return Ok(InfoRecovery::default());
    };
    let decode = |error: aruna_core::errors::ConversionError| {
        ExplorerError::Decode(format!("quarantine usage: {error}"))
    };
    let read = db.read_tx();
    let stored = read.get(stores.usage, QUARANTINE_USAGE_KEY)?;
    let mut usage = match changes.usage.or(stored.as_deref()) {
        Some(value) => SyncQuarantineUsage::from_bytes(value).map_err(decode)?,
        None => SyncQuarantineUsage::default(),
    };
    let mut recovery = InfoRecovery::default();
    for entry in read.iter(stores.quarantine) {
        let (key, value) = entry.into_inner()?;
        if changes.cleared.iter().any(|cleared| cleared[..] == key[..]) {
            continue;
        }
        let Ok(record) = SyncQuarantineRecord::from_bytes(&value) else {
            continue;
        };
        let Some((target, document, bytes)) = quarantined_node_info(&record, realm_id) else {
            continue;
        };
        recovery.scanned += 1;
        let row = target.storage_key().to_vec();
        let current = match recovery.documents.get(&row) {
            Some(pending) => Some(pending.clone()),
            None if changes.deleted.contains(&row) => None,
            None => match changes.relaid.iter().find(|(key, _)| *key == row) {
                Some((_, relaid)) => Some(relaid.clone()),
                None => read.get(stores.nodes, &row)?.map(|value| value.to_vec()),
            },
        };
        if node_info_supersedes(&document, current.as_deref()) {
            recovery.documents.insert(row, bytes);
        }
        usage.release(1, value.len() as u64);
        recovery.removed.push(key.to_vec());
    }
    if !recovery.removed.is_empty() {
        recovery.usage = Some(usage.to_bytes().map_err(decode)?);
    }
    Ok(recovery)
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{node_state, read, write};
    use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentEvent, DocumentSyncRevision, DocumentTarget,
    };
    use aruna_core::keyspaces::{
        NODE_INFO_KEYSPACE, QUARANTINE_USAGE_KEYSPACE, SYNC_QUARANTINE_KEYSPACE,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::node_info::{
        AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
    };
    use aruna_core::structs::sync_quarantine::{
        QUARANTINE_USAGE_KEY, SyncQuarantineEvidence, SyncQuarantineIdentity, SyncQuarantineRecord,
        SyncQuarantineUsage,
    };
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn document(node_id: aruna_core::id::NodeId) -> NodeInfoDocument {
        let epoch = AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 2,
            observed_at_ms: 1_000,
        };
        NodeInfoDocument {
            node_id,
            executors: Vec::new(),
            labels: BTreeMap::new(),
            urls: NodeUrls {
                api: Some("https://node.example".to_string()),
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 1,
                documents_held: None,
                load_permille: None,
                heartbeat_at_ms: 1_000,
                download_slots: None,
                storage_capacity_bytes: None,
            },
            updated_at_ms: 1_000,
            epoch,
            compute_draining: false,
            leaving: false,
            demand: ComputeDemandSnapshot {
                epoch,
                ..Default::default()
            },
            reservation: ComputeReservationSnapshot {
                epoch,
                ..Default::default()
            },
        }
    }

    /// A quarantine row of a node info upsert the peer published on `topic`, with `reason`.
    fn quarantined(
        target: &DocumentTarget,
        topic: irokle::TopicId,
        actor_seq: u64,
        reason: &str,
    ) -> (Vec<u8>, Vec<u8>) {
        let DocumentTarget::NodeInfo { node_id, .. } = target else {
            panic!("a node info target");
        };
        let event = DocumentEvent::Upsert {
            event_id: Ulid::from_parts(1, u128::from(actor_seq)),
            target: target.clone(),
            bytes: document(*node_id).to_bytes().unwrap(),
            change: DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_parts(1, u128::from(actor_seq)),
                    actor: *node_id,
                    updated_at_ms: 1_000,
                },
                kind: DocumentChangeKind::Upsert,
                placement: PlacementRef::NIL,
            },
        };
        let peer = irokle::PeerId::from_bytes(*node_id.as_bytes());
        let identity = SyncQuarantineIdentity {
            topic,
            actor: irokle::actor_id_for(topic, peer),
            actor_seq,
        };
        let evidence = SyncQuarantineEvidence::from_event(&event);
        let record = SyncQuarantineRecord::new(identity, evidence, reason, 1);
        (record.storage_key(), record.to_bytes().unwrap())
    }

    /// Stores quarantine `rows` with the usage they take.
    fn store(path: &std::path::Path, rows: Vec<(Vec<u8>, Vec<u8>)>) {
        let usage = SyncQuarantineUsage {
            records: rows.len() as u64,
            bytes: rows.iter().map(|(_, row)| row.len() as u64).sum(),
        };
        let rows = (rows.iter()).map(|(key, row)| (&key[..], row.clone()));
        write(path, SYNC_QUARANTINE_KEYSPACE, rows.collect());
        write(
            path,
            QUARANTINE_USAGE_KEYSPACE,
            vec![(QUARANTINE_USAGE_KEY, usage.to_bytes().unwrap())],
        );
    }

    // A peer's node info an older version quarantined is applied, its quarantine row and usage
    // go, and a second run changes nothing.
    #[test]
    fn recovers_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        node_state(path);
        let realm_id = RealmId::from_bytes([3; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let target = DocumentTarget::NodeInfo { realm_id, node_id };
        let bytes = document(node_id).to_bytes().unwrap();
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let reason = "invalid node info document: Found a bool that wasn't 0 or 1";
        store(path, vec![quarantined(&target, topic, 17, reason)]);

        let database = path.to_str().unwrap();
        let output = migrate_output(database).unwrap();
        assert_eq!(
            (
                output.quarantined_infos_scanned,
                output.node_infos_recovered,
                output.quarantine_rows_cleared
            ),
            (1, 1, 1)
        );
        let stored = read(path, NODE_INFO_KEYSPACE);
        assert_eq!(stored.get(target.storage_key().as_ref()), Some(&bytes));
        assert!(read(path, SYNC_QUARANTINE_KEYSPACE).is_empty());
        let usage = read(path, QUARANTINE_USAGE_KEYSPACE);
        let usage = SyncQuarantineUsage::from_bytes(&usage[QUARANTINE_USAGE_KEY]).unwrap();
        assert_eq!(usage, SyncQuarantineUsage::default());

        let again = migrate_output(database).unwrap();
        assert_eq!(
            (again.node_infos_recovered, again.quarantine_rows_cleared),
            (0, 0)
        );
        assert_eq!(read(path, NODE_INFO_KEYSPACE), stored);
    }

    // Node info live sync refused for its publisher policy, sent on another topic or of another
    // realm keeps its quarantine row and usage, and nothing is applied.
    #[test]
    fn keeps_other_rejections() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        node_state(path);
        let realm_id = RealmId::from_bytes([3; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let target = DocumentTarget::NodeInfo { realm_id, node_id };
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let decode = "invalid node info document: Found a bool that wasn't 0 or 1";
        let policy = "shard publisher is outside the current holder set";
        let other = RealmId::from_bytes([4; 32]);
        let foreign = DocumentTarget::NodeInfo {
            realm_id: other,
            node_id,
        };
        let rows = vec![
            quarantined(&target, topic, 1, policy),
            quarantined(&target, irokle::TopicId::from_bytes([8; 32]), 2, decode),
            quarantined(
                &foreign,
                foreign.sync_topic_id(other, &PlacementRef::NIL),
                3,
                decode,
            ),
        ];
        store(path, rows);
        let quarantine = read(path, SYNC_QUARANTINE_KEYSPACE);
        let usage = read(path, QUARANTINE_USAGE_KEYSPACE);

        let output = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(
            (
                output.quarantined_infos_scanned,
                output.node_infos_recovered,
                output.quarantine_rows_cleared
            ),
            (0, 0, 0)
        );
        assert!(read(path, NODE_INFO_KEYSPACE).is_empty());
        assert_eq!(read(path, SYNC_QUARANTINE_KEYSPACE), quarantine);
        assert_eq!(read(path, QUARANTINE_USAGE_KEYSPACE), usage);
    }
}
