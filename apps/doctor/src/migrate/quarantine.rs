//! Applies node info an older version quarantined but this version reads, and drops those rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::explorer::ExplorerError;
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

/// Every quarantined node info upsert that now validates leaves the quarantine; one that is newer
/// than the stored document is written, so a peer's advertisement an older version could not read
/// is not lost. A rerun finds nothing.
pub(super) fn recovered_infos(
    db: &OptimisticTxDatabase,
    stores: InfoStores<'_>,
    changes: InfoChanges<'_>,
) -> Result<InfoRecovery, ExplorerError> {
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
        let Some((target, document, bytes)) = quarantined_node_info(&record) else {
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
    use super::super::tests::{read, write};
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

    // A peer's node info an older version quarantined is applied, its quarantine row and usage
    // go, and a second run changes nothing.
    #[test]
    fn recovers_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let realm_id = RealmId::from_bytes([3; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let target = DocumentTarget::NodeInfo { realm_id, node_id };
        let bytes = document(node_id).to_bytes().unwrap();
        let event = DocumentEvent::Upsert {
            event_id: Ulid::from_parts(1, 1),
            target: target.clone(),
            bytes: bytes.clone(),
            change: DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_parts(1, 1),
                    actor: node_id,
                    updated_at_ms: 1_000,
                },
                kind: DocumentChangeKind::Upsert,
                placement: PlacementRef::NIL,
            },
        };
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let peer = irokle::PeerId::from_bytes(*node_id.as_bytes());
        let identity = SyncQuarantineIdentity {
            topic,
            actor: irokle::actor_id_for(topic, peer),
            actor_seq: 17,
        };
        let evidence = SyncQuarantineEvidence::from_event(&event);
        let reason = "invalid node info document: Found a bool that wasn't 0 or 1";
        let record = SyncQuarantineRecord::new(identity, evidence, reason, 1);
        let row = record.to_bytes().unwrap();
        let usage = SyncQuarantineUsage {
            records: 1,
            bytes: row.len() as u64,
        };
        write(
            path,
            SYNC_QUARANTINE_KEYSPACE,
            vec![(&record.storage_key()[..], row)],
        );
        write(
            path,
            QUARANTINE_USAGE_KEYSPACE,
            vec![(QUARANTINE_USAGE_KEY, usage.to_bytes().unwrap())],
        );

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
}
