//! Writes the size row of every logged metadata event that has none, so budget checks can
//! sum size rows instead of decoding the history.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, decode_error};
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::EVENT_LOG_KEYSPACE;
use aruna_core::metadata::{EventSize, MetadataEventRecord};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

/// Size rows to write for logged events whose size row is missing.
pub(super) fn missing_sizes(
    db: &OptimisticTxDatabase,
    events: &OptimisticTxKeyspace,
    sizes: &OptimisticTxKeyspace,
) -> Result<Rewrites, ExplorerError> {
    let read = db.read_tx();
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in read.iter(events) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        if read.get(sizes, &key)?.is_some() {
            continue;
        }
        let event: MetadataEventRecord = postcard::from_bytes(&value)
            .map_err(|error| decode_error(EVENT_LOG_KEYSPACE, &key, error))?;
        let size = EventSize {
            node_id: event.node_id,
            bytes: value.len() as u64,
        };
        let size = postcard::to_allocvec(&size)
            .map_err(|error| decode_error(EVENT_LOG_KEYSPACE, &key, error))?;
        rows.push((key.to_vec(), size));
    }
    Ok(Rewrites { scanned, rows })
}

#[cfg(test)]
mod tests {
    use super::super::migrate_output;
    use super::super::tests::{read, write};
    use aruna_core::keyspaces::{EVENT_LOG_KEYSPACE, EVENT_SIZE_KEYSPACE};
    use aruna_core::metadata::{EventSize, MetadataEventPayload, MetadataEventRecord};
    use aruna_core::storage_entries::logged_event_entries;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use ulid::Ulid;

    fn event() -> MetadataEventRecord {
        let realm_id = RealmId::from_bytes([8; 32]);
        let (group_id, document_id) = (Ulid::from(1), Ulid::from(2));
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        MetadataEventRecord {
            event_id: document_id,
            record: MetadataRegistryRecord {
                realm_id,
                group_id,
                document_id,
                document_path: "datasets/sizes".into(),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: false,
                permission_path: String::new(),
                placement: PlacementRef::NIL,
                holder_node_ids: vec![node],
                created_at_ms: 1,
                updated_at_ms: 1,
                establishing_event_id: document_id,
                last_event_id: document_id,
            },
            user_id: aruna_core::UserId::nil(realm_id),
            node_id: node,
            payload: MetadataEventPayload::Scaffold {
                name: "Sizes".into(),
                description: "Size rows".into(),
                date_published: "2026-01-01".into(),
                license: None,
            },
            occurred_at_ms: 1,
        }
    }

    #[test]
    fn writes_missing_sizes() {
        let event = event();
        let entries = logged_event_entries(&event).expect("entries");
        let (_, key, log) = &entries[0];
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("db");
        let database = path.to_str().expect("path");
        write(
            &path,
            EVENT_LOG_KEYSPACE,
            vec![(key.as_ref(), log.to_vec())],
        );

        let output = migrate_output(database).expect("migration runs");

        assert_eq!(output.event_sizes_written, 1);
        let rows = read(&path, EVENT_SIZE_KEYSPACE);
        let size: EventSize = postcard::from_bytes(&rows[key.as_ref()]).expect("size decodes");
        assert_eq!(size.node_id, event.node_id);
        assert_eq!(size.bytes, log.len() as u64);
        let again = migrate_output(database).expect("migration repeats");
        assert_eq!(again.event_sizes_written, 0);
    }
}
