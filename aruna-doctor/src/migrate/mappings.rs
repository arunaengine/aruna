//! Re-encodes persistent id mappings from before the secondary identifiers, including the
//! copies queued for publishing, and rebuilds the identifier index from the mappings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{Rewrites, decode_error, rewritten};
use crate::explorer::ExplorerError;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
use aruna_core::structs::{LegacyMapping, PersistentIdMapping, secondary_index_entries};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use std::collections::BTreeMap;

/// Identifier index rows to write and to remove so the index matches the mappings.
pub(super) struct IndexRebuild {
    pub(super) writes: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) removes: Vec<Vec<u8>>,
}

/// Queued publishes carry the mapping bytes, so a legacy mapping inside one is re-encoded.
pub(super) fn outbox_rows(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    name: &str,
) -> Result<Rewrites, ExplorerError> {
    let mut scanned = 0;
    let mut rows = Vec::new();
    for entry in db.read_tx().iter(keyspace) {
        let (key, value) = entry.into_inner()?;
        scanned += 1;
        let mut record: DocumentOutboxRecord =
            postcard::from_bytes(&value).map_err(|error| decode_error(name, &key, error))?;
        let DocumentOutboxEvent::Upsert { bytes, .. } = &mut record.event else {
            continue;
        };
        if !matches!(record.target, DocumentTarget::PersistentIdMapping { .. }) {
            continue;
        }
        match rewritten::<PersistentIdMapping, LegacyMapping>(bytes) {
            Ok(Some(mapping)) => *bytes = mapping,
            Ok(None) => continue,
            Err(error) => return Err(decode_error(name, &key, error)),
        }
        let value =
            postcard::to_allocvec(&record).map_err(|error| decode_error(name, &key, error))?;
        rows.push((key.to_vec(), value));
    }
    Ok(Rewrites { scanned, rows })
}

/// Compares the index with the rows the mappings need, reading `rewrites` over stored rows.
pub(super) fn index_rebuild(
    db: &OptimisticTxDatabase,
    mappings: &OptimisticTxKeyspace,
    rewrites: &[(Vec<u8>, Vec<u8>)],
    index: &OptimisticTxKeyspace,
    name: &str,
) -> Result<IndexRebuild, ExplorerError> {
    let rewrites: BTreeMap<&[u8], &[u8]> = rewrites
        .iter()
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect();
    let mut wanted = BTreeMap::new();
    for entry in db.read_tx().iter(mappings) {
        let (key, stored) = entry.into_inner()?;
        let value = rewrites.get(key.as_ref()).copied().unwrap_or(&stored);
        let mapping = PersistentIdMapping::from_bytes(value)
            .map_err(|error| decode_error(name, &key, error))?;
        for (_, key, value) in secondary_index_entries(&mapping) {
            wanted.insert(key.to_vec(), value.to_vec());
        }
    }
    let mut removes = Vec::new();
    for entry in db.read_tx().iter(index) {
        let (key, value) = entry.into_inner()?;
        match wanted.get(key.as_ref()) {
            Some(expected) if expected.as_slice() == value.as_ref() => {
                wanted.remove(key.as_ref());
            }
            Some(_) => {}
            None => removes.push(key.to_vec()),
        }
    }
    Ok(IndexRebuild {
        writes: wanted.into_iter().collect(),
        removes,
    })
}

#[cfg(test)]
mod tests {
    use crate::migrate::migrate_output;
    use crate::migrate::tests::{read, write};
    use aruna_core::UserId;
    use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
    use aruna_core::keyspaces::{ID_MAPPING_KEYSPACE, SECONDARY_ID_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
    use aruna_core::structs::LegacyMapping;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::secondary_id::{
        IdentifierOrigin, SecondaryIdKind, SecondaryIdentifier,
    };
    use aruna_core::structs::{PersistentIdMapping, PersistentIdRevision, persistent_id_change};
    use aruna_operations::sync::document_outbox::new_outbox_record;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn mapping(seed: u8) -> PersistentIdMapping {
        let revision = PersistentIdRevision {
            event_id: Ulid::from_bytes([seed; 16]),
            actor: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            occurred_at_ms: 1_000,
        };
        PersistentIdMapping::requested(
            Ulid::from_bytes([seed; 16]),
            false,
            UserId::nil(RealmId([3u8; 32])),
            JobId::from_bytes([4u8; 16]),
            true,
            "/documents/test".to_string(),
            revision,
        )
    }

    fn legacy(mapping: PersistentIdMapping) -> LegacyMapping {
        LegacyMapping {
            pid: mapping.pid,
            target: mapping.target,
            kind: mapping.kind,
            provider: mapping.provider,
            status: mapping.status,
            requested_at_ms: mapping.requested_at_ms,
            requested_by: mapping.requested_by,
            job_id: mapping.job_id,
            public: mapping.public,
            permission_path: mapping.permission_path,
            minted_at_ms: mapping.minted_at_ms,
            minted_by: mapping.minted_by,
            failure: mapping.failure,
            withdrawn_at_ms: mapping.withdrawn_at_ms,
            withdrawn_by: mapping.withdrawn_by,
            withdrawal_reason: mapping.withdrawal_reason,
            revision: mapping.revision,
        }
    }

    fn outbox(mapping: &PersistentIdMapping, bytes: Vec<u8>) -> Vec<u8> {
        let record: DocumentOutboxRecord = new_outbox_record(
            iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            DocumentTarget::PersistentIdMapping {
                document_id: mapping.target,
            },
            Vec::new(),
            DocumentOutboxEvent::Upsert {
                bytes,
                change: persistent_id_change(mapping, PlacementRef::NIL),
            },
            PlacementRef::NIL,
            false,
        );
        postcard::to_allocvec(&record).unwrap()
    }

    #[test]
    fn rewrites_legacy_mappings() {
        // Old mappings gain an empty identifier set, queued publishes are re-encoded and
        // the index keeps only rows the current mappings name.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let old = mapping(1);
        let legacy_bytes = postcard::to_allocvec(&legacy(old.clone())).unwrap();
        // Postcard is positional: the old row is the new one without the empty set.
        let current_old = old.to_bytes().unwrap();
        assert_eq!(legacy_bytes, current_old[..current_old.len() - 1]);
        let mut new = mapping(2);
        let doi = SecondaryIdentifier::new(
            SecondaryIdKind::Doi,
            "10.1/new",
            None,
            IdentifierOrigin::Published,
        )
        .unwrap();
        new.secondary_identifiers.insert(doi.clone());
        let new_bytes = new.to_bytes().unwrap();
        write(
            &path,
            ID_MAPPING_KEYSPACE,
            vec![(b"old", legacy_bytes.clone()), (b"new", new_bytes.clone())],
        );
        write(
            &path,
            SYNC_OUTBOX_KEYSPACE,
            vec![
                (b"queued-old", outbox(&old, legacy_bytes)),
                (b"queued-new", outbox(&new, new_bytes.clone())),
            ],
        );
        write(
            &path,
            SECONDARY_ID_KEYSPACE,
            vec![(b"doi\x0010.1/stale\x00", old.target.to_bytes().to_vec())],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.mappings_scanned, output.mappings_rewritten), (2, 1));
        assert_eq!((output.outbox_scanned, output.outbox_rewritten), (2, 1));
        assert_eq!(
            (
                output.identifier_index_written,
                output.identifier_index_removed
            ),
            (1, 1)
        );
        let rows = read(&path, ID_MAPPING_KEYSPACE);
        assert_eq!(rows[b"new".as_slice()], new_bytes);
        assert_eq!(rows[b"old".as_slice()], current_old);
        let queued = read(&path, SYNC_OUTBOX_KEYSPACE);
        let record: DocumentOutboxRecord =
            postcard::from_bytes(&queued[b"queued-old".as_slice()]).unwrap();
        let DocumentOutboxEvent::Upsert { bytes, .. } = record.event else {
            panic!("the queued row is an upsert");
        };
        assert_eq!(bytes, current_old);
        let index = read(&path, SECONDARY_ID_KEYSPACE);
        assert_eq!(index.len(), 1);
        assert_eq!(
            index[doi.index_key(new.target).as_slice()],
            new.target.to_bytes()
        );

        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(
            (
                again.mappings_rewritten,
                again.outbox_rewritten,
                again.identifier_index_written,
                again.identifier_index_removed
            ),
            (0, 0, 0, 0)
        );
    }
}
