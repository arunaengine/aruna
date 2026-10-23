//! Checks one bounded page of a connector's reference rows against their versions, shared by
//! delete and replace. Stale rows of the page are pruned in the same transaction; when more rows
//! follow, the rest is queued for background pruning and the caller is asked to retry.
//! Each calling operation still owns that transaction and its final mutation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE};
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::{Effects, Key, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::connectors::reference_prune::prune_queue_write;
use crate::connectors::repository::{REFERENCE_PAGE_SIZE, StorageReadError, references_connector};

/// Connector id bytes ahead of the version key in a reference row.
const ROW_PREFIX: usize = 16;

/// What one scan event asks its operation to do next.
pub(crate) enum ScanStep {
    Referenced,
    Next(Effects),
    Complete,
    /// The page was stale but more rows follow: commit the pruning queued here, then retry.
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPhase {
    Rows,
    Versions,
    Prune,
    Queue,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ReferenceScan {
    connector_id: Ulid,
    phase: ScanPhase,
    rows: Vec<Key>,
    next: Option<Key>,
}

impl ReferenceScan {
    pub(crate) fn new(connector_id: Ulid) -> Self {
        Self {
            connector_id,
            phase: ScanPhase::Rows,
            rows: Vec::new(),
            next: None,
        }
    }

    /// Reads the first page of the connector's reference rows.
    pub(crate) fn start(&mut self, txn_id: TxnId) -> Effects {
        self.phase = ScanPhase::Rows;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(self.connector_id.to_bytes().to_vec())),
            start: None,
            limit: REFERENCE_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    pub(crate) fn step(
        &mut self,
        event: Event,
        txn_id: TxnId,
    ) -> Result<ScanStep, StorageReadError> {
        match (self.phase, event) {
            (
                ScanPhase::Rows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                if values.is_empty() {
                    return Ok(ScanStep::Complete);
                }
                self.rows = values.into_iter().map(|(key, _)| key).collect();
                self.next = next_start_after;
                self.phase = ScanPhase::Versions;
                let reads = self
                    .rows
                    .iter()
                    .map(|row| {
                        let version_key = row.get(ROW_PREFIX..).unwrap_or_default();
                        (
                            BLOB_VERSIONS_KEYSPACE.to_string(),
                            ByteView::from(version_key.to_vec()),
                        )
                    })
                    .collect();
                Ok(ScanStep::Next(smallvec![Effect::Storage(
                    StorageEffect::BatchRead {
                        reads,
                        txn_id: Some(txn_id),
                    }
                )]))
            }
            (ScanPhase::Versions, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let mut stale = Vec::new();
                for (row, (_, value)) in self.rows.iter().zip(values) {
                    let version = value
                        .map(|value| BlobVersion::from_bytes(value.as_ref()))
                        .transpose()
                        .map_err(StorageReadError::Conversion)?;
                    match version {
                        Some(version) if references_connector(&version, self.connector_id) => {
                            return Ok(ScanStep::Referenced);
                        }
                        _ => stale.push((CONNECTOR_REFERENCE_KEYSPACE.to_string(), row.clone())),
                    }
                }
                self.phase = ScanPhase::Prune;
                Ok(ScanStep::Next(smallvec![Effect::Storage(
                    StorageEffect::BatchDelete {
                        deletes: stale,
                        txn_id: Some(txn_id),
                    }
                )]))
            }
            (ScanPhase::Prune, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                let Some(cursor) = self.next.take() else {
                    return Ok(ScanStep::Complete);
                };
                // More rows follow than one request checks: the rest is pruned in the background.
                self.phase = ScanPhase::Queue;
                let (key_space, key, value) = prune_queue_write(self.connector_id, cursor);
                Ok(ScanStep::Next(smallvec![Effect::Storage(
                    StorageEffect::Write {
                        key_space,
                        key,
                        value,
                        txn_id: Some(txn_id),
                    }
                )]))
            }
            (ScanPhase::Queue, Event::Storage(StorageEvent::WriteResult { .. })) => {
                Ok(ScanStep::Deferred)
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                Err(StorageReadError::Storage(error))
            }
            (phase, event) => Err(StorageReadError::Storage(StorageError::ReadError(format!(
                "unexpected reference scan event in {phase:?}: {event:?}"
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::connector_reference_key;
    use aruna_core::keyspaces::CONNECTOR_PRUNE_KEYSPACE;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::VersionKey;
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn txn() -> TxnId {
        Ulid::from_bytes([9; 16])
    }

    fn row(connector_id: Ulid, key: &str) -> Key {
        connector_reference_key(connector_id, &VersionKey::new("bucket", key, Ulid::nil())).unwrap()
    }

    fn reference(connector_id: Ulid) -> Option<ByteView> {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::new(),
                source_path: "a".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(connector_id),
        };
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let now = SystemTime::UNIX_EPOCH;
        let version = BlobVersion::reference(binding, metadata, now, Default::default(), now);
        Some(ByteView::from(version.to_bytes().unwrap()))
    }

    fn rows(keys: Vec<Key>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: keys
                .into_iter()
                .map(|key| (key, ByteView::from(Vec::new())))
                .collect(),
            next_start_after: next,
        })
    }

    #[test]
    fn live_row_refers() {
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut scan = ReferenceScan::new(connector_id);
        assert!(matches!(
            scan.start(txn()).as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == CONNECTOR_REFERENCE_KEYSPACE
        ));
        let Ok(ScanStep::Next(reads)) = scan.step(rows(vec![row(connector_id, "a")], None), txn())
        else {
            panic!("expected the version reads");
        };
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = reads.as_slice() else {
            panic!("expected one batch read");
        };
        let expected = VersionKey::new("bucket", "a", Ulid::nil())
            .to_bytes()
            .unwrap();
        assert_eq!(reads[0].1.as_ref(), expected.as_slice());

        let versions = Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(reads[0].1.clone(), reference(connector_id))],
        });
        assert!(matches!(
            scan.step(versions, txn()),
            Ok(ScanStep::Referenced)
        ));
    }

    #[test]
    fn stale_rows_pruned() {
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut scan = ReferenceScan::new(connector_id);
        scan.start(txn());
        let page = vec![row(connector_id, "gone"), row(connector_id, "rebound")];
        let next = page[1].clone();
        scan.step(rows(page, Some(next.clone())), txn()).ok();
        let versions = Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(b"gone".to_vec()), None),
                (
                    Key::from(b"rebound".to_vec()),
                    reference(Ulid::from_bytes([3; 16])),
                ),
            ],
        });

        let Ok(ScanStep::Next(prune)) = scan.step(versions, txn()) else {
            panic!("expected the stale rows to be pruned");
        };
        assert!(matches!(
            prune.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] if deletes.len() == 2
        ));
        let deleted = Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        });
        // A request checks one page; the rest is queued for the background and retried.
        let Ok(ScanStep::Next(queued)) = scan.step(deleted, txn()) else {
            panic!("expected the pruning to be queued");
        };
        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = queued.as_slice()
        else {
            panic!("expected one queue write");
        };
        assert_eq!(key_space, CONNECTOR_PRUNE_KEYSPACE);
        assert_eq!(value.as_ref(), next.as_ref());
        let written = Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        });
        assert!(matches!(scan.step(written, txn()), Ok(ScanStep::Deferred)));
    }
}
