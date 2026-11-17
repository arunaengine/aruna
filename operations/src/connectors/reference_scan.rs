//! Checks one bounded page of a connector's reference rows against their versions, shared by
//! delete and replace. Stale rows of the page are pruned in the same transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// When more rows follow, the rest is queued for background pruning and the caller retries.
// Each calling operation still owns that transaction and its final mutation.

use aruna_core::calls::{IoCall, IoResult};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE};
use aruna_core::rows;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::Key;
use byteview::ByteView;
use ulid::Ulid;

use crate::connectors::reference_prune::prune_queue_write;
use crate::connectors::repository::{REFERENCE_PAGE_SIZE, StorageReadError, references_connector};

/// Connector id bytes ahead of the version key in a reference row.
const ROW_PREFIX: usize = 16;

/// What one scan result asks its operation to do next.
#[derive(Debug, PartialEq)]
pub(crate) enum ScanStep {
    Referenced,
    Next(IoCall),
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
    pub(crate) fn start(&mut self) -> IoCall {
        self.phase = ScanPhase::Rows;
        IoCall::Page {
            space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: ByteView::from(self.connector_id.to_bytes().to_vec()),
            after: None,
            limit: REFERENCE_PAGE_SIZE,
        }
    }

    pub(crate) fn step(&mut self, result: IoResult) -> Result<ScanStep, StorageReadError> {
        match self.phase {
            ScanPhase::Rows => {
                let (values, next) = rows::page(result)?;
                if values.is_empty() {
                    return Ok(ScanStep::Complete);
                }
                self.rows = values.into_iter().map(|(key, _)| key).collect();
                self.next = next;
                self.phase = ScanPhase::Versions;
                let reads = (self.rows.iter())
                    .map(|row| {
                        let version_key = row.get(ROW_PREFIX..).unwrap_or_default();
                        (
                            BLOB_VERSIONS_KEYSPACE.to_string(),
                            ByteView::from(version_key.to_vec()),
                        )
                    })
                    .collect();
                Ok(ScanStep::Next(IoCall::Read(reads)))
            }
            ScanPhase::Versions => {
                let mut stale = Vec::new();
                for (row, value) in self.rows.iter().zip(rows::all_values(result)?) {
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
                Ok(ScanStep::Next(IoCall::Delete(stale)))
            }
            ScanPhase::Prune => {
                rows::done(result)?;
                let Some(cursor) = self.next.take() else {
                    return Ok(ScanStep::Complete);
                };
                // More rows follow than one request checks: the rest is pruned in the background.
                self.phase = ScanPhase::Queue;
                let write = prune_queue_write(self.connector_id, cursor);
                Ok(ScanStep::Next(IoCall::Write(vec![write])))
            }
            ScanPhase::Queue => {
                rows::done(result)?;
                Ok(ScanStep::Deferred)
            }
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
                namespace: None,
                root: None,
            },
            connector_id: Some(connector_id),
            mirrors: Vec::new(),
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

    fn rows(keys: Vec<Key>, next: Option<Key>) -> IoResult {
        IoResult::Page {
            rows: (keys.into_iter())
                .map(|key| (key, ByteView::from(Vec::new())))
                .collect(),
            next,
        }
    }

    #[test]
    fn live_row_refers() {
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut scan = ReferenceScan::new(connector_id);
        assert!(matches!(
            scan.start(),
            IoCall::Page { space, .. } if space == CONNECTOR_REFERENCE_KEYSPACE
        ));
        let Ok(ScanStep::Next(IoCall::Read(reads))) =
            scan.step(rows(vec![row(connector_id, "a")], None))
        else {
            panic!("expected the version reads");
        };
        let expected = VersionKey::new("bucket", "a", Ulid::nil())
            .to_bytes()
            .unwrap();
        assert_eq!(reads[0].1.as_ref(), expected.as_slice());

        let versions = IoResult::Values(vec![reference(connector_id)]);
        assert_eq!(scan.step(versions), Ok(ScanStep::Referenced));
    }

    #[test]
    fn stale_rows_pruned() {
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut scan = ReferenceScan::new(connector_id);
        scan.start();
        let page = vec![row(connector_id, "gone"), row(connector_id, "rebound")];
        let next = page[1].clone();
        scan.step(rows(page, Some(next.clone()))).unwrap();
        let versions = IoResult::Values(vec![None, reference(Ulid::from_bytes([3; 16]))]);

        let Ok(ScanStep::Next(IoCall::Delete(deletes))) = scan.step(versions) else {
            panic!("expected the stale rows to be pruned");
        };
        assert_eq!(deletes.len(), 2);
        // A request checks one page; the rest is queued for the background and retried.
        let Ok(ScanStep::Next(IoCall::Write(queued))) = scan.step(IoResult::Done) else {
            panic!("expected the pruning to be queued");
        };
        let [(key_space, _, value)] = queued.as_slice() else {
            panic!("expected one queue write");
        };
        assert_eq!(key_space, CONNECTOR_PRUNE_KEYSPACE);
        assert_eq!(value.as_ref(), next.as_ref());
        assert_eq!(scan.step(IoResult::Done), Ok(ScanStep::Deferred));
    }
}
