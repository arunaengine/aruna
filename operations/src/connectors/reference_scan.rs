//! Pages through a connector's reference rows and confirms each against its version, shared by
//! delete and replace. Rows whose version is gone or bound elsewhere are pruned in the same
//! transaction; each calling operation still owns that transaction and its final mutation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE};
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::{Effects, Key, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::connectors::repository::{REFERENCE_PAGE_SIZE, StorageReadError, references_connector};

/// Connector id bytes ahead of the version key in a reference row.
const ROW_PREFIX: usize = 16;

/// What one scan event asks its operation to do next.
pub(crate) enum ScanStep {
    Referenced,
    Next(Effects),
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanPhase {
    Rows,
    Versions,
    Prune,
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

    /// Reads the next page of the connector's reference rows.
    pub(crate) fn start(&mut self, start_after: Option<Key>, txn_id: TxnId) -> Effects {
        self.phase = ScanPhase::Rows;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(self.connector_id.to_bytes().to_vec())),
            start: start_after.map(IterStart::After),
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
                Ok(match self.next.take() {
                    Some(start_after) => ScanStep::Next(self.start(Some(start_after), txn_id)),
                    None => ScanStep::Complete,
                })
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
