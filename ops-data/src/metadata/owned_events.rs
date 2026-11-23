//! Finds the create events that queued applied batches still own, so a projection recovery
//! leaves them to the applied drain, which projects a document's batches in acceptance order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{AppliedSync, order_counter_key};
use aruna_core::keyspaces::{APPLIED_ORDER_KEYSPACE, APPLIED_SYNC_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::types::Key;
use ulid::Ulid;

/// Order entries one page reads at most.
const OWNED_PAGE: usize = 64;

/// The document and event of each create event an outstanding batch owns.
pub type Owned = BTreeSet<(Ulid, Ulid)>;

type Turn = Result<Next<Owned>, IoError>;

#[derive(Debug, PartialEq)]
enum OwnedStage {
    Entries,
    Batches,
}

/// Reads the queued batches of each document from its order entries, then the create events
/// those batches carry. A batch whose row is gone owns nothing.
#[derive(Debug, PartialEq)]
pub struct OwnedEventsOperation {
    documents: VecDeque<Ulid>,
    after: Option<Key>,
    batches: BTreeSet<Ulid>,
    stage: OwnedStage,
}

impl OwnedEventsOperation {
    pub fn new(documents: impl IntoIterator<Item = Ulid>) -> Self {
        let documents: BTreeSet<_> = documents.into_iter().collect();
        Self {
            documents: documents.into_iter().collect(),
            after: None,
            batches: BTreeSet::new(),
            stage: OwnedStage::Entries,
        }
    }

    fn next_document(&mut self) -> Turn {
        let Some(document_id) = self.documents.front().copied() else {
            return self.read_batches();
        };
        let counter = order_counter_key(document_id);
        let after = self.after.clone().unwrap_or_else(|| counter.clone());
        self.stage = OwnedStage::Entries;
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_ORDER_KEYSPACE.to_string(),
            prefix: counter,
            after: Some(after),
            limit: OWNED_PAGE,
        }))
    }

    fn read_batches(&mut self) -> Turn {
        if self.batches.is_empty() {
            return Ok(Next::Done(Owned::new()));
        }
        self.stage = OwnedStage::Batches;
        let rows = (self.batches.iter()).map(|id| {
            (
                APPLIED_SYNC_KEYSPACE.to_string(),
                Key::from(id.to_bytes().to_vec()),
            )
        });
        Ok(Next::Call(IoCall::Read(rows.collect())))
    }
}

impl Operation for OwnedEventsOperation {
    type Output = Owned;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.next_document()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.stage {
            OwnedStage::Entries => {
                let (rows, next) = result.page()?;
                for (_, batch) in rows {
                    if let Ok(id) = <[u8; 16]>::try_from(batch.as_ref()) {
                        self.batches.insert(Ulid::from_bytes(id));
                    }
                }
                self.after = next;
                if self.after.is_none() {
                    self.documents.pop_front();
                }
                self.next_document()
            }
            OwnedStage::Batches => {
                let mut owned = Owned::new();
                for row in result.values()?.into_iter().flatten() {
                    let Ok(batch) = AppliedSync::from_bytes(row.as_ref()) else {
                        continue;
                    };
                    let events = batch.result.metadata_create_events.iter();
                    owned.extend(events.map(|event| (event.record.document_id, event.event_id)));
                }
                Ok(Next::Done(owned))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        AppliedStep, DocumentReconcileResult, applied_sync_entry, order_entry_key,
    };
    use aruna_core::types::Value;

    fn batch(id: Ulid, event: Ulid) -> AppliedSync {
        let created = crate::metadata::raw_revision::tests::test_event(event, 1);
        AppliedSync {
            id,
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            realm_id: None,
            result: DocumentReconcileResult {
                metadata_create_events: vec![created],
                ..DocumentReconcileResult::default()
            },
            next: AppliedStep::Metadata,
        }
    }

    // The events of a document's queued batches are owned; a batch whose row is gone owns none.
    #[test]
    fn queued_batches_own() {
        let queued = batch(Ulid::from_parts(5, 1), Ulid::from_parts(10, 1));
        let document_id = queued.result.metadata_create_events[0].record.document_id;
        let mut owned = OwnedEventsOperation::new([document_id, document_id]);
        let counter = order_counter_key(document_id);
        let page = IoCall::Page {
            space: APPLIED_ORDER_KEYSPACE.to_string(),
            prefix: counter.clone(),
            after: Some(counter),
            limit: OWNED_PAGE,
        };
        assert_eq!(owned.first(), Ok(Next::Call(page)));
        let gone = Ulid::from_parts(6, 1);
        let rows = [(0, queued.id), (1, gone)].map(|(sequence, id)| {
            let entry = order_entry_key(document_id, sequence);
            (entry, Value::from(id.to_bytes().to_vec()))
        });
        let entries = IoResult::Page {
            rows: rows.to_vec(),
            next: None,
        };
        let reads = owned.next(entries);
        assert!(matches!(reads, Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 2));
        let (_, _, stored) = applied_sync_entry(&queued).unwrap();
        let read = IoResult::Values(vec![Some(stored), None]);
        let expected = Owned::from([(document_id, Ulid::from_parts(10, 1))]);
        assert_eq!(owned.next(read), Ok(Next::Done(expected)));
    }
}
