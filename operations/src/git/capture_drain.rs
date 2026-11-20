//! Snapshots the graph each queued capture mark holds, per document in capture sequence order,
//! and deletes the mark only after the snapshot, so a crash never loses one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageReadError;
use aruna_core::git::{CaptureMark, GitNode};
use aruna_core::keyspaces::MATERIALIZATION_CAPTURE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::Key;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::GitError;
use super::snapshot::capture;
use crate::metadata::record_reads::DocumentRecordOperation;

/// When the drain runs again after a snapshot that failed; its mark stays for that run.
pub const CAPTURE_RETRY_AFTER: Duration = Duration::from_secs(30);

type Turn = Result<Next<Option<Duration>>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum CaptureStep {
    Page,
    Record,
    Capture,
    Delete,
}

/// One drain run over the queued capture marks in sequence order, so Git gets a document's graphs
/// in finish order; a failed snapshot stops its document. It reads one mark at a time, so a
/// backlog loads one graph at most, and answers when to run again after a failed snapshot.
#[derive(Debug, PartialEq)]
pub struct CaptureDrainOperation {
    node: GitNode,
    /// The last key the drain read; the next mark comes after it.
    after: Option<Key>,
    /// The document of the last mark and its record, `None` once it was deleted.
    known: Option<(Ulid, Option<MetadataRegistryRecord>)>,
    /// The mark being snapshotted.
    current: Option<(Key, CaptureMark)>,
    failed: bool,
    step: CaptureStep,
}

/// The document of a queued capture mark, keyed by the document and its sequence.
fn mark_document(key: &[u8]) -> Option<Ulid> {
    let document = <[u8; 16]>::try_from(key.get(..16)?).ok()?;
    (key.len() == 24).then(|| Ulid::from_bytes(document))
}

/// Whether a row is a job's hold, keyed by its document and event, which waits for its finish.
fn is_hold(key: &[u8]) -> bool {
    key.len() == 32
}

/// A key after every row of `document`, so the drain leaves the rest of it for the next run.
fn past_document(document: Ulid) -> Key {
    let mut key = document.to_bytes().to_vec();
    key.extend_from_slice(&[0xff; 16]);
    ByteView::from(key)
}

impl CaptureDrainOperation {
    pub fn new(node: GitNode) -> Self {
        Self {
            node,
            after: None,
            known: None,
            current: None,
            failed: false,
            step: CaptureStep::Page,
        }
    }

    fn page(&mut self) -> Turn {
        self.step = CaptureStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            prefix: ByteView::from(Vec::new()),
            after: self.after.clone(),
            limit: 1,
        }))
    }

    fn take(&mut self, result: IoResult) -> Turn {
        let (rows, _) = result.page()?;
        let Some((key, value)) = rows.into_iter().next() else {
            return Ok(Next::Done(self.failed.then_some(CAPTURE_RETRY_AFTER)));
        };
        self.after = Some(key.clone());
        if is_hold(&key) {
            return self.page();
        }
        let Some(document_id) = mark_document(&key) else {
            return self.delete(key);
        };
        let mark = match CaptureMark::from_bytes(&value) {
            Ok(mark) => mark,
            Err(error) => {
                warn!(%document_id, %error, "Deleting an unreadable Git capture mark");
                return self.delete(key);
            }
        };
        self.current = Some((key, mark));
        let known = (self.known.as_ref()).is_some_and(|(known, _)| *known == document_id);
        if known {
            return self.snapshot();
        }
        self.known = None;
        self.step = CaptureStep::Record;
        let read = DocumentRecordOperation::new(document_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Snapshots the current mark's graph; a deleted document has no snapshot to take.
    fn snapshot(&mut self) -> Turn {
        let (key, mark) = self.current.as_ref().ok_or(IoError::Unfinished)?;
        let Some((_, Some(record))) = &self.known else {
            let key = key.clone();
            return self.delete(key);
        };
        let graph = Some((mark.event_id, mark.jsonld.clone()));
        let snapshot = capture(&self.node, record, graph);
        self.step = CaptureStep::Capture;
        Ok(Next::Call(IoCall::child(snapshot)))
    }

    /// A failed step keeps the mark and leaves the rest of its document until the next run.
    fn kept(&mut self, error: impl std::fmt::Display) -> Turn {
        let (key, _) = self.current.take().ok_or(IoError::Unfinished)?;
        let document_id = mark_document(&key).unwrap_or_else(Ulid::nil);
        warn!(%document_id, %error, "The Git snapshot of a materialized document waits");
        self.failed = true;
        self.known = None;
        self.after = Some(past_document(document_id));
        self.page()
    }

    fn delete(&mut self, key: Key) -> Turn {
        self.current = None;
        self.step = CaptureStep::Delete;
        let row = (MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for CaptureDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, CaptureStep::Page) {
            CaptureStep::Page => self.take(result),
            CaptureStep::Record => {
                match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
                    Ok(record) => {
                        let document_id = (self.current.as_ref())
                            .and_then(|(key, _)| mark_document(key))
                            .ok_or(IoError::Unfinished)?;
                        self.known = Some((document_id, record));
                        self.snapshot()
                    }
                    Err(error) => self.kept(format!("{error:?}")),
                }
            }
            CaptureStep::Capture => match result.child::<(), GitError>()? {
                Ok(()) => {
                    let (key, _) = self.current.take().ok_or(IoError::Unfinished)?;
                    self.delete(key)
                }
                Err(error) => self.kept(error),
            },
            CaptureStep::Delete => {
                result.done()?;
                self.page()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::storage_entries::{capture_key, document_job_key};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::Value;

    const DOCUMENT: Ulid = Ulid::from_parts(1, 1);
    const EVENT: Ulid = Ulid::from_parts(2, 2);

    fn queued(sequence: u64, jsonld: &str) -> (Key, Value) {
        let mark = CaptureMark {
            event_id: EVENT,
            jsonld: jsonld.to_string(),
        };
        let value = Value::from(mark.to_bytes().unwrap());
        (capture_key(DOCUMENT, sequence), value)
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn record() -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(3, 3);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id: DOCUMENT,
            document_path: "datasets/a".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(DOCUMENT),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/a",
                DOCUMENT,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: EVENT,
            last_event_id: EVENT,
        }
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn found() -> IoResult {
        child(Ok::<_, StorageReadError>(Some(record())))
    }

    fn snapshot_of(jsonld: &str) -> Turn {
        let graph = Some((EVENT, jsonld.to_string()));
        let snapshot = capture(&GitNode::default(), &record(), graph);
        Ok(Next::Call(IoCall::child(snapshot)))
    }

    fn deleted(sequence: u64) -> Turn {
        let row = (
            MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            capture_key(DOCUMENT, sequence),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn paged(after: Option<Key>) -> Turn {
        Ok(Next::Call(IoCall::Page {
            space: MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            prefix: ByteView::from(Vec::new()),
            after,
            limit: 1,
        }))
    }

    fn one(row: (Key, Value)) -> IoResult {
        page(vec![row])
    }

    fn done() -> IoResult {
        child(Ok::<(), GitError>(()))
    }

    /// Runs a drain up to the snapshot of one queued mark. The drain asks for no status: a newer
    /// event's failure never decides about this mark.
    fn snapshotting() -> CaptureDrainOperation {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        assert_eq!(drain.first(), paged(None));
        let record_read = IoCall::child(DocumentRecordOperation::new(DOCUMENT));
        let next = drain.next(one(queued(0, "first")));
        assert_eq!(next, Ok(Next::Call(record_read)));
        assert_eq!(drain.next(found()), snapshot_of("first"));
        drain
    }

    // A queued mark gets the snapshot of the graph it holds, and goes only after it.
    #[test]
    fn snapshot_then_delete() {
        let mut drain = snapshotting();
        assert_eq!(drain.next(done()), deleted(0));
        let after = Some(capture_key(DOCUMENT, 0));
        assert_eq!(drain.next(IoResult::Done), paged(after));
        assert_eq!(drain.next(page(Vec::new())), Ok(Next::Done(None)));
    }

    // The drain reads one mark per page and loads the next only after the last one went, so a
    // backlog never holds more than one graph; the document's record is read once.
    #[test]
    fn streams_one_mark() {
        let mut drain = snapshotting();
        assert_eq!(drain.next(done()), deleted(0));
        drain.next(IoResult::Done).unwrap();
        assert_eq!(drain.next(one(queued(1, "second"))), snapshot_of("second"));
    }

    // A failed snapshot keeps its mark, so the next run, as after a crash, takes it.
    #[test]
    fn failed_snapshot_kept() {
        let mut drain = snapshotting();
        let failed = child(Err::<(), _>(GitError::Unavailable));
        drain.next(failed).unwrap();
        let ended = drain.next(page(Vec::new()));
        assert_eq!(ended, Ok(Next::Done(Some(CAPTURE_RETRY_AFTER))));
        let mut rerun = snapshotting();
        assert_eq!(rerun.next(done()), deleted(0));
    }

    // A job's hold waits for its finish without a snapshot, and stops no queued mark.
    #[test]
    fn hold_passed_over() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let hold = (document_job_key(DOCUMENT, EVENT), Value::from(Vec::new()));
        let after = Some(hold.0.clone());
        assert_eq!(drain.next(one(hold)), paged(after));
        drain.next(one(queued(0, "first"))).unwrap();
        assert_eq!(drain.next(found()), snapshot_of("first"));
    }

    // Graphs of one winning event go in their capture sequence, the order their finishes ran.
    #[test]
    fn sequence_orders_marks() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        drain.next(one(queued(1, "first"))).unwrap();
        assert_eq!(drain.next(found()), snapshot_of("first"));
        assert_eq!(drain.next(done()), deleted(1));
        drain.next(IoResult::Done).unwrap();
        assert_eq!(drain.next(one(queued(2, "second"))), snapshot_of("second"));
        assert_eq!(drain.next(done()), deleted(2));
        drain.next(IoResult::Done).unwrap();
        assert_eq!(drain.next(page(Vec::new())), Ok(Next::Done(None)));
    }

    // A failed snapshot stops its document: the drain reads on past all of its marks, so the
    // next run retries it before any later graph of that document.
    #[test]
    fn failed_snapshot_stops() {
        let mut drain = snapshotting();
        let failed = child(Err::<(), _>(GitError::Unavailable));
        assert_eq!(drain.next(failed), paged(Some(past_document(DOCUMENT))));
    }

    // A mark that does not decode goes without a snapshot.
    #[test]
    fn unreadable_mark_deleted() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let row = (capture_key(DOCUMENT, 0), Value::from(vec![0xff]));
        assert_eq!(drain.next(one(row)), deleted(0));
    }
}
