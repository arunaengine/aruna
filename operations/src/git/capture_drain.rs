//! Snapshots the graph each ready capture mark holds, per document in materialization order,
//! and deletes the mark only after the snapshot, so a crash never loses one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageReadError;
use aruna_core::git::{CaptureMark, GitNode};
use aruna_core::keyspaces::MATERIALIZATION_CAPTURE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::GitError;
use super::snapshot::capture;
use crate::metadata::record_reads::DocumentRecordOperation;

/// Marks one page of the drain reads at most.
const CAPTURE_PAGE: usize = 64;
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

/// One drain run over every capture mark, per document in materialization order, so Git never
/// gets an older graph after a newer one: the first mark not ready, or whose snapshot failed,
/// stops its document. Answers when to run again after a failed snapshot.
#[derive(Debug, PartialEq)]
pub struct CaptureDrainOperation {
    node: GitNode,
    pages: PrefixPages,
    /// Rows of the last page not gathered yet.
    marks: VecDeque<(Key, Value)>,
    /// The document whose marks are gathered or taken.
    document: Option<Ulid>,
    gathered: Vec<(Key, CaptureMark)>,
    /// The document's ready marks before its first open one, in materialization order.
    taking: VecDeque<(Key, CaptureMark)>,
    record: Option<MetadataRegistryRecord>,
    current: Option<Key>,
    failed: bool,
    step: CaptureStep,
}

/// The document of a capture mark key, which ends with its job's event.
fn mark_document(key: &[u8]) -> Option<Ulid> {
    let document = <[u8; 16]>::try_from(key.get(..16)?).ok()?;
    (key.len() == 32).then(|| Ulid::from_bytes(document))
}

impl CaptureDrainOperation {
    pub fn new(node: GitNode) -> Self {
        Self {
            node,
            pages: PrefixPages::new(
                MATERIALIZATION_CAPTURE_KEYSPACE,
                ByteView::from(Vec::new()),
                CAPTURE_PAGE,
            ),
            marks: VecDeque::new(),
            document: None,
            gathered: Vec::new(),
            taking: VecDeque::new(),
            record: None,
            current: None,
            failed: false,
            step: CaptureStep::Page,
        }
    }

    fn next_mark(&mut self) -> Turn {
        loop {
            if let Some((key, mark)) = self.taking.pop_front() {
                let record = self.record.as_ref().ok_or(IoError::Unfinished)?;
                let snapshot = capture(&self.node, record, Some((mark.event_id, mark.jsonld)));
                self.current = Some(key);
                self.step = CaptureStep::Capture;
                return Ok(Next::Call(IoCall::child(snapshot)));
            }
            if let Some(turn) = self.gather() {
                return turn;
            }
            if self.gathered.is_empty() {
                return Ok(Next::Done(self.failed.then_some(CAPTURE_RETRY_AFTER)));
            }
            let mut gathered = std::mem::take(&mut self.gathered);
            gathered.sort_by_key(|(_, mark)| (mark.materialized_at_ms, mark.event_id));
            let open = gathered.iter().position(|(_, mark)| !mark.ready);
            gathered.truncate(open.unwrap_or(gathered.len()));
            if gathered.is_empty() {
                continue;
            }
            self.taking = gathered.into();
            let document_id = self.document.ok_or(IoError::Unfinished)?;
            self.step = CaptureStep::Record;
            return Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(document_id))));
        }
    }

    /// Gathers the marks of one document, reading on across pages; answers a call while it
    /// needs one, and nothing once the document's marks are all gathered.
    fn gather(&mut self) -> Option<Turn> {
        while let Some((key, value)) = self.marks.pop_front() {
            let Some(document_id) = mark_document(&key) else {
                return Some(self.delete(vec![key]));
            };
            if self.document != Some(document_id) && !self.gathered.is_empty() {
                self.marks.push_front((key, value));
                return None;
            }
            self.document = Some(document_id);
            match CaptureMark::from_bytes(&value) {
                Ok(mark) => self.gathered.push((key, mark)),
                Err(error) => {
                    warn!(%document_id, %error, "Deleting an unreadable Git capture mark");
                    return Some(self.delete(vec![key]));
                }
            }
        }
        if self.pages.done() {
            return None;
        }
        self.step = CaptureStep::Page;
        Some(Ok(Next::Call(self.pages.call())))
    }

    /// A failed step keeps the mark and stops its document until the next run.
    fn kept(&mut self, error: impl std::fmt::Display) -> Turn {
        let document_id = self.document.unwrap_or_else(Ulid::nil);
        warn!(%document_id, %error, "The Git snapshot of a materialized document waits");
        self.failed = true;
        self.current = None;
        self.taking.clear();
        self.next_mark()
    }

    fn delete(&mut self, keys: Vec<Key>) -> Turn {
        self.step = CaptureStep::Delete;
        let rows = keys
            .into_iter()
            .map(|key| (MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), key));
        Ok(Next::Call(IoCall::Delete(rows.collect())))
    }
}

impl Operation for CaptureDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, CaptureStep::Page) {
            CaptureStep::Page => {
                self.marks.extend(self.pages.take(result)?);
                self.next_mark()
            }
            CaptureStep::Record => {
                match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
                    Ok(Some(record)) => {
                        self.record = Some(record);
                        self.next_mark()
                    }
                    // A deleted document has no snapshot to take.
                    Ok(None) => {
                        let keys = self.taking.drain(..).map(|(key, _)| key).collect();
                        self.delete(keys)
                    }
                    Err(error) => self.kept(format!("{error:?}")),
                }
            }
            CaptureStep::Capture => match result.child::<(), GitError>()? {
                Ok(()) => {
                    let key = self.current.take().ok_or(IoError::Unfinished)?;
                    self.delete(vec![key])
                }
                Err(error) => self.kept(error),
            },
            CaptureStep::Delete => {
                result.done()?;
                self.next_mark()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::storage_entries::document_job_key;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    const DOCUMENT: Ulid = Ulid::from_parts(1, 1);
    const EVENT: Ulid = Ulid::from_parts(2, 2);

    fn mark(jsonld: &str, ready: bool) -> CaptureMark {
        CaptureMark {
            materialized_at_ms: 10,
            event_id: EVENT,
            jsonld: jsonld.to_string(),
            ready,
        }
    }

    fn mark_row(job_event: Ulid, mark: &CaptureMark) -> (Key, Value) {
        let value = Value::from(mark.to_bytes().unwrap());
        (document_job_key(DOCUMENT, job_event), value)
    }

    fn marks(mark: &CaptureMark) -> IoResult {
        let rows = vec![mark_row(EVENT, mark)];
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

    fn delete_mark() -> Turn {
        let row = (
            MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            document_job_key(DOCUMENT, EVENT),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    /// Runs a drain up to the snapshot of one ready mark. The drain asks for no status: a newer
    /// event's failure never decides about this mark.
    fn snapshotting() -> CaptureDrainOperation {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let record_read = IoCall::child(DocumentRecordOperation::new(DOCUMENT));
        assert_eq!(drain.next(marks(&mark("first", true))), Ok(Next::Call(record_read)));
        let found = child(Ok::<_, StorageReadError>(Some(record())));
        let graph = Some((EVENT, "first".to_string()));
        let snapshot = IoCall::child(capture(&GitNode::default(), &record(), graph));
        assert_eq!(drain.next(found), Ok(Next::Call(snapshot)));
        drain
    }

    // A ready mark gets the snapshot of the graph it holds, and goes only after it.
    #[test]
    fn snapshot_then_delete() {
        let mut drain = snapshotting();
        assert_eq!(drain.next(child(Ok::<(), GitError>(()))), delete_mark());
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // A failed snapshot keeps its mark, so the next run, as after a crash, takes it.
    #[test]
    fn failed_snapshot_kept() {
        let mut drain = snapshotting();
        let failed = child(Err::<(), _>(GitError::Unavailable));
        assert_eq!(
            drain.next(failed),
            Ok(Next::Done(Some(CAPTURE_RETRY_AFTER)))
        );
        let mut rerun = snapshotting();
        assert_eq!(rerun.next(child(Ok::<(), GitError>(()))), delete_mark());
    }

    // A mark whose finish has not stored its graph stays, without a snapshot.
    #[test]
    fn unready_mark_kept() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        assert_eq!(drain.next(marks(&mark("first", false))), Ok(Next::Done(None)));
    }

    fn ordered(event: Ulid, materialized_at_ms: u64, ready: bool) -> (Key, Value) {
        let mark = CaptureMark {
            materialized_at_ms,
            event_id: event,
            jsonld: event.to_string(),
            ready,
        };
        mark_row(event, &mark)
    }

    fn snapshot_of(event: Ulid) -> Turn {
        let graph = Some((event, event.to_string()));
        let snapshot = capture(&GitNode::default(), &record(), graph);
        Ok(Next::Call(IoCall::child(snapshot)))
    }

    fn deleted(event: Ulid) -> Turn {
        let row = (
            MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            document_job_key(DOCUMENT, event),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn found() -> IoResult {
        child(Ok::<_, StorageReadError>(Some(record())))
    }

    const LATER: Ulid = Ulid::from_parts(3, 3);

    // A document's marks go in the order their jobs materialized them, not by event: a late
    // older event that materialized last is snapshotted last, so its graph stays on top.
    #[test]
    fn ordered_by_materialization() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![ordered(EVENT, 20, true), ordered(LATER, 10, true)];
        drain.next(IoResult::Page { rows, next: None }).unwrap();
        assert_eq!(drain.next(found()), snapshot_of(LATER));
        assert_eq!(drain.next(child(Ok::<(), GitError>(()))), deleted(LATER));
        assert_eq!(drain.next(IoResult::Done), snapshot_of(EVENT));
        assert_eq!(drain.next(child(Ok::<(), GitError>(()))), deleted(EVENT));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // The first mark not ready stops its document: a later ready graph waits behind it.
    #[test]
    fn open_mark_stops() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![ordered(EVENT, 10, false), ordered(LATER, 20, true)];
        let page = IoResult::Page { rows, next: None };
        assert_eq!(drain.next(page), Ok(Next::Done(None)));
    }

    // A failed snapshot stops its document too, so the next run retries it before any later
    // graph of that document.
    #[test]
    fn failed_snapshot_stops() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![ordered(EVENT, 10, true), ordered(LATER, 20, true)];
        drain.next(IoResult::Page { rows, next: None }).unwrap();
        assert_eq!(drain.next(found()), snapshot_of(EVENT));
        let failed = child(Err::<(), _>(GitError::Unavailable));
        assert_eq!(drain.next(failed), Ok(Next::Done(Some(CAPTURE_RETRY_AFTER))));
    }

    // Marks of one document that a page boundary splits are gathered before any is taken.
    #[test]
    fn pages_join_document() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let first = vec![ordered(EVENT, 20, true)];
        let after = Some(document_job_key(DOCUMENT, EVENT));
        let page = drain.next(IoResult::Page { rows: first, next: after });
        assert!(matches!(page, Ok(Next::Call(IoCall::Page { .. }))));
        let rest = vec![ordered(LATER, 10, true)];
        drain.next(IoResult::Page { rows: rest, next: None }).unwrap();
        assert_eq!(drain.next(found()), snapshot_of(LATER));
    }

    // A mark that does not decode goes without a snapshot.
    #[test]
    fn unreadable_mark_deleted() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![(document_job_key(DOCUMENT, EVENT), Value::from(vec![0xff]))];
        assert_eq!(drain.next(IoResult::Page { rows, next: None }), delete_mark());
    }
}
