//! Snapshots the graph each ready capture mark holds and deletes the mark only after the
//! snapshot, so a crash never loses one.
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
use super::snapshot::{Source, capture};
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

/// One drain run over every capture mark. A mark the finish has not made ready stays; a ready
/// one gets the snapshot of the graph it holds and then goes. Answers when to run again after
/// a failed snapshot.
#[derive(Debug, PartialEq)]
pub struct CaptureDrainOperation {
    node: GitNode,
    pages: PrefixPages,
    marks: VecDeque<(Key, Value)>,
    current: Option<(Key, Ulid)>,
    graph: Source,
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
            current: None,
            graph: None,
            failed: false,
            step: CaptureStep::Page,
        }
    }

    fn next_mark(&mut self) -> Turn {
        loop {
            let Some((key, value)) = self.marks.pop_front() else {
                if !self.pages.done() {
                    self.step = CaptureStep::Page;
                    return Ok(Next::Call(self.pages.call()));
                }
                return Ok(Next::Done(self.failed.then_some(CAPTURE_RETRY_AFTER)));
            };
            let Some(document_id) = mark_document(&key) else {
                self.current = Some((key, Ulid::nil()));
                return self.delete();
            };
            let mark = match CaptureMark::from_bytes(&value) {
                Ok(mark) => mark,
                Err(error) => {
                    warn!(%document_id, %error, "Deleting an unreadable Git capture mark");
                    self.current = Some((key, document_id));
                    return self.delete();
                }
            };
            // A job whose finish has not stored its graph yet keeps its mark.
            if !mark.ready {
                continue;
            }
            self.graph = Some((mark.event_id, mark.jsonld));
            self.current = Some((key, document_id));
            self.step = CaptureStep::Record;
            let read = DocumentRecordOperation::new(document_id);
            return Ok(Next::Call(IoCall::child(read)));
        }
    }

    /// A failed step keeps the mark for the next run.
    fn kept(&mut self, error: impl std::fmt::Display) -> Turn {
        let (_, document_id) = self.current.take().ok_or(IoError::Unfinished)?;
        warn!(%document_id, %error, "The Git snapshot of a materialized document waits");
        self.failed = true;
        self.next_mark()
    }

    fn delete(&mut self) -> Turn {
        let (key, _) = self.current.take().ok_or(IoError::Unfinished)?;
        self.step = CaptureStep::Delete;
        let row = (MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
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
                let rows = self.pages.take(result)?;
                self.marks = rows.into_iter().collect();
                self.next_mark()
            }
            CaptureStep::Record => {
                match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
                    Ok(Some(record)) => {
                        self.step = CaptureStep::Capture;
                        let snapshot = capture(&self.node, &record, self.graph.take());
                        Ok(Next::Call(IoCall::child(snapshot)))
                    }
                    // A deleted document has no snapshot to take.
                    Ok(None) => self.delete(),
                    Err(error) => self.kept(format!("{error:?}")),
                }
            }
            CaptureStep::Capture => match result.child::<(), GitError>()? {
                Ok(()) => self.delete(),
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

    // A mark that does not decode goes without a snapshot.
    #[test]
    fn unreadable_mark_deleted() {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![(document_job_key(DOCUMENT, EVENT), Value::from(vec![0xff]))];
        assert_eq!(drain.next(IoResult::Page { rows, next: None }), delete_mark());
    }
}
