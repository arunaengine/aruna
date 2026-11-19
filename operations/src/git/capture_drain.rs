//! Snapshots the revision each capture mark holds once its materialization finished, and
//! deletes the mark only after the snapshot, so a crash never loses one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageReadError;
use aruna_core::git::GitNode;
use aruna_core::keyspaces::{MATERIALIZATION_CAPTURE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{MaterializationState, MaterializationStatusRecord, MetadataRawRevision};
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::storage_entries::materialization_status_key;
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
    Status,
    Record,
    Capture,
    Delete,
}

/// One drain run over every capture mark. A mark whose job still runs stays, one whose status
/// reached it as materialized gets its snapshot and then goes, and one whose job failed goes
/// without a snapshot. Answers when to run again after a failed snapshot.
#[derive(Debug, PartialEq)]
pub struct CaptureDrainOperation {
    node: GitNode,
    pages: PrefixPages,
    marks: VecDeque<(Key, Value)>,
    current: Option<(Key, Ulid, Ulid)>,
    revision: Option<MetadataRawRevision>,
    failed: bool,
    step: CaptureStep,
}

/// The document and event of a capture mark key.
fn mark_parts(key: &[u8]) -> Option<(Ulid, Ulid)> {
    let document = <[u8; 16]>::try_from(key.get(..16)?).ok()?;
    let event = <[u8; 16]>::try_from(key.get(16..)?).ok()?;
    Some((Ulid::from_bytes(document), Ulid::from_bytes(event)))
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
            revision: None,
            failed: false,
            step: CaptureStep::Page,
        }
    }

    fn next_mark(&mut self) -> Turn {
        let Some((key, value)) = self.marks.pop_front() else {
            if !self.pages.done() {
                self.step = CaptureStep::Page;
                return Ok(Next::Call(self.pages.call()));
            }
            return Ok(Next::Done(self.failed.then_some(CAPTURE_RETRY_AFTER)));
        };
        let Some((document_id, event_id)) = mark_parts(&key) else {
            self.current = Some((key, Ulid::nil(), Ulid::nil()));
            return self.delete();
        };
        // A mark that does not decode still gets a snapshot, of the graph the document shows.
        self.revision = postcard::from_bytes(&value).unwrap_or_else(|error| {
            warn!(%document_id, %error, "A Git capture mark holds no readable revision");
            None
        });
        self.current = Some((key, document_id, event_id));
        self.step = CaptureStep::Status;
        let row = (
            MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            materialization_status_key(document_id),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn current(&self) -> Result<&(Key, Ulid, Ulid), IoError> {
        self.current.as_ref().ok_or(IoError::Unfinished)
    }

    /// The mark of a job whose status reached it: materialized gets its snapshot, any other
    /// final state goes without one; a job still running keeps its mark.
    fn settled(&mut self, status: Option<MaterializationStatusRecord>) -> Turn {
        let (_, document_id, event_id) = *self.current()?;
        match status {
            None => self.delete(),
            Some(status) if status.event_id < event_id => self.next_mark(),
            Some(status) => match status.state {
                MaterializationState::Pending => self.next_mark(),
                MaterializationState::Failed => self.delete(),
                MaterializationState::Materialized => {
                    self.step = CaptureStep::Record;
                    let read = DocumentRecordOperation::new(document_id);
                    Ok(Next::Call(IoCall::child(read)))
                }
            },
        }
    }

    /// A failed step keeps the mark for the next run.
    fn kept(&mut self, error: impl std::fmt::Display) -> Turn {
        let (_, document_id, _) = *self.current()?;
        warn!(%document_id, %error, "The Git snapshot of a materialized document waits");
        self.failed = true;
        self.next_mark()
    }

    fn delete(&mut self) -> Turn {
        let (key, ..) = self.current.take().ok_or(IoError::Unfinished)?;
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
            CaptureStep::Status => {
                let value = result.value()?;
                let status = value.as_deref().map(postcard::from_bytes).transpose();
                match status {
                    Ok(status) => self.settled(status),
                    Err(error) => self.kept(error),
                }
            }
            CaptureStep::Record => {
                match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
                    Ok(Some(record)) => {
                        self.step = CaptureStep::Capture;
                        let snapshot = capture(&self.node, &record, self.revision.as_ref());
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
    use aruna_core::types::Value;

    const DOCUMENT: Ulid = Ulid::from_parts(1, 1);
    const EVENT: Ulid = Ulid::from_parts(2, 2);
    const LATER: Ulid = Ulid::from_parts(2, 9);

    fn revision(jsonld: &str, event_id: Ulid) -> MetadataRawRevision {
        MetadataRawRevision {
            jsonld: jsonld.to_string(),
            winning_event_id: event_id,
            context_digest: [0; 32],
            dataset_digest: None,
            merged: None,
        }
    }

    fn mark_row(event_id: Ulid, revision: &MetadataRawRevision) -> (Key, Value) {
        let value = postcard::to_allocvec(&Some(revision.clone())).unwrap();
        (document_job_key(DOCUMENT, event_id), Value::from(value))
    }

    fn marks() -> IoResult {
        let rows = vec![mark_row(EVENT, &revision("first", EVENT))];
        IoResult::Page { rows, next: None }
    }

    fn status(event_id: Ulid, state: MaterializationState) -> IoResult {
        let record = MaterializationStatusRecord {
            document_id: DOCUMENT,
            event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(DOCUMENT),
            context_digest: None,
            dataset_digest: None,
            state,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        let value = Value::from(postcard::to_allocvec(&record).unwrap());
        IoResult::Values(vec![Some(value)])
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

    /// Runs a drain up to the snapshot of the one materialized mark.
    fn snapshotting() -> CaptureDrainOperation {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        drain.next(marks()).unwrap();
        let read = drain.next(status(EVENT, MaterializationState::Materialized));
        let record_read = IoCall::child(DocumentRecordOperation::new(DOCUMENT));
        assert_eq!(read, Ok(Next::Call(record_read)));
        let found = child(Ok::<_, StorageReadError>(Some(record())));
        let first = revision("first", EVENT);
        let snapshot = IoCall::child(capture(&GitNode::default(), &record(), Some(&first)));
        assert_eq!(drain.next(found), Ok(Next::Call(snapshot)));
        drain
    }

    // A materialized document gets its snapshot, and its mark goes only after it.
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

    // Two quick edits each get the snapshot of the revision their own job displayed, even
    // when the newer one finished before the drain ran.
    #[test]
    fn marks_keep_revisions() {
        let (first, second) = (revision("first", EVENT), revision("second", LATER));
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        let rows = vec![mark_row(EVENT, &first), mark_row(LATER, &second)];
        drain.next(IoResult::Page { rows, next: None }).unwrap();
        for (event_id, displayed) in [(EVENT, first), (LATER, second)] {
            drain.next(status(LATER, MaterializationState::Materialized)).unwrap();
            let found = child(Ok::<_, StorageReadError>(Some(record())));
            let snapshot = capture(&GitNode::default(), &record(), Some(&displayed));
            assert_eq!(drain.next(found), Ok(Next::Call(IoCall::child(snapshot))));
            let row = (
                MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
                document_job_key(DOCUMENT, event_id),
            );
            let deleted = Ok(Next::Call(IoCall::Delete(vec![row])));
            assert_eq!(drain.next(child(Ok::<(), GitError>(()))), deleted);
            drain.next(IoResult::Done).unwrap();
        }
    }

    fn reading() -> CaptureDrainOperation {
        let mut drain = CaptureDrainOperation::new(GitNode::default());
        drain.first().unwrap();
        drain.next(marks()).unwrap();
        drain
    }

    // A job still running keeps its mark; a failed one drops it without a snapshot.
    #[test]
    fn unfinished_mark_kept() {
        let pending = reading().next(status(EVENT, MaterializationState::Pending));
        assert_eq!(pending, Ok(Next::Done(None)));
        let older = Ulid::from_parts(1, 9);
        let earlier = reading().next(status(older, MaterializationState::Materialized));
        assert_eq!(earlier, Ok(Next::Done(None)));
        let failed = reading().next(status(EVENT, MaterializationState::Failed));
        assert_eq!(failed, delete_mark());
    }
}
