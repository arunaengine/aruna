//! Queues and drains prune jobs that remove deleted metadata graphs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    DATA_LINK_KEYSPACE, DOCUMENT_IRI_KEYSPACE, DOCUMENT_LINK_KEYSPACE, IRI_INDEX_KEYSPACE,
    PRUNE_JOB_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, GraphPruneRecord, MetadataEffect, MetadataError, MetadataEvent,
};
use aruna_core::operation::Operation;
use aruna_core::repository::remote::NodeScope;
use aruna_core::rows::{PrefixPages, done};
use aruna_core::storage_entries::graph_prune_entry;
use aruna_core::structs::storage::data::link::document_key;
use aruna_core::types::{Key, KeySpace};
use byteview::ByteView;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::prune_jobs::{DuePrune, DuePruneOperation, RescheduleOperation};
use crate::jobs::repository::link_queue::QueueDeletedOperation;
use crate::jobs::repository::links::LinkError;
use crate::metadata::call_results::effect_event;
use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};
use crate::metadata::repository::schedule_prune_drain;
use crate::tasks::queue_backoff::due_after;

const PRUNE_BATCH_SIZE: usize = 128;
/// Index rows deleted per call.
const INDEX_BATCH_SIZE: usize = 128;
/// Data link rows deleted per call.
const LINK_BATCH_SIZE: usize = 256;
/// Document rows read per page.
const ROW_PAGE: usize = 256;

pub const GRAPH_POLL_AFTER: Duration = Duration::from_secs(5);
pub const GRAPH_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq)]
pub struct GraphDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TombstoneProcessingResult {
    pub enqueued: usize,
    pub pruned: usize,
}

#[derive(Debug, Error, PartialEq)]
pub enum MetadataGraphError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Conversion(String),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected event while processing metadata graph prune queue: {0}")]
    UnexpectedEvent(String),
}

impl From<ConversionError> for MetadataGraphError {
    fn from(error: ConversionError) -> Self {
        Self::Conversion(error.to_string())
    }
}

// The runner's conflict keeps its storage error, as before the runner owned the transaction.
impl From<IoError> for MetadataGraphError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

fn graph_effect(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

/// A graph answer, or the error it carries.
fn graph_answer(result: IoResult) -> Result<MetadataEvent, MetadataError> {
    match effect_event(result)? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
        Event::Metadata(event) => Ok(event),
        other => Err(MetadataError::Backend(format!(
            "unexpected graph answer: {}",
            other.label()
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PruneStage {
    Lifecycle,
    Contains,
    Delete,
}

/// Removes the local copy of a graph once its lifecycle marks it deleted; answers whether it
/// was deleted. A node without the graph store fails with `HandleMissing`.
#[derive(Debug, PartialEq)]
pub(crate) struct PruneGraphOperation {
    graph_iri: String,
    stage: PruneStage,
}

impl PruneGraphOperation {
    pub(crate) fn new(graph_iri: &str) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
            stage: PruneStage::Lifecycle,
        }
    }
}

impl Operation for PruneGraphOperation {
    type Output = bool;
    type Error = MetadataError;

    fn first(&mut self) -> Result<Next<bool>, MetadataError> {
        Ok(Next::Call(IoCall::child(GraphDeletedOperation::new(
            &self.graph_iri,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, MetadataError> {
        match self.stage {
            PruneStage::Lifecycle => {
                let deleted = result
                    .child::<bool, GraphDeletedError>()?
                    .map_err(MetadataError::from)?;
                if !deleted {
                    return Ok(Next::Done(false));
                }
                self.stage = PruneStage::Contains;
                Ok(Next::Call(graph_effect(MetadataEffect::ContainsGraph {
                    graph_iri: self.graph_iri.clone(),
                })))
            }
            PruneStage::Contains => match graph_answer(result)? {
                MetadataEvent::ContainsGraphResult { exists: false, .. } => Ok(Next::Done(true)),
                _ => {
                    self.stage = PruneStage::Delete;
                    Ok(Next::Call(graph_effect(MetadataEffect::DeleteGraph {
                        graph_iri: self.graph_iri.clone(),
                    })))
                }
            },
            PruneStage::Delete => {
                graph_answer(result)?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum IndexStage {
    Iri,
    Links,
    Delete,
}

/// Prunes the IRI index and data link rows of deleted documents, read through each document's
/// own rows, so a deletion prunes its postings without touching any other document.
#[derive(Debug, PartialEq)]
pub(crate) struct DeletedIndexOperation {
    documents: Vec<Ulid>,
    stage: IndexStage,
    position: usize,
    pages: Option<PrefixPages>,
    deletes: VecDeque<Vec<(KeySpace, Key)>>,
    iri: Vec<(KeySpace, Key)>,
    links: Vec<(KeySpace, Key)>,
}

impl DeletedIndexOperation {
    pub(crate) fn new(documents: impl IntoIterator<Item = Ulid>) -> Self {
        Self {
            documents: documents.into_iter().collect(),
            stage: IndexStage::Iri,
            position: 0,
            pages: None,
            deletes: VecDeque::new(),
            iri: Vec::new(),
            links: Vec::new(),
        }
    }

    fn space(&self) -> &'static str {
        match self.stage {
            IndexStage::Iri => DOCUMENT_IRI_KEYSPACE,
            _ => DOCUMENT_LINK_KEYSPACE,
        }
    }

    /// The next page of the current document's rows, the next document, or the deletes.
    fn scan(&mut self) -> Result<Next<()>, MetadataError> {
        loop {
            if let Some(pages) = self.pages.as_ref()
                && !pages.done()
            {
                return Ok(Next::Call(pages.call()));
            }
            if self.pages.take().is_some() {
                self.position += 1;
            }
            if let Some(document_id) = self.documents.get(self.position) {
                let prefix = Key::from(document_id.to_bytes().to_vec());
                self.pages = Some(PrefixPages::new(self.space(), prefix, ROW_PAGE));
                continue;
            }
            match self.stage {
                IndexStage::Iri => {
                    self.stage = IndexStage::Links;
                    self.position = 0;
                }
                _ => return self.delete_next(),
            }
        }
    }

    fn page_read(&mut self, result: IoResult) -> Result<Next<()>, MetadataError> {
        let document_id = self.documents[self.position];
        let pages = self.pages.as_mut().ok_or(MetadataError::InvalidEffect)?;
        for (key, _) in pages.take(result)? {
            let Some(posting) = key.get(16..).map(<[u8]>::to_vec) else {
                continue;
            };
            let row = Key::from(document_key(document_id, &posting));
            match self.stage {
                IndexStage::Iri => {
                    self.iri
                        .push((IRI_INDEX_KEYSPACE.to_string(), Key::from(posting)));
                    self.iri.push((DOCUMENT_IRI_KEYSPACE.to_string(), row));
                }
                _ => {
                    self.links.push((DOCUMENT_LINK_KEYSPACE.to_string(), row));
                    self.links
                        .push((DATA_LINK_KEYSPACE.to_string(), Key::from(posting)));
                }
            }
        }
        self.scan()
    }

    fn delete_next(&mut self) -> Result<Next<()>, MetadataError> {
        if self.stage != IndexStage::Delete {
            self.stage = IndexStage::Delete;
            for chunk in std::mem::take(&mut self.iri).chunks(INDEX_BATCH_SIZE) {
                self.deletes.push_back(chunk.to_vec());
            }
            for chunk in std::mem::take(&mut self.links).chunks(LINK_BATCH_SIZE) {
                self.deletes.push_back(chunk.to_vec());
            }
        }
        match self.deletes.pop_front() {
            Some(chunk) => Ok(Next::Call(IoCall::Delete(chunk))),
            None => Ok(Next::Done(())),
        }
    }
}

impl Operation for DeletedIndexOperation {
    type Output = ();
    type Error = MetadataError;

    fn first(&mut self) -> Result<Next<()>, MetadataError> {
        self.scan()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataError> {
        match self.stage {
            IndexStage::Delete => {
                done(result)?;
                self.delete_next()
            }
            _ => self.page_read(result),
        }
    }
}

/// Queues the prune job of one graph and wakes the prune drain; the stored job covers a failed
/// wake.
#[derive(Debug, PartialEq)]
pub(crate) struct EnqueuePruneOperation {
    graph_iri: String,
    now_ms: u64,
    written: bool,
}

impl EnqueuePruneOperation {
    pub(crate) fn new(graph_iri: &str) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
            now_ms: 0,
            written: false,
        }
    }
}

impl Operation for EnqueuePruneOperation {
    type Output = ();
    type Error = MetadataGraphError;

    fn stamp(&mut self, now_ms: u64) {
        if !self.written {
            self.now_ms = now_ms;
        }
    }

    fn first(&mut self) -> Result<Next<()>, MetadataGraphError> {
        let job = GraphPruneRecord::new(self.graph_iri.clone(), self.now_ms);
        Ok(Next::Call(IoCall::Write(vec![graph_prune_entry(&job)?])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataGraphError> {
        if !self.written {
            done(result)?;
            self.written = true;
            return Ok(Next::Call(IoCall::Effect(Box::new(schedule_prune_drain()))));
        }
        if let Ok(Event::Task(aruna_core::task::TaskEvent::Error { message, .. })) =
            effect_event(result)
        {
            warn!(%message, graph_iri = %self.graph_iri, "Failed to wake the metadata graph prune drain");
        }
        Ok(Next::Done(()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TombstoneStage {
    Uncache,
    Enqueue,
    Prune,
    Index,
}

/// Queues and runs the prunes for graphs a document sync deleted: each graph's registry row
/// leaves the cache, its prune job is queued, and its local copy is removed when this node keeps
/// a graph store; the deleted documents' index rows go last. A failure of one graph is logged
/// and the others continue.
#[derive(Debug, PartialEq)]
pub struct TombstonesOperation {
    tombstones: VecDeque<GraphLifecycleRecord>,
    current: Option<GraphLifecycleRecord>,
    deleted_documents: BTreeSet<Ulid>,
    stage: TombstoneStage,
    result: TombstoneProcessingResult,
}

impl TombstonesOperation {
    pub fn new(tombstones: impl IntoIterator<Item = GraphLifecycleRecord>) -> Self {
        let mut seen = BTreeSet::new();
        let tombstones = tombstones
            .into_iter()
            .filter(|tombstone| tombstone.is_deleted() && seen.insert(tombstone.graph_iri.clone()))
            .collect();
        Self {
            tombstones,
            current: None,
            deleted_documents: BTreeSet::new(),
            stage: TombstoneStage::Uncache,
            result: TombstoneProcessingResult::default(),
        }
    }

    fn next_tombstone(&mut self) -> Result<Next<TombstoneProcessingResult>, MetadataGraphError> {
        let Some(tombstone) = self.tombstones.pop_front() else {
            self.current = None;
            self.stage = TombstoneStage::Index;
            let documents = std::mem::take(&mut self.deleted_documents);
            return Ok(Next::Call(IoCall::child(DeletedIndexOperation::new(
                documents,
            ))));
        };
        self.deleted_documents.insert(tombstone.document_id);
        let uncache = MetadataEffect::UncacheRecord {
            document_id: tombstone.document_id,
        };
        self.current = Some(tombstone);
        self.stage = TombstoneStage::Uncache;
        Ok(Next::Call(graph_effect(uncache)))
    }

    fn graph_iri(&self) -> String {
        self.current
            .as_ref()
            .map(|tombstone| tombstone.graph_iri.clone())
            .unwrap_or_default()
    }
}

impl Operation for TombstonesOperation {
    type Output = TombstoneProcessingResult;
    type Error = MetadataGraphError;

    fn first(&mut self) -> Result<Next<TombstoneProcessingResult>, MetadataGraphError> {
        self.next_tombstone()
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<TombstoneProcessingResult>, MetadataGraphError> {
        let graph_iri = self.graph_iri();
        match self.stage {
            TombstoneStage::Uncache => {
                // A node without the cache has nothing to drop.
                let _ = effect_event(result);
                self.stage = TombstoneStage::Enqueue;
                Ok(Next::Call(IoCall::child(EnqueuePruneOperation::new(
                    &graph_iri,
                ))))
            }
            TombstoneStage::Enqueue => {
                if let Err(error) = result.child::<(), MetadataGraphError>()? {
                    warn!(%graph_iri, error = ?error, "Failed to enqueue metadata graph prune job");
                    return self.next_tombstone();
                }
                self.result.enqueued = self.result.enqueued.saturating_add(1);
                self.stage = TombstoneStage::Prune;
                Ok(Next::Call(IoCall::child(PruneGraphOperation::new(
                    &graph_iri,
                ))))
            }
            TombstoneStage::Prune => {
                match result.child::<bool, MetadataError>()? {
                    Ok(true) => self.result.pruned = self.result.pruned.saturating_add(1),
                    Ok(false) | Err(MetadataError::HandleMissing) => {}
                    Err(error) => {
                        warn!(%graph_iri, error = ?error, "Failed to prune deleted metadata graph");
                    }
                }
                self.next_tombstone()
            }
            TombstoneStage::Index => {
                if let Err(error) = result.child::<(), MetadataError>()? {
                    warn!(error = ?error, "Failed to prune IRI index rows for deleted documents");
                }
                Ok(Next::Done(std::mem::take(&mut self.result)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum BatchStage {
    Due,
    /// Whether the current group's graph is deleted.
    Lifecycle,
    Prune,
    Links,
    Reschedule,
    Finish,
}

/// One drain pass of the prune queue: every due graph whose lifecycle is deleted loses its local
/// copy and queues its repository link checks; a failure reschedules the graph's job.
#[derive(Debug, PartialEq)]
pub struct PruneBatchOperation {
    scope: NodeScope,
    now_ms: u64,
    stage: BatchStage,
    groups: VecDeque<(String, Vec<(Vec<u8>, GraphPruneRecord)>)>,
    current: Option<(String, Vec<(Vec<u8>, GraphPruneRecord)>)>,
    completed: Vec<Vec<u8>>,
    processed: usize,
    jobs: usize,
    has_more_due: bool,
    next_due_ms: Option<u64>,
}

type BatchTurn = Result<Next<GraphDrainResult>, MetadataGraphError>;

impl PruneBatchOperation {
    pub fn new(scope: NodeScope) -> Self {
        Self {
            scope,
            now_ms: 0,
            stage: BatchStage::Due,
            groups: VecDeque::new(),
            current: None,
            completed: Vec::new(),
            processed: 0,
            jobs: 0,
            has_more_due: false,
            next_due_ms: None,
        }
    }

    fn due_read(&mut self, result: IoResult) -> BatchTurn {
        let DuePrune {
            jobs,
            has_more_due,
            next_due_ms,
        } = result.child::<DuePrune, IoError>()??;
        self.jobs = jobs.len();
        self.has_more_due = has_more_due;
        self.next_due_ms = next_due_ms;
        let mut groups: BTreeMap<String, Vec<(Vec<u8>, GraphPruneRecord)>> = BTreeMap::new();
        for (key, job) in jobs {
            groups
                .entry(job.graph_iri.clone())
                .or_default()
                .push((key, job));
        }
        self.groups = groups.into_iter().collect();
        self.next_group()
    }

    fn next_group(&mut self) -> BatchTurn {
        let Some((graph_iri, jobs)) = self.groups.pop_front() else {
            return self.finish();
        };
        self.current = Some((graph_iri.clone(), jobs));
        self.stage = BatchStage::Lifecycle;
        Ok(Next::Call(IoCall::child(GraphDeletedOperation::new(
            &graph_iri,
        ))))
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        (self.current.iter())
            .flat_map(|(_, jobs)| jobs.iter().map(|(key, _)| key.clone()))
            .collect()
    }

    fn graph_iri(&self) -> String {
        self.current
            .as_ref()
            .map(|(graph, _)| graph.clone())
            .unwrap_or_default()
    }

    fn complete(&mut self) -> BatchTurn {
        self.completed.extend(self.keys());
        self.next_group()
    }

    fn reschedule(&mut self, error: String) -> BatchTurn {
        let Some((graph_iri, jobs)) = self.current.as_ref() else {
            return self.next_group();
        };
        let job = (jobs.iter().map(|(_, job)| job))
            .max_by_key(|job| (job.attempts, job.due_at_ms))
            .cloned()
            .unwrap_or_else(|| GraphPruneRecord::new(graph_iri.clone(), self.now_ms));
        let reschedule = RescheduleOperation::new(&self.keys(), &job, error, self.now_ms)?;
        self.stage = BatchStage::Reschedule;
        Ok(Next::Call(IoCall::child(reschedule)))
    }

    fn finish(&mut self) -> BatchTurn {
        self.stage = BatchStage::Finish;
        let deletes: Vec<(KeySpace, Key)> = std::mem::take(&mut self.completed)
            .into_iter()
            .map(|key| (PRUNE_JOB_KEYSPACE.to_string(), ByteView::from(key)))
            .collect();
        if deletes.is_empty() {
            return Ok(self.result());
        }
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn result(&self) -> Next<GraphDrainResult> {
        if self.jobs > 0 {
            info!(
                event = "pipeline.metadata_prune.summary",
                jobs = self.jobs,
                processed = self.processed,
                has_more_due = self.has_more_due,
                "Metadata graph prune batch summary"
            );
        }
        Next::Done(GraphDrainResult {
            processed: self.processed,
            has_more_due: self.has_more_due,
            next_due_after: match self.has_more_due {
                true => Some(Duration::ZERO),
                false => self.next_due_ms.map(|due| due_after(self.now_ms, due)),
            },
        })
    }
}

impl Operation for PruneBatchOperation {
    type Output = GraphDrainResult;
    type Error = MetadataGraphError;

    fn stamp(&mut self, now_ms: u64) {
        if self.stage == BatchStage::Due {
            self.now_ms = now_ms;
        }
    }

    fn first(&mut self) -> BatchTurn {
        Ok(Next::Call(IoCall::child(DuePruneOperation::new(
            self.now_ms,
            PRUNE_BATCH_SIZE,
        ))))
    }

    fn next(&mut self, result: IoResult) -> BatchTurn {
        match self.stage {
            BatchStage::Due => self.due_read(result),
            BatchStage::Lifecycle => {
                let deleted = result
                    .child::<bool, GraphDeletedError>()?
                    .map_err(GraphDeletedError::into_error::<MetadataGraphError>)?;
                if !deleted {
                    return self.complete();
                }
                self.processed = self.processed.saturating_add(1);
                self.stage = BatchStage::Prune;
                Ok(Next::Call(IoCall::child(PruneGraphOperation::new(
                    &self.graph_iri(),
                ))))
            }
            BatchStage::Prune => match result.child::<bool, MetadataError>()? {
                Ok(_) => {
                    // A retry after a failed link queueing finds the graph already pruned, so it
                    // queues again.
                    self.stage = BatchStage::Links;
                    Ok(Next::Call(IoCall::child(QueueDeletedOperation::new(
                        self.scope.clone(),
                        self.graph_iri(),
                    ))))
                }
                Err(MetadataError::HandleMissing) => {
                    self.reschedule("metadata handle missing".to_string())
                }
                Err(error) => self.reschedule(error.to_string()),
            },
            BatchStage::Links => match result.child::<(), LinkError>()? {
                Ok(()) => self.complete(),
                Err(error) => {
                    self.reschedule(format!("queueing the repository links failed: {error}"))
                }
            },
            BatchStage::Reschedule => {
                result.child::<(), IoError>()??;
                self.next_group()
            }
            BatchStage::Finish => {
                done(result)?;
                Ok(self.result())
            }
        }
    }
}

/// The delay after which the prune drain has work: at once while jobs are due, otherwise at the
/// next due job; `None` without any job.
#[derive(Debug, PartialEq)]
pub struct PruneTimerOperation {
    now_ms: u64,
    started: bool,
}

impl PruneTimerOperation {
    pub fn new() -> Self {
        Self {
            now_ms: 0,
            started: false,
        }
    }
}

impl Default for PruneTimerOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for PruneTimerOperation {
    type Output = Option<Duration>;
    type Error = MetadataGraphError;

    fn stamp(&mut self, now_ms: u64) {
        if !self.started {
            self.now_ms = now_ms;
        }
    }

    fn first(&mut self) -> Result<Next<Option<Duration>>, MetadataGraphError> {
        self.started = true;
        Ok(Next::Call(IoCall::child(DuePruneOperation::new(
            self.now_ms,
            1,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Duration>>, MetadataGraphError> {
        let due = result.child::<DuePrune, IoError>()??;
        if !due.jobs.is_empty() || due.has_more_due {
            return Ok(Next::Done(Some(Duration::ZERO)));
        }
        Ok(Next::Done(
            due.next_due_ms.map(|due| due_after(self.now_ms, due)),
        ))
    }
}

/// Whether any prune job is queued, due or not; repairs the queue on the way.
pub(crate) fn jobs_queued() -> DuePruneOperation {
    DuePruneOperation::new(u64::MAX, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::DriverContext;
    use crate::testing::{process_graph_tombstones, process_prune_batch, prune_jobs_exist};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
    use aruna_core::storage_entries::{graph_lifecycle_entry, registry_write_entries};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_storage::FjallStorage;
    use aruna_storage::StorageHandle;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    use crate::metadata::MetadataHandle;
    use crate::testing::write_entries;
    use crate::tests::metadata::storage_key_exists;

    fn lifecycle(graph_iri: &str) -> GraphLifecycleRecord {
        GraphLifecycleRecord::deleted(
            graph_iri.to_string(),
            RealmId::from_bytes([7u8; 32]),
            Ulid::from_parts(7, 1),
            Ulid::from_parts(7, 2),
            1,
        )
    }

    fn registry_record(group_id: Ulid, path: &str) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let document_id = Ulid::generate();
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    async fn index_row_exists(storage: &StorageHandle, key: ByteView) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::keyspaces::IRI_INDEX_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<GraphPruneRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: PRUNE_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).expect("job decodes"))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    fn storage_context(storage: &StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    #[tokio::test]
    async fn lifecycle_tombstone_processing() {
        let dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let graph_handle = aruna_graph::GraphHandle::open(
            metadata_dir.path(),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .expect("metadata handle opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle.clone()),
            graph_handle: Some(graph_handle.clone()),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let record = registry_record(Ulid::from_parts(8, 1), "docs/tombstoned");
        write_entries(
            &storage,
            registry_write_entries(&record).expect("registry entries"),
        )
        .await;
        metadata_handle.expire_visibility_caches();
        let stale = crate::metadata::list_cached_group(&context, record.group_id)
            .await
            .expect("registry cache fills");
        assert_eq!(stale.as_ref(), &vec![record.clone()]);
        let tombstone = GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );
        write_entries(
            &storage,
            vec![graph_lifecycle_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        let processed = process_graph_tombstones(&context, vec![tombstone.clone()]).await;

        assert_eq!(processed.enqueued, 1);
        let listed = crate::metadata::list_cached_group(&context, record.group_id)
            .await
            .expect("registry cache reads");
        assert!(listed.is_empty());
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, record.graph_iri);
    }

    #[tokio::test]
    async fn prunes_index_rows() {
        // Deleting a document drops its IRI reference index rows.
        let dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let graph_handle = aruna_graph::GraphHandle::open(
            metadata_dir.path(),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .expect("metadata handle opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            graph_handle: Some(graph_handle),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let record = registry_record(Ulid::from_parts(9, 1), "docs/deleted");
        let index_key = aruna_core::storage_entries::iri_reference_key(
            "https://schema.org/conformsTo",
            "https://example.test/profile",
            record.document_id,
        );
        let document_row =
            aruna_core::structs::storage::data::link::document_key(record.document_id, &index_key);
        write_entries(
            &storage,
            vec![
                (
                    aruna_core::keyspaces::IRI_INDEX_KEYSPACE.to_string(),
                    index_key.clone(),
                    ByteView::from(vec![1u8]),
                ),
                (
                    aruna_core::keyspaces::DOCUMENT_IRI_KEYSPACE.to_string(),
                    ByteView::from(document_row),
                    ByteView::from(Vec::new()),
                ),
            ],
        )
        .await;
        let tombstone = GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );

        process_graph_tombstones(&context, vec![tombstone]).await;

        assert!(!index_row_exists(&storage, index_key).await);
    }

    #[tokio::test]
    async fn job_dropped_when() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let job = GraphPruneRecord::new("urn:graph:not-deleted".to_string(), 1);
        write_entries(&storage, vec![graph_prune_entry(&job).expect("job entry")]).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context).await.expect("queue drains");

        assert_eq!(result.processed, 0);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn corrupt_prune_job() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        write_entries(
            &storage,
            vec![(
                PRUNE_JOB_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(vec![1, 2, 3]),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(!storage_key_exists(&storage, PRUNE_JOB_KEYSPACE, corrupt_key).await);
    }

    #[tokio::test]
    async fn existence_deletes_corrupt() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        let valid_job = GraphPruneRecord::new("urn:graph:valid".to_string(), 1);
        write_entries(
            &storage,
            vec![
                (
                    PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(corrupt_key.clone()),
                    ByteView::from(vec![1, 2, 3]),
                ),
                graph_prune_entry(&valid_job).expect("job entry"),
            ],
        )
        .await;

        assert!(prune_jobs_exist(&storage_context(&storage)).await.unwrap());
        assert!(!storage_key_exists(&storage, PRUNE_JOB_KEYSPACE, corrupt_key).await);
    }

    #[tokio::test]
    async fn missing_handle_reschedules() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let graph_iri = "urn:graph:deleted";
        let job = GraphPruneRecord::new(graph_iri.to_string(), 1);
        write_entries(
            &storage,
            vec![
                graph_lifecycle_entry(&lifecycle(graph_iri)).expect("lifecycle entry"),
                graph_prune_entry(&job).expect("job entry"),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context)
            .await
            .expect("queue reschedules");

        assert_eq!(result.processed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, graph_iri);
        assert_eq!(jobs[0].attempts, 1);
        assert_eq!(
            jobs[0].last_error.as_deref(),
            Some("metadata handle missing")
        );
        assert!(jobs[0].due_at_ms > job.due_at_ms);
    }

    #[tokio::test]
    async fn existence_reflects_queue() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");

        assert!(!prune_jobs_exist(&storage_context(&storage)).await.unwrap());
        let job = GraphPruneRecord::new("urn:graph:queued".to_string(), 1);
        write_entries(&storage, vec![graph_prune_entry(&job).expect("job entry")]).await;

        assert!(prune_jobs_exist(&storage_context(&storage)).await.unwrap());
    }
}
