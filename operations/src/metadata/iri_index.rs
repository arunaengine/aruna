//! Builds and rebuilds the IRI reference index that maps referenced IRIs back to documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Rows are keyed object first, so a backlink lookup is a seek, and each document keeps its
// own rows, so a materialization replaces exactly its own postings.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    DOCUMENT_IRI_KEYSPACE, IRI_INDEX_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    METADATA_INDEX_KEYSPACE, prefix_upper_bound,
};
use aruna_core::metadata::{
    IriIndexRecord, MaterializationState, MaterializationStatusRecord, MetadataEffect,
    MetadataError, MetadataEvent,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{all_values, event};
use aruna_core::storage_entries::{
    iri_object_prefix, iri_reference_entry, iri_reference_prefix, materialization_status_key,
};
use aruna_core::structs::storage::data::link::{DataLink, LinkPosting, document_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::Key;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::api::read_access::{child_results, children};
use crate::metadata::call_results::page_rows;
use crate::metadata::live_records::{LiveRecordsError, LiveRecordsOperation};
use crate::metadata::record_reads::DocumentRecordOperation;

use super::data_links::{
    AssociateVersionsOperation, DataLinkError, DocumentRowsOperation, LinkBudget, LinkRows,
    LinkRowsOperation,
};
use super::repository::StorageReadError;

const INDEX_PAGE_SIZE: usize = 128;

/// Keeps the storage cause intact so callers can tell an overloaded backend
/// apart from a genuine index or document problem.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum MetadataIriError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    DataLink(#[from] DataLinkError),
    #[error("unexpected metadata IRI reference index event: {0}")]
    UnexpectedEvent(String),
}

impl From<IoError> for MetadataIriError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            error => Self::UnexpectedEvent(error.to_string()),
        }
    }
}

impl From<MetadataIriError> for MetadataError {
    fn from(error: MetadataIriError) -> Self {
        MetadataError::Backend(error.to_string())
    }
}

pub(crate) fn project_iri_references(
    document_id: Ulid,
    document_cursor: Ulid,
    references: Vec<(String, String, String)>,
) -> Vec<IriIndexRecord> {
    let mut grouped = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for (subject_iri, predicate_iri, object_iri) in references {
        grouped
            .entry((predicate_iri, object_iri))
            .or_default()
            .insert(subject_iri);
    }

    grouped
        .into_iter()
        .map(
            |((predicate_iri, object_iri), subject_iris)| IriIndexRecord {
                document_id,
                document_cursor,
                predicate_iri,
                object_iri,
                subject_iris: subject_iris.into_iter().collect(),
            },
        )
        .collect()
}

/// One page of the postings that name an object: every row of at most `limit` documents
/// after the requested one, by document id. `next` is the last document when more may follow.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct BacklinkPage {
    pub documents: BTreeMap<Ulid, Vec<IriIndexRecord>>,
    pub next: Option<Ulid>,
}

/// Why a backlink page could not be read.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum BacklinkError {
    #[error("metadata IRI backlink lookup exceeded its budget")]
    BudgetExhausted,
    /// A stored row does not decode: callers charge it to the document.
    #[error("metadata IRI backlink row is malformed: {0}")]
    Malformed(String),
    #[error("metadata IRI backlink read failed: {0}")]
    Unavailable(String),
}

impl From<aruna_core::calls::IoError> for BacklinkError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        match error {
            aruna_core::calls::IoError::Codec(error) => Self::Malformed(error),
            other => Self::Unavailable(other.to_string()),
        }
    }
}

#[derive(Debug, PartialEq)]
enum BacklinkStage {
    /// The seek for the next predicate group under the object.
    Group,
    /// The rows of the group with this prefix.
    Rows(Vec<u8>),
}

/// Reads one page of the postings that name `object_iri`, within `budget`, which comes back
/// with what is left. Without a predicate, each predicate group under the object is found by
/// one seek and read from `after` on. Hash collisions are dropped by comparing stored IRIs.
#[derive(Debug, PartialEq)]
pub(crate) struct BacklinksOperation {
    object_iri: String,
    predicate_iri: Option<String>,
    after: Option<Ulid>,
    limit: usize,
    budget: LinkBudget,
    start: Option<Vec<u8>>,
    documents: BTreeMap<Ulid, Vec<IriIndexRecord>>,
    truncated: bool,
    stage: BacklinkStage,
}

type BacklinkTurn = Result<Next<(BacklinkPage, LinkBudget)>, BacklinkError>;

fn index_iter(prefix: Vec<u8>, start: IterStart, limit: usize) -> IoCall {
    IoCall::Effect(Box::new(Effect::Storage(StorageEffect::Iter {
        key_space: IRI_INDEX_KEYSPACE.to_string(),
        prefix: Some(ByteView::from(prefix)),
        start: Some(start),
        limit,
        txn_id: None,
    })))
}

impl BacklinksOperation {
    pub(crate) fn new(
        (object_iri, predicate_iri): (&str, Option<&str>),
        (after, limit): (Option<Ulid>, usize),
        budget: LinkBudget,
    ) -> Self {
        Self {
            object_iri: object_iri.to_string(),
            predicate_iri: predicate_iri.map(str::to_string),
            after,
            limit: limit.max(1),
            budget,
            start: Some(iri_object_prefix(object_iri).to_vec()),
            documents: BTreeMap::new(),
            truncated: false,
            stage: BacklinkStage::Group,
        }
    }

    /// Reads up to `limit + 1` rows of one predicate group after the page start.
    fn read_group(&mut self, prefix: Vec<u8>) -> BacklinkTurn {
        if !self.budget.take_seek() {
            return Err(BacklinkError::BudgetExhausted);
        }
        let start = match self.after {
            Some(document_id) => IterStart::After(ByteView::from(group_key(&prefix, document_id))),
            None => IterStart::At(ByteView::from(prefix.clone())),
        };
        let call = index_iter(prefix.clone(), start, self.limit.saturating_add(1));
        self.stage = BacklinkStage::Rows(prefix);
        Ok(Next::Call(call))
    }

    /// Seeks the next predicate group under the object, or ends the page.
    fn seek_group(&mut self) -> BacklinkTurn {
        let Some(start) = self.start.take() else {
            return Ok(Next::Done(self.page()));
        };
        if !self.budget.take_seek() {
            return Err(BacklinkError::BudgetExhausted);
        }
        let object = iri_object_prefix(&self.object_iri).to_vec();
        self.stage = BacklinkStage::Group;
        Ok(Next::Call(index_iter(
            object,
            IterStart::At(ByteView::from(start)),
            1,
        )))
    }

    fn rows(&mut self, result: IoResult) -> BacklinkTurn {
        let (values, next) = page_rows(result)?;
        self.budget.rows =
            (self.budget.rows.checked_sub(values.len())).ok_or(BacklinkError::BudgetExhausted)?;
        for (_, value) in values {
            let record = postcard::from_bytes::<IriIndexRecord>(&value)
                .map_err(|error| BacklinkError::Malformed(error.to_string()))?;
            if record.object_iri != self.object_iri
                || (self.predicate_iri.as_deref())
                    .is_some_and(|predicate| record.predicate_iri != predicate)
            {
                continue;
            }
            self.documents
                .entry(record.document_id)
                .or_default()
                .push(record);
        }
        self.truncated |= next.is_some();
        if self.predicate_iri.is_some() {
            return Ok(Next::Done(self.page()));
        }
        self.seek_group()
    }

    fn group_found(&mut self, result: IoResult) -> BacklinkTurn {
        let (values, _) = page_rows(result)?;
        let Some(prefix) = values.first().and_then(|(key, _)| key.get(..64)) else {
            return Ok(Next::Done(self.page()));
        };
        let prefix = prefix.to_vec();
        self.start = prefix_upper_bound(&prefix);
        self.read_group(prefix)
    }

    fn page(&mut self) -> (BacklinkPage, LinkBudget) {
        let documents = std::mem::take(&mut self.documents);
        let more = self.truncated || documents.len() > self.limit;
        let documents: BTreeMap<_, _> = documents.into_iter().take(self.limit).collect();
        let next = more
            .then(|| documents.keys().next_back().copied())
            .flatten();
        (BacklinkPage { documents, next }, self.budget)
    }
}

impl Operation for BacklinksOperation {
    type Output = (BacklinkPage, LinkBudget);
    type Error = BacklinkError;

    fn first(&mut self) -> BacklinkTurn {
        match self.predicate_iri.as_deref() {
            Some(predicate) => {
                let prefix = iri_reference_prefix(predicate, &self.object_iri).to_vec();
                self.start = None;
                self.read_group(prefix)
            }
            None => self.seek_group(),
        }
    }

    fn next(&mut self, result: IoResult) -> BacklinkTurn {
        match std::mem::replace(&mut self.stage, BacklinkStage::Group) {
            BacklinkStage::Group => self.group_found(result),
            BacklinkStage::Rows(_) => self.rows(result),
        }
    }
}

fn group_key(prefix: &[u8], document_id: Ulid) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&document_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IriFreshnessState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IriFreshness {
    pub state: IriFreshnessState,
    pub oldest_status_updated: Option<u64>,
}

/// The status rows of the given documents; callers pass the documents a lookup returned, never
/// the whole registry.
pub(crate) fn status_rows(registry_records: &[MetadataRegistryRecord]) -> Vec<(String, ByteView)> {
    (registry_records.iter())
        .map(|record| {
            let key = materialization_status_key(record.document_id);
            (MATERIALIZATION_STATUS_KEYSPACE.to_string(), key)
        })
        .collect()
}

/// Materialization state of the given documents from their [`status_rows`] values.
pub(crate) fn iri_freshness(
    registry_records: &[MetadataRegistryRecord],
    values: Vec<Option<ByteView>>,
) -> Result<IriFreshness, MetadataError> {
    let mut current = false;
    let mut pending = false;
    let mut failed = false;
    let mut oldest_status_updated = None;
    for (record, value) in registry_records.iter().zip(values) {
        let status = value
            .map(|bytes| postcard::from_bytes::<MaterializationStatusRecord>(&bytes))
            .transpose()
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        if let Some(status) = status.as_ref() {
            oldest_status_updated = Some(
                oldest_status_updated.map_or(status.updated_at_ms, |oldest: u64| {
                    oldest.min(status.updated_at_ms)
                }),
            );
        }
        match status {
            Some(status) if status_current(&status, record) => current = true,
            Some(status) if status.state == MaterializationState::Failed => {
                failed = true;
            }
            Some(_) | None => pending = true,
        }
    }
    let kinds = usize::from(current) + usize::from(pending) + usize::from(failed);
    let state = if kinds > 1 {
        IriFreshnessState::Mixed
    } else if failed {
        IriFreshnessState::Failed
    } else if pending {
        IriFreshnessState::Pending
    } else {
        IriFreshnessState::Current
    };
    Ok(IriFreshness {
        state,
        oldest_status_updated,
    })
}

fn status_current(status: &MaterializationStatusRecord, record: &MetadataRegistryRecord) -> bool {
    status.event_id == record.last_event_id
        && status.graph_iri == record.graph_iri
        && status.state == MaterializationState::Materialized
}

fn collect_iri_matches(
    records: Vec<IriIndexRecord>,
    registry_records: &[MetadataRegistryRecord],
    predicate_iri: &str,
    object_iri: &str,
) -> BTreeMap<Ulid, Vec<String>> {
    let current_cursors = registry_records
        .iter()
        .map(|record| (record.document_id, record.last_event_id))
        .collect::<std::collections::HashMap<_, _>>();
    let mut matches = BTreeMap::<Ulid, BTreeSet<String>>::new();
    for record in records {
        if record.predicate_iri != predicate_iri
            || record.object_iri != object_iri
            || current_cursors.get(&record.document_id) != Some(&record.document_cursor)
        {
            continue;
        }
        matches
            .entry(record.document_id)
            .or_default()
            .extend(record.subject_iris);
    }
    matches
        .into_iter()
        .map(|(document_id, subject_iris)| (document_id, subject_iris.into_iter().collect()))
        .collect()
}

fn status_error(error: StorageReadError) -> MetadataIriError {
    match error {
        StorageReadError::Storage(error) => error.into(),
        StorageReadError::Conversion(error) => error.into(),
    }
}

/// Replaces the document's IRI postings: writes the projection and deletes every older row of
/// the document that the projection does not write again.
#[derive(Debug, PartialEq)]
pub(crate) struct IriRowsOperation {
    document_id: Ulid,
    records: Vec<IriIndexRecord>,
}

impl IriRowsOperation {
    pub(crate) fn new(document_id: Ulid, records: Vec<IriIndexRecord>) -> Self {
        Self {
            document_id,
            records,
        }
    }
}

impl Operation for IriRowsOperation {
    type Output = LinkRows;
    type Error = MetadataIriError;

    fn first(&mut self) -> Result<Next<LinkRows>, MetadataIriError> {
        let rows = DocumentRowsOperation::new(DOCUMENT_IRI_KEYSPACE, self.document_id);
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LinkRows>, MetadataIriError> {
        let existing = result.child::<Vec<Vec<u8>>, DataLinkError>()??;
        let document_id = self.document_id;
        let mut rows = LinkRows::default();
        let mut written = HashSet::new();
        for record in &self.records {
            let (key_space, key, value) = iri_reference_entry(record)?;
            rows.writes.push((
                DOCUMENT_IRI_KEYSPACE.to_string(),
                document_key(document_id, &key).into(),
                ByteView::from(Vec::new()),
            ));
            written.insert(key.to_vec());
            rows.writes.push((key_space, key, value));
        }
        for key in existing {
            if !written.contains(&key) {
                let document_row = document_key(document_id, &key).into();
                rows.deletes
                    .push((IRI_INDEX_KEYSPACE.to_string(), key.into()));
                rows.deletes
                    .push((DOCUMENT_IRI_KEYSPACE.to_string(), document_row));
            }
        }
        Ok(Next::Done(rows))
    }
}

/// The document a projection belongs to: its id, the event it projects and its graph.
pub(crate) type ProjectedDocument = (Ulid, Ulid, String);

#[derive(Debug, PartialEq)]
enum ProjectStage {
    References,
    Versions(Vec<IriIndexRecord>),
    Links(Vec<IriIndexRecord>),
    Iris(LinkRows, usize),
}

/// The IRI index and data link rows of one document's graph as one event left it, with the
/// stale rows of the document they replace, and how many postings they write.
#[derive(Debug, PartialEq)]
pub(crate) struct ProjectRowsOperation {
    document: ProjectedDocument,
    stage: ProjectStage,
}

impl ProjectRowsOperation {
    pub(crate) fn new(document: ProjectedDocument) -> Self {
        Self {
            document,
            stage: ProjectStage::References,
        }
    }
}

type ProjectedRows = (LinkRows, usize);

impl Operation for ProjectRowsOperation {
    type Output = ProjectedRows;
    type Error = MetadataIriError;

    fn first(&mut self) -> Result<Next<ProjectedRows>, MetadataIriError> {
        let graph_iri = self.document.2.clone();
        let effect = MetadataEffect::IriReferences { graph_iri };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            effect,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ProjectedRows>, MetadataIriError> {
        let (document_id, cursor, graph_iri) = self.document.clone();
        match std::mem::replace(&mut self.stage, ProjectStage::References) {
            ProjectStage::References => {
                let references = match event(result)? {
                    Event::Metadata(MetadataEvent::IriReferences { references }) => references,
                    Event::Metadata(MetadataEvent::Error { error, .. }) => {
                        return Err(MetadataIriError::UnexpectedEvent(error.to_string()));
                    }
                    other => {
                        return Err(MetadataIriError::UnexpectedEvent(other.label().to_string()));
                    }
                };
                let links = super::data_links::project_links(&graph_iri, cursor, &references);
                let records = project_iri_references(document_id, cursor, references);
                self.stage = ProjectStage::Versions(records);
                Ok(Next::Call(IoCall::child(AssociateVersionsOperation::new(
                    links,
                ))))
            }
            ProjectStage::Versions(records) => {
                let links = result.child::<BTreeMap<DataLink, LinkPosting>, DataLinkError>()??;
                self.stage = ProjectStage::Links(records);
                Ok(Next::Call(IoCall::child(LinkRowsOperation::new(
                    document_id,
                    links,
                ))))
            }
            ProjectStage::Links(records) => {
                let rows = result.child::<LinkRows, DataLinkError>()??;
                self.stage = ProjectStage::Iris(rows, records.len());
                Ok(Next::Call(IoCall::child(IriRowsOperation::new(
                    document_id,
                    records,
                ))))
            }
            ProjectStage::Iris(mut rows, postings) => {
                let iri_rows = result.child::<LinkRows, MetadataIriError>()??;
                rows.writes.extend(iri_rows.writes);
                rows.deletes.extend(iri_rows.deletes);
                Ok(Next::Done((rows, postings)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReplaceStage {
    Status,
    Written(Vec<(String, ByteView)>),
    Deleted,
}

/// Commits one document's rows only while its status still names the projected event, so a
/// newer materialization is never overwritten; `false` when one got there first.
#[derive(Debug, PartialEq)]
pub(crate) struct ReplaceRowsOperation {
    record: MetadataRegistryRecord,
    rows: Option<LinkRows>,
    stage: ReplaceStage,
}

impl ReplaceRowsOperation {
    pub(crate) fn new(record: MetadataRegistryRecord, rows: LinkRows) -> Self {
        Self {
            record,
            rows: Some(rows),
            stage: ReplaceStage::Status,
        }
    }
}

impl Operation for ReplaceRowsOperation {
    type Output = bool;
    type Error = MetadataIriError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    // A materialization that commits first wins; the rebuild leaves the document to it.
    fn is_conflict(_error: &MetadataIriError) -> bool {
        false
    }

    fn first(&mut self) -> Result<Next<bool>, MetadataIriError> {
        Ok(Next::Call(IoCall::Read(status_rows(std::slice::from_ref(
            &self.record,
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, MetadataIriError> {
        match std::mem::replace(&mut self.stage, ReplaceStage::Deleted) {
            ReplaceStage::Status => {
                let value = result.value()?;
                let status = value
                    .map(|bytes| postcard::from_bytes::<MaterializationStatusRecord>(&bytes))
                    .transpose()
                    .map_err(ConversionError::from)?;
                if !status.is_some_and(|status| status_current(&status, &self.record)) {
                    return Ok(Next::Done(false));
                }
                let rows = self.rows.take().unwrap_or_default();
                self.stage = ReplaceStage::Written(rows.deletes);
                Ok(Next::Call(IoCall::Write(rows.writes)))
            }
            ReplaceStage::Written(deletes) => {
                result.done()?;
                self.stage = ReplaceStage::Deleted;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            ReplaceStage::Deleted => {
                result.done()?;
                Ok(Next::Done(true))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RebuildStage {
    Page,
    Live,
    Statuses(Vec<MetadataRegistryRecord>),
    Project(MetadataRegistryRecord),
    Replace(usize),
}

/// Re-projects each live materialized document on its own and replaces its postings, walking
/// the registry a page at a time so its size never bounds the rebuild. Answers how many
/// postings it wrote.
#[derive(Debug, PartialEq, Default)]
pub(crate) struct RebuildIrisOperation {
    next: Option<Key>,
    current: VecDeque<MetadataRegistryRecord>,
    written: usize,
    stage: Option<RebuildStage>,
}

impl RebuildIrisOperation {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn page(&mut self, after: Option<Key>) -> Result<Next<usize>, MetadataIriError> {
        self.stage = Some(RebuildStage::Page);
        Ok(Next::Call(IoCall::Page {
            space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: INDEX_PAGE_SIZE,
        }))
    }

    fn project(&mut self) -> Result<Next<usize>, MetadataIriError> {
        let Some(record) = self.current.pop_front() else {
            return match self.next.take() {
                Some(next) => self.page(Some(next)),
                None => Ok(Next::Done(self.written)),
            };
        };
        let document = (
            record.document_id,
            record.last_event_id,
            record.graph_iri.clone(),
        );
        self.stage = Some(RebuildStage::Project(record));
        Ok(Next::Call(IoCall::child(ProjectRowsOperation::new(
            document,
        ))))
    }
}

impl Operation for RebuildIrisOperation {
    type Output = usize;
    type Error = MetadataIriError;

    fn first(&mut self) -> Result<Next<usize>, MetadataIriError> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, MetadataIriError> {
        let backend =
            |error: &dyn std::fmt::Display| MetadataIriError::UnexpectedEvent(error.to_string());
        match self.stage.take() {
            None => Err(MetadataIriError::UnexpectedEvent(
                "rebuild not started".to_string(),
            )),
            Some(RebuildStage::Page) => {
                let (rows, next) = page_rows(result).map_err(|error| backend(&error))?;
                self.next = next;
                let records: Vec<MetadataRegistryRecord> = rows
                    .iter()
                    .filter_map(|(_, value)| postcard::from_bytes(value).ok())
                    .collect();
                self.stage = Some(RebuildStage::Live);
                Ok(Next::Call(IoCall::child(LiveRecordsOperation::new(
                    records, None,
                ))))
            }
            Some(RebuildStage::Live) => {
                let live = result.child::<Vec<MetadataRegistryRecord>, LiveRecordsError>();
                let live = live
                    .map_err(|error| backend(&error))?
                    .map_err(|error| backend(&error))?;
                let reads = status_rows(&live);
                self.stage = Some(RebuildStage::Statuses(live));
                Ok(Next::Call(IoCall::Read(reads)))
            }
            Some(RebuildStage::Statuses(live)) => {
                let values = all_values(result).map_err(|error| backend(&error))?;
                for (record, value) in live.into_iter().zip(values) {
                    let status = value.and_then(|bytes| {
                        postcard::from_bytes::<MaterializationStatusRecord>(&bytes).ok()
                    });
                    if status.is_some_and(|status| status_current(&status, &record)) {
                        self.current.push_back(record);
                    }
                }
                self.project()
            }
            Some(RebuildStage::Project(record)) => {
                let projected = result.child::<ProjectedRows, MetadataIriError>();
                let (rows, postings) = projected.map_err(|error| backend(&error))??;
                self.stage = Some(RebuildStage::Replace(postings));
                Ok(Next::Call(IoCall::child(ReplaceRowsOperation::new(
                    record, rows,
                ))))
            }
            Some(RebuildStage::Replace(postings)) => {
                match result
                    .child::<bool, MetadataIriError>()
                    .map_err(|error| backend(&error))?
                {
                    Ok(true) => self.written = self.written.saturating_add(postings),
                    Ok(false)
                    | Err(MetadataIriError::Storage(StorageError::TransactionConflict)) => {}
                    Err(error) => return Err(error),
                }
                self.project()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum LookupStage {
    Page,
    Records(Vec<IriIndexRecord>),
    Live(Vec<IriIndexRecord>),
}

/// The live documents whose current postings name `object_iri` through `predicate_iri`, with
/// their subjects: backlink pages by document id and one point read per document, at most
/// `limit` documents and a bounded number of index rows. Never a walk over the registry.
#[derive(Debug, PartialEq)]
pub(crate) struct ReferenceLookupOperation {
    iris: (String, String),
    limit: usize,
    budget: LinkBudget,
    after: Option<Ulid>,
    next: Option<Ulid>,
    found: Vec<MetadataRegistryRecord>,
    matches: BTreeMap<Ulid, Vec<String>>,
    stage: LookupStage,
}

impl ReferenceLookupOperation {
    pub(crate) fn new((predicate_iri, object_iri): (&str, &str), limit: usize) -> Self {
        Self {
            iris: (predicate_iri.to_string(), object_iri.to_string()),
            limit,
            budget: LinkBudget {
                seeks: limit.max(INDEX_PAGE_SIZE),
                rows: limit.saturating_mul(2).max(INDEX_PAGE_SIZE * 2),
            },
            after: None,
            next: None,
            found: Vec::new(),
            matches: BTreeMap::new(),
            stage: LookupStage::Page,
        }
    }

    fn page(&mut self) -> Result<Next<ReferenceMatches>, MetadataIriError> {
        if self.found.len() >= self.limit {
            return Ok(Next::Done(self.answer()));
        }
        let (predicate_iri, object_iri) = (&self.iris.0, &self.iris.1);
        let read = BacklinksOperation::new(
            (object_iri, Some(predicate_iri)),
            (self.after, INDEX_PAGE_SIZE),
            self.budget,
        );
        self.stage = LookupStage::Page;
        Ok(Next::Call(IoCall::child(read)))
    }

    fn answer(&mut self) -> ReferenceMatches {
        (
            std::mem::take(&mut self.found),
            std::mem::take(&mut self.matches),
        )
    }
}

/// The documents a reference lookup found and the subjects of each.
pub(crate) type ReferenceMatches = (Vec<MetadataRegistryRecord>, BTreeMap<Ulid, Vec<String>>);

impl Operation for ReferenceLookupOperation {
    type Output = ReferenceMatches;
    type Error = MetadataIriError;

    fn first(&mut self) -> Result<Next<ReferenceMatches>, MetadataIriError> {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReferenceMatches>, MetadataIriError> {
        let backend =
            |error: &dyn std::fmt::Display| MetadataIriError::UnexpectedEvent(error.to_string());
        match std::mem::replace(&mut self.stage, LookupStage::Page) {
            LookupStage::Page => {
                let page = result.child::<(BacklinkPage, LinkBudget), BacklinkError>();
                let (page, left) = match page.map_err(|error| backend(&error))? {
                    Ok(found) => found,
                    // The budget bounds the lookup; the documents found so far are the answer.
                    Err(BacklinkError::BudgetExhausted) => return Ok(Next::Done(self.answer())),
                    Err(error) => return Err(backend(&error)),
                };
                self.budget = left;
                self.next = page.next;
                let reads: Vec<_> = page
                    .documents
                    .keys()
                    .map(|id| DocumentRecordOperation::new(*id))
                    .collect();
                let postings = page.documents.into_values().flatten().collect();
                if reads.is_empty() {
                    return self.paged();
                }
                self.stage = LookupStage::Records(postings);
                Ok(Next::Call(children(reads)))
            }
            LookupStage::Records(postings) => {
                let loaded =
                    child_results::<Option<MetadataRegistryRecord>, StorageReadError>(result)
                        .map_err(|error| backend(&error))?;
                let mut records = Vec::with_capacity(loaded.len());
                for record in loaded {
                    let record = record.ok_or_else(|| {
                        MetadataIriError::UnexpectedEvent("record read unfinished".to_string())
                    })?;
                    if let Some(record) = record.map_err(status_error)? {
                        records.push(record);
                    }
                }
                self.stage = LookupStage::Live(postings);
                Ok(Next::Call(IoCall::child(LiveRecordsOperation::new(
                    records, None,
                ))))
            }
            LookupStage::Live(postings) => {
                let live = result.child::<Vec<MetadataRegistryRecord>, LiveRecordsError>();
                let live = live
                    .map_err(|error| backend(&error))?
                    .map_err(|error| backend(&error))?;
                let (predicate_iri, object_iri) = (&self.iris.0, &self.iris.1);
                let page_matches = collect_iri_matches(postings, &live, predicate_iri, object_iri);
                for record in live {
                    if self.found.len() < self.limit
                        && let Some(subjects) = page_matches.get(&record.document_id)
                    {
                        self.matches.insert(record.document_id, subjects.clone());
                        self.found.push(record);
                    }
                }
                self.paged()
            }
        }
    }
}

impl ReferenceLookupOperation {
    fn paged(&mut self) -> Result<Next<ReferenceMatches>, MetadataIriError> {
        match self.next.take() {
            Some(next) => {
                self.after = Some(next);
                self.page()
            }
            None => Ok(Next::Done(self.answer())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::data_links::tests::apply_rows;
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::tests::calls::{child, runs_child};
    use aruna_core::effects::StorageEffect;
    use aruna_core::entity_patch::DCTERMS_CONFORMS_IRI;
    use aruna_core::events::StorageEvent;
    use aruna_core::storage_entries::{iri_reference_key, materialization_status_entry};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_storage::FjallStorage;
    use aruna_storage::StorageHandle;

    const OBJECT: &str = "https://example.test/object";

    /// One backlink page read from `storage`, spending `budget` as the lookup does.
    async fn read_backlinks(
        storage: &StorageHandle,
        object_iri: &str,
        predicate_iri: Option<&str>,
        after: Option<Ulid>,
        limit: usize,
        budget: &mut LinkBudget,
    ) -> Result<BacklinkPage, BacklinkError> {
        let read = BacklinksOperation::new((object_iri, predicate_iri), (after, limit), *budget);
        let context = DriverContext::storage_only(storage.clone());
        let (page, left) = drive(read, &context).await?;
        *budget = left;
        Ok(page)
    }

    fn record(document_id: Ulid, cursor: Ulid, predicate: &str, object: &str) -> IriIndexRecord {
        IriIndexRecord {
            document_id,
            document_cursor: cursor,
            predicate_iri: predicate.to_string(),
            object_iri: object.to_string(),
            subject_iris: vec!["https://example.test/root".to_string()],
        }
    }

    async fn plan_iri_rows(
        storage: &StorageHandle,
        document_id: Ulid,
        records: &[IriIndexRecord],
    ) -> Result<LinkRows, MetadataIriError> {
        let rows = IriRowsOperation::new(document_id, records.to_vec());
        drive(rows, &DriverContext::storage_only(storage.clone())).await
    }

    async fn replace_rows(
        storage: &StorageHandle,
        record: &MetadataRegistryRecord,
        rows: LinkRows,
    ) -> Result<bool, MetadataIriError> {
        let replace = ReplaceRowsOperation::new(record.clone(), rows);
        drive(replace, &DriverContext::storage_only(storage.clone())).await
    }

    async fn lookup_iri_references(
        context: &DriverContext,
        predicate_iri: &str,
        object_iri: &str,
        limit: usize,
    ) -> Result<ReferenceMatches, MetadataIriError> {
        drive(
            ReferenceLookupOperation::new((predicate_iri, object_iri), limit),
            context,
        )
        .await
    }

    async fn write_document(
        storage: &StorageHandle,
        document_id: Ulid,
        records: &[IriIndexRecord],
    ) {
        let rows = plan_iri_rows(storage, document_id, records).await.unwrap();
        apply_rows(storage, rows).await.unwrap();
    }

    async fn row_exists(storage: &StorageHandle, key_space: &str, key: Vec<u8>) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    fn open_budget() -> LinkBudget {
        LinkBudget {
            seeks: 64,
            rows: 1024,
        }
    }

    fn registry_record(document_id: Ulid, cursor: Ulid) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let group_id = Ulid::from_parts(1, 1);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/indexed".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/indexed",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: cursor,
            last_event_id: cursor,
        }
    }

    #[test]
    fn groups_sorts_subjects() {
        let document_id = Ulid::from_parts(1, 1);
        let cursor = Ulid::from_parts(2, 1);
        let records = project_iri_references(
            document_id,
            cursor,
            vec![
                (
                    "https://example.test/subject-b".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
                (
                    "https://example.test/subject-a".to_string(),
                    DCTERMS_CONFORMS_IRI.to_string(),
                    "https://example.test/profile".to_string(),
                ),
            ],
        );

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, document_id);
        assert_eq!(records[0].document_cursor, cursor);
        assert_eq!(
            records[0].subject_iris,
            vec![
                "https://example.test/subject-a".to_string(),
                "https://example.test/subject-b".to_string(),
            ]
        );
    }

    #[test]
    fn lookup_fences_exact() {
        let current_document_id = Ulid::from_parts(1, 1);
        let stale_document_id = Ulid::from_parts(1, 2);
        let cursor = Ulid::from_parts(2, 1);
        let profile = "https://example.test/profile";
        let records = vec![
            IriIndexRecord {
                document_id: current_document_id,
                document_cursor: cursor,
                predicate_iri: DCTERMS_CONFORMS_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/current".to_string()],
            },
            IriIndexRecord {
                document_id: stale_document_id,
                document_cursor: Ulid::from_parts(2, 0),
                predicate_iri: DCTERMS_CONFORMS_IRI.to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/stale".to_string()],
            },
            IriIndexRecord {
                document_id: current_document_id,
                document_cursor: cursor,
                predicate_iri: "https://example.test/hash-collision".to_string(),
                object_iri: profile.to_string(),
                subject_iris: vec!["https://example.test/collision".to_string()],
            },
        ];
        let registry_records = vec![
            registry_record(current_document_id, cursor),
            registry_record(stale_document_id, cursor),
        ];

        let matches =
            collect_iri_matches(records, &registry_records, DCTERMS_CONFORMS_IRI, profile);

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches.get(&current_document_id),
            Some(&vec!["https://example.test/current".to_string()])
        );
    }

    #[tokio::test]
    async fn lookup_reads_points() {
        // Candidates come from backlink rows and point reads, capped, without the registry list.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(2, 1);
        let (first, stale, last) = (
            Ulid::from_parts(5, 1),
            Ulid::from_parts(5, 2),
            Ulid::from_parts(5, 3),
        );
        for (doc, posted) in [
            (first, cursor),
            (stale, Ulid::from_parts(1, 1)),
            (last, cursor),
        ] {
            write_document(&storage, doc, &[record(doc, posted, "p", OBJECT)]).await;
            let registry = registry_record(doc, cursor);
            let write = StorageEffect::Write {
                key_space: aruna_core::keyspaces::DOCUMENT_INDEX_KEYSPACE.to_string(),
                key: aruna_core::storage_entries::metadata_document_key(doc),
                value: postcard::to_allocvec(&registry).unwrap().into(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let (found, matches) = lookup_iri_references(&context, "p", OBJECT, 8)
            .await
            .unwrap();
        let ids: Vec<Ulid> = found.iter().map(|record| record.document_id).collect();
        assert_eq!(ids, [first, last], "a stale posting names no document");
        assert_eq!(matches.len(), 2);
        let (capped, _) = lookup_iri_references(&context, "p", OBJECT, 1)
            .await
            .unwrap();
        assert_eq!(capped.len(), 1);
    }

    #[tokio::test]
    async fn replaces_old_postings() {
        // A new revision deletes the rows it does not write again, with their document rows.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let doc = Ulid::from_parts(5, 1);
        let first = Ulid::from_parts(1, 1);
        let second = Ulid::from_parts(2, 1);
        write_document(
            &storage,
            doc,
            &[record(doc, first, "p", "o1"), record(doc, first, "p", "o2")],
        )
        .await;
        write_document(&storage, doc, &[record(doc, second, "p", "o2")]).await;

        let dropped = iri_reference_key("p", "o1", doc).to_vec();
        let kept = iri_reference_key("p", "o2", doc).to_vec();
        assert!(!row_exists(&storage, IRI_INDEX_KEYSPACE, dropped.clone()).await);
        assert!(!row_exists(&storage, DOCUMENT_IRI_KEYSPACE, document_key(doc, &dropped)).await);
        assert!(row_exists(&storage, DOCUMENT_IRI_KEYSPACE, document_key(doc, &kept)).await);
        let page = read_backlinks(&storage, "o2", None, None, 10, &mut open_budget())
            .await
            .unwrap();
        assert_eq!(page.documents[&doc][0].document_cursor, second);
    }

    #[tokio::test]
    async fn reads_object_prefix() {
        // More than a registry's worth of rows under other objects: a predicate-less lookup
        // still costs a few seeks and only the object's own rows.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let noise: Vec<_> = (0..1100u64)
            .map(|index| {
                let doc = Ulid::from_parts(100, index.into());
                record(
                    doc,
                    cursor,
                    "p",
                    &format!("https://example.test/other/{index}"),
                )
            })
            .collect();
        let rows = LinkRows {
            writes: noise
                .iter()
                .map(|record| iri_reference_entry(record).unwrap())
                .collect(),
            deletes: Vec::new(),
        };
        apply_rows(&storage, rows).await.unwrap();
        let doc = Ulid::from_parts(7, 1);
        write_document(
            &storage,
            doc,
            &[
                record(doc, cursor, "p1", OBJECT),
                record(doc, cursor, "p2", OBJECT),
            ],
        )
        .await;

        let mut budget = LinkBudget { seeks: 5, rows: 4 };
        let page = read_backlinks(&storage, OBJECT, None, None, 10, &mut budget)
            .await
            .unwrap();

        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[&doc].len(), 2);
        assert_eq!(page.next, None);
        assert_eq!(budget.rows, 2);
    }

    #[tokio::test]
    async fn pages_after_document() {
        // Documents come in id order across predicates; `next` continues after the last one.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let docs: Vec<_> = (1..=5u128)
            .map(|index| Ulid::from_parts(9, index))
            .collect();
        for (index, doc) in docs.iter().enumerate() {
            let predicate = if index % 2 == 0 { "p1" } else { "p2" };
            let mut records = vec![record(*doc, cursor, predicate, OBJECT)];
            if index == 2 {
                records.push(record(*doc, cursor, "p2", OBJECT));
            }
            write_document(&storage, *doc, &records).await;
        }

        let mut budget = open_budget();
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = read_backlinks(&storage, OBJECT, None, after, 2, &mut budget)
                .await
                .unwrap();
            assert!(page.documents.len() <= 2);
            seen.extend(page.documents.keys().copied());
            if let Some(both) = page.documents.get(&docs[2]) {
                assert_eq!(both.len(), 2);
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        assert_eq!(seen, docs);

        let filtered = read_backlinks(&storage, OBJECT, Some("p2"), None, 10, &mut budget)
            .await
            .unwrap();
        let ids: Vec<_> = filtered.documents.keys().copied().collect();
        assert_eq!(ids, vec![docs[1], docs[2], docs[3]]);
    }

    #[tokio::test]
    async fn budget_ends_lookup() {
        // An exhausted budget fails the lookup instead of returning a partial page.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let doc = Ulid::from_parts(3, 1);
        let cursor = Ulid::from_parts(1, 1);
        write_document(&storage, doc, &[record(doc, cursor, "p", OBJECT)]).await;

        let mut budget = LinkBudget { seeks: 1, rows: 8 };
        let error = read_backlinks(&storage, OBJECT, None, None, 10, &mut budget)
            .await
            .expect_err("two seeks are needed");
        assert!(matches!(error, BacklinkError::BudgetExhausted));
    }

    #[tokio::test]
    async fn decode_error_classified() {
        // A corrupt row must stay a conversion failure: callers charge those to
        // the document, while storage failures must never spend that budget.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let key = iri_reference_key("p", OBJECT, Ulid::from_parts(1, 1));
        let rows = LinkRows {
            writes: vec![(
                IRI_INDEX_KEYSPACE.to_string(),
                key,
                ByteView::from(vec![1u8]),
            )],
            deletes: Vec::new(),
        };
        apply_rows(&storage, rows).await.unwrap();

        let error = read_backlinks(&storage, OBJECT, None, None, 10, &mut open_budget())
            .await
            .expect_err("malformed row fails to decode");
        assert!(matches!(error, BacklinkError::Malformed(_)));
    }

    #[test]
    fn freshness_of_documents() {
        // Only the documents passed in count; a pending one elsewhere does not.
        let cursor = Ulid::from_parts(1, 1);
        let current = registry_record(Ulid::from_parts(4, 1), cursor);
        let pending = registry_record(Ulid::from_parts(4, 2), cursor);
        let status = |record: &MetadataRegistryRecord, state| MaterializationStatusRecord {
            document_id: record.document_id,
            event_id: cursor,
            graph_iri: record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 5,
        };
        let value = |record, state| {
            let bytes = postcard::to_allocvec(&status(record, state)).unwrap();
            Some(ByteView::from(bytes))
        };
        let materialized = value(&current, MaterializationState::Materialized);
        let returned =
            iri_freshness(std::slice::from_ref(&current), vec![materialized.clone()]).unwrap();
        assert_eq!(returned.state, IriFreshnessState::Current);
        assert_eq!(returned.oldest_status_updated, Some(5));
        let values = vec![materialized, value(&pending, MaterializationState::Pending)];
        let both = iri_freshness(&[current.clone(), pending.clone()], values).unwrap();
        assert_eq!(status_rows(&[current, pending]).len(), 2);
        assert_eq!(both.state, IriFreshnessState::Mixed);
    }

    #[tokio::test]
    async fn rebuild_skips_newer() {
        // A rebuild commits a document's rows only while its status still names the event.
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let cursor = Ulid::from_parts(1, 1);
        let record_at = registry_record(Ulid::from_parts(6, 1), cursor);
        let doc = record_at.document_id;
        let rows = plan_iri_rows(&storage, doc, &[record(doc, cursor, "p", OBJECT)])
            .await
            .unwrap();

        assert!(!replace_rows(&storage, &record_at, rows).await.unwrap());
        let key = iri_reference_key("p", OBJECT, doc).to_vec();
        assert!(!row_exists(&storage, IRI_INDEX_KEYSPACE, key.clone()).await);

        let status = MaterializationStatusRecord {
            document_id: doc,
            event_id: cursor,
            graph_iri: record_at.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state: MaterializationState::Materialized,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        let status_rows = LinkRows {
            writes: vec![materialization_status_entry(&status).unwrap()],
            deletes: Vec::new(),
        };
        apply_rows(&storage, status_rows).await.unwrap();
        let rows = plan_iri_rows(&storage, doc, &[record(doc, cursor, "p", OBJECT)])
            .await
            .unwrap();
        assert!(replace_rows(&storage, &record_at, rows).await.unwrap());
        assert!(row_exists(&storage, IRI_INDEX_KEYSPACE, key).await);
    }

    fn rows(records: &[IriIndexRecord]) -> IoResult {
        let values = (records.iter())
            .map(|record| {
                let (_, key, value) = iri_reference_entry(record).unwrap();
                (key, value)
            })
            .collect();
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })))
    }

    #[test]
    fn predicate_reads_once() {
        let budget = LinkBudget { seeks: 2, rows: 8 };
        let mut read = BacklinksOperation::new((OBJECT, Some("p")), (None, 10), budget);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let document = Ulid::from_parts(1, 1);
        let found = record(document, Ulid::from_parts(2, 2), "p", OBJECT);
        let other = record(
            Ulid::from_parts(3, 3),
            Ulid::from_parts(2, 2),
            "p",
            "elsewhere",
        );
        let Ok(Next::Done((page, left))) = read.next(rows(&[found.clone(), other])) else {
            panic!("expected the page");
        };
        // A hash collision on another object is dropped.
        assert_eq!(page.documents, BTreeMap::from([(document, vec![found])]));
        assert_eq!(page.next, None);
        assert_eq!(left, LinkBudget { seeks: 1, rows: 6 });
    }

    #[test]
    fn spent_budget_refuses() {
        let budget = LinkBudget { seeks: 0, rows: 8 };
        let mut read = BacklinksOperation::new((OBJECT, None), (None, 10), budget);
        assert_eq!(read.first(), Err(BacklinkError::BudgetExhausted));
    }

    #[test]
    fn empty_object_ends() {
        let budget = LinkBudget { seeks: 4, rows: 8 };
        let mut read = BacklinksOperation::new((OBJECT, None), (None, 10), budget);
        read.first().unwrap();
        let Ok(Next::Done((page, _))) = read.next(rows(&[])) else {
            panic!("expected an empty page");
        };
        assert_eq!(page, BacklinkPage::default());
    }

    fn materialized(record: &MetadataRegistryRecord) -> ByteView {
        let status = MaterializationStatusRecord {
            document_id: record.document_id,
            event_id: record.last_event_id,
            graph_iri: record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state: MaterializationState::Materialized,
            attempts: 1,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        ByteView::from(postcard::to_allocvec(&status).unwrap())
    }

    #[test]
    fn iri_rows_replace() {
        let document_id = Ulid::from_parts(1, 1);
        let fresh = record(document_id, Ulid::from_parts(2, 1), "p", OBJECT);
        let mut rows = IriRowsOperation::new(document_id, vec![fresh.clone()]);
        assert!(runs_child(rows.first()));
        let stale = vec![9u8; 8];
        let existing = child(Ok::<_, DataLinkError>(vec![stale.clone()]));
        let Ok(Next::Done(planned)) = rows.next(existing) else {
            panic!("expected the planned rows");
        };
        let (key_space, key, value) = iri_reference_entry(&fresh).unwrap();
        assert!(planned.writes.contains(&(key_space, key, value)));
        let stale_row = (
            IRI_INDEX_KEYSPACE.to_string(),
            ByteView::from(stale.clone()),
        );
        assert!(planned.deletes.contains(&stale_row));
        let document_row = document_key(document_id, &stale).into();
        assert!((planned.deletes).contains(&(DOCUMENT_IRI_KEYSPACE.to_string(), document_row)));
    }

    #[test]
    fn projection_reads_references() {
        let document_id = Ulid::from_parts(1, 1);
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let document = (document_id, Ulid::from_parts(2, 1), graph_iri.clone());
        let mut project = ProjectRowsOperation::new(document);
        let effect = Effect::Metadata(MetadataEffect::IriReferences { graph_iri });
        assert_eq!(
            project.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(effect))))
        );
        let references = MetadataEvent::IriReferences {
            references: Vec::new(),
        };
        let answered = IoResult::Event(Box::new(Event::Metadata(references)));
        assert!(runs_child(project.next(answered)));
        let links = child(Ok::<_, DataLinkError>(
            BTreeMap::<DataLink, LinkPosting>::new(),
        ));
        assert!(runs_child(project.next(links)));
        assert!(runs_child(
            project.next(child(Ok::<_, DataLinkError>(LinkRows::default())))
        ));
        let iris = child(Ok::<_, MetadataIriError>(LinkRows::default()));
        assert_eq!(project.next(iris), Ok(Next::Done((LinkRows::default(), 0))));
    }

    #[test]
    fn projection_error_unexpected() {
        let document_id = Ulid::from_parts(1, 1);
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let mut project = ProjectRowsOperation::new((document_id, document_id, graph_iri));
        project.first().unwrap();
        let failed = MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::GraphNotFound,
        };
        let answered = IoResult::Event(Box::new(Event::Metadata(failed)));
        assert!(matches!(
            project.next(answered),
            Err(MetadataIriError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn replace_needs_current() {
        let record = registry_record(Ulid::from_parts(1, 1), Ulid::from_parts(2, 1));
        let mut replace = ReplaceRowsOperation::new(record.clone(), LinkRows::default());
        assert_eq!(replace.transaction(), TransactionMode::Write);
        let read = IoCall::Read(status_rows(std::slice::from_ref(&record)));
        assert_eq!(replace.first(), Ok(Next::Call(read)));
        assert_eq!(
            replace.next(IoResult::Values(vec![None])),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn replace_writes_first() {
        let record = registry_record(Ulid::from_parts(1, 1), Ulid::from_parts(2, 1));
        let write = (
            IRI_INDEX_KEYSPACE.to_string(),
            ByteView::from(vec![1]),
            ByteView::from(vec![2]),
        );
        let delete = (IRI_INDEX_KEYSPACE.to_string(), ByteView::from(vec![3]));
        let rows = LinkRows {
            writes: vec![write.clone()],
            deletes: vec![delete.clone()],
        };
        let mut replace = ReplaceRowsOperation::new(record.clone(), rows);
        replace.first().unwrap();
        let current = IoResult::Values(vec![Some(materialized(&record))]);
        assert_eq!(
            replace.next(current),
            Ok(Next::Call(IoCall::Write(vec![write])))
        );
        assert_eq!(
            replace.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(vec![delete])))
        );
        assert_eq!(replace.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn rebuild_empty_registry() {
        let mut rebuild = RebuildIrisOperation::new();
        let page = IoCall::Page {
            space: METADATA_INDEX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: INDEX_PAGE_SIZE,
        };
        assert_eq!(rebuild.first(), Ok(Next::Call(page)));
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(runs_child(rebuild.next(empty)));
        let live = child(Ok::<_, LiveRecordsError>(
            Vec::<MetadataRegistryRecord>::new(),
        ));
        assert_eq!(rebuild.next(live), Ok(Next::Call(IoCall::Read(Vec::new()))));
        assert_eq!(
            rebuild.next(IoResult::Values(Vec::new())),
            Ok(Next::Done(0))
        );
    }

    #[test]
    fn rebuild_counts_postings() {
        let record = registry_record(Ulid::from_parts(1, 1), Ulid::from_parts(2, 1));
        let mut rebuild = RebuildIrisOperation::new();
        rebuild.first().unwrap();
        let row = (
            Key::from(vec![1]),
            ByteView::from(postcard::to_allocvec(&record).unwrap()),
        );
        let page = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        rebuild.next(page).unwrap();
        let live = child(Ok::<_, LiveRecordsError>(vec![record.clone()]));
        rebuild.next(live).unwrap();
        let statuses = IoResult::Values(vec![Some(materialized(&record))]);
        assert!(runs_child(rebuild.next(statuses)));
        let projected = child(Ok::<_, MetadataIriError>((LinkRows::default(), 3usize)));
        assert!(runs_child(rebuild.next(projected)));
        let replaced = child(Ok::<_, MetadataIriError>(true));
        assert_eq!(rebuild.next(replaced), Ok(Next::Done(3)));
    }

    #[test]
    fn lookup_empty_page() {
        let mut lookup = ReferenceLookupOperation::new(("p", OBJECT), 10);
        assert!(runs_child(lookup.first()));
        let page = (BacklinkPage::default(), open_budget());
        let answered = child(Ok::<_, BacklinkError>(page));
        assert_eq!(
            lookup.next(answered),
            Ok(Next::Done((Vec::new(), BTreeMap::new())))
        );
    }

    #[test]
    fn lookup_budget_ends() {
        let mut lookup = ReferenceLookupOperation::new(("p", OBJECT), 10);
        lookup.first().unwrap();
        let spent = child(Err::<(BacklinkPage, LinkBudget), _>(
            BacklinkError::BudgetExhausted,
        ));
        assert_eq!(
            lookup.next(spent),
            Ok(Next::Done((Vec::new(), BTreeMap::new())))
        );
    }
}
