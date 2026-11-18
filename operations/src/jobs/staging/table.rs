//! Reads a staging job's table in bounded chunks: file rows are registered as references with
//! their row metadata, folder rows get a folder object and wait in the directory queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{AuthorizationError, StagingSourceError};
use aruna_core::events::Event;
use aruna_core::jobs::{ByteStream, GzipBroken, GzipWork, JobEffect, JobEvent};
use aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    StagingJobCheckpoint, StagingJobDirectory, StagingJobPhase, TableRead,
};
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::structs::execution::table::{
    GzipCursor, RowKind, RowMapper, RowOutcome, TABLE_CHUNK, TableEntry, TableError, TableImport,
    TableSource, is_gzip, parse_rows, read_header, source_path,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::routing::{RoutingError, RoutingSnapshot};
use aruna_core::types::{Key, KeySpace, Value};
use bytes::Bytes;
use byteview::ByteView;
use ulid::Ulid;

use super::limiter::{in_turns, retried_call, retried_result, turns_result};
use super::manifest::{lasting_head, lasting_read};
use super::page::ImportPageOperation;
use super::walk::{
    StagedFile, Turns, WalkPlace, authority_call, key_turns, own_ranges, read_check_call,
    record_failure, rules_call, rules_result, source_permission_path,
};
use super::{ItemFailure, PageFailure, StagingRun, committed, pinned, queue_key};
use crate::auth::permission_rules::PermissionRules;
use crate::connectors::get_connector::{
    GetSourceError, GetSourceInput, GetSourceOperation, GetSourceResult,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::placement::policy::gate::PolicyGateError;
use crate::placement::policy::read::ReadPolicyError;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::replication::queue::{
    BlobQueueError, LiveVersionInput, LiveVersionOperation, QueueBlobResult,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult, ObjectRangeRequest};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use crate::s3::object::read::GetRoutedOperation;
use crate::s3::purge_fence::PurgeFenceError;
use crate::s3::usage::QuotaGateError;
use crate::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation, HeadSourceResult,
};
use crate::staging::page::rows::MAX_PAGE_ENTRIES;
use crate::staging::read_source::{
    ReadSourceError, ReadSourceInput, ReadSourceOperation, ReadSourceResult,
};

/// Directories the queue holds before the table is read further.
const QUEUE_PAUSE: u64 = 1024;
/// Rows one step maps, so one page and its HEADs stay bounded.
const STEP_ROWS: usize = 1000;
/// Table bytes one step of record rows reads; they need no HEAD, so their pages are larger.
const RECORD_WINDOW: usize = 4 << 20;

/// The rows one step maps and the table bytes it reads for them.
fn step_size(import: &TableImport) -> (usize, usize) {
    match import.rows.kind {
        RowKind::Record => (MAX_PAGE_ENTRIES, RECORD_WINDOW),
        RowKind::File | RowKind::Folder => (STEP_ROWS, TABLE_CHUNK),
    }
}

/// Whether the job reads its table next: the table is open, no directory is being listed
/// and the queue has room.
pub(super) fn reads_table(checkpoint: &StagingJobCheckpoint) -> bool {
    checkpoint.table.as_ref().is_some_and(|table| !table.done)
        && checkpoint.walk.is_none()
        && checkpoint.queue_tail.saturating_sub(checkpoint.queue_head) < QUEUE_PAUSE
}

/// One window of the table and what the read learned about it.
#[derive(Debug, PartialEq)]
pub(super) struct Chunk {
    bytes: Vec<u8>,
    complete: bool,
    read: TableRead,
}

#[derive(Debug, PartialEq)]
pub(super) enum ChunkFailure {
    /// The table changed between chunks, so it is read again from its start.
    Changed,
    /// The table cannot be read; the job records it and ends the table.
    Lasting(String),
    Retry(String),
}

impl From<IoError> for ChunkFailure {
    fn from(error: IoError) -> Self {
        Self::Retry(error.to_string())
    }
}

/// A gzip table's open stream and decoder, kept across the steps of one job run so the table
/// is decoded once instead of from its start for every chunk.
#[derive(Debug, PartialEq)]
pub(super) struct GzipStream {
    version_id: Option<Ulid>,
    observation: Option<SourceMetadata>,
    work: GzipWork,
}

/// A chunk that was read, and the stream to keep for the next step.
#[derive(Debug, PartialEq)]
pub(super) struct ChunkRead {
    chunk: Chunk,
    kept: Option<GzipStream>,
}

/// Where the table's bytes come from.
#[derive(Debug, PartialEq)]
enum Origin {
    Connector {
        path: String,
    },
    Object {
        bucket: String,
        key: String,
        group_id: Option<Ulid>,
    },
}

#[derive(Debug, PartialEq)]
enum ChunkStep {
    Bucket,
    Allowed,
    Head,
    OpenRange,
    OpenGzip { reused: bool },
    Fill { reused: bool },
    Collect { end: u64, size: Option<u64> },
}

/// Reads the next window of a table: below the read connector, every chunk bound to its first
/// observation, or an uploaded object, read as the submitter and pinned to its first version.
#[derive(Debug, PartialEq)]
pub(super) struct ChunkReadOperation {
    run: StagingRun,
    origin: Origin,
    read: TableRead,
    kept: Option<GzipStream>,
    observation: Option<SourceMetadata>,
    gzip: bool,
    window: usize,
    opened: Option<Ulid>,
    step: ChunkStep,
}

impl ChunkReadOperation {
    fn new(
        run: StagingRun,
        origin: Origin,
        read: TableRead,
        kept: Option<GzipStream>,
        window: usize,
    ) -> Self {
        Self {
            run,
            origin,
            read,
            kept,
            observation: None,
            gzip: false,
            window,
            opened: None,
            step: ChunkStep::Allowed,
        }
    }

    fn import(&self) -> Result<&TableImport, ChunkFailure> {
        (self.run.spec.table.as_ref())
            .ok_or_else(|| ChunkFailure::Retry("the job has no table".to_string()))
    }

    fn name(&self) -> &str {
        match &self.origin {
            Origin::Connector { path } => path,
            Origin::Object { key, .. } => key,
        }
    }

    fn done(&mut self, bytes: Vec<u8>, complete: bool) -> Next<ChunkRead> {
        Next::Done(ChunkRead {
            chunk: Chunk {
                bytes,
                complete,
                read: self.read.clone(),
            },
            kept: self.kept.take(),
        })
    }

    /// Decides the table's kind, then reads its bytes.
    fn start(&mut self) -> Result<Next<ChunkRead>, ChunkFailure> {
        let format = &self.import()?.format;
        self.gzip = self
            .read
            .gzip
            .unwrap_or_else(|| is_gzip(format, self.name(), &[]));
        self.proceed()
    }

    fn proceed(&mut self) -> Result<Next<ChunkRead>, ChunkFailure> {
        if self.gzip {
            return self.gzip_chunk();
        }
        let start = self.read.offset;
        match &self.origin {
            Origin::Connector { .. } => {
                let length = self.observation()?.content_length;
                let end = (start + self.window as u64).min(length);
                if start >= end {
                    return Ok(self.done(Vec::new(), true));
                }
                self.step = ChunkStep::OpenRange;
                Ok(Next::Call(self.source_call(Some(start..end))?))
            }
            Origin::Object { .. } => {
                let last = start + self.window as u64 - 1;
                self.step = ChunkStep::OpenRange;
                Ok(Next::Call(self.object_call(Some((start, last)))?))
            }
        }
    }

    fn observation(&self) -> Result<&SourceMetadata, ChunkFailure> {
        (self.observation.as_ref())
            .ok_or_else(|| ChunkFailure::Retry("the table has no observation".to_string()))
    }

    /// The connector table's bytes in `range`, or all of them, bound to its observation.
    fn source_call(&self, range: Option<std::ops::Range<u64>>) -> Result<IoCall, ChunkFailure> {
        let spec = &self.run.spec;
        let input = ReadSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: self.name().to_string(),
            range,
            expected: Some(self.observation()?.clone()),
        };
        let transient: fn(&ReadSourceError) -> bool = |error| !lasting_read(error);
        Ok(retried_call(
            &self.run,
            input,
            ReadSourceOperation::new,
            transient,
        ))
    }

    fn object_call(&self, range: Option<(u64, u64)>) -> Result<IoCall, ChunkFailure> {
        let Origin::Object {
            bucket,
            key,
            group_id,
        } = &self.origin
        else {
            return Err(ChunkFailure::Retry("the table is no object".to_string()));
        };
        let group_id = group_id.ok_or_else(|| ChunkFailure::Retry("no bucket".to_string()))?;
        let spec = &self.run.spec;
        let auth = &spec.auth_context;
        let input = GetObjectInput {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: self.read.version_id,
            range: range.map(|(start, end)| ObjectRangeRequest::StartEnd { start, end }),
            group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: spec.node_id,
        };
        let restrictions = auth.path_restrictions.clone();
        let read = GetRoutedOperation::new(input, restrictions, None);
        Ok(IoCall::child(read))
    }

    /// The decompressed chunk, read on from a stream an earlier step left open at this version,
    /// observation and offset, or from a new stream skipped to the offset once.
    fn gzip_chunk(&mut self) -> Result<Next<ChunkRead>, ChunkFailure> {
        let offset = self.read.offset;
        let (version_id, observation) = (self.read.version_id, self.observation.clone());
        let kept = (self.kept.take())
            .filter(|kept| kept.version_id == version_id && kept.observation == observation)
            .and_then(|mut kept| kept.work.cursor.advance(offset).then_some(kept));
        match kept {
            Some(kept) => {
                self.step = ChunkStep::Fill { reused: true };
                let version = kept.version_id;
                let observation = kept.observation;
                self.kept = Some(GzipStream {
                    version_id: version,
                    observation,
                    work: empty_work(),
                });
                Ok(Next::Call(job_call(JobEffect::FillGzip(kept.work))))
            }
            None => self.open_gzip(),
        }
    }

    fn open_gzip(&mut self) -> Result<Next<ChunkRead>, ChunkFailure> {
        self.kept = None;
        self.step = ChunkStep::OpenGzip { reused: false };
        let call = match self.origin {
            Origin::Connector { .. } => self.source_call(None)?,
            Origin::Object { .. } => self.object_call(None)?,
        };
        Ok(Next::Call(call))
    }

    /// A stream the source just opened, as the connector or the object store answered it.
    fn opened(&mut self, result: IoResult) -> Result<OpenedTable, ChunkFailure> {
        match self.origin {
            Origin::Connector { .. } => {
                let seen = self.observation()?;
                match retried_result::<ReadSourceResult, ReadSourceError>(result)? {
                    Ok(read) if read.metadata.compare(seen) == ObservationMatch::Changed => {
                        Err(ChunkFailure::Changed)
                    }
                    Ok(read) => Ok(OpenedTable::Stream(read.stream, None, None)),
                    Err(ReadSourceError::Staging(StagingSourceError::ConditionFailed)) => {
                        Err(ChunkFailure::Changed)
                    }
                    Err(error) if lasting_read(&error) => {
                        Err(ChunkFailure::Lasting(format!("table read failed: {error}")))
                    }
                    Err(error) => Err(ChunkFailure::Retry(error.to_string())),
                }
            }
            Origin::Object { .. } => match result.child::<GetObjectResult, GetObjectError>()? {
                Ok(found) => Ok(OpenedTable::Stream(
                    found.blob,
                    found.resolved_version_id.or(found.version_id),
                    Some(found.info.size),
                )),
                // A range from the table's end on reads nothing.
                Err(GetObjectError::InvalidRange) => Ok(OpenedTable::Past),
                Err(
                    error @ (GetObjectError::NoSuchKey
                    | GetObjectError::NoSuchVersion
                    | GetObjectError::DeleteMarker),
                ) => Err(ChunkFailure::Lasting(format!("table unavailable: {error}"))),
                Err(error) => Err(ChunkFailure::Retry(error.to_string())),
            },
        }
    }

    fn fill(&mut self, stream: ByteStream, version: Option<Ulid>) -> Next<ChunkRead> {
        let version_id = self.read.version_id.or(version);
        self.opened = version;
        let cursor = GzipCursor::new(self.read.offset, self.window);
        self.kept = Some(GzipStream {
            version_id,
            observation: self.observation.clone(),
            work: empty_work(),
        });
        self.step = ChunkStep::Fill { reused: false };
        Next::Call(job_call(JobEffect::FillGzip(GzipWork { stream, cursor })))
    }

    fn collected(
        &mut self,
        result: IoResult,
        end: u64,
        size: Option<u64>,
    ) -> Result<Next<ChunkRead>, ChunkFailure> {
        let event = result.event()?;
        let Event::Job(JobEvent::Collected(bytes)) = event else {
            return Err(IoError::Unexpected {
                expected: "the collected table bytes",
                got: event.label(),
            }
            .into());
        };
        let bytes = bytes.map_err(ChunkFailure::Retry)?;
        let format = &self.import()?.format;
        // The first chunk decides by its magic bytes whether a table is compressed.
        if self.read.gzip.is_none() && is_gzip(format, self.name(), &bytes) {
            self.gzip = true;
            return self.proceed();
        }
        self.read.gzip = Some(false);
        let complete = match size {
            Some(size) => self.read.offset + bytes.len() as u64 >= size,
            None => end == self.observation()?.content_length,
        };
        Ok(self.done(bytes, complete))
    }
}

/// Holds a kept stream's place while its work is out at the adapter.
fn empty_work() -> GzipWork {
    GzipWork {
        stream: BackendStream::new(futures_util::stream::empty::<Result<Bytes, StreamError>>()),
        cursor: GzipCursor::new(0, 0),
    }
}

/// What opening a table stream gave.
enum OpenedTable {
    Stream(ByteStream, Option<Ulid>, Option<u64>),
    /// The range starts at or after the table's end.
    Past,
}

fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

impl Operation for ChunkReadOperation {
    type Output = ChunkRead;
    type Error = ChunkFailure;

    fn first(&mut self) -> Result<Next<ChunkRead>, ChunkFailure> {
        match &self.origin {
            Origin::Connector { path } => {
                let spec = &self.run.spec;
                let allowed = source_permission_path(spec, path);
                Ok(Next::Call(read_check_call(spec, allowed)))
            }
            Origin::Object { bucket, .. } => {
                self.step = ChunkStep::Bucket;
                let bucket = GetBucketOperation::new(bucket.clone());
                Ok(Next::Call(IoCall::child(bucket)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ChunkRead>, ChunkFailure> {
        match std::mem::replace(&mut self.step, ChunkStep::Allowed) {
            ChunkStep::Bucket => {
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => info,
                    Err(GetBucketError::NotFound) => {
                        return Err(ChunkFailure::Lasting(
                            "the table's bucket does not exist".to_string(),
                        ));
                    }
                    Err(error) => return Err(ChunkFailure::Retry(error.to_string())),
                };
                let spec = self.run.spec.clone();
                let Origin::Object {
                    bucket,
                    key,
                    group_id,
                } = &mut self.origin
                else {
                    return Err(ChunkFailure::Retry("the table is no object".to_string()));
                };
                *group_id = Some(info.group_id);
                let auth = &spec.auth_context;
                let path =
                    object_permission_path(auth.realm_id, info.group_id, spec.node_id, bucket, key);
                Ok(Next::Call(read_check_call(&spec, path)))
            }
            ChunkStep::Allowed => {
                let allowed = (result.child::<bool, AuthorizationError>()?)
                    .map_err(|error| ChunkFailure::Retry(error.to_string()))?;
                if !allowed {
                    return Err(ChunkFailure::Lasting("table read denied".to_string()));
                }
                if matches!(self.origin, Origin::Object { .. }) {
                    return self.start();
                }
                match self.read.observation.clone() {
                    Some(observation) => {
                        self.observation = Some(observation);
                        self.start()
                    }
                    None => {
                        self.step = ChunkStep::Head;
                        let spec = &self.run.spec;
                        let input = HeadSourceInput {
                            group_id: spec.group_id,
                            connector_id: spec.connector_id,
                            source_path: self.name().to_string(),
                        };
                        let transient: fn(&HeadSourceError) -> bool = |error| !lasting_head(error);
                        let head =
                            retried_call(&self.run, input, HeadSourceOperation::new, transient);
                        Ok(Next::Call(head))
                    }
                }
            }
            ChunkStep::Head => match retried_result::<HeadSourceResult, HeadSourceError>(result)? {
                Ok(head) => {
                    self.read.observation = Some(head.metadata.clone());
                    self.observation = Some(head.metadata);
                    self.start()
                }
                Err(error) if lasting_head(&error) => {
                    Err(ChunkFailure::Lasting(format!("table unavailable: {error}")))
                }
                Err(error) => Err(ChunkFailure::Retry(error.to_string())),
            },
            ChunkStep::OpenRange => {
                let start = self.read.offset;
                match self.opened(result)? {
                    OpenedTable::Past => Ok(self.done(Vec::new(), true)),
                    OpenedTable::Stream(stream, version, size) => {
                        self.read.version_id = self.read.version_id.or(version);
                        let end = match &self.origin {
                            Origin::Connector { .. } => {
                                let length = self.observation()?.content_length;
                                (start + self.window as u64).min(length)
                            }
                            Origin::Object { .. } => 0,
                        };
                        self.step = ChunkStep::Collect { end, size };
                        Ok(Next::Call(job_call(JobEffect::Collect(stream))))
                    }
                }
            }
            ChunkStep::OpenGzip { .. } => match self.opened(result)? {
                OpenedTable::Past => Ok(self.done(Vec::new(), true)),
                OpenedTable::Stream(stream, version, _) => Ok(self.fill(stream, version)),
            },
            ChunkStep::Fill { reused } => {
                let event = result.event()?;
                let Event::Job(JobEvent::GzipFilled {
                    work,
                    result: filled,
                }) = event
                else {
                    return Err(IoError::Unexpected {
                        expected: "the filled gzip chunk",
                        got: event.label(),
                    }
                    .into());
                };
                match filled {
                    Ok(()) => {
                        let (bytes, complete) = work.cursor.chunk();
                        if let Some(kept) = self.kept.as_mut() {
                            kept.work = work;
                        }
                        self.read.version_id = self.read.version_id.or(self.opened.take());
                        self.read.gzip = Some(true);
                        Ok(self.done(bytes, complete))
                    }
                    // A stream kept between steps may have been closed or its source rewritten
                    // meanwhile; opening it again checks the source like the first read did.
                    Err(_) if reused => self.open_gzip(),
                    Err(GzipBroken::Stream(error)) => Err(ChunkFailure::Retry(error)),
                    Err(GzipBroken::Gzip(error)) => Err(ChunkFailure::Lasting(error)),
                }
            }
            ChunkStep::Collect { end, size } => self.collected(result, end, size),
        }
    }
}

/// The checkpoint a table step left, and the stream kept for the next one.
#[derive(Debug, PartialEq)]
pub(super) struct TableStepEnd {
    pub(super) checkpoint: StagingJobCheckpoint,
    pub(super) kept: Option<GzipStream>,
}

#[derive(Debug, PartialEq)]
enum TableStep {
    Connector,
    Chunk,
    Commit,
    Rows,
}

/// Reads the next chunk of the table: its header first, then rows, and commits their rows
/// with the checkpoint.
#[derive(Debug, PartialEq)]
pub(super) struct TableChunkOperation {
    run: StagingRun,
    next: StagingJobCheckpoint,
    base: Option<String>,
    kept: Option<GzipStream>,
    step: TableStep,
}

impl TableChunkOperation {
    pub(super) fn new(
        run: StagingRun,
        checkpoint: &StagingJobCheckpoint,
        kept: Option<GzipStream>,
    ) -> Self {
        let mut next = checkpoint.clone();
        next.phase = StagingJobPhase::Discovering;
        next.current_path = run.spec.table.as_ref().map(table_name);
        Self {
            run,
            next,
            base: None,
            kept,
            step: TableStep::Connector,
        }
    }

    fn import(&self) -> Result<TableImport, PageFailure> {
        (self.run.spec.table.clone())
            .ok_or_else(|| PageFailure::Retry("the job has no table".to_string()))
    }

    fn done(&mut self, checkpoint: StagingJobCheckpoint) -> Next<TableStepEnd> {
        Next::Done(TableStepEnd {
            checkpoint,
            kept: self.kept.take(),
        })
    }

    fn commit(&mut self) -> Result<Next<TableStepEnd>, PageFailure> {
        self.step = TableStep::Commit;
        let call = (self.run)
            .commit_call(None, &self.next, Vec::new(), Vec::new())
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }

    fn read_chunk(&mut self) -> Result<Next<TableStepEnd>, PageFailure> {
        let import = self.import()?;
        let Some(read) = self.next.table.clone() else {
            return Ok(self.done(self.next.clone()));
        };
        let origin = match &import.source {
            TableSource::Connector(path) => Origin::Connector {
                path: source_path(path, self.base.as_deref()).map_err(PageFailure::Permanent)?,
            },
            TableSource::Object { bucket, key } => Origin::Object {
                bucket: bucket.clone(),
                key: key.clone(),
                group_id: None,
            },
        };
        self.step = TableStep::Chunk;
        let window = step_size(&import).1;
        let kept = self.kept.take();
        let chunk = ChunkReadOperation::new(self.run.clone(), origin, *read, kept, window);
        Ok(Next::Call(IoCall::child(chunk)))
    }

    /// Maps the chunk's rows and records them; the header comes first, in a step of its own.
    fn rows(&mut self, chunk: Chunk) -> Result<Next<TableStepEnd>, PageFailure> {
        let import = self.import()?;
        let name = table_name(&import);
        let permanent = |error: TableError| PageFailure::Permanent(error.to_string());
        let mut read = chunk.read;
        let Some(columns) = read.columns.clone() else {
            let (columns, used) =
                read_header(&import.format, &chunk.bytes, chunk.complete).map_err(permanent)?;
            RowMapper::new(&import.rows, &columns).map_err(permanent)?;
            read.columns = Some(columns);
            read.offset += used as u64;
            self.next.table = Some(Box::new(read));
            return self.commit();
        };
        let mapper = RowMapper::new(&import.rows, &columns).map_err(permanent)?;
        let width = Some(columns.len());
        let (mut rows, mut used) =
            parse_rows(&import.format, width, &chunk.bytes, chunk.complete).map_err(permanent)?;
        let mut complete = chunk.complete;
        let (step_rows, _) = step_size(&import);
        if rows.len() > step_rows {
            used = rows[step_rows - 1].1;
            rows.truncate(step_rows);
            complete = false;
        }
        let mut entries = Vec::new();
        for (row, end) in rows {
            let at = read.offset + end as u64;
            let (outcome, target) = match row {
                Ok(fields) => match mapper.map(&fields, self.base.as_deref()) {
                    failed @ RowOutcome::Failed(_) => (failed, mapper.target_key(&fields)),
                    outcome => (outcome, None),
                },
                Err(reason) => (RowOutcome::Failed(reason), None),
            };
            match outcome {
                RowOutcome::Filtered | RowOutcome::Skipped => {}
                RowOutcome::Failed(reason) => record_failure(
                    &mut self.next,
                    &name,
                    target.as_deref().unwrap_or_default(),
                    format!("row ending at byte {at}: {reason}"),
                ),
                RowOutcome::Import(entry) => entries.push(entry),
            }
        }
        let (entries, conflicts) = distinct_targets(entries);
        for entry in conflicts {
            let message = "another row of the table maps to this key".to_string();
            record_failure(&mut self.next, &entry.source_path, &entry.target, message);
        }
        read.offset += used as u64;
        read.done = complete;
        self.next.table = Some(Box::new(read));
        self.step = TableStep::Rows;
        let run = self.run.clone();
        let next = self.next.clone();
        Ok(Next::Call(match mapper.kind() {
            RowKind::File | RowKind::Record => {
                IoCall::child(FileRowsOperation::new(run, next, entries))
            }
            RowKind::Folder => IoCall::child(FolderRowsOperation::new(run, next, entries)),
        }))
    }
}

impl Operation for TableChunkOperation {
    type Output = TableStepEnd;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<TableStepEnd>, PageFailure> {
        let spec = &self.run.spec;
        Ok(Next::Call(IoCall::child(GetSourceOperation::new(
            GetSourceInput {
                group_id: spec.group_id,
                connector_id: spec.connector_id,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<TableStepEnd>, PageFailure> {
        match self.step {
            TableStep::Connector => {
                let source = (result.child::<GetSourceResult, GetSourceError>()?)
                    .map_err(|error| PageFailure::Retry(error.to_string()))?;
                self.base = source.connector.base_url();
                self.read_chunk()
            }
            TableStep::Chunk => {
                let name = table_name(&self.import()?);
                match result.child::<ChunkRead, ChunkFailure>()? {
                    Ok(read) => {
                        self.kept = read.kept;
                        self.rows(read.chunk)
                    }
                    Err(ChunkFailure::Changed) => {
                        self.next.table = Some(Box::default());
                        self.commit()
                    }
                    Err(ChunkFailure::Lasting(message)) => {
                        record_failure(&mut self.next, &name, "", message);
                        self.next.table = None;
                        self.commit()
                    }
                    Err(ChunkFailure::Retry(message)) => Err(PageFailure::Retry(message)),
                }
            }
            TableStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(self.done(self.next.clone()))
            }
            TableStep::Rows => {
                let checkpoint = result.child::<StagingJobCheckpoint, PageFailure>()??;
                Ok(self.done(checkpoint))
            }
        }
    }
}

/// Keeps the first row of each target; a later row mapping it alike is dropped, one mapping it
/// differently is returned as a conflict, so a page never registers one key twice.
fn distinct_targets(entries: Vec<TableEntry>) -> (Vec<TableEntry>, Vec<TableEntry>) {
    let mut kept: Vec<TableEntry> = Vec::with_capacity(entries.len());
    let mut seen = HashMap::new();
    let mut conflicts = Vec::new();
    for entry in entries {
        match seen.get(&entry.target) {
            Some(&index) if kept[index] == entry => {}
            Some(_) => conflicts.push(entry),
            None => {
                seen.insert(entry.target.clone(), kept.len());
                kept.push(entry);
            }
        }
    }
    (kept, conflicts)
}

fn table_name(import: &TableImport) -> String {
    match &import.source {
        TableSource::Connector(path) => path.clone(),
        TableSource::Object { bucket, key } => format!("{bucket}/{key}"),
    }
}

#[derive(Debug, PartialEq)]
enum FileRowsStep {
    Empty,
    Import,
}

/// Registers the files of file and record rows as references carrying their row metadata, and
/// writes the empty objects of record rows without a source.
#[derive(Debug, PartialEq)]
struct FileRowsOperation {
    run: StagingRun,
    next: StagingJobCheckpoint,
    empty: Vec<TableEntry>,
    entries: Vec<TableEntry>,
    step: FileRowsStep,
}

impl FileRowsOperation {
    fn new(run: StagingRun, next: StagingJobCheckpoint, entries: Vec<TableEntry>) -> Self {
        let (empty, entries) =
            (entries.into_iter()).partition(|entry: &TableEntry| entry.source_path.is_empty());
        Self {
            run,
            next,
            empty,
            entries,
            step: FileRowsStep::Empty,
        }
    }

    fn import(&mut self) -> Next<StagingJobCheckpoint> {
        self.step = FileRowsStep::Import;
        // A row with a size is its own observation; its checksums bind the reads without a
        // strong ETag, and a row without a size is headed.
        let files = (std::mem::take(&mut self.entries).into_iter())
            .map(|entry| StagedFile {
                source_path: entry.source_path,
                target_key: entry.target,
                size: entry.size,
                observation: entry.size.map(|content_length| SourceMetadata {
                    content_length,
                    content_type: entry.content_type,
                    etag: entry.etag,
                    last_modified: entry.modified,
                    source_version: None,
                    digests: Default::default(),
                }),
                manifest: entry.digests,
                metadata: entry.metadata,
                modified: entry.modified,
                mirrors: entry.mirrors,
            })
            .collect();
        Next::Call(IoCall::child(ImportPageOperation::new(
            self.run.clone(),
            WalkPlace::default(),
            self.next.clone(),
            (files, Vec::new(), Vec::new()),
            None,
        )))
    }
}

impl Operation for FileRowsOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        if self.empty.is_empty() {
            return Ok(self.import());
        }
        let entries = std::mem::take(&mut self.empty);
        let empty = EmptyObjectsOperation::new(self.run.clone(), self.next.clone(), entries);
        Ok(Next::Call(IoCall::child(empty)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match self.step {
            FileRowsStep::Empty => {
                self.next = result.child::<StagingJobCheckpoint, PageFailure>()??;
                Ok(self.import())
            }
            FileRowsStep::Import => {
                let checkpoint = result.child::<StagingJobCheckpoint, PageFailure>()??;
                Ok(Next::Done(checkpoint))
            }
        }
    }
}

/// The turns one key is written in, read from the set's authority rows when it has any.
fn turn_of(
    bucket: &BucketInfo,
    key: &str,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<Option<Turns>, PageFailure> {
    let Some(binding) = bucket.distribution.as_ref() else {
        return Ok(Some(Turns::new()));
    };
    let mut turns = key_turns(binding, &[key], record, fence).map_err(PageFailure::Retry)?;
    Ok(turns.pop().flatten())
}

#[derive(Debug, PartialEq)]
enum EmptyStep {
    Bucket,
    Rules,
    Authority,
    Put,
}

/// Writes each empty object this node coordinates, with its row metadata, before the chunk
/// commits; a transient failure retries the chunk, which writes equal objects only once.
#[derive(Debug, PartialEq)]
struct EmptyObjectsOperation {
    run: StagingRun,
    next: StagingJobCheckpoint,
    entries: VecDeque<TableEntry>,
    bucket: Option<BucketInfo>,
    rules: Option<PermissionRules>,
    authority: (Option<Value>, Option<Value>),
    current: Option<TableEntry>,
    step: EmptyStep,
}

impl EmptyObjectsOperation {
    fn new(run: StagingRun, next: StagingJobCheckpoint, entries: Vec<TableEntry>) -> Self {
        Self {
            run,
            next,
            entries: entries.into(),
            bucket: None,
            rules: None,
            authority: (None, None),
            current: None,
            step: EmptyStep::Bucket,
        }
    }

    fn advance(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let spec = self.run.spec.clone();
        let bucket = self.bucket.clone().ok_or(lost_rows())?;
        let (record, fence) = (self.authority.0.clone(), self.authority.1.clone());
        while let Some(entry) = self.entries.pop_front() {
            let target = object_permission_path(
                spec.auth_context.realm_id,
                spec.group_id,
                spec.node_id,
                &spec.bucket,
                &entry.target,
            );
            let rules = self.rules.as_ref().ok_or(lost_rows())?;
            if !rules.allows(&target, &Permission::WRITE) {
                record_failure(
                    &mut self.next,
                    "",
                    &entry.target,
                    "permission denied".to_string(),
                );
                continue;
            }
            let turn = turn_of(&bucket, &entry.target, record.as_deref(), fence.as_deref())?;
            let Some(turn) = turn else {
                continue;
            };
            // Pages of other jobs into the key's range wait instead of conflicting with the put.
            let put = PutFolderOperation::new(
                self.run.clone(),
                bucket.clone(),
                entry.target.clone(),
                entry.metadata.clone(),
            );
            self.current = Some(entry);
            self.step = EmptyStep::Put;
            return Ok(Next::Call(in_turns(turn, put)));
        }
        Ok(Next::Done(self.next.clone()))
    }
}

fn lost_rows() -> PageFailure {
    PageFailure::Retry("the table rows lost their state".to_string())
}

impl Operation for EmptyObjectsOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match self.step {
            EmptyStep::Bucket => {
                self.bucket = Some(pinned(&self.run.spec, result).map_err(page_failure)?);
                self.step = EmptyStep::Rules;
                Ok(Next::Call(rules_call(&self.run.spec)))
            }
            EmptyStep::Rules => {
                self.rules = Some(rules_result(result).map_err(PageFailure::Retry)?);
                let bucket = self.bucket.as_ref().ok_or(lost_rows())?;
                match bucket.distribution.as_ref() {
                    Some(binding) => {
                        self.step = EmptyStep::Authority;
                        Ok(Next::Call(authority_call(binding)))
                    }
                    None => self.advance(),
                }
            }
            EmptyStep::Authority => {
                let [record, fence] = values::<2>(result)?;
                self.authority = (record, fence);
                self.advance()
            }
            EmptyStep::Put => {
                let entry = self.current.take().ok_or(lost_rows())?;
                match turns_result::<(), ItemFailure>(result)? {
                    Ok(()) => {
                        let next = &mut self.next;
                        next.items_current = next.items_current.saturating_add(1);
                        next.items_succeeded = next.items_succeeded.saturating_add(1);
                    }
                    Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                        record_failure(&mut self.next, "", &entry.target, message)
                    }
                    Err(failure) => return Err(page_failure(failure)),
                }
                self.advance()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FolderStep {
    Bucket,
    Ranges,
    Rules,
    Turn,
    Put,
    Commit,
}

/// Queues the folders of folder rows whose keys overlap a range this node coordinates, after
/// writing each folder object on the coordinator of its key.
#[derive(Debug, PartialEq)]
struct FolderRowsOperation {
    run: StagingRun,
    next: StagingJobCheckpoint,
    entries: VecDeque<TableEntry>,
    bucket: Option<BucketInfo>,
    ranges: Option<Vec<(String, Option<String>)>>,
    rules: Option<PermissionRules>,
    current: Option<TableEntry>,
    writes: Vec<(KeySpace, Key, Value)>,
    step: FolderStep,
}

impl FolderRowsOperation {
    fn new(run: StagingRun, next: StagingJobCheckpoint, entries: Vec<TableEntry>) -> Self {
        Self {
            run,
            next,
            entries: entries.into(),
            bucket: None,
            ranges: None,
            rules: None,
            current: None,
            writes: Vec::new(),
            step: FolderStep::Bucket,
        }
    }

    fn rules_call(&mut self) -> Next<StagingJobCheckpoint> {
        self.step = FolderStep::Rules;
        Next::Call(rules_call(&self.run.spec))
    }

    fn advance(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let spec = self.run.spec.clone();
        while let Some(entry) = self.entries.pop_front() {
            let prefix = format!("{}/", entry.target);
            if !overlaps(self.ranges.as_deref(), &prefix) {
                continue;
            }
            let source = source_permission_path(&spec, &entry.source_path);
            let target = object_permission_path(
                spec.auth_context.realm_id,
                spec.group_id,
                spec.node_id,
                &spec.bucket,
                &prefix,
            );
            let rules = self.rules.as_ref().ok_or(lost_rows())?;
            if !rules.allows(&source, &Permission::READ)
                || !rules.allows(&target, &Permission::WRITE)
            {
                let denied = "permission denied".to_string();
                record_failure(&mut self.next, &entry.source_path, &prefix, denied);
                continue;
            }
            self.current = Some(entry);
            let bucket = self.bucket.as_ref().ok_or(lost_rows())?;
            return match bucket.distribution.as_ref() {
                Some(binding) => {
                    self.step = FolderStep::Turn;
                    Ok(Next::Call(authority_call(binding)))
                }
                None => self.put(Some(Turns::new())),
            };
        }
        self.step = FolderStep::Commit;
        let writes = std::mem::take(&mut self.writes);
        let call = (self.run)
            .commit_call(None, &self.next, writes, Vec::new())
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }

    /// Writes the current folder's object in its turn; a folder of another node just queues.
    fn put(&mut self, turn: Option<Turns>) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let Some(turn) = turn else {
            return self.queue();
        };
        let entry = self.current.as_ref().ok_or(lost_rows())?;
        let bucket = self.bucket.clone().ok_or(lost_rows())?;
        let key = format!("{}/", entry.target);
        let metadata = entry.metadata.clone();
        let put = PutFolderOperation::new(self.run.clone(), bucket, key, metadata);
        self.step = FolderStep::Put;
        Ok(Next::Call(in_turns(turn, put)))
    }

    fn queue(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let entry = self.current.take().ok_or(lost_rows())?;
        let directory = StagingJobDirectory {
            source_path: entry.source_path,
            target_prefix: entry.target,
        };
        let value = postcard::to_allocvec(&directory)
            .map_err(|error| PageFailure::Permanent(error.to_string()))?;
        let key = queue_key(self.run.job_id, self.next.queue_tail);
        self.writes.push((
            STAGING_QUEUE_KEYSPACE.to_string(),
            key,
            ByteView::from(value),
        ));
        self.next.queue_tail += 1;
        self.advance()
    }
}

impl Operation for FolderRowsOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match self.step {
            FolderStep::Bucket => {
                let bucket = pinned(&self.run.spec, result).map_err(page_failure)?;
                let authority = bucket.distribution.as_ref().map(authority_call);
                self.bucket = Some(bucket);
                match authority {
                    Some(call) => {
                        self.step = FolderStep::Ranges;
                        Ok(Next::Call(call))
                    }
                    None => Ok(self.rules_call()),
                }
            }
            FolderStep::Ranges => {
                let [record, fence] = values::<2>(result)?;
                let bucket = self.bucket.as_ref().ok_or(lost_rows())?;
                let binding = bucket.distribution.as_ref().ok_or(lost_rows())?;
                let ranges = own_ranges(binding, record.as_deref(), fence.as_deref())?;
                self.ranges = Some(ranges);
                Ok(self.rules_call())
            }
            FolderStep::Rules => {
                self.rules = Some(rules_result(result).map_err(PageFailure::Retry)?);
                self.advance()
            }
            FolderStep::Turn => {
                let [record, fence] = values::<2>(result)?;
                let entry = self.current.as_ref().ok_or(lost_rows())?;
                let bucket = self.bucket.as_ref().ok_or(lost_rows())?;
                let prefix = format!("{}/", entry.target);
                let turn = turn_of(bucket, &prefix, record.as_deref(), fence.as_deref())?;
                self.put(turn)
            }
            FolderStep::Put => {
                match turns_result::<(), ItemFailure>(result)? {
                    Ok(()) => {}
                    Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                        let entry = self.current.as_ref().ok_or(lost_rows())?;
                        let prefix = format!("{}/", entry.target);
                        record_failure(&mut self.next, &entry.source_path, &prefix, message)
                    }
                    Err(failure) => return Err(page_failure(failure)),
                }
                self.queue()
            }
            FolderStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(Next::Done(self.next.clone()))
            }
        }
    }
}

fn page_failure(failure: ItemFailure) -> PageFailure {
    match failure {
        ItemFailure::System(message) => PageFailure::Retry(message),
        ItemFailure::Denied(message)
        | ItemFailure::Stage(message)
        | ItemFailure::Lasting(message) => PageFailure::Permanent(message),
    }
}

/// Whether keys below `prefix` lie in one of `ranges`; every key does without ranges.
fn overlaps(ranges: Option<&[(String, Option<String>)]>, prefix: &str) -> bool {
    let Some(ranges) = ranges else {
        return true;
    };
    // `0` follows `/`, so keys below the prefix sort before this one.
    let end = format!("{}0", prefix.trim_end_matches('/'));
    (ranges.iter()).any(|(lower, upper)| {
        lower.as_str() < end.as_str() && upper.as_deref().is_none_or(|upper| upper > prefix)
    })
}

#[derive(Debug, PartialEq)]
enum PutStep {
    Head,
    Routing,
    Gate(Box<RoutingSnapshot>),
    Quota(Box<RoutingSnapshot>, Option<Box<GateContext>>),
    Put,
    Replicate,
}

/// Writes the zero-byte folder object at `key` with the row metadata, through the ordinary put
/// as the submitter, and queues its replication; an equal head stays. The version's origin is
/// the job's import, like the references of its other rows.
#[derive(Debug, PartialEq)]
pub(super) struct PutFolderOperation {
    run: StagingRun,
    bucket: BucketInfo,
    key: String,
    metadata: BTreeMap<String, String>,
    now_ms: u64,
    step: PutStep,
}

impl PutFolderOperation {
    pub(super) fn new(
        run: StagingRun,
        bucket: BucketInfo,
        key: String,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        Self {
            run,
            bucket,
            key,
            metadata,
            now_ms: 0,
            step: PutStep::Head,
        }
    }

    fn put(
        &mut self,
        routing: RoutingSnapshot,
        gate: Option<GateContext>,
        quota_ceiling: Option<u64>,
    ) -> Next<()> {
        let spec = &self.run.spec;
        let auth = &spec.auth_context;
        let body = BackendStream::new(futures_util::stream::iter([Ok::<_, StreamError>(
            Bytes::new(),
        )]));
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: auth.user_id,
            group_id: self.bucket.group_id,
            realm_id: auth.realm_id,
            node_id: spec.node_id,
            request: PutObjectInput {
                bucket: spec.bucket.clone(),
                key: self.key.clone(),
                content_length: Some(0),
                body: Some(body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling,
            routing,
        })
        .with_bucket_guard(self.bucket.clone())
        .with_version_origin(Some(VersionOrigin::Source {
            job_id: Some(self.run.job_id.as_ulid()),
        }))
        .with_restrictions(auth.path_restrictions.clone())
        .with_metadata(self.metadata.clone().into_iter().collect::<HashMap<_, _>>());
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        self.step = PutStep::Put;
        Next::Call(IoCall::child(operation))
    }
}

fn system<T: 'static, E: ToString + 'static>(result: IoResult) -> Result<T, ItemFailure> {
    (result.child::<T, E>()?).map_err(|error| ItemFailure::System(error.to_string()))
}

impl Operation for PutFolderOperation {
    type Output = ();
    type Error = ItemFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, ItemFailure> {
        Ok(Next::Call(IoCall::child(HeadObjectOperation::new(
            HeadObjectInput {
                bucket: self.run.spec.bucket.clone(),
                key: self.key.clone(),
                version_id: None,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ItemFailure> {
        let realm_id = self.run.spec.auth_context.realm_id;
        match std::mem::replace(&mut self.step, PutStep::Replicate) {
            PutStep::Head => {
                if let Ok(head) = result.child::<HeadObjectResult, HeadObjectError>()? {
                    let empty =
                        (head.location.as_ref()).is_some_and(|location| location.blob_size == 0);
                    let metadata = &self.metadata;
                    let same = head.metadata.len() == metadata.len()
                        && (metadata.iter())
                            .all(|(key, value)| head.metadata.get(key) == Some(value));
                    if empty && same {
                        return Ok(Next::Done(()));
                    }
                }
                self.step = PutStep::Routing;
                Ok(Next::Call(IoCall::child(SnapshotOperation::new(
                    &self.run.routing,
                    self.bucket.group_id,
                    BucketRules::of(&self.bucket),
                ))))
            }
            PutStep::Routing => {
                let routing = system::<RoutingSnapshot, RoutingInputsError>(result)?;
                self.step = PutStep::Gate(Box::new(routing));
                let gate = GateOperation::new(realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            PutStep::Gate(routing) => {
                let gate = system::<Option<GateContext>, GateContextError>(result)?;
                self.step = PutStep::Quota(routing, gate.map(Box::new));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
            }
            PutStep::Quota(routing, gate) => {
                let config = system::<RealmConfigDocument, GetConfigError>(result)?;
                let quota = config.quota.effective_group_ceiling(&self.bucket.group_id);
                Ok(self.put(*routing, gate.map(|gate| *gate), quota))
            }
            PutStep::Put => {
                let put = result
                    .child::<PutObjectResult, PutObjectError>()?
                    .map_err(put_failure)?;
                let spec = &self.run.spec;
                self.step = PutStep::Replicate;
                Ok(Next::Call(IoCall::child(LiveVersionOperation::new(
                    LiveVersionInput {
                        local_node_id: spec.node_id,
                        auth_context: spec.auth_context.clone(),
                        bucket: spec.bucket.clone(),
                        key: self.key.clone(),
                        version_id: put.version_id,
                        delete_marker: false,
                    },
                ))))
            }
            PutStep::Replicate => {
                result
                    .child::<QueueBlobResult, BlobQueueError>()?
                    .map_err(|error| {
                        ItemFailure::Stage(format!("replication queue failed: {error}"))
                    })?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// A put that fails the same way again fails its row; any other retries the chunk, which keeps
/// its checkpoint, so no folder is queued without its folder object.
fn put_failure(error: PutObjectError) -> ItemFailure {
    let message = format!("object not written: {error}");
    match lasting_put(&error) {
        true => ItemFailure::Stage(message),
        false => ItemFailure::System(message),
    }
}

/// Whether a put fails the same way when it runs again.
fn lasting_put(error: &PutObjectError) -> bool {
    use PutObjectError as Put;
    match error {
        Put::QuotaExceeded { .. }
        | Put::Unmapped
        | Put::PolicyError(_)
        | Put::ObjectExists
        | Put::ConversionError(_) => true,
        Put::QuotaGateError(error) => match error {
            QuotaGateError::ConversionError(_) => true,
            QuotaGateError::UnexpectedEvent(_) | QuotaGateError::Io(_) => false,
        },
        Put::RoutingFailed(error) => match error {
            RoutingError::BackendFull(_) => false,
            RoutingError::UnknownBackend(_)
            | RoutingError::GroupEgressDisabled
            | RoutingError::DuplicateRule(_)
            | RoutingError::InvalidClass(_)
            | RoutingError::OperatorBackendTarget
            | RoutingError::ForeignBackend(_)
            | RoutingError::FolderTarget(_)
            | RoutingError::AmbiguousRules { .. } => true,
        },
        Put::PolicyGate(error) => lasting_gate(error),
        Put::PurgeFence(error) => match error {
            PurgeFenceError::Invalid => true,
            PurgeFenceError::Suspended
            | PurgeFenceError::Busy
            | PurgeFenceError::Storage(_)
            | PurgeFenceError::Unexpected(_)
            | PurgeFenceError::Distribution(_) => false,
        },
        Put::StorageError(_)
        | Put::InvalidOperationState
        | Put::NoTransactionFound
        | Put::MissingOutput
        | Put::MissingHash(_)
        | Put::MissingBody
        | Put::IncompleteBody
        | Put::MissingExpectedChecksum(_)
        | Put::ChecksumMismatch(_)
        | Put::WriteFailed(_)
        | Put::BlobWriteFailed(_)
        | Put::InvalidPreassignedVersion
        | Put::UsageUpdateError(_)
        | Put::BackendFenceError(_)
        | Put::ManagedCopyError(_)
        | Put::Distribution(_)
        | Put::PutObjectFailed
        | Put::HeadChanged
        | Put::ImportRemoved
        | Put::VersionExists
        | Put::HistoryDeleted
        | Put::FenceTaken
        | Put::UsageRebuilding => false,
    }
}

/// Whether a placement policy gate refuses the same way again; a stopped admission, missing
/// policies or a storage failure pass.
fn lasting_gate(error: &PolicyGateError) -> bool {
    match error {
        PolicyGateError::NoSubject
        | PolicyGateError::Denied { .. }
        | PolicyGateError::ForeignPolicy { .. }
        | PolicyGateError::Invalid
        | PolicyGateError::Required { .. }
        | PolicyGateError::Conversion(_)
        | PolicyGateError::Policy(_) => true,
        PolicyGateError::Unavailable { .. }
        | PolicyGateError::Drift
        | PolicyGateError::AdmissionStopped
        | PolicyGateError::InvalidEvent => false,
        PolicyGateError::Read(error) => match error {
            ReadPolicyError::Conversion(_)
            | ReadPolicyError::Policy(_)
            | ReadPolicyError::Authority(_)
            | ReadPolicyError::NotFound { .. }
            | ReadPolicyError::DigestMismatch
            | ReadPolicyError::RealmMismatch
            | ReadPolicyError::UnexpectedPublisher => true,
            ReadPolicyError::Storage(_)
            | ReadPolicyError::RealmConfigMissing
            | ReadPolicyError::PlacementUnavailable
            | ReadPolicyError::PlacementResolve(_)
            | ReadPolicyError::Unavailable(_)
            | ReadPolicyError::NotFinished
            | ReadPolicyError::UnexpectedEvent { .. } => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Storage hiccups and usage rebuilds retry the chunk; a full quota fails the row.
    #[test]
    fn put_failures_classified() {
        use aruna_core::errors::StorageError;
        let retried = |error| matches!(put_failure(error), ItemFailure::System(_));
        assert!(retried(PutObjectError::UsageRebuilding));
        assert!(retried(PutObjectError::FenceTaken));
        assert!(retried(PutObjectError::StorageError(StorageError::Timeout)));
        let quota = PutObjectError::QuotaExceeded { limit: 1, usage: 2 };
        assert!(matches!(put_failure(quota), ItemFailure::Stage(_)));
        // Temporary refusals inside the gate and the purge fence retry too.
        assert!(retried(PolicyGateError::AdmissionStopped.into()));
        assert!(retried(
            PurgeFenceError::Storage(StorageError::Timeout).into()
        ));
        assert!(retried(
            RoutingError::BackendFull(
                aruna_core::structs::storage::blob::BackendRef::node_default()
            )
            .into()
        ));
        let denied = PolicyGateError::Denied {
            policy_ids: Vec::new(),
        };
        assert!(!retried(denied.into()));
        assert!(!retried(RoutingError::GroupEgressDisabled.into()));
    }

    // Rows naming one key alike import once; rows naming it differently fail, not overwrite.
    #[test]
    fn duplicate_targets_once() {
        let entry = |source: &str, target: &str| TableEntry {
            source_path: source.to_string(),
            target: target.to_string(),
            metadata: BTreeMap::new(),
            size: None,
            digests: Vec::new(),
            modified: None,
            etag: None,
            content_type: None,
            mirrors: Vec::new(),
        };
        let rows = vec![
            entry("a.fq", "x/a.fq"),
            entry("a.fq", "x/a.fq"),
            entry("b.fq", "x/a.fq"),
            entry("c.fq", "x/c.fq"),
        ];
        let (kept, conflicts) = distinct_targets(rows);
        assert_eq!(kept, [entry("a.fq", "x/a.fq"), entry("c.fq", "x/c.fq")]);
        assert_eq!(conflicts, [entry("b.fq", "x/a.fq")]);
    }

    // A folder spanning a range boundary is queued by both coordinators, others by one.
    #[test]
    fn folders_follow_ranges() {
        let ranges = [
            (
                String::new(),
                Some("GCF_000005845.2_ASM584v2/genomic.fna".to_string()),
            ),
            ("GCF_000005845.2_ASM584v2/genomic.fna".to_string(), None),
        ];
        let prefix = "GCF_000005845.2_ASM584v2/";
        assert!(overlaps(Some(&ranges[..1]), prefix));
        assert!(overlaps(Some(&ranges[1..]), prefix));
        let before = [(String::new(), Some("GCF_000005845.2_ASM584v2/".to_string()))];
        assert!(!overlaps(Some(&before), prefix));
        let after = [("GCF_000005845.2_ASM584v20".to_string(), None)];
        assert!(!overlaps(Some(&after), prefix));
        assert!(overlaps(None, prefix));
    }
}

#[cfg(test)]
#[path = "table_tests.rs"]
mod adapter_tests;

#[cfg(test)]
#[path = "chunk_tests.rs"]
mod chunk_tests;
