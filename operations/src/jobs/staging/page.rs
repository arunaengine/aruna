//! Records one page of staged files: as references in a page transaction that takes its range
//! turns, or as snapshot copies followed by the page's checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{SourceResolutionError, StorageError};
use aruna_core::events::Event;
use aruna_core::jobs::{JobEffect, JobEvent, JobReport};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::job::StagingJobCheckpoint;
use aruna_core::structs::execution::source_access::{ResolvedSourceConnector, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::{Key, KeySpace, Value};
use ulid::Ulid;

use super::item::{ElsewhereOperation, StageItemOperation};
use super::limiter::{in_turns, retried_call, retried_result, turns_result};
use super::manifest::{lasting_head, lasting_resolution};
use super::mirrors::{MirroredPage, MirrorsOperation};
use super::walk::{
    StagedFile, Turns, WalkPlace, authority_call, file_access, lane_turn, own_files,
    record_failure, rules_call, rules_result, with_manifest,
};
use super::{ItemFailure, PageFailure, StagingRun, checkpoint_key, committed, pinned};
use crate::auth::permission_rules::PermissionRules;
use crate::connectors::resolver::{ResolveConnectorInput, ResolveConnectorOperation};
use crate::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation, HeadSourceResult,
};
use crate::staging::page::import::{
    PageEntry, PageImportError, PageImportInput, PageImportOperation, PageImportResult,
    PreparePageOperation,
};
use crate::staging::page::rows::PreparedRows;

/// Page transactions one listed page runs at most while their commits conflict.
const PAGE_ATTEMPTS: u32 = 8;
/// Pause before the next page transaction, times the attempts so far.
const CONFLICT_PAUSE: Duration = Duration::from_millis(20);

/// The connector a page's references name, as resolved for its listing or its first HEAD.
pub(super) type PageSource = (SourceConnector, Option<[u8; 16]>);

/// A page's files with the job rows written and deleted with it.
pub(super) type PageFiles = (
    Vec<StagedFile>,
    Vec<(KeySpace, Key, Value)>,
    Vec<(KeySpace, Key)>,
);

#[derive(Debug, PartialEq)]
enum ImportStep {
    Bucket,
    Authority,
    Rules,
    Head,
    Mirrors,
    Resolve,
    Prepare,
    Turns,
    Commit,
}

/// Registers a page's files as references with listing metadata, in one transaction with the
/// checkpoint. Files a listing gives no size for are read with one HEAD each.
#[derive(Debug, PartialEq)]
pub(super) struct ImportPageOperation {
    run: StagingRun,
    place: WalkPlace,
    next: StagingJobCheckpoint,
    files: VecDeque<StagedFile>,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
    source: Option<PageSource>,
    bucket: Option<BucketInfo>,
    ranges: Turns,
    rules: Option<PermissionRules>,
    heading: Option<StagedFile>,
    entries: Vec<(PageEntry, Vec<(Ulid, String)>)>,
    kept: Vec<PageEntry>,
    mirror_connectors: Vec<SourceConnector>,
    input: Option<PageImportInput>,
    now_ms: u64,
    step: ImportStep,
}

impl ImportPageOperation {
    pub(super) fn new(
        run: StagingRun,
        place: WalkPlace,
        next: StagingJobCheckpoint,
        (files, writes, deletes): PageFiles,
        source: Option<PageSource>,
    ) -> Self {
        Self {
            run,
            place,
            next,
            files: files.into(),
            writes,
            deletes,
            source,
            bucket: None,
            ranges: Turns::new(),
            rules: None,
            heading: None,
            entries: Vec::new(),
            kept: Vec::new(),
            mirror_connectors: Vec::new(),
            input: None,
            now_ms: 0,
            step: ImportStep::Bucket,
        }
    }

    fn rules_step(&mut self) -> Next<StagingJobCheckpoint> {
        self.step = ImportStep::Rules;
        Next::Call(rules_call(&self.run.spec))
    }

    /// Decides the next file: one that needs a HEAD waits for it, others go on at once.
    fn next_file(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let spec = self.run.spec.clone();
        while let Some(mut file) = self.files.pop_front() {
            let rules = self.rules.as_ref().ok_or(lost("rules"))?;
            if let Err(failure) = file_access(rules, &spec, &file.source_path, &file.target_key) {
                self.failed(&file, failure)?;
                continue;
            }
            match file.observation.take() {
                Some(observation) => self.observed(file, observation),
                None => {
                    self.step = ImportStep::Head;
                    let input = HeadSourceInput {
                        group_id: spec.group_id,
                        connector_id: spec.connector_id,
                        source_path: file.source_path.clone(),
                    };
                    self.heading = Some(file);
                    let transient: fn(&HeadSourceError) -> bool = |error| !lasting_head(error);
                    let head =
                        retried_call(&self.run, input, HeadSourceOperation::new, transient);
                    return Ok(Next::Call(head));
                }
            }
        }
        self.step = ImportStep::Mirrors;
        let mirrors = MirrorsOperation::new(
            self.run.clone(),
            self.rules.take().ok_or(lost("rules"))?,
            self.next.clone(),
            std::mem::take(&mut self.entries),
        );
        Ok(Next::Call(IoCall::child(mirrors)))
    }

    fn observed(&mut self, file: StagedFile, mut observation: SourceMetadata) {
        observation.last_modified = file.modified.or(observation.last_modified);
        let entry = PageEntry {
            source_path: file.source_path,
            target_key: file.target_key,
            observation: with_manifest(observation, file.manifest),
            metadata: file.metadata,
            mirrors: Vec::new(),
        };
        self.entries.push((entry, file.mirrors));
    }

    fn failed(&mut self, file: &StagedFile, failure: ItemFailure) -> Result<(), PageFailure> {
        match failure {
            ItemFailure::Denied(message) | ItemFailure::Stage(message) => {
                record_failure(&mut self.next, &file.source_path, &file.target_key, message);
                Ok(())
            }
            ItemFailure::System(message) => Err(PageFailure::Retry(message)),
            ItemFailure::Lasting(message) => Err(PageFailure::Permanent(message)),
        }
    }

    /// One HEAD; the page takes its connector from the first one, and a changed one replays it.
    fn headed(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let file = self.heading.take().ok_or(lost("file"))?;
        let head = match retried_result::<HeadSourceResult, HeadSourceError>(result)? {
            Ok(head) => head,
            Err(error) if lasting_head(&error) => {
                self.failed(&file, ItemFailure::Stage(error.to_string()))?;
                return self.next_file();
            }
            // A transient failure replays the page instead of failing the file for good.
            Err(error) => return Err(PageFailure::Retry(error.to_string())),
        };
        let source = (head.connector, head.secret_fingerprint);
        match &self.source {
            Some(known) if *known != source => {
                let message = "the connector changed while the page was read".to_string();
                return Err(PageFailure::Retry(message));
            }
            Some(_) => {}
            None => self.source = Some(source),
        }
        self.observed(file, head.metadata);
        self.next_file()
    }

    fn commit(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        self.step = ImportStep::Commit;
        let (writes, deletes) = (
            std::mem::take(&mut self.writes),
            std::mem::take(&mut self.deletes),
        );
        let call = (self.run)
            .commit_call(self.place.slot, &self.next, writes, deletes)
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }

    /// Files a table row describes are not headed; the page then resolves its connector itself.
    fn resolve(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match (self.source.is_some(), self.kept.first()) {
            (true, _) => self.prepare(),
            (false, None) => self.commit(),
            (false, Some(entry)) => {
                self.step = ImportStep::Resolve;
                let spec = &self.run.spec;
                let input = ResolveConnectorInput {
                    group_id: spec.group_id,
                    connector_id: spec.connector_id,
                    source_path: entry.source_path.clone(),
                    allow_root: false,
                };
                Ok(Next::Call(IoCall::child(ResolveConnectorOperation::new(input))))
            }
        }
    }

    /// The rows are built before the turn, so pages of other jobs build theirs meanwhile.
    fn prepare(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let spec = self.run.spec.clone();
        let (connector, secret_fingerprint) = self.source.take().ok_or(lost("source"))?;
        let input = PageImportInput {
            job_id: self.run.job_id,
            claim_token: self.run.claim_token,
            realm_id: spec.auth_context.realm_id,
            group_id: spec.group_id,
            user_id: spec.auth_context.user_id,
            auth_context: spec.auth_context.clone(),
            node_id: spec.node_id,
            bucket: spec.bucket.clone(),
            expected_bucket: self.bucket.take().ok_or(lost("bucket"))?,
            connector,
            secret_fingerprint,
            entries: std::mem::take(&mut self.kept),
            checkpoint: self.next.clone(),
            checkpoint_key: checkpoint_key(self.run.job_id, self.place.slot),
            writes: std::mem::take(&mut self.writes),
            deletes: std::mem::take(&mut self.deletes),
            now: SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms),
            ranges: None,
            mirror_connectors: std::mem::take(&mut self.mirror_connectors),
        };
        self.step = ImportStep::Prepare;
        let prepare = PreparePageOperation::new(input.clone());
        self.input = Some(input);
        Ok(Next::Call(IoCall::child(prepare)))
    }
}

/// A state a step needs was not left by the step before it.
fn lost(what: &'static str) -> PageFailure {
    PageFailure::Retry(format!("the page lost its {what}"))
}

impl Operation for ImportPageOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match self.step {
            ImportStep::Bucket => {
                let bucket = pinned(&self.run.spec, result)?;
                let authority = bucket.distribution.as_ref().map(authority_call);
                self.bucket = Some(bucket);
                match authority {
                    Some(call) => {
                        self.step = ImportStep::Authority;
                        Ok(Next::Call(call))
                    }
                    None => Ok(self.rules_step()),
                }
            }
            ImportStep::Authority => {
                let [record, fence] = values::<2>(result)?;
                let bucket = self.bucket.as_ref().ok_or(lost("bucket"))?;
                let binding = bucket.distribution.as_ref().ok_or(lost("binding"))?;
                let files = std::mem::take(&mut self.files).into();
                let (files, ranges) =
                    own_files(binding, files, record.as_deref(), fence.as_deref())?;
                self.files = files.into();
                self.ranges = ranges;
                Ok(self.rules_step())
            }
            ImportStep::Rules => {
                // The submitter's rules are collected once and decide every file in memory.
                self.rules = Some(rules_result(result).map_err(PageFailure::Retry)?);
                self.next_file()
            }
            ImportStep::Head => self.headed(result),
            ImportStep::Mirrors => {
                let page = result.child::<MirroredPage, PageFailure>()??;
                self.next = page.next;
                self.kept = page.entries;
                self.mirror_connectors = page.connectors;
                self.resolve()
            }
            ImportStep::Resolve => {
                let resolved =
                    result.child::<ResolvedSourceConnector, SourceResolutionError>()?;
                match resolved {
                    Ok(resolved) => {
                        self.source = Some((resolved.connector, resolved.secret_fingerprint));
                        self.prepare()
                    }
                    Err(error) if lasting_resolution(&error) => {
                        Err(PageFailure::Permanent(error.to_string()))
                    }
                    Err(error) => Err(PageFailure::Retry(error.to_string())),
                }
            }
            ImportStep::Prepare => {
                // A failed preparation only costs time.
                let prepared = result.child::<PreparedRows, PageImportError>()?.ok();
                let input = self.input.take().ok_or(lost("input"))?;
                self.step = ImportStep::Turns;
                let lane = self.place.lane.then(|| lane_turn(self.run.job_id));
                let turns = std::mem::take(&mut self.ranges);
                let page = PageTurnsOperation::new(input, turns, lane, prepared);
                Ok(Next::Call(IoCall::child(page)))
            }
            ImportStep::Turns => {
                let page = result.child::<Result<PageImportResult, PageImportError>, IoError>()?;
                match page.map_err(|error| PageFailure::Retry(error.to_string()))? {
                    Ok(imported) => Ok(Next::Done(imported.checkpoint)),
                    Err(
                        error @ (PageImportError::BucketChanged
                        | PageImportError::TooManyBytes(_)),
                    ) => Err(PageFailure::Permanent(error.to_string())),
                    Err(error @ PageImportError::Storage(StorageError::TransactionConflict)) => {
                        Err(PageFailure::Conflict(error.to_string()))
                    }
                    // A changed connector or a lost claim replays the page from its cursor.
                    Err(error) => Err(PageFailure::Retry(error.to_string())),
                }
            }
            ImportStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(Next::Done(self.next.clone()))
            }
        }
    }
}

/// How the attempts inside one set of range turns ended.
#[derive(Debug, PartialEq)]
pub(super) struct AttemptsEnd {
    attempts: u32,
    result: Result<PageImportResult, PageImportError>,
}

/// Runs the page transaction of `input` in the turns of `ranges`, again in new turns while its
/// ranges moved. Pages of other jobs into these ranges wait instead of conflicting on their rows.
#[derive(Debug, PartialEq)]
pub(super) struct PageTurnsOperation {
    input: PageImportInput,
    ranges: Turns,
    set_id: Option<Ulid>,
    lane: Option<(Ulid, Ulid)>,
    prepared: Option<PreparedRows>,
    attempts: u32,
}

impl PageTurnsOperation {
    pub(super) fn new(
        mut input: PageImportInput,
        ranges: Turns,
        lane: Option<(Ulid, Ulid)>,
        prepared: Option<PreparedRows>,
    ) -> Self {
        let set_id = ranges.first().map(|(set_id, _)| *set_id);
        input.ranges = set_id.map(|_| ranges.iter().map(|(_, range)| *range).collect());
        Self {
            input,
            ranges,
            set_id,
            lane,
            prepared,
            attempts: 1,
        }
    }

    fn attempts_call(&mut self) -> IoCall {
        let mut turns = self.ranges.clone();
        turns.extend(self.lane);
        let attempts = PageAttemptsOperation {
            input: self.input.clone(),
            prepared: self.prepared.take(),
            attempts: self.attempts,
            pausing: false,
            last: None,
        };
        in_turns(turns, attempts)
    }
}

impl Operation for PageTurnsOperation {
    type Output = Result<PageImportResult, PageImportError>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(self.attempts_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        let end = turns_result::<AttemptsEnd, IoError>(result)??;
        self.attempts = end.attempts;
        match (end.result, self.set_id) {
            // A range split or merged meanwhile: the page runs again in the turns of the new ones.
            (Err(PageImportError::RangesMoved(moved)), Some(set_id))
                if self.attempts < PAGE_ATTEMPTS =>
            {
                self.ranges = moved.iter().map(|range| (set_id, *range)).collect();
                self.input.ranges = Some(moved);
                self.attempts += 1;
                Ok(Next::Call(self.attempts_call()))
            }
            (result, _) => Ok(Next::Done(result)),
        }
    }
}

/// Runs page transactions while their commits conflict, each a new transaction that checks every
/// guard again, at most [`PAGE_ATTEMPTS`] counted across the page's turns.
#[derive(Debug, PartialEq)]
struct PageAttemptsOperation {
    input: PageImportInput,
    prepared: Option<PreparedRows>,
    attempts: u32,
    pausing: bool,
    last: Option<PageImportError>,
}

impl PageAttemptsOperation {
    fn attempt(&mut self) -> Next<AttemptsEnd> {
        let page = PageImportOperation::new(self.input.clone()).with_prepared(self.prepared.take());
        Next::Call(IoCall::child(page))
    }

    fn end(&self, result: Result<PageImportResult, PageImportError>) -> Next<AttemptsEnd> {
        Next::Done(AttemptsEnd {
            attempts: self.attempts,
            result,
        })
    }
}

impl Operation for PageAttemptsOperation {
    type Output = AttemptsEnd;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<AttemptsEnd>, IoError> {
        Ok(self.attempt())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AttemptsEnd>, IoError> {
        if self.pausing {
            self.pausing = false;
            let event = result.event()?;
            let Event::Job(JobEvent::Paused { stopped }) = event else {
                return Err(IoError::Unexpected {
                    expected: "the end of the pause",
                    got: event.label(),
                });
            };
            if stopped {
                let conflict = self.last.take().unwrap_or(conflict());
                return Ok(self.end(Err(conflict)));
            }
            self.attempts += 1;
            return Ok(self.attempt());
        }
        let page = result.child::<PageImportResult, PageImportError>()?;
        match page {
            Err(error) if error == conflict() && self.attempts < PAGE_ATTEMPTS => {
                self.pausing = true;
                self.last = Some(error);
                let pause = CONFLICT_PAUSE * self.attempts;
                let pause = Effect::Job(JobEffect::Pause(pause));
                Ok(Next::Call(IoCall::Effect(Box::new(pause))))
            }
            result => Ok(self.end(result)),
        }
    }
}

fn conflict() -> PageImportError {
    PageImportError::Storage(StorageError::TransactionConflict)
}

#[derive(Debug, PartialEq)]
enum SnapshotStep {
    Bucket,
    Authority,
    Stops,
    Stage,
    Moved(String),
    Reported,
    Commit,
}

/// Copies a page's files one by one; the page's checkpoint follows its last file, and a replayed
/// page finds already copied files through the job's retry key. A stop leaves the page open.
#[derive(Debug, PartialEq)]
pub(super) struct SnapshotPageOperation {
    run: StagingRun,
    checkpoint: StagingJobCheckpoint,
    next: StagingJobCheckpoint,
    files: VecDeque<StagedFile>,
    current: Option<StagedFile>,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
    bucket: Option<BucketInfo>,
    step: SnapshotStep,
}

impl SnapshotPageOperation {
    pub(super) fn new(
        run: StagingRun,
        checkpoint: StagingJobCheckpoint,
        next: StagingJobCheckpoint,
        (files, writes, deletes): PageFiles,
    ) -> Self {
        Self {
            run,
            checkpoint,
            next,
            files: files.into(),
            current: None,
            writes,
            deletes,
            bucket: None,
            step: SnapshotStep::Bucket,
        }
    }

    fn next_file(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        if self.files.is_empty() {
            self.step = SnapshotStep::Commit;
            let (writes, deletes) = (
                std::mem::take(&mut self.writes),
                std::mem::take(&mut self.deletes),
            );
            let call = (self.run)
                .commit_call(None, &self.next, writes, deletes)
                .map_err(PageFailure::Retry)?;
            return Ok(Next::Call(call));
        }
        self.step = SnapshotStep::Stops;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(JobEffect::Stops)))))
    }

    fn stage(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let mut file = self.files.pop_front().ok_or(lost("file"))?;
        self.next.current_path = Some(file.source_path.clone());
        let expected = (std::mem::take(&mut file.manifest).into_iter())
            .map(|(algorithm, digest)| ExpectedChecksum { algorithm, digest })
            .collect();
        let paths = (file.source_path.clone(), file.target_key.clone());
        let index = self.next.items_current;
        self.current = Some(file);
        self.step = SnapshotStep::Stage;
        let stage = StageItemOperation::new(self.run.clone(), paths, expected, index);
        Ok(Next::Call(IoCall::child(stage)))
    }

    /// The file is settled: shows the progress, then goes on with the next one.
    fn report(&mut self) -> Next<StagingJobCheckpoint> {
        self.step = SnapshotStep::Reported;
        let report = JobReport {
            current: Some(self.next.items_current),
            ..JobReport::default()
        };
        let report = Effect::Job(JobEffect::Report(report));
        Next::Call(IoCall::Effect(Box::new(report)))
    }
}

impl Operation for SnapshotPageOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match std::mem::replace(&mut self.step, SnapshotStep::Commit) {
            SnapshotStep::Bucket => {
                let bucket = pinned(&self.run.spec, result)?;
                let authority = bucket.distribution.as_ref().map(authority_call);
                self.bucket = Some(bucket);
                match authority {
                    Some(call) => {
                        self.step = SnapshotStep::Authority;
                        Ok(Next::Call(call))
                    }
                    None => self.next_file(),
                }
            }
            SnapshotStep::Authority => {
                let [record, fence] = values::<2>(result)?;
                let bucket = self.bucket.as_ref().ok_or(lost("bucket"))?;
                let binding = bucket.distribution.as_ref().ok_or(lost("binding"))?;
                let files = std::mem::take(&mut self.files).into();
                let (files, _) = own_files(binding, files, record.as_deref(), fence.as_deref())?;
                self.files = files.into();
                self.next_file()
            }
            SnapshotStep::Stops => {
                let event = result.event()?;
                let Event::Job(JobEvent::Stops(stops)) = event else {
                    return Err(IoError::Unexpected {
                        expected: "the job's stop state",
                        got: event.label(),
                    }
                    .into());
                };
                match stops.any() {
                    true => Ok(Next::Done(self.checkpoint.clone())),
                    false => self.stage(),
                }
            }
            SnapshotStep::Stage => match result.child::<u64, ItemFailure>()? {
                Ok(size) => {
                    let next = &mut self.next;
                    next.items_current = next.items_current.saturating_add(1);
                    next.items_succeeded = next.items_succeeded.saturating_add(1);
                    next.bytes_current = next.bytes_current.saturating_add(size);
                    Ok(self.report())
                }
                // A key whose range moved meanwhile belongs to its new coordinator's job.
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    self.step = SnapshotStep::Moved(message);
                    let file = self.current.as_ref().ok_or(lost("file"))?;
                    let moved =
                        ElsewhereOperation::new(self.run.clone(), file.target_key.clone());
                    Ok(Next::Call(IoCall::child(moved)))
                }
                Err(ItemFailure::System(message)) => Err(PageFailure::Retry(message)),
                Err(ItemFailure::Lasting(message)) => Err(PageFailure::Permanent(message)),
            },
            SnapshotStep::Moved(message) => {
                if !matches!(result.child::<bool, ItemFailure>()?, Ok(true)) {
                    let file = self.current.as_ref().ok_or(lost("file"))?;
                    let (source, target) = (&file.source_path, &file.target_key);
                    record_failure(&mut self.next, source, target, message);
                }
                Ok(self.report())
            }
            SnapshotStep::Reported => {
                let event = result.event()?;
                if !matches!(event, Event::Job(JobEvent::Reported)) {
                    return Err(IoError::Unexpected {
                        expected: "the recorded progress",
                        got: event.label(),
                    }
                    .into());
                }
                self.next_file()
            }
            SnapshotStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(Next::Done(self.next.clone()))
            }
        }
    }
}

#[cfg(test)]
#[path = "page_tests.rs"]
mod tests;
