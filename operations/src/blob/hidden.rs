//! Sweeps hidden blobs, upload cleanup rows and abandoned RO-Crate uploads on a timer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, CACHE_ENTRY_KEYSPACE, JOB_KEYSPACE, NODE_STATE_KEYSPACE,
    ROCRATE_UPLOAD_KEYSPACE, UPLOAD_CLEANUP_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::{
    JobId, JobRecord, JobResultPayload, RoCrateUploadCleanup, RoCrateUploadRecord, job_record_key,
};
use aruna_core::structs::storage::blob::{BackendLocation, HiddenBlobEntry, HiddenBlobKey};
use aruna_core::structs::storage::distribution::on_use::{
    ATTACHMENT_NAMESPACE, Attachment, CACHE_NAMESPACE, CacheEntry, attachment_key,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use crate::blob::migration::shared::MigrationError;

const SWEEP_PAGE_SIZE: usize = 128;
const SWEEP_BUDGET: Duration = Duration::from_secs(30);
const MAX_JOB_BYTES: usize = 1024 * 1024;
pub const HIDDEN_SWEEP_AFTER: Duration = Duration::from_secs(60 * 60);
pub const HIDDEN_SWEEP_RETRY: Duration = Duration::from_secs(30);
const ORPHAN_GRACE: Duration = Duration::from_secs(60 * 60);
const HIDDEN_CURSOR_KEY: &[u8] = b"hidden-sweep";

type Turn<T> = Result<Next<T>, MigrationError>;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct SweepCursor {
    phase: SweepPhase,
    start_after: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
enum SweepPhase {
    Cleanup,
    Uploads,
    Hidden,
}

impl Default for SweepCursor {
    fn default() -> Self {
        Self {
            phase: SweepPhase::Cleanup,
            start_after: None,
        }
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct HiddenSweepOutcome {
    pub cleanup_pending: bool,
    pub uploads_deleted: usize,
    pub orphans_deleted: usize,
}

fn failed(error: impl std::fmt::Display) -> MigrationError {
    MigrationError::failed(error)
}

/// Ends a sweep that used up its time; the next one goes on from the stored cursor.
fn within_budget(started_ms: u64) -> Result<(), MigrationError> {
    let spent = unix_timestamp_millis().saturating_sub(started_ms);
    if spent >= SWEEP_BUDGET.as_millis() as u64 {
        return Err(MigrationError::Failed(
            "hidden sweep budget exhausted".to_string(),
        ));
    }
    Ok(())
}

fn page_call(space: &str, after: Option<Vec<u8>>) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: Key::from(Vec::new()),
        after: after.map(Into::into),
        limit: SWEEP_PAGE_SIZE,
    }
}

fn delete_call(key: HiddenBlobKey) -> IoCall {
    Effect::Blob(BlobEffect::DeleteHidden { key }).into()
}

/// Takes the answer of a hidden blob delete.
fn hidden_deleted(event: Event) -> Result<(), MigrationError> {
    match event {
        Event::Blob(BlobEvent::HiddenDeleted) => Ok(()),
        Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => Err(no_blob_handle()),
        Event::Blob(BlobEvent::Error(error)) => Err(failed(error)),
        other => Err(MigrationError::Failed(format!(
            "unexpected hidden blob delete event: {}",
            other.label()
        ))),
    }
}

fn no_blob_handle() -> MigrationError {
    MigrationError::Failed("blob handle unavailable".to_string())
}

/// Deletes one hidden blob.
#[derive(Debug, PartialEq)]
pub struct DeleteHiddenOperation {
    key: HiddenBlobKey,
}

impl DeleteHiddenOperation {
    fn of_key(key: HiddenBlobKey) -> Self {
        Self { key }
    }
}

impl Operation for DeleteHiddenOperation {
    type Output = ();
    type Error = MigrationError;

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(delete_call(self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        hidden_deleted(result.event()?)?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum SweepStep {
    Cursor,
    Page(SweepPhase),
    Saved(HiddenSweepOutcome),
}

/// Runs one page of the phase the stored cursor names and stores where the next run goes on.
#[derive(Debug, PartialEq)]
pub struct ProcessHiddenOperation {
    now_ms: u64,
    started_ms: u64,
    step: SweepStep,
}

impl ProcessHiddenOperation {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            started_ms: 0,
            step: SweepStep::Cursor,
        }
    }

    fn run_phase(&mut self, cursor: SweepCursor) -> Turn<HiddenSweepOutcome> {
        let scope = PageScope {
            after: cursor.start_after,
            now_ms: self.now_ms,
            started_ms: self.started_ms,
        };
        self.step = SweepStep::Page(cursor.phase);
        Ok(Next::Call(match cursor.phase {
            SweepPhase::Cleanup => IoCall::child(CleanupPageOperation::new(scope)),
            SweepPhase::Uploads => IoCall::child(UploadsPageOperation::new(scope)),
            SweepPhase::Hidden => IoCall::child(HiddenPageOperation::new(scope)),
        }))
    }

    fn page_done(&mut self, phase: SweepPhase, pass: PagePass) -> Turn<HiddenSweepOutcome> {
        let more = pass.next.is_some();
        let next_phase = match phase {
            SweepPhase::Cleanup => SweepPhase::Uploads,
            SweepPhase::Uploads => SweepPhase::Hidden,
            SweepPhase::Hidden => SweepPhase::Cleanup,
        };
        let cursor = match more {
            true => SweepCursor {
                phase,
                start_after: pass.next,
            },
            false => SweepCursor {
                phase: next_phase,
                start_after: None,
            },
        };
        // Only the end of the last phase ends a cycle.
        let again = phase != SweepPhase::Hidden || more;
        let mut outcome = HiddenSweepOutcome {
            cleanup_pending: pass.pending || again,
            ..HiddenSweepOutcome::default()
        };
        match phase {
            SweepPhase::Cleanup => {}
            SweepPhase::Uploads => outcome.uploads_deleted = pass.deleted,
            SweepPhase::Hidden => outcome.orphans_deleted = pass.deleted,
        }
        let value = postcard::to_allocvec(&cursor).map_err(failed)?;
        let row = (
            NODE_STATE_KEYSPACE.to_string(),
            HIDDEN_CURSOR_KEY.to_vec().into(),
            value.into(),
        );
        self.step = SweepStep::Saved(outcome);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for ProcessHiddenOperation {
    type Output = HiddenSweepOutcome;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<HiddenSweepOutcome> {
        self.started_ms = unix_timestamp_millis();
        let read = read_row(NODE_STATE_KEYSPACE, HIDDEN_CURSOR_KEY.to_vec());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Turn<HiddenSweepOutcome> {
        match std::mem::replace(&mut self.step, SweepStep::Cursor) {
            SweepStep::Cursor => {
                let cursor = decode_cursor(result.value()?)?;
                self.run_phase(cursor)
            }
            SweepStep::Page(phase) => {
                let pass = result.child::<PagePass, MigrationError>()??;
                self.page_done(phase, pass)
            }
            SweepStep::Saved(outcome) => {
                result.done()?;
                Ok(Next::Done(outcome))
            }
        }
    }
}

fn decode_cursor(value: Option<Value>) -> Result<SweepCursor, MigrationError> {
    match value {
        Some(value) => postcard::from_bytes(value.as_ref()).map_err(failed),
        None => Ok(SweepCursor::default()),
    }
}

/// Where one sweep page starts and the clock it runs on.
#[derive(Debug, PartialEq)]
struct PageScope {
    after: Option<Vec<u8>>,
    now_ms: u64,
    started_ms: u64,
}

/// What one sweep page did.
#[derive(Debug, Default, PartialEq)]
struct PagePass {
    deleted: usize,
    /// Rows stayed that a later run must look at again.
    pending: bool,
    /// Where the next page of this phase starts; `None` after the last one.
    next: Option<Vec<u8>>,
}

fn take_page(
    result: IoResult,
    pass: &mut PagePass,
) -> Result<VecDeque<(Key, Value)>, MigrationError> {
    let (rows, next) = result.page()?;
    pass.next = next.map(|key| key.to_vec());
    Ok(rows.into())
}

#[derive(Debug, PartialEq)]
enum CleanupStep {
    Page,
    Claim(Key, HiddenBlobKey),
    Blob(Key),
    Row,
}

/// Compensates the upload records of one page of cleanup rows and deletes their blobs.
#[derive(Debug, PartialEq)]
struct CleanupPageOperation {
    scope: PageScope,
    rows: VecDeque<(Key, Value)>,
    pass: PagePass,
    step: CleanupStep,
}

impl CleanupPageOperation {
    fn new(scope: PageScope) -> Self {
        Self {
            scope,
            rows: VecDeque::new(),
            pass: PagePass::default(),
            step: CleanupStep::Page,
        }
    }

    fn next_row(&mut self) -> Turn<PagePass> {
        while let Some((key, value)) = self.rows.pop_front() {
            within_budget(self.scope.started_ms)?;
            let cleanup = match RoCrateUploadCleanup::from_bytes(&value) {
                Ok(cleanup) => cleanup,
                Err(error) => {
                    warn!(error = %error, "Failed to decode RO-Crate upload cleanup");
                    continue;
                }
            };
            if key.as_ref() != cleanup.upload_id.to_bytes().as_slice() {
                warn!("RO-Crate upload cleanup key does not match its upload id");
                self.pass.pending = true;
                continue;
            }
            let claim = DeleteUnclaimedOperation::new(cleanup.upload_id, self.scope.now_ms);
            self.step = CleanupStep::Claim(key, cleanup.hidden_key);
            return Ok(Next::Call(IoCall::child(claim)));
        }
        Ok(Next::Done(std::mem::take(&mut self.pass)))
    }

    fn claimed(
        &mut self,
        key: Key,
        hidden: HiddenBlobKey,
        claim: Result<RecordCleanup, MigrationError>,
    ) -> Turn<PagePass> {
        match claim {
            Ok(RecordCleanup::Protected) => self.pass.pending = true,
            Ok(RecordCleanup::Missing | RecordCleanup::Deleted) => {
                let delete = DeleteHiddenOperation::of_key(hidden);
                self.step = CleanupStep::Blob(key);
                return Ok(Next::Call(IoCall::child(delete)));
            }
            Err(error) => {
                warn!(error = %error, "Failed to compensate RO-Crate upload record");
                self.pass.pending = true;
            }
        }
        self.next_row()
    }
}

impl Operation for CleanupPageOperation {
    type Output = PagePass;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<PagePass> {
        let after = self.scope.after.take();
        Ok(Next::Call(page_call(UPLOAD_CLEANUP_KEYSPACE, after)))
    }

    fn next(&mut self, result: IoResult) -> Turn<PagePass> {
        match std::mem::replace(&mut self.step, CleanupStep::Row) {
            CleanupStep::Page => {
                self.rows = take_page(result, &mut self.pass)?;
                self.next_row()
            }
            CleanupStep::Claim(key, hidden) => self.claimed(key, hidden, result.child()?),
            CleanupStep::Blob(key) => {
                if let Err(error) = result.child::<(), MigrationError>()? {
                    warn!(error = %error, "Failed to delete upload cleanup blob");
                    self.pass.pending = true;
                    return self.next_row();
                }
                let delete = DeleteRowOperation { key: Some(key) };
                self.step = CleanupStep::Row;
                Ok(Next::Call(IoCall::child(delete)))
            }
            CleanupStep::Row => {
                if let Err(error) = result.child::<(), MigrationError>()? {
                    warn!(error = %error, "Failed to delete upload cleanup row");
                    self.pass.pending = true;
                }
                self.next_row()
            }
        }
    }
}

/// Deletes one upload cleanup row; a failure leaves it for a later sweep.
#[derive(Debug, PartialEq)]
struct DeleteRowOperation {
    key: Option<Key>,
}

impl Operation for DeleteRowOperation {
    type Output = ();
    type Error = MigrationError;

    fn first(&mut self) -> Turn<()> {
        let key = self.key.take().ok_or_else(|| failed("no cleanup row"))?;
        let row = (UPLOAD_CLEANUP_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum UploadsStep {
    Page,
    Claim(HiddenBlobKey),
    Blob,
}

/// Claims and deletes the stale RO-Crate uploads of one page.
#[derive(Debug, PartialEq)]
struct UploadsPageOperation {
    scope: PageScope,
    rows: VecDeque<(Key, Value)>,
    pass: PagePass,
    step: UploadsStep,
}

impl UploadsPageOperation {
    fn new(scope: PageScope) -> Self {
        Self {
            scope,
            rows: VecDeque::new(),
            pass: PagePass::default(),
            step: UploadsStep::Page,
        }
    }

    fn next_row(&mut self) -> Turn<PagePass> {
        while let Some((key, value)) = self.rows.pop_front() {
            within_budget(self.scope.started_ms)?;
            let record: RoCrateUploadRecord = match postcard::from_bytes(&value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "Failed to decode RO-Crate upload");
                    continue;
                }
            };
            if key.as_ref() != record.upload_id.to_bytes().as_slice() {
                warn!("RO-Crate upload key does not match its upload id");
                self.pass.pending = true;
                continue;
            }
            let hidden = match HiddenBlobKey::try_from(&record.location) {
                Ok(hidden) => hidden,
                Err(error) => {
                    warn!(error = %error, "Invalid RO-Crate upload location");
                    continue;
                }
            };
            let claim = DeleteUnclaimedOperation::new(record.upload_id, self.scope.now_ms);
            self.step = UploadsStep::Claim(hidden);
            return Ok(Next::Call(IoCall::child(claim)));
        }
        Ok(Next::Done(std::mem::take(&mut self.pass)))
    }
}

impl Operation for UploadsPageOperation {
    type Output = PagePass;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<PagePass> {
        let after = self.scope.after.take();
        Ok(Next::Call(page_call(ROCRATE_UPLOAD_KEYSPACE, after)))
    }

    fn next(&mut self, result: IoResult) -> Turn<PagePass> {
        match std::mem::replace(&mut self.step, UploadsStep::Blob) {
            UploadsStep::Page => {
                self.rows = take_page(result, &mut self.pass)?;
                self.next_row()
            }
            UploadsStep::Claim(hidden) => match result.child::<RecordCleanup, MigrationError>()? {
                Ok(RecordCleanup::Protected) => self.next_row(),
                Ok(_) => {
                    let delete = DeleteHiddenOperation::of_key(hidden);
                    self.step = UploadsStep::Blob;
                    Ok(Next::Call(IoCall::child(delete)))
                }
                Err(error) => {
                    warn!(error = %error, "Failed to claim stale RO-Crate upload cleanup");
                    self.pass.pending = true;
                    self.next_row()
                }
            },
            UploadsStep::Blob => {
                match result.child::<(), MigrationError>()? {
                    Ok(()) => self.pass.deleted = self.pass.deleted.saturating_add(1),
                    Err(error) => {
                        warn!(error = %error, "Failed to delete stale upload blob");
                        self.pass.pending = true;
                    }
                }
                self.next_row()
            }
        }
    }
}

fn upload_is_live(record: &RoCrateUploadRecord, claimed_live: bool, now_ms: u64) -> bool {
    match record.claimed_by {
        Some(_) => claimed_live,
        None => record.expires_at_ms > now_ms,
    }
}

fn hidden_is_old(entry: &HiddenBlobEntry, cutoff_ms: u64) -> bool {
    entry.modified_at.is_some_and(|modified| {
        modified
            .duration_since(UNIX_EPOCH)
            .ok()
            .is_some_and(|time| time.as_millis() as u64 <= cutoff_ms)
    })
}

#[derive(Debug, PartialEq)]
enum HiddenStep {
    List,
    Protected(HiddenBlobKey),
    Blob,
}

/// Deletes the old hidden blobs of one listing page that nothing protects.
#[derive(Debug, PartialEq)]
struct HiddenPageOperation {
    scope: PageScope,
    entries: VecDeque<HiddenBlobEntry>,
    pass: PagePass,
    step: HiddenStep,
}

impl HiddenPageOperation {
    fn new(scope: PageScope) -> Self {
        Self {
            scope,
            entries: VecDeque::new(),
            pass: PagePass::default(),
            step: HiddenStep::List,
        }
    }

    fn listed(&mut self, event: Event) -> Turn<PagePass> {
        match event {
            Event::Blob(BlobEvent::HiddenListed {
                entries,
                next_cursor,
            }) => {
                self.entries = entries.into();
                self.pass.next = next_cursor;
                self.next_entry()
            }
            Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => Err(no_blob_handle()),
            Event::Blob(BlobEvent::Error(error)) => Err(failed(error)),
            other => Err(MigrationError::Failed(format!(
                "unexpected hidden blob list event: {}",
                other.label()
            ))),
        }
    }

    fn next_entry(&mut self) -> Turn<PagePass> {
        let grace = ORPHAN_GRACE.as_millis() as u64;
        let cutoff_ms = self.scope.now_ms.saturating_sub(grace);
        while let Some(entry) = self.entries.pop_front() {
            within_budget(self.scope.started_ms)?;
            if !hidden_is_old(&entry, cutoff_ms) {
                continue;
            }
            let check = IsProtectedOperation::new(entry.key.clone(), self.scope.now_ms);
            self.step = HiddenStep::Protected(entry.key);
            return Ok(Next::Call(IoCall::child(check)));
        }
        Ok(Next::Done(std::mem::take(&mut self.pass)))
    }
}

impl Operation for HiddenPageOperation {
    type Output = PagePass;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<PagePass> {
        let list = BlobEffect::ListHidden {
            namespace: None,
            cursor: self.scope.after.take(),
        };
        Ok(Next::Call(Effect::Blob(list).into()))
    }

    fn next(&mut self, result: IoResult) -> Turn<PagePass> {
        match std::mem::replace(&mut self.step, HiddenStep::Blob) {
            HiddenStep::List => self.listed(result.event()?),
            HiddenStep::Protected(key) => match result.child::<bool, MigrationError>()? {
                Ok(true) => self.next_entry(),
                Ok(false) => {
                    let delete = DeleteHiddenOperation::of_key(key);
                    self.step = HiddenStep::Blob;
                    Ok(Next::Call(IoCall::child(delete)))
                }
                Err(error) => {
                    warn!(error = %error, "Failed to validate hidden blob ownership");
                    self.pass.pending = true;
                    self.next_entry()
                }
            },
            HiddenStep::Blob => {
                match result.child::<(), MigrationError>()? {
                    Ok(()) => self.pass.deleted = self.pass.deleted.saturating_add(1),
                    Err(error) => {
                        warn!(error = %error, "Failed to delete hidden orphan");
                        self.pass.pending = true;
                    }
                }
                self.next_entry()
            }
        }
    }
}

fn job_row(value: &[u8]) -> Result<JobRecord, MigrationError> {
    if value.len() > MAX_JOB_BYTES {
        return Err(MigrationError::Failed(
            "job record exceeds limit".to_string(),
        ));
    }
    JobRecord::from_bytes(value).map_err(failed)
}

/// Whether a stored job row keeps an RO-Crate job running.
fn job_live(value: Option<Value>) -> Result<bool, MigrationError> {
    let Some(value) = value else {
        return Ok(false);
    };
    let record = job_row(&value)?;
    Ok(!record.state.is_terminal() && record.payload.is_rocrate())
}

/// The row a kept copy's name points at: set and version id for an attachment, the hex BLAKE3
/// for a cache entry.
fn kept_row(key: &HiddenBlobKey, namespace: Ulid) -> Option<(&'static str, Vec<u8>)> {
    let name = std::path::Path::new(&key.backend_path)
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.rsplit_once('_'))
        .map(|(name, _)| name)?;
    if namespace == ATTACHMENT_NAMESPACE {
        let (set, version) = name.split_once('-')?;
        let (set, version) = (
            Ulid::from_string(set).ok()?,
            Ulid::from_string(version).ok()?,
        );
        return Some((ATTACHMENT_KEYSPACE, attachment_key(set, version)));
    }
    let blake3 = hex::decode(name).ok().filter(|blake3| blake3.len() == 32)?;
    Some((CACHE_ENTRY_KEYSPACE, blake3))
}

#[derive(Debug, PartialEq)]
enum ProtectStep {
    Kept(bool),
    Upload,
    UploadJob(Box<RoCrateUploadRecord>),
    Job,
}

/// Whether something still needs a hidden blob: a kept copy's row, a live upload or a job.
#[derive(Debug, PartialEq)]
struct IsProtectedOperation {
    key: HiddenBlobKey,
    now_ms: u64,
    namespace: Option<Ulid>,
    step: ProtectStep,
}

impl IsProtectedOperation {
    fn new(key: HiddenBlobKey, now_ms: u64) -> Self {
        Self {
            key,
            now_ms,
            namespace: None,
            step: ProtectStep::Upload,
        }
    }

    fn read_job(&mut self) -> Turn<bool> {
        let namespace = self.namespace.ok_or_else(|| failed("no blob namespace"))?;
        let Ok(job_id) = JobId::try_from_bytes(namespace.to_bytes()) else {
            return Ok(Next::Done(false));
        };
        self.step = ProtectStep::Job;
        Ok(Next::Call(read_row(JOB_KEYSPACE, job_record_key(job_id))))
    }

    fn upload_read(&mut self, value: Option<Value>) -> Turn<bool> {
        let Some(value) = value else {
            return self.read_job();
        };
        if value.len() > MAX_JOB_BYTES {
            return Err(MigrationError::Failed(
                "RO-Crate upload record exceeds limit".to_string(),
            ));
        }
        let record: RoCrateUploadRecord = postcard::from_bytes(&value).map_err(failed)?;
        let record_key = HiddenBlobKey::try_from(&record.location).map_err(failed)?;
        if record_key != self.key {
            return self.read_job();
        }
        match record.claimed_by {
            Some(job_id) => {
                self.step = ProtectStep::UploadJob(Box::new(record));
                Ok(Next::Call(read_row(JOB_KEYSPACE, job_record_key(job_id))))
            }
            None => self.upload_checked(&record, false),
        }
    }

    fn upload_checked(&mut self, record: &RoCrateUploadRecord, claimed: bool) -> Turn<bool> {
        match upload_is_live(record, claimed, self.now_ms) {
            true => Ok(Next::Done(true)),
            false => self.read_job(),
        }
    }

    fn job_read(&self, value: Option<Value>) -> Turn<bool> {
        let Some(value) = value else {
            return Ok(Next::Done(false));
        };
        let record = job_row(&value)?;
        if !record.state.is_terminal() && record.payload.is_rocrate() {
            return Ok(Next::Done(true));
        }
        if let Some(JobResultPayload::ExportRoCrate(result)) = &record.result
            && let Some(artifact) = &result.artifact
        {
            let artifact_key = HiddenBlobKey::try_from(&artifact.location).map_err(failed)?;
            return Ok(Next::Done(artifact_key == self.key));
        }
        Ok(Next::Done(false))
    }

    fn kept_read(&self, attachment: bool, value: Option<Value>) -> Turn<bool> {
        let Some(value) = value else {
            return Ok(Next::Done(false));
        };
        let location = match attachment {
            true => Attachment::from_bytes(&value).map(|kept| kept.location),
            false => CacheEntry::from_bytes(&value).map(|kept| kept.location),
        }
        .map_err(failed)?;
        let same = HiddenBlobKey::try_from(&location).is_ok_and(|kept| kept == self.key);
        Ok(Next::Done(same))
    }
}

impl Operation for IsProtectedOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        let namespace = self.key.namespace()?;
        self.namespace = Some(namespace);
        if namespace == ATTACHMENT_NAMESPACE || namespace == CACHE_NAMESPACE {
            // An evicted copy a read still holds stays until the read ends.
            if crate::distribution::pull::pinned_path(&self.key.backend_path) {
                return Ok(Next::Done(true));
            }
            let Some((key_space, row_key)) = kept_row(&self.key, namespace) else {
                return Ok(Next::Done(false));
            };
            self.step = ProtectStep::Kept(namespace == ATTACHMENT_NAMESPACE);
            return Ok(Next::Call(read_row(key_space, row_key)));
        }
        let key = namespace.to_bytes().to_vec();
        Ok(Next::Call(read_row(ROCRATE_UPLOAD_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match std::mem::replace(&mut self.step, ProtectStep::Job) {
            ProtectStep::Kept(attachment) => self.kept_read(attachment, result.value()?),
            ProtectStep::Upload => self.upload_read(result.value()?),
            ProtectStep::UploadJob(record) => {
                let claimed = job_live(result.value()?)?;
                self.upload_checked(&record, claimed)
            }
            ProtectStep::Job => self.job_read(result.value()?),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordCleanup {
    Missing,
    Protected,
    Deleted,
}

#[derive(Debug, PartialEq)]
enum ClaimStep {
    Record,
    Job(Box<RoCrateUploadRecord>),
    Deleted,
}

/// Removes an upload record nothing claims any more, in one transaction with its check.
#[derive(Debug, PartialEq)]
struct DeleteUnclaimedOperation {
    upload_id: Ulid,
    now_ms: u64,
    step: ClaimStep,
}

impl DeleteUnclaimedOperation {
    fn new(upload_id: Ulid, now_ms: u64) -> Self {
        Self {
            upload_id,
            now_ms,
            step: ClaimStep::Record,
        }
    }

    fn row_key(&self) -> Key {
        self.upload_id.to_bytes().to_vec().into()
    }

    fn decide(&mut self, record: &RoCrateUploadRecord, claimed: bool) -> Turn<RecordCleanup> {
        if upload_is_live(record, claimed, self.now_ms) {
            return Ok(Next::Done(RecordCleanup::Protected));
        }
        self.step = ClaimStep::Deleted;
        let row = (ROCRATE_UPLOAD_KEYSPACE.to_string(), self.row_key());
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for DeleteUnclaimedOperation {
    type Output = RecordCleanup;
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<RecordCleanup> {
        Ok(Next::Call(read_row(
            ROCRATE_UPLOAD_KEYSPACE,
            self.row_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<RecordCleanup> {
        match std::mem::replace(&mut self.step, ClaimStep::Deleted) {
            ClaimStep::Record => {
                let Some(value) = result.value()? else {
                    return Ok(Next::Done(RecordCleanup::Missing));
                };
                let record: RoCrateUploadRecord =
                    postcard::from_bytes(value.as_ref()).map_err(failed)?;
                match record.claimed_by {
                    Some(job_id) => {
                        self.step = ClaimStep::Job(Box::new(record));
                        Ok(Next::Call(read_row(JOB_KEYSPACE, job_record_key(job_id))))
                    }
                    None => self.decide(&record, false),
                }
            }
            ClaimStep::Job(record) => {
                let claimed = job_live(result.value()?)?;
                self.decide(&record, claimed)
            }
            ClaimStep::Deleted => {
                result.done()?;
                Ok(Next::Done(RecordCleanup::Deleted))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::execution::job::{
        ArtifactRef, ExportRoCrateResult, ExportRoCrateSpec, JobPayload, JobState,
        RoCrateCheckpointRefs, RoCrateLimits, RoCrateMediaType,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_storage::FjallStorage;
    use aruna_storage::StorageHandle;
    use byteview::ByteView;
    use serde::Serialize;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn upload_record(
        upload_id: Ulid,
        expires_at_ms: u64,
        claimed_by: Option<JobId>,
    ) -> RoCrateUploadRecord {
        RoCrateUploadRecord {
            upload_id,
            owner: UserId::nil(RealmId::from_bytes([1u8; 32])),
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: format!("_jobs/{upload_id}/input_01"),
                ulid: Ulid::from_bytes([2u8; 16]),
                format: StoredFormat::default(),
                created_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
                created_at: UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: HashMap::new(),
            },
            blake3: [3u8; 32],
            size: 1,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by,
        }
    }

    #[test]
    fn upload_liveness() {
        let upload_id = Ulid::from_bytes([4u8; 16]);
        let job_id = JobId::from_bytes([5u8; 16]);

        assert!(upload_is_live(
            &upload_record(upload_id, 11, None),
            false,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 10, None),
            false,
            10
        ));
        assert!(upload_is_live(
            &upload_record(upload_id, 0, Some(job_id)),
            true,
            10
        ));
        assert!(!upload_is_live(
            &upload_record(upload_id, 0, Some(job_id)),
            false,
            10
        ));
    }

    #[test]
    fn orphan_selection() {
        let namespace = Ulid::from_bytes([7u8; 16]);
        let key = HiddenBlobKey::new(
            BackendRef::node_default(),
            "/data".to_string(),
            "storage".to_string(),
            format!("_jobs/{namespace}/artifact_01"),
        )
        .unwrap();
        let entry = HiddenBlobEntry {
            key,
            modified_at: Some(UNIX_EPOCH),
        };

        assert!(hidden_is_old(&entry, 0));
        assert!(!hidden_is_old(
            &HiddenBlobEntry {
                key: entry.key.clone(),
                modified_at: Some(UNIX_EPOCH + Duration::from_millis(1)),
            },
            0
        ));
        assert!(!hidden_is_old(
            &HiddenBlobEntry {
                key: entry.key,
                modified_at: None,
            },
            0
        ));
    }

    #[test]
    fn checkpoint_prefix_decodes() {
        #[derive(Serialize)]
        struct Checkpoint {
            refs: RoCrateCheckpointRefs,
            phase: u8,
        }

        let location = upload_record(Ulid::from_bytes([8u8; 16]), 10, None).location;
        let encoded = postcard::to_allocvec(&Checkpoint {
            refs: RoCrateCheckpointRefs {
                hidden_locations: vec![location.clone()],
            },
            phase: 3,
        })
        .unwrap();
        let (refs, remainder) =
            postcard::take_from_bytes::<RoCrateCheckpointRefs>(&encoded).unwrap();

        assert_eq!(refs.hidden_locations, vec![location]);
        assert!(!remainder.is_empty());
    }

    fn temp_context() -> (tempfile::TempDir, DriverContext) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (
            dir,
            DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                graph_handle: None,
                task_handle: None,
                compute_handle: None,
                outbound_handle: None,
            },
        )
    }

    async fn is_protected(
        context: &DriverContext,
        key: &HiddenBlobKey,
        now_ms: u64,
    ) -> Result<bool, MigrationError> {
        drive(IsProtectedOperation::new(key.clone(), now_ms), context).await
    }

    async fn delete_unclaimed(
        context: &DriverContext,
        upload_id: Ulid,
        now_ms: u64,
    ) -> Result<RecordCleanup, MigrationError> {
        drive(DeleteUnclaimedOperation::new(upload_id, now_ms), context).await
    }

    async fn sweep_uploads(
        context: &DriverContext,
        now_ms: u64,
    ) -> Result<(usize, bool, bool), MigrationError> {
        let scope = PageScope {
            after: None,
            now_ms,
            started_ms: unix_timestamp_millis(),
        };
        let pass = drive(UploadsPageOperation::new(scope), context).await?;
        Ok((pass.deleted, pass.next.is_some(), pass.pending))
    }

    async fn sweep_at(context: &DriverContext, now_ms: u64) -> Result<HiddenSweepOutcome, String> {
        drive(ProcessHiddenOperation::new(now_ms), context)
            .await
            .map_err(|error| error.to_string())
    }

    async fn load_cursor(context: &DriverContext) -> SweepCursor {
        let read = StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: HIDDEN_CURSOR_KEY.to_vec().into(),
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                decode_cursor(value).expect("cursor decodes")
            }
            other => panic!("unexpected cursor read: {}", other.label()),
        }
    }

    #[test]
    fn missing_handle_named() {
        let mut delete = DeleteHiddenOperation::of_key(
            HiddenBlobKey::new(
                aruna_core::structs::storage::blob::BackendRef::node_default(),
                "/data".to_string(),
                "storage".to_string(),
                format!("_jobs/{}/artifact_01", Ulid::from_bytes([7u8; 16])),
            )
            .unwrap(),
        );
        assert!(matches!(delete.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let missing = Event::Blob(BlobEvent::Error(BlobError::HandleMissing));
        let answer = delete.next(IoResult::Event(Box::new(missing)));
        assert_eq!(answer, Err(no_blob_handle()));
    }

    #[test]
    fn missing_upload_unclaimed() {
        let mut claim = DeleteUnclaimedOperation::new(Ulid::from_bytes([1; 16]), 5);
        assert!(matches!(claim.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = claim.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(RecordCleanup::Missing)));
    }

    #[test]
    fn expired_budget_stops() {
        assert!(within_budget(0).is_err());
        assert!(within_budget(unix_timestamp_millis()).is_ok());
    }

    fn hidden_location(namespace: Ulid, name: &str) -> BackendLocation {
        let mut location = upload_record(namespace, 0, None).location;
        location.backend_path = format!("_jobs/{namespace}/{name}");
        location
    }

    fn job_record(job_id: JobId, state: JobState) -> JobRecord {
        let realm = RealmId::from_bytes([1u8; 32]);
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ExportRoCrate(ExportRoCrateSpec {
                selection: None,
                destination: None,
                auth_context: AuthContext {
                    user_id: UserId::nil(realm),
                    realm_id: realm,
                    path_restrictions: None,
                    session: None,
                },
                document_id: Ulid::from_bytes([2u8; 16]),
                limits: RoCrateLimits::default(),
            }),
            UserId::nil(realm),
            iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            0,
            0,
            None,
        );
        record.state = state;
        record
    }

    fn export_result(location: BackendLocation) -> JobResultPayload {
        JobResultPayload::ExportRoCrate(ExportRoCrateResult {
            repository: None,
            artifact: Some(ArtifactRef {
                location,
                blake3: [0u8; 32],
                size: 1,
                expires_at_ms: 0,
            }),
            included: 0,
            omitted: Default::default(),
            report_digest: [0u8; 32],
        })
    }

    async fn write_row(storage: &StorageHandle, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: ByteView::from(key),
                    value: ByteView::from(value),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn stored_upload(storage: &StorageHandle, upload_id: Ulid) -> bool {
        let read = StorageEffect::Read {
            key_space: ROCRATE_UPLOAD_KEYSPACE.to_string(),
            key: upload_id.to_bytes().to_vec().into(),
            txn_id: None,
        };
        match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected upload read: {}", other.label()),
        }
    }

    // A kept copy is protected only while its attachment or cache entry names exactly it.
    #[tokio::test]
    async fn kept_copies_guarded() {
        let (_dir, context) = temp_context();
        let (set, version) = (Ulid::from_bytes([6u8; 16]), Ulid::from_bytes([7u8; 16]));
        let name = format!("{set}-{version}");
        let attached = hidden_location(ATTACHMENT_NAMESPACE, &format!("{name}_{}", Ulid::nil()));
        let row = Attachment {
            blake3: [1u8; 32],
            location: attached.clone(),
        };
        write_row(
            &context.storage_handle,
            ATTACHMENT_KEYSPACE,
            attachment_key(set, version),
            row.to_bytes().expect("encode attachment"),
        )
        .await;
        let key = HiddenBlobKey::try_from(&attached).expect("hidden key");
        let leftover = hidden_location(ATTACHMENT_NAMESPACE, &format!("{name}_{set}"));
        let leftover = HiddenBlobKey::try_from(&leftover).expect("hidden key");
        let cached = hidden_location(
            CACHE_NAMESPACE,
            &format!("{}_{}", hex::encode([2u8; 32]), set),
        );
        let cached = HiddenBlobKey::try_from(&cached).expect("hidden key");

        assert!(is_protected(&context, &key, 0).await.unwrap());
        assert!(!is_protected(&context, &leftover, 0).await.unwrap());
        assert!(!is_protected(&context, &cached, 0).await.unwrap());
    }

    #[tokio::test]
    async fn upload_guards_liveness() {
        // Only the clock moves between the protected and the sweepable case.
        let (_dir, context) = temp_context();
        let upload_id = Ulid::from_bytes([4u8; 16]);
        let record = upload_record(upload_id, 10, None);
        let key = HiddenBlobKey::try_from(&record.location).expect("hidden key");
        write_row(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
            postcard::to_allocvec(&record).expect("encode upload"),
        )
        .await;

        assert!(is_protected(&context, &key, 9).await.expect("live upload"));
        assert!(
            !is_protected(&context, &key, 10)
                .await
                .expect("expired upload")
        );
    }

    #[tokio::test]
    async fn upload_guards_key() {
        // A live record protects the blob it points at, not its whole namespace.
        let (_dir, context) = temp_context();
        let upload_id = Ulid::from_bytes([5u8; 16]);
        let record = upload_record(upload_id, 10, None);
        let key = HiddenBlobKey::try_from(&record.location).expect("hidden key");
        let sibling = HiddenBlobKey::try_from(&hidden_location(upload_id, "input_02"))
            .expect("sibling hidden key");
        write_row(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
            postcard::to_allocvec(&record).expect("encode upload"),
        )
        .await;

        assert!(
            is_protected(&context, &key, 9)
                .await
                .expect("recorded blob")
        );
        assert!(
            !is_protected(&context, &sibling, 9)
                .await
                .expect("unrecorded blob")
        );
    }

    #[tokio::test]
    async fn job_guards_hidden() {
        // A running export owns its namespace; once terminal it keeps only the
        // artifact it published.
        let (_dir, context) = temp_context();
        let job_id = JobId::from_bytes([5u8; 16]);
        let namespace = Ulid::from_bytes(job_id.to_bytes());
        let artifact = hidden_location(namespace, "artifact_01");
        let key = HiddenBlobKey::try_from(&artifact).expect("hidden key");
        let scratch = HiddenBlobKey::try_from(&hidden_location(namespace, "artifact_02"))
            .expect("scratch hidden key");
        let running = job_record(job_id, JobState::Running);
        write_row(
            &context.storage_handle,
            JOB_KEYSPACE,
            job_record_key(job_id).to_vec(),
            running.to_bytes().expect("encode job"),
        )
        .await;

        assert!(
            is_protected(&context, &scratch, 0)
                .await
                .expect("running job")
        );

        let mut finished = job_record(job_id, JobState::Succeeded);
        finished.result = Some(export_result(artifact));
        write_row(
            &context.storage_handle,
            JOB_KEYSPACE,
            job_record_key(job_id).to_vec(),
            finished.to_bytes().expect("encode job"),
        )
        .await;

        assert!(
            is_protected(&context, &key, 0)
                .await
                .expect("published artifact")
        );
        assert!(
            !is_protected(&context, &scratch, 0)
                .await
                .expect("finished scratch blob")
        );
    }

    #[tokio::test]
    async fn cleanup_keeps_live() {
        // The claim only removes a record that is no longer live.
        let (_dir, context) = temp_context();
        let upload_id = Ulid::from_bytes([6u8; 16]);
        write_row(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
            postcard::to_allocvec(&upload_record(upload_id, 10, None)).expect("encode upload"),
        )
        .await;

        assert_eq!(
            delete_unclaimed(&context, upload_id, 9)
                .await
                .expect("live record"),
            RecordCleanup::Protected
        );
        assert!(stored_upload(&context.storage_handle, upload_id).await);
        assert_eq!(
            delete_unclaimed(&context, upload_id, 10)
                .await
                .expect("expired record"),
            RecordCleanup::Deleted
        );
        assert!(!stored_upload(&context.storage_handle, upload_id).await);
        assert_eq!(
            delete_unclaimed(&context, upload_id, 10)
                .await
                .expect("absent record"),
            RecordCleanup::Missing
        );
    }

    #[tokio::test]
    async fn cleanup_checks_claim() {
        // An expired record claimed by a job lives exactly as long as that job.
        let (_dir, context) = temp_context();
        let job_id = JobId::from_bytes([5u8; 16]);
        let upload_id = Ulid::from_bytes([7u8; 16]);
        write_row(
            &context.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
            postcard::to_allocvec(&upload_record(upload_id, 0, Some(job_id)))
                .expect("encode upload"),
        )
        .await;
        write_row(
            &context.storage_handle,
            JOB_KEYSPACE,
            job_record_key(job_id).to_vec(),
            job_record(job_id, JobState::Running)
                .to_bytes()
                .expect("encode job"),
        )
        .await;

        assert_eq!(
            delete_unclaimed(&context, upload_id, 10)
                .await
                .expect("claimed by a running job"),
            RecordCleanup::Protected
        );
        write_row(
            &context.storage_handle,
            JOB_KEYSPACE,
            job_record_key(job_id).to_vec(),
            job_record(job_id, JobState::Failed)
                .to_bytes()
                .expect("encode job"),
        )
        .await;
        assert_eq!(
            delete_unclaimed(&context, upload_id, 10)
                .await
                .expect("claimed by a finished job"),
            RecordCleanup::Deleted
        );
    }

    #[tokio::test]
    async fn upload_key_mismatch() {
        // A row filed under a foreign key must not drive cleanup of the upload id
        // it names. Blob deletion is unreachable here, so both passes stay pending.
        let (_dir, mismatched) = temp_context();
        let victim = Ulid::from_bytes([8u8; 16]);
        let record = postcard::to_allocvec(&upload_record(victim, 0, None)).expect("encode upload");
        write_row(
            &mismatched.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            Ulid::from_bytes([9u8; 16]).to_bytes().to_vec(),
            record.clone(),
        )
        .await;

        let outcome = sweep_uploads(&mismatched, 10)
            .await
            .expect("scan completes");
        assert_eq!(outcome, (0, false, true));
        assert!(
            stored_upload(&mismatched.storage_handle, Ulid::from_bytes([9u8; 16])).await,
            "the mismatched row is retained"
        );

        let (_other_dir, keyed) = temp_context();
        write_row(
            &keyed.storage_handle,
            ROCRATE_UPLOAD_KEYSPACE,
            victim.to_bytes().to_vec(),
            record,
        )
        .await;
        assert!(sweep_uploads(&keyed, 10).await.expect("scan completes").2);
        assert!(
            !stored_upload(&keyed.storage_handle, victim).await,
            "a correctly keyed stale row is claimed"
        );
    }

    #[tokio::test]
    async fn sweep_advances_phase() {
        // The sweep persists its phase and keeps asking for another pass until a
        // cycle completes; a phase that fails is retried, not skipped.
        let (_dir, context) = temp_context();
        let first = sweep_at(&context, 0).await.expect("cleanup phase");
        assert!(first.cleanup_pending, "the cycle is unfinished");
        assert!(matches!(
            load_cursor(&context).await.phase,
            SweepPhase::Uploads
        ));

        let second = sweep_at(&context, 0).await.expect("uploads phase");
        assert_eq!(second.uploads_deleted, 0);
        assert!(second.cleanup_pending);
        assert!(matches!(
            load_cursor(&context).await.phase,
            SweepPhase::Hidden
        ));

        let error = sweep_at(&context, 0)
            .await
            .expect_err("the hidden phase needs a blob adapter");
        assert!(error.contains("blob handle"), "{error}");
        assert!(matches!(
            load_cursor(&context).await.phase,
            SweepPhase::Hidden
        ));
    }
}
