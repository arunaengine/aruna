//! Runs a storage purge in turns: each turn is one operation up to the next point where the job
//! may stop, so the caller checks for a cancel or shutdown between turns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, RENAMED_ROW_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, JobId, JobProgress};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation, object_permission_path};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::release::MembershipQuery;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::structs::storage::storage_purge::{
    StoragePurgeCheckpoint, StoragePurgeResult, StoragePurgeScope, StoragePurgeSpec,
};
use aruna_core::types::Key;
use ulid::Ulid;

use super::super::store::JobMutationError;
use super::super::store::job_calls::{RenewOutcome, flush_progress, put_purge_checkpoint};
use super::super::store::row_calls::read_purge_checkpoint;
use super::purge_audit::{WriteAuditOperation, purge_audit_record};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::blob::pause::{PauseError, PausedOperation};
use crate::blob::purge::{PlanFilesOperation, PurgeFilesError, StorageFile, StorageFilesOperation};
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::release::BucketScope;
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use crate::s3::multipart::uploads::{
    ListUploadsError, ListUploadsInput, ListUploadsOperation, ListUploadsResult,
};
use crate::s3::object::delete::DeleteObjectError;
use crate::s3::object::delete::bulk::{
    BulkDeleteEntry, BulkDeleteInput, BulkDeleteOperation, BulkDeleteOutcome,
};
use crate::s3::object::delete::pinned::{PinnedError, ReleaseGuards, VersionPinsOperation};
use crate::s3::object::versions::{
    ListVersionsError, ListVersionsInput, ListVersionsItem, ListVersionsOperation,
    ListVersionsResult,
};

const PURGE_BATCH_SIZE: usize = 1_000;

/// Why a purge turn ended the run.
#[derive(Debug, PartialEq)]
pub enum PurgeStop {
    /// The job record asks for a cancel.
    Cancelled,
    Failed(JobError),
}

impl From<JobError> for PurgeStop {
    fn from(error: JobError) -> Self {
        Self::Failed(error)
    }
}

impl From<IoError> for PurgeStop {
    fn from(error: IoError) -> Self {
        Self::Failed(error.into())
    }
}

fn retry(message: impl Into<String>) -> PurgeStop {
    PurgeStop::Failed(JobError::retryable(message))
}

/// Which count a page belongs to: the first one fixes the job's total.
#[derive(Clone, Copy, Debug, PartialEq)]
enum CountFor {
    Initial,
    Remaining,
}

/// Where the purge stands between two turns.
#[derive(Debug, PartialEq)]
enum PurgePhase {
    Begin,
    CountVersions {
        purpose: CountFor,
        key_marker: Option<String>,
        version_marker: Option<Ulid>,
        counted: u64,
    },
    CountUploads {
        purpose: CountFor,
        versions: u64,
        key_marker: Option<String>,
        upload_marker: Option<Ulid>,
        counted: u64,
    },
    AbortPage,
    AbortUpload {
        uploads: VecDeque<MultipartUpload>,
        removed: u64,
    },
    Guards {
        key_marker: Option<String>,
        version_marker: Option<Ulid>,
    },
    DeletePage,
    FinalList,
}

/// The state of one purge across its turns.
#[derive(Debug, PartialEq)]
pub struct PurgeRun {
    job_id: JobId,
    token: Ulid,
    spec: StoragePurgeSpec,
    files: bool,
    routing: NodeRouting,
    serving: Option<(NodeId, RealmId)>,
    progress: JobProgress,
    checkpoint: StoragePurgeCheckpoint,
    /// The bucket as the purge found it, when it exists in the authorized group.
    bucket: Option<BucketInfo>,
    /// The trail is published only for the bucket incarnation the purge began on.
    incarnation: Option<u128>,
    guards: Option<ReleaseGuards>,
    phase: PurgePhase,
}

impl PurgeRun {
    pub fn new(
        (job_id, token): (JobId, Ulid),
        spec: &StoragePurgeSpec,
        files: bool,
        (routing, serving): (NodeRouting, Option<(NodeId, RealmId)>),
        progress: JobProgress,
    ) -> Self {
        Self {
            job_id,
            token,
            spec: spec.clone(),
            files,
            routing,
            serving,
            progress,
            checkpoint: StoragePurgeCheckpoint::default(),
            bucket: None,
            incarnation: None,
            guards: None,
            phase: PurgePhase::Begin,
        }
    }

    /// The progress so far, which the job's heartbeat reports.
    pub fn progress(&self) -> &JobProgress {
        &self.progress
    }

    fn scope(&self) -> &StoragePurgeScope {
        &self.spec.scope
    }

    fn total(&self) -> u64 {
        (self.checkpoint.initial_versions)
            .saturating_add(self.checkpoint.initial_multipart_uploads)
            .saturating_add(u64::from(self.scope().is_bucket()))
    }

    fn versions_call(
        &self,
        limit: usize,
        key_marker: Option<String>,
        version_id_marker: Option<Ulid>,
    ) -> IoCall {
        IoCall::child(ListVersionsOperation::new(ListVersionsInput {
            bucket: self.scope().bucket().to_string(),
            prefix: self.scope().list_prefix().map(str::to_string),
            delimiter: None,
            key_marker,
            version_id_marker,
            max_keys: Some(limit),
        }))
    }

    fn uploads_call(
        &self,
        limit: usize,
        key_marker: Option<String>,
        upload_id_marker: Option<Ulid>,
    ) -> IoCall {
        let list = ListUploadsOperation::new(ListUploadsInput {
            bucket: self.scope().bucket().to_string(),
            prefix: self.scope().list_prefix().map(str::to_string),
            delimiter: None,
            key_marker,
            upload_id_marker,
            max_uploads: limit,
        });
        IoCall::child(list.including_in_progress().complete_scan())
    }

    fn authorize_call(&self, key: &str) -> IoCall {
        let spec = &self.spec;
        let path = object_permission_path(
            spec.auth_context.realm_id,
            spec.group_id,
            spec.node_id,
            spec.scope.bucket(),
            key,
        );
        IoCall::child(AuthorizeOperation::new(
            spec.auth_context.realm_id,
            &spec.auth_context,
            &path,
            &Permission::WRITE,
            PolicyRequestExtras::rest(),
        ))
    }
}

/// How one turn ended.
#[derive(Debug, PartialEq)]
pub enum PurgeTurn {
    Continue,
    /// The caller paused the purge; the same turn runs again after a wait.
    Paused,
    Finished(StoragePurgeResult),
}

/// The run after a turn, and how the turn ended.
#[derive(Debug, PartialEq)]
pub struct TurnEnd {
    pub run: Box<PurgeRun>,
    pub turn: PurgeTurn,
}

/// What a turn does once a batch is on record.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AfterFlush {
    /// Writes the checkpoint, then ends the turn or, after the bucket went, proves it gone.
    Checkpoint { prove: bool },
    /// Ends the turn.
    Turn,
    /// Goes on to the bucket itself, with no stop in between.
    BucketEnd,
    /// Proves the bucket is gone.
    Prove,
}

#[derive(Debug, PartialEq)]
enum TurnStep {
    Start,
    Bucket,
    Incarnation(Box<BucketInfo>),
    Checkpoint,
    Versions,
    Uploads,
    PutInitial,
    Flush(AfterFlush),
    PutBatch {
        prove: bool,
    },
    Paused,
    AbortList,
    AbortAuthorized,
    Aborted,
    GuardList,
    GuardPins {
        truncated: bool,
        key_marker: Option<String>,
        version_marker: Option<Ulid>,
    },
    DeleteList,
    DeleteAuthorized {
        keys: VecDeque<String>,
        entries: Vec<BulkDeleteEntry>,
    },
    Files(Vec<BulkDeleteEntry>),
    Plan {
        entries: Vec<BulkDeleteEntry>,
        indexes: Vec<Key>,
    },
    Deleted {
        indexes: Vec<Key>,
        batch: u64,
    },
    Dropped {
        batch: u64,
    },
    FinalVersions,
    FinalUploads,
    BucketDeleted,
    Proved {
        deleted: bool,
    },
    Audit {
        deleted: bool,
    },
}

type TurnNext = Result<Next<TurnEnd>, PurgeStop>;

/// Runs one turn of a purge.
#[derive(Debug, PartialEq)]
pub struct PurgeTurnOperation {
    run: Option<Box<PurgeRun>>,
    now_ms: u64,
    step: TurnStep,
}

/// One listed page of versions, narrowed to the file a file purge names.
fn version_page(scope: &StoragePurgeScope, mut page: ListVersionsResult) -> ListVersionsResult {
    if let StoragePurgeScope::File { key, .. } = scope {
        page.items.retain(|item| item_key(item) == key.as_str());
        if page.next_key_marker.as_deref() != Some(key.as_str()) {
            page.is_truncated = false;
            page.next_key_marker = None;
            page.next_version_marker = None;
        }
    }
    page
}

fn upload_page(scope: &StoragePurgeScope, mut page: ListUploadsResult) -> ListUploadsResult {
    if let StoragePurgeScope::File { key, .. } = scope {
        page.uploads.retain(|upload| upload.key == *key);
        if page.next_key_marker.as_deref() != Some(key.as_str()) {
            page.is_truncated = false;
            page.next_key_marker = None;
            page.next_upload_marker = None;
        }
    }
    page
}

fn item_key(item: &ListVersionsItem) -> &str {
    match item {
        ListVersionsItem::Version { key, .. } | ListVersionsItem::DeleteMarker { key, .. } => key,
    }
}

fn item_version(item: &ListVersionsItem) -> Ulid {
    match item {
        ListVersionsItem::Version { version_id, .. }
        | ListVersionsItem::DeleteMarker { version_id, .. } => *version_id,
    }
}

fn versions_of(result: IoResult) -> Result<ListVersionsResult, PurgeStop> {
    result
        .child::<ListVersionsResult, ListVersionsError>()?
        .map_err(|error| retry(format!("purge version list failed: {error}")))
}

fn uploads_of(result: IoResult) -> Result<ListUploadsResult, PurgeStop> {
    result
        .child::<ListUploadsResult, ListUploadsError>()?
        .map_err(|error| retry(format!("purge multipart list failed: {error}")))
}

fn authorized(result: IoResult) -> Result<(), PurgeStop> {
    result
        .child::<(), AuthorizeError>()?
        .map_err(|error| match error {
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => PurgeStop::Failed(
                JobError::permanent(format!("purge object authorization failed: {error}")),
            ),
            other => retry(format!("purge object authorization failed: {other}")),
        })
}

fn checkpoint_written(result: IoResult) -> Result<(), PurgeStop> {
    result
        .child::<(), JobMutationError>()?
        .map_err(|error| retry(format!("purge checkpoint write failed: {error}")))
}

fn file_plan_error(error: PurgeFilesError) -> PurgeStop {
    retry(format!("purge file plan failed: {error}"))
}

impl PurgeTurnOperation {
    pub fn new(run: PurgeRun, now_ms: u64) -> Self {
        Self {
            run: Some(Box::new(run)),
            now_ms,
            step: TurnStep::Start,
        }
    }

    fn run(&mut self) -> Result<&mut PurgeRun, PurgeStop> {
        self.run
            .as_deref_mut()
            .ok_or_else(|| retry("the purge turn already ended"))
    }

    fn end(&mut self, turn: PurgeTurn) -> TurnNext {
        let run = self
            .run
            .take()
            .ok_or_else(|| retry("the purge turn already ended"))?;
        Ok(Next::Done(TurnEnd { run, turn }))
    }

    fn call(&mut self, step: TurnStep, call: IoCall) -> TurnNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn flush(&mut self, after: AfterFlush) -> TurnNext {
        let now_ms = self.now_ms;
        let run = self.run()?;
        let flush = flush_progress(run.job_id, run.token, run.progress.clone(), now_ms);
        self.call(TurnStep::Flush(after), IoCall::child(flush))
    }

    fn put_checkpoint(&mut self, step: TurnStep) -> TurnNext {
        let run = self.run()?;
        let put = put_purge_checkpoint(run.job_id, run.token, &run.checkpoint);
        self.call(step, IoCall::child(put))
    }

    /// Records a finished batch of `completed` objects: progress first, then the checkpoint.
    fn finish_batch(&mut self, completed: u64, prove: bool) -> TurnNext {
        let run = self.run()?;
        run.checkpoint.batches_completed = run.checkpoint.batches_completed.saturating_add(1);
        run.progress.current = run.progress.current.saturating_add(completed);
        self.flush(AfterFlush::Checkpoint { prove })
    }

    fn set_phase(&mut self, phase: PurgePhase) -> Result<(), PurgeStop> {
        self.run()?.phase = phase;
        Ok(())
    }

    fn count_versions(purpose: CountFor) -> PurgePhase {
        PurgePhase::CountVersions {
            purpose,
            key_marker: None,
            version_marker: None,
            counted: 0,
        }
    }

    fn begun(&mut self, checkpoint: Option<StoragePurgeCheckpoint>) -> TurnNext {
        let run = self.run()?;
        let purpose = match checkpoint {
            Some(checkpoint) => {
                run.checkpoint = checkpoint;
                run.progress.total = Some(run.total());
                CountFor::Remaining
            }
            None => CountFor::Initial,
        };
        run.phase = Self::count_versions(purpose);
        self.end(PurgeTurn::Continue)
    }

    fn versions_counted(&mut self, page: ListVersionsResult) -> TurnNext {
        let run = self.run()?;
        let PurgePhase::CountVersions {
            purpose,
            counted,
            key_marker,
            version_marker,
        } = &mut run.phase
        else {
            return Err(retry("the purge lost its version count"));
        };
        *counted = counted.saturating_add(page.items.len() as u64);
        if page.is_truncated {
            if page.next_key_marker.is_none() {
                return Err(retry("version inventory truncated without a cursor"));
            }
            *key_marker = page.next_key_marker;
            *version_marker = page.next_version_marker;
        } else {
            run.phase = PurgePhase::CountUploads {
                purpose: *purpose,
                versions: *counted,
                key_marker: None,
                upload_marker: None,
                counted: 0,
            };
        }
        self.end(PurgeTurn::Continue)
    }

    fn uploads_counted(&mut self, page: ListUploadsResult) -> TurnNext {
        let run = self.run()?;
        let PurgePhase::CountUploads {
            purpose,
            versions,
            counted,
            key_marker,
            upload_marker,
        } = &mut run.phase
        else {
            return Err(retry("the purge lost its upload count"));
        };
        *counted = counted.saturating_add(page.uploads.len() as u64);
        if page.is_truncated {
            if page.next_key_marker.is_none() {
                return Err(retry("multipart inventory truncated without a cursor"));
            }
            *key_marker = page.next_key_marker;
            *upload_marker = page.next_upload_marker;
            return self.end(PurgeTurn::Continue);
        }
        let (purpose, versions, uploads) = (*purpose, *versions, *counted);
        match purpose {
            CountFor::Initial => {
                run.checkpoint.initial_versions = versions;
                run.checkpoint.initial_multipart_uploads = uploads;
                run.phase = Self::count_versions(CountFor::Remaining);
                run.progress.total = Some(run.total());
                self.put_checkpoint(TurnStep::PutInitial)
            }
            CountFor::Remaining => {
                let bucket = u64::from(run.scope().is_bucket() && run.bucket_exists());
                let remaining = versions.saturating_add(uploads).saturating_add(bucket);
                run.progress.current = run.total().saturating_sub(remaining);
                match run.bucket {
                    Some(_) => {
                        run.phase = PurgePhase::AbortPage;
                        self.flush(AfterFlush::Turn)
                    }
                    None => self.flush(AfterFlush::BucketEnd),
                }
            }
        }
    }

    /// Checks whether the purge is paused before it works on the next page.
    fn pause_check(&mut self) -> TurnNext {
        let job = self.run()?.job_id.as_ulid();
        self.call(TurnStep::Paused, IoCall::child(PausedOperation::new(job)))
    }

    fn page_after_pause(&mut self) -> TurnNext {
        let run = self.run()?;
        match run.phase {
            PurgePhase::AbortPage => {
                let list = run.uploads_call(PURGE_BATCH_SIZE, None, None);
                self.call(TurnStep::AbortList, list)
            }
            _ => {
                let list = run.versions_call(PURGE_BATCH_SIZE, None, None);
                self.call(TurnStep::DeleteList, list)
            }
        }
    }

    fn abort_listed(&mut self, uploads: Vec<MultipartUpload>) -> TurnNext {
        let run = self.run()?;
        if uploads.is_empty() {
            run.phase = PurgePhase::Guards {
                key_marker: None,
                version_marker: None,
            };
        } else {
            run.phase = PurgePhase::AbortUpload {
                uploads: uploads.into(),
                removed: 0,
            };
        }
        self.end(PurgeTurn::Continue)
    }

    fn abort_next(&mut self) -> TurnNext {
        let run = self.run()?;
        let PurgePhase::AbortUpload { uploads, .. } = &run.phase else {
            return Err(retry("the purge lost its upload page"));
        };
        let Some(upload) = uploads.front() else {
            return Err(retry("the purge upload page is empty"));
        };
        let authorize = run.authorize_call(&upload.key);
        self.call(TurnStep::AbortAuthorized, authorize)
    }

    fn abort_authorized(&mut self) -> TurnNext {
        let now_ms = self.now_ms;
        let run = self.run()?;
        let PurgePhase::AbortUpload { uploads, .. } = &run.phase else {
            return Err(retry("the purge lost its upload page"));
        };
        let Some(upload) = uploads.front() else {
            return Err(retry("the purge upload page is empty"));
        };
        let abort = AbortUploadOperation::new(AbortUploadInput {
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            upload_id: upload.upload_id,
            now_ms,
        })
        .including_in_progress();
        self.call(TurnStep::Aborted, IoCall::child(abort))
    }

    fn aborted(&mut self, result: Result<(), AbortUploadError>) -> TurnNext {
        match result {
            Ok(()) | Err(AbortUploadError::NoSuchUpload) => {}
            Err(AbortUploadError::UploadNotOpen) => {
                return Err(retry(
                    "matching multipart upload changed state during purge",
                ));
            }
            Err(error) => return Err(retry(format!("multipart abort failed: {error}"))),
        }
        let run = self.run()?;
        let PurgePhase::AbortUpload { uploads, removed } = &mut run.phase else {
            return Err(retry("the purge lost its upload page"));
        };
        uploads.pop_front();
        *removed += 1;
        if !uploads.is_empty() {
            return self.end(PurgeTurn::Continue);
        }
        let completed = *removed;
        run.phase = PurgePhase::AbortPage;
        self.finish_batch(completed, false)
    }

    /// Refuses the purge while a release pins any version in its scope, also one published after
    /// it was submitted; else keeps the guards of the first check, which every delete keeps.
    fn guard_listed(&mut self, page: ListVersionsResult) -> TurnNext {
        let run = self.run()?;
        let Some(info) = &run.bucket else {
            return Err(retry("the purge lost its bucket"));
        };
        let spec = &run.spec;
        let scope = BucketScope {
            realm_id: spec.auth_context.realm_id,
            node_id: spec.node_id,
            bucket: spec.scope.bucket(),
            info,
        };
        let queries: Vec<MembershipQuery> = (page.items.iter())
            .map(|item| MembershipQuery {
                key: item_key(item).to_string(),
                version_id: item_version(item),
            })
            .collect();
        let pins = VersionPinsOperation::new(scope, &spec.auth_context, queries);
        let step = TurnStep::GuardPins {
            truncated: page.is_truncated,
            key_marker: page.next_key_marker,
            version_marker: page.next_version_marker,
        };
        self.call(step, IoCall::child(pins))
    }

    fn guards_read(
        &mut self,
        (pins, read): (Vec<bool>, ReleaseGuards),
        truncated: bool,
        key_marker: Option<String>,
        version_marker: Option<Ulid>,
    ) -> TurnNext {
        if pins.contains(&true) {
            let refused = JobError::permanent(PinnedError::Scope.to_string());
            return Err(PurgeStop::Failed(refused));
        }
        let run = self.run()?;
        run.guards.get_or_insert(read);
        run.phase = match (truncated, key_marker) {
            (false, _) => PurgePhase::DeletePage,
            (true, None) => return Err(retry("version inventory truncated without a cursor")),
            (true, Some(key_marker)) => PurgePhase::Guards {
                key_marker: Some(key_marker),
                version_marker,
            },
        };
        self.end(PurgeTurn::Continue)
    }

    fn delete_listed(&mut self, page: ListVersionsResult) -> TurnNext {
        if page.items.is_empty() {
            self.set_phase(PurgePhase::FinalList)?;
            return self.end(PurgeTurn::Continue);
        }
        let entries: Vec<BulkDeleteEntry> = (page.items.iter())
            .map(|item| BulkDeleteEntry {
                key: item_key(item).to_string(),
                version_id: Some(item_version(item)),
            })
            .collect();
        let keys: BTreeSet<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
        let keys = keys.into_iter().map(str::to_string).collect();
        self.authorize_next(keys, entries)
    }

    fn authorize_next(
        &mut self,
        mut keys: VecDeque<String>,
        entries: Vec<BulkDeleteEntry>,
    ) -> TurnNext {
        let Some(key) = keys.pop_front() else {
            return self.after_authorized(entries);
        };
        let authorize = self.run()?.authorize_call(&key);
        self.call(TurnStep::DeleteAuthorized { keys, entries }, authorize)
    }

    /// Plans the release of the linked-storage files the entries read, before they go.
    fn after_authorized(&mut self, entries: Vec<BulkDeleteEntry>) -> TurnNext {
        let run = self.run()?;
        if !run.files {
            return self.delete_entries(entries, Vec::new());
        }
        let versions: Vec<(String, Ulid)> = (entries.iter())
            .filter_map(|entry| Some((entry.key.clone(), entry.version_id?)))
            .collect();
        let scope = (run.spec.scope.bucket(), run.spec.group_id);
        let files = StorageFilesOperation::new(run.routing.clone(), scope, &versions, None);
        self.call(TurnStep::Files(entries), IoCall::child(files))
    }

    fn delete_entries(&mut self, entries: Vec<BulkDeleteEntry>, indexes: Vec<Key>) -> TurnNext {
        let run = self.run()?;
        let batch = entries.len() as u64;
        let spec = &run.spec;
        let delete = BulkDeleteOperation::new(BulkDeleteInput {
            bucket: spec.scope.bucket().to_string(),
            entries,
            group_id: spec.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: spec.node_id,
            deleted_by: spec.auth_context.user_id,
            restrictions: spec.auth_context.path_restrictions.clone(),
            guards: run.guards.clone().unwrap_or_default(),
        });
        self.call(TurnStep::Deleted { indexes, batch }, IoCall::child(delete))
    }

    fn deleted(
        &mut self,
        outcomes: Result<Vec<BulkDeleteOutcome>, DeleteObjectError>,
        indexes: Vec<Key>,
        batch: u64,
    ) -> TurnNext {
        let failed = |error| retry(format!("versioned purge delete failed: {error}"));
        for outcome in outcomes.map_err(failed)? {
            match outcome.result {
                Ok(_) | Err(DeleteObjectError::NoSuchVersion) => {}
                Err(error) => return Err(failed(error)),
            }
        }
        // The rows and files of second copies go through the planned releases.
        let deletes = (indexes.into_iter())
            .map(|key| (RENAMED_ROW_KEYSPACE.to_string(), key))
            .collect();
        self.call(TurnStep::Dropped { batch }, IoCall::Delete(deletes))
    }

    fn bucket_end(&mut self) -> TurnNext {
        let run = self.run()?;
        if !run.scope().is_bucket() {
            return self.audit(false);
        }
        if run.bucket.is_none() {
            run.progress.current = run.total();
            return self.flush(AfterFlush::Prove);
        }
        let bucket = run.scope().bucket().to_string();
        let delete = DeleteBucketOperation::new(bucket).purge_notices(run.serving);
        self.call(TurnStep::BucketDeleted, IoCall::child(delete))
    }

    fn prove(&mut self) -> TurnNext {
        let bucket = self.run()?.scope().bucket().to_string();
        let read = GetBucketOperation::new(bucket);
        self.call(TurnStep::Proved { deleted: true }, IoCall::child(read))
    }

    /// Records the completed purge once; see [`WriteAuditOperation`].
    fn audit(&mut self, deleted: bool) -> TurnNext {
        let now_ms = self.now_ms;
        let run = self.run()?;
        let incarnation = run.incarnation.filter(|_| !deleted);
        let record = purge_audit_record(&run.spec, now_ms);
        let write = WriteAuditOperation::new(record, run.job_id.as_ulid(), incarnation)
            .map_err(|error| retry(format!("purge audit encode failed: {error}")))?;
        self.call(TurnStep::Audit { deleted }, IoCall::child(write))
    }

    fn finished(&mut self, bucket_deleted: bool) -> TurnNext {
        let run = self.run()?;
        run.progress.current = run.total();
        let result = StoragePurgeResult {
            scope: run.spec.scope.clone(),
            versions_removed: run.checkpoint.initial_versions,
            multipart_uploads_removed: run.checkpoint.initial_multipart_uploads,
            batches_completed: run.checkpoint.batches_completed,
            bucket_deleted,
            emptiness_proven: true,
        };
        self.end(PurgeTurn::Finished(result))
    }

    fn bucket_read(&mut self, read: Result<BucketInfo, GetBucketError>) -> TurnNext {
        let run = self.run()?;
        match read {
            // A member bucket of a distributed bucket is purged only by deleting its set.
            Ok(info) if info.distribution.is_some() => Err(PurgeStop::Failed(JobError::permanent(
                "purge bucket is a member of a distributed bucket; delete the distributed bucket",
            ))),
            Ok(info) if info.group_id == run.spec.group_id => {
                let key = Key::from(run.scope().bucket().as_bytes().to_vec());
                let rows = vec![
                    (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
                    (BUCKET_CREATION_KEYSPACE.to_string(), key),
                ];
                self.call(TurnStep::Incarnation(Box::new(info)), IoCall::Read(rows))
            }
            Ok(_) => Err(PurgeStop::Failed(JobError::permanent(
                "purge bucket is outside the authorized group",
            ))),
            Err(GetBucketError::NotFound) => self.read_checkpoint(),
            Err(error) => Err(retry(format!("purge bucket read failed: {error}"))),
        }
    }

    fn read_checkpoint(&mut self) -> TurnNext {
        let read = read_purge_checkpoint(self.run()?.job_id);
        self.call(TurnStep::Checkpoint, IoCall::child(read))
    }
}

impl PurgeRun {
    fn bucket_exists(&self) -> bool {
        self.bucket.is_some()
    }
}

impl Operation for PurgeTurnOperation {
    type Output = TurnEnd;
    type Error = PurgeStop;

    fn first(&mut self) -> TurnNext {
        let run = self.run()?;
        match &run.phase {
            PurgePhase::Begin => {
                let read = GetBucketOperation::new(run.scope().bucket().to_string());
                self.call(TurnStep::Bucket, IoCall::child(read))
            }
            PurgePhase::CountVersions {
                key_marker,
                version_marker,
                ..
            } => {
                let list = run.versions_call(PURGE_BATCH_SIZE, key_marker.clone(), *version_marker);
                self.call(TurnStep::Versions, list)
            }
            PurgePhase::CountUploads {
                key_marker,
                upload_marker,
                ..
            } => {
                let list = run.uploads_call(PURGE_BATCH_SIZE, key_marker.clone(), *upload_marker);
                self.call(TurnStep::Uploads, list)
            }
            PurgePhase::AbortPage | PurgePhase::DeletePage => self.pause_check(),
            PurgePhase::AbortUpload { .. } => self.abort_next(),
            PurgePhase::Guards {
                key_marker,
                version_marker,
            } => {
                let list = run.versions_call(PURGE_BATCH_SIZE, key_marker.clone(), *version_marker);
                self.call(TurnStep::GuardList, list)
            }
            PurgePhase::FinalList => {
                let list = run.versions_call(1, None, None);
                self.call(TurnStep::FinalVersions, list)
            }
        }
    }

    fn next(&mut self, result: IoResult) -> TurnNext {
        match std::mem::replace(&mut self.step, TurnStep::Start) {
            TurnStep::Start => Err(retry("the purge turn got an answer before a call")),
            TurnStep::Bucket => self.bucket_read(result.child()?),
            TurnStep::Incarnation(info) => {
                let [bucket, creation] = aruna_core::rows::values::<2>(result)?;
                let run = self.run()?;
                run.incarnation = bucket.map(|_| incarnation(creation.as_deref()));
                run.bucket = Some(*info);
                self.read_checkpoint()
            }
            TurnStep::Checkpoint => {
                let checkpoint = result
                    .child::<Option<StoragePurgeCheckpoint>, JobMutationError>()?
                    .map_err(|error| retry(format!("purge checkpoint read failed: {error}")))?;
                self.begun(checkpoint)
            }
            TurnStep::Versions => {
                let page = version_page(self.run()?.scope(), versions_of(result)?);
                self.versions_counted(page)
            }
            TurnStep::Uploads => {
                let page = upload_page(self.run()?.scope(), uploads_of(result)?);
                self.uploads_counted(page)
            }
            TurnStep::PutInitial => {
                checkpoint_written(result)?;
                self.end(PurgeTurn::Continue)
            }
            TurnStep::PutBatch { prove } => {
                checkpoint_written(result)?;
                match prove {
                    true => self.prove(),
                    false => self.end(PurgeTurn::Continue),
                }
            }
            TurnStep::Flush(after) => {
                let renew = result
                    .child::<RenewOutcome, JobMutationError>()?
                    .map_err(|error| retry(format!("purge progress write failed: {error}")))?;
                if renew.cancel_requested {
                    return Err(PurgeStop::Cancelled);
                }
                match after {
                    AfterFlush::Checkpoint { prove } => {
                        self.put_checkpoint(TurnStep::PutBatch { prove })
                    }
                    AfterFlush::Turn => self.end(PurgeTurn::Continue),
                    AfterFlush::BucketEnd => self.bucket_end(),
                    AfterFlush::Prove => self.prove(),
                }
            }
            TurnStep::Paused => {
                let paused = result
                    .child::<bool, PauseError>()?
                    .map_err(|error| retry(format!("purge pause read failed: {error}")))?;
                match paused {
                    true => self.end(PurgeTurn::Paused),
                    false => self.page_after_pause(),
                }
            }
            TurnStep::AbortList => {
                let page = upload_page(self.run()?.scope(), uploads_of(result)?);
                self.abort_listed(page.uploads)
            }
            TurnStep::AbortAuthorized => {
                authorized(result)?;
                self.abort_authorized()
            }
            TurnStep::Aborted => self.aborted(result.child()?),
            TurnStep::GuardList => {
                let page = version_page(self.run()?.scope(), versions_of(result)?);
                self.guard_listed(page)
            }
            TurnStep::GuardPins {
                truncated,
                key_marker,
                version_marker,
            } => {
                let pins = result
                    .child::<(Vec<bool>, ReleaseGuards), PinnedError>()?
                    .map_err(|error| retry(error.to_string()))?;
                self.guards_read(pins, truncated, key_marker, version_marker)
            }
            TurnStep::DeleteList => {
                let page = version_page(self.run()?.scope(), versions_of(result)?);
                self.delete_listed(page)
            }
            TurnStep::DeleteAuthorized { keys, entries } => {
                authorized(result)?;
                self.authorize_next(keys, entries)
            }
            TurnStep::Files(entries) => {
                let files = result
                    .child::<Vec<StorageFile>, PurgeFilesError>()?
                    .map_err(file_plan_error)?;
                let indexes = files.iter().filter_map(|file| file.index.clone()).collect();
                let plan = PlanFilesOperation::new(self.run()?.job_id.as_ulid(), &files);
                let step = TurnStep::Plan { entries, indexes };
                self.call(step, IoCall::child(plan))
            }
            TurnStep::Plan { entries, indexes } => {
                result
                    .child::<(), PurgeFilesError>()?
                    .map_err(file_plan_error)?;
                self.delete_entries(entries, indexes)
            }
            TurnStep::Deleted { indexes, batch } => self.deleted(result.child()?, indexes, batch),
            TurnStep::Dropped { batch } => {
                result.done().map_err(|error| {
                    retry(format!("purge could not drop copy indexes: {error}"))
                })?;
                self.finish_batch(batch, false)
            }
            TurnStep::FinalVersions => {
                let page = version_page(self.run()?.scope(), versions_of(result)?);
                if !page.items.is_empty() {
                    return Err(retry("final purge version re-list was not empty"));
                }
                let list = self.run()?.uploads_call(1, None, None);
                self.call(TurnStep::FinalUploads, list)
            }
            TurnStep::FinalUploads => {
                let page = upload_page(self.run()?.scope(), uploads_of(result)?);
                if !page.uploads.is_empty() {
                    return Err(retry("final purge multipart re-list was not empty"));
                }
                self.bucket_end()
            }
            TurnStep::BucketDeleted => {
                match result.child::<(), DeleteBucketError>()? {
                    Ok(()) | Err(DeleteBucketError::NotFound) => {}
                    Err(error) => {
                        return Err(retry(format!("purge bucket delete failed: {error}")));
                    }
                }
                self.finish_batch(1, true)
            }
            TurnStep::Proved { deleted } => match result.child::<BucketInfo, GetBucketError>()? {
                Err(GetBucketError::NotFound) => self.audit(deleted),
                Ok(_) => Err(retry("bucket still exists after purge")),
                Err(error) => Err(retry(format!("bucket emptiness proof failed: {error}"))),
            },
            TurnStep::Audit { deleted } => {
                result
                    .child::<(), IoError>()?
                    .map_err(|error| retry(format!("purge audit write failed: {error}")))?;
                self.finished(deleted)
            }
        }
    }
}

#[cfg(test)]
#[path = "purge_turn_tests.rs"]
mod tests;
