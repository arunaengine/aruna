//! Answers a run's calls about its install's jobs from their work rows, starts the jobs of new
//! work, and keeps the rows in step with the jobs they name.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::execution::job::{
    JobId, JobPayload, JobRecord, PluginWorkSpec, RETENTION_MS, WorkspaceMode,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::broker::{ErrorKind, Field, HostError, JobStatus};
use base64::Engine;
use serde_json::Value;
use tracing::warn;
use ulid::Ulid;

use super::check::{CallError, RunScope};
use super::grants::short_path;
use super::rows::{Rows, StoreError, decode, delete_rows, scan_rows};
use super::submit::{PrepareWork, PreparedWork, WORK_ACTIVE, WorkRequest};
use super::unavailable;
use super::work::{
    SendBoundary, Summary, WorkChange, WorkKind, WorkRow, WorkState, dedup_key, index_key,
    install_at, summary_of, update_work, work_scan, work_state,
};
use super::work_calls::JobRow;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::{CancelRequestOutcome, set_cancel_requested};
use crate::jobs::store::row_calls::read_job_record;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult, SubmitJobSpec};
use crate::jobs::submit_local::{
    DedupJobOperation, MintJobOperation, QueueJobOperation, ServingNode,
};

fn record_call(job: JobId) -> IoCall {
    IoCall::child(read_job_record(job))
}

fn record_answer(result: IoResult) -> Result<Result<Option<JobRecord>, String>, IoError> {
    let record = result.child::<Option<JobRecord>, JobMutationError>()?;
    Ok(record.map_err(|error| error.to_string()))
}

fn cancel_call(job: JobId, now_ms: u64) -> IoCall {
    IoCall::child(set_cancel_requested(job, now_ms))
}

fn call_unavailable(message: impl Into<String>) -> CallError {
    CallError(unavailable(message))
}

/// What job `id` did: from its kept summary once settled, else from its job record.
#[derive(Debug, PartialEq)]
pub(crate) struct JobState {
    install: PluginInstall,
    local: NodeId,
    id: String,
    row: Option<Box<WorkRow>>,
}

impl JobState {
    pub(crate) fn new((install, local): (&PluginInstall, NodeId), id: &str) -> Self {
        Self {
            install: install.clone(),
            local,
            id: id.to_string(),
            row: None,
        }
    }

    fn status(&self, row: &WorkRow, record: Option<&JobRecord>) -> JobStatus {
        let state = work_state(row, record);
        let finished = record.and_then(|record| summary_of(Some(record)));
        let summary = row.summary.clone().or(finished);
        let path = match row.kind {
            WorkKind::Copy | WorkKind::Download => row.target.as_deref(),
            WorkKind::Upload => row.source.as_deref(),
            WorkKind::Recipe => None,
        };
        let place = (&self.install, self.local);
        let mut status = JobStatus {
            id: row.work_id.to_string(),
            kind: kind_name(row.kind).to_string(),
            path: path.map_or_else(String::new, |path| short_path(path, place)),
            state: state.to_string(),
            message: None,
            bytes: None,
            version: None,
            reply_status: None,
            reply_headers: Vec::new(),
            reply_body: None,
            exit_code: None,
            report: None,
        };
        if let Some(summary) = summary {
            fill(&mut status, summary);
        } else if let Some(record) = record {
            status.message = (record.last_error.as_ref()).map(|error| error.message.clone());
        }
        // An upload's answer is kept as soon as it came, before the work settles.
        if let (
            None,
            Some(SendBoundary::Sent {
                status: code,
                reply,
                ..
            }),
        ) = (status.reply_status, &row.boundary)
        {
            answer(&mut status, *code, reply.as_deref());
        }
        status
    }
}

impl Operation for JobState {
    type Output = JobStatus;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<JobStatus>, CallError> {
        Ok(Next::Call(IoCall::child(JobRow::new(
            &self.install,
            &self.id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobStatus>, CallError> {
        if let Some(row) = self.row.take() {
            let record = record_answer(result)?.map_err(call_unavailable)?;
            return Ok(Next::Done(self.status(&row, record.as_ref())));
        }
        let row = result.child::<WorkRow, CallError>()??;
        match (&row.summary, row.job) {
            (None, Some(job)) => {
                self.row = Some(Box::new(row));
                Ok(Next::Call(record_call(job)))
            }
            _ => Ok(Next::Done(self.status(&row, None))),
        }
    }
}

/// Records the job of a reserved work whose submission committed but whose answer was lost; the
/// job is found by the work's dedup key. `None` while no job was submitted.
#[derive(Debug, PartialEq)]
pub(crate) struct ReconcileWork {
    install_id: Ulid,
    work_id: Ulid,
    account: UserId,
    found: Option<JobId>,
}

impl ReconcileWork {
    pub(crate) fn new(install_id: Ulid, row: &WorkRow) -> Self {
        Self {
            install_id,
            work_id: row.work_id,
            account: row.account,
            found: None,
        }
    }
}

impl Operation for ReconcileWork {
    type Output = Option<JobId>;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<Option<JobId>>, StoreError> {
        let key = dedup_key(self.work_id);
        let find = DedupJobOperation::new(self.account, &key);
        Ok(Next::Call(IoCall::child(find)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<JobId>>, StoreError> {
        if let Some(job) = self.found {
            result.child::<Option<WorkRow>, StoreError>()??;
            return Ok(Next::Done(Some(job)));
        }
        let found = result.child::<Option<(JobId, [u8; 32])>, SubmitJobError>()?;
        let found = found.map_err(|error| StoreError(error.to_string()))?;
        let Some((job, _)) = found else {
            return Ok(Next::Done(None));
        };
        self.found = Some(job);
        let change = WorkChange::Dispatched(job);
        let update = update_work((self.install_id, self.work_id), change);
        Ok(Next::Call(IoCall::child(update)))
    }
}

fn reconcile_answer(result: IoResult) -> Result<Result<Option<JobId>, StoreError>, IoError> {
    result.child::<Option<JobId>, StoreError>()
}

#[derive(Debug, PartialEq)]
enum CancelPhase {
    Row,
    Reconcile,
    Cancel,
}

/// Asks the job of `id` to stop. A job that ended, or whose record is gone, has nothing left to
/// stop; a work whose job is not known yet cannot be stopped yet. A running job sees the request
/// at its next lease renewal.
#[derive(Debug, PartialEq)]
pub struct CancelWork {
    install: PluginInstall,
    id: String,
    now_ms: u64,
    phase: CancelPhase,
}

impl CancelWork {
    pub fn new(install: &PluginInstall, id: &str, now_ms: u64) -> Self {
        Self {
            install: install.clone(),
            id: id.to_string(),
            now_ms,
            phase: CancelPhase::Row,
        }
    }

    fn cancel(&mut self, job: JobId) -> Next<()> {
        self.phase = CancelPhase::Cancel;
        Next::Call(cancel_call(job, self.now_ms))
    }
}

impl Operation for CancelWork {
    type Output = ();
    type Error = CallError;

    fn first(&mut self) -> Result<Next<()>, CallError> {
        Ok(Next::Call(IoCall::child(JobRow::new(
            &self.install,
            &self.id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CallError> {
        match self.phase {
            CancelPhase::Row => {
                let row = result.child::<WorkRow, CallError>()??;
                match (row.state, row.job) {
                    (WorkState::Settled, _) => Ok(Next::Done(())),
                    (_, Some(job)) => Ok(self.cancel(job)),
                    (_, None) => {
                        self.phase = CancelPhase::Reconcile;
                        let reconcile = ReconcileWork::new(self.install.install_id, &row);
                        Ok(Next::Call(IoCall::child(reconcile)))
                    }
                }
            }
            CancelPhase::Reconcile => match reconcile_answer(result)? {
                Ok(Some(job)) => Ok(self.cancel(job)),
                Ok(None) => Err(call_unavailable("the job is not submitted yet")),
                Err(error) => Err(call_unavailable(error.0)),
            },
            CancelPhase::Cancel => {
                match result.child::<CancelRequestOutcome, JobMutationError>()? {
                    Ok(_) | Err(JobMutationError::NotFound) => Ok(Next::Done(())),
                    Err(error) => Err(call_unavailable(error.to_string())),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettlePhase {
    Scan,
    Reconcile(Ulid, Ulid),
    Record(Ulid, Ulid),
    Update,
}

/// Keeps the summary of every work whose job finished, so its status outlives the job record;
/// a reserved work first takes the job its dedup key names. Answers how many settled.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleWork {
    now_ms: u64,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    settled: usize,
    phase: SettlePhase,
}

impl SettleWork {
    pub(crate) fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            rows: VecDeque::new(),
            settled: 0,
            phase: SettlePhase::Scan,
        }
    }

    fn advance(&mut self) -> Result<Next<usize>, StoreError> {
        while let Some((key, value)) = self.rows.pop_front() {
            let row: WorkRow = decode(&value)?;
            let Some(install_id) = install_at(&key) else {
                continue;
            };
            match (row.state, row.job) {
                (WorkState::Dispatched, Some(job)) => {
                    self.phase = SettlePhase::Record(install_id, row.work_id);
                    return Ok(Next::Call(record_call(job)));
                }
                (WorkState::Reserved, _) => {
                    self.phase = SettlePhase::Reconcile(install_id, row.work_id);
                    let reconcile = ReconcileWork::new(install_id, &row);
                    return Ok(Next::Call(IoCall::child(reconcile)));
                }
                _ => {}
            }
        }
        Ok(Next::Done(self.settled))
    }
}

impl Operation for SettleWork {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(IoCall::child(scan_rows(work_scan()))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, SettlePhase::Update) {
            SettlePhase::Scan => {
                self.rows = result.child::<Rows, StoreError>()??.into();
                self.advance()
            }
            SettlePhase::Reconcile(install_id, work_id) => match reconcile_answer(result)?? {
                Some(job) => {
                    self.phase = SettlePhase::Record(install_id, work_id);
                    Ok(Next::Call(record_call(job)))
                }
                None => self.advance(),
            },
            SettlePhase::Record(install_id, work_id) => {
                let record = record_answer(result)?.map_err(StoreError)?;
                let Some(summary) = summary_of(record.as_ref()) else {
                    return self.advance();
                };
                let change = WorkChange::Settled(Box::new(summary), self.now_ms);
                let update = update_work((install_id, work_id), change);
                Ok(Next::Call(IoCall::child(update)))
            }
            SettlePhase::Update => {
                let changed = result.child::<Option<WorkRow>, StoreError>()??;
                self.settled += usize::from(changed.is_some());
                self.advance()
            }
        }
    }
}

/// A work row of a removed install, on its way out.
#[derive(Debug, PartialEq)]
struct RetiredRow {
    key: Vec<u8>,
    install_id: Ulid,
    row: WorkRow,
}

#[derive(Debug, PartialEq)]
enum RetirePhase {
    Scan,
    Reconcile(Box<RetiredRow>),
    Record(Box<RetiredRow>, JobId),
    Cancel(Box<RetiredRow>, JobId),
    Mark,
    Delete(usize),
}

/// Asks the jobs of removed installs to cancel and deletes their rows once each job is settled
/// and kept; a row stays as long as its cancel was not accepted. Answers how many rows went.
#[derive(Debug, PartialEq)]
pub(crate) struct RetireWork {
    kept: Vec<Ulid>,
    now_ms: u64,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    dropped: Vec<Vec<u8>>,
    phase: RetirePhase,
}

impl RetireWork {
    pub(crate) fn new(config: &RealmConfigDocument, now_ms: u64) -> Self {
        let kept = (config.installs.iter())
            .filter(|install| install.state != InstallState::Removing)
            .map(|install| install.install_id)
            .collect();
        Self {
            kept,
            now_ms,
            rows: VecDeque::new(),
            dropped: Vec::new(),
            phase: RetirePhase::Scan,
        }
    }

    fn drop_row(&mut self, item: RetiredRow) {
        let index = index_key(item.install_id, item.row.work_id);
        self.dropped.extend([index, item.key]);
    }

    /// Reads the job of a row that still names one; a row without a live job goes.
    fn check_job(&mut self, item: RetiredRow) -> Option<Next<usize>> {
        let live = item.row.state != WorkState::Settled;
        let Some(job) = item.row.job.filter(|_| live) else {
            self.drop_row(item);
            return None;
        };
        self.phase = RetirePhase::Record(Box::new(item), job);
        Some(Next::Call(record_call(job)))
    }

    fn advance(&mut self) -> Result<Next<usize>, StoreError> {
        while let Some((key, value)) = self.rows.pop_front() {
            let Some(install_id) = install_at(&key) else {
                continue;
            };
            if self.kept.contains(&install_id) {
                continue;
            }
            let row: WorkRow = decode(&value)?;
            let item = RetiredRow {
                key,
                install_id,
                row,
            };
            if item.row.state == WorkState::Reserved {
                let reconcile = ReconcileWork::new(install_id, &item.row);
                self.phase = RetirePhase::Reconcile(Box::new(item));
                return Ok(Next::Call(IoCall::child(reconcile)));
            }
            if let Some(next) = self.check_job(item) {
                return Ok(next);
            }
        }
        let dropped = std::mem::take(&mut self.dropped);
        let count = dropped.len() / 2;
        self.phase = RetirePhase::Delete(count);
        let keys = (dropped.into_iter()).map(|key| (PLUGIN_WORK_KEYSPACE, key));
        Ok(Next::Call(IoCall::child(delete_rows(keys.collect()))))
    }
}

impl Operation for RetireWork {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(IoCall::child(scan_rows(work_scan()))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, StoreError> {
        match std::mem::replace(&mut self.phase, RetirePhase::Mark) {
            RetirePhase::Scan => {
                self.rows = result.child::<Rows, StoreError>()??.into();
                self.advance()
            }
            RetirePhase::Reconcile(mut item) => {
                match reconcile_answer(result)?? {
                    Some(job) => {
                        (item.row.job, item.row.state) = (Some(job), WorkState::Dispatched);
                        if let Some(next) = self.check_job(*item) {
                            return Ok(next);
                        }
                    }
                    None => self.drop_row(*item),
                }
                self.advance()
            }
            RetirePhase::Record(item, job) => {
                let record = record_answer(result)?.map_err(StoreError)?;
                let Some(record) = record else {
                    self.drop_row(*item);
                    return self.advance();
                };
                // A settled job keeps its outcome; the settle step stores it before the row goes.
                if item.row.cancel || record.is_settled() {
                    return self.advance();
                }
                self.phase = RetirePhase::Cancel(item, job);
                Ok(Next::Call(cancel_call(job, self.now_ms)))
            }
            RetirePhase::Cancel(item, job) => {
                match result.child::<CancelRequestOutcome, JobMutationError>()? {
                    Ok(_) => {
                        let change = WorkChange::Cancelling(job);
                        let ids = (item.install_id, item.row.work_id);
                        return Ok(Next::Call(IoCall::child(update_work(ids, change))));
                    }
                    Err(JobMutationError::NotFound) => self.drop_row(*item),
                    Err(error) => {
                        warn!(%error, "A removed install's plugin work did not accept a cancel");
                    }
                }
                self.advance()
            }
            RetirePhase::Mark => {
                result.child::<Option<WorkRow>, StoreError>()??;
                self.advance()
            }
            RetirePhase::Delete(count) => {
                result.child::<(), StoreError>()??;
                Ok(Next::Done(count))
            }
        }
    }
}

fn fill(status: &mut JobStatus, summary: Summary) {
    status.message = summary.message;
    status.bytes = summary.bytes;
    status.version = summary.version_id.map(|version| version.to_string());
    if let Some(code) = summary.status {
        answer(status, code, summary.reply.as_deref());
    }
    status.exit_code = summary.exit_code;
    status.report = summary.report;
}

/// The status, headers and body of a kept answer, stored as the broker keeps whole answers; a
/// reply that was too large to keep has no headers or body.
fn answer(status: &mut JobStatus, code: u16, reply: Option<&str>) {
    status.reply_status = Some(code);
    let Some(value) = reply.and_then(|reply| serde_json::from_str::<Value>(reply).ok()) else {
        return;
    };
    let headers = value.get("headers").cloned().unwrap_or(Value::Null);
    let headers: Vec<(String, String)> = serde_json::from_value(headers).unwrap_or_default();
    status.reply_headers = (headers.into_iter())
        .map(|(name, value)| Field { name, value })
        .collect();
    let body = value
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or_default();
    status.reply_body = base64::engine::general_purpose::STANDARD.decode(body).ok();
}

fn kind_name(kind: WorkKind) -> &'static str {
    match kind {
        WorkKind::Copy => "copy",
        WorkKind::Download => "download",
        WorkKind::Upload => "upload",
        WorkKind::Recipe => "task",
    }
}

#[derive(Debug, PartialEq)]
enum StartPhase {
    Prepare,
    Mint(UserId, Box<PluginWorkSpec>),
    Queue(Ulid, Ulid),
    Record(Ulid),
}

/// Starts or finds the work of a request and answers its work id. The dedup key is the work id,
/// so a repeated submission with the same payload answers the same job. The work's row stays
/// reserved after any refusal, so the same call submits again later.
#[derive(Debug, PartialEq)]
pub(crate) struct StartWork {
    scope: RunScope,
    request: Option<WorkRequest>,
    now_ms: u64,
    work_id: Ulid,
    serving: ServingNode,
    phase: StartPhase,
}

impl StartWork {
    pub(crate) fn new(
        scope: &RunScope,
        request: WorkRequest,
        (now_ms, work_id): (u64, Ulid),
        serving: ServingNode,
    ) -> Self {
        Self {
            scope: scope.clone(),
            request: Some(request),
            now_ms,
            work_id,
            serving,
            phase: StartPhase::Prepare,
        }
    }

    fn queue(&self, account: UserId, spec: PluginWorkSpec, job_id: JobId) -> QueueJobOperation {
        let submitted = SubmitJobSpec {
            dedup_key: Some(dedup_key(spec.work_id)),
            payload: JobPayload::PluginWork(spec),
            created_by: account,
            owner_node_id: self.scope.local,
            now_ms: self.now_ms,
            retention_ms: RETENTION_MS,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: Some(WORK_ACTIVE),
        };
        QueueJobOperation::new(submitted, job_id, self.serving)
    }
}

fn submit_error(error: SubmitJobError) -> CallError {
    CallError(match error {
        SubmitJobError::ActiveJobLimit { limit } => HostError::new(
            ErrorKind::Limit,
            format!("the install has {limit} unfinished work already"),
        ),
        other => unavailable(other.to_string()),
    })
}

impl Operation for StartWork {
    type Output = Ulid;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Ulid>, CallError> {
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let minted = (self.now_ms, self.work_id);
        let prepare = PrepareWork::new(&self.scope, request, minted);
        Ok(Next::Call(IoCall::child(prepare)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Ulid>, CallError> {
        match std::mem::replace(&mut self.phase, StartPhase::Prepare) {
            StartPhase::Prepare => match result.child::<PreparedWork, CallError>()?? {
                PreparedWork::Known(work_id) => Ok(Next::Done(work_id)),
                PreparedWork::Submit { account, spec } => {
                    let dedup = dedup_key(spec.work_id);
                    let (realm_id, local) = (self.scope.realm_id, self.scope.local);
                    let mint = MintJobOperation::new(realm_id, local, Some(&dedup), self.serving);
                    self.phase = StartPhase::Mint(account, spec);
                    Ok(Next::Call(IoCall::child(mint)))
                }
            },
            StartPhase::Mint(account, spec) => {
                let job_id = result.child::<JobId, SubmitJobError>()?;
                let job_id = job_id.map_err(submit_error)?;
                self.phase = StartPhase::Queue(spec.install_id, spec.work_id);
                let queue = self.queue(account, *spec, job_id);
                Ok(Next::Call(IoCall::child(queue)))
            }
            StartPhase::Queue(install_id, work_id) => {
                let queued = result.child::<SubmitJobResult, SubmitJobError>()?;
                let queued = queued.map_err(submit_error)?;
                self.phase = StartPhase::Record(work_id);
                let change = WorkChange::Dispatched(queued.job_id);
                let update = update_work((install_id, work_id), change);
                Ok(Next::Call(IoCall::child(update)))
            }
            StartPhase::Record(work_id) => {
                let recorded = result.child::<Option<WorkRow>, StoreError>()?;
                recorded.map_err(|error| call_unavailable(error.0))?;
                Ok(Next::Done(work_id))
            }
        }
    }
}

#[cfg(test)]
#[path = "job_work_tests.rs"]
mod tests;
