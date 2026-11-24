//! The event stream of a run: the JSON of each `run`, `state`, `step` and `task` event as the
//! portal reads it, the rows that hold them in sequence, and the read after a sequence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::RUN_EVENT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    RunEventRecord, RunFile, StepRecord, WorkflowRunRecord, WorkflowTaskRecord, event_key,
    run_prefix,
};
use aruna_core::structs::identity::auth::AuthContext;
use byteview::ByteView;
use serde_json::{Value, json};
use ulid::Ulid;

use super::{RunAccessOperation, Writes, access_of};
use crate::workflows::{CatalogError, Need};

/// Steps with at most this many tasks list them in the snapshot; wider scatters leave them out.
pub const LISTED_TASKS: u64 = 25;

/// Milliseconds since the Unix epoch as an RFC 3339 time.
pub fn time_text(ms: u64) -> Option<String> {
    let ms = i64::try_from(ms).ok()?;
    chrono::DateTime::from_timestamp_millis(ms).map(|time| time.to_rfc3339())
}

/// The run as the portal's `WorkflowRun` reads it.
pub fn run_json(record: &WorkflowRunRecord) -> Value {
    json!({
        "run_id": record.run_id.to_string(),
        "group_id": record.group_id.to_string(),
        "workflow_id": record.workflow_id.to_string(),
        "version": record.version.to_string(),
        "state": record.state.as_str(),
        "node_id": record.node_id.to_string(),
        "created_at": time_text(record.created_at_ms),
        "finished_at": record.finished_at_ms.and_then(time_text),
        "created_by": record.created_by.to_string(),
        "error": record.error,
        "lineage": record.lineage.to_string(),
        "workflow_name": record.workflow_name,
        "local_only": record.local_only,
    })
}

/// The fields of the run a state change touches.
pub fn state_json(record: &WorkflowRunRecord) -> Value {
    json!({
        "run_id": record.run_id.to_string(),
        "state": record.state.as_str(),
        "error": record.error,
        "finished_at": record.finished_at_ms.and_then(time_text),
        "node_id": record.node_id.to_string(),
    })
}

/// The delivered outputs of the run, merged into the snapshot like a state change.
pub fn outputs_json(record: &WorkflowRunRecord) -> Value {
    json!({
        "run_id": record.run_id.to_string(),
        "outputs": record.outputs.iter().map(output_json).collect::<Vec<_>>(),
    })
}

fn output_json(file: &RunFile) -> Value {
    json!({
        "name": file.name,
        "bucket": file.bucket.clone().unwrap_or_default(),
        "key": file.key.clone().unwrap_or_default(),
        "version_id": file.version_id,
        "node_id": file.node_id.map(|node| node.to_string()),
        "size": file.size,
    })
}

fn file_json(file: &RunFile) -> Value {
    json!({
        "name": file.name,
        "bucket": file.bucket,
        "key": file.key,
        "version_id": file.version_id,
        "node_id": file.node_id.map(|node| node.to_string()),
        "size": file.size,
        "digest": file.digest,
    })
}

/// One task as the portal's `WorkflowTask` reads it, with the run it belongs to.
pub fn task_json(task: &WorkflowTaskRecord) -> Value {
    json!({
        "run_id": task.run_id.to_string(),
        "task_id": task.task_id.to_string(),
        "step": task.step,
        "state": task.state.as_str(),
        "attempt": task.attempt,
        "node_id": task.node_id.map(|node| node.to_string()),
        "cached": task.cached,
        "started_at": task.started_at_ms.and_then(time_text),
        "finished_at": task.finished_at_ms.and_then(time_text),
        "exit_code": task.exit_code,
        "index": task.index,
        "mode": task.mode,
        "staged_bytes": task.staged_bytes,
        "cpu_seconds": task.cpu_seconds,
        "max_rss_bytes": task.max_rss_bytes,
        "cache_key": task.cache_key,
        "inputs": task.inputs.iter().map(file_json).collect::<Vec<_>>(),
        "outputs": task.outputs.iter().map(file_json).collect::<Vec<_>>(),
    })
}

/// The counts of one step as the portal's `WorkflowStepSummary` reads them.
pub fn step_json(run_id: Ulid, step: &StepRecord) -> Value {
    let mut summary = step_summary(step);
    summary["run_id"] = json!(run_id.to_string());
    summary
}

/// A step's summary without the run id; the snapshot lists these.
pub fn step_summary(step: &StepRecord) -> Value {
    json!({
        "step": step.step,
        "state": step.state().as_str(),
        "total": step.total(),
        "counts": {
            "queued": step.queued,
            "running": step.running,
            "succeeded": step.succeeded,
            "failed": step.failed,
            "cancelled": step.cancelled,
            "cached": step.cached,
        },
        "node_id": step.node_id.map(|node| node.to_string()),
        "mode": step.mode,
    })
}

/// Appends one event row after the newest one of the run.
pub(crate) fn push(
    record: &mut WorkflowRunRecord,
    kind: &str,
    payload: Value,
    writes: &mut Writes,
) -> Result<(), CatalogError> {
    record.last_event_seq += 1;
    let row = RunEventRecord {
        seq: record.last_event_seq,
        event_type: kind.to_owned(),
        payload_json: payload.to_string(),
    };
    writes.push((
        RUN_EVENT_KEYSPACE.to_string(),
        event_key(record.run_id, row.seq),
        ByteView::from(row.to_bytes()?),
    ));
    Ok(())
}

/// The events of a run after a sequence, and the run as it stands, for one caller who reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct EventBatch {
    pub events: Vec<RunEventRecord>,
    pub record: WorkflowRunRecord,
    /// More rows follow the batch than the limit let in.
    pub more: bool,
}

/// Reads at most `limit` events of a run after `after`, checked like the run.
#[derive(Debug, PartialEq)]
pub struct ReadEventsOperation {
    auth: AuthContext,
    run_id: Ulid,
    after: u64,
    limit: usize,
    record: Option<WorkflowRunRecord>,
}

impl ReadEventsOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid, after: u64, limit: usize) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            after,
            limit: limit.max(1),
            record: None,
        }
    }
}

impl Operation for ReadEventsOperation {
    type Output = EventBatch;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<EventBatch>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<EventBatch>, CatalogError> {
        let Some(record) = self.record.take() else {
            let (record, _) = access_of(result)?;
            self.record = Some(record);
            let after = (self.after > 0).then(|| event_key(self.run_id, self.after));
            return Ok(Next::Call(IoCall::Page {
                space: RUN_EVENT_KEYSPACE.to_string(),
                prefix: run_prefix(self.run_id),
                after,
                limit: self.limit,
            }));
        };
        let (rows, next) = result.page()?;
        let events = (rows.iter())
            .map(|(_, value)| RunEventRecord::from_bytes(value))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Next::Done(EventBatch {
            events,
            record,
            more: next.is_some(),
        }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}
