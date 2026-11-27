//! The reports of recorded runs: why a task ran, two runs compared, where the time went, what
//! changed since, and the HTML report. uwe computes them in a coordinator in report mode.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WORKFLOW_TASK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    WorkflowRunRecord, WorkflowTaskRecord, task_key,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError};
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_workflow_contract::message::{CoordinatorMode, CoordinatorToHost, HostToCoordinator};
use aruna_workflow_contract::preflight::RunLogPage;
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use super::admit::PinnedInput;
use super::control::pins_of;
use super::data::ObjectAccessOperation;
use super::events::{run_json, time_text};
use super::log::{LineageLogOperation, LoadLogOperation, pages_answer};
use super::run::{replay_chunks, workflow_answer, workflow_call};
use super::{RunAccessOperation, RunTasksOperation, access_of, tasks_answer};
use crate::workflows::versions::{GetVersionOperation, VersionView};
use crate::workflows::{CatalogError, Need};

/// What one report call answers: the result, or the coordinator's failure for the caller to
/// fall back on or report.
pub type ReportAnswer = Result<PreflightResult, WorkflowError>;

#[derive(Debug, PartialEq)]
enum CallStep {
    Open,
    Replay,
    Send,
    Receive,
    Close,
}

/// Extracts report log pages from `kind` so they stream in bounded `RunReplayPages` frames.
fn take_pages(kind: &mut PreflightKind) -> Vec<RunLogPage> {
    match kind {
        PreflightKind::ComputeWhy { log_pages, .. }
        | PreflightKind::ComputeTime { log_pages, .. }
        | PreflightKind::ComputeChanges { log_pages, .. }
        | PreflightKind::BuildRunCrate { log_pages, .. }
        | PreflightKind::BuildReport { log_pages, .. } => std::mem::take(log_pages),
        PreflightKind::ComputeCompare {
            first_log_pages,
            second_log_pages,
            ..
        } => {
            let mut pages = std::mem::take(first_log_pages);
            pages.extend(std::mem::take(second_log_pages));
            pages
        }
        _ => Vec::new(),
    }
}

/// One report request to a coordinator attempt of its own in report mode: it opens the session,
/// streams log pages in bounded batches, sends the request, waits for the answer and closes.
#[derive(Debug, PartialEq)]
pub struct ReportCallOperation {
    kind: Option<PreflightKind>,
    chunks: VecDeque<Vec<RunLogPage>>,
    run_id: Ulid,
    session: String,
    answer: Option<ReportAnswer>,
    opened: bool,
    step: CallStep,
}

impl ReportCallOperation {
    pub fn new(run_id: Ulid, mut kind: PreflightKind) -> Self {
        let pages = take_pages(&mut kind);
        let chunks = replay_chunks(&pages).into();
        Self {
            kind: Some(kind),
            chunks,
            run_id,
            session: format!("report-{run_id}"),
            answer: None,
            opened: false,
            step: CallStep::Open,
        }
    }

    fn effect(
        &mut self,
        step: CallStep,
        effect: WorkflowEffect,
    ) -> Result<Next<ReportAnswer>, CatalogError> {
        self.step = step;
        Ok(Next::Call(workflow_call(effect)))
    }

    fn send_next(&mut self) -> Result<Next<ReportAnswer>, CatalogError> {
        let session = self.session.clone();
        if let Some(pages) = self.chunks.pop_front() {
            let message = HostToCoordinator::RunReplayPages { pages };
            return self.effect(CallStep::Replay, WorkflowEffect::Send { session, message });
        }
        let kind = self.kind.take().ok_or(IoError::Unfinished)?;
        let message = HostToCoordinator::PreflightRequest {
            request_id: 1,
            kind,
        };
        self.effect(CallStep::Send, WorkflowEffect::Send { session, message })
    }

    /// Keeps the answer and closes the session, which every path after the open does.
    fn close(&mut self, answer: ReportAnswer) -> Result<Next<ReportAnswer>, CatalogError> {
        self.answer = Some(answer);
        let session = self.session.clone();
        self.effect(CallStep::Close, WorkflowEffect::Close { session })
    }

    fn receive(&mut self) -> Result<Next<ReportAnswer>, CatalogError> {
        let session = self.session.clone();
        self.effect(CallStep::Receive, WorkflowEffect::Receive { session })
    }
}

impl Operation for ReportCallOperation {
    type Output = ReportAnswer;
    type Error = CatalogError;

    // Report sessions of one run may run at once, so each gets a name of its own.
    fn seed(&mut self, entropy: u128) {
        self.session = format!("report-{}-{entropy:032x}", self.run_id);
    }

    fn first(&mut self) -> Result<Next<ReportAnswer>, CatalogError> {
        let open = WorkflowEffect::Open {
            session: self.session.clone(),
            mode: CoordinatorMode::Report,
            run_id: self.run_id.to_string(),
            claim_token_digest: String::new(),
        };
        self.effect(CallStep::Open, open)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReportAnswer>, CatalogError> {
        let answer = workflow_answer(result)?;
        match (std::mem::replace(&mut self.step, CallStep::Close), answer) {
            (CallStep::Open, WorkflowAnswer::Opened(Ok(()))) => {
                self.opened = true;
                self.send_next()
            }
            (CallStep::Open, WorkflowAnswer::Opened(Err(error))) => Ok(Next::Done(Err(error))),
            (CallStep::Replay, WorkflowAnswer::Sent(Ok(()))) => self.send_next(),
            (CallStep::Replay, WorkflowAnswer::Sent(Err(error))) => self.close(Err(error)),
            (CallStep::Send, WorkflowAnswer::Sent(Ok(()))) => self.receive(),
            (CallStep::Send, WorkflowAnswer::Sent(Err(error))) => self.close(Err(error)),
            (CallStep::Receive, WorkflowAnswer::Received(received)) => match received {
                Ok(Some(CoordinatorToHost::PreflightResponse { result, .. })) => {
                    self.close(result.map_err(WorkflowError::Refused))
                }
                // A report session sends nothing else; anything that comes first is skipped.
                Ok(Some(_)) => self.receive(),
                Ok(None) => self.close(Err(WorkflowError::Crashed(
                    "the coordinator ended the report session without an answer".into(),
                ))),
                Err(error) => self.close(Err(error)),
            },
            (CallStep::Close, WorkflowAnswer::Closed(_)) => {
                self.opened = false;
                Ok(Next::Done(self.answer.take().ok_or(IoError::Unfinished)?))
            }
            _ => Err(crate::workflows::unexpected(
                "the answer of a report session step",
            )),
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        match std::mem::take(&mut self.opened) {
            true => vec![workflow_call(WorkflowEffect::Close {
                session: self.session.clone(),
            })],
            false => Vec::new(),
        }
    }
}

/// The answer of a [`ReportCallOperation`] child; `None` when the report cannot be computed from
/// the run log, so the caller answers from the run's records instead.
pub(crate) fn report_answer(result: IoResult) -> Result<Option<PreflightResult>, CatalogError> {
    match result.child::<ReportAnswer, CatalogError>()?? {
        Ok(answer) => Ok(Some(answer)),
        Err(
            WorkflowError::Unavailable
            | WorkflowError::Refused(ContractError::NotFound { .. } | ContractError::Invalid { .. }),
        ) => Ok(None),
        Err(error) => Err(CatalogError::Coordinator(error)),
    }
}

fn report_call(run_id: Ulid, kind: PreflightKind) -> IoCall {
    IoCall::child(ReportCallOperation::new(run_id, kind))
}

#[derive(Debug, PartialEq)]
enum WhyStep {
    Access,
    Task,
    Log(Box<WorkflowTaskRecord>),
    Report(Box<WorkflowTaskRecord>),
}

/// Why a task ran, ran again or came from the cache, by uwe's comparison with the same task in
/// the run's lineage, or from the task's record when no earlier run had it.
#[derive(Debug, PartialEq)]
pub struct TaskWhyOperation {
    auth: AuthContext,
    run_id: Ulid,
    task_id: u64,
    step: WhyStep,
}

impl TaskWhyOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid, task_id: u64) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            task_id,
            step: WhyStep::Access,
        }
    }
}

impl Operation for TaskWhyOperation {
    type Output = Value;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        match std::mem::replace(&mut self.step, WhyStep::Access) {
            WhyStep::Access => {
                access_of(result)?;
                self.step = WhyStep::Task;
                Ok(Next::Call(IoCall::Read(vec![(
                    WORKFLOW_TASK_KEYSPACE.to_string(),
                    task_key(self.run_id, self.task_id),
                )])))
            }
            WhyStep::Task => {
                let value = result.value()?.ok_or(CatalogError::NotFound)?;
                let task = WorkflowTaskRecord::from_bytes(&value)?;
                self.step = WhyStep::Log(Box::new(task));
                let lineage = LineageLogOperation::including_repeats(self.run_id);
                Ok(Next::Call(IoCall::child(lineage)))
            }
            WhyStep::Log(task) => {
                let log_pages = pages_answer(result)?;
                if log_pages.is_empty() {
                    return Ok(Next::Done(why_json(&task, None)));
                }
                let kind = PreflightKind::ComputeWhy {
                    run_id: self.run_id.to_string(),
                    task_id: self.task_id,
                    log_pages,
                };
                self.step = WhyStep::Report(task);
                Ok(Next::Call(report_call(self.run_id, kind)))
            }
            WhyStep::Report(task) => match report_answer(result)? {
                Some(PreflightResult::ComputeWhy { why_json: why }) => {
                    Ok(Next::Done(why_json(&task, why.as_ref())))
                }
                Some(_) => Err(crate::workflows::unexpected("PreflightResult::ComputeWhy")),
                None => Ok(Next::Done(why_json(&task, None))),
            },
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// The why answer in the portal's shape: a verdict, a summary and one change per difference.
/// `why` is uwe's comparison with an earlier run, when one had the task.
pub fn why_json(task: &WorkflowTaskRecord, why: Option<&Value>) -> Value {
    let changes: Vec<Value> = (why.and_then(|why| why["changes"].as_array()))
        .into_iter()
        .flatten()
        .map(|change| match change {
            Value::String(text) => json!({ "field": text }),
            other => other.clone(),
        })
        .collect();
    let verdict = match (task.cached, why.is_some()) {
        (true, _) => "cached",
        (false, true) => "reran",
        (false, false) => "ran",
    };
    let summary = match verdict {
        "cached" => {
            "The result came from an earlier run with the same task key; the task did not run."
                .to_owned()
        }
        "reran" if changes.is_empty() => "The task ran again.".to_owned(),
        "reran" => {
            let lines: Vec<&str> = (why.and_then(|why| why["changes"].as_array()))
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            format!("The task ran again: {}", lines.join("; "))
        }
        _ => "The task ran; no earlier run of its lineage had this task.".to_owned(),
    };
    let earlier = why.map(|why| json!({ "run_id": why["run"], "task_id": why["task"] }));
    json!({
        "run_id": task.run_id.to_string(),
        "task_id": task.task_id.to_string(),
        "verdict": verdict,
        "summary": summary,
        "cache_key": task.cache_key,
        "deterministic": task.deterministic,
        "changes": changes,
        "earlier": earlier,
        "source": if why.is_some() { "uwe" } else { "record" },
    })
}

#[derive(Debug, PartialEq)]
enum CompareStep {
    First,
    Second(Box<WorkflowRunRecord>),
    FirstLog(Box<(WorkflowRunRecord, WorkflowRunRecord)>),
    SecondLog(Box<(WorkflowRunRecord, WorkflowRunRecord)>, Vec<RunLogPage>),
    Report(Box<(WorkflowRunRecord, WorkflowRunRecord)>),
}

/// Two runs side by side: their summaries, the values and inputs that differ, and uwe's task by
/// task comparison. The caller must read both runs; either one hidden is not found.
#[derive(Debug, PartialEq)]
pub struct CompareRunsOperation {
    auth: AuthContext,
    first: Ulid,
    second: Ulid,
    step: CompareStep,
}

impl CompareRunsOperation {
    pub fn new(auth: &AuthContext, first: Ulid, second: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            first,
            second,
            step: CompareStep::First,
        }
    }
}

impl Operation for CompareRunsOperation {
    type Output = Value;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.first, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        match std::mem::replace(&mut self.step, CompareStep::First) {
            CompareStep::First => {
                let (first, _) = access_of(result)?;
                self.step = CompareStep::Second(Box::new(first));
                let access = RunAccessOperation::new(&self.auth, self.second, Need::Read);
                Ok(Next::Call(IoCall::child(access)))
            }
            CompareStep::Second(first) => {
                let (second, _) = access_of(result)?;
                self.step = CompareStep::FirstLog(Box::new((*first, second)));
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(self.first))))
            }
            CompareStep::FirstLog(runs) => {
                let pages = pages_answer(result)?;
                self.step = CompareStep::SecondLog(runs, pages);
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.second,
                ))))
            }
            CompareStep::SecondLog(runs, first_log_pages) => {
                let second_log_pages = pages_answer(result)?;
                if first_log_pages.is_empty() || second_log_pages.is_empty() {
                    return Ok(Next::Done(comparison_json(&runs.0, &runs.1, None)));
                }
                let kind = PreflightKind::ComputeCompare {
                    first_run_id: self.first.to_string(),
                    second_run_id: self.second.to_string(),
                    first_log_pages,
                    second_log_pages,
                };
                self.step = CompareStep::Report(runs);
                Ok(Next::Call(report_call(self.first, kind)))
            }
            CompareStep::Report(runs) => {
                let comparison = match report_answer(result)? {
                    Some(PreflightResult::ComputeCompare { comparison_json }) => comparison_json,
                    Some(_) => {
                        return Err(crate::workflows::unexpected(
                            "PreflightResult::ComputeCompare",
                        ));
                    }
                    None => None,
                };
                Ok(Next::Done(comparison_json(
                    &runs.0,
                    &runs.1,
                    comparison.as_ref(),
                )))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// The comparison answer: both runs, what differs in their values and inputs by JSON pointer,
/// and uwe's comparison when the run logs gave one.
pub fn comparison_json(
    first: &WorkflowRunRecord,
    second: &WorkflowRunRecord,
    comparison: Option<&Value>,
) -> Value {
    let parsed = |text: Option<&str>| {
        text.and_then(|text| serde_json::from_str::<Value>(text).ok())
            .unwrap_or(Value::Null)
    };
    json!({
        "first": run_json(first),
        "second": run_json(second),
        "same_workflow": first.workflow_id == second.workflow_id,
        "same_version": first.workflow_id == second.workflow_id && first.version == second.version,
        "values": diff_values(&parsed(Some(&first.values_json)), &parsed(Some(&second.values_json))),
        "inputs": diff_values(&parsed(first.inputs_json.as_deref()), &parsed(second.inputs_json.as_deref())),
        "comparison": comparison,
    })
}

/// The values that differ between two documents, by JSON pointer; lists compare whole.
pub fn diff_values(first: &Value, second: &Value) -> Vec<Value> {
    let mut left = BTreeMap::new();
    let mut right = BTreeMap::new();
    flatten(first, String::new(), &mut left);
    flatten(second, String::new(), &mut right);
    let mut changes = Vec::new();
    for (path, value) in &left {
        match right.get(path) {
            None => changes.push(json!({"path": path, "first": value, "kind": "removed"})),
            Some(other) if other != value => changes
                .push(json!({"path": path, "first": value, "second": other, "kind": "changed"})),
            Some(_) => {}
        }
    }
    for (path, value) in &right {
        if !left.contains_key(path) {
            changes.push(json!({"path": path, "second": value, "kind": "added"}));
        }
    }
    changes.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    changes
}

fn flatten(value: &Value, pointer: String, into: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (key, child) in map {
                let token = key.replace('~', "~0").replace('/', "~1");
                flatten(child, format!("{pointer}/{token}"), into);
            }
        }
        Value::Null if pointer.is_empty() => {}
        other => {
            into.insert(pointer, other.clone());
        }
    }
}

#[derive(Debug, PartialEq)]
enum TimeStep {
    Access,
    Log(Box<WorkflowRunRecord>),
    Report(Box<WorkflowRunRecord>),
    Tasks(Box<WorkflowRunRecord>),
}

/// Where the time of a run went, as uwe reads its run log; without a log, from the tasks.
#[derive(Debug, PartialEq)]
pub struct RunTimeOperation {
    auth: AuthContext,
    run_id: Ulid,
    now_ms: u64,
    step: TimeStep,
}

impl RunTimeOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            now_ms: 0,
            step: TimeStep::Access,
        }
    }

    fn tasks(&mut self, record: Box<WorkflowRunRecord>) -> Result<Next<Value>, CatalogError> {
        self.step = TimeStep::Tasks(record);
        Ok(Next::Call(IoCall::child(RunTasksOperation::new(
            self.run_id,
        ))))
    }
}

impl Operation for RunTimeOperation {
    type Output = Value;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        match std::mem::replace(&mut self.step, TimeStep::Access) {
            TimeStep::Access => {
                let (record, _) = access_of(result)?;
                self.step = TimeStep::Log(Box::new(record));
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.run_id,
                ))))
            }
            TimeStep::Log(record) => {
                let log_pages = pages_answer(result)?;
                if log_pages.is_empty() {
                    return self.tasks(record);
                }
                let kind = PreflightKind::ComputeTime {
                    run_id: self.run_id.to_string(),
                    log_pages,
                };
                self.step = TimeStep::Report(record);
                Ok(Next::Call(report_call(self.run_id, kind)))
            }
            TimeStep::Report(record) => match report_answer(result)? {
                Some(PreflightResult::ComputeTime {
                    time_json: Some(mut time),
                }) => {
                    time["run_id"] = json!(self.run_id.to_string());
                    time["source"] = json!("uwe");
                    Ok(Next::Done(time))
                }
                Some(PreflightResult::ComputeTime { time_json: None }) | None => self.tasks(record),
                Some(_) => Err(crate::workflows::unexpected("PreflightResult::ComputeTime")),
            },
            TimeStep::Tasks(record) => {
                let tasks = tasks_answer(result)?;
                Ok(Next::Done(time_json(&record, &tasks, self.now_ms)))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// The time facet from the run's records, in the fields of uwe's time report; waits and the
/// critical path are unknown without the run log.
pub fn time_json(record: &WorkflowRunRecord, tasks: &[WorkflowTaskRecord], now_ms: u64) -> Value {
    let started = record.created_at_ms;
    let ended = record.finished_at_ms.unwrap_or(now_ms).max(started);
    let length = |task: &WorkflowTaskRecord| match (task.started_at_ms, task.finished_at_ms) {
        (Some(start), Some(end)) if !task.cached => end.saturating_sub(start),
        _ => 0,
    };
    let mut steps: Vec<(String, u64, u64)> = Vec::new();
    for task in tasks {
        let position = match steps.iter().position(|(step, ..)| *step == task.step) {
            Some(position) => position,
            None => {
                steps.push((task.step.clone(), 0, 0));
                steps.len() - 1
            }
        };
        steps[position].1 += 1;
        steps[position].2 += length(task);
    }
    let steps: Vec<Value> = (steps.into_iter())
        .map(|(step, count, ran)| json!({"step": step, "tasks": count, "ran": ran, "waited": 0}))
        .collect();
    json!({
        "run_id": record.run_id.to_string(),
        "started": started,
        "ended": ended,
        "tasks": tasks.len(),
        "reused": tasks.iter().filter(|task| task.cached).count(),
        "ran": tasks.iter().map(length).sum::<u64>(),
        "waited": 0,
        "busy": busy_ms(tasks),
        "before": 0,
        "after": 0,
        "critical_path": [],
        "steps": steps,
        "source": "record",
    })
}

/// How long at least one task ran: the length of the union of the task intervals.
fn busy_ms(tasks: &[WorkflowTaskRecord]) -> u64 {
    let mut spans: Vec<(u64, u64)> = (tasks.iter())
        .filter(|task| !task.cached)
        .filter_map(|task| Some((task.started_at_ms?, task.finished_at_ms?)))
        .filter(|(start, end)| end > start)
        .collect();
    spans.sort_unstable();
    let mut busy = 0;
    let mut reach = 0;
    for (start, end) in spans {
        let from = start.max(reach);
        if end > from {
            busy += end - from;
        }
        reach = reach.max(end);
    }
    busy
}

/// The changes found so far and the inputs still to check.
#[derive(Debug, PartialEq)]
struct Found {
    record: WorkflowRunRecord,
    recorded: Option<Value>,
    changes: Vec<Value>,
    pending: Vec<PinnedInput>,
}

#[derive(Debug, PartialEq)]
enum ChangesStep {
    Access,
    Log(Box<WorkflowRunRecord>),
    Report(Box<WorkflowRunRecord>),
    Version(Box<Found>),
    Input(Box<Found>, PinnedInput),
    Head(Box<Found>, PinnedInput),
}

/// What changed since a run: a newer workflow version, another head version of a pinned input on
/// this node, or another image pin, measured against what the run log recorded.
#[derive(Debug, PartialEq)]
pub struct RunChangesOperation {
    auth: AuthContext,
    run_id: Ulid,
    step: ChangesStep,
}

impl RunChangesOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            step: ChangesStep::Access,
        }
    }

    fn version(
        &mut self,
        record: WorkflowRunRecord,
        recorded: Option<Value>,
    ) -> Result<Next<Value>, CatalogError> {
        let pending = match record.inputs_json.as_deref() {
            Some(text) => pins_of(&crate::workflows::from_text(text)?)?,
            None => Vec::new(),
        };
        let read = GetVersionOperation::new(&self.auth, record.workflow_id, record.version);
        let found = Found {
            record,
            recorded,
            changes: Vec::new(),
            pending: pending.into_iter().rev().collect(),
        };
        self.step = ChangesStep::Version(Box::new(found));
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Checks the next pinned input this node holds; inputs of other nodes are left out.
    fn next_input(&mut self, mut found: Box<Found>) -> Result<Next<Value>, CatalogError> {
        let node_id = found.record.node_id;
        while let Some(pin) = found.pending.pop() {
            if pin.source_node_id.is_some_and(|source| source != node_id) {
                continue;
            }
            let access = ObjectAccessOperation::new(
                &self.auth,
                node_id,
                (&pin.bucket, &pin.key),
                Permission::READ,
            );
            self.step = ChangesStep::Input(found, pin);
            return Ok(Next::Call(IoCall::child(access)));
        }
        let changed = !found.changes.is_empty();
        Ok(Next::Done(json!({
            "run_id": found.record.run_id.to_string(),
            "changed": changed,
            "changes": found.changes,
            "recorded": found.recorded,
        })))
    }
}

impl Operation for RunChangesOperation {
    type Output = Value;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        match std::mem::replace(&mut self.step, ChangesStep::Access) {
            ChangesStep::Access => {
                let (record, _) = access_of(result)?;
                self.step = ChangesStep::Log(Box::new(record));
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.run_id,
                ))))
            }
            ChangesStep::Log(record) => {
                let log_pages = pages_answer(result)?;
                if log_pages.is_empty() {
                    return self.version(*record, None);
                }
                let kind = PreflightKind::ComputeChanges {
                    run_id: self.run_id.to_string(),
                    log_pages,
                };
                self.step = ChangesStep::Report(record);
                Ok(Next::Call(report_call(self.run_id, kind)))
            }
            ChangesStep::Report(record) => match report_answer(result)? {
                Some(PreflightResult::ComputeChanges { changes_json }) => {
                    self.version(*record, Some(changes_json))
                }
                Some(_) => Err(crate::workflows::unexpected(
                    "PreflightResult::ComputeChanges",
                )),
                None => self.version(*record, None),
            },
            ChangesStep::Version(mut found) => {
                match result.child::<VersionView, CatalogError>()? {
                    Ok(view) => version_changes(&mut found, &view),
                    Err(CatalogError::NotFound | CatalogError::Forbidden) => {
                        found.changes.push(json!({
                            "field": "workflow.version",
                            "before": found.record.version,
                            "after": null,
                            "summary": "the workflow version is no longer readable",
                        }));
                    }
                    Err(error) => return Err(error),
                }
                self.next_input(found)
            }
            ChangesStep::Input(found, pin) => match result.child::<BucketInfo, CatalogError>()? {
                Ok(_) => {
                    let head = HeadObjectOperation::new(HeadObjectInput {
                        bucket: pin.bucket.clone(),
                        key: pin.key.clone(),
                        version_id: None,
                    });
                    self.step = ChangesStep::Head(found, pin);
                    Ok(Next::Call(IoCall::child(head)))
                }
                Err(CatalogError::NotFound | CatalogError::Forbidden) => {
                    let mut found = found;
                    found
                        .changes
                        .push(input_change(&pin, None, "the input is no longer readable"));
                    self.next_input(found)
                }
                Err(error) => Err(error),
            },
            ChangesStep::Head(mut found, pin) => {
                match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => {
                        let current = head.resolved_version_id.or(head.version_id);
                        if current != Some(pin.version_id) {
                            let summary = "the input has a newer version";
                            found.changes.push(input_change(&pin, current, summary));
                        }
                    }
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => {
                        let summary = "the input was deleted";
                        found.changes.push(input_change(&pin, None, summary));
                    }
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                }
                self.next_input(found)
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

fn input_change(pin: &PinnedInput, current: Option<Ulid>, summary: &str) -> Value {
    json!({
        "field": format!("inputs.{}", pin.name),
        "before": format!("s3://{}/{}@{}", pin.bucket, pin.key, pin.version_id),
        "after": current.map(|version| format!("s3://{}/{}@{version}", pin.bucket, pin.key)),
        "summary": summary,
    })
}

/// A newer version of the workflow, and images that ran at another pin than the version holds.
fn version_changes(found: &mut Found, view: &VersionView) {
    let latest = view.workflow.version_count;
    if latest > found.record.version {
        found.changes.push(json!({
            "field": "workflow.version",
            "before": found.record.version,
            "after": latest,
            "summary": "the workflow has a newer version",
        }));
    }
    let images = (found.recorded.as_ref())
        .and_then(|recorded| recorded["images"].as_array())
        .cloned()
        .unwrap_or_default();
    for image in images {
        let (Some(named), Some(ran)) = (image["named"].as_str(), image["ran"].as_str()) else {
            continue;
        };
        match view.version.images.get(named) {
            Some(pinned) if pinned != ran => found.changes.push(json!({
                "field": format!("images.{named}"),
                "before": ran,
                "after": pinned,
                "summary": "the image is pinned to another digest now",
            })),
            _ => {}
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReportStep {
    Access,
    Log(Box<WorkflowRunRecord>),
    Report(Box<WorkflowRunRecord>),
    Tasks(Box<WorkflowRunRecord>),
}

/// The standalone HTML report of a run as uwe writes it; a run without a run log gets a short
/// report from its records.
#[derive(Debug, PartialEq)]
pub struct RunReportOperation {
    auth: AuthContext,
    run_id: Ulid,
    step: ReportStep,
}

impl RunReportOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            step: ReportStep::Access,
        }
    }

    fn tasks(&mut self, record: Box<WorkflowRunRecord>) -> Result<Next<String>, CatalogError> {
        self.step = ReportStep::Tasks(record);
        Ok(Next::Call(IoCall::child(RunTasksOperation::new(
            self.run_id,
        ))))
    }
}

impl Operation for RunReportOperation {
    type Output = String;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<String>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, CatalogError> {
        match std::mem::replace(&mut self.step, ReportStep::Access) {
            ReportStep::Access => {
                let (record, _) = access_of(result)?;
                self.step = ReportStep::Log(Box::new(record));
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.run_id,
                ))))
            }
            ReportStep::Log(record) => {
                let log_pages = pages_answer(result)?;
                if log_pages.is_empty() {
                    return self.tasks(record);
                }
                let kind = PreflightKind::BuildReport {
                    run_id: self.run_id.to_string(),
                    log_pages,
                };
                self.step = ReportStep::Report(record);
                Ok(Next::Call(report_call(self.run_id, kind)))
            }
            ReportStep::Report(record) => match report_answer(result)? {
                Some(PreflightResult::BuildReport { html }) => Ok(Next::Done(html)),
                Some(_) => Err(crate::workflows::unexpected("PreflightResult::BuildReport")),
                None => self.tasks(record),
            },
            ReportStep::Tasks(record) => {
                let tasks = tasks_answer(result)?;
                Ok(Next::Done(report_html(&record, &tasks)))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// A short HTML report from the run's records, for a run that left no run log.
pub fn report_html(record: &WorkflowRunRecord, tasks: &[WorkflowTaskRecord]) -> String {
    let mut rows = String::new();
    for task in tasks {
        rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            escape(&task.name),
            escape(&task.step),
            task.state.as_str(),
            if task.cached { "reused" } else { "" },
        ));
    }
    let finished = record
        .finished_at_ms
        .and_then(time_text)
        .unwrap_or_default();
    let error = (record.error.as_deref())
        .map(|error| format!("<p>Error: {}</p>\n", escape(error)))
        .unwrap_or_default();
    format!(
        "<!doctype html>\n<html lang=\"en\">\n<head><meta charset=\"utf-8\"><title>Run {run}</title></head>\n<body>\n<h1>Run {run} of {name}</h1>\n<p>State: {state}. Started {started}. Ended {finished}.</p>\n{error}<table>\n<tr><th>Task</th><th>Step</th><th>State</th><th>Result</th></tr>\n{rows}</table>\n</body>\n</html>\n",
        run = record.run_id,
        name = escape(&record.workflow_name),
        state = record.state.as_str(),
        started = time_text(record.created_at_ms).unwrap_or_default(),
    )
}

fn escape(text: &str) -> String {
    (text
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;"))
    .replace('"', "&quot;")
    .replace('\'', "&#39;")
}
