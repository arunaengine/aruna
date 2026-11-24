//! Drives the coordinator session of one workflow run job: frames go to the run's operations in
//! order, host calls run side by side, and cancellation or shutdown ends the session.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::id::NodeId;
use aruna_core::jobs::JobRunOutcome;
use aruna_core::structs::execution::job::local::{JobError, JobResultPayload};
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, WorkflowRunSpec};
use aruna_core::structs::identity::realm::RealmId;
use aruna_ops_work::jobs::lifecycle::submit_external::IngressNode;
use aruna_ops_work::jobs::node_inputs::ingress_node;
use aruna_ops_work::workflows::runs::provenance::WriteProvenanceOperation;
use aruna_ops_work::workflows::runs::recover::{InterruptRunOperation, Interrupted};
use aruna_ops_work::workflows::runs::replica::ReplicateLogOperation;
use aruna_ops_work::workflows::runs::run::{
    AnswerCallOperation, ApplyEventOperation, CallNode, Chunk, FinishRunOperation, Inbound,
    OpenUploadOperation, PrepareRunOperation, Prepared, SessionOperation, Uploads, claim_digest,
    inbound,
};
use aruna_runner::driver::{DriverContext, drive, node_routing};
use aruna_runner::jobs::context::JobContext;
use aruna_workflow_contract::host::HostRpcResult;
use aruna_workflow_contract::message::{CoordinatorToHost, HostToCoordinator, RunFinishedPayload};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::warn;
use ulid::Ulid;

/// How long a cancelled run's coordinator may take to stop its tasks and report.
const CANCEL_GRACE: Duration = Duration::from_secs(60);
/// Frames read ahead of the session loop.
const FRAME_QUEUE: usize = 64;
/// How often a running run sends its new log pages to the holders of its group's records.
const REPLICATE_EVERY: Duration = Duration::from_secs(60);

/// Runs one attempt of `spec`'s run on this node until the run ends, the job is cancelled or
/// the node shuts down.
pub(crate) async fn run_workflow(ctx: &JobContext, spec: &WorkflowRunSpec) -> JobRunOutcome {
    let claim = claim_digest(ctx.claim_token);
    let prepare = PrepareRunOperation::new(spec, ctx.owner_node_id, claim);
    let prepared = tokio::select! {
        biased;
        () = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        prepared = drive(prepare, &ctx.driver) => prepared,
    };
    wake(&ctx.driver, spec.run_id);
    let outcome = match prepared {
        Ok(Prepared::Ready(record)) => Session::new(ctx, spec, *record).run().await,
        Ok(Prepared::Ended(state)) => ended(spec.run_id, state, None),
        // The run waits for the next attempt, or fails on the last one, never stays planning.
        Err(error) => Session::interrupt(ctx, spec.run_id, error.to_string()).await,
    };
    if !matches!(outcome, JobRunOutcome::Interrupted) {
        write_crate(ctx, spec).await;
        let local = (ctx.owner_node_id, spec.realm_id);
        replicate(ctx.driver.clone(), spec.run_id, local, Arc::default()).await;
    }
    outcome
}

/// Sends the run and its log pages from the last sent page on to the holders of the run
/// group's records, so another node can resume the run while this one is unreachable.
async fn replicate(
    driver: Arc<DriverContext>,
    run_id: Ulid,
    local: (NodeId, RealmId),
    sent: Arc<AtomicU32>,
) {
    let send = ReplicateLogOperation::new(run_id, local, sent.load(Ordering::Relaxed));
    match drive(send, &driver).await {
        Ok(pages) => sent.store(pages, Ordering::Relaxed),
        Err(error) => warn!(%error, run = %run_id, "the run log was not copied"),
    }
}

/// Writes the crate of a run that ended, once; a run that goes on is left alone. A failed
/// publication is recorded on the run, so this only warns.
async fn write_crate(ctx: &JobContext, spec: &WorkflowRunSpec) {
    let write = WriteProvenanceOperation::new(spec.run_id, (ctx.owner_node_id, spec.realm_id));
    if let Err(error) = drive(write, &ctx.driver).await {
        warn!(%error, run = %spec.run_id, "the run crate was not written");
    }
}

/// Tells the run's event streams that it stored new events.
fn wake(driver: &DriverContext, run_id: Ulid) {
    if let Some(registry) = driver.compute_handle.as_ref() {
        registry.wake_run(run_id.to_string());
    }
}

/// The job outcome of a run that ended in `state`.
fn ended(run_id: Ulid, state: RunState, error: Option<String>) -> JobRunOutcome {
    match state {
        RunState::Succeeded => JobRunOutcome::Succeeded(JobResultPayload::WorkflowRun {
            run_id,
            state: state.as_str().to_owned(),
        }),
        RunState::Cancelled => JobRunOutcome::Cancelled,
        _ => JobRunOutcome::Failed(JobError::permanent(
            error.unwrap_or_else(|| format!("the run ended as {}", state.as_str())),
        )),
    }
}

/// Reads the session's frames into a queue, so a frame is never lost to a cancelled read.
async fn read_frames(
    driver: Arc<DriverContext>,
    run_id: Ulid,
    frames: mpsc::Sender<Result<CoordinatorToHost, String>>,
) {
    loop {
        let read = match drive(SessionOperation::receive(run_id), &driver).await {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err("the coordinator ended its channel".to_owned()),
            Err(error) => Err(error.to_string()),
        };
        let last = read.is_err();
        if frames.send(read).await.is_err() || last {
            return;
        }
    }
}

/// One open coordinator session of a run.
struct Session<'a> {
    ctx: &'a JobContext,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    run: WorkflowRunRecord,
    uploads: Uploads,
    calls: JoinSet<()>,
    /// The log pages the holders of the run group's records got so far.
    sent: Arc<AtomicU32>,
}

impl<'a> Session<'a> {
    fn new(ctx: &'a JobContext, spec: &WorkflowRunSpec, run: WorkflowRunRecord) -> Self {
        Self {
            ctx,
            realm_id: spec.realm_id,
            run,
            uploads: Uploads::default(),
            calls: JoinSet::new(),
            sent: Arc::default(),
        }
    }

    async fn run(mut self) -> JobRunOutcome {
        let (sender, mut frames) = mpsc::channel(FRAME_QUEUE);
        let driver = self.ctx.driver.clone();
        let reader = tokio::spawn(read_frames(driver, self.run.run_id, sender));
        let mut deadline: Option<tokio::time::Instant> = None;
        let mut copies = tokio::time::interval(REPLICATE_EVERY);
        let outcome = loop {
            let grace = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = self.ctx.shutdown.cancelled() => break self.shutdown().await,
                () = self.ctx.cancel.cancelled(), if deadline.is_none() => {
                    deadline = Some(tokio::time::Instant::now() + CANCEL_GRACE);
                    self.cancel().await;
                }
                () = grace => {
                    let reason = "the coordinator did not stop in time".to_owned();
                    break self.finish(RunState::Cancelled, Some(reason)).await;
                }
                frame = frames.recv() => match frame {
                    Some(Ok(frame)) => {
                        if let Some(outcome) = self.frame(frame).await {
                            break outcome;
                        }
                    }
                    Some(Err(reason)) => break Self::interrupt(self.ctx, self.run.run_id, reason).await,
                    None => {
                        let reason = "the coordinator channel closed".to_owned();
                        break Self::interrupt(self.ctx, self.run.run_id, reason).await;
                    }
                },
                Some(_) = self.calls.join_next() => {}
                _ = copies.tick() => {
                    let driver = self.ctx.driver.clone();
                    let local = (self.ctx.owner_node_id, self.realm_id);
                    let copy = replicate(driver, self.run.run_id, local, self.sent.clone());
                    self.calls.spawn(copy);
                }
            }
        };
        reader.abort();
        self.calls.abort_all();
        outcome
    }

    /// This node as a call sees it now: its routing and its fresh heartbeats.
    fn call_node(&self) -> CallNode {
        let driver = &self.ctx.driver;
        let now_ms = unix_timestamp_millis();
        let serving = driver.node_and_realm();
        let ingress =
            ingress_node(serving, driver.fresh_heartbeats(), now_ms).unwrap_or_else(|| {
                IngressNode {
                    local: self.ctx.owner_node_id,
                    realm_id: self.realm_id,
                    now_ms,
                    fresh: Default::default(),
                }
            });
        CallNode {
            node_id: self.ctx.owner_node_id,
            realm_id: self.realm_id,
            routing: node_routing(driver),
            ingress,
        }
    }

    /// Answers a call beside the others; each sends its own answer when it is done.
    fn spawn(&mut self, answer: AnswerCallOperation) {
        let driver = self.ctx.driver.clone();
        let run_id = self.run.run_id;
        self.calls.spawn(async move {
            if let Err(error) = drive(answer, &driver).await {
                warn!(%error, run = %run_id, "a coordinator call was not answered");
            }
            wake(&driver, run_id);
        });
    }

    /// Sends one answer the session decided without an operation of its own.
    async fn reply(&self, call_id: u64, result: HostRpcResult) {
        let message = HostToCoordinator::RpcResponse { call_id, result };
        let send = SessionOperation::send(self.run.run_id, message);
        if let Err(error) = drive(send, &self.ctx.driver).await {
            warn!(%error, run = %self.run.run_id, "a coordinator answer was not sent");
        }
    }

    async fn frame(&mut self, frame: CoordinatorToHost) -> Option<JobRunOutcome> {
        match inbound(frame) {
            Inbound::Call { call_id, request } => {
                let answer =
                    AnswerCallOperation::new(self.run.clone(), self.call_node(), call_id, request);
                self.spawn(answer);
            }
            Inbound::UploadStart { call_id, address } => {
                let open = OpenUploadOperation::new(self.run.clone(), call_id, address);
                match drive(open, &self.ctx.driver).await {
                    Ok(Some(target)) => self.uploads.open(call_id, target),
                    Ok(None) => {}
                    Err(error) => warn!(%error, run = %self.run.run_id, "an upload was not opened"),
                }
            }
            Inbound::UploadChunk {
                call_id,
                upload_id,
                seq,
                bytes,
                eof,
            } => match self.uploads.chunk(&upload_id, seq, bytes, eof) {
                Chunk::Answer(result) => self.reply(call_id, result).await,
                Chunk::Store { target, bytes, seq } => {
                    let node = self.call_node();
                    let store = (target, bytes, seq);
                    let answer =
                        AnswerCallOperation::upload(self.run.clone(), node, call_id, store);
                    self.spawn(answer);
                }
            },
            Inbound::Event(event) => {
                let apply = ApplyEventOperation::new(self.run.run_id, &event);
                match drive(apply, &self.ctx.driver).await {
                    Ok(Some(_)) => wake(&self.ctx.driver, self.run.run_id),
                    Ok(None) => {}
                    Err(error) => {
                        warn!(%error, run = %self.run.run_id, "a live event was not stored")
                    }
                }
            }
            Inbound::Finished(finished) => return Some(self.finished(finished).await),
            Inbound::Unexpected(what) => {
                warn!(run = %self.run.run_id, what, "the coordinator sent an unexpected frame");
            }
        }
        None
    }

    async fn finished(&mut self, finished: RunFinishedPayload) -> JobRunOutcome {
        let finish = FinishRunOperation::reported(self.run.run_id, &finished);
        self.record_end(finish, finished.error).await
    }

    async fn finish(&mut self, state: RunState, error: Option<String>) -> JobRunOutcome {
        let finish = FinishRunOperation::new(self.run.run_id, state, error.clone());
        self.record_end(finish, error).await
    }

    async fn record_end(
        &mut self,
        finish: FinishRunOperation,
        error: Option<String>,
    ) -> JobRunOutcome {
        let run_id = self.run.run_id;
        let outcome = match drive(finish, &self.ctx.driver).await {
            Ok(record) => ended(run_id, record.state, record.error.or(error)),
            Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
        };
        wake(&self.ctx.driver, run_id);
        outcome
    }

    /// Asks the coordinator to stop the run; a coordinator that cannot be told ends it here.
    async fn cancel(&mut self) {
        let cancel = SessionOperation::cancel(self.run.run_id, "the run was cancelled");
        if let Err(error) = drive(cancel, &self.ctx.driver).await {
            warn!(%error, run = %self.run.run_id, "the coordinator was not told to stop");
        }
    }

    /// The node stops: the session ends, the children run on, and the next attempt adopts them.
    async fn shutdown(&mut self) -> JobRunOutcome {
        let close = SessionOperation::close(self.run.run_id);
        if let Err(error) = drive(close, &self.ctx.driver).await {
            warn!(%error, run = %self.run.run_id, "the coordinator session was not closed");
        }
        JobRunOutcome::Interrupted
    }

    /// The coordinator broke off: another attempt replays the run, or the last one fails it.
    async fn interrupt(ctx: &JobContext, run_id: Ulid, reason: String) -> JobRunOutcome {
        let interrupt = InterruptRunOperation::new(run_id, ctx.final_attempt, reason.clone());
        let outcome = match drive(interrupt, &ctx.driver).await {
            Ok(Interrupted::Retry) => JobRunOutcome::Failed(JobError::retryable(reason)),
            Ok(Interrupted::Ended(state)) => ended(run_id, state, Some(reason)),
            Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
        };
        wake(&ctx.driver, run_id);
        outcome
    }
}
