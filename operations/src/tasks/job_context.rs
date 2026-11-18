//! The context one job run gets from the job runner, and the seam to payload handlers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{JobId, JobPayload};
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::driver::DriverContext;
use crate::jobs::executor::{JobRunOutcome, ProgressReporter, RangeTurns};

pub struct JobContext {
    pub driver: Arc<DriverContext>,
    pub job_id: JobId,
    pub owner_node_id: NodeId,
    pub claim_token: ulid::Ulid,
    pub final_attempt: bool,
    /// User-initiated cancel: terminal `Cancelled` plus cleanup.
    pub cancel: CancellationToken,
    /// Node shutdown: stop where you are, the lease is handed back and the job re-runs.
    pub shutdown: CancellationToken,
    pub progress: ProgressReporter,
    pub range_turns: RangeTurns,
    /// Runs the payload kinds that other domains own, as the node wiring registered them.
    pub handlers: Vec<Arc<dyn PayloadHandler>>,
}

impl JobContext {
    /// This job as the repository and harvest operations see it.
    pub fn job_scope(&self) -> crate::jobs::repository::JobScope {
        crate::jobs::repository::JobScope {
            job_id: self.job_id,
            owner_node: self.owner_node_id,
            claim_token: self.claim_token,
            stop: aruna_blob::remote::job_stop(self.cancel.clone(), self.shutdown.clone()),
            progress: self.progress.clone(),
        }
    }
}

/// Runs and keeps the jobs of a payload kind that `jobs` does not own. The node wiring
/// registers one per owning domain.
pub trait PayloadHandler: Send + Sync {
    /// Runs one job, or `None` when this handler does not own the payload.
    fn run<'a>(
        &'a self,
        ctx: &'a JobContext,
        payload: &'a JobPayload,
    ) -> Option<BoxFuture<'a, JobRunOutcome>>;

    /// Whether the job prune keeps this job for now.
    fn keeps_job<'a>(
        &'a self,
        context: &'a DriverContext,
        payload: &'a JobPayload,
    ) -> BoxFuture<'a, Result<bool, String>>;
}
