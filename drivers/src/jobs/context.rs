//! The context one job run gets from the job runner, and the seams to payload handlers and to
//! the reconciler of lost external attempts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord};
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::driver::DriverContext;
use crate::jobs::executor::RangeTurns;
use aruna_core::jobs::{JobRunOutcome, ProgressReporter};

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

/// Runs the jobs of a payload kind that `jobs` does not own. The node wiring registers one per
/// owning domain.
pub trait PayloadHandler: Send + Sync {
    /// Runs one job, or `None` when this handler does not own the payload.
    fn run<'a>(
        &'a self,
        ctx: &'a JobContext,
        payload: &'a JobPayload,
    ) -> Option<BoxFuture<'a, JobRunOutcome>>;
}

/// Reconciliation seam for lost external attempts. A lost lease or node restart must never
/// blindly requeue one (that double-runs the container); the sweep and recovery route it here.
/// Until a reconciler is registered the job is untouched.
pub trait ExternalReconciler: Send + Sync {
    fn reconcile_lost_attempt<'a>(
        &'a self,
        context: &'a DriverContext,
        record: JobRecord,
    ) -> BoxFuture<'a, ()>;
}
