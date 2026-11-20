//! The context one job run gets from the job runner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::identity::JobId;
use tokio_util::sync::CancellationToken;

use crate::driver::DriverContext;
use crate::jobs::executor::RangeTurns;
use aruna_core::jobs::ProgressReporter;

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
}
