//! Takes launch offers at the execution target: gathers what only this node knows, drives the
//! operation that decides, and performs the wakeups an admission owes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::{ExecutorKind, ResourceEnvelope};
use aruna_core::effects::{LaunchFrame, ReceiptFrame};
use aruna_core::events::LaunchDecline;
use aruna_core::structs::execution::job::{JobFamilyRecord, LaunchIntent};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use super::cancel::cancel_local_run;
use super::launch_admit::{AdmitLaunchOperation, CommitReceiptOperation, LaunchAnswer, LaunchNode};
use super::reservation::ReserveExecutionConfig;
use crate::driver::{DriverContext, drive};

/// Admits one offered launch, or answers why it was refused. `None` means the
/// target could not decide at all, which is an availability answer and never a
/// refusal the scheduler should act on.
pub async fn admit_launch(
    context: &Arc<DriverContext>,
    launch: LaunchFrame,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let (local, realm_id) = context.node_and_realm()?;
    let limits = match &launch.envelope().record {
        JobFamilyRecord::Launch(intent) => backend_limits(context, intent),
        _ => None,
    };
    let node = LaunchNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        execution_id: Ulid::generate(),
        limits,
    };
    let admit = AdmitLaunchOperation::new(launch, node);
    let answer = drive(admit, context.as_ref()).await.ok()?;
    Some(settle(context, answer).await)
}

/// Commits one stored receipt with its reservation. `None` is undecidable: the
/// offer was neither admitted nor refused and the scheduler may ask again.
pub(crate) async fn commit_receipt(
    context: &Arc<DriverContext>,
    config: ReserveExecutionConfig,
    intent: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    let commit = CommitReceiptOperation::new(config, intent.clone());
    let answer = drive(commit, context.as_ref()).await.ok()?;
    Some(settle(context, answer).await)
}

/// Answers one decided offer and, when it was admitted, re-arms the runtime the
/// acceptance owes, after applying any cancellation to the committed physical row.
pub(crate) async fn settle(
    context: &Arc<DriverContext>,
    answer: LaunchAnswer,
) -> Result<ReceiptFrame, LaunchDecline> {
    if answer.decision.is_ok() {
        if let Some(job_id) = answer.cancel {
            cancel_local_run(context.as_ref(), job_id).await;
        }
        super::outbox::timers::kick(context).await;
        schedule_local(context).await;
    }
    answer.decision
}

async fn schedule_local(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref() {
        use aruna_core::handle::Handle;
        let _ = task
            .send_effect(crate::jobs::submit::schedule_drain_effect())
            .await;
    }
}

/// This node's own advertisement for the offered executor kind.
fn backend_limits(context: &DriverContext, intent: &LaunchIntent) -> Option<ResourceEnvelope> {
    let registry: &ExecutorRegistry = context.compute_handle.as_ref()?;
    let backend = registry.get(&ExecutorKind::from_wire(&intent.target.executor_kind))?;
    Some(backend.capabilities().limits)
}
