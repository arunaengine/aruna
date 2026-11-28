//! Background worker ticking automation event matching, schedule triggers and DAG firing
//! progress until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::id::NodeId;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmId;
use aruna_ops_work::automations::control::AckCancelsOperation;
use aruna_ops_work::automations::matcher::MatchEventsOperation;
use aruna_ops_work::automations::runner::StepFiringsOperation;
use aruna_ops_work::automations::timer::TickTimersOperation;
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::jobs::runtime::JobsRuntime;
use aruna_scheduler::shutdown::Shutdown;
use tracing::{debug, warn};

const WORKER_IDLE: Duration = Duration::from_secs(2);
const WORKER_BUSY: Duration = Duration::from_millis(200);

/// Runs automation journal matching, schedule ticks and DAG firing advancement until shutdown.
pub fn spawn_worker(
    context: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    realm_id: RealmId,
    node_id: NodeId,
    disabled_features: BTreeSet<Feature>,
    shutdown: &Shutdown,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let mut busy = false;
            let matcher = MatchEventsOperation::new(realm_id)
                .with_disabled_features(disabled_features.clone());
            match drive(matcher, context.as_ref()).await {
                Ok(report) => {
                    if report.scanned > report.deferred || report.fired > 0 || report.skipped > 0 {
                        busy = true;
                        debug!(?report, "Matched automation journal events");
                    }
                }
                Err(error) => warn!(%error, "Automation event matching failed"),
            }
            let timers = TickTimersOperation::new(realm_id)
                .with_disabled_features(disabled_features.clone());
            match drive(timers, context.as_ref()).await {
                Ok(report) => {
                    if report.fired > 0 || report.skipped > 0 {
                        busy = true;
                        debug!(?report, "Processed scheduled automation triggers");
                    }
                }
                Err(error) => warn!(%error, "Automation schedule tick failed"),
            }
            let stepper = StepFiringsOperation::new((node_id, realm_id))
                .with_disabled_features(disabled_features.clone());
            match drive(stepper, context.as_ref()).await {
                Ok(report) => {
                    for job_id in &report.cancel_jobs {
                        jobs_runtime.request_cancel(*job_id);
                    }
                    if let Some(registry) = context.compute_handle.as_ref() {
                        for run_id in &report.wake_runs {
                            registry.wake_run(run_id.to_string());
                        }
                    }
                    if !report.cancel_acks.is_empty()
                        && let Err(error) = drive(
                            AckCancelsOperation::new(report.cancel_acks.clone()),
                            context.as_ref(),
                        )
                        .await
                    {
                        warn!(%error, "Automation cancel acknowledgement failed");
                    }
                    if report.advanced > 0 || report.finished > 0 {
                        busy = true;
                        debug!(?report, "Advanced active automation firings");
                    }
                }
                Err(error) => warn!(%error, "Automation firing step failed"),
            }
            let wait = if busy { WORKER_BUSY } else { WORKER_IDLE };
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    });
}
