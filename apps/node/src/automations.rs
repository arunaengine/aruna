//! Background worker ticking automation event matching and DAG firing progress until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::id::NodeId;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmId;
use aruna_ops_work::automations::matcher::MatchEventsOperation;
use aruna_ops_work::automations::runner::StepFiringsOperation;
use aruna_runner::driver::{DriverContext, drive};
use aruna_scheduler::shutdown::Shutdown;
use tracing::{debug, warn};

const WORKER_IDLE: Duration = Duration::from_secs(2);
const WORKER_BUSY: Duration = Duration::from_millis(200);

/// Runs automation journal matching and DAG firing advancement until shutdown.
pub fn spawn_worker(
    context: Arc<DriverContext>,
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
                    if report.scanned > 0 || report.fired > 0 {
                        busy = true;
                        debug!(?report, "Matched automation journal events");
                    }
                }
                Err(error) => warn!(%error, "Automation event matching failed"),
            }
            let stepper = StepFiringsOperation::new((node_id, realm_id))
                .with_disabled_features(disabled_features.clone());
            match drive(stepper, context.as_ref()).await {
                Ok(report) => {
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
