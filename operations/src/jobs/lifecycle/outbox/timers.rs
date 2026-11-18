//! Arms the outbox and witness drain timers: on demand after a queued record, and after a restart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::events::Event;
use aruna_core::handle::Handle;
use tracing::warn;

use super::{OUTBOX_RETRY_AFTER, schedule_outbox_drain};
use crate::driver::DriverContext;

/// Asks the drain to run now, after a record was queued for replication.
pub async fn kick(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref() {
        let _ = task
            .send_effect(schedule_outbox_drain(Duration::ZERO))
            .await;
    }
}

/// Re-arms the replication and witness queues after a restart. Both queues are
/// durable rows, so the timers only have to be brought back, never the work.
pub async fn restore_lifecycle_timers(
    context: &DriverContext,
    task_handle: &aruna_tasks::TaskHandle,
) {
    let pending = crate::storage_read::iter_prefix_page(
        &context.storage_handle,
        aruna_core::keyspaces::WITNESS_DEADLINE_KEYSPACE,
        None,
        None,
        1,
        None,
    )
    .await
    .map(|(rows, _)| !rows.is_empty())
    .unwrap_or(false);
    let mut timers = vec![schedule_outbox_drain(OUTBOX_RETRY_AFTER)];
    if pending {
        timers.push(crate::jobs::lifecycle::witness_arm::schedule_witness_drain(
            Duration::from_secs(1),
        ));
    }
    for timer in timers {
        if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) =
            task_handle.send_effect(timer).await
        {
            warn!(message = %message, "Failed to restore a job lifecycle timer");
        }
    }
}
