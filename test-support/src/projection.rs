//! Runs the metadata projection for tests: the event log replay, logged events and the drain wake.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::metadata::projection::{
    MetadataProjectionError, ProjectLoggedOperation, ProjectionWakeOperation, ReplayPageOperation,
};
use ulid::Ulid;

/// Replays the whole event log into the registry projection.
pub async fn replay_event_log(context: &DriverContext) -> Result<usize, MetadataProjectionError> {
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let mut after = None;
    let mut projected = 0usize;
    loop {
        let page = ReplayPageOperation::new(after, local_node);
        let (count, next) = drive(page, context).await?;
        projected = projected.saturating_add(count);
        match next {
            Some(next) => after = Some(next),
            None => return Ok(projected),
        }
    }
}

/// Projects logged events by their document and event ids.
pub async fn project_logged_events(
    context: &DriverContext,
    targets: impl IntoIterator<Item = (Ulid, Ulid)>,
) -> Result<usize, MetadataProjectionError> {
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let project = ProjectLoggedOperation::new(targets, false, local_node);
    Ok(drive(project, context).await?.projected)
}

/// Projects one logged event.
pub async fn project_logged_event(
    context: &DriverContext,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<(), MetadataProjectionError> {
    project_logged_events(context, [(document_id, event_id)])
        .await
        .map(|_| ())
}

/// Wakes the projection drain after `after`.
pub async fn schedule_projection_drain(
    context: &DriverContext,
    after: Duration,
) -> Result<(), MetadataProjectionError> {
    drive(ProjectionWakeOperation::new(after), context).await
}
