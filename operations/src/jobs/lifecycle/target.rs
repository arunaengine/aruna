//! What a launch offer needs to know about this node before its operation decides.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::{ExecutorKind, ResourceEnvelope};
use aruna_core::effects::LaunchFrame;
use aruna_core::structs::execution::job::{JobFamilyRecord, LaunchIntent};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use super::launch_admit::LaunchNode;
use crate::driver::DriverContext;

/// This node as an offered launch sees it; `None` without a network, which leaves the offer
/// undecided.
pub(crate) fn launch_node(context: &DriverContext, launch: &LaunchFrame) -> Option<LaunchNode> {
    let (local, realm_id) = context.node_and_realm()?;
    let limits = match &launch.envelope().record {
        JobFamilyRecord::Launch(intent) => backend_limits(context, intent),
        _ => None,
    };
    Some(LaunchNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        execution_id: Ulid::generate(),
        limits,
    })
}

/// This node's own advertisement for the offered executor kind.
fn backend_limits(context: &DriverContext, intent: &LaunchIntent) -> Option<ResourceEnvelope> {
    let registry: &ExecutorRegistry = context.compute_handle.as_ref()?;
    let backend = registry.get(&ExecutorKind::from_wire(&intent.target.executor_kind))?;
    Some(backend.capabilities().limits)
}
