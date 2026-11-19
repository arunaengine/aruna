//! Reads what the job operations need to know about this node from the driver context.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_compute::ExecutorRegistry;
use aruna_core::compute::{ExecutorKind, ResourceEnvelope};
use aruna_core::effects::LaunchFrame;
use aruna_core::structs::execution::job::{JobFamilyRecord, LaunchIntent};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::jobs::lifecycle::launch_admit::LaunchNode;
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::lifecycle::witness::WitnessNode;
use crate::jobs::local_run::{LocalExecutionConfig, LocalRunOperation};

/// This node as the submission operations see it; `None` without a network.
pub fn ingress_node(context: &DriverContext) -> Option<IngressNode> {
    let (local, realm_id) = context.node_and_realm()?;
    Some(IngressNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        fresh: context.fresh_heartbeats(),
    })
}

/// This node as an offered launch sees it; `None` without a network, which leaves the offer
/// undecided.
pub fn launch_node(context: &DriverContext, launch: &LaunchFrame) -> Option<LaunchNode> {
    let (local, realm_id) = context.node_and_realm()?;
    let limits = match &launch.envelope().record {
        JobFamilyRecord::Launch(intent) => offered_limits(context, intent),
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
fn offered_limits(context: &DriverContext, intent: &LaunchIntent) -> Option<ResourceEnvelope> {
    let registry: &ExecutorRegistry = context.compute_handle.as_ref()?;
    let backend = registry.get(&ExecutorKind::from_wire(&intent.target.executor_kind))?;
    Some(backend.capabilities().limits)
}

/// This node as witness rounds see it; `None` without a network.
pub fn witness_node(context: &DriverContext) -> Option<WitnessNode> {
    let (local, realm_id) = context.node_and_realm()?;
    let registry: Option<&ExecutorRegistry> = context.compute_handle.as_deref();
    Some(WitnessNode {
        local,
        realm_id,
        fresh: context.fresh_heartbeats(),
        limits: registry.map(ExecutorRegistry::limits).unwrap_or_default(),
    })
}

/// A run on this node: the ceilings of the backend it would select, and the realm it serves.
pub fn local_run(config: LocalExecutionConfig, context: &DriverContext) -> LocalRunOperation {
    let limits = context.compute_handle.as_ref().and_then(|registry| {
        let constraint = config.spec.executor_constraint.as_deref();
        let constraint = constraint.map(ExecutorKind::from_wire);
        let backend = registry.select(constraint.as_ref())?;
        Some(backend.capabilities().limits)
    });
    LocalRunOperation::new(config, limits, context.node_and_realm())
}
