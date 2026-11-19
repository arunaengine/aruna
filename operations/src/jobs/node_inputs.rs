//! Builds what the job operations need to know about this node from the facts its caller read:
//! the node and realm it serves, the fresh heartbeats and the limits of its executor backends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::offer::{BackendLimit, BackendLimits};
use aruna_core::effects::LaunchFrame;
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::structs::execution::job::JobFamilyRecord;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use crate::jobs::lifecycle::launch_admit::LaunchNode;
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::lifecycle::witness::WitnessNode;
use crate::jobs::local_run::{LocalExecutionConfig, LocalRunOperation};

/// This node as the submission operations see it; `None` without a network.
pub fn ingress_node(
    serving: Option<(NodeId, RealmId)>,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
) -> Option<IngressNode> {
    let (local, realm_id) = serving?;
    Some(IngressNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        fresh,
    })
}

/// This node as an offered launch sees it; `None` without a network, which leaves the offer
/// undecided. `limits` holds each local backend's wire kind and limits by its name.
pub fn launch_node(
    serving: Option<(NodeId, RealmId)>,
    limits: &BackendLimits,
    launch: &LaunchFrame,
) -> Option<LaunchNode> {
    let (local, realm_id) = serving?;
    let limit = match &launch.envelope().record {
        JobFamilyRecord::Launch(intent) => limits.get(&intent.target.backend),
        _ => None,
    };
    Some(LaunchNode {
        local,
        realm_id,
        now_ms: unix_timestamp_millis(),
        execution_id: Ulid::generate(),
        limits: limit.map(|limit| limit.limits),
        config_digest: limit.map(|limit| limit.config_digest),
    })
}

/// This node as witness rounds see it; `None` without a network.
pub fn witness_node(
    serving: Option<(NodeId, RealmId)>,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
    limits: BackendLimits,
) -> Option<WitnessNode> {
    let (local, realm_id) = serving?;
    Some(WitnessNode {
        local,
        realm_id,
        fresh,
        limits,
    })
}

/// A run on this node: the limits of the backend it would select (the named one, else the first
/// of the constrained kind, else the first one), and the realm it serves.
pub fn local_run(
    config: LocalExecutionConfig,
    limits: &BackendLimits,
    serving: Option<(NodeId, RealmId)>,
) -> LocalRunOperation {
    let constraint = config.spec.executor_constraint.as_deref();
    let fits = |limit: &&BackendLimit| constraint.is_none_or(|kind| limit.kind == kind);
    let selected = match config.spec.backend.as_deref() {
        Some(name) => limits.get(name).filter(fits),
        None => limits.values().find(fits),
    };
    let selected = selected.map(|limit| limit.limits);
    LocalRunOperation::new(config, selected, serving)
}
