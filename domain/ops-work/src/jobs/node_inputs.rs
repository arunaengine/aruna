//! Builds what the job operations need to know about this node from the facts its caller read:
//! the node and realm it serves, the fresh heartbeats and the limits of its executor backends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::compute::offer::BackendLimits;
use aruna_core::effects::LaunchFrame;
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::family::JobFamilyRecord;
use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

use crate::jobs::lifecycle::launch_admit::LaunchNode;
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::lifecycle::witness::round::WitnessNode;

/// This node as the submission operations see it at `now_ms`; `None` without a network.
pub fn ingress_node(
    serving: Option<(NodeId, RealmId)>,
    fresh: BTreeMap<NodeId, HeldHeartbeat>,
    now_ms: u64,
) -> Option<IngressNode> {
    let (local, realm_id) = serving?;
    Some(IngressNode {
        local,
        realm_id,
        now_ms,
        fresh,
    })
}

/// This node as an offered launch sees it; `None` without a network, which leaves the offer
/// undecided. `limits` holds each local backend's wire kind and limits by its name; the caller
/// mints `execution_id` for the execution the launch would start here.
pub fn launch_node(
    serving: Option<(NodeId, RealmId)>,
    limits: &BackendLimits,
    launch: &LaunchFrame,
    now_ms: u64,
    execution_id: Ulid,
) -> Option<LaunchNode> {
    let (local, realm_id) = serving?;
    let limit = match &launch.envelope().record {
        JobFamilyRecord::Launch(intent) => limits.get(&intent.target.backend),
        _ => None,
    };
    Some(LaunchNode {
        local,
        realm_id,
        now_ms,
        execution_id,
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
