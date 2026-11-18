//! What a distribution operation needs to know about this node, read once by its caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::structs::identity::realm::NodeRealm;

use super::status_view::{Reach, heartbeat_reach};
use crate::driver::{DriverContext, node_routing};

/// Who this node hears from: itself, and each node's heartbeat age and wall receive time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Heard {
    pub local: Option<NodeId>,
    pub nodes: BTreeMap<NodeId, (u64, u64)>,
}

impl Heard {
    /// Whether this node hears from `node_id`.
    pub fn reach(&self, node_id: NodeId) -> Reach {
        heartbeat_reach(self.local, self.nodes.clone())(node_id)
    }
}

/// The node an operation runs on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Here {
    pub ids: NodeRealm,
    /// The bytes this node's backends can hold, when all of them state a quota.
    pub capacity: Option<u64>,
    pub heard: Heard,
}

/// Who this node hears from now, from its heartbeats.
pub fn heard(context: &DriverContext) -> Heard {
    let nodes = context
        .held_heartbeats()
        .into_iter()
        .map(|(node_id, held)| (node_id, (held.age_ms, held.received_at_ms)))
        .collect();
    Heard {
        local: context.net_handle.as_ref().map(|net| net.node_id()),
        nodes,
    }
}

/// This node as operations see it.
pub fn here(context: &DriverContext) -> Here {
    Here {
        ids: context.node_and_realm(),
        capacity: node_routing(context).catalog.capacity(),
        heard: heard(context),
    }
}

/// Whether this node hears from `node_id` now, from its heartbeats.
pub fn node_reach(context: &DriverContext, node_id: NodeId) -> Reach {
    heard(context).reach(node_id)
}
