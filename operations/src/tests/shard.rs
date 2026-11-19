//! Builds the realm settings the shard restore tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};

pub fn flat_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    for node_id in nodes {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}
