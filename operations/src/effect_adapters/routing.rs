//! Reads this node's in-memory routing from the blob handle, for callers building a routing input.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::routing::NodeRouting;

use crate::driver::DriverContext;

/// Node-local routing inputs for a caller assembling an operation config.
/// Pure in-memory state: operations never fetch this from inside a step.
pub fn node_routing(context: &DriverContext) -> NodeRouting {
    context
        .blob_handle
        .as_ref()
        .map(|handle| handle.routing())
        .unwrap_or_default()
}
