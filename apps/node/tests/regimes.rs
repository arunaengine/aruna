//! End-to-end node test regimes grouped by subsystem.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

#[path = "operations/blob_node/mod.rs"]
mod blob_node;
#[path = "operations/convergence/mod.rs"]
mod convergence;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/metadata_calls.rs"]
mod metadata_calls;
#[path = "shared/mod.rs"]
mod shared;
#[path = "operations/topology/mod.rs"]
mod topology;
#[path = "operations/user_calls.rs"]
mod user_calls;

mod compute_regime;
mod federation_regime;
mod identity_regime;
mod metadata_regime;
mod ops_regime;
mod restart_regime;
mod storage_regime;

use convergence::fresh_ids;
