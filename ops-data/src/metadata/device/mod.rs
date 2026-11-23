//! Keeps metadata on devices: the replicas a device refreshes and edits offline, the drain that
//! publishes its queued intents, and the realm-node side that serves its documents and edits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod drain;
pub mod edit;
pub mod install_graph;
pub mod refresh;
pub mod selection;
pub mod serve;
