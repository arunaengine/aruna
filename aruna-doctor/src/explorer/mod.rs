//! Owns the read-only inspection of a stopped node's persisted state.
//! Note that node-state prints the persisted identity including its network secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod commands;
mod decode;
mod present;

pub use commands::{
    explore_entries, explore_keyspaces, print_node_state, print_topic_placements,
    print_topic_status, print_topics_list, scan_locations,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum ExplorerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error("keyspace not found: {0}")]
    KeyspaceNotFound(String),
    #[error("decode failed: {0}")]
    Decode(String),
}
