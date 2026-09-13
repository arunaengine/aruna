//! Local, read-only inspection of a stopped node's persisted state.
//!
//! Three responsibilities, in order: persisted decoding ([`decode`]: one
//! decoder per keyspace record), the doctor commands that read and summarize
//! the keyspaces ([`commands`]), and presentation ([`present`]: the JSON output
//! records).
//!
//! Output policy: the doctor is a local operator tool. `node-state` prints the
//! persisted identity record including its network secret by design, because
//! recovering a node may require it. Fixtures use synthetic values only. Any
//! redaction or reveal-flag change is a CLI contract decision, not an
//! incidental extraction change.

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
