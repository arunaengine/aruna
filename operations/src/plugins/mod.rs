//! Plugin use cases: publishing artifacts to the realm and keeping them on management nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod account;
pub mod artifacts;
pub mod broker;
pub mod check;
pub mod cleanup;
pub mod clock;
pub mod copies;
pub mod decide;
pub mod dispatch;
pub mod forward;
pub mod grants;
pub mod install;
pub mod intents;
pub mod journal;
pub mod lifecycle;
pub mod matcher;
pub mod publish;
pub mod rate;
pub mod receiver;
pub mod records;
pub mod replay;
pub mod runs;
pub mod settings;
pub mod state;
pub mod store;
pub mod sweep;
pub mod waiting;
pub mod worker;

#[cfg(test)]
#[path = "events_tests.rs"]
mod events_tests;

use serde::{Deserialize, Serialize};

/// Artifact bytes in a node message; Debug prints only their length.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactBytes(pub Vec<u8>);

impl std::fmt::Debug for ArtifactBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArtifactBytes({} bytes)", self.0.len())
    }
}

/// Management nodes that must hold a copy before a publish is answered: a majority, and at
/// least two once the realm has two. Plain copies with acknowledgements, no vote.
pub fn copies_needed(managers: usize) -> usize {
    match managers {
        0 | 1 => 1,
        count => count / 2 + 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majority_copies() {
        let needed: Vec<usize> = (0..=6).map(copies_needed).collect();
        assert_eq!(needed, [1, 1, 2, 2, 3, 3, 4]);
        let debug = format!("{:?}", ArtifactBytes(vec![0; 3]));
        assert_eq!(debug, "ArtifactBytes(3 bytes)");
    }
}
