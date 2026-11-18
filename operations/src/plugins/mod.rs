//! Plugin use cases: publishing artifacts to the realm and keeping them on management nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod account;
pub mod artifacts;
pub(crate) mod attempt;
pub mod bound;
pub mod check;
pub mod cleanup;
pub mod clock;
pub mod copies;
pub mod copy_rows;
pub mod creates;
pub mod dataset;
pub mod decide;
pub mod dispatch;
pub mod entry;
pub mod finished;
pub mod forward;
pub mod grants;
pub mod held;
pub mod hooks;
pub(crate) mod host_calls;
pub mod install;
pub mod intents;
pub(crate) mod job_work;
pub mod journal;
pub mod lifecycle;
pub mod marks;

pub mod matcher;
pub mod objects;
pub mod outgoing;
pub(crate) mod peer;
pub mod prune;
pub mod publish;
pub mod rate;
pub mod receiver;
pub(crate) mod reconcile;
pub mod records;
pub mod replay;
pub mod rows;
pub mod rules;
pub mod runs;
pub(crate) mod secret_use;
pub mod settings;
pub mod state;
pub mod submit;
pub mod sweep;
pub(crate) mod transfer_steps;
pub mod waiting;
pub mod work;
pub(crate) mod work_calls;

use aruna_plugins::broker::{ErrorKind, HostError};

#[cfg(test)]
#[path = "calls_tests.rs"]
pub(crate) mod calls;
#[cfg(test)]
#[path = "fixtures_tests.rs"]
pub(crate) mod fixtures;
#[cfg(test)]
#[path = "store_tests.rs"]
pub(crate) mod store;

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod delivery_tests;
#[cfg(test)]
#[path = "events_tests.rs"]
mod events_tests;

/// A host call that cannot be answered now; the guest may try again.
pub(crate) fn unavailable(message: impl Into<String>) -> HostError {
    HostError::new(ErrorKind::Unavailable, message)
}

/// Management nodes that must hold a copy before a publish is answered: a majority, and at
/// least two once the realm has two. Plain copies with acknowledgements, no vote.
pub(crate) fn copies_needed(managers: usize) -> usize {
    match managers {
        0 | 1 => 1,
        count => count / 2 + 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::plugins::ArtifactBytes;

    #[test]
    fn majority_copies() {
        let needed: Vec<usize> = (0..=6).map(copies_needed).collect();
        assert_eq!(needed, [1, 1, 2, 2, 3, 3, 4]);
        let debug = format!("{:?}", ArtifactBytes(vec![0; 3]));
        assert_eq!(debug, "ArtifactBytes(3 bytes)");
    }
}
