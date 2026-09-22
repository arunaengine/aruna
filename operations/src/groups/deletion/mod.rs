//! Coordinates empty group deletion and commits durable per-node decisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod commit;
mod coordinator;
mod onboarding;
mod peer;
mod prepare;
mod resources;

pub use coordinator::delete_group;
pub(crate) use coordinator::sync_decision;
pub use onboarding::install_onboarding;
pub(crate) use peer::apply_request;

#[cfg(test)]
mod barrier_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;
