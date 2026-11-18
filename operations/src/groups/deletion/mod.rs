//! Coordinates empty group deletion and commits durable per-node decisions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod access;
mod check;
mod commit;
mod coordinator;
mod onboarding;
mod peer;
mod prepare;
mod resources;

pub use check::{CheckGroupOperation, GroupBlockers};
pub(crate) use coordinator::SyncDecisionOperation;
pub use coordinator::{DeleteGroupOperation, InstallOnboardingOperation};
pub(crate) use peer::ApplyRequestOperation;

#[cfg(test)]
mod barrier_tests;
#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;
