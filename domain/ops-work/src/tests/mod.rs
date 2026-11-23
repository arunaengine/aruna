//! Groups the shared test helper modules of the operations tests. The pure location, notification
//! and shard fixtures are public under `test-hooks` for the node crate's tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
pub(crate) use aruna_ops_identity::tests::{calls, peers};
pub use aruna_ops_identity::tests::{locations, shard};

#[cfg(test)]
pub(crate) mod export;
pub mod notifications;
#[cfg(test)]
pub(crate) mod records;
