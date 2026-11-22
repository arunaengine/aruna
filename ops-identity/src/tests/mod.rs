//! Groups the shared test helpers of the identity and realm operations crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod calls;
#[cfg(test)]
pub(crate) mod groups;
pub mod locations;
pub mod peers;
pub mod policy;
#[cfg(test)]
pub(crate) mod realm;
pub mod shard;
