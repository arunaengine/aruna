//! Storage, blob, replication, transfer, metadata, git, connector and device state machines.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Large operation types and trait bounds exceed the default recursion limit on fresh builds.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod blob;
pub mod connectors;
pub mod device;
pub mod distribution;
pub mod git;
pub mod holder_choice;
pub mod metadata;
pub mod replication;
pub mod s3;
pub mod shares;
pub mod staging;
#[cfg(test)]
pub(crate) use aruna_ops_identity::stepper;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "test_objects.rs"]
pub mod testing_objects;
#[cfg(test)]
pub(crate) mod tests;
pub mod timer;
pub mod transfer;
pub mod usage_publish;
pub mod vfs;
