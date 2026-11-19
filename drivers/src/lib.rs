//! Runs operations: the drive loop, the effect adapters that carry effects to the backends, the
//! run recorder and the runtime of the jobs on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh (non-incremental) builds overflow the default query depth in the
// deeply nested async state machines; incremental builds mask it.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod driver;
pub mod effect_adapters;
pub mod jobs;
pub mod recorder;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
