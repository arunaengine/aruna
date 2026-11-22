//! Runs operations: the drive loop, the effect adapters that carry effects to the backends, the
//! run recorder and the runtime of the jobs on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Deep effect adapter dispatch chains exceed the default recursion limit on fresh builds.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod clock;
pub mod driver;
pub mod effect_adapters;
pub mod jobs;
pub mod position;
pub mod recorder;
#[cfg(any(test, feature = "test-hooks"))]
pub mod testing;
