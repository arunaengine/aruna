//! Wires the crate's shared test fixture modules for effects, reducer and scheduling.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "tests_effects.rs"]
pub(crate) mod effects;
#[path = "tests_reducer.rs"]
pub(crate) mod reducer;
#[path = "tests_scheduling.rs"]
pub(crate) mod scheduling;
