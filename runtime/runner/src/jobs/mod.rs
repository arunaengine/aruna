//! Runs the jobs of this node: the context of one run, the registry of running jobs and the
//! turns of distribution ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod context;
pub mod executor;
pub mod runtime;

/// Maximum in-process jobs executing locally at once; excess stays queued.
pub const JOB_CONCURRENCY_CAP: usize = 8;
/// External-attempt supervision slots. Supervising a remote container is IO-trivial,
/// so it gets its own bounded budget and never starves in-process jobs (or vice versa).
pub const EXTERNAL_CONCURRENCY_CAP: usize = 64;
