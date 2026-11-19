//! Groups the task and job wiring of the node: the timer handler, the job runner and host,
//! the payload dispatch and the restore of durable timers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod incoming;
pub(crate) mod job_dispatch;
pub mod job_host;
pub mod job_runner;
pub mod metadata_sweeps;
pub mod rocrate_jobs;
pub mod task_persistence;

#[cfg(test)]
mod reconcile_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod workflow_fixtures;
#[cfg(test)]
mod workflow_tests;
