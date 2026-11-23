//! Builds the compute job and workflow tests and their shared fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod compute_execution;
#[path = "operations/convergence/mod.rs"]
mod convergence;
#[path = "operations/copy_jobs.rs"]
mod copy_jobs;
mod job_api;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/lane_jobs.rs"]
mod lane_jobs;
mod mcp_artifacts;
#[path = "operations/output_versioning.rs"]
mod output_versioning;
#[path = "operations/plugin_tasks.rs"]
mod plugin_tasks;
#[path = "operations/run_publish.rs"]
mod run_publish;
mod shared;
#[path = "operations/table_jobs.rs"]
mod table_jobs;
#[path = "operations/topology/mod.rs"]
mod topology;
