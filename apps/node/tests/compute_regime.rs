//! Groups the compute job and workflow tests under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "compute_execution.rs"]
mod compute_execution;
#[path = "operations/copy_jobs.rs"]
mod copy_jobs;
#[path = "job_api.rs"]
mod job_api;
#[path = "operations/lane_jobs.rs"]
mod lane_jobs;
#[path = "mcp_artifacts.rs"]
mod mcp_artifacts;
#[path = "operations/output_versioning.rs"]
mod output_versioning;
#[path = "operations/plugin_tasks.rs"]
mod plugin_tasks;
#[path = "operations/run_publish.rs"]
mod run_publish;
#[path = "operations/table_jobs.rs"]
mod table_jobs;
#[path = "operations/workflow_runs.rs"]
mod workflow_runs;
