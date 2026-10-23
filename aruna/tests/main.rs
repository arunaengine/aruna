//! Builds the node integration test files and their shared harness into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod abe_access;
mod compression;
mod compute_execution;
mod cors;
mod credentials;
mod drs;
mod git_native;
mod groups;
mod job_api;
mod mcp_artifacts;
mod network_recovery;
mod observability;
mod oidc_registration;
mod onboarding;
mod public_access;
mod replication;
mod s3_attributes;
mod s3_copy;
mod s3_operations;
mod s3_security;
mod self_service;
mod shared;
