//! Builds the S3, DRS and blob storage tests and their shared fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

mod abe_access;
#[path = "operations/blob_node/mod.rs"]
mod blob_node;
mod compression;
#[path = "operations/convergence/mod.rs"]
mod convergence;
mod drs;
mod elabftw_storage;
#[path = "operations/job_calls.rs"]
mod job_calls;
#[path = "operations/linked_import.rs"]
mod linked_import;
#[path = "operations/multipart.rs"]
mod multipart;
mod public_access;
mod s3_attributes;
mod s3_copy;
mod s3_operations;
mod s3_security;
mod shared;
#[path = "operations/storage_check.rs"]
mod storage_check;
#[path = "operations/storage_move.rs"]
mod storage_move;
#[path = "operations/storage_purge.rs"]
mod storage_purge;
#[path = "operations/usage_deltas.rs"]
mod usage_deltas;
