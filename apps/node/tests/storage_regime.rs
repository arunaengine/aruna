//! Groups the S3, DRS and blob storage tests under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "abe_access.rs"]
mod abe_access;
#[path = "compression.rs"]
mod compression;
#[path = "drs.rs"]
mod drs;
#[path = "elabftw_storage.rs"]
mod elabftw_storage;
#[path = "operations/linked_import.rs"]
mod linked_import;
#[path = "operations/multipart.rs"]
mod multipart;
#[path = "public_access.rs"]
mod public_access;
#[path = "s3_attributes.rs"]
mod s3_attributes;
#[path = "s3_copy.rs"]
mod s3_copy;
#[path = "s3_operations.rs"]
mod s3_operations;
#[path = "s3_security.rs"]
mod s3_security;
#[path = "operations/storage_check.rs"]
mod storage_check;
#[path = "operations/storage_move.rs"]
mod storage_move;
#[path = "operations/storage_purge.rs"]
mod storage_purge;
#[path = "operations/usage_deltas.rs"]
mod usage_deltas;
