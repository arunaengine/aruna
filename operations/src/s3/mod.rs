//! Groups the S3 surface: buckets, objects, multipart uploads, access keys and policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod access;
pub mod bucket;
pub mod listing;
pub mod multipart;
pub mod object;
pub mod policy;
pub mod purge_fence;
pub mod session;
mod write_cleanup;
