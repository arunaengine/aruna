//! Groups the S3 surface: buckets, objects, multipart uploads, access keys and policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod access;
pub mod bucket;
pub mod holder_seal;
pub mod key_status;
pub mod listing;
pub mod multipart;
pub mod object;
pub mod policy;
pub mod purge_fence;
pub mod restart_notice;
pub mod session;
pub mod unlock_limit;
mod write_cleanup;
