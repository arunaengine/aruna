//! Groups the S3 surface: buckets, objects, multipart uploads, access keys, policies and the
//! backend import rules that fill buckets from group storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod abe_copies;
pub mod access;
pub mod bucket;
pub mod fence_store;
pub mod holder_seal;
pub mod imports;
pub mod listing;
pub mod multipart;
pub mod object;
pub mod policy;
pub mod purge_fence;
pub mod realm_usage;
mod rebuild_counts;
pub mod restart_notice;
mod row_fence;
pub mod row_reads;
pub mod session;
pub mod summary_refresh;
pub mod unlock_limit;
pub mod usage;
pub mod usage_rebuild;
mod write_cleanup;
