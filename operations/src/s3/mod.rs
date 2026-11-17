//! Groups the S3 surface: buckets, objects, multipart uploads, access keys, policies and the
//! backend import rules that fill buckets from group storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod abe_copies;
pub mod abe_rekey;
pub mod access;
pub mod bucket;
pub mod fence_store;
pub mod holder_seal;
pub mod imports;
pub mod key_status;
pub mod listing;
pub mod multipart;
pub mod object;
pub mod policy;
pub mod purge_fence;
pub mod restart_notice;
mod row_fence;
pub mod session;
pub mod unlock_limit;
mod write_cleanup;
