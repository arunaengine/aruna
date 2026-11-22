//! Delivers a distributed bucket's reference records to a member in bounded pages, applied in
//! one transaction on the member and acknowledged only after that commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod apply;
pub mod build;
pub mod deliver;
mod plan;

/// Records one page carries at most.
pub const PAGE_RECORDS: usize = 8192;
/// Encoded bytes one page carries at most, below the 16 MiB metadata message limit.
pub const PAGE_BYTES: usize = 12 * 1024 * 1024;
