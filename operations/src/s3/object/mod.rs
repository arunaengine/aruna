//! Groups the S3 object operations: get, put, head, list, copy, delete, search and versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod attributes;
pub mod delete;
pub mod get;
pub mod head;
pub mod head_source;
pub mod link;
pub mod list;
pub(crate) mod lookup;
#[cfg(test)]
#[path = "managed_copy_tests.rs"]
mod managed_copy_tests;
pub mod member_merge;
pub mod member_read;
pub mod member_rest;
pub mod member_write;
pub mod metadata;
pub mod metadata_drain;
pub mod pages;
pub mod placement;
pub mod put;
pub mod range_list;
pub mod read_access;
pub mod reference;
mod refresh_mint;
mod refresh_observe;
pub mod search;
pub mod source;
pub mod versions;
pub mod visible;
