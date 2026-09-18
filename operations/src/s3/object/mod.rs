//! Groups the S3 object operations: get, put, head, list, copy, delete, search and versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod attributes;
pub mod copy;
pub mod delete;
pub mod get;
pub mod head;
pub mod list;
mod lookup;
pub mod metadata;
pub mod placement;
pub mod put;
pub mod search;
pub mod versions;
