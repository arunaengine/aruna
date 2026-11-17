//! Groups the bucket operations: create, read, list, search, delete and bucket settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod audit_retry;
pub mod checked_rename;
pub mod compression;
pub mod cors;
pub mod create;
pub mod dataset;
pub mod delete;
pub mod encryption;
pub(crate) mod forward;
pub mod get;
pub mod holders;
pub(crate) mod job_pages;
pub(crate) mod job_progress;
pub mod key;
pub mod linked;
pub mod list;
pub(crate) mod local_search;
pub mod metadata;
pub mod placement;
pub mod release;
pub mod remap;
pub mod rename;
pub mod rotate;
pub mod routing;
pub mod seal_missing;
pub(crate) mod search;
pub mod sources;
pub mod storage_check;
pub mod storage_move;
pub mod token_list;
