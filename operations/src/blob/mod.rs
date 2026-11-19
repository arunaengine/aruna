//! Groups the blob modules for records, copies, cleanup, reclaim, renames and holder refresh.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod cleanup;
pub(crate) mod cleanup_rows;
pub mod hidden;
pub mod holders;
pub mod import_rows;
pub(crate) mod merge_copies;
pub mod migration;
pub mod pause;
pub(crate) mod pending_reclaim;
pub(crate) mod promote;
pub mod purge;
pub mod reclaim;
pub mod reclaim_sweep;
pub(crate) mod release;
pub(crate) mod request;
pub mod sweep_timer;
pub(crate) mod trail;
pub mod unify;
pub mod unlocked;

#[cfg(test)]
#[path = "paths_tests.rs"]
mod paths_tests;
