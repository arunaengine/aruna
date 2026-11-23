//! Groups the blob modules for records, copies, cleanup, reclaim, renames and holder refresh.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod cleanup;
pub mod cleanup_timer;
pub mod hidden;
pub mod holders;
pub mod import_rows;
pub mod merge_copies;
pub mod migration;
pub mod pause;
pub mod pending_reclaim;
pub mod promote;
pub mod purge;
pub mod reclaim;
pub mod reclaim_sweep;
pub mod release;
pub mod request;
pub mod sweep_timer;
pub mod trail;
pub mod unify;
pub mod unlocked;

#[cfg(test)]
#[path = "paths_tests.rs"]
mod paths_tests;
