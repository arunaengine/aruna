//! Groups the bucket operations: create, read, list, search, delete and bucket settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod audit_retry;
pub mod compression;
pub mod cors;
pub mod create;
pub mod delete;
pub mod encryption;
pub mod forward;
pub mod get;
pub mod holders;
pub mod key;
pub mod list;
pub mod placement;
pub mod rotate;
pub mod routing;
pub mod seal_missing;
pub mod search;
pub mod token_admit;
pub mod token_list;
pub mod usage;

pub use key::{
    extend as key_extend, grant as key_grant, install as key_install, lock as key_lock,
    recovery as key_recovery, removal as key_removal, restart as key_restart, rows as key_rows,
    startup as key_startup, unlock as key_unlock,
};
