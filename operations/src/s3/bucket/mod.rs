//! Groups the bucket operations: create, read, list, search, delete and bucket settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod compression;
pub mod cors;
pub mod create;
pub mod delete;
pub mod encryption;
pub mod forward;
pub mod get;
pub mod holders;
pub mod key_extend;
pub mod key_grant;
pub mod key_install;
pub mod key_lock;
pub mod key_recovery;
pub mod key_removal;
pub mod key_restart;
pub mod key_rows;
pub mod key_startup;
pub mod key_unlock;
pub mod list;
pub mod placement;
pub mod rotate;
pub mod routing;
pub mod seal_missing;
pub mod search;
pub mod usage;
