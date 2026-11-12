//! Groups the operations that create, read, list, revoke and sweep S3 access credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod create;
pub mod cutoff;
pub mod get;
pub(crate) mod index;
pub mod list;
pub mod revoke;
