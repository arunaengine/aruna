//! Shared per-peer and per-principal rate limits, slot permits and live credential admission
//! across HTTP, S3, SMB and NFS frontends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod limits;
mod slots;
#[cfg(test)]
mod tests;

pub use limits::{RateLimits, VfsLimits};
pub use slots::{LocalKey, LocalLease, LocalPermit, SlotPermit};
