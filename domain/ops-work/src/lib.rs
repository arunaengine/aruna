//! Domain state machines; the drivers crate runs them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Large operation types and trait bounds exceed the default recursion limit on fresh builds.
#![recursion_limit = "512"]
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod applets;
pub mod assistant;
pub mod automations;
pub mod federation;
pub mod harvest;
pub mod jobs;
pub mod node;
pub mod notifications;
pub mod plugins;
pub mod relay;
pub mod triggers;
pub mod workflows;

#[cfg(any(test, feature = "test-hooks"))]
pub mod tests;

/// Formats a millisecond UNIX timestamp as an RFC 3339 string.
pub(crate) fn rfc3339(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| ms.to_string())
}
