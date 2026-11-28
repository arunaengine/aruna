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

use aruna_core::automation::keys::record_key as automation_record_key;
use aruna_core::automation::record::Automation;
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::AUTOMATIONS_KEYSPACE;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;

/// Formats a millisecond UNIX timestamp as an RFC 3339 string.
pub(crate) fn rfc3339(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| ms.to_string())
}

/// Encodes an automation record row for `AUTOMATIONS_KEYSPACE`.
pub(crate) fn automation_row(
    automation: &Automation,
) -> Result<(KeySpace, Key, Value), ConversionError> {
    Ok((
        AUTOMATIONS_KEYSPACE.to_string(),
        automation_record_key(automation.automation_id),
        ByteView::from(automation.to_bytes()?),
    ))
}
