//! Storage row keys for automations, cursors, kept events, dedup entries and firings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::types::Key;

/// Row tags in `AUTOMATIONS_KEYSPACE`.
pub const RECORD_ROW: u8 = 1;
pub const CURSOR_ROW: u8 = 2;
pub const KEPT_ROW: u8 = 3;
pub const SEEN_ROW: u8 = 4;
pub const USAGE_ROW: u8 = 5;
pub const REPLAY_ROW: u8 = 6;

/// Row tags in `AUTOMATION_RUNS_KEYSPACE`.
pub const FIRING_ROW: u8 = 1;
pub const ACTIVE_ROW: u8 = 2;
pub const INDEX_ROW: u8 = 3;
pub const COPY_PLAN_ROW: u8 = 4;

pub fn record_prefix() -> Key {
    Key::from(vec![RECORD_ROW])
}

pub fn record_key(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(RECORD_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

/// Single cursor key tracking the last scanned `JOURNAL_ROW` key for automations.
pub fn cursor_key() -> Key {
    Key::from(vec![CURSOR_ROW])
}

pub fn kept_prefix(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(KEPT_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

pub fn kept_key(automation_id: Ulid, event_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(33);
    row.push(KEPT_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    row.extend_from_slice(&event_id.to_bytes());
    Key::from(row)
}

/// Maps a replayed kept `(automation_id, event_id)` to its created `firing_id` for idempotency.
pub fn replay_key(automation_id: Ulid, event_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(33);
    row.push(REPLAY_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    row.extend_from_slice(&event_id.to_bytes());
    Key::from(row)
}

pub fn seen_prefix(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(SEEN_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

pub fn seen_key(automation_id: Ulid, source: &str) -> Key {
    let hash = blake3::hash(source.as_bytes());
    let mut row = Vec::with_capacity(49);
    row.push(SEEN_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    row.extend_from_slice(hash.as_bytes());
    Key::from(row)
}

pub fn firing_prefix(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(FIRING_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

pub fn firing_key(automation_id: Ulid, firing_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(33);
    row.push(FIRING_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    row.extend_from_slice(&firing_id.to_bytes());
    Key::from(row)
}

pub fn active_prefix() -> Key {
    Key::from(vec![ACTIVE_ROW])
}

pub fn active_key(firing_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(ACTIVE_ROW);
    row.extend_from_slice(&firing_id.to_bytes());
    Key::from(row)
}

pub fn index_key(firing_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(INDEX_ROW);
    row.extend_from_slice(&firing_id.to_bytes());
    Key::from(row)
}

/// Companion plan row for an in-flight copy intent, keyed by `(stage_id, ordinal)`.
pub fn copy_plan_key(stage_id: Ulid, ordinal: u32) -> Key {
    let mut row = Vec::with_capacity(21);
    row.push(COPY_PLAN_ROW);
    row.extend_from_slice(&stage_id.to_bytes());
    row.extend_from_slice(&ordinal.to_be_bytes());
    Key::from(row)
}
