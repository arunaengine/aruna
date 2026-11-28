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
pub const SCHEDULE_DUE_ROW: u8 = 7;
pub const SCHEDULE_CURSOR_ROW: u8 = 8;

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

/// Prefix of every due-time-ordered scheduled automation index row in `AUTOMATIONS_KEYSPACE`.
pub fn schedule_due_prefix() -> Key {
    Key::from(vec![SCHEDULE_DUE_ROW])
}

/// Due-time-ordered index row `[SCHEDULE_DUE_ROW, due_ms_be8, automation_id_16]` for one automation.
pub fn schedule_due_key(due_ms: u64, automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(25);
    row.push(SCHEDULE_DUE_ROW);
    row.extend_from_slice(&due_ms.to_be_bytes());
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

/// Parses a `SCHEDULE_DUE_ROW` key into `(due_ms, automation_id)`.
pub fn parse_due_key(key: &[u8]) -> Option<(u64, Ulid)> {
    if key.len() != 25 || key.first().copied() != Some(SCHEDULE_DUE_ROW) {
        return None;
    }
    let due_bytes: [u8; 8] = key.get(1..9)?.try_into().ok()?;
    let id_bytes: [u8; 16] = key.get(9..25)?.try_into().ok()?;
    Some((u64::from_be_bytes(due_bytes), Ulid::from_bytes(id_bytes)))
}

/// Per-automation schedule cursor storing its current due timestamp and per-schedule anchors.
pub fn schedule_cursor_key(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(SCHEDULE_CURSOR_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
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
