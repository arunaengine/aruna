//! Hourly and daily firing counters and daily copy byte counters for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::keys::USAGE_ROW;
use crate::types::Key;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Window {
    Hour,
    Day,
}

impl Window {
    pub fn span_ms(self) -> u64 {
        match self {
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
        }
    }

    pub fn start(self, now_ms: u64) -> u32 {
        u32::try_from(now_ms / self.span_ms()).unwrap_or(u32::MAX)
    }

    fn tag(self) -> u8 {
        match self {
            Self::Hour => 0,
            Self::Day => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterKind {
    Firings,
    CopyBytes,
}

impl CounterKind {
    fn tag(self) -> u8 {
        match self {
            Self::Firings => 0,
            Self::CopyBytes => 1,
        }
    }
}

pub fn counter_prefix(automation_id: Ulid) -> Key {
    let mut row = Vec::with_capacity(17);
    row.push(USAGE_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    Key::from(row)
}

pub fn counter_key(automation_id: Ulid, kind: CounterKind, (window, start): (Window, u32)) -> Key {
    let mut row = Vec::with_capacity(23);
    row.push(USAGE_ROW);
    row.extend_from_slice(&automation_id.to_bytes());
    row.push(kind.tag());
    row.push(window.tag());
    row.extend_from_slice(&start.to_be_bytes());
    Key::from(row)
}

pub fn counter_ended(key: &[u8], now_ms: u64) -> bool {
    let (Some(&tag), Some(start)) = (key.get(18), key.get(19..23)) else {
        return false;
    };
    let window = match tag {
        0 => Window::Hour,
        _ => Window::Day,
    };
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(start);
    u32::from_be_bytes(bytes) < window.start(now_ms)
}

pub fn count_u64(value: Option<&[u8]>) -> u64 {
    value
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map_or(0, u64::from_be_bytes)
}
