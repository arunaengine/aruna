//! Counters of what visitors and guests did with an applet, per hour and per day.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::applet::record::Applet;
use crate::types::Key;

const USAGE_ROW: u8 = 3;

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

    /// The number of the window `now_ms` falls into.
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

/// What one visitor may do per hour and per day, and the whole applet per day.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Caps {
    pub hourly: Option<u32>,
    pub daily: Option<u32>,
    pub applet_daily: Option<u32>,
}

/// The caps of a signed-in visitor, or of a guest under the applet's guest policy.
pub fn caps(applet: &Applet, guest: bool) -> Caps {
    let limits = &applet.grants.limits;
    match (guest, &applet.guests) {
        (true, Some(policy)) => Caps {
            hourly: Some(policy.hourly),
            daily: Some(policy.daily),
            applet_daily: limits.applet_daily,
        },
        // A guest without a policy may do nothing.
        (true, None) => Caps {
            hourly: Some(0),
            daily: Some(0),
            applet_daily: Some(0),
        },
        (false, _) => Caps {
            hourly: limits.visitor_hourly,
            daily: limits.visitor_daily,
            applet_daily: limits.applet_daily,
        },
    }
}

/// The counter row of `visitor` (a pseudonym; `None` counts the whole applet) in one window.
pub fn usage_key(applet_id: Ulid, (window, start): (Window, u32), visitor: Option<&str>) -> Key {
    let mut row = usage_prefix(applet_id).to_vec();
    row.push(window.tag());
    row.extend_from_slice(&start.to_be_bytes());
    row.extend_from_slice(visitor.unwrap_or_default().as_bytes());
    Key::from(row)
}

/// The prefix of every counter row of `applet_id`.
pub fn usage_prefix(applet_id: Ulid) -> Key {
    let mut row = vec![USAGE_ROW];
    row.extend_from_slice(&applet_id.to_bytes());
    Key::from(row)
}

/// Whether a counter row belongs to a window that ended before `now_ms`.
pub fn usage_ended(key: &[u8], now_ms: u64) -> bool {
    let (Some(&tag), Some(start)) = (key.get(17), key.get(18..22)) else {
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

/// The count a counter row holds; a missing row counts nothing.
pub fn usage_count(value: Option<&[u8]>) -> u32 {
    value
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map_or(0, u32::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_end_ordered() {
        let applet = Ulid::from_bytes([1; 16]);
        let now = 2 * Window::Day.span_ms() + 5;
        let hour = usage_key(applet, (Window::Hour, Window::Hour.start(now)), Some("ab"));
        let day = usage_key(applet, (Window::Day, Window::Day.start(now)), None);
        assert!(hour.starts_with(usage_prefix(applet)));
        assert!(!usage_ended(&hour, now));
        assert!(usage_ended(&hour, now + Window::Hour.span_ms()));
        assert!(!usage_ended(&day, now + Window::Hour.span_ms()));
        assert!(usage_ended(&day, now + Window::Day.span_ms()));
        assert_eq!(usage_count(Some(&7u32.to_be_bytes())), 7);
        assert_eq!(usage_count(None), 0);
    }
}
