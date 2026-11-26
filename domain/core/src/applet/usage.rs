//! Counters of what visitors and guests did with an applet, per hour and per day.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use crate::applet::record::Applet;
use crate::structs::execution::workflow_run::RunLimits;
use crate::types::Key;

const USAGE_ROW: u8 = 3;
/// Default active workflow run cap for anonymous guest runs when the applet sets none.
pub const GUEST_ACTIVE_CAP: u32 = 4;

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

fn min_cap<T: Copy + Ord>(left: Option<T>, right: T) -> Option<T> {
    Some(left.map_or(right, |left| left.min(right)))
}

/// Compute and concurrency caps for one workflow run started through `applet`.
pub fn run_limits(applet: &Applet, guest: bool) -> RunLimits {
    let limits = &applet.grants.limits;
    match (guest, &applet.guests) {
        (true, Some(policy)) => {
            let guest_active = limits
                .active_runs
                .unwrap_or(GUEST_ACTIVE_CAP)
                .min(GUEST_ACTIVE_CAP)
                .min(policy.hourly);
            RunLimits {
                cpu_cores: min_cap(limits.cpu_cores, policy.cpu_cores),
                ram_bytes: min_cap(limits.ram_bytes, policy.ram_bytes),
                walltime_ms: min_cap(limits.walltime_ms, policy.walltime_ms),
                active_cap: Some(guest_active),
            }
        }
        (true, None) => RunLimits {
            cpu_cores: Some(0),
            ram_bytes: Some(0),
            walltime_ms: Some(0),
            active_cap: Some(0),
        },
        (false, _) => RunLimits {
            cpu_cores: limits.cpu_cores,
            ram_bytes: limits.ram_bytes,
            walltime_ms: limits.walltime_ms,
            active_cap: limits.active_runs,
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

    #[test]
    fn guest_limits_bounded() {
        use crate::applet::record::{Access, Applet, Embedding, Grants, GuestPolicy, Lifecycle};
        use crate::structs::identity::realm::RealmId;
        use crate::user::id::UserId;

        let realm = RealmId([1; 32]);
        let user = UserId::local(Ulid::from_bytes([2; 16]), realm);
        let mut applet = Applet {
            applet_id: Ulid::from_bytes([3; 16]),
            group_id: Ulid::from_bytes([4; 16]),
            name: "demo".into(),
            slug: "demo".into(),
            host: "demo.local".into(),
            repository: None,
            host_node: iroh::SecretKey::from_bytes(&[5; 32]).public(),
            lifecycle: Lifecycle::Live,
            access: Access::Public,
            live: Some(1),
            versions: Vec::new(),
            drafts: Vec::new(),
            grants: Grants::default(),
            embedding: Embedding::default(),
            guests: None,
            domains: Vec::new(),
            pending: None,
            account: None,
            role_id: None,
            visitor_salt: [0; 32],
            created_by: user,
            created_at_ms: 1,
            revision: 1,
        };
        assert_eq!(run_limits(&applet, true).active_cap, Some(0));
        applet.grants.limits.cpu_cores = Some(2);
        applet.grants.limits.active_runs = Some(6);
        applet.guests = Some(GuestPolicy {
            enabled_by: user,
            in_frames: false,
            cpu_cores: 4,
            ram_bytes: 512,
            walltime_ms: 10_000,
            file_bytes: 1024,
            hourly: 3,
            daily: 10,
            expiry_days: 1,
        });
        let guest = run_limits(&applet, true);
        assert_eq!(guest.cpu_cores, Some(2));
        assert_eq!(guest.ram_bytes, Some(512));
        assert_eq!(guest.walltime_ms, Some(10_000));
        assert_eq!(guest.active_cap, Some(3));
        let signed = run_limits(&applet, false);
        assert_eq!(signed.cpu_cores, Some(2));
        assert_eq!(signed.active_cap, Some(6));
    }
}
