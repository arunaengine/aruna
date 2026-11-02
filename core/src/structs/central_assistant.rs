//! The realm's central assistant provider and the per-user daily count of its model calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::AssistantProvider;
use serde::{Deserialize, Serialize};

/// Reserved provider id; stored provider ids are 26-character ULIDs and never collide with it.
pub const CENTRAL_PROVIDER_ID: &str = "central";
/// Default upstream header that carries the pseudonymous user id.
pub const USER_HEADER: &str = "x-user-id";
/// Inbound header with the chat session id, and the default upstream header for it.
pub const SESSION_HEADER: &str = "x-session-id";
const DAY_SECS: u64 = 86_400;

/// The one central provider a realm admin set up on the holder node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CentralAssistant {
    /// Sealed like any provider; `user_id` is the admin who saved it last.
    pub provider: AssistantProvider,
    /// Model calls per user per UTC day; `None` means no limit.
    pub daily_limit: Option<u32>,
    pub user_header: String,
    pub session_header: String,
    pub changed_at: u64,
}

/// Model calls one user made on `day` (days since 1970, UTC).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantUsage {
    pub day: u32,
    pub count: u32,
}

/// The daily limit refused a call; `used` calls were already made today.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuotaReached {
    pub used: u32,
    pub limit: u32,
}

impl CentralAssistant {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl AssistantUsage {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Calls counted for `today`; a row of an earlier day counts as none.
    pub fn used_on(usage: Option<Self>, today: u32) -> u32 {
        usage
            .filter(|usage| usage.day == today)
            .map_or(0, |usage| usage.count)
    }
}

/// The UTC day a unix time falls on.
pub fn utc_day(unix_secs: u64) -> u32 {
    u32::try_from(unix_secs / DAY_SECS).unwrap_or(u32::MAX)
}

/// Seconds until the next UTC midnight, at least one.
pub fn secs_to_midnight(unix_secs: u64) -> u64 {
    DAY_SECS - unix_secs % DAY_SECS
}

/// Counts one more call for `today`, or refuses it when the limit is reached.
pub fn charge(
    usage: Option<AssistantUsage>,
    today: u32,
    limit: Option<u32>,
) -> Result<AssistantUsage, QuotaReached> {
    let used = AssistantUsage::used_on(usage, today);
    if let Some(limit) = limit
        && used >= limit
    {
        return Err(QuotaReached { used, limit });
    }
    Ok(AssistantUsage {
        day: today,
        count: used.saturating_add(1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_same_day() {
        let usage = AssistantUsage { day: 5, count: 2 };
        assert_eq!(
            charge(Some(usage), 5, Some(3)),
            Ok(AssistantUsage { day: 5, count: 3 })
        );
    }

    #[test]
    fn new_day_resets() {
        let usage = AssistantUsage { day: 4, count: 50 };
        assert_eq!(
            charge(Some(usage), 5, Some(50)),
            Ok(AssistantUsage { day: 5, count: 1 })
        );
    }

    #[test]
    fn refuses_at_limit() {
        let usage = AssistantUsage { day: 5, count: 3 };
        assert_eq!(
            charge(Some(usage), 5, Some(3)),
            Err(QuotaReached { used: 3, limit: 3 })
        );
        assert_eq!(
            charge(None, 5, Some(0)),
            Err(QuotaReached { used: 0, limit: 0 })
        );
    }

    #[test]
    fn no_limit_saturates() {
        let usage = AssistantUsage {
            day: 5,
            count: u32::MAX,
        };
        assert_eq!(charge(Some(usage), 5, None).unwrap().count, u32::MAX);
    }

    #[test]
    fn day_boundary() {
        assert_eq!(utc_day(86_399), 0);
        assert_eq!(utc_day(86_400), 1);
        assert_eq!(secs_to_midnight(86_399), 1);
        assert_eq!(secs_to_midnight(86_400), 86_400);
    }
}
