//! Reads another infrastructure node's clock with a round trip, which bounds the offset between
//! the two clocks; a replay maps an install's activation time onto this node's journal with it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::time::unix_timestamp_millis;

use super::artifacts::local_config;
use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataReadError;
use crate::realm::mutate_placement::node_kind;

/// Supported clock skew between nodes; a replay starts this much before its mapped boundary.
pub const SKEW_MARGIN_MS: u64 = 5 * 60 * 1000;
/// Longest probe round trip that still bounds the other node's clock.
const PROBE_LIMIT_MS: u64 = 30 * 1000;

/// One reading of another node's clock: local send time, its time, local receive time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockReading {
    pub sent_ms: u64,
    pub remote_ms: u64,
    pub received_ms: u64,
}

/// The earliest local time that can hold an event at or after `boundary_ms` on the clock a
/// reading came from, less the margin; `None` without a reading or with a slow round trip.
pub fn local_start(boundary_ms: u64, reading: Option<ClockReading>) -> Option<u64> {
    let reading = reading?;
    let trip = reading.received_ms.checked_sub(reading.sent_ms)?;
    if trip > PROBE_LIMIT_MS {
        return None;
    }
    // The other clock is ahead of this one by at most `remote - sent`.
    let start = i128::from(boundary_ms) + i128::from(reading.sent_ms)
        - i128::from(reading.remote_ms)
        - i128::from(SKEW_MARGIN_MS);
    u64::try_from(start.max(0)).ok()
}

/// This node's clock for an infrastructure peer that maps an install's activation time.
pub(crate) async fn serve_clock(
    ctx: &DriverContext,
    peer: NodeId,
) -> Result<u64, MetadataReadError> {
    let (config, _) = local_config(ctx).await?;
    match node_kind(&config, peer).is_some_and(|kind| kind.is_sync_eligible()) {
        true => Ok(unix_timestamp_millis()),
        false => Err(MetadataReadError::Forbidden),
    }
}

/// One reading of `node`'s clock over the metadata transport.
pub async fn probe_clock(ctx: &DriverContext, node: NodeId) -> Option<ClockReading> {
    let handle = ctx.metadata_handle.as_ref()?;
    let sent_ms = unix_timestamp_millis();
    let remote_ms = handle.request_clock(node).await.ok()?;
    let received_ms = unix_timestamp_millis();
    Some(ClockReading {
        sent_ms,
        remote_ms,
        received_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: u64 = 60 * 1000;

    #[test]
    fn clock_bounds() {
        let (sent, boundary) = (1_000 * MINUTE, 1_003 * MINUTE);
        let reading = |offset: i64, trip: u64| ClockReading {
            sent_ms: sent,
            remote_ms: sent.saturating_add_signed(offset) + trip / 2,
            received_ms: sent + trip,
        };
        // The activation happened here at `boundary - offset`; replay starts at or before it.
        for offset in [6 * MINUTE as i64, -6 * MINUTE as i64] {
            let start = local_start(boundary, Some(reading(offset, 20_000))).unwrap();
            let here = boundary.saturating_add_signed(-offset);
            assert!(start <= here && start + SKEW_MARGIN_MS + 20_000 >= here);
        }
        assert_eq!(local_start(boundary, Some(reading(0, 2 * MINUTE))), None);
        assert_eq!(local_start(boundary, None), None);
    }
}
