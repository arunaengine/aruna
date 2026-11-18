//! Reads another infrastructure node's clock with a round trip, which bounds the offset between
//! the two clocks; a replay maps an install's activation time onto this node's journal with it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;

use super::artifacts::{ServingNode, served_config};
use super::peer::{ask_peer, peer_answer};
use crate::document_repository::GetConfigOperation;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
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
pub(crate) fn local_start(boundary_ms: u64, reading: Option<ClockReading>) -> Option<u64> {
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
#[derive(Debug, PartialEq)]
pub(crate) struct ServeClock {
    node: ServingNode,
    peer: NodeId,
    now_ms: u64,
}

impl ServeClock {
    pub(crate) fn new(node: ServingNode, peer: NodeId, now_ms: u64) -> Self {
        Self { node, peer, now_ms }
    }
}

impl Operation for ServeClock {
    type Output = u64;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<u64>, MetadataReadError> {
        let read = GetConfigOperation::new(self.node.0);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, MetadataReadError> {
        let config = served_config(result)?;
        match node_kind(&config, self.peer).is_some_and(|kind| kind.is_sync_eligible()) {
            true => Ok(Next::Done(self.now_ms)),
            false => Err(MetadataReadError::Forbidden),
        }
    }
}

/// The clock of `node` over the metadata transport; `None` when it does not answer.
#[derive(Debug, PartialEq)]
pub(crate) struct AskClock(pub NodeId);

impl Operation for AskClock {
    type Output = Option<u64>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<u64>>, IoError> {
        let probe = ask_peer(self.0, &MetadataTransportMessage::ProbeClock);
        Ok(match probe {
            Some(call) => Next::Call(call),
            None => Next::Done(None),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, IoError> {
        Ok(Next::Done(match peer_answer(self.0, result)? {
            Some(MetadataTransportMessage::ClockProbed { result: Ok(clock) }) => Some(clock),
            _ => None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::plugins::fixtures::{
        self, config_result, is_child, is_effect, node, peer_says, peer_silent,
    };

    const MINUTE: u64 = 60 * 1000;

    #[test]
    fn serve_checks_peer() {
        let place = (fixtures::realm(), node(1));
        let mut serve = ServeClock::new(place, node(3), 7);
        assert!(is_child(&serve.first()));
        let answer = serve.next(config_result(fixtures::realm_nodes()));
        assert_eq!(answer, Ok(Next::Done(7)));
        let mut device = ServeClock::new(place, node(4), 7);
        device.first().unwrap();
        let refused = device.next(config_result(fixtures::realm_nodes()));
        assert_eq!(refused, Err(MetadataReadError::Forbidden));
    }

    #[test]
    fn ask_reads_answer() {
        let mut ask = AskClock(node(1));
        assert!(is_effect(&ask.first()));
        let probed = MetadataTransportMessage::ClockProbed { result: Ok(9) };
        assert_eq!(
            ask.next(peer_says(node(1), &probed)),
            Ok(Next::Done(Some(9)))
        );
        assert_eq!(ask.next(peer_silent(node(1))), Ok(Next::Done(None)));
    }

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
