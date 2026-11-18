//! Fetches one bounded page of a job family from its current holders, one holder after another.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, MetadataRequestEffect, NetEffect, PageLimit};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::SubmissionId;
use aruna_core::structs::placement::record::PlacementRef;
use tracing::{debug, warn};

use super::LifecycleError;
use crate::metadata::protocol::{
    JobPageReply, MetadataTransportMessage, decode_message, encode_message,
};

/// The holder that answered, if any, and the records of its page.
pub(super) type FetchedFamily = (Option<NodeId>, Vec<JobRecordFrame>);

/// Asks the holders in order for one page of a family. The first holder that answers with a page
/// ends the fetch; a holder that refuses or fails is passed over, and the shared deadline ends it.
#[derive(Debug, PartialEq)]
pub(super) struct FetchFamilyOperation {
    holders: Vec<NodeId>,
    frame: Option<Vec<u8>>,
    remaining: Duration,
    asked: usize,
}

impl FetchFamilyOperation {
    pub(super) fn new(
        placement: PlacementRef,
        holders: Vec<NodeId>,
        family: (SubmissionId, [u8; 32]),
        deadline: Duration,
    ) -> Self {
        let message = MetadataTransportMessage::ForwardRecordPage {
            placement,
            submission_id: family.0,
            request_digest: Some(family.1),
            cursor: None,
            limit: PageLimit::default(),
        };
        let frame = encode_message(&message)
            .inspect_err(|error| warn!(error = %error, "Job record fetch frame does not encode"))
            .ok();
        Self {
            holders,
            frame,
            remaining: deadline,
            asked: 0,
        }
    }

    /// Asks the next holder, or ends with no page when none is left or time ran out.
    fn ask(&mut self) -> Next<FetchedFamily> {
        let (Some(frame), Some(peer)) = (self.frame.as_ref(), self.holders.get(self.asked)) else {
            return Next::Done((None, Vec::new()));
        };
        if self.remaining.is_zero() {
            return Next::Done((None, Vec::new()));
        }
        Next::Call(IoCall::Effect(Box::new(Effect::Net(
            NetEffect::MetadataRequest(Box::new(MetadataRequestEffect {
                peer: *peer,
                frame: frame.clone(),
                timeout: self.remaining,
            })),
        ))))
    }
}

impl Operation for FetchFamilyOperation {
    type Output = FetchedFamily;
    type Error = LifecycleError;

    fn first(&mut self) -> Result<Next<FetchedFamily>, LifecycleError> {
        Ok(self.ask())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FetchedFamily>, LifecycleError> {
        let unexpected = || LifecycleError::UnexpectedEvent {
            state: "Fetch".to_string(),
            expected: "metadata answer",
            got: "another result",
        };
        let IoResult::Event(event) = result else {
            return Err(unexpected());
        };
        let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
            return Err(unexpected());
        };
        let MetadataAnswer {
            peer,
            elapsed,
            result,
        } = *answer;
        self.remaining = self.remaining.saturating_sub(elapsed);
        self.asked += 1;
        let bytes = match result {
            Ok(bytes) => bytes,
            // The shared deadline passed; no later holder gets a turn.
            Err(PeerRequestError::TimedOut) => return Ok(Next::Done((None, Vec::new()))),
            Err(PeerRequestError::Failed(error) | PeerRequestError::NotSent(error)) => {
                warn!(peer = %peer, error = %error, "Job record fetch failed");
                return Ok(self.ask());
            }
        };
        match decode_message(&bytes) {
            Ok(MetadataTransportMessage::ForwardedRecordPage {
                result: Ok(JobPageReply { page, .. }),
            }) => Ok(Next::Done((Some(peer), page.into_inner()))),
            Ok(MetadataTransportMessage::ForwardedRecordPage {
                result: Err(reason),
            }) => {
                warn!(peer = %peer, reason = ?reason, "Job record holder refused the fetch");
                Ok(self.ask())
            }
            Ok(MetadataTransportMessage::ForwardedWriteUnavailable) => {
                debug!(peer = %peer, "Job record holder is not ready");
                Ok(self.ask())
            }
            Ok(other) => {
                warn!(
                    peer = %peer,
                    reply = crate::metadata::transport_message_kind(&other),
                    "Unexpected job record fetch reply"
                );
                Ok(self.ask())
            }
            Err(error) => {
                warn!(peer = %peer, error = %error, "Job record fetch reply does not decode");
                Ok(self.ask())
            }
        }
    }
}

#[cfg(test)]
#[path = "fetch_tests.rs"]
mod state_machine_tests;
