//! Offers one durable launch to its execution target and returns the target's answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, LaunchFrame, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, LaunchDecline, MetadataAnswer, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use tracing::{debug, warn};

use super::LifecycleError;
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};

/// What a target answered to one offer.
#[derive(Debug, PartialEq)]
pub enum OfferOutcome {
    Accepted(JobRecordFrame),
    Declined(LaunchDecline),
    /// The target never answered; it may still have accepted the launch.
    Unavailable,
}

/// Offers one durable launch to a remote target and returns the answer verbatim. A launch
/// planned onto this node is admitted by its caller instead: no transport dials itself.
#[derive(Debug, PartialEq)]
pub struct OfferLaunchOperation {
    target: NodeId,
    frame: Option<Vec<u8>>,
    timeout: Duration,
}

impl OfferLaunchOperation {
    pub fn new(target: NodeId, launch: LaunchFrame, timeout: Duration) -> Self {
        let message = MetadataTransportMessage::ForwardLaunchOffer {
            launch: Box::new(launch),
        };
        let frame = encode_message(&message)
            .inspect_err(|error| warn!(error = %error, "Launch offer frame does not encode"))
            .ok();
        Self {
            target,
            frame,
            timeout,
        }
    }
}

impl Operation for OfferLaunchOperation {
    type Output = OfferOutcome;
    type Error = LifecycleError;

    fn first(&mut self) -> Result<Next<OfferOutcome>, LifecycleError> {
        let Some(frame) = self.frame.take() else {
            return Ok(Next::Done(OfferOutcome::Unavailable));
        };
        Ok(Next::Call(IoCall::Effect(Effect::Net(
            NetEffect::MetadataRequest(Box::new(MetadataRequestEffect {
                peer: self.target,
                frame,
                timeout: self.timeout,
            })),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<OfferOutcome>, LifecycleError> {
        let IoResult::Event(Event::Net(NetEvent::MetadataAnswer(answer))) = result else {
            return Err(LifecycleError::UnexpectedEvent {
                state: "Offer".to_string(),
                expected: "metadata answer",
                got: "another result",
            });
        };
        let MetadataAnswer { result, .. } = *answer;
        let reply = match result {
            Ok(bytes) => decode_message(&bytes).map_err(|error| error.to_string()),
            Err(error) => Err(format!("{error:?}")),
        };
        Ok(Next::Done(match reply {
            Ok(MetadataTransportMessage::ForwardedLaunchOffer {
                result: Ok(receipt),
            }) => match JobRecordFrame::new(receipt.into_inner()) {
                Ok(frame) => OfferOutcome::Accepted(frame),
                Err(error) => return Err(LifecycleError::Store(error.into())),
            },
            Ok(MetadataTransportMessage::ForwardedLaunchOffer {
                result: Err(reason),
            }) => {
                debug!(peer = %self.target, reason = ?reason, "Execution target declined a launch");
                OfferOutcome::Declined(reason)
            }
            Ok(other) => {
                debug!(
                    peer = %self.target,
                    reply = crate::metadata::transport_message_kind(&other),
                    "Execution target answered a launch offer with another message"
                );
                OfferOutcome::Unavailable
            }
            Err(message) => {
                debug!(message, "Execution target did not answer the offer");
                OfferOutcome::Unavailable
            }
        }))
    }
}

#[cfg(test)]
#[path = "offer_tests.rs"]
mod state_machine_tests;
