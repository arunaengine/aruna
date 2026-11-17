//! Offers one durable launch to its execution target and returns the target's answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, JobRecordFrame, LaunchOfferEffect, NetEffect};
use aruna_core::events::{Event, LaunchDecline, LaunchOfferEvent, NetEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use tracing::debug;

use super::LifecycleError;

/// What a target answered to one offer.
#[derive(Debug, PartialEq)]
pub enum OfferOutcome {
    Accepted(JobRecordFrame),
    Declined(LaunchDecline),
    /// The target never answered; it may still have accepted the launch.
    Unavailable,
}

/// Offers one durable launch to its target and returns the answer verbatim.
#[derive(Debug, PartialEq)]
pub struct OfferLaunchOperation {
    effect: Option<LaunchOfferEffect>,
    outcome: Option<Result<OfferOutcome, LifecycleError>>,
    sent: bool,
}

impl OfferLaunchOperation {
    pub fn new(effect: LaunchOfferEffect) -> Self {
        Self {
            effect: Some(effect),
            outcome: None,
            sent: false,
        }
    }
}

impl EffectOperation for OfferLaunchOperation {
    type Output = OfferOutcome;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        let Some(effect) = self.effect.take() else {
            self.outcome = Some(Err(LifecycleError::NotFinished));
            return smallvec![];
        };
        self.sent = true;
        smallvec![Effect::Net(NetEffect::LaunchOffer(Box::new(effect)))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if !self.sent {
            self.outcome = Some(Err(LifecycleError::UnexpectedEvent {
                state: "Settled".to_string(),
                expected: "no further event",
                got: event.label(),
            }));
            return smallvec![];
        }
        self.outcome = Some(match event {
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Accepted { receipt, .. })) => {
                match JobRecordFrame::new(receipt.into_inner()) {
                    Ok(frame) => Ok(OfferOutcome::Accepted(frame)),
                    Err(error) => Err(LifecycleError::Store(error.into())),
                }
            }
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Declined { target, reason })) => {
                debug!(peer = %target, reason = ?reason, "Execution target declined a launch");
                Ok(OfferOutcome::Declined(reason))
            }
            Event::Net(NetEvent::LaunchOffer(LaunchOfferEvent::Unavailable(message))) => {
                debug!(message, "Execution target did not answer the offer");
                Ok(OfferOutcome::Unavailable)
            }
            other => Err(LifecycleError::UnexpectedEvent {
                state: "Offer".to_string(),
                expected: "launch offer result",
                got: other.label(),
            }),
        });
        self.sent = false;
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
