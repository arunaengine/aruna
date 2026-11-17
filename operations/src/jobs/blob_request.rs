//! Sends one external listing or hashing request to the blob adapter and returns its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;

/// Emits one listing or hashing effect and hands back the blob adapter's answer.
#[derive(Debug, PartialEq)]
pub(crate) struct BlobRequest {
    effect: Option<BlobEffect>,
    answer: Option<Answer>,
}

/// The one event kind that answers a sent request, besides an error.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Answer {
    Listed,
    Hashed,
}

impl BlobRequest {
    pub(crate) fn new(effect: BlobEffect) -> Self {
        let answer = match &effect {
            BlobEffect::ListExternal { .. } => Some(Answer::Listed),
            BlobEffect::HashExternal { .. } => Some(Answer::Hashed),
            _ => None,
        };
        Self {
            effect: Some(effect),
            answer,
        }
    }
}

impl Operation for BlobRequest {
    type Output = BlobEvent;
    type Error = BlobError;

    fn first(&mut self) -> Result<Next<BlobEvent>, BlobError> {
        match (self.answer, self.effect.take()) {
            (Some(_), Some(effect)) => Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(
                effect,
            ))))),
            (None, _) => Err(BlobError::ReadError("not a listing or hashing request".into())),
            (Some(_), None) => Err(BlobError::ReadError("request already sent".into())),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BlobEvent>, BlobError> {
        let label = result.label();
        let IoResult::Event(event) = result else {
            return Err(BlobError::ReadError(format!("unexpected result {label}")));
        };
        match (self.answer.take(), *event) {
            (_, Event::Blob(BlobEvent::Error(error))) => Err(error),
            (Some(Answer::Listed), Event::Blob(event @ BlobEvent::ExternalListed { .. }))
            | (Some(Answer::Hashed), Event::Blob(event @ BlobEvent::ExternalHashed { .. })) => {
                Ok(Next::Done(event))
            }
            _ => Err(BlobError::ReadError(format!("unexpected event {label}"))),
        }
    }
}
