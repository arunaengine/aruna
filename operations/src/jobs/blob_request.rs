//! Sends one external listing or hashing request to the blob adapter and returns its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::EffectOperation;
use aruna_core::types::Effects;
use smallvec::smallvec;

/// Emits one listing or hashing effect and hands back the blob adapter's answer.
#[derive(Debug, PartialEq)]
pub(crate) struct BlobRequest {
    state: RequestState,
    output: Option<Result<BlobEvent, BlobError>>,
}

#[derive(Debug, PartialEq)]
enum RequestState {
    Ready(BlobEffect),
    Sent(Answer),
    Done,
}

/// The one event kind that answers a sent request, besides an error.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Answer {
    Listed,
    Hashed,
}

impl BlobRequest {
    pub(crate) fn new(effect: BlobEffect) -> Self {
        Self {
            state: RequestState::Ready(effect),
            output: None,
        }
    }

    fn finish(&mut self, output: Result<BlobEvent, BlobError>) -> Effects {
        self.state = RequestState::Done;
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for BlobRequest {
    type Output = BlobEvent;
    type Error = BlobError;

    fn start(&mut self) -> Effects {
        let RequestState::Ready(effect) = std::mem::replace(&mut self.state, RequestState::Done)
        else {
            return self.finish(Err(BlobError::ReadError("request already sent".into())));
        };
        let answer = match &effect {
            BlobEffect::ListExternal { .. } => Answer::Listed,
            BlobEffect::HashExternal { .. } => Answer::Hashed,
            other => {
                let error = format!("not a listing or hashing request: {other:?}");
                return self.finish(Err(BlobError::ReadError(error)));
            }
        };
        self.state = RequestState::Sent(answer);
        smallvec![Effect::Blob(effect)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let RequestState::Sent(answer) = self.state else {
            let error = format!("event outside a sent request: {}", event.label());
            return self.finish(Err(BlobError::ReadError(error)));
        };
        match (answer, event) {
            (_, Event::Blob(BlobEvent::Error(error))) => self.finish(Err(error)),
            (Answer::Listed, Event::Blob(event @ BlobEvent::ExternalListed { .. }))
            | (Answer::Hashed, Event::Blob(event @ BlobEvent::ExternalHashed { .. })) => {
                self.finish(Ok(event))
            }
            (_, other) => {
                let error = format!("unexpected event {}", other.label());
                self.finish(Err(BlobError::ReadError(error)))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RequestState::Done
    }

    fn finalize(self) -> Result<BlobEvent, BlobError> {
        self.output
            .unwrap_or_else(|| Err(BlobError::ReadError("no answer".into())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
