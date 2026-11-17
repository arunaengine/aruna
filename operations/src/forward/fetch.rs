//! Asks peers with encoded metadata requests: the holders of a record in rank order under one
//! deadline, or many peers at once as one effect group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, EffectGroup, GroupForm, MetadataRequestEffect, NetEffect};
use aruna_core::events::{
    Event, GroupEvent, GroupResult, MetadataAnswer, NetEvent, PeerRequestError,
};
use aruna_core::operation::EffectOperation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{debug, warn};

use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};

/// What one holder's answer means to the fetch.
#[derive(Debug, PartialEq)]
pub enum Answer<T> {
    Found(T),
    /// The holder answered without the record.
    Missing,
    /// The holder refused the caller; no other holder is asked.
    Denied,
    /// The answer says nothing; the next holder is asked.
    Skip,
}

/// A record a holder fetch can read from one holder's answer.
pub trait HolderRecord: Sized {
    /// Names the record in the unavailable reason, for example `policy`.
    const LABEL: &'static str;
    fn read(message: MetadataTransportMessage) -> Answer<Self>;
}

/// The outcome of a fetch. Only a complete absence of answers is `Unavailable`.
#[derive(Debug, PartialEq)]
pub enum Fetched<T> {
    Found { holder: NodeId, value: T },
    NotFound,
    Denied,
    Unavailable(String),
}

/// How the deadline spans the holders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Budget {
    /// Each request may use the whole remaining time; a timeout ends the fetch.
    Shared,
    /// The remaining time is split evenly over the holders left; a timeout asks the next one.
    Split,
}

#[derive(Debug, Error, PartialEq)]
pub enum FetchError {
    #[error("the holder fetch never finished")]
    Unfinished,
    #[error("unexpected event while waiting for a holder answer")]
    Unexpected,
}

/// One request sent to each holder in turn until one answers with the record or refuses.
#[derive(Debug, PartialEq)]
pub struct HolderFetchOperation<T> {
    holders: Vec<NodeId>,
    frame: Vec<u8>,
    remaining: Duration,
    budget: Budget,
    asked: Option<usize>,
    answered: bool,
    output: Option<Result<Fetched<T>, FetchError>>,
}

impl<T: HolderRecord> HolderFetchOperation<T> {
    pub fn new(
        holders: &[NodeId],
        request: &MetadataTransportMessage,
        deadline: Duration,
        budget: Budget,
    ) -> Self {
        let (frame, output) = match encode_message(request) {
            Ok(frame) => (frame, None),
            Err(error) => (Vec::new(), Some(Ok(Fetched::Unavailable(error)))),
        };
        Self {
            holders: holders.to_vec(),
            frame,
            remaining: deadline,
            budget,
            asked: None,
            answered: false,
            output,
        }
    }

    fn ask(&mut self, index: usize) -> Effects {
        let Some(peer) = self.holders.get(index).copied() else {
            return self.finish();
        };
        if self.remaining.is_zero() {
            return self.finish();
        }
        let timeout = match self.budget {
            Budget::Shared => self.remaining,
            Budget::Split => self.remaining / (self.holders.len() - index) as u32,
        };
        self.asked = Some(index);
        smallvec![Effect::Net(NetEffect::MetadataRequest(Box::new(
            MetadataRequestEffect {
                peer,
                frame: self.frame.clone(),
                timeout,
            }
        )))]
    }

    fn finish(&mut self) -> Effects {
        self.asked = None;
        let outcome = match self.answered {
            true => Fetched::NotFound,
            false => Fetched::Unavailable(format!("no {} holder answered", T::LABEL)),
        };
        self.output = Some(Ok(outcome));
        smallvec![]
    }

    fn answer(&mut self, index: usize, answer: MetadataAnswer) -> Effects {
        self.remaining = self.remaining.saturating_sub(answer.elapsed);
        let holder = answer.peer;
        let bytes = match answer.result {
            Ok(bytes) => bytes,
            Err(PeerRequestError::TimedOut) if self.budget == Budget::Shared => {
                return self.finish();
            }
            Err(error) => {
                warn!(peer = %holder, error = ?error, fetch = T::LABEL, "Holder fetch failed");
                return self.ask(index + 1);
            }
        };
        let message = match decode_message(&bytes) {
            Ok(message) => message,
            Err(error) => {
                warn!(peer = %holder, error = %error, fetch = T::LABEL, "Undecodable holder answer");
                return self.ask(index + 1);
            }
        };
        match T::read(message) {
            Answer::Found(value) => {
                self.asked = None;
                self.output = Some(Ok(Fetched::Found { holder, value }));
                smallvec![]
            }
            Answer::Denied => {
                self.asked = None;
                self.output = Some(Ok(Fetched::Denied));
                smallvec![]
            }
            Answer::Missing => {
                self.answered = true;
                self.ask(index + 1)
            }
            Answer::Skip => {
                debug!(peer = %holder, fetch = T::LABEL, "Holder answer carried no record");
                self.ask(index + 1)
            }
        }
    }
}

impl<T: HolderRecord + std::fmt::Debug + PartialEq + Send> EffectOperation for HolderFetchOperation<T> {
    type Output = Fetched<T>;
    type Error = FetchError;

    fn start(&mut self) -> Effects {
        if self.output.is_some() {
            return smallvec![];
        }
        self.ask(0)
    }

    fn step(&mut self, event: Event) -> Effects {
        let index = self.asked.take();
        match (index, event) {
            (Some(index), Event::Net(NetEvent::MetadataAnswer(answer)))
                if Some(answer.peer) == self.holders.get(index).copied() =>
            {
                self.answer(index, *answer)
            }
            _ => {
                self.output = Some(Err(FetchError::Unexpected));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Fetched<T>, FetchError> {
        self.output.unwrap_or(Err(FetchError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// One peer's answer, or why there is none.
pub type PeerAnswer = (NodeId, Result<MetadataTransportMessage, String>);

/// Asks every peer at once, at most `limit` at a time, and answers once all of them answered.
#[derive(Debug, PartialEq)]
pub struct FanoutOperation {
    requests: Vec<(NodeId, Result<Vec<u8>, String>)>,
    limit: usize,
    timeout: Duration,
    started: bool,
    output: Option<Result<Vec<PeerAnswer>, FetchError>>,
}

impl FanoutOperation {
    pub fn new(
        requests: Vec<(NodeId, MetadataTransportMessage)>,
        limit: usize,
        timeout: Duration,
    ) -> Self {
        let requests = (requests.into_iter())
            .map(|(peer, message)| (peer, encode_message(&message)))
            .collect();
        Self {
            requests,
            limit,
            timeout,
            started: false,
            output: None,
        }
    }

    fn answers(&self, results: Vec<GroupResult>) -> Vec<PeerAnswer> {
        let mut results = results.into_iter();
        (self.requests.iter())
            .map(|(peer, frame)| {
                let answer = match frame {
                    Err(error) => Err(error.clone()),
                    Ok(_) => read_answer(*peer, results.next()),
                };
                (*peer, answer)
            })
            .collect()
    }
}

/// Decodes one member's answer; a missing, cut off or failed answer is an error text.
fn read_answer(
    peer: NodeId,
    result: Option<GroupResult>,
) -> Result<MetadataTransportMessage, String> {
    match result {
        Some(GroupResult::Done(event)) => match *event {
            Event::Net(NetEvent::MetadataAnswer(answer)) if answer.peer == peer => {
                let MetadataAnswer { result, .. } = *answer;
                let bytes = result.map_err(|error| format!("{error:?}"))?;
                decode_message(&bytes).map_err(|error| error.to_string())
            }
            other => Err(format!("unexpected answer {}", other.label())),
        },
        Some(GroupResult::Unfinished) => Err("the peer did not answer in time".to_string()),
        Some(GroupResult::Refused) | None => Err("the request did not run".to_string()),
    }
}

impl EffectOperation for FanoutOperation {
    type Output = Vec<PeerAnswer>;
    type Error = FetchError;

    fn start(&mut self) -> Effects {
        self.started = true;
        let members: Vec<Effect> = (self.requests.iter())
            .filter_map(|(peer, frame)| {
                let frame = frame.as_ref().ok()?.clone();
                Some(Effect::Net(NetEffect::MetadataRequest(Box::new(
                    MetadataRequestEffect {
                        peer: *peer,
                        frame,
                        timeout: self.timeout,
                    },
                ))))
            })
            .collect();
        if members.is_empty() {
            self.output = Some(Ok(self.answers(Vec::new())));
            return smallvec![];
        }
        smallvec![Effect::Group(EffectGroup {
            members,
            limit: self.limit,
            form: GroupForm::All { deadline: None },
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match event {
            Event::Group(GroupEvent::Finished { results })
                if self.started && self.output.is_none() =>
            {
                Ok(self.answers(results))
            }
            _ => Err(FetchError::Unexpected),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Vec<PeerAnswer>, FetchError> {
        self.output.unwrap_or(Err(FetchError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "fetch_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "fanout_tests.rs"]
mod fanout_tests;
