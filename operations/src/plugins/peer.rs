//! Asks another node over the metadata transport from a plugin operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{Decisions, EventBatch};
use aruna_core::plugins::{Chunk, NodeCheckError, ReleaseError, ReleaseRequest, Released};
use aruna_core::secrets::{SecretKind, SecretName};

use super::artifacts::{ServeFetch, ServeStore, ServingNode};
use super::bound::{NodeCheckFailure, ServeNodeCheck};
use super::clock::ServeClock;
use super::decide::ServeEvents;
use super::secret_use::{ReleaseFailure, ServeRelease};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, encode_message};

/// Upper bound of one plugin request to another node.
const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// The call that sends `message` to `node`; `None` when it cannot be encoded.
pub(crate) fn ask_peer(node: NodeId, message: &MetadataTransportMessage) -> Option<IoCall> {
    let frame = encode_message(message).ok()?;
    Some(peer_call(node, frame, PEER_TIMEOUT))
}

/// What `node` answered to [`ask_peer`]; `None` when it did not answer.
pub(crate) fn peer_answer(
    node: NodeId,
    result: IoResult,
) -> Result<Option<MetadataTransportMessage>, IoError> {
    Ok(peer_reply(node, result)?.ok())
}

/// What another node asks of this node's plugins.
#[derive(Debug, PartialEq)]
enum PeerAsk {
    Store(Chunk),
    Fetch(String, u32),
    Clock,
    Events(EventBatch),
    Release(ReleaseRequest),
    NodeCheck(SecretName, SecretKind),
}

/// Which answer a plugin request gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerAnswer {
    Stored,
    Fetched,
    Clock,
    Decided,
    Released,
    Checked,
}

impl PeerAnswer {
    /// The answer of a node that cannot serve the request now.
    pub(crate) fn unavailable(self) -> MetadataTransportMessage {
        use MetadataReadError::Unavailable;
        match self {
            Self::Stored => MetadataTransportMessage::ArtifactStored {
                result: Err(Unavailable),
            },
            Self::Fetched => MetadataTransportMessage::FetchedArtifact {
                result: Err(Unavailable),
            },
            Self::Clock => MetadataTransportMessage::ClockProbed {
                result: Err(Unavailable),
            },
            Self::Decided => MetadataTransportMessage::EventsDecided {
                result: Err(Unavailable),
            },
            Self::Released => MetadataTransportMessage::SecretReleased {
                result: Err(ReleaseError::Unavailable("no network".to_string())),
            },
            Self::Checked => MetadataTransportMessage::NodeSecretChecked {
                result: Err(NodeCheckError::Unavailable),
            },
        }
    }
}

/// Answers one plugin request of another node; a node without a network serves none.
#[derive(Debug, PartialEq)]
pub struct AnswerPeer {
    node: Option<ServingNode>,
    peer: NodeId,
    now_ms: u64,
    ask: Option<PeerAsk>,
    answer: PeerAnswer,
}

impl AnswerPeer {
    /// The operation for `message`, or the message back when it is no plugin request.
    pub fn new(
        node: Option<ServingNode>,
        peer: NodeId,
        message: MetadataTransportMessage,
        now_ms: u64,
    ) -> Result<Self, MetadataTransportMessage> {
        use MetadataTransportMessage as Message;
        let (ask, answer) = match message {
            Message::StoreArtifact { chunk } => (PeerAsk::Store(chunk), PeerAnswer::Stored),
            Message::FetchArtifact { digest, index } => {
                (PeerAsk::Fetch(digest, index), PeerAnswer::Fetched)
            }
            Message::ProbeClock => (PeerAsk::Clock, PeerAnswer::Clock),
            Message::ForwardEvents { batch } => (PeerAsk::Events(batch), PeerAnswer::Decided),
            Message::ReleaseSecret { request } => (PeerAsk::Release(request), PeerAnswer::Released),
            Message::CheckNodeSecret { name, kind } => {
                (PeerAsk::NodeCheck(name, kind), PeerAnswer::Checked)
            }
            other => return Err(other),
        };
        Ok(Self {
            node,
            peer,
            now_ms,
            ask: Some(ask),
            answer,
        })
    }

    /// The kind of answer this request gets, also when the runner gives up on it.
    pub fn answer(&self) -> PeerAnswer {
        self.answer
    }

    fn call(&self, node: ServingNode, ask: PeerAsk) -> IoCall {
        let (peer, now_ms) = (self.peer, self.now_ms);
        match ask {
            PeerAsk::Store(chunk) => IoCall::child(ServeStore::new(node, peer, chunk, now_ms)),
            PeerAsk::Fetch(digest, index) => {
                IoCall::child(ServeFetch::new(node, peer, digest, index))
            }
            PeerAsk::Clock => IoCall::child(ServeClock::new(node, peer, now_ms)),
            PeerAsk::Events(batch) => IoCall::child(ServeEvents::new(node, peer, batch, now_ms)),
            PeerAsk::Release(request) => IoCall::child(ServeRelease::new(node, peer, request)),
            PeerAsk::NodeCheck(name, kind) => {
                IoCall::child(ServeNodeCheck::new(node, peer, name, kind))
            }
        }
    }
}

/// A served read's result; a child the runner could not finish reads as unavailable.
fn served<T: 'static>(result: IoResult) -> Result<T, MetadataReadError> {
    let served = result.child::<T, MetadataReadError>();
    served.unwrap_or(Err(MetadataReadError::Unavailable))
}

impl Operation for AnswerPeer {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<MetadataTransportMessage>, IoError> {
        let ask = self.ask.take().ok_or(IoError::Unfinished)?;
        Ok(match self.node {
            Some(node) => Next::Call(self.call(node, ask)),
            None => Next::Done(self.answer.unavailable()),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataTransportMessage>, IoError> {
        use MetadataTransportMessage as Message;
        Ok(Next::Done(match self.answer {
            PeerAnswer::Stored => Message::ArtifactStored {
                result: served::<bool>(result),
            },
            PeerAnswer::Fetched => Message::FetchedArtifact {
                result: served::<Chunk>(result),
            },
            PeerAnswer::Clock => Message::ClockProbed {
                result: served::<u64>(result),
            },
            PeerAnswer::Decided => Message::EventsDecided {
                result: served::<Decisions>(result),
            },
            PeerAnswer::Released => {
                let released = result.child::<Released, ReleaseFailure>();
                let unavailable = || ReleaseError::Unavailable("no answer".to_string());
                Message::SecretReleased {
                    result: match released {
                        Ok(released) => released.map_err(|failure| failure.0),
                        Err(_) => Err(unavailable()),
                    },
                }
            }
            PeerAnswer::Checked => {
                let checked = result.child::<Option<String>, NodeCheckFailure>();
                Message::NodeSecretChecked {
                    result: match checked {
                        Ok(checked) => checked.map_err(|failure| failure.0),
                        Err(_) => Err(NodeCheckError::Unavailable),
                    },
                }
            }
        }))
    }
}

#[cfg(test)]
#[path = "peer_tests.rs"]
mod tests;
