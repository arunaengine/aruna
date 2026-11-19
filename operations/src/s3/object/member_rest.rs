//! One request to another member's REST API with the caller's bearer token.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::member_read::{MemberAnswer, MemberBody, MemberFetch, MemberMethod, MemberSend};
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutboundEffect, OutboundEvent};
use aruna_core::structs::storage::node_info::NodeUrls;
use bytes::Bytes;
use ulid::Ulid;

use crate::s3::object::member_read::Header;

/// The nest every REST route is served under.
pub const API_PREFIX: &str = "/api/v1";
/// The nodes a relayed call passed, comma-separated.
pub const RELAY_HOP_HEADER: &str = "x-aruna-relay-hop";
/// The node a directed call is meant for; that node answers it itself.
pub const RELAY_NODE_HEADER: &str = "x-aruna-relay-node";
const RELAY_SET_HEADER: &str = "x-aruna-relay-set";
/// How long a node may take to start its answer to a read.
const ANSWER_LIMIT: Duration = Duration::from_secs(30);

/// What a read through a member's REST route sends. Never shown in a `Debug` print, as it holds
/// the caller's token.
#[derive(Clone, PartialEq)]
pub struct RestAsk {
    pub local: NodeId,
    pub path: String,
    pub bearer: Option<SharedSecret>,
    pub headers: Vec<Header>,
    /// Hands a refusal back as an answer, so its status and body stream through.
    pub stream: bool,
}

impl std::fmt::Debug for RestAsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RestAsk")
    }
}

/// One REST request to one node. Never shown in a `Debug` print, as it holds the caller's token.
#[derive(Clone, PartialEq)]
pub struct RestCall {
    pub node_id: NodeId,
    pub path: String,
    pub bearer: Option<SharedSecret>,
    /// This node and the distributed bucket, for a relayed request.
    pub relay: Option<(NodeId, Ulid)>,
    pub headers: Vec<Header>,
    /// The JSON body of a POST; without one the request is a GET.
    pub json: Option<Vec<u8>>,
    /// Whether the node must start answering in time.
    pub timed: bool,
    pub any_status: bool,
}

impl std::fmt::Debug for RestCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RestCall")
    }
}

/// What a node answered a REST request.
#[derive(Debug, PartialEq)]
pub enum RestAnswer {
    Body(MemberBody),
    Refused {
        status: u16,
        body: String,
    },
    /// The node has no published API, cannot be reached or did not answer in time.
    Silent,
}

/// The url of `path` on a node's published API base, which may already carry the nest.
pub fn rest_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    match base.ends_with(API_PREFIX) {
        true => format!("{base}{path}"),
        false => format!("{base}{API_PREFIX}{path}"),
    }
}

/// Finds the node's API and sends one request there.
#[derive(Debug, PartialEq)]
pub struct RestCallOperation {
    call: RestCall,
    asked: bool,
}

impl RestCallOperation {
    pub fn new(call: RestCall) -> Self {
        Self { call, asked: false }
    }

    /// The relayed read `ask` names, at `node_id`.
    pub(crate) fn relay(node_id: NodeId, set_id: Ulid, ask: &RestAsk) -> Self {
        Self::new(RestCall {
            node_id,
            path: ask.path.clone(),
            bearer: ask.bearer.clone(),
            relay: Some((ask.local, set_id)),
            headers: ask.headers.clone(),
            json: None,
            timed: true,
            any_status: ask.stream,
        })
    }

    fn fetch(&mut self, base: &str) -> Result<Next<RestAnswer>, IoError> {
        let call = &mut self.call;
        let mut headers = std::mem::take(&mut call.headers);
        if let Some((local, set_id)) = call.relay {
            headers.push((RELAY_HOP_HEADER.to_string(), local.to_string()));
            headers.push((RELAY_SET_HEADER.to_string(), set_id.to_string()));
        }
        if let Some(bearer) = &call.bearer {
            // A token that is no text cannot be sent, so the node counts as silent.
            let Ok(token) = std::str::from_utf8(bearer.bytes().expose()) else {
                return Ok(Next::Done(RestAnswer::Silent));
            };
            headers.push(("authorization".to_string(), format!("Bearer {token}")));
        }
        let (method, body) = match call.json.take() {
            Some(json) => {
                headers.push(("content-type".to_string(), "application/json".to_string()));
                (MemberMethod::Post, MemberSend::Bytes(Bytes::from(json)))
            }
            None => (MemberMethod::Get, MemberSend::Empty),
        };
        let fetch = MemberFetch {
            method,
            url: rest_url(base, &call.path),
            headers,
            raw_headers: Vec::new(),
            body,
            answer_limit: call.timed.then_some(ANSWER_LIMIT),
            whole_limit: None,
            any_status: call.any_status,
            no_redirect: false,
        };
        self.asked = true;
        let fetch = OutboundEffect::MemberFetch(Box::new(fetch));
        Ok(Next::Call(Effect::Outbound(fetch).into()))
    }
}

impl Operation for RestCallOperation {
    type Output = RestAnswer;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<RestAnswer>, IoError> {
        let urls = NodeUrlsOperation::new(self.call.node_id);
        Ok(Next::Call(IoCall::child(urls)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RestAnswer>, IoError> {
        if !self.asked {
            let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
            return match urls.ok().flatten().and_then(|urls| urls.api) {
                Some(base) => self.fetch(&base),
                None => Ok(Next::Done(RestAnswer::Silent)),
            };
        }
        let answer = match result.event()? {
            Event::Outbound(OutboundEvent::MemberAnswered(answer)) => *answer,
            other => {
                return Err(IoError::Unexpected {
                    expected: "OutboundEvent::MemberAnswered",
                    got: other.label(),
                });
            }
        };
        Ok(Next::Done(match answer {
            MemberAnswer::Body(body) => RestAnswer::Body(body),
            MemberAnswer::Refused { status, body } => RestAnswer::Refused { status, body },
            MemberAnswer::Silent | MemberAnswer::Late | MemberAnswer::Lost => RestAnswer::Silent,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::compute::SecretBytes;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn call() -> RestCall {
        RestCall {
            node_id: node(2),
            path: "/x".to_string(),
            bearer: Some(SharedSecret::new(SecretBytes::new(b"token".to_vec()))),
            relay: Some((node(1), Ulid::from_bytes([3; 16]))),
            headers: Vec::new(),
            json: None,
            timed: true,
            any_status: false,
        }
    }

    fn urls(api: Option<&str>) -> IoResult {
        let urls = NodeUrls {
            api: api.map(str::to_string),
            s3: None,
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, NodeInfoError>(Some(urls))))
    }

    #[test]
    fn urls_keep_nest() {
        assert_eq!(rest_url("https://a/", "/x"), "https://a/api/v1/x");
        assert_eq!(rest_url("https://a/api/v1", "/x"), "https://a/api/v1/x");
    }

    #[test]
    fn unpublished_node_silent() {
        let mut rest = RestCallOperation::new(call());
        rest.first().unwrap();
        assert_eq!(rest.next(urls(None)), Ok(Next::Done(RestAnswer::Silent)));
    }

    #[test]
    fn relay_names_caller() {
        let mut rest = RestCallOperation::new(call());
        rest.first().unwrap();
        let Ok(Next::Call(IoCall::Effect(effect))) = rest.next(urls(Some("http://a"))) else {
            panic!("the request is sent");
        };
        let Effect::Outbound(OutboundEffect::MemberFetch(fetch)) = *effect else {
            panic!("the request is sent");
        };
        assert_eq!(fetch.url, "http://a/api/v1/x");
        let header = |name: &str| fetch.headers.iter().find(|(key, _)| key == name);
        assert!(header(RELAY_HOP_HEADER).is_some() && header(RELAY_SET_HEADER).is_some());
        assert_eq!(header("authorization").unwrap().1, "Bearer token");
    }

    #[test]
    fn late_node_silent() {
        let mut rest = RestCallOperation::new(call());
        rest.asked = true;
        let late = Event::Outbound(OutboundEvent::MemberAnswered(Box::new(MemberAnswer::Late)));
        let answer = rest.next(IoResult::Event(Box::new(late)));
        assert_eq!(answer, Ok(Next::Done(RestAnswer::Silent)));
    }
}
