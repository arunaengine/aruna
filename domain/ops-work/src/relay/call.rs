//! One request a relay sends to another node, and the answer it reads back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::member_read::{MemberAnswer, MemberBody, MemberFetch, MemberMethod, MemberSend};
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutboundEffect, OutboundEvent};
use bytes::Bytes;
use thiserror::Error;

/// Bytes of a node's answer read at most where the answer is capped.
pub const MAX_REPLY_BYTES: u64 = 16 * 1024 * 1024;

/// A header as a relay received it, its value byte for byte.
pub type RawHeader = (String, Vec<u8>);

/// One request to a node. Never shown in a `Debug` print, as it holds the caller's credentials.
#[derive(Clone, PartialEq)]
pub struct NodeCall {
    pub method: MemberMethod,
    pub url: String,
    pub headers: Vec<RawHeader>,
    pub body: Bytes,
    /// How long the whole exchange may take, the answer's body included.
    pub limit: Option<Duration>,
    pub no_redirect: bool,
}

impl std::fmt::Debug for NodeCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeCall")
    }
}

impl NodeCall {
    pub(crate) fn fetch(self) -> IoCall {
        let fetch = MemberFetch {
            method: self.method,
            url: self.url,
            headers: Vec::new(),
            raw_headers: self.headers,
            body: MemberSend::Bytes(self.body),
            answer_limit: None,
            whole_limit: self.limit,
            any_status: true,
            no_redirect: self.no_redirect,
        };
        Effect::Outbound(OutboundEffect::MemberFetch(Box::new(fetch))).into()
    }
}

/// A node's whole answer. Its headers and body may hold secrets, so `Debug` shows the status.
#[derive(Clone, PartialEq)]
pub struct RelayReply {
    pub status: u16,
    /// Header names in lowercase.
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl std::fmt::Debug for RelayReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut print = f.debug_struct("RelayReply");
        print.field("status", &self.status).finish_non_exhaustive()
    }
}

impl RelayReply {
    /// This answer with only the headers `names`.
    pub(crate) fn keeping(mut self, names: &[&str]) -> Self {
        self.headers
            .retain(|(name, _)| names.contains(&name.as_str()));
        self
    }
}

/// Why a relayed request brought no whole answer.
#[derive(Debug, Error, PartialEq)]
pub enum RelayFailure {
    #[error("the node cannot be reached")]
    Silent,
    #[error("the node did not answer in time")]
    Late,
    #[error("the request broke off")]
    Lost,
    #[error("the answer broke off: {0}")]
    Broken(String),
    #[error("the answer exceeds {MAX_REPLY_BYTES} bytes")]
    TooLong,
    #[error(transparent)]
    Io(#[from] IoError),
}

/// The body a node started to answer with, or why there is none.
pub(crate) fn read_answer(result: IoResult) -> Result<MemberBody, RelayFailure> {
    let answer = match result.event()? {
        Event::Outbound(OutboundEvent::MemberAnswered(answer)) => *answer,
        other => {
            return Err(IoError::Unexpected {
                expected: "OutboundEvent::MemberAnswered",
                got: other.label(),
            }
            .into());
        }
    };
    match answer {
        MemberAnswer::Body(body) => Ok(body),
        MemberAnswer::Silent => Err(RelayFailure::Silent),
        MemberAnswer::Late => Err(RelayFailure::Late),
        // A relay asks for every status as a body, so a refusal is no answer it can pass on.
        MemberAnswer::Refused { .. } | MemberAnswer::Lost => Err(RelayFailure::Lost),
    }
}

/// The answer's status and headers, and the call that reads its body, within `cap` if any.
pub(crate) fn start_read(body: MemberBody, cap: Option<u64>) -> (RelayReply, IoCall) {
    let read = match cap {
        Some(limit) => JobEffect::CollectUpTo {
            stream: body.stream,
            limit,
        },
        None => JobEffect::Collect(body.stream),
    };
    let head = RelayReply {
        status: body.status,
        headers: body.headers,
        body: Bytes::new(),
    };
    (head, Effect::Job(read).into())
}

/// `head` with the body the call of [`start_read`] collected.
pub(crate) fn finish_read(
    mut head: RelayReply,
    result: IoResult,
) -> Result<RelayReply, RelayFailure> {
    let bytes = match result.event()? {
        Event::Job(JobEvent::Collected(bytes)) => bytes.map_err(RelayFailure::Broken)?,
        Event::Job(JobEvent::CollectedUpTo(bytes)) => bytes
            .map_err(RelayFailure::Broken)?
            .ok_or(RelayFailure::TooLong)?,
        other => {
            return Err(IoError::Unexpected {
                expected: "JobEvent::Collected",
                got: other.label(),
            }
            .into());
        }
    };
    head.body = Bytes::from(bytes);
    Ok(head)
}

/// Sends one request and hands back the answer as soon as it starts, its body still streaming.
#[derive(Debug, PartialEq)]
pub struct StartCallOperation {
    call: Option<NodeCall>,
}

impl StartCallOperation {
    pub fn new(call: NodeCall) -> Self {
        Self { call: Some(call) }
    }
}

impl Operation for StartCallOperation {
    type Output = MemberBody;
    type Error = RelayFailure;

    fn first(&mut self) -> Result<Next<MemberBody>, RelayFailure> {
        let call = self.call.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(call.fetch()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MemberBody>, RelayFailure> {
        read_answer(result).map(Next::Done)
    }
}

/// Sends one request and reads the whole answer, within `cap` bytes if any.
#[derive(Debug, PartialEq)]
pub struct NodeCallOperation {
    call: Option<NodeCall>,
    cap: Option<u64>,
    head: Option<RelayReply>,
}

impl NodeCallOperation {
    pub fn new(call: NodeCall, cap: Option<u64>) -> Self {
        Self {
            call: Some(call),
            cap,
            head: None,
        }
    }
}

impl Operation for NodeCallOperation {
    type Output = RelayReply;
    type Error = RelayFailure;

    fn first(&mut self) -> Result<Next<RelayReply>, RelayFailure> {
        let call = self.call.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(call.fetch()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RelayReply>, RelayFailure> {
        match self.head.take() {
            Some(head) => finish_read(head, result).map(Next::Done),
            None => {
                let (head, read) = start_read(read_answer(result)?, self.cap);
                self.head = Some(head);
                Ok(Next::Call(read))
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use aruna_core::stream::BackendStream;

    use super::*;

    pub(in crate::relay) fn call(url: &str) -> NodeCall {
        NodeCall {
            method: MemberMethod::Post,
            url: url.to_string(),
            headers: vec![("authorization".to_string(), b"Bearer secret".to_vec())],
            body: Bytes::from_static(b"sent"),
            limit: Some(Duration::from_secs(30)),
            no_redirect: true,
        }
    }

    pub(in crate::relay) fn body(status: u16, headers: &[(&str, &str)]) -> MemberBody {
        let chunks = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
        MemberBody {
            status,
            headers: (headers.iter())
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            stream: BackendStream::new(chunks),
        }
    }

    pub(in crate::relay) fn answered(answer: MemberAnswer) -> IoResult {
        let answer = OutboundEvent::MemberAnswered(Box::new(answer));
        IoResult::Event(Box::new(Event::Outbound(answer)))
    }

    pub(in crate::relay) fn job(event: JobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Job(event)))
    }

    pub(in crate::relay) fn sent<T>(next: Result<Next<T>, impl std::fmt::Debug>) -> MemberFetch {
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("a request is sent");
        };
        let Effect::Outbound(OutboundEffect::MemberFetch(fetch)) = *effect else {
            panic!("the call is a member request");
        };
        *fetch
    }

    #[test]
    fn request_carries_all() {
        let mut start = StartCallOperation::new(call("http://a/x"));
        let fetch = sent(start.first());
        assert_eq!(fetch.method, MemberMethod::Post);
        assert_eq!(fetch.url, "http://a/x");
        assert!(fetch.headers.is_empty());
        assert_eq!(fetch.raw_headers[0].1, b"Bearer secret");
        assert_eq!(fetch.body, MemberSend::Bytes(Bytes::from_static(b"sent")));
        assert_eq!(fetch.answer_limit, None);
        assert_eq!(fetch.whole_limit, Some(Duration::from_secs(30)));
        assert!(fetch.any_status && fetch.no_redirect);
    }

    #[test]
    fn answers_name_failure() {
        let failures = [
            (MemberAnswer::Silent, RelayFailure::Silent),
            (MemberAnswer::Late, RelayFailure::Late),
            (MemberAnswer::Lost, RelayFailure::Lost),
        ];
        for (answer, failure) in failures {
            let mut start = StartCallOperation::new(call("http://a"));
            start.first().unwrap();
            assert_eq!(start.next(answered(answer)), Err(failure));
        }
    }

    #[test]
    fn capped_reply_reads() {
        let mut node = NodeCallOperation::new(call("http://a"), Some(MAX_REPLY_BYTES));
        node.first().unwrap();
        let answer = MemberAnswer::Body(body(201, &[("etag", "\"e\"")]));
        let Ok(Next::Call(IoCall::Effect(effect))) = node.next(answered(answer)) else {
            panic!("the body is read");
        };
        let Effect::Job(JobEffect::CollectUpTo { limit, .. }) = *effect else {
            panic!("the read is capped");
        };
        assert_eq!(limit, MAX_REPLY_BYTES);
        let read = JobEvent::CollectedUpTo(Ok(Some(b"done".to_vec())));
        let Ok(Next::Done(reply)) = node.next(job(read)) else {
            panic!("the reply is whole");
        };
        assert_eq!((reply.status, &reply.body[..]), (201, &b"done"[..]));
        assert_eq!(reply.headers[0].0, "etag");
    }

    #[test]
    fn long_reply_refused() {
        let mut node = NodeCallOperation::new(call("http://a"), Some(1));
        node.first().unwrap();
        node.next(answered(MemberAnswer::Body(body(200, &[]))))
            .unwrap();
        let read = JobEvent::CollectedUpTo(Ok(None));
        assert_eq!(node.next(job(read)), Err(RelayFailure::TooLong));
    }

    #[test]
    fn uncapped_reply_breaks() {
        let mut node = NodeCallOperation::new(call("http://a"), None);
        node.first().unwrap();
        let next = node.next(answered(MemberAnswer::Body(body(200, &[]))));
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("the body is read");
        };
        assert!(matches!(*effect, Effect::Job(JobEffect::Collect(_))));
        let read = JobEvent::Collected(Err("cut".to_string()));
        let broken = RelayFailure::Broken("cut".to_string());
        assert_eq!(node.next(job(read)), Err(broken));
    }

    #[test]
    fn prints_hide_secrets() {
        let reply = RelayReply {
            status: 200,
            headers: vec![("set-cookie".to_string(), "secret".to_string())],
            body: Bytes::from_static(b"secret"),
        };
        let printed = format!("{:?} {:?}", call("http://secret"), reply);
        assert!(!printed.contains("secret"), "{printed}");
    }
}
