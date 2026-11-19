//! Relays a plugin webhook to the install's receiver with its raw bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::member_read::MemberMethod;
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::node_info::NodeUrls;
use bytes::Bytes;
use tracing::warn;
use ulid::Ulid;

use super::call::{
    MAX_REPLY_BYTES, NodeCall, NodeCallOperation, RawHeader, RelayFailure, RelayReply,
};
use super::management::next_hops;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::plugins::dispatch::HookAuthorization;
use crate::s3::object::member_rest::{RELAY_HOP_HEADER, rest_url};

const HOOK_TIMEOUT: Duration = Duration::from_secs(30);
/// The answer headers a webhook provider gets back.
const REPLY_HEADERS: &[&str] = &["retry-after", "content-type"];

/// Headers a relay never passes on: hop-by-hop ones and credentials no check reads.
const NOT_RELAYED: &[&str] = &[
    "connection",
    "content-length",
    "cookie",
    "host",
    "keep-alive",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// The headers a relay passes on: all but hop-by-hop ones, `cookie` and `proxy-authorization`,
/// and `authorization` only when the verify secret checks it.
pub fn hook_headers(received: Vec<RawHeader>, keep_authorization: bool) -> Vec<RawHeader> {
    let passes = |name: &str| {
        !NOT_RELAYED.contains(&name)
            && name != RELAY_HOP_HEADER
            && (keep_authorization || name != "authorization")
    };
    let mut sent = received;
    sent.retain(|(name, _)| passes(name));
    sent
}

/// A webhook to relay. Never shown in a `Debug` print, as it holds the sender's credentials.
#[derive(PartialEq)]
pub struct HookRelay {
    pub realm_id: RealmId,
    pub local: NodeId,
    pub receiver: NodeId,
    pub install_id: Ulid,
    pub path: String,
    /// The hop header as received, empty when there was none.
    pub passed: String,
    pub headers: Vec<RawHeader>,
    pub body: Bytes,
}

impl std::fmt::Debug for HookRelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HookRelay")
    }
}

#[derive(Debug, PartialEq)]
enum HookStep {
    Config,
    Check,
    Urls { keep: bool },
    Reply,
}

/// Sends a webhook on to its receiver. `None` when it cannot go there now: the call would circle,
/// whether `authorization` may pass cannot be told, or the receiver gave no whole answer.
#[derive(Debug, PartialEq)]
pub struct HookRelayOperation {
    relay: HookRelay,
    hops: String,
    step: HookStep,
}

impl HookRelayOperation {
    pub fn new(relay: HookRelay) -> Self {
        Self {
            relay,
            hops: String::new(),
            step: HookStep::Config,
        }
    }

    fn request(&mut self, base: &str, keep: bool) -> NodeCall {
        let relay = &mut self.relay;
        let mut headers = hook_headers(std::mem::take(&mut relay.headers), keep);
        let hops = std::mem::take(&mut self.hops).into_bytes();
        headers.push((RELAY_HOP_HEADER.to_string(), hops));
        NodeCall {
            method: MemberMethod::Post,
            url: rest_url(base, &relay.path),
            headers,
            body: std::mem::take(&mut relay.body),
            limit: Some(HOOK_TIMEOUT),
            no_redirect: true,
        }
    }
}

impl Operation for HookRelayOperation {
    type Output = Option<RelayReply>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let relay = &self.relay;
        let Some(hops) = next_hops(relay.local, &relay.passed, Some(relay.receiver)) else {
            return Ok(Next::Done(None));
        };
        self.hops = hops;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            relay.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match std::mem::replace(&mut self.step, HookStep::Reply) {
            HookStep::Config => {
                let Ok(config) = result.child::<RealmConfigDocument, GetConfigError>()? else {
                    return Ok(Next::Done(None));
                };
                self.step = HookStep::Check;
                let place = (self.relay.local, self.relay.install_id);
                Ok(Next::Call(IoCall::child(HookAuthorization::new(
                    config, place,
                ))))
            }
            HookStep::Check => {
                // Without knowing whether the check needs `authorization`, nothing is forwarded.
                let Ok(Some(keep)) = result.child::<Option<bool>, IoError>()? else {
                    return Ok(Next::Done(None));
                };
                self.step = HookStep::Urls { keep };
                let urls = NodeUrlsOperation::new(self.relay.receiver);
                Ok(Next::Call(IoCall::child(urls)))
            }
            HookStep::Urls { keep } => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                let Some(base) = urls.ok().flatten().and_then(|urls| urls.api) else {
                    return Ok(Next::Done(None));
                };
                let call = self.request(&base, keep);
                let call = NodeCallOperation::new(call, Some(MAX_REPLY_BYTES));
                Ok(Next::Call(IoCall::child(call)))
            }
            HookStep::Reply => match result.child::<RelayReply, RelayFailure>()? {
                Ok(reply) => Ok(Next::Done(Some(reply.keeping(REPLY_HEADERS)))),
                Err(error) => {
                    let receiver = self.relay.receiver;
                    warn!(%receiver, error = %error, "A plugin webhook relay failed");
                    Ok(Next::Done(None))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn header(name: &str, value: &str) -> RawHeader {
        (name.to_string(), value.as_bytes().to_vec())
    }

    fn relay(passed: &str) -> HookRelay {
        HookRelay {
            realm_id: RealmId::from_bytes([1; 32]),
            local: node(1),
            receiver: node(2),
            install_id: Ulid::from_bytes([3; 16]),
            path: "/plugin-hooks/x".to_string(),
            passed: passed.to_string(),
            headers: vec![
                header("authorization", "Bearer canary-auth"),
                header("x-delivery", "d-1"),
            ],
            body: Bytes::from_static(b"event"),
        }
    }

    fn child<T, E>(answer: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(answer))
    }

    #[test]
    fn relay_drops_credentials() {
        let received = vec![
            header("cookie", "canary-cookie"),
            header("proxy-authorization", "Basic canary-proxy"),
            header("authorization", "Bearer canary-auth"),
            header("connection", "close"),
            header("x-aruna-relay-hop", "spoofed"),
            header("x-hub-signature-256", "sha256=ab"),
            header("x-delivery", "d-1"),
            header("content-type", "application/json"),
        ];
        let sent = hook_headers(received.clone(), false);
        let names: Vec<&str> = sent.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["x-hub-signature-256", "x-delivery", "content-type"]);
        let checked = hook_headers(received, true);
        let kept = checked.iter().find(|(name, _)| name == "authorization");
        assert_eq!(kept.unwrap().1, b"Bearer canary-auth");
        assert!(!checked.iter().any(|(name, _)| name == "cookie"));
    }

    #[test]
    fn circling_hook_stops() {
        let passed = node(2).to_string();
        let mut hook = HookRelayOperation::new(relay(&passed));
        assert_eq!(hook.first(), Ok(Next::Done(None)));
    }

    #[test]
    fn unknown_check_stops() {
        let mut hook = HookRelayOperation::new(relay(""));
        hook.first().unwrap();
        hook.step = HookStep::Check;
        assert_eq!(
            hook.next(child(Ok::<Option<bool>, IoError>(None))),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn request_goes_direct() {
        let mut hook = HookRelayOperation::new(relay(""));
        hook.first().unwrap();
        let call = hook.request("http://receiver", false);
        assert_eq!(call.url, "http://receiver/api/v1/plugin-hooks/x");
        assert_eq!(call.method, MemberMethod::Post);
        assert_eq!(call.limit, Some(HOOK_TIMEOUT));
        assert!(call.no_redirect);
        assert_eq!(&call.body[..], b"event");
        let names: Vec<&str> = call.headers.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["x-delivery", RELAY_HOP_HEADER]);
        assert_eq!(call.headers[1].1, node(1).to_string().into_bytes());
    }

    #[test]
    fn reply_keeps_headers() {
        let mut hook = HookRelayOperation::new(relay(""));
        hook.step = HookStep::Reply;
        let reply = RelayReply {
            status: 429,
            headers: vec![
                ("retry-after".to_string(), "7".to_string()),
                ("set-cookie".to_string(), "secret".to_string()),
            ],
            body: Bytes::new(),
        };
        let Ok(Next::Done(Some(kept))) = hook.next(child(Ok::<_, RelayFailure>(reply))) else {
            panic!("the reply passes");
        };
        assert_eq!(kept.status, 429);
        assert_eq!(kept.headers, [("retry-after".to_string(), "7".to_string())]);
    }

    #[test]
    fn failed_send_unavailable() {
        let mut hook = HookRelayOperation::new(relay(""));
        hook.step = HookStep::Reply;
        let failed = child(Err::<RelayReply, _>(RelayFailure::Lost));
        assert_eq!(hook.next(failed), Ok(Next::Done(None)));
    }
}
