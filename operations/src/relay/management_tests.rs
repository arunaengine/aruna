//! Tests relay route matching, target urls and the order a relay tries its targets in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::effects::Effect;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::member_read::MemberAnswer;
use std::collections::BTreeMap;

use super::super::call::tests::{answered, body, job, sent};
use super::*;

const TOKEN_ROUTE: &str = "/access/token";

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn call(method: MemberMethod, route: &'static str, targets: &[&str]) -> ManagementRelayOperation {
    ManagementRelayOperation::new(ManagementCall {
        route,
        method,
        path: "/api/v1/access/token".to_string(),
        query: Some("limit=5".to_string()),
        headers: vec![("authorization".to_string(), b"Bearer token".to_vec())],
        body: Bytes::from_static(b"sent"),
        targets: targets.iter().map(|target| target.to_string()).collect(),
    })
}

fn mark(target: &str, failed: bool) -> TargetMark {
    let target = target.to_string();
    TargetMark { target, failed }
}

fn reply_of(status: u16) -> IoResult {
    answered(MemberAnswer::Body(body(status, &[])))
}

fn is_read<T>(next: Result<Next<T>, IoError>) -> bool {
    let Ok(Next::Call(IoCall::Effect(effect))) = next else {
        return false;
    };
    matches!(*effect, Effect::Job(JobEffect::Collect(_)))
}

fn done(next: RelayTurn) -> RelayOutcome {
    let Ok(Next::Done(outcome)) = next else {
        panic!("the relay ended");
    };
    outcome
}

#[test]
fn matches_allowlisted_routes() {
    let route = |method, path| relay_route(method, Some(path), false);
    assert_eq!(
        route("PUT", "/api/v1/system/realm/quota"),
        Some("/system/realm/quota")
    );
    assert_eq!(
        route("DELETE", "/api/v1/access/onboarding/secrets/{id}"),
        Some("/access/onboarding/secrets/{id}")
    );
    assert_eq!(route("GET", "/api/v1/access/token"), Some("/access/token"));
    assert_eq!(
        route("POST", "/api/v1/access/sessions"),
        Some("/access/sessions")
    );
    // Node-local admin routes stay on the node they were called on.
    assert_eq!(route("POST", "/api/v1/compute/drain"), None);
    assert_eq!(route("GET", "/api/v1/data/placement/diagnostics"), None);
    // The method is part of the match: only PUT on the quota route relays.
    assert_eq!(route("GET", "/api/v1/system/realm/quota"), None);
}

#[test]
fn central_reaches_holder() {
    let proxy = Some("/api/v1/system/assistant/providers/{id}/proxy/{*path}");
    let central = "/api/v1/system/assistant/providers/central/proxy/v1/messages";
    let own = "/api/v1/system/assistant/providers/01JCNCTR0123456789ABCDEFGH/proxy/v1/messages";
    assert!(holder_route("POST", proxy, central));
    // A user's own provider stays on the node that stored it.
    assert!(!holder_route("POST", proxy, own));
    let setup = "/api/v1/system/assistant/central";
    assert!(holder_route("PUT", Some(setup), setup));
    assert!(!holder_route("PATCH", Some(setup), setup));
}

#[test]
fn merge_reaches_coordinator() {
    // Every node, a management node too, passes a merge on to the coordinator.
    let route = Some("/api/v1/access/users/me/merge");
    assert_eq!(
        coordinated_route("POST", route),
        Some("/access/users/me/merge")
    );
    assert_eq!(
        coordinated_route("PUT", Some("/api/v1/system/realm/merge-coordinator")),
        Some("/system/realm/merge-coordinator")
    );
    assert!(
        !RELAYED_ROUTES
            .iter()
            .any(|(_, path)| *path == "/access/users/me/merge")
    );
    // Attaching a sign-in runs where merges run, so it cannot race one on another node.
    let attach = Some("/api/v1/access/users/me/subjects");
    assert_eq!(
        coordinated_route("POST", attach),
        Some("/access/users/me/subjects")
    );
    assert_eq!(relay_route("POST", attach, false), None);
}

#[test]
fn relays_user_registration() {
    // A device has no subject index for realm users; a local register mints a stray user.
    let route = Some("/api/v1/access/users/register");
    assert_eq!(
        relay_route("POST", route, false),
        Some("/access/users/register")
    );
    assert_eq!(relay_route("POST", route, true), None);
}

#[test]
fn management_answers_itself() {
    let route = Some("/api/v1/system/realm/quota");
    assert_eq!(relay_route("PUT", route, true), None);
}

#[test]
fn builds_target_url() {
    // Node info documents publish `API_PUBLIC_URL` as a bare origin.
    let (path, query) = ("/api/v1/access/onboarding/secrets", Some("limit=5"));
    assert_eq!(
        target_url("http://127.0.0.1:43001", path, query),
        "http://127.0.0.1:43001/api/v1/access/onboarding/secrets?limit=5"
    );
    assert_eq!(
        target_url("https://mgmt.example.test/api/v1/", path, query),
        "https://mgmt.example.test/api/v1/access/onboarding/secrets?limit=5"
    );
}

#[test]
fn hops_stop_circles() {
    let (own, target) = (node(1), node(2));
    assert_eq!(next_hops(own, "", None), Some(own.to_string()));
    assert_eq!(next_hops(own, "peer", None), Some(format!("peer,{own}")));
    assert_eq!(next_hops(own, &format!("peer, {own}"), None), None);
    assert_eq!(next_hops(own, &target.to_string(), Some(target)), None);
}

#[test]
fn retry_rules_hold() {
    assert!(may_try_next(MemberMethod::Post, true));
    assert!(may_try_next(MemberMethod::Get, true));
    assert!(may_try_next(MemberMethod::Get, false));
    // A failure after the connect succeeded may already have minted a secret.
    assert!(!may_try_next(MemberMethod::Post, false));
    assert!(!may_try_next(MemberMethod::Delete, false));
    assert!(!gateway_failed(MemberMethod::Post, 503));
    assert!(gateway_failed(MemberMethod::Get, 504));
    assert!(!gateway_failed(MemberMethod::Get, 500));
}

#[test]
fn enrollment_miss_advances() {
    assert!(may_try_response("/access/onboarding/secrets/{id}", 404));
    assert!(may_try_response(
        "/access/onboarding/secrets/{id}/status",
        404
    ));
    assert!(may_try_response("/access/users/me/devices/{id}", 404));
    assert!(may_try_response("/access/onboarding/bootstrap", 401));
    assert!(!may_try_response("/access/onboarding/secrets", 404));
}

#[test]
fn promotion_fits_relay() {
    // A relayed promotion answers, ready or not, well before the relay gives up on it.
    let margin = Duration::from_secs(5);
    let budget = crate::realm::promote_node::CALL_BUDGET;
    assert!(budget + margin <= RELAY_TIMEOUT);
}

#[test]
fn stream_keeps_headers() {
    let answer = body(
        200,
        &[("cache-control", "no-store"), ("set-cookie", "secret")],
    );
    let kept = [("cache-control".to_string(), "no-store".to_string())];
    assert_eq!(stream_headers(&answer), kept);
}

#[test]
fn gateway_moves_get() {
    let mut relay = call(
        MemberMethod::Get,
        TOKEN_ROUTE,
        &["http://a", "http://b/api/v1"],
    );
    let fetch = sent(relay.first());
    assert_eq!(fetch.url, "http://a/api/v1/access/token?limit=5");
    assert_eq!(fetch.whole_limit, Some(RELAY_TIMEOUT));
    assert_eq!(fetch.raw_headers[0].1, b"Bearer token");
    let fetch = sent(relay.next(reply_of(503)));
    assert_eq!(fetch.url, "http://b/api/v1/access/token?limit=5");
    assert!(is_read(relay.next(answered(MemberAnswer::Body(body(
        200,
        &[("etag", "\"e\""), ("set-cookie", "secret")],
    ))))));
    let read = job(JobEvent::Collected(Ok(b"token".to_vec())));
    let outcome = done(relay.next(read));
    assert_eq!(
        outcome.marks,
        [mark("http://a", true), mark("http://b/api/v1", false)]
    );
    let RelayEnd::Reply(reply) = outcome.end else {
        panic!("the second target answered");
    };
    assert_eq!(&reply.body[..], b"token");
    assert_eq!(reply.headers, [("etag".to_string(), "\"e\"".to_string())]);
}

#[test]
fn gateway_ends_post() {
    let mut relay = call(MemberMethod::Post, TOKEN_ROUTE, &["http://a", "http://b"]);
    relay.first().unwrap();
    // A POST takes the gateway's answer, but the target still counts as failing.
    assert!(is_read(relay.next(reply_of(503))));
    let outcome = done(relay.next(job(JobEvent::Collected(Ok(Vec::new())))));
    assert_eq!(outcome.marks, [mark("http://a", true)]);
    assert!(matches!(outcome.end, RelayEnd::Reply(reply) if reply.status == 503));
}

#[test]
fn connect_failure_advances() {
    let mut relay = call(MemberMethod::Post, TOKEN_ROUTE, &["http://a", "http://b"]);
    relay.first().unwrap();
    let fetch = sent(relay.next(answered(MemberAnswer::Silent)));
    assert!(fetch.url.starts_with("http://b/"));
    let outcome = done(relay.next(answered(MemberAnswer::Silent)));
    assert_eq!(outcome.end, RelayEnd::NoNode);
    assert_eq!(
        outcome.marks,
        [mark("http://a", true), mark("http://b", true)]
    );
}

#[test]
fn lost_post_stops() {
    for answer in [MemberAnswer::Lost, MemberAnswer::Late] {
        let mut relay = call(MemberMethod::Post, TOKEN_ROUTE, &["http://a", "http://b"]);
        relay.first().unwrap();
        let outcome = done(relay.next(answered(answer)));
        assert_eq!(outcome.end, RelayEnd::Failed);
        assert_eq!(outcome.marks, [mark("http://a", false)]);
    }
}

#[test]
fn lost_get_advances() {
    let mut relay = call(MemberMethod::Get, TOKEN_ROUTE, &["http://a", "http://b"]);
    relay.first().unwrap();
    let fetch = sent(relay.next(answered(MemberAnswer::Lost)));
    assert!(fetch.url.starts_with("http://b/"));
}

#[test]
fn three_targets_tried() {
    let targets = ["http://a", "http://b", "http://c", "http://d"];
    let mut relay = call(MemberMethod::Get, TOKEN_ROUTE, &targets);
    relay.first().unwrap();
    sent(relay.next(answered(MemberAnswer::Silent)));
    sent(relay.next(answered(MemberAnswer::Silent)));
    let outcome = done(relay.next(answered(MemberAnswer::Silent)));
    assert_eq!(outcome.end, RelayEnd::NoNode);
    assert_eq!(outcome.marks.len(), 3);
}

#[test]
fn last_miss_answers() {
    let route = "/access/onboarding/bootstrap";
    let mut relay = call(MemberMethod::Post, route, &["http://a", "http://b"]);
    relay.first().unwrap();
    sent(relay.next(reply_of(401)));
    // No target knows the secret, so the last refusal is the answer.
    assert!(is_read(relay.next(reply_of(401))));
    let outcome = done(relay.next(job(JobEvent::Collected(Ok(Vec::new())))));
    assert!(matches!(outcome.end, RelayEnd::Reply(reply) if reply.status == 401));
}

#[test]
fn truncated_reply_fails() {
    let mut relay = call(MemberMethod::Get, TOKEN_ROUTE, &["http://a", "http://b"]);
    relay.first().unwrap();
    relay.next(reply_of(200)).unwrap();
    let cut = job(JobEvent::Collected(Err("cut".to_string())));
    assert_eq!(done(relay.next(cut)).end, RelayEnd::Failed);
}

#[test]
fn prints_hide_credentials() {
    let printed = format!("{:?}", call(MemberMethod::Get, TOKEN_ROUTE, &["http://a"]));
    assert!(!printed.contains("Bearer"), "{printed}");
}

fn relay_to(passed: &str) -> NodeRelayOperation {
    NodeRelayOperation::new(NodeRelay {
        realm_id: RealmId::from_bytes([1; 32]),
        local: node(1),
        node: node(2),
        method: MemberMethod::Get,
        path: "/api/v1/plugin-runs".to_string(),
        query: None,
        passed: passed.to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    })
}

fn known(urls: &[(u8, &str)]) -> IoResult {
    let realm_id = RealmId::from_bytes([1; 32]);
    let config = aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
        realm_id,
        Vec::new(),
    );
    let urls: BTreeMap<NodeId, String> = (urls.iter())
        .map(|(seed, url)| (node(*seed), url.to_string()))
        .collect();
    let nodes = RealmNodes { config, urls };
    IoResult::Child(ChildOutcome::new(Ok::<_, TargetError>(nodes)))
}

#[test]
fn named_node_checked() {
    let mut relay = relay_to("");
    relay.first().unwrap();
    assert_eq!(
        relay.next(known(&[])),
        Ok(Next::Done(NodeRelayEnd::NoAddress))
    );
    let mut circling = relay_to(&node(2).to_string());
    circling.first().unwrap();
    let answer = circling.next(known(&[(2, "http://two")]));
    assert_eq!(answer, Ok(Next::Done(NodeRelayEnd::Circling)));
    let mut unread = relay_to("");
    unread.first().unwrap();
    let failed = IoResult::Child(ChildOutcome::new(Err::<RealmNodes, _>(TargetError::Config)));
    assert_eq!(unread.next(failed), Ok(Next::Done(NodeRelayEnd::NoConfig)));
}

#[test]
fn named_node_answers() {
    let mut relay = relay_to("");
    relay.first().unwrap();
    assert!(matches!(
        relay.next(known(&[(2, "http://two")])),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let lost = IoResult::Child(ChildOutcome::new(Err::<RelayReply, _>(RelayFailure::Lost)));
    assert_eq!(relay.next(lost), Ok(Next::Done(NodeRelayEnd::Failed)));
}
