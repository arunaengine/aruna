//! Tests what a forwarded tool call sends, and what the node's answer means.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::compute::SecretBytes;
use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};

use super::*;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([3; 32])
}

fn call() -> ToolCall {
    ToolCall {
        realm_id: realm(),
        node_id: node(2),
        bearer: SharedSecret::new(SecretBytes::new(b"caller-token".to_vec())),
        tool: "list_objects".to_string(),
        arguments: json!({"bucket": "spire"}),
    }
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let device = RealmNodeKind::User {
        owner: aruna_core::UserId::nil(realm()),
    };
    for (seed, kind) in [(2, RealmNodeKind::Server), (3, device)] {
        config.nodes.push(RealmNode {
            node_id: node(seed).to_string(),
            kind,
        });
    }
    config
}

fn child<T, E>(answer: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(answer))
}

fn reply(status: u16) -> RelayReply {
    RelayReply {
        status,
        headers: Vec::new(),
        body: Bytes::from_static(b"{}"),
    }
}

fn at_reply() -> ToolCallOperation {
    let mut tool = ToolCallOperation::new(call());
    tool.step = ToolStep::Reply;
    tool
}

#[test]
fn devices_never_targets() {
    let config = config();
    assert!(realm_server(&config, node(2)));
    assert!(!realm_server(&config, node(3)));
    assert!(!realm_server(&config, node(4)));
}

#[test]
fn outsider_gets_nothing() {
    let mut outsider = call();
    outsider.node_id = node(3);
    let mut tool = ToolCallOperation::new(outsider);
    tool.first().unwrap();
    let answer = tool.next(child(Ok::<_, GetConfigError>(config())));
    assert_eq!(answer, Err(ToolRefusal::NotServer));
}

#[test]
fn unpublished_node_refused() {
    let mut tool = ToolCallOperation::new(call());
    tool.step = ToolStep::Urls;
    let answer = tool.next(child(Ok::<Option<NodeUrls>, NodeInfoError>(None)));
    assert_eq!(answer, Err(ToolRefusal::NoAddress));
}

#[test]
fn url_drops_nest() {
    assert_eq!(
        mcp_url("https://api.node-2.example/api/v1/"),
        "https://api.node-2.example/mcp"
    );
    assert_eq!(mcp_url("http://127.0.0.1:9"), "http://127.0.0.1:9/mcp");
}

#[test]
fn request_names_node() {
    let mut tool = ToolCallOperation::new(call());
    let sent = tool.request("http://peer/api/v1").unwrap();
    assert_eq!(
        (sent.method, sent.url.as_str()),
        (MemberMethod::Post, "http://peer/mcp")
    );
    assert_eq!(sent.limit, Some(CALL_TIMEOUT));
    assert!(!sent.no_redirect);
    let header = |wanted: &str| {
        let found = sent.headers.iter().find(|(name, _)| name == wanted);
        found.map(|(_, value)| value.clone()).unwrap()
    };
    assert_eq!(header("authorization"), b"Bearer caller-token");
    assert_eq!(header(RELAY_NODE_HEADER), node(2).to_string().into_bytes());
    assert_eq!(header("content-type"), b"application/json");
    let body: Value = serde_json::from_slice(&sent.body).unwrap();
    assert_eq!(body["method"], "tools/call");
    assert_eq!(body["params"]["name"], "list_objects");
    assert_eq!(body["params"]["arguments"]["bucket"], "spire");
    assert_eq!(body["params"]["arguments"]["node_id"], node(2).to_string());
}

#[test]
fn statuses_keep_meaning() {
    let answer = |status| at_reply().next(child(Ok::<_, RelayFailure>(reply(status))));
    assert_eq!(answer(200), Ok(Next::Done(Bytes::from_static(b"{}"))));
    assert_eq!(answer(401), Err(ToolRefusal::Unauthorized));
    assert_eq!(answer(403), Err(ToolRefusal::Forbidden));
    assert_eq!(answer(500), Err(ToolRefusal::Status(500)));
}

#[test]
fn failures_keep_meaning() {
    let answer = |failure| at_reply().next(child(Err::<RelayReply, _>(failure)));
    assert_eq!(answer(RelayFailure::Late), Err(ToolRefusal::Late));
    let text = RelayFailure::TooLong.to_string();
    assert_eq!(
        answer(RelayFailure::TooLong),
        Err(ToolRefusal::Silent(text))
    );
}

#[test]
fn prints_hide_token() {
    let printed = format!("{:?}", ToolCallOperation::new(call()));
    assert!(!printed.contains("caller-token"), "{printed}");
}

fn holder(seed: u8, set: Option<u8>) -> Holder {
    Holder {
        node_id: node(seed),
        set_id: set.map(|set| Ulid::from_parts(1, set.into())),
    }
}

#[test]
fn set_counts_once() {
    let mut holders = vec![holder(1, Some(7))];
    add_holder(&mut holders, holder(2, Some(7)));
    add_holder(&mut holders, holder(3, None));
    add_holder(&mut holders, holder(3, None));
    assert_eq!(holders, [holder(1, Some(7)), holder(3, None)]);
}

#[test]
fn local_stays_here() {
    let here = node(1);
    assert_eq!(pick_node(here, (None, true, None), &[], &[]), Ok(None));
    assert_eq!(
        pick_node(here, (Some(here), false, None), &[], &[]),
        Ok(None)
    );
    // A named node wins over a local bucket of the same name.
    assert_eq!(
        pick_node(here, (Some(node(2)), true, None), &[], &[]),
        Ok(Some(node(2)))
    );
}

#[test]
fn routed_goes_member() {
    // A member without the key's records, here or named, sends the call to the member with them.
    let (here, holder) = (node(1), Some(node(2)));
    assert_eq!(pick_node(here, (None, true, holder), &[], &[]), Ok(holder));
    assert_eq!(
        pick_node(here, (Some(here), true, holder), &[], &[]),
        Ok(holder)
    );
    assert_eq!(
        pick_node(here, (None, true, Some(here)), &[], &[]),
        Ok(None)
    );
}

#[test]
fn search_stops() {
    // Ends at the last page, once a holder is known and nodes failed, and when failed nodes
    // leave nothing new to read, as a cursor would ask them again.
    assert!(search_done(false, (true, false), true));
    assert!(search_done(true, (true, true), false));
    assert!(search_done(false, (false, true), false));
    assert!(!search_done(false, (true, true), false));
    // A page past the name proves nothing for a node that stopped early with a sealed cursor:
    // its own bucket of the same name may still follow, so the search goes on.
    assert!(!search_done(true, (true, false), false));
}

#[test]
fn remote_goes_holder() {
    let here = node(1);
    let one = [holder(2, None)];
    assert_eq!(
        pick_node(here, (None, false, None), &one, &[]),
        Ok(Some(node(2)))
    );
    assert_eq!(
        pick_node(here, (None, false, None), &[], &[]),
        Err(RouteError::Missing)
    );
    // Different buckets of one name on several nodes need the caller's choice.
    let two = [holder(2, None), holder(3, None)];
    let ambiguous = Err(RouteError::Ambiguous(vec![node(2), node(3)]));
    assert_eq!(pick_node(here, (None, false, None), &two, &[]), ambiguous);
    let mixed = [holder(2, Some(7)), holder(3, None)];
    assert!(pick_node(here, (None, false, None), &mixed, &[]).is_err());
}

#[test]
fn distributed_goes_member() {
    let here = node(1);
    let set = [holder(2, Some(7))];
    // The members that keep the key come first; this node never answers for a bucket it lacks.
    let members = [here, node(3), node(2)];
    assert_eq!(
        pick_node(here, (None, false, None), &set, &members),
        Ok(Some(node(3)))
    );
    // Without a record naming the key's members, the member the search found answers.
    assert_eq!(
        pick_node(here, (None, false, None), &set, &[]),
        Ok(Some(node(2)))
    );
}
