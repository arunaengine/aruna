//! Tests where a data tool call runs and how a forwarded call's answer comes back.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{ListObjectsInput, ObjectsOutput};
use crate::tests::distributed::{
    BUCKET, CLOSED_API, LOCAL, PEER, distribute, mock_peer, node_id, node_state, publish_api,
};
use axum::http::StatusCode;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn holder(seed: u8, set: Option<u8>) -> Holder {
    Holder {
        node_id: node(seed),
        set_id: set.map(|set| Ulid::from_parts(1, set.into())),
    }
}

fn body(result: CallToolResult) -> Value {
    result
        .structured_content
        .expect("a tool error carries the structured body")
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

// The record of a distributed bucket names the member for each key: this node below `m`, the
// peer from `m` on, so a node outside the set sends each key to its own member.
#[tokio::test]
async fn record_picks_member() {
    let fixture = node_state().await;
    let snapshot = distribute(&fixture.state, fixture.group_id, BUCKET).await;
    let set = [Holder {
        node_id: node_id(LOCAL),
        set_id: Some(snapshot.set_id()),
    }];
    let context = fixture.state.get_ctx();
    let outside = node(9);
    for (key, member) in [("a/x", LOCAL), ("z/y", PEER)] {
        let members = key_holders(&context, snapshot.set_id(), key).await;
        let picked = pick_node(outside, (None, false, None), &set, &members);
        assert_eq!(picked, Ok(Some(node_id(member))), "{key}");
    }
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
fn answers_keep_code() {
    let peer = node(2);
    let refused = json!({"jsonrpc": "2.0", "id": 1, "result": {
        "content": [{"type": "text", "text": "Not found: no such key"}],
        "structuredContent": {"code": "Not found", "error": "Not found: no such key"},
        "isError": true,
    }});
    let error = answer::<Value>(peer, refused.to_string().as_bytes()).unwrap_err();
    let error = body(error);
    assert_eq!(error["code"], "Not found");
    let text = error["error"].as_str().unwrap_or_default();
    assert!(
        text.ends_with(&format!("(answered by node {peer})")),
        "{text}"
    );
    let failed = json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "bad"}});
    let error = answer::<Value>(peer, failed.to_string().as_bytes()).unwrap_err();
    assert_eq!(body(error)["code"], "Bad gateway");
    let done = json!({"jsonrpc": "2.0", "id": 1, "result": {
        "content": [], "structuredContent": {"bucket": "b"},
    }});
    let output = answer::<Value>(peer, done.to_string().as_bytes()).unwrap();
    assert_eq!(output["bucket"], "b");
}

fn caller(fixture: &crate::tests::distributed::Node, bearer: bool) -> http::request::Parts {
    let (mut parts, ()) = http::Request::new(()).into_parts();
    parts.extensions.insert(Some(fixture.owner.clone()));
    let token = bearer.then(|| crate::auth::ValidatedBearer::new_for_test("caller-token"));
    parts.extensions.insert(token);
    parts
}

fn listing() -> ListObjectsInput {
    ListObjectsInput {
        bucket: "elsewhere".to_string(),
        prefix: None,
        cursor: None,
        limit: None,
        node_id: None,
    }
}

// A call goes to the node's /mcp with the caller's token, and the node's answer comes back as
// is; without a token, or from a node that does not answer, it fails naming that node.
#[tokio::test]
async fn forwards_as_caller() {
    let fixture = node_state().await;
    let server = McpServer::new(fixture.state.clone());
    let peer = node_id(PEER);
    let reply = json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [],
        "structuredContent": {"bucket": "elsewhere", "objects": [], "next_cursor": null,
            "node_id": peer.to_string()}}});
    let mock = mock_peer(StatusCode::OK, &reply.to_string()).await;
    publish_api(&fixture.state, peer, &format!("{}/api/v1", mock.url)).await;
    let (signed, unsigned, input) = (caller(&fixture, true), caller(&fixture, false), listing());
    let forwarded = server.forward::<ObjectsOutput>(&signed, peer, "list_objects", &input);
    assert_eq!(forwarded.await.unwrap().node_id, peer.to_string());
    assert_eq!(*mock.paths.lock().unwrap(), ["/mcp"]);
    let refused = server.forward::<ObjectsOutput>(&unsigned, peer, "list_objects", &input);
    assert_eq!(body(refused.await.unwrap_err())["code"], "Not authorized");
    publish_api(&fixture.state, peer, CLOSED_API).await;
    let silent = server.forward::<ObjectsOutput>(&signed, peer, "list_objects", &input);
    let silent = body(silent.await.unwrap_err());
    assert_eq!(silent["code"], "Service unavailable");
    let text = silent["error"].as_str().unwrap_or_default();
    assert!(text.contains(&peer.to_string()), "{text}");
}

// The list tool sends a call naming another node there, whatever this node holds.
#[tokio::test]
async fn tool_follows_node() {
    use rmcp::handler::server::tool::Extension;
    use rmcp::handler::server::wrapper::Parameters;
    let fixture = node_state().await;
    let server = McpServer::new(fixture.state.clone());
    let peer = node_id(PEER);
    let reply = json!({"jsonrpc": "2.0", "id": 1, "result": {"content": [],
        "structuredContent": {"bucket": BUCKET, "objects": [], "next_cursor": null,
            "node_id": peer.to_string()}}});
    let mock = mock_peer(StatusCode::OK, &reply.to_string()).await;
    publish_api(&fixture.state, peer, &mock.url).await;
    let input = ListObjectsInput {
        bucket: BUCKET.to_string(),
        node_id: Some(peer.to_string()),
        ..listing()
    };
    let listed = server.list_objects(Extension(caller(&fixture, true)), Parameters(input));
    assert_eq!(listed.await.unwrap().0.node_id, peer.to_string());
    assert_eq!(mock.paths.lock().unwrap().len(), 1);
}

fn auth_parts(auth: &AuthContext, directed: Option<NodeId>) -> http::request::Parts {
    let (mut parts, ()) = http::Request::new(()).into_parts();
    parts.extensions.insert(Some(auth.clone()));
    let token = crate::auth::ValidatedBearer::new_for_test("caller-token");
    parts.extensions.insert(Some(token));
    if let Some(node_id) = directed {
        let value = http::HeaderValue::try_from(node_id.to_string()).unwrap();
        let header = crate::routes::storage::relay::RELAY_NODE_HEADER;
        parts.headers.insert(header, value);
    }
    parts
}

// A node outside the realm's servers never gets the caller's token, even when it publishes
// an address.
#[tokio::test]
async fn forward_needs_member() {
    let fixture = node_state().await;
    let server = McpServer::new(fixture.state.clone());
    let outsider = node(9);
    let mock = mock_peer(StatusCode::OK, "{}").await;
    publish_api(&fixture.state, outsider, &mock.url).await;
    let (parts, input) = (caller(&fixture, true), listing());
    let refused = server.forward::<ObjectsOutput>(&parts, outsider, "list_objects", &input);
    let refused = body(refused.await.unwrap_err());
    assert_eq!(refused["code"], "Bad request");
    assert!(mock.paths.lock().unwrap().is_empty());
}

#[test]
fn devices_never_targets() {
    use aruna_core::structs::identity::realm::{RealmId, RealmNode, RealmNodeKind};
    let realm_id = RealmId([3; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    let device = RealmNodeKind::User {
        owner: aruna_core::UserId::nil(realm_id),
    };
    for (seed, kind) in [(2, RealmNodeKind::Server), (3, device)] {
        config.nodes.push(RealmNode {
            node_id: node(seed).to_string(),
            kind,
        });
    }
    assert!(realm_server(&config, node(2)));
    assert!(!realm_server(&config, node(3)));
    assert!(!realm_server(&config, node(4)));
}

// A call another node forwarded runs here and is never forwarded again; one directed at
// another node is refused, so two nodes advertising one endpoint cannot loop.
#[tokio::test]
async fn directed_never_forwards() {
    let fixture = node_state().await;
    let server = McpServer::new(fixture.state.clone());
    let (here, peer) = (node_id(LOCAL), node_id(PEER));
    let named = Some(peer.to_string());
    let parts = auth_parts(&fixture.owner, Some(here));
    let call = (&parts, &fixture.owner);
    let local = server
        .bucket_node(call, BUCKET, named.as_deref(), "a")
        .await;
    assert_eq!(local.unwrap(), None);
    let parts = auth_parts(&fixture.owner, Some(peer));
    let call = (&parts, &fixture.owner);
    let looped = server
        .bucket_node(call, BUCKET, named.as_deref(), "a")
        .await;
    assert_eq!(body(looped.unwrap_err())["code"], "Conflict");
}

// A member of a distributed bucket that keeps no records of a key's range sends the call to
// the member that does, as the REST reads do; a key of its own range stays here.
#[tokio::test]
async fn member_routes_key() {
    let fixture = node_state().await;
    distribute(&fixture.state, fixture.group_id, BUCKET).await;
    let server = McpServer::new(fixture.state.clone());
    let parts = auth_parts(&fixture.owner, None);
    let here = Some(node_id(LOCAL).to_string());
    for named in [None, here.as_deref()] {
        let call = (&parts, &fixture.owner);
        let routed = server.bucket_node(call, BUCKET, named, "z/x").await;
        assert_eq!(routed.unwrap(), Some(node_id(PEER)));
        let call = (&parts, &fixture.owner);
        let own = server.bucket_node(call, BUCKET, named, "a/x").await;
        assert_eq!(own.unwrap(), None);
    }
}
