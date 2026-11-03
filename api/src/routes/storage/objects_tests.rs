//! Tests the object listing route: pages, cursors, permissions, relayed bounds and the merged
//! listing of a distributed bucket whose other range a peer lists.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::relay::RELAY_SET_HEADER;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
    replicate,
};
use axum::http::StatusCode;

async fn store(node: &Node, keys: &[&str]) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    for key in keys {
        let input = WriteObjectInput {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            text: "x".to_string(),
            content_type: None,
        };
        store_text(&node.state, &node.owner, &info, input, Default::default())
            .await
            .unwrap();
    }
}

fn query(prefix: Option<&str>, cursor: Option<String>, limit: usize) -> ObjectListQuery {
    ObjectListQuery {
        prefix: prefix.map(str::to_string),
        delimiter: Some("/".to_string()),
        cursor,
        limit: Some(limit),
        ..Default::default()
    }
}

async fn list(
    node: &Node,
    auth: &AuthContext,
    headers: HeaderMap,
    query: ObjectListQuery,
) -> ServerResult<ObjectListResponse> {
    let listed = list_objects(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(BUCKET.to_string()),
        Query(query),
    )
    .await?;
    Ok(listed.0)
}

fn keys(page: &ObjectListResponse) -> Vec<&str> {
    page.objects
        .iter()
        .map(|object| object.key.as_str())
        .collect()
}

/// The headers of a listing another member of `set_id` relays.
fn relayed(set_id: ulid::Ulid) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(RELAY_HOP_HEADER, "peer".parse().unwrap());
    headers.insert(RELAY_SET_HEADER, set_id.to_string().parse().unwrap());
    headers
}

#[tokio::test]
async fn pages_local_bucket() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md", "dir/c.md", "dir/d.md"]).await;

    let first = list(&node, &node.owner, HeaderMap::new(), query(None, None, 2))
        .await
        .unwrap();
    assert_eq!(keys(&first), vec!["a.md", "b.md"]);
    let cursor = first.next_cursor.clone().expect("more entries follow");
    let rest = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 2),
    )
    .await;
    let rest = rest.unwrap();
    assert!(rest.objects.is_empty());
    assert_eq!(rest.prefixes, vec!["dir/".to_string()]);
    assert!(rest.next_cursor.is_none());

    let inside = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(Some("dir/"), None, 10),
    )
    .await;
    assert_eq!(keys(&inside.unwrap()), vec!["dir/c.md", "dir/d.md"]);
}

#[tokio::test]
async fn refuses_bad_listings() {
    let node = node_state().await;
    let stranger = list(
        &node,
        &node.stranger,
        HeaderMap::new(),
        query(None, None, 5),
    )
    .await;
    assert!(matches!(stranger, Err(ServerError::Forbidden)));
    let from = encode_resume(&Resume::From("m".to_string())).unwrap();
    let cursor = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(from), 5),
    )
    .await;
    assert!(
        cursor.is_err(),
        "a range position is no cursor of a local listing"
    );
    let mut upper = query(None, None, 5);
    upper.upper = Some("m".to_string());
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), upper).await,
        Err(ServerError::BadRequestReason(_))
    ));
    let mut lower = query(None, None, 5);
    lower.lower = Some(String::new());
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), lower).await,
        Err(ServerError::BadRequestReason(_))
    ));
}

#[tokio::test]
async fn relayed_range_listing() {
    let node = node_state().await;
    store(&node, &["a.md", "z.md"]).await;
    let set_id = distribute(&node.state, node.group_id, BUCKET)
        .await
        .set_id();
    let mut bounded = query(None, None, 10);
    bounded.lower = Some(String::new());
    bounded.upper = Some("m".to_string());
    // Bounds come only from another member of the set.
    let direct = list(&node, &node.owner, HeaderMap::new(), bounded.clone()).await;
    assert!(matches!(direct, Err(ServerError::BadRequestReason(_))));

    let page = list(&node, &node.owner, relayed(set_id), bounded)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md"]);

    let mut other = query(None, None, 10);
    other.lower = Some("m".to_string());
    let foreign = list(&node, &node.owner, relayed(set_id), other).await;
    assert!(matches!(
        foreign,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
}

#[tokio::test]
async fn merges_peer_range() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md"]).await;
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":["y/"],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;

    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md", "b.md", "x.md"]);
    assert_eq!(page.prefixes, vec!["y/".to_string()]);
    assert!(page.next_cursor.is_none());
    let paths = peer.paths.lock().unwrap().clone();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("/api/v1/data/buckets/routed/objects?limit=8"));
    assert!(paths[0].contains("lower=m"));

    // A full page stops before the peer's range and resumes after the last local key.
    let first = list(&node, &node.owner, HeaderMap::new(), query(None, None, 2))
        .await
        .unwrap();
    assert_eq!(keys(&first), vec!["a.md", "b.md"]);
    let cursor = first.next_cursor.expect("the peer's range follows");
    let rest = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 2),
    )
    .await;
    assert_eq!(keys(&rest.unwrap()), vec!["x.md"]);
}

// A copy may lag behind, so the coordinator lists the range even where a copy is kept.
#[tokio::test]
async fn copy_defers_listing() {
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    replicate(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":[],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["x.md"]);
    assert_eq!(peer.paths.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn start_after_resumes() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md", "c.md"]).await;
    let mut after = query(None, None, 10);
    after.start_after = Some("a.md".to_string());
    let page = list(&node, &node.owner, HeaderMap::new(), after.clone())
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["b.md", "c.md"]);
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":[],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let page = list(&node, &node.owner, HeaderMap::new(), after)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["b.md", "c.md", "x.md"]);
}

// A start key on or inside a common prefix skips the whole prefix, here and in later ranges.
#[tokio::test]
async fn start_after_groups() {
    let node = node_state().await;
    store(&node, &["a/a/x", "a/b/x"]).await;
    for start in ["a/a/", "a/a/x"] {
        let mut after = query(Some("a/"), None, 10);
        after.start_after = Some(start.to_string());
        let page = list(&node, &node.owner, HeaderMap::new(), after)
            .await
            .unwrap();
        assert_eq!(page.prefixes, vec!["a/b/".to_string()], "{start}");
    }
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[],"prefixes":["y/","z/"],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let mut after = query(None, None, 10);
    after.start_after = Some("a/a/x".to_string());
    let page = list(&node, &node.owner, HeaderMap::new(), after.clone())
        .await
        .unwrap();
    assert_eq!(page.prefixes, vec!["y/".to_string(), "z/".into()]);
    // In the peer's range the prefix it names is not listed again.
    after.start_after = Some("y/q".to_string());
    let page = list(&node, &node.owner, HeaderMap::new(), after)
        .await
        .unwrap();
    assert_eq!(page.prefixes, vec!["z/".to_string()]);
    assert!(peer.paths.lock().unwrap()[1].contains("cursor="));
}

// While the coordinator cannot answer, this node's copy lists the range, marked stale, and a
// relayed listing asks a copy only with `copy`.
#[tokio::test]
async fn down_coordinator_copy() {
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md"]);
    let range_id = snapshot.config.ranges[0].range_id.to_string();
    assert_eq!(page.stale_ranges, [range_id]);

    let mut bounded = query(None, None, 10);
    bounded.lower = Some(String::new());
    let relayed_set = relayed(snapshot.set_id());
    let refused = list(&node, &node.owner, relayed_set.clone(), bounded.clone()).await;
    assert!(matches!(
        refused,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    bounded.copy = Some(true);
    let page = list(&node, &node.owner, relayed_set, bounded)
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md"]);
}

#[tokio::test]
async fn unanswered_range_fails() {
    let node = node_state().await;
    distribute(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let Err(ServerError::ServiceUnavailableReason(reason)) =
        list(&node, &node.owner, HeaderMap::new(), query(None, None, 10)).await
    else {
        panic!("a range no holder answers fails the page");
    };
    assert!(reason.contains("incomplete"));
    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    assert!(matches!(
        list(&node, &node.owner, HeaderMap::new(), query(None, None, 10)).await,
        Err(ServerError::Forbidden)
    ));
}

/// Writes a link at `key` in the test bucket to `target`, as the owner.
async fn link(node: &Node, key: &str, target: aruna_core::structs::storage::link::LinkTarget) {
    use aruna_operations::staging::reference::{ReferenceWrite, write_reference_version};
    let write = ReferenceWrite {
        preassigned_version_id: None,
        group_id: node.group_id,
        user_id: node.owner.user_id,
        realm_id: node.owner.realm_id,
        node_id: node.state.get_node_id(),
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        expected_bucket: None,
        version_source: target.binding(),
        metadata: aruna_core::structs::execution::source_access::SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: Some(target.arn.version.to_string()),
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        },
        inherited_policies: Vec::new(),
        connector_guard: None,
        restrictions: None,
    };
    write_reference_version(&node.state.get_ctx(), write)
        .await
        .unwrap();
}

#[tokio::test]
async fn hides_unreadable_links() {
    // A link names a target the caller cannot read: its key stays out of the listing, while a
    // link to a readable target shows like any object.
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    let target =
        |bucket: &str, key: &str, group_id| aruna_core::structs::storage::link::LinkTarget {
            arn: aruna_core::structs::storage::replication::VersionedObjectArn {
                realm_id: node.owner.realm_id,
                node_id: node.state.get_node_id(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                version: ulid::Ulid::from_parts(5, 5),
            },
            group_id,
        };
    link(&node, "open.md", target(BUCKET, "a.md", node.group_id)).await;
    link(
        &node,
        "secret.md",
        target("other", "x", ulid::Ulid::from_parts(9, 9)),
    )
    .await;
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md", "open.md"]);
}

#[tokio::test]
async fn hides_link_folders() {
    // A folder holding only unreadable links is no prefix, and a cursor continues after the last
    // key a page showed, never after a hidden one.
    let node = node_state().await;
    store(&node, &["a.md", "c.md"]).await;
    let hidden = |key: &str| aruna_core::structs::storage::link::LinkTarget {
        arn: aruna_core::structs::storage::replication::VersionedObjectArn {
            realm_id: node.owner.realm_id,
            node_id: node.state.get_node_id(),
            bucket: "other".to_string(),
            key: key.to_string(),
            version: ulid::Ulid::from_parts(5, 5),
        },
        group_id: ulid::Ulid::from_parts(9, 9),
    };
    link(&node, "b-secret.md", hidden("x")).await;
    link(&node, "hidden/x.md", hidden("y")).await;
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md", "c.md"]);
    assert!(page.prefixes.is_empty());
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 2))
        .await
        .unwrap();
    assert_eq!(keys(&page), vec!["a.md"]);
    let cursor = page.next_cursor.expect("more entries follow");
    let after = Resume::After {
        key: "a.md".to_string(),
        group: None,
    };
    assert_eq!(decode_resume(&cursor).unwrap(), after);
}

#[tokio::test]
async fn merges_past_hidden() {
    // A local range that starts with more hidden entries than one request examines goes on
    // from this member's sealed cursor inside the merged cursor, then on to the peer's range.
    use aruna_operations::s3::object::visible::SCAN_BUDGET;
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    let hidden = aruna_core::structs::storage::link::LinkTarget {
        arn: aruna_core::structs::storage::replication::VersionedObjectArn {
            realm_id: node.owner.realm_id,
            node_id: node.state.get_node_id(),
            bucket: "other".to_string(),
            key: "x".to_string(),
            version: ulid::Ulid::from_parts(5, 5),
        },
        group_id: ulid::Ulid::from_parts(9, 9),
    };
    for index in 0..=SCAN_BUDGET {
        link(&node, &format!("b/{index:05}"), hidden.clone()).await;
    }
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":[],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let first = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert_eq!(keys(&first), vec!["a.md"]);
    let second = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, first.next_cursor, 10),
    )
    .await
    .unwrap();
    assert!(second.objects.is_empty() && second.prefixes.is_empty());
    let cursor = second.next_cursor.expect("the hidden run goes on");
    assert!(matches!(decode_resume(&cursor), Ok(Resume::Hidden { .. })));
    let third = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 10),
    )
    .await
    .unwrap();
    assert_eq!(keys(&third), vec!["x.md"]);
    assert!(third.prefixes.is_empty() && third.next_cursor.is_none());
}

/// A replicated bucket whose coordinator cannot be reached: this member lists its copy, which
/// starts with more hidden links than one request examines, and seals the range there.
async fn sealed_copy_page() -> (Node, String, String) {
    use aruna_operations::s3::object::visible::SCAN_BUDGET;
    let node = node_state().await;
    store(&node, &["z.md"]).await;
    let hidden = aruna_core::structs::storage::link::LinkTarget {
        arn: aruna_core::structs::storage::replication::VersionedObjectArn {
            realm_id: node.owner.realm_id,
            node_id: node.state.get_node_id(),
            bucket: "other".to_string(),
            key: "x".to_string(),
            version: ulid::Ulid::from_parts(5, 5),
        },
        group_id: ulid::Ulid::from_parts(9, 9),
    };
    for index in 0..=SCAN_BUDGET {
        link(&node, &format!("b/{index:05}"), hidden.clone()).await;
    }
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let page = list(&node, &node.owner, HeaderMap::new(), query(None, None, 10))
        .await
        .unwrap();
    assert!(page.objects.is_empty() && page.prefixes.is_empty());
    let range_id = snapshot.config.ranges[0].range_id.to_string();
    assert_eq!(page.stale_ranges, std::slice::from_ref(&range_id));
    let cursor = page.next_cursor.expect("the copy sealed its range");
    (node, cursor, range_id)
}

#[tokio::test]
async fn cursor_names_copy() {
    // A copy that sealed its range is named as a copy, so a member resuming there asks it as
    // one while the coordinator is down.
    let (_node, cursor, _) = sealed_copy_page().await;
    let resume = decode_resume(&cursor).unwrap();
    assert!(
        matches!(resume, Resume::Hidden { copy: true, .. }),
        "{resume:?}"
    );
}

#[tokio::test]
async fn resumed_copy_stale() {
    // A range resumed at a copy is marked stale like the page that first listed the copy.
    let (node, cursor, range_id) = sealed_copy_page().await;
    let page = list(
        &node,
        &node.owner,
        HeaderMap::new(),
        query(None, Some(cursor), 10),
    )
    .await
    .unwrap();
    assert_eq!(keys(&page), vec!["z.md"]);
    assert_eq!(page.stale_ranges, [range_id]);
}
