//! Tests share links on a distributed bucket: the member a link was made on serves the visit,
//! and the keys of another member's range are listed, read and written at that member.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::visit::{OpenShareRequest, VisitListQuery, VisitListing, list_files, open_share};
use super::*;
use crate::tests::members::{BUCKET, Members, members};
use crate::tests::routes::test_state;
use aruna_core::structs::identity::auth::NodeCapabilities;
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, HeaderValue, header};
use s3s::S3;
use s3s::dto::StreamingBlob;
use std::net::SocketAddr;

/// Two members split at `m`: member 0 serves the links and coordinates the keys below `m`,
/// member 1 the rest.
struct Shared {
    members: Members,
    state: Arc<ServerState>,
    owner: AuthContext,
}

async fn shared() -> Shared {
    let members = members(2, &["m"], 0).await;
    let node = &members.nodes[0];
    let capabilities = NodeCapabilities::user_node(members.realm_id).unwrap();
    let context = node.context.clone();
    let state = test_state(context, members.realm_id, node.node_id(), capabilities).await;
    let owner = AuthContext {
        user_id: members.user.user_identity,
        realm_id: members.realm_id,
        path_restrictions: None,
        session: None,
    };
    Shared {
        members,
        state: Arc::new(state),
        owner,
    }
}

/// An S3 request on member `on` as the access hook admits the owner's own credential.
async fn s3_request<T>(
    shared: &Shared,
    on: usize,
    input: T,
    method: http::Method,
) -> s3s::S3Request<T> {
    let node = &shared.members.nodes[on];
    let mut extensions = http::Extensions::new();
    extensions.insert(shared.members.user_on(on));
    extensions.insert(node.bucket().await);
    extensions.insert(aruna_operations::auth::request_policy::PolicyRequestExtras::rest());
    s3s::S3Request {
        input,
        method,
        uri: http::Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Stores `text` as `key` through member `on`, which forwards keys it does not coordinate.
async fn put(shared: &Shared, on: usize, key: &str, text: &str) {
    let input = s3s::dto::PutObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        content_length: Some(text.len() as i64),
        body: Some(StreamingBlob::from(bytes::Bytes::from(text.to_string()))),
        ..Default::default()
    };
    let request = s3_request(shared, on, input, http::Method::PUT).await;
    shared.members.nodes[on]
        .service
        .put_object(request)
        .await
        .unwrap();
}

/// The bytes of `key` as member `on` reads them, or `None` when it has no such key.
async fn stored(shared: &Shared, on: usize, key: &str) -> Option<Vec<u8>> {
    use futures_util::TryStreamExt;
    let input = s3s::dto::GetObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        ..Default::default()
    };
    let request = s3_request(shared, on, input, http::Method::GET).await;
    let output = shared.members.nodes[on]
        .service
        .get_object(request)
        .await
        .ok()?
        .output;
    let chunks: Vec<bytes::Bytes> = output.body?.try_collect().await.unwrap();
    Some(chunks.concat())
}

fn peer() -> ConnectInfo<SocketAddr> {
    ConnectInfo("192.0.2.77:4000".parse().unwrap())
}

fn share_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_str(&format!("Share {token}")).unwrap();
    headers.insert(header::AUTHORIZATION, value);
    headers
}

fn code(error: ServerError) -> &'static str {
    match error {
        ServerError::Refused(_, code, _) => code,
        ServerError::NotFound => "not_found",
        ServerError::Conflict(_) => "conflict",
        other => panic!("unexpected error {other:?}"),
    }
}

/// A link to the whole bucket in `mode`, and an open visit of it.
async fn visit(shared: &Shared, mode: ShareModeBody) -> (ShareDetails, String) {
    let request = CreateShareRequest {
        target: ShareTargetBody::Bucket {
            bucket: BUCKET.to_string(),
        },
        mode,
        label: String::new(),
        expires_at: None,
        password: None,
    };
    let state = State(shared.state.clone());
    let auth = Extension(Some(shared.owner.clone()));
    let (_, Json(link)) = create_share(state, auth, Json(request)).await.unwrap();
    let headers = share_headers(&link.token);
    let opened = open_share(
        State(shared.state.clone()),
        peer(),
        headers,
        Some(Json(OpenShareRequest::default())),
    )
    .await;
    let Json(opened) = opened.unwrap();
    (link, opened.visit_token)
}

async fn page(
    shared: &Shared,
    visit: &str,
    (prefix, cursor, limit): (&str, Option<String>, Option<usize>),
) -> ServerResult<VisitListing> {
    let query = VisitListQuery {
        prefix: Some(prefix.to_string()),
        cursor,
        limit,
    };
    let state = State(shared.state.clone());
    let Json(page) = list_files(state, share_headers(visit), Query(query)).await?;
    Ok(page)
}

/// Every name of one folder, page by page, files and folders in key order.
async fn names(
    shared: &Shared,
    visit: &str,
    prefix: &str,
    limit: usize,
) -> ServerResult<Vec<String>> {
    let (mut names, mut cursor) = (Vec::new(), None);
    loop {
        let listed = page(shared, visit, (prefix, cursor, Some(limit))).await?;
        let mut entries: Vec<String> = (listed.files.into_iter().map(|file| file.name))
            .chain(listed.folders)
            .collect();
        entries.sort();
        names.extend(entries);
        cursor = listed.next_cursor;
        if cursor.is_none() {
            return Ok(names);
        }
    }
}

// The member a link was made on lists both ranges in key order, and its pages go on across
// the range bound without losing or repeating a name.
#[tokio::test]
async fn members_listed_together() {
    let shared = shared().await;
    put(&shared, 0, "a.txt", "alpha").await;
    put(&shared, 0, "d/b.txt", "beta").await;
    for key in ["x.txt", "x/deep.txt", "y.txt"] {
        put(&shared, 1, key, "far").await;
    }
    let (_, visit) = visit(&shared, ShareModeBody::Read).await;
    let all = ["a.txt", "d/", "x.txt", "x/", "y.txt"];
    let mut whole = names(&shared, &visit, "", 100).await.unwrap();
    whole.sort();
    assert_eq!(whole, all);
    let mut paged = names(&shared, &visit, "", 1).await.unwrap();
    paged.sort();
    assert_eq!(paged, all);
    assert_eq!(
        names(&shared, &visit, "x/", 10).await.unwrap(),
        ["x/deep.txt"]
    );
    let listed = page(&shared, &visit, ("", None, None)).await.unwrap();
    let far = listed
        .files
        .iter()
        .find(|file| file.name == "y.txt")
        .unwrap();
    assert_eq!(far.size, Some(3));
    assert!(far.last_modified.is_some());
}

// A share policy on the holder decides each key it lists: denied names and sizes stay out of
// the visit, folders included.
#[tokio::test]
async fn holder_policy_hides() {
    let shared = shared().await;
    put(&shared, 1, "public.txt", "open").await;
    put(&shared, 1, "private/secret.txt", "closed").await;
    let expression = "request.session.kind == 'share' && path.contains('/private/')";
    shared.members.deny_on(1, expression).await;
    let (_, visit) = visit(&shared, ShareModeBody::Read).await;
    assert_eq!(
        names(&shared, &visit, "", 100).await.unwrap(),
        ["public.txt"]
    );
    assert!(
        names(&shared, &visit, "private/", 100)
            .await
            .unwrap()
            .is_empty()
    );
    // The owner's own S3 listing on that member still shows the key.
    assert!(stored(&shared, 1, "private/secret.txt").await.is_some());
}

// A range whose coordinator is down cannot be listed, and the visitor is told to retry.
#[tokio::test]
async fn coordinator_down_listing() {
    let mut shared = shared().await;
    put(&shared, 0, "a.txt", "alpha").await;
    let (_, visit) = visit(&shared, ShareModeBody::ReadUpload).await;
    shared.members.nodes[1].stop().await;
    let error = page(&shared, &visit, ("", None, None)).await.unwrap_err();
    assert_eq!(code(error), "range_unavailable");
}
