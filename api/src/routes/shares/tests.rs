//! Tests share links end to end on one node: owner routes, opening, listing, tickets,
//! downloads and uploads, each acting as the creator inside the link's scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::visit::{
    DownloadQuery, OpenShareRequest, TicketRequest, UploadQuery, VisitListQuery, create_ticket,
    download_file, list_files, open_share, upload_file,
};
use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::deletion::bucket_info;
use crate::tests::distributed::{BUCKET, Node, node_state};
use aruna_core::structs::identity::auth::{PathRestriction, Permission, SessionRef};
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, HeaderValue, header};
use std::net::SocketAddr;

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
        ServerError::Forbidden => "forbidden",
        ServerError::NotFound => "not_found",
        ServerError::BadRequestReason(_) => "bad_request",
        other => panic!("unexpected error {other:?}"),
    }
}

async fn put_text(node: &Node, key: &str, text: &str) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let input = WriteObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        text: text.to_string(),
        content_type: None,
    };
    store_text(&node.state, &node.owner, &info, input, Default::default())
        .await
        .unwrap();
}

fn folder_request(prefix: &str, mode: ShareModeBody) -> CreateShareRequest {
    CreateShareRequest {
        target: ShareTargetBody::Folder {
            bucket: BUCKET.to_string(),
            prefix: prefix.to_string(),
        },
        mode,
        label: "for reviewers".to_string(),
        expires_at: None,
        password: None,
    }
}

async fn create(
    node: &Node,
    auth: &AuthContext,
    request: CreateShareRequest,
) -> ServerResult<ShareDetails> {
    create_share(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Json(request),
    )
    .await
    .map(|(_, Json(details))| details)
}

async fn open(node: &Node, token: &str, password: Option<&str>) -> ServerResult<String> {
    let request = OpenShareRequest {
        password: password.map(str::to_string),
    };
    open_share(
        State(node.state.clone()),
        peer(),
        share_headers(token),
        Some(Json(request)),
    )
    .await
    .map(|Json(opened)| opened.visit_token)
}

async fn list(node: &Node, visit: &str, prefix: &str) -> ServerResult<Vec<String>> {
    let query = VisitListQuery {
        prefix: Some(prefix.to_string()),
        ..Default::default()
    };
    let Json(page) = list_files(
        State(node.state.clone()),
        share_headers(visit),
        Query(query),
    )
    .await?;
    Ok(page
        .files
        .into_iter()
        .map(|file| file.name)
        .chain(page.folders)
        .collect())
}

async fn download(node: &Node, visit: &str, name: &str) -> ServerResult<(String, Vec<u8>)> {
    let request = TicketRequest {
        name: name.to_string(),
    };
    let Json(ticket) = create_ticket(
        State(node.state.clone()),
        share_headers(visit),
        Json(request),
    )
    .await?;
    let ticket = ticket.path.split_once("ticket=").unwrap().1.to_string();
    let bytes = fetch(node, &ticket).await?;
    Ok((ticket, bytes))
}

async fn fetch(node: &Node, ticket: &str) -> ServerResult<Vec<u8>> {
    let response = download_file(
        State(node.state.clone()),
        peer(),
        HeaderMap::new(),
        Query(DownloadQuery {
            ticket: ticket.to_string(),
        }),
    )
    .await?;
    let disposition = response.headers()[header::CONTENT_DISPOSITION].clone();
    assert!(disposition.to_str().unwrap().starts_with("attachment"));
    Ok(to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec())
}

async fn upload(node: &Node, visit: &str, name: &str, text: &str) -> ServerResult<String> {
    upload_file(
        State(node.state.clone()),
        peer(),
        share_headers(visit),
        Query(UploadQuery {
            name: name.to_string(),
        }),
        Body::from(text.to_string()),
    )
    .await
    .map(|(_, Json(uploaded))| uploaded.name)
}

#[tokio::test]
async fn owner_manages_links() {
    let node = node_state().await;
    let created = create(
        &node,
        &node.owner,
        folder_request("shared/", ShareModeBody::Read),
    )
    .await
    .unwrap();
    assert_eq!(created.share.status, ShareStatus::Active);
    let Json(listed) = list_shares(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
    )
    .await
    .unwrap();
    assert_eq!(listed.shares.len(), 1);
    let Json(again) = get_share(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(created.share.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(again.token, created.token);
    // Another user neither reads nor revokes the link.
    let foreign = get_share(
        State(node.state.clone()),
        Extension(Some(node.stranger.clone())),
        Path(created.share.id.clone()),
    )
    .await;
    assert!(matches!(foreign, Err(ServerError::NotFound)));
    let Json(stranger_list) = list_shares(
        State(node.state.clone()),
        Extension(Some(node.stranger.clone())),
    )
    .await
    .unwrap();
    assert!(stranger_list.shares.is_empty());
    assert!(open(&node, &created.token, None).await.is_ok());
    revoke_share(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(created.share.id.clone()),
    )
    .await
    .unwrap();
    let error = open(&node, &created.token, None).await.unwrap_err();
    assert_eq!(code(error), "share_unavailable");
}

#[tokio::test]
async fn refuses_bad_creates() {
    let node = node_state().await;
    let request = || folder_request("shared/", ShareModeBody::Read);
    let error = create(&node, &node.stranger, request()).await.unwrap_err();
    assert_eq!(code(error), "forbidden");
    let mut assistant = node.owner.clone();
    assistant.session = Some(SessionRef {
        sid: "s".into(),
        kind: SessionKind::Assistant,
        name: None,
        via: None,
    });
    assert_eq!(
        code(create(&node, &assistant, request()).await.unwrap_err()),
        "forbidden"
    );
    let mut restricted = node.owner.clone();
    restricted.path_restrictions = Some(vec![PathRestriction {
        pattern: "/**".into(),
        permission: Permission::READ,
    }]);
    assert_eq!(
        code(create(&node, &restricted, request()).await.unwrap_err()),
        "forbidden"
    );
    let object = CreateShareRequest {
        target: ShareTargetBody::Object {
            bucket: BUCKET.to_string(),
            key: "a.txt".into(),
        },
        ..folder_request("shared/", ShareModeBody::Upload)
    };
    assert_eq!(
        code(create(&node, &node.owner, object).await.unwrap_err()),
        "bad_request"
    );
    let mut past = request();
    past.expires_at = Some(Utc::now() - chrono::Duration::hours(1));
    assert_eq!(
        code(create(&node, &node.owner, past).await.unwrap_err()),
        "bad_request"
    );
    let mut missing = request();
    missing.target = ShareTargetBody::Bucket {
        bucket: "missing".into(),
    };
    assert_eq!(
        code(create(&node, &node.owner, missing).await.unwrap_err()),
        "not_found"
    );
}

#[tokio::test]
async fn visit_stays_in_scope() {
    let node = node_state().await;
    put_text(&node, "shared/a.txt", "alpha").await;
    put_text(&node, "shared/deep/b.txt", "beta").await;
    put_text(&node, "private/c.txt", "gamma").await;
    let link = create(
        &node,
        &node.owner,
        folder_request("shared/", ShareModeBody::Read),
    )
    .await
    .unwrap();
    // The link token only opens; every other route wants a visit token.
    let error = list(&node, &link.token, "").await.unwrap_err();
    assert_eq!(code(error), "share_unavailable");
    let visit = open(&node, &link.token, None).await.unwrap();
    assert_eq!(list(&node, &visit, "").await.unwrap(), ["a.txt", "deep/"]);
    assert_eq!(list(&node, &visit, "deep/").await.unwrap(), ["deep/b.txt"]);
    assert_eq!(
        code(list(&node, &visit, "../").await.unwrap_err()),
        "bad_request"
    );
    let (ticket, bytes) = download(&node, &visit, "a.txt").await.unwrap();
    assert_eq!(bytes, b"alpha");
    // A ticket works once.
    assert_eq!(
        code(fetch(&node, &ticket).await.unwrap_err()),
        "share_unavailable"
    );
    let outside = download(&node, &visit, "../private/c.txt")
        .await
        .unwrap_err();
    assert_eq!(code(outside), "bad_request");
    assert_eq!(
        code(download(&node, &visit, "gone.txt").await.unwrap_err()),
        "not_found"
    );
    let error = upload(&node, &visit, "new.txt", "x").await.unwrap_err();
    assert_eq!(code(error), "read_only");
}

#[tokio::test]
async fn password_opens_once() {
    let node = node_state().await;
    let mut request = folder_request("shared/", ShareModeBody::Read);
    request.password = Some("correct horse".into());
    let link = create(&node, &node.owner, request).await.unwrap();
    assert!(link.share.password);
    assert_eq!(
        code(open(&node, &link.token, None).await.unwrap_err()),
        "password_required"
    );
    let wrong = open(&node, &link.token, Some("wrong")).await.unwrap_err();
    assert_eq!(code(wrong), "wrong_password");
    assert!(
        open(&node, &link.token, Some("correct horse"))
            .await
            .is_ok()
    );
    // A forged secret gets the same answer as an unknown link.
    let forged = format!("{}.{}", link.share.id, "A".repeat(43));
    assert_eq!(
        code(open(&node, &forged, None).await.unwrap_err()),
        "share_unavailable"
    );
    let Json(log) = share_log(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(link.share.id.clone()),
        Query(ShareLogQuery::default()),
    )
    .await
    .unwrap();
    let actions: Vec<_> = log
        .entries
        .iter()
        .map(|entry| entry.action.as_str())
        .collect();
    assert_eq!(actions, ["wrong_password", "open"]);
    assert_eq!(log.entries[0].client.as_deref(), Some("192.0.2.0/24"));
}

#[tokio::test]
async fn upload_never_overwrites() {
    let node = node_state().await;
    put_text(&node, "drop/report.txt", "original").await;
    let link = create(
        &node,
        &node.owner,
        folder_request("drop/", ShareModeBody::Upload),
    )
    .await
    .unwrap();
    let visit = open(&node, &link.token, None).await.unwrap();
    assert_eq!(
        upload(&node, &visit, "report.txt", "new").await.unwrap(),
        "report (1).txt"
    );
    assert_eq!(
        upload(&node, &visit, "in/data.csv", "1,2").await.unwrap(),
        "in/data.csv"
    );
    assert_eq!(
        code(upload(&node, &visit, "../x.txt", "x").await.unwrap_err()),
        "bad_request"
    );
    // A file drop shows nothing that is stored.
    assert_eq!(
        code(list(&node, &visit, "").await.unwrap_err()),
        "upload_only"
    );
    let owner_view = folder_request("drop/", ShareModeBody::Read);
    let reader = create(&node, &node.owner, owner_view).await.unwrap();
    let read_visit = open(&node, &reader.token, None).await.unwrap();
    let (_, original) = download(&node, &read_visit, "report.txt").await.unwrap();
    assert_eq!(original, b"original");
    let (_, uploaded) = download(&node, &read_visit, "report (1).txt")
        .await
        .unwrap();
    assert_eq!(uploaded, b"new");
}
