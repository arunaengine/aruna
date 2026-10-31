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

/// Gives `user` a role in the node's group with exactly `rules` below the bucket.
async fn grant(node: &Node, user: aruna_core::UserId, rules: &[(&str, Permission)]) {
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::identity::auth::{Actor, Role};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::storage::blob::object_permission_path;
    let (realm, group_id) = (node.owner.realm_id, node.group_id);
    let mut group_auth =
        GroupAuthorizationDocument::default_group_doc(node.owner.user_id, realm, group_id);
    let role_id = Ulid::from_bytes([42; 16]);
    let node_id = node.state.get_node_id();
    let permissions = rules
        .iter()
        .map(|(key, permission)| {
            let path = object_permission_path(realm, group_id, node_id, BUCKET, key);
            (path, permission.clone())
        })
        .collect();
    group_auth.roles.insert(
        role_id,
        Role {
            role_id,
            name: "drop".into(),
            permissions,
            assigned_users: [user].into(),
        },
    );
    let group = Group {
        display_name: "routed".into(),
        group_id,
        realm_id: realm,
        roles: group_auth.roles.keys().copied().collect(),
        owner: node.owner.user_id,
    };
    let actor = Actor {
        node_id,
        user_id: node.owner.user_id,
        realm_id: realm,
    };
    let context = node.state.get_ctx();
    let rows = [
        (AUTH_KEYSPACE, group_auth.to_bytes(&actor).unwrap()),
        (GROUP_KEYSPACE, group.to_bytes(&actor).unwrap()),
    ];
    for (key_space, value) in rows {
        let key = group_id.to_bytes().to_vec().into();
        crate::tests::routes::write_doc(&context, key_space, key, value.into()).await;
    }
}

#[tokio::test]
async fn upload_authorizes_final_name() {
    // The creator may write `report.txt` only; a renamed upload must not slip past that.
    let node = node_state().await;
    put_text(&node, "drop/report.txt", "original").await;
    let writer = node.stranger.clone();
    let rules = [
        ("drop/", Permission::WRITE),
        ("drop/report.txt", Permission::WRITE),
    ];
    grant(&node, writer.user_id, &rules).await;
    let link = create(
        &node,
        &writer,
        folder_request("drop/", ShareModeBody::Upload),
    )
    .await
    .unwrap();
    let visit = open(&node, &link.token, None).await.unwrap();
    let error = upload(&node, &visit, "report.txt", "new")
        .await
        .unwrap_err();
    assert_eq!(code(error), "share_unavailable");
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    let renamed = super::visit::current_version(&node.state, BUCKET, "drop/report (1).txt").await;
    assert!(matches!(renamed, Err(ServerError::NotFound)), "{info:?}");
}

#[tokio::test]
async fn uploads_never_share_a_name() {
    let node = node_state().await;
    let link = create(
        &node,
        &node.owner,
        folder_request("drop/", ShareModeBody::Upload),
    )
    .await
    .unwrap();
    let visit = open(&node, &link.token, None).await.unwrap();
    // A running upload holds its name, so the next one takes another.
    let claim = aruna_operations::shares::names::ClaimNameOperation::new(
        BUCKET,
        "drop/a.txt",
        unix_now(),
        unix_now() + 60,
    );
    assert!(drive(claim, &node.state.get_ctx()).await.unwrap());
    assert_eq!(
        upload(&node, &visit, "a.txt", "one").await.unwrap(),
        "a (1).txt"
    );
    let (first, second) = tokio::join!(
        upload(&node, &visit, "b.txt", "first"),
        upload(&node, &visit, "b.txt", "second"),
    );
    let mut names = [first.unwrap(), second.unwrap()];
    names.sort();
    assert_eq!(names, ["b (1).txt", "b.txt"]);
    let reader = create(
        &node,
        &node.owner,
        folder_request("drop/", ShareModeBody::Read),
    )
    .await
    .unwrap();
    let read_visit = open(&node, &reader.token, None).await.unwrap();
    let (_, one) = download(&node, &read_visit, "b.txt").await.unwrap();
    let (_, two) = download(&node, &read_visit, "b (1).txt").await.unwrap();
    let mut bodies = [one, two];
    bodies.sort();
    assert_eq!(bodies, [b"first".to_vec(), b"second".to_vec()]);
}

#[tokio::test]
async fn only_uploads_take_large_bodies() {
    use tower::ServiceExt;
    let node = node_state().await;
    let link = create(
        &node,
        &node.owner,
        folder_request("drop/", ShareModeBody::Upload),
    )
    .await
    .unwrap();
    let visit = open(&node, &link.token, None).await.unwrap();
    let app = super::router()
        .split_for_parts()
        .0
        .with_state(node.state.clone());
    let large = "x".repeat(100 * 1024);
    let request = |method: &str, uri: &str, token: &str, body: String| {
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Share {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        request.extensions_mut().insert(peer());
        request
    };
    let password = format!("{{\"password\":\"{large}\"}}");
    let open = request("POST", "/shares/open", &link.token, password);
    let answer = app.clone().oneshot(open).await.unwrap();
    assert_eq!(answer.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let upload = request("PUT", "/shares/visit/uploads?name=big.txt", &visit, large);
    let answer = app.oneshot(upload).await.unwrap();
    assert_eq!(answer.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn ticket_ends_with_visit() {
    let node = node_state().await;
    put_text(&node, "shared/a.txt", "alpha").await;
    let link = create(
        &node,
        &node.owner,
        folder_request("shared/", ShareModeBody::Read),
    )
    .await
    .unwrap();
    let id = Ulid::from_string(&link.share.id).unwrap();
    let stored = drive(GetShareOperation::new(id), &node.state.get_ctx())
        .await
        .unwrap()
        .unwrap();
    let visit_end = unix_now() + 5;
    let short_visit = stored.visit_token(visit_end);
    let Json(ticket) = create_ticket(
        State(node.state.clone()),
        share_headers(&short_visit),
        Json(TicketRequest {
            name: "a.txt".into(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(ticket.expires_at, rfc3339(visit_end));
}

#[tokio::test]
async fn owner_sees_use_counts() {
    let node = node_state().await;
    put_text(&node, "drop/a.txt", "alpha").await;
    let link = create(
        &node,
        &node.owner,
        folder_request("drop/", ShareModeBody::ReadUpload),
    )
    .await
    .unwrap();
    assert_eq!((link.share.downloads, link.share.uploads), (0, 0));
    assert!(link.share.last_access_at.is_none());
    let visit = open(&node, &link.token, None).await.unwrap();
    download(&node, &visit, "a.txt").await.unwrap();
    download(&node, &visit, "a.txt").await.unwrap();
    upload(&node, &visit, "b.txt", "beta").await.unwrap();
    let Json(listed) = list_shares(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
    )
    .await
    .unwrap();
    let share = &listed.shares[0];
    assert_eq!((share.downloads, share.uploads), (2, 1));
    assert!(share.last_access_at.is_some());
    let Json(details) = get_share(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(link.share.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!((details.share.downloads, details.share.uploads), (2, 1));
}

#[tokio::test]
async fn deactivated_creator_ends_open() {
    use aruna_core::document::DocumentTarget;
    use aruna_core::effects::Effect;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::user::User;
    let node = node_state().await;
    let mut request = folder_request("shared/", ShareModeBody::Read);
    request.password = Some("correct horse".into());
    let link = create(&node, &node.owner, request).await.unwrap();
    let creator = DocumentTarget::User {
        user_id: node.owner.user_id,
    };
    let account = User {
        user_id: node.owner.user_id,
        name: "owner".into(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: [(
            aruna_core::user::validation::DEACTIVATED_ATTRIBUTE.to_string(),
            "true".to_string(),
        )]
        .into(),
    };
    let actor = Actor {
        node_id: node.state.get_node_id(),
        user_id: node.owner.user_id,
        realm_id: node.owner.realm_id,
    };
    let bytes = account.to_bytes(&actor).unwrap();
    let write = aruna_operations::document_repository::write_effect(&creator, bytes, None);
    let Effect::Storage(write) = write else {
        panic!("a user document is one storage write");
    };
    node.state
        .get_ctx()
        .storage_handle
        .send_storage_effect(write)
        .await;
    // No password step and no log entry: the link answers like an ended one.
    for password in [None, Some("wrong"), Some("correct horse")] {
        let error = open(&node, &link.token, password).await.unwrap_err();
        assert_eq!(code(error), "share_unavailable");
    }
    let Json(details) = get_share(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(link.share.id.clone()),
    )
    .await
    .unwrap();
    assert!(details.share.last_access_at.is_none());
    let Json(log) = share_log(
        State(node.state.clone()),
        Extension(Some(node.owner.clone())),
        Path(link.share.id.clone()),
        Query(ShareLogQuery::default()),
    )
    .await
    .unwrap();
    assert!(log.entries.is_empty());
}
