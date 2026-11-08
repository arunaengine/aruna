//! Tests selection downloads: refused selections, distributed folders, and archive links that
//! work once, expire, are replaced and keep the creator's token checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::mcp::data::{WriteObjectInput, store_text};
use crate::routes::storage::deletion::bucket_info;
use crate::tests::assistant::spawn_mock;
use crate::tests::distributed::{
    BUCKET, CLOSED_API, Node, PEER, distribute, mock_peer, node_id, node_state, publish_api,
    publish_urls, replicate, unsigned_token,
};
use aruna_core::structs::identity::auth::{PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_operations::s3::object::archive::{EntryStatus, REPORT_PATH, Report};
use std::io::Read;

fn request(entries: Vec<ArchiveEntry>) -> ArchiveRequest {
    ArchiveRequest { entries }
}

fn entry(key: Option<&str>, prefix: Option<&str>, version_id: Option<&str>) -> ArchiveEntry {
    ArchiveEntry {
        bucket: BUCKET.to_string(),
        key: key.map(str::to_string),
        prefix: prefix.map(str::to_string),
        version_id: version_id.map(str::to_string),
    }
}

fn whole_bucket() -> ArchiveRequest {
    request(vec![entry(None, Some(""), None)])
}

async fn store(node: &Node, keys: &[&str]) {
    let info = bucket_info(&node.state, BUCKET).await.unwrap();
    for key in keys {
        let input = WriteObjectInput {
            node_id: None,
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            text: format!("text of {key}"),
            content_type: None,
        };
        store_text(&node.state, &node.owner, &info, input, Default::default())
            .await
            .unwrap();
    }
}

fn bearer(auth: &AuthContext) -> ValidatedBearer {
    ValidatedBearer::new_for_test(unsigned_token(auth))
}

/// The archive's files by name in archive order, without the report.
async fn files(response: Response) -> Vec<(String, Vec<u8>)> {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the archive streams to its end");
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid archive");
    let mut files = Vec::new();
    for index in 0..archive.len() {
        let mut file = archive.by_index(index).unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).unwrap();
        files.push((file.name().unwrap().to_string(), content));
    }
    files
}

/// A peer S3 endpoint that serves every GET as `version` with `body`, and stops on drop.
struct MockS3(String, tokio::task::JoinHandle<()>);

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.1.abort();
    }
}

async fn mock_s3(version: &'static str, body: &'static str) -> MockS3 {
    let answer = move || async move {
        let headers = [
            ("x-amz-version-id", version),
            ("content-type", "text/plain"),
        ];
        (StatusCode::OK, headers, body)
    };
    let (url, task) = spawn_mock(axum::Router::new().fallback(answer)).await;
    MockS3(url, task)
}

const REMOTE_VERSION: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

/// The archive's file names, without the report, and the report.
async fn unzip(response: Response) -> (Vec<String>, Report) {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the archive streams to its end");
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid archive");
    let mut names: Vec<String> = archive
        .file_names()
        .map(|name| name.expect("utf-8 name").into_owned())
        .collect();
    names.sort();
    let mut report = Vec::new();
    let mut file = archive.by_name(REPORT_PATH).expect("report");
    file.read_to_end(&mut report).unwrap();
    names.retain(|name| name != REPORT_PATH);
    (names, serde_json::from_slice(&report).unwrap())
}

async fn archive(node: &Node, auth: &AuthContext, body: ArchiveRequest) -> ServerResult<Response> {
    let bearer = Some(bearer(auth));
    let state = node.state.clone();
    download_selection(
        State(state),
        Extension(Some(auth.clone())),
        Extension(bearer),
        Json(body),
    )
    .await
}

async fn link(node: &Node, auth: &AuthContext, body: ArchiveRequest) -> ServerResult<Response> {
    let bearer = Some(bearer(auth));
    let state = node.state.clone();
    create_link(
        State(state),
        Extension(Some(auth.clone())),
        Extension(bearer),
        Json(body),
    )
    .await
}

async fn redeem(node: &Node, ticket: &str) -> ServerResult<Response> {
    let query = LinkQuery {
        ticket: ticket.to_string(),
    };
    download_link(State(node.state.clone()), Query(query)).await
}

/// The ticket of a link response, after checking the response keeps it out of caches and logs.
async fn ticket_of(response: Response) -> String {
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: LinkResponse = serde_json::from_slice(&bytes).unwrap();
    let (route, ticket) = body.path.split_once("?ticket=").expect("a query ticket");
    assert_eq!(route, DOWNLOAD_PATH, "the route path is fixed");
    assert!(!route.contains(ticket));
    ticket.to_string()
}

#[tokio::test]
async fn refuses_bad_selections() {
    let node = node_state().await;
    let auth = node.owner.clone();
    let anonymous = download_selection(
        State(node.state.clone()),
        Extension(None),
        Extension(None),
        Json(whole_bucket()),
    )
    .await;
    assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    let refused = [
        request(Vec::new()),
        request(vec![entry(Some("a"), Some("b"), None)]),
        request(vec![entry(None, None, None)]),
        request(vec![entry(
            None,
            Some("a/"),
            Some("01JVERS0123456789ABCDEFGHJ"),
        )]),
        request(vec![entry(Some("a"), None, Some("not-a-ulid"))]),
        request(vec![ArchiveEntry {
            bucket: "_x".to_string(),
            ..entry(Some("a"), None, None)
        }]),
    ];
    for body in refused {
        let answer = archive(&node, &auth, body.clone()).await;
        assert!(
            matches!(answer, Err(ServerError::BadRequestReason(_))),
            "{body:?}"
        );
        let answer = link(&node, &auth, body).await;
        assert!(matches!(answer, Err(ServerError::BadRequestReason(_))));
    }
    let no_bearer = create_link(
        State(node.state.clone()),
        Extension(Some(auth)),
        Extension(None),
        Json(whole_bucket()),
    )
    .await;
    assert!(matches!(no_bearer, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn distributed_folder_ranges() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md"]).await;
    let snapshot = distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":[],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    publish_api(&node.state, node_id(PEER), &peer.url).await;
    let response = archive(&node, &node.owner, whole_bucket()).await.unwrap();
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    let (names, report) = unzip(response).await;
    assert_eq!(names, ["routed/a.md", "routed/b.md"]);
    // The peer's range is merged in; its key is held there, so it is reported, not read here.
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].key.as_deref(), Some("x.md"));
    assert_eq!(report.entries[0].status, EntryStatus::Unavailable);
    assert!(peer.paths.lock().unwrap()[0].contains("lower=m"));

    // A range no holder answers is one row, and the rest of the folder still streams.
    publish_api(&node.state, node_id(PEER), CLOSED_API).await;
    let response = archive(&node, &node.owner, whole_bucket()).await.unwrap();
    let (names, report) = unzip(response).await;
    assert_eq!(names, ["routed/a.md", "routed/b.md"]);
    let range_id = snapshot.config.ranges[1].range_id.to_string();
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].range_id, Some(range_id));
    assert_eq!(report.entries[0].prefix.as_deref(), Some(""));
    assert_eq!(report.entries[0].status, EntryStatus::Unavailable);
    assert_eq!((report.totals.included, report.totals.unavailable), (2, 1));
}

// A healthy range another member coordinates is read there with the caller's credential.
#[tokio::test]
async fn remote_range_read() {
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    distribute(&node.state, node.group_id, BUCKET).await;
    let answer = r#"{"bucket":"routed","objects":[{"key":"x.md","size":1,"etag":null,
        "version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "last_modified":null,"content_type":null,"referenced":false,"content_id":null}],
        "prefixes":[],"next_cursor":null}"#;
    let peer = mock_peer(StatusCode::OK, answer).await;
    let s3 = mock_s3(REMOTE_VERSION, "remote x").await;
    publish_urls(&node.state, node_id(PEER), (&peer.url, Some(&s3.0))).await;
    let response = archive(&node, &node.owner, whole_bucket()).await.unwrap();
    let files = files(response).await;
    let names: Vec<_> = files.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, ["routed/a.md", "routed/x.md", REPORT_PATH]);
    assert_eq!(files[1].1, b"remote x");
    let report: Report = serde_json::from_slice(&files[2].1).unwrap();
    assert!(report.entries.is_empty(), "{:?}", report.entries);
    assert_eq!(report.totals.included, 2);
}

// A copy's head may be behind, so a current version comes from the coordinator; a copy answers
// only while the coordinator cannot, and the report says so.
#[tokio::test]
async fn coordinator_version_wins() {
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    let snapshot = replicate(&node.state, node.group_id, BUCKET).await;
    let s3 = mock_s3(REMOTE_VERSION, "coordinator").await;
    publish_urls(&node.state, node_id(PEER), (CLOSED_API, Some(&s3.0))).await;
    let file = || request(vec![entry(Some("a.md"), None, None)]);
    let current = files(archive(&node, &node.owner, file()).await.unwrap()).await;
    assert_eq!(
        current[0],
        ("routed/a.md".to_string(), b"coordinator".to_vec())
    );
    let report: Report = serde_json::from_slice(&current[1].1).unwrap();
    assert!(report.entries.is_empty());

    publish_urls(&node.state, node_id(PEER), (CLOSED_API, Some(CLOSED_API))).await;
    let response = archive(&node, &node.owner, file()).await.unwrap();
    let (names, report) = unzip(response).await;
    assert_eq!(names, ["routed/a.md"]);
    assert_eq!(report.entries.len(), 1);
    let row = &report.entries[0];
    assert_eq!(row.status, EntryStatus::Stale);
    assert_eq!(
        row.range_id,
        Some(snapshot.config.ranges[0].range_id.to_string())
    );
    assert_eq!(report.totals.stale, 1);
}

// A holder that refuses its range costs that range only, never the keys listed before it.
#[tokio::test]
async fn refused_range_continues() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md"]).await;
    let snapshot = distribute(&node.state, node.group_id, BUCKET).await;
    let refusal = mock_peer(StatusCode::FORBIDDEN, r#"{"error":"no grant"}"#).await;
    publish_api(&node.state, node_id(PEER), &refusal.url).await;
    let response = archive(&node, &node.owner, whole_bucket()).await.unwrap();
    let (names, report) = unzip(response).await;
    assert_eq!(names, ["routed/a.md", "routed/b.md"]);
    let range_id = snapshot.config.ranges[1].range_id.to_string();
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].range_id, Some(range_id));
    assert_eq!(report.entries[0].status, EntryStatus::Unavailable);
}

#[tokio::test]
async fn link_redeems_once() {
    let node = node_state().await;
    store(&node, &["a.md"]).await;
    let ticket = ticket_of(link(&node, &node.owner, whole_bucket()).await.unwrap()).await;
    let response = redeem(&node, &ticket).await.unwrap();
    assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    assert_eq!(unzip(response).await.0, ["routed/a.md"]);
    assert!(matches!(
        redeem(&node, &ticket).await,
        Err(ServerError::NotFound)
    ));
    assert!(matches!(
        redeem(&node, "forged").await,
        Err(ServerError::NotFound)
    ));

    // A newer link of the same user replaces the older one.
    let older = ticket_of(link(&node, &node.owner, whole_bucket()).await.unwrap()).await;
    let newer = ticket_of(link(&node, &node.owner, whole_bucket()).await.unwrap()).await;
    assert!(matches!(
        redeem(&node, &older).await,
        Err(ServerError::NotFound)
    ));
    assert!(redeem(&node, &newer).await.is_ok());
}

fn pending(auth: &AuthContext, grant_expiry: u64) -> PendingLink {
    let token = unsigned_token(auth);
    let mut grant = token_grant(&token).unwrap();
    grant.expires_at_secs = grant_expiry;
    PendingLink {
        auth: auth.clone(),
        grant,
        bearer: ValidatedBearer::new_for_test(token),
        entries: Vec::new(),
        expires_at: 0,
    }
}

#[test]
fn links_expire() {
    let links = ArchiveLinks::default();
    let (realm_id, now) = (RealmId([3; 32]), 1_000);
    let user = |seed: u8| AuthContext {
        user_id: UserId::local(ulid::Ulid::from_bytes([seed; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let (first, second) = (user(1), user(2));
    let (ticket, expires_at) = links.create(pending(&first, u64::MAX), now);
    assert_eq!(expires_at, now + LINK_SECS);
    assert_eq!(links.creator(&ticket, now + 59), Some(first.user_id));
    assert!(links.creator(&ticket, now + 60).is_none());
    assert!(links.redeem(&ticket, now + 60).is_none());
    // The credential's expiry ends a link sooner, and users do not replace each other's links.
    let (short, expires_at) = links.create(pending(&first, now + 10), now);
    let (other, _) = links.create(pending(&second, u64::MAX), now);
    assert_eq!(expires_at, now + 10);
    assert!(links.creator(&short, now + 10).is_none());
    assert_eq!(links.creator(&other, now + 10), Some(second.user_id));
}

#[tokio::test]
async fn revoked_token_refused() {
    let node = node_state().await;
    // A token whose realm has no configuration here counts as revoked.
    let foreign = AuthContext {
        realm_id: RealmId([9; 32]),
        ..node.owner.clone()
    };
    let mut link = pending(&foreign, u64::MAX);
    link.auth = node.owner.clone();
    let (ticket, _) = node.state.archive_links().create(link, unix_now());
    assert!(matches!(
        redeem(&node, &ticket).await,
        Err(ServerError::Unauthorized)
    ));
    assert!(matches!(
        redeem(&node, &ticket).await,
        Err(ServerError::NotFound)
    ));
}

#[tokio::test]
async fn restricted_link_kept() {
    let node = node_state().await;
    store(&node, &["a.md", "b.md"]).await;
    let (realm_id, node_id) = (node.state.get_realm_id(), node.state.get_node_id());
    let pattern = object_permission_path(realm_id, node.group_id, node_id, BUCKET, "a.md");
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern,
            permission: Permission::READ,
        }]),
        ..node.owner.clone()
    };
    let files = request(vec![
        entry(Some("a.md"), None, None),
        entry(Some("b.md"), None, None),
    ]);
    let ticket = ticket_of(link(&node, &restricted, files).await.unwrap()).await;
    let (names, report) = unzip(redeem(&node, &ticket).await.unwrap()).await;
    assert_eq!(names, ["routed/a.md"]);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].key.as_deref(), Some("b.md"));
    assert_eq!(report.entries[0].status, EntryStatus::Denied);
}
