//! Tests share links on a distributed bucket: the member a link was made on serves the visit,
//! and the keys of another member's range are listed, read and written at that member.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::visit::{
    DownloadQuery, OpenShareRequest, TicketRequest, UploadQuery, VisitListQuery, VisitListing,
    create_ticket, download_file, list_files, open_share, upload_file,
};
use super::*;
use crate::tests::members::{BUCKET, Members, members};
use crate::tests::routes::test_state;
use aruna_core::structs::identity::auth::NodeCapabilities;
use axum::body::{Body, to_bytes};
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

async fn ticket(shared: &Shared, visit: &str, name: &str) -> ServerResult<String> {
    let request = TicketRequest {
        name: name.to_string(),
    };
    let state = State(shared.state.clone());
    let Json(ticket) = create_ticket(state, share_headers(visit), Json(request)).await?;
    Ok(ticket.path.split_once("ticket=").unwrap().1.to_string())
}

async fn fetch(shared: &Shared, ticket: &str) -> ServerResult<Vec<u8>> {
    let query = Query(DownloadQuery {
        ticket: ticket.to_string(),
    });
    let state = State(shared.state.clone());
    let response = download_file(state, peer(), HeaderMap::new(), query).await?;
    Ok(to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec())
}

async fn upload(shared: &Shared, visit: &str, name: &str, text: &str) -> ServerResult<String> {
    let query = Query(UploadQuery {
        name: name.to_string(),
    });
    let mut headers = share_headers(visit);
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(text.len()));
    let body = Body::from(text.to_string());
    let state = State(shared.state.clone());
    let uploaded = upload_file(state, peer(), headers, query, body).await;
    uploaded.map(|(_, Json(uploaded))| uploaded.name)
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

// The serving member decides the keys another member lists as it decides its own tickets: a
// share policy on this member hides them, and no cursor names a hidden entry.
#[tokio::test]
async fn serving_policy_hides() {
    use aruna_operations::s3::object::pages::{Resume, decode_resume};
    let shared = shared().await;
    put(&shared, 1, "private/secret.txt", "closed").await;
    put(&shared, 1, "public.txt", "open").await;
    let expression = "request.session.kind == 'share' && path.contains('/private/')";
    shared.members.deny_on(0, expression).await;
    let (_, visit) = visit(&shared, ShareModeBody::Read).await;
    let error = ticket(&shared, &visit, "private/secret.txt")
        .await
        .unwrap_err();
    assert_eq!(code(error), "share_unavailable");
    assert_eq!(
        names(&shared, &visit, "", 100).await.unwrap(),
        ["public.txt"]
    );
    let listed = page(&shared, &visit, ("private/", None, None))
        .await
        .unwrap();
    assert!(listed.files.is_empty() && listed.folders.is_empty());
    let (mut cursor, mut shown) = (None, Vec::new());
    loop {
        let listed = page(&shared, &visit, ("", cursor, Some(1))).await.unwrap();
        shown.extend(listed.files.into_iter().map(|file| file.name));
        shown.extend(listed.folders);
        let Some(next) = listed.next_cursor else {
            break;
        };
        if let Ok(Resume::After { key, group }) = decode_resume::<()>(&next) {
            assert!(
                !key.contains("private") && group.is_none_or(|group| !group.contains("private"))
            );
        }
        cursor = Some(next);
    }
    assert_eq!(shown, ["public.txt"]);
}

// A range whose coordinator is down cannot be listed or written, and the visitor is told to
// retry; this member's own range still takes uploads.
#[tokio::test]
async fn coordinator_down_refused() {
    let mut shared = shared().await;
    put(&shared, 0, "a.txt", "alpha").await;
    let (_, visit) = visit(&shared, ShareModeBody::ReadUpload).await;
    shared.members.nodes[1].stop().await;
    let error = page(&shared, &visit, ("", None, None)).await.unwrap_err();
    assert_eq!(code(error), "range_unavailable");
    let error = upload(&shared, &visit, "x.txt", "far").await.unwrap_err();
    assert_eq!(code(error), "range_unavailable");
    assert_eq!(
        upload(&shared, &visit, "b.txt", "near").await.unwrap(),
        "b.txt"
    );
}

// An upload to another member's range lands at its coordinator and never replaces a live
// key there; a deleted key takes its name again.
#[tokio::test]
async fn uploads_reach_coordinator() {
    let shared = shared().await;
    put(&shared, 1, "y.txt", "kept").await;
    put(&shared, 1, "z.txt", "deleted").await;
    let input = s3s::dto::DeleteObjectInput {
        bucket: BUCKET.to_string(),
        key: "z.txt".to_string(),
        ..Default::default()
    };
    let request = s3_request(&shared, 1, input, http::Method::DELETE).await;
    shared.members.nodes[1]
        .service
        .delete_object(request)
        .await
        .unwrap();
    let (link, visit) = visit(&shared, ShareModeBody::Upload).await;
    assert_eq!(
        upload(&shared, &visit, "x.txt", "new").await.unwrap(),
        "x.txt"
    );
    assert_eq!(
        upload(&shared, &visit, "y.txt", "other").await.unwrap(),
        "y (1).txt"
    );
    assert_eq!(
        upload(&shared, &visit, "z.txt", "again").await.unwrap(),
        "z.txt"
    );
    assert_eq!(stored(&shared, 1, "x.txt").await.unwrap(), b"new");
    assert_eq!(stored(&shared, 1, "y.txt").await.unwrap(), b"kept");
    assert_eq!(stored(&shared, 1, "y (1).txt").await.unwrap(), b"other");
    assert_eq!(stored(&shared, 1, "z.txt").await.unwrap(), b"again");
    let state = State(shared.state.clone());
    let auth = Extension(Some(shared.owner.clone()));
    let query = Query(ShareLogQuery::default());
    let Json(log) = share_log(state, auth, Path(link.share.id), query)
        .await
        .unwrap();
    let sizes: Vec<_> = log.entries.iter().filter_map(|entry| entry.bytes).collect();
    assert_eq!(sizes, [3, 5, 5]);
}

/// Gives the group a storage quota of `bytes` on member `on` only.
async fn quota_on(shared: &Shared, on: usize, bytes: u64) {
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    let members = &shared.members;
    let mut config = RealmConfigDocument::default_for_realm(members.realm_id, Vec::new());
    for node in &members.nodes {
        config.ensure_node(node.node_id(), RealmNodeKind::Server);
    }
    config.quota.default_quota_bytes = Some(bytes);
    config.quota.grace_factor_percent = 100;
    let node = &members.nodes[on];
    let actor = Actor {
        node_id: node.node_id(),
        user_id: members.user.user_identity,
        realm_id: members.realm_id,
    };
    let value = config.to_bytes(&actor).unwrap();
    let key = (*members.realm_id.as_bytes()).into();
    let space = aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    crate::tests::routes::write_doc(&node.context, space, key, value.into()).await;
}

// The coordinator charges an upload to its own quota: a quota there refuses a file for its
// range, and this member, without one, still takes its own keys.
#[tokio::test]
async fn quota_on_coordinator() {
    let shared = shared().await;
    quota_on(&shared, 1, 4).await;
    let (_, visit) = visit(&shared, ShareModeBody::Upload).await;
    let error = upload(&shared, &visit, "x.txt", "too large")
        .await
        .unwrap_err();
    assert_eq!(code(error), "conflict");
    assert!(stored(&shared, 1, "x.txt").await.is_none());
    assert_eq!(
        upload(&shared, &visit, "a.txt", "too large").await.unwrap(),
        "a.txt"
    );
}

// A key another member coordinates downloads through the link's member, and the owner's log
// records its size there.
#[tokio::test]
async fn remote_key_downloads() {
    let shared = shared().await;
    put(&shared, 1, "y.txt", "far away").await;
    let (link, visit) = visit(&shared, ShareModeBody::Read).await;
    let ticket = ticket(&shared, &visit, "y.txt").await.unwrap();
    assert_eq!(fetch(&shared, &ticket).await.unwrap(), b"far away");
    assert_eq!(
        code(fetch(&shared, &ticket).await.unwrap_err()),
        "share_unavailable"
    );
    assert_eq!(
        code(ticket_error(&shared, &visit, "gone.txt").await),
        "not_found"
    );
    let state = State(shared.state.clone());
    let auth = Extension(Some(shared.owner.clone()));
    let query = Query(ShareLogQuery::default());
    let Json(log) = share_log(state, auth, Path(link.share.id), query)
        .await
        .unwrap();
    let download = log
        .entries
        .iter()
        .find(|entry| entry.action == "download")
        .unwrap();
    assert_eq!(download.bytes, Some(8));
}

async fn ticket_error(shared: &Shared, visit: &str, name: &str) -> ServerError {
    ticket(shared, visit, name).await.unwrap_err()
}

// Revoking or expiring the link on its member ends the next request for another member's key:
// a ticket already handed out and a listing alike.
#[tokio::test]
async fn ended_link_refused() {
    use aruna_core::effects::Effect;
    use aruna_operations::shares::store::save_effect;
    let shared = shared().await;
    put(&shared, 1, "y.txt", "far").await;
    let (link, opened) = visit(&shared, ShareModeBody::Read).await;
    let issued = ticket(&shared, &opened, "y.txt").await.unwrap();
    let state = State(shared.state.clone());
    let auth = Extension(Some(shared.owner.clone()));
    revoke_share(state, auth, Path(link.share.id))
        .await
        .unwrap();
    assert_eq!(
        code(fetch(&shared, &issued).await.unwrap_err()),
        "share_unavailable"
    );
    let listed = page(&shared, &opened, ("", None, None)).await.unwrap_err();
    assert_eq!(code(listed), "share_unavailable");

    let (link, opened) = visit(&shared, ShareModeBody::Read).await;
    let issued = ticket(&shared, &opened, "y.txt").await.unwrap();
    let id = Ulid::from_string(&link.share.id).unwrap();
    let context = shared.state.get_ctx();
    let mut stored = drive(GetShareOperation::new(id), &context)
        .await
        .unwrap()
        .unwrap();
    stored.expires_at = Some(unix_now() - 1);
    let Effect::Storage(write) = save_effect(&stored).unwrap() else {
        panic!("a link record is one storage write");
    };
    context.storage_handle.send_storage_effect(write).await;
    assert_eq!(
        code(fetch(&shared, &issued).await.unwrap_err()),
        "share_unavailable"
    );
    let listed = page(&shared, &opened, ("", None, None)).await.unwrap_err();
    assert_eq!(code(listed), "share_unavailable");
}

/// A peer S3 endpoint that lists `x/a.txt` for every listing and serves it for every read; a
/// request whose delegation names another group than `group` is denied, as a member does.
async fn listing_peer(group: Ulid) -> (String, tokio::task::JoinHandle<()>) {
    use aruna_core::structs::storage::distribution::delegation::{
        DELEGATION_HEADER, SignedDelegation,
    };
    let answer = move |uri: http::Uri, headers: HeaderMap| async move {
        let delegation = (headers.get(DELEGATION_HEADER))
            .and_then(|value| SignedDelegation::decode(value.to_str().ok()?).ok());
        if delegation.is_none_or(|signed| signed.delegation.group_id != group) {
            let denied = "<Error><Code>AccessDenied</Code><Message>group</Message></Error>";
            return (
                http::StatusCode::FORBIDDEN,
                [("x-amz-version-id", "")],
                denied,
            );
        }
        let listing = uri
            .query()
            .is_some_and(|query| query.contains("list-type=2"));
        let body = match listing {
            true => {
                "<ListBucketResult><Contents><Key>x/a.txt</Key><Size>6</Size>\
                     <LastModified>2026-10-30T08:00:00.000Z</LastModified></Contents>\
                     </ListBucketResult>"
            }
            false => "remote",
        };
        let version = [("x-amz-version-id", "01ARZ3NDEKTSV4RRFFQ69G5FAV")];
        (http::StatusCode::OK, version, body)
    };
    crate::tests::assistant::spawn_mock(axum::Router::new().fallback(answer)).await
}

/// Shares a dataset whose folder part lies in another member's range of a bucket in
/// `bucket_group`, then browses and downloads that part through the link's member.
async fn member_part(
    test: &crate::routes::metadata::tests::TestState,
    bucket_group: Ulid,
) -> ServerResult<Vec<u8>> {
    use super::dataset::{
        DatasetTicketRequest, PartQuery, dataset_ticket, list_part, read_dataset,
    };
    use crate::tests::distributed::{PEER, distribute, node_id, publish_urls};
    let state = test.state.clone();
    distribute(&state, bucket_group, "spire").await;
    let (url, server) = listing_peer(bucket_group).await;
    publish_urls(&state, node_id(PEER), (&url, Some(&url))).await;
    let folder = format!(
        "arn:aruna:{}:{}:s3/spire/x/",
        state.get_realm_id(),
        state.get_node_id()
    );
    let document_id = crate::tests::datasets::describe(test, "/shared", false, &folder).await;
    let request = CreateShareRequest {
        target: ShareTargetBody::Dataset {
            dataset_id: document_id.to_string(),
        },
        mode: ShareModeBody::Read,
        label: String::new(),
        expires_at: None,
        password: None,
    };
    let auth = Extension(Some(test.auth.clone()));
    let (_, Json(link)) = create_share(State(state.clone()), auth, Json(request)).await?;
    let open = Some(Json(OpenShareRequest::default()));
    let headers = share_headers(&link.token);
    let opened = open_share(State(state.clone()), peer(), headers, open).await;
    let visit = opened?.0.visit_token;
    let Json(dataset) = read_dataset(State(state.clone()), share_headers(&visit)).await?;
    assert_eq!(dataset.files.len(), 1, "{:?}", dataset.files);
    assert_eq!(dataset.files[0].kind, "folder");
    let part = dataset.files[0].entity_id.clone();
    let query = Query(PartQuery {
        part: part.clone(),
        prefix: None,
        cursor: None,
        limit: None,
    });
    let Json(listed) = list_part(State(state.clone()), share_headers(&visit), query).await?;
    let names: Vec<_> = listed.files.iter().map(|file| file.name.as_str()).collect();
    assert_eq!(names, ["a.txt"]);
    let request = Json(DatasetTicketRequest {
        entity_id: part,
        name: Some("a.txt".to_string()),
    });
    let ticket = dataset_ticket(State(state.clone()), share_headers(&visit), request).await;
    let Json(ticket) = ticket?;
    let ticket = ticket.path.split_once("ticket=").unwrap().1.to_string();
    let query = Query(DownloadQuery { ticket });
    let response = download_file(State(state.clone()), peer(), HeaderMap::new(), query).await;
    let bytes = to_bytes(response?.into_body(), usize::MAX).await.unwrap();
    server.abort();
    Ok(bytes.to_vec())
}

// A dataset whose folder part lies in another member's range is browsed and downloaded
// through the link's member.
#[tokio::test]
async fn dataset_member_folder() {
    let test = crate::routes::metadata::tests::setup_network_state().await;
    let group = test.group_id;
    assert_eq!(member_part(&test, group).await.unwrap(), b"remote");
}

// A part in a bucket of another group than the dataset's is read as that bucket's group, which
// the member checks the delegation against.
#[tokio::test]
async fn dataset_part_group() {
    use aruna_core::structs::identity::auth::Actor;
    let test = crate::routes::metadata::tests::setup_network_state().await;
    let other = Ulid::from_bytes([31; 16]);
    let actor = Actor {
        node_id: test.state.get_node_id(),
        user_id: test.auth.user_id,
        realm_id: test.auth.realm_id,
    };
    let context = test.state.get_ctx();
    let (realm, owner) = (test.auth.realm_id, test.auth.user_id);
    crate::tests::routes::seed_group_docs(&context, realm, &actor, other, "other", owner).await;
    assert_eq!(member_part(&test, other).await.unwrap(), b"remote");
}

/// Indexes a new version of `x/a.txt` with content `hash` in `set_id` on this member, as
/// replication of the other member's range leaves it, and returns its id.
async fn index_content(state: &ServerState, set_id: Ulid, hash: [u8; 32]) -> Ulid {
    use crate::tests::distributed::write;
    use aruna_core::keyspaces::{CONTENT_INDEX_KEYSPACE, VERSION_INDEX_KEYSPACE};
    use aruna_core::structs::storage::distribution::hint::mint_version;
    use aruna_core::structs::storage::distribution::inventory::{
        IndexEntry, VersionEntry, content_key, index_key, version_position,
    };
    use aruna_core::structs::storage::distribution::{OriginalEpoch, SetHint};
    let version = mint_version(Some(SetHint::of_set(set_id)));
    let position = version_position(set_id, "x/a.txt", OriginalEpoch(1), 1, version);
    let entry = IndexEntry {
        position,
        version: VersionEntry::default(),
    };
    let index = index_key(set_id, version);
    write(
        state,
        VERSION_INDEX_KEYSPACE,
        index,
        entry.to_bytes().unwrap(),
    )
    .await;
    let content = content_key(set_id, &hash, version);
    write(state, CONTENT_INDEX_KEYSPACE, content, Vec::new()).await;
    version
}

// A part named only by its global version or content hash is served when this node is a bound
// member of the set it resolves to, though no other part names that set or bucket.
#[tokio::test]
async fn dataset_global_parts() {
    use super::dataset::{DatasetTicketRequest, dataset_ticket, read_dataset};
    use crate::tests::distributed::{PEER, distribute, node_id, publish_urls};
    use aruna_core::structs::storage::data::identity::content_id;
    use aruna_core::structs::storage::global_id::GlobalId;
    let test = crate::routes::metadata::tests::setup_network_state().await;
    let state = test.state.clone();
    let set_id = distribute(&state, test.group_id, "spire").await.set_id();
    let hash = [9; 32];
    let version = index_content(&state, set_id, hash).await;
    let (url, server) = listing_peer(test.group_id).await;
    publish_urls(&state, node_id(PEER), (&url, Some(&url))).await;
    let global = GlobalId::Version {
        realm_id: state.get_realm_id(),
        version_id: version,
    }
    .to_url();
    let parts = [(global.as_str(), "File"), (&content_id(hash), "File")];
    let describe = crate::tests::datasets::describe_parts(&test, "/shared", false, &parts);
    let document_id = describe.await;
    let request = CreateShareRequest {
        target: ShareTargetBody::Dataset {
            dataset_id: document_id.to_string(),
        },
        mode: ShareModeBody::Read,
        label: String::new(),
        expires_at: None,
        password: None,
    };
    let auth = Extension(Some(test.auth.clone()));
    let (_, Json(link)) = create_share(State(state.clone()), auth, Json(request))
        .await
        .unwrap();
    let open = Some(Json(OpenShareRequest::default()));
    let opened = open_share(
        State(state.clone()),
        peer(),
        share_headers(&link.token),
        open,
    )
    .await;
    let visit = opened.unwrap().0.visit_token;
    let Json(dataset) = read_dataset(State(state.clone()), share_headers(&visit))
        .await
        .unwrap();
    let mut ids: Vec<_> = (dataset.files.iter())
        .map(|file| file.entity_id.clone())
        .collect();
    ids.sort();
    let mut expected = [global.clone(), content_id(hash)];
    expected.sort();
    assert_eq!(ids, expected);
    let request = Json(DatasetTicketRequest {
        entity_id: global.clone(),
        name: None,
    });
    let ticket = dataset_ticket(State(state.clone()), share_headers(&visit), request).await;
    let ticket = ticket
        .unwrap()
        .0
        .path
        .split_once("ticket=")
        .unwrap()
        .1
        .to_string();
    let query = Query(DownloadQuery { ticket });
    let response = download_file(State(state.clone()), peer(), HeaderMap::new(), query).await;
    let bytes = to_bytes(response.unwrap().into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"remote");
    server.abort();
}

// A folder whose name holds glob characters is listed at the member that holds it, which
// admits the link's escaped folder scope as that literal folder.
#[tokio::test]
async fn escaped_folder_listed() {
    let shared = shared().await;
    put(&shared, 1, "results [1]/a.txt", "far").await;
    put(&shared, 1, "results [2]/b.txt", "other").await;
    let request = CreateShareRequest {
        target: ShareTargetBody::Folder {
            bucket: BUCKET.to_string(),
            prefix: "results [1]/".to_string(),
        },
        mode: ShareModeBody::Read,
        label: String::new(),
        expires_at: None,
        password: None,
    };
    let state = State(shared.state.clone());
    let auth = Extension(Some(shared.owner.clone()));
    let (_, Json(link)) = create_share(state, auth, Json(request)).await.unwrap();
    let open = Some(Json(OpenShareRequest::default()));
    let opened = open_share(
        State(shared.state.clone()),
        peer(),
        share_headers(&link.token),
        open,
    );
    let visit = opened.await.unwrap().0.visit_token;
    assert_eq!(names(&shared, &visit, "", 100).await.unwrap(), ["a.txt"]);
}

// Identical content in several sets is served from the first one the link's creator may read;
// a set the creator cannot read is passed over, though it resolves first.
#[tokio::test]
async fn content_skips_unreadable() {
    use super::dataset::{DatasetTicketRequest, dataset_ticket, read_dataset};
    use crate::tests::distributed::{LOCAL, PEER, distribute, node_id, publish_urls};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::storage::data::identity::content_id;
    use aruna_core::structs::storage::distribution::SetRoot;
    let test = crate::routes::metadata::tests::setup_network_state().await;
    let state = test.state.clone();
    let set_of = |owner_group| {
        let publisher = node_id(LOCAL);
        let nonce = [7; 16];
        SetRoot {
            publisher,
            owner_group,
            nonce,
        }
        .set_id()
    };
    // Sets resolve in id order, so the closed group's set must sort before the open one.
    let open_set = set_of(test.group_id);
    let closed = (1..=u8::MAX)
        .map(|seed| Ulid::from_bytes([seed; 16]))
        .find(|group| set_of(*group) < open_set)
        .unwrap();
    let stranger = aruna_core::UserId::local(Ulid::from_bytes([98; 16]), test.auth.realm_id);
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: stranger,
        realm_id: test.auth.realm_id,
    };
    let (context, realm) = (state.get_ctx(), test.auth.realm_id);
    crate::tests::routes::seed_group_docs(&context, realm, &actor, closed, "closed", stranger)
        .await;
    let hash = [9; 32];
    for (group, bucket) in [(closed, "closed"), (test.group_id, "spire")] {
        let set_id = distribute(&state, group, bucket).await.set_id();
        index_content(&state, set_id, hash).await;
    }
    let (url, server) = listing_peer(test.group_id).await;
    publish_urls(&state, node_id(PEER), (&url, Some(&url))).await;
    let content = content_id(hash);
    let parts = [(content.as_str(), "File")];
    let describe = crate::tests::datasets::describe_parts(&test, "/shared", false, &parts);
    let document_id = describe.await;
    let request = CreateShareRequest {
        target: ShareTargetBody::Dataset {
            dataset_id: document_id.to_string(),
        },
        mode: ShareModeBody::Read,
        label: String::new(),
        expires_at: None,
        password: None,
    };
    let auth = Extension(Some(test.auth.clone()));
    let (_, Json(link)) = create_share(State(state.clone()), auth, Json(request))
        .await
        .unwrap();
    let open = Some(Json(OpenShareRequest::default()));
    let opened = open_share(
        State(state.clone()),
        peer(),
        share_headers(&link.token),
        open,
    )
    .await;
    let visit = opened.unwrap().0.visit_token;
    let Json(dataset) = read_dataset(State(state.clone()), share_headers(&visit))
        .await
        .unwrap();
    let ids: Vec<_> = (dataset.files.iter())
        .map(|file| file.entity_id.clone())
        .collect();
    assert_eq!(ids, std::slice::from_ref(&content));
    let request = Json(DatasetTicketRequest {
        entity_id: content,
        name: None,
    });
    let ticket = dataset_ticket(State(state.clone()), share_headers(&visit), request).await;
    let ticket = ticket
        .unwrap()
        .0
        .path
        .split_once("ticket=")
        .unwrap()
        .1
        .to_string();
    let query = Query(DownloadQuery { ticket });
    let response = download_file(State(state.clone()), peer(), HeaderMap::new(), query).await;
    let bytes = to_bytes(response.unwrap().into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"remote");
    server.abort();
}
