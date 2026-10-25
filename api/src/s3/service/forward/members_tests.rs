//! Tests forwarding between in-process members of one distributed bucket: writes to the
//! range's coordinator, reads from a holder, copies, uploads and merged listings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use http::{Extensions, HeaderMap, Method, Uri};
use s3s::dto::{GetObjectInput, PutObjectInput, StreamingBlob};
use s3s::{S3, S3Request};

use aruna_operations::s3::object::delete::bulk::BulkDeleteEntry;

use super::members::{BUCKET, Members, members};

/// A request the access hook admitted for `user` on `bucket`.
fn request<T>(input: T, method: Method, user: UserAccess, bucket: BucketInfo) -> S3Request<T> {
    let mut extensions = Extensions::new();
    extensions.insert(user);
    extensions.insert(bucket);
    extensions.insert(PolicyRequestExtras::rest());
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions,
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn body_of(blob: Option<StreamingBlob>) -> Vec<u8> {
    use futures_util::TryStreamExt;
    let chunks: Vec<bytes::Bytes> = blob.unwrap().try_collect().await.unwrap();
    chunks.concat()
}

/// Puts `body` as `key` through node `on`.
async fn put(members: &Members, on: usize, key: &str, body: &[u8]) -> String {
    let node = &members.nodes[on];
    let input = PutObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        content_length: Some(body.len() as i64),
        body: Some(StreamingBlob::from(bytes::Bytes::copy_from_slice(body))),
        ..Default::default()
    };
    let put = request(input, Method::PUT, members.user_on(on), node.bucket().await);
    let output = node.service.put_object(put).await.unwrap().output;
    output
        .version_id
        .expect("the coordinator names the version")
}

// A write to a key another member coordinates lands there, and a read from a member keeping
// no records of the range is answered by that holder.
#[tokio::test]
async fn forwards_put_get() {
    let members = members(3, &["h", "p"], 0).await;
    let version = put(&members, 0, "k/one", b"forwarded").await;

    let coordinator = &members.nodes[1];
    let local = GetObjectInput {
        bucket: BUCKET.to_string(),
        key: "k/one".to_string(),
        ..Default::default()
    };
    let get = request(
        local.clone(),
        Method::GET,
        members.user_on(1),
        coordinator.bucket().await,
    );
    let output = coordinator.service.get_object(get).await.unwrap().output;
    assert_eq!(output.version_id.as_deref(), Some(version.as_str()));
    assert_eq!(body_of(output.body).await, b"forwarded");

    let entry = &members.nodes[0];
    let get = request(local, Method::GET, members.user_on(0), entry.bucket().await);
    let output = entry.service.get_object(get).await.unwrap().output;
    assert_eq!(output.version_id.as_deref(), Some(version.as_str()));
    assert_eq!(body_of(output.body).await, b"forwarded");
}

fn get_input(key: &str) -> GetObjectInput {
    GetObjectInput {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        ..Default::default()
    }
}

/// The error a GET of `key` through node `on` answers with.
async fn get_error(members: &Members, on: usize, key: &str) -> s3s::S3Error {
    let node = &members.nodes[on];
    let get = request(
        get_input(key),
        Method::GET,
        members.user_on(on),
        node.bucket().await,
    );
    match node.service.get_object(get).await {
        Ok(_) => panic!("{key} is still readable"),
        Err(error) => error,
    }
}

// A HEAD through a member keeping no records of the range is the holder's answer.
#[tokio::test]
async fn head_through_holder() {
    let members = members(3, &["h", "p"], 0).await;
    let version = put(&members, 1, "q/far", b"held").await;
    let node = &members.nodes[0];
    let input = s3s::dto::HeadObjectInput {
        bucket: BUCKET.to_string(),
        key: "q/far".to_string(),
        ..Default::default()
    };
    let head = request(input, Method::HEAD, members.user_on(0), node.bucket().await);
    let output = node.service.head_object(head).await.unwrap().output;
    assert_eq!(output.content_length, Some(4));
    assert_eq!(output.version_id.as_deref(), Some(version.as_str()));
}

// A delete of a key another member coordinates sets its marker there, and a bulk delete
// keeps local keys and exact versions here while each foreign key goes to its coordinator.
#[tokio::test]
async fn deletes_reach_coordinators() {
    use s3s::dto::DeleteObjectInput;
    let members = members(3, &["h", "p"], 0).await;
    for key in ["a/near", "k/one", "k/two", "q/far"] {
        put(&members, 0, key, b"doomed").await;
    }
    let node = &members.nodes[0];
    let input = DeleteObjectInput {
        bucket: BUCKET.to_string(),
        key: "k/one".to_string(),
        ..Default::default()
    };
    let delete = request(
        input,
        Method::DELETE,
        members.user_on(0),
        node.bucket().await,
    );
    let output = node.service.delete_object(delete).await.unwrap().output;
    assert_eq!(output.delete_marker, Some(true));
    assert!(output.version_id.is_some());
    let gone = get_error(&members, 1, "k/one").await;
    assert_eq!(*gone.code(), s3s::S3ErrorCode::NoSuchKey);

    let entries = ["a/near", "k/two", "q/far", "k/pinned"].map(|key| BulkDeleteEntry {
        key: key.to_string(),
        version_id: (key == "k/pinned").then(ulid::Ulid::nil),
    });
    let info = node.bucket().await;
    let target = (BUCKET, Some(&info));
    let (local, deleted, errors) = (node.service)
        .forward_deletes(
            &Extensions::new(),
            target,
            &members.user_on(0),
            entries.into(),
        )
        .await;
    // A key this node coordinates, and any exact version, stays for the local bulk delete.
    let local: Vec<&str> = local.iter().map(|entry| entry.key.as_str()).collect();
    assert_eq!(local, ["a/near", "k/pinned"]);
    let deleted: Vec<String> = deleted.into_iter().filter_map(|entry| entry.key).collect();
    assert_eq!(deleted, ["k/two", "q/far"]);
    assert!(errors.is_empty());
    for (on, key) in [(1, "k/two"), (2, "q/far")] {
        let gone = get_error(&members, on, key).await;
        assert_eq!(*gone.code(), s3s::S3ErrorCode::NoSuchKey, "{key}");
    }
}

// An unreachable coordinator fails the write at once with the degraded-range hint, and a
// bulk delete reports the key it could not forward instead of dropping it.
#[tokio::test]
async fn unreachable_coordinator_fails() {
    let mut members = members(3, &["h", "p"], 0).await;
    members.nodes[1].stop().await;
    let node = &members.nodes[0];
    let input = PutObjectInput {
        bucket: BUCKET.to_string(),
        key: "k/lost".to_string(),
        content_length: Some(4),
        body: Some(StreamingBlob::from(bytes::Bytes::from_static(b"lost"))),
        ..Default::default()
    };
    let put = request(input, Method::PUT, members.user_on(0), node.bucket().await);
    let error = node.service.put_object(put).await.unwrap_err();
    let code = s3s::S3ErrorCode::Custom("CoordinatorUnreachable".into());
    assert_eq!(*error.code(), code);
    assert_eq!(
        error.status_code(),
        Some(http::StatusCode::SERVICE_UNAVAILABLE)
    );
    let coordinator = members.nodes[1].node_id().to_string();
    assert!(
        error
            .message()
            .is_some_and(|text| text.contains(&coordinator))
    );

    let entries = vec![BulkDeleteEntry {
        key: "k/lost".to_string(),
        version_id: None,
    }];
    let info = node.bucket().await;
    let target = (BUCKET, Some(&info));
    let (_, deleted, errors) = (node.service)
        .forward_deletes(&Extensions::new(), target, &members.user_on(0), entries)
        .await;
    assert!(deleted.is_empty());
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].key.as_deref(), Some("k/lost"));
    assert_eq!(errors[0].code.as_deref(), Some("CoordinatorUnreachable"));
}

// Every member checks the caller on its own node path: a grant for this node only is
// refused by the coordinator, and the answer says that node globs fix it.
#[tokio::test]
async fn node_grant_refused() {
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_core::structs::storage::blob::bucket_permission_path;
    let members = members(3, &["h", "p"], 0).await;
    let node = &members.nodes[0];
    let mut user = members.user_on(0);
    let root = bucket_permission_path(members.realm_id, members.group_id, node.node_id(), BUCKET);
    user.path_restrictions = Some(vec![PathRestriction {
        pattern: format!("{root}/**"),
        permission: Permission::WRITE,
    }]);
    let input = PutObjectInput {
        bucket: BUCKET.to_string(),
        key: "k/denied".to_string(),
        content_length: Some(1),
        body: Some(StreamingBlob::from(bytes::Bytes::from_static(b"x"))),
        ..Default::default()
    };
    let put = request(input, Method::PUT, user, node.bucket().await);
    let error = node.service.put_object(put).await.unwrap_err();
    assert_eq!(error.status_code(), Some(http::StatusCode::FORBIDDEN));
    assert!(
        error
            .message()
            .is_some_and(|text| text.contains("node glob"))
    );
}

/// A copy of `source` to `dest` sent to node `on`, with the copy source header a client sets.
async fn copy(
    members: &Members,
    on: usize,
    (source, dest): (&str, &str),
    metadata: Option<(&str, &str)>,
) -> s3s::S3Result<s3s::dto::CopyObjectOutput> {
    use s3s::dto::{CopyObjectInput, CopySource, MetadataDirective};
    let node = &members.nodes[on];
    let mut input = CopyObjectInput::builder()
        .bucket(BUCKET.to_string())
        .key(dest.to_string())
        .copy_source(CopySource::Bucket {
            bucket: BUCKET.into(),
            key: source.into(),
            version_id: None,
        })
        .build()
        .unwrap();
    if let Some((name, value)) = metadata {
        input.metadata_directive = Some(MetadataDirective::from_static(MetadataDirective::REPLACE));
        input.metadata = Some(std::collections::HashMap::from([(
            name.to_string(),
            value.to_string(),
        )]));
    }
    let mut copy = request(input, Method::PUT, members.user_on(on), node.bucket().await);
    let header = format!("{BUCKET}/{source}");
    copy.headers
        .insert("x-amz-copy-source", header.parse().unwrap());
    node.service
        .copy_object(copy)
        .await
        .map(|response| response.output)
}

/// The body and metadata `key` holds on node `on`.
async fn read(members: &Members, on: usize, key: &str) -> (Vec<u8>, s3s::dto::GetObjectOutput) {
    let node = &members.nodes[on];
    let get = request(
        get_input(key),
        Method::GET,
        members.user_on(on),
        node.bucket().await,
    );
    let mut output = node.service.get_object(get).await.unwrap().output;
    let body = body_of(output.body.take()).await;
    (body, output)
}

/// The origin the version of `key` on node `on` records.
async fn origin_of(
    members: &Members,
    on: usize,
    key: &str,
) -> Option<aruna_core::structs::storage::origin::VersionOrigin> {
    let node = &members.nodes[on];
    let local = (members.realm_id, node.node_id());
    let view =
        aruna_operations::s3::object::origin::read_origin(&node.context, local, BUCKET, key, None)
            .await
            .unwrap();
    view.origin
}

fn global_copy(
    members: &Members,
    version: &str,
) -> Option<aruna_core::structs::storage::origin::VersionOrigin> {
    use aruna_core::structs::storage::global_id::GlobalId;
    use aruna_core::structs::storage::origin::{OriginVersion, VersionOrigin};
    Some(VersionOrigin::Copy {
        from: OriginVersion::Global(GlobalId::Version {
            realm_id: members.realm_id,
            version_id: version.parse().unwrap(),
        }),
    })
}

// A member keeping the source streams it to the destination's coordinator, which records
// the copy's origin and the replaced metadata as a local copy would.
#[tokio::test]
async fn copy_streams_source() {
    let members = members(3, &["h", "p"], 0).await;
    let source = put(&members, 0, "a/src", b"streamed").await;
    let output = copy(&members, 0, ("a/src", "k/dst"), Some(("lab", "spire")))
        .await
        .unwrap();
    assert_eq!(
        output.copy_source_version_id.as_deref(),
        Some(source.as_str())
    );
    let (body, read) = read(&members, 1, "k/dst").await;
    assert_eq!(body, b"streamed");
    assert_eq!(read.version_id, output.version_id);
    let metadata = read.metadata.unwrap_or_default();
    assert_eq!(metadata.get("lab").map(String::as_str), Some("spire"));
    assert_eq!(
        origin_of(&members, 1, "k/dst").await,
        global_copy(&members, &source)
    );
}

// When only the destination's coordinator keeps the source, it copies from its own records.
#[tokio::test]
async fn copy_delegated_coordinator() {
    let members = members(3, &["h", "p"], 0).await;
    let source = put(&members, 0, "k/src", b"delegated").await;
    let output = copy(&members, 0, ("k/src", "k/dst"), None).await.unwrap();
    assert_eq!(
        output.copy_source_version_id.as_deref(),
        Some(source.as_str())
    );
    assert!(
        output
            .copy_object_result
            .and_then(|result| result.e_tag)
            .is_some()
    );
    let (body, read) = read(&members, 1, "k/dst").await;
    assert_eq!(body, b"delegated");
    assert_eq!(read.version_id, output.version_id);
}

// A source neither this member nor the coordinator keeps is read through its holder and
// relayed, still recorded as a copy of that exact version.
#[tokio::test]
async fn copy_relays_holder() {
    let members = members(3, &["h", "p"], 0).await;
    let source = put(&members, 0, "q/src", b"relayed").await;
    let output = copy(&members, 0, ("q/src", "k/dst"), None).await.unwrap();
    assert_eq!(
        output.copy_source_version_id.as_deref(),
        Some(source.as_str())
    );
    let (body, _) = read(&members, 1, "k/dst").await;
    assert_eq!(body, b"relayed");
    assert_eq!(
        origin_of(&members, 1, "k/dst").await,
        global_copy(&members, &source)
    );
}

// A copy this member writes itself needs the source here; the refusal names who keeps it.
#[tokio::test]
async fn copy_source_elsewhere() {
    let members = members(3, &["h", "p"], 0).await;
    put(&members, 0, "k/src", b"far").await;
    let error = copy(&members, 0, ("k/src", "a/dst"), None)
        .await
        .unwrap_err();
    assert_eq!(
        *error.code(),
        s3s::S3ErrorCode::Custom("CopySourceNotHeld".into())
    );
    assert_eq!(
        error.status_code(),
        Some(http::StatusCode::SERVICE_UNAVAILABLE)
    );
    let holder = members.nodes[1].node_id().to_string();
    assert!(error.message().is_some_and(|text| text.contains(&holder)));
}
