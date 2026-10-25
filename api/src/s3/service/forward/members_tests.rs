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
