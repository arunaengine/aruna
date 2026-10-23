//! Create-only writes to tenant paths on a storage that, like Ceph RGW, ignores the conditions
//! of copies and multipart completions but honors them on a single put.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::backend_import::PlainPath;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::Response;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// One object of the fake storage.
#[derive(Clone)]
struct Stored {
    body: Bytes,
    etag: String,
    metadata: Vec<(String, String)>,
}

#[derive(Default)]
struct Fake {
    objects: BTreeMap<String, Stored>,
    uploads: BTreeMap<String, BTreeMap<u16, Bytes>>,
    next: u64,
    /// A foreign write that lands at its key just before the next delete of that key.
    before_delete: Option<(String, Stored)>,
    /// A foreign write that lands at its key right after a reservation of that key.
    after_reserve: Option<(String, Stored)>,
    /// A foreign write that lands at its key right after the next look at that key.
    after_head: Option<(String, Stored)>,
}

type Shared = Arc<Mutex<Fake>>;

fn md5_hex(bytes: &[u8]) -> String {
    hex::encode(md5::compute(bytes).0)
}

fn stored(body: &[u8], metadata: &[(&str, &str)]) -> Stored {
    let metadata = metadata
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()));
    Stored {
        body: Bytes::copy_from_slice(body),
        etag: md5_hex(body),
        metadata: metadata.collect(),
    }
}

fn answer(status: StatusCode, headers: &[(String, String)], body: impl Into<Body>) -> Response {
    let mut response = Response::builder().status(status);
    for (name, value) in headers {
        response = response.header(name, value);
    }
    response.body(body.into()).unwrap()
}

fn refuse(status: StatusCode, code: &str) -> Response {
    let body = format!("<Error><Code>{code}</Code><Message>{code}</Message></Error>");
    let xml = [("content-type".to_string(), "application/xml".to_string())];
    answer(status, &xml, body)
}

fn xml(body: String) -> Response {
    let xml = [("content-type".to_string(), "application/xml".to_string())];
    answer(StatusCode::OK, &xml, body)
}

fn decoded(path: &str) -> String {
    let path = path.split('?').next().unwrap_or_default();
    let path = percent_encoding::percent_decode_str(path).decode_utf8_lossy();
    path.trim_start_matches('/').to_string()
}

/// The inclusive byte range a header like `bytes=2-5` names within `size` bytes.
fn range(header: Option<&str>, size: usize) -> Option<(usize, usize)> {
    let (first, last) = header?.strip_prefix("bytes=")?.split_once('-')?;
    let first = first.parse().ok()?;
    let end = size.saturating_sub(1);
    let last = last.parse().map_or(end, |last: usize| last.min(end));
    Some((first, last))
}

/// Answers the S3 calls the blob adapter makes, keeping objects in memory.
async fn serve(
    State(fake): State<Shared>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key = decoded(uri.path());
    let query = uri.query().unwrap_or_default().to_string();
    let param = |name: &str| {
        let prefix = format!("{name}=");
        let found = query.split('&').find_map(|pair| pair.strip_prefix(&prefix));
        found.map(decoded)
    };
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    let unquoted = |etag: &str| etag.trim_matches('"').to_string();
    let mut fake = fake.lock().unwrap();
    let source = header("x-amz-copy-source").map(decoded);
    let copied = source
        .as_ref()
        .map(|source| fake.objects.get(source).cloned());
    // Copies honor only the source condition, never one about the target.
    if let (Some(Some(found)), Some(etag)) = (&copied, header("x-amz-copy-source-if-match"))
        && unquoted(etag) != found.etag
    {
        return refuse(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
    }
    if matches!(copied, Some(None)) {
        return refuse(StatusCode::NOT_FOUND, "NoSuchKey");
    }
    let copied = copied.flatten();
    let head = method == Method::HEAD;
    match (method, param("uploadId")) {
        (Method::GET, _) if param("list-type").is_some() => listed(&fake, &key, param("prefix")),
        (Method::HEAD | Method::GET, _) => {
            let Some(found) = fake.objects.get(&key) else {
                return refuse(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            let size = found.body.len();
            let mut listed = vec![
                ("etag".to_string(), format!("\"{}\"", found.etag)),
                (
                    "last-modified".to_string(),
                    "Fri, 23 Oct 2026 00:00:00 GMT".to_string(),
                ),
            ];
            for (name, value) in &found.metadata {
                listed.push((format!("x-amz-meta-{name}"), value.clone()));
            }
            let (status, body) = match range(header("range"), size).filter(|_| size > 0) {
                Some((first, last)) => {
                    let total = format!("bytes {first}-{last}/{size}");
                    listed.push(("content-range".to_string(), total));
                    (StatusCode::PARTIAL_CONTENT, found.body.slice(first..=last))
                }
                None => (StatusCode::OK, found.body.clone()),
            };
            if head && let Some((_, foreign)) = fake.after_head.take_if(|(at, _)| *at == key) {
                fake.objects.insert(key.clone(), foreign);
            }
            listed.push(("content-length".to_string(), body.len().to_string()));
            match head {
                true => answer(status, &listed, Body::empty()),
                false => answer(status, &listed, body),
            }
        }
        (Method::PUT, Some(upload)) => {
            let Some(number) = param("partNumber").and_then(|number| number.parse().ok()) else {
                return refuse(StatusCode::BAD_REQUEST, "InvalidArgument");
            };
            let part = match &copied {
                Some(found) => {
                    let size = found.body.len();
                    match range(header("x-amz-copy-source-range"), size) {
                        Some((first, last)) => {
                            found.body.slice(first.min(size)..(last + 1).min(size))
                        }
                        None => found.body.clone(),
                    }
                }
                None => body,
            };
            let etag = md5_hex(&part);
            let Some(parts) = fake.uploads.get_mut(&upload) else {
                return refuse(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            parts.insert(number, part);
            match copied {
                Some(_) => xml(format!(
                    "<CopyPartResult><ETag>\"{etag}\"</ETag></CopyPartResult>"
                )),
                None => answer(
                    StatusCode::OK,
                    &[("etag".into(), format!("\"{etag}\""))],
                    "",
                ),
            }
        }
        (Method::PUT, None) => {
            let written = match copied {
                // A copy keeps the source's ETag, as Ceph RGW does.
                Some(found) => found,
                None => {
                    if header("if-none-match") == Some("*") && fake.objects.contains_key(&key) {
                        return refuse(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                    }
                    let metadata = headers.iter().filter_map(|(name, value)| {
                        let name = name.as_str().strip_prefix("x-amz-meta-")?;
                        Some((name.to_string(), value.to_str().ok()?.to_string()))
                    });
                    Stored {
                        etag: md5_hex(&body),
                        body,
                        metadata: metadata.collect(),
                    }
                }
            };
            let etag = format!("\"{}\"", written.etag);
            let copy = source.is_some();
            fake.objects.insert(key.clone(), written);
            if let Some((_, foreign)) = fake.after_reserve.take_if(|(at, _)| *at == key) {
                fake.objects.insert(key, foreign);
            }
            match copy {
                true => xml(format!(
                    "<CopyObjectResult><ETag>{etag}</ETag></CopyObjectResult>"
                )),
                false => answer(StatusCode::OK, &[("etag".into(), etag)], ""),
            }
        }
        (Method::POST, None) => {
            fake.next += 1;
            let upload = format!("upload-{}", fake.next);
            fake.uploads.insert(upload.clone(), BTreeMap::new());
            let result = format!("<Bucket>b</Bucket><Key>{key}</Key><UploadId>{upload}</UploadId>");
            xml(format!(
                "<InitiateMultipartUploadResult>{result}</InitiateMultipartUploadResult>"
            ))
        }
        // A completion ignores any condition on the target.
        (Method::POST, Some(upload)) => {
            let Some(parts) = fake.uploads.remove(&upload) else {
                return refuse(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            let body = String::from_utf8_lossy(&body).to_string();
            let numbers = body.split("<PartNumber>").skip(1);
            let numbers = numbers.filter_map(|rest| rest.split('<').next()?.parse::<u16>().ok());
            let chosen: Vec<Bytes> = numbers
                .filter_map(|number| parts.get(&number).cloned())
                .collect();
            let digests: Vec<u8> = chosen
                .iter()
                .flat_map(|part| md5::compute(part).0)
                .collect();
            let etag = format!("{}-{}", md5_hex(&digests), chosen.len());
            let written = Stored {
                body: chosen.concat().into(),
                etag: etag.clone(),
                metadata: Vec::new(),
            };
            fake.objects.insert(key.clone(), written);
            let result = format!("<Bucket>b</Bucket><Key>{key}</Key><ETag>\"{etag}\"</ETag>");
            xml(format!(
                "<CompleteMultipartUploadResult>{result}</CompleteMultipartUploadResult>"
            ))
        }
        (Method::DELETE, Some(upload)) => {
            fake.uploads.remove(&upload);
            answer(StatusCode::NO_CONTENT, &[], "")
        }
        (Method::DELETE, None) => {
            if let Some((_, foreign)) = fake.before_delete.take_if(|(at, _)| *at == key) {
                fake.objects.insert(key.clone(), foreign);
            }
            let found = fake.objects.get(&key).map(|found| found.etag.clone());
            if let (Some(etag), Some(wanted)) = (found, header("if-match"))
                && unquoted(wanted) != etag
            {
                return refuse(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
            }
            fake.objects.remove(&key);
            answer(StatusCode::NO_CONTENT, &[], "")
        }
        _ => refuse(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    }
}

/// One page with every object of `bucket` under `prefix`, as ListObjectsV2 answers.
fn listed(fake: &Fake, bucket: &str, prefix: Option<String>) -> Response {
    let scope = format!("{}/", bucket.trim_end_matches('/'));
    let prefix = prefix.unwrap_or_default();
    let mut contents = String::new();
    for (key, found) in &fake.objects {
        let Some(key) = key
            .strip_prefix(&scope)
            .filter(|key| key.starts_with(&prefix))
        else {
            continue;
        };
        let size = found.body.len();
        let modified = "2026-10-23T00:00:00.000Z";
        contents.push_str(&format!(
            "<Contents><Key>{key}</Key><Size>{size}</Size><ETag>\"{}\"</ETag>\
             <LastModified>{modified}</LastModified></Contents>",
            found.etag
        ));
    }
    let truncated = "<IsTruncated>false</IsTruncated>";
    xml(format!(
        "<ListBucketResult>{truncated}{contents}</ListBucketResult>"
    ))
}

/// Serves the fake storage on an ephemeral port.
async fn fake_s3() -> (String, Shared) {
    let fake = Shared::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().fallback(serve).with_state(fake.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (endpoint, fake)
}

/// A handler with one group S3 storage, bucket `tenant`, on the fake storage.
async fn tenant() -> (TestContext, BlobHandler, ResolvedBackend, Shared) {
    let (endpoint, fake) = fake_s3().await;
    let context = setup_context(TestContextSetup::TwoFilesystem).await;
    let config = HashMap::from([
        ("endpoint".to_string(), endpoint),
        ("region".to_string(), "us-east-1".to_string()),
        ("access_key_id".to_string(), "key".to_string()),
        ("secret_access_key".to_string(), "secret".to_string()),
        ("force_path_style".to_string(), "true".to_string()),
        ("bucket".to_string(), "tenant".to_string()),
    ]);
    let backend_id = Ulid::generate();
    let entry = NodeBackend::new(
        BackendConfig {
            backend_type: Backend::Group(GroupBackendKind::S3),
            root: String::new(),
            service_config: config,
            bucket_prefix: None,
            max_bucket_size: None,
            multipart_bucket: Some("tenant".to_string()),
            timeouts: Default::default(),
        },
        None,
    );
    let groups = HashMap::from([(backend_id, Arc::new(entry))]);
    let handler = BlobHandler {
        registry: context.blob_handle.handler.registry.with_groups(groups),
        ..context.blob_handle.handler.clone()
    };
    let resolved = ResolvedBackend::new(BackendRef::Group(backend_id), None);
    (context, handler, resolved, fake)
}

/// A create-only write of `version_id` to exactly `path`, as a rename makes it.
fn exact(path: &str, version_id: Ulid) -> PlainPath {
    PlainPath {
        path: path.to_string(),
        ulid: Ulid::generate(),
        version_id,
        if_not_exists: true,
        exact: true,
    }
}

/// Writes `bytes` to `path` as the source of a rename.
async fn source(
    handler: &BlobHandler,
    resolved: &ResolvedBackend,
    bytes: &[u8],
) -> BackendLocation {
    let plain = PlainPath {
        if_not_exists: false,
        ..exact("raw/7_a.bin", Ulid::generate())
    };
    let written = handler.write_plain(
        resolved.clone(),
        plain,
        test_user_id(),
        stream_from_bytes(bytes),
    );
    let BlobEvent::PlainWritten { location, .. } = written.await else {
        panic!("source write failed")
    };
    location
}

fn body(fake: &Shared, key: &str) -> Option<Bytes> {
    let fake = fake.lock().unwrap();
    fake.objects.get(key).map(|found| found.body.clone())
}

fn place(fake: &Shared, key: &str, object: Stored) {
    fake.lock().unwrap().objects.insert(key.to_string(), object);
}

#[tokio::test]
async fn rename_keeps_taken() {
    // A taken new name is refused, though the storage ignores the copy's own conditions.
    let (_context, handler, resolved, fake) = tenant().await;
    let old = source(&handler, &resolved, b"renamed bytes").await;
    place(&fake, "tenant/raw/a.bin", stored(b"foreign", &[]));
    let copied = handler.copy_plain(
        resolved.clone(),
        exact("raw/a.bin", Ulid::generate()),
        old,
        None,
    );
    assert!(matches!(
        copied.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert_eq!(body(&fake, "tenant/raw/a.bin").unwrap(), &b"foreign"[..]);
    // Another write's reservation stays where it is too.
    let other = Ulid::generate().to_string();
    let reserved = stored(b"", &[("aruna-reservation", other.as_str())]);
    place(&fake, "tenant/raw/b.bin", reserved);
    let old = source(&handler, &resolved, b"renamed bytes").await;
    let copied = handler.copy_plain(resolved, exact("raw/b.bin", Ulid::generate()), old, None);
    assert!(matches!(
        copied.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert!(body(&fake, "tenant/raw/b.bin").unwrap().is_empty());
}

#[tokio::test]
async fn rename_reuses_reservation() {
    // The reservation an interrupted try of this version left is its own, and the copy lands.
    let (_context, handler, resolved, fake) = tenant().await;
    let old = source(&handler, &resolved, b"renamed bytes").await;
    let version = Ulid::generate();
    let reserved = stored(b"", &[("aruna-reservation", version.to_string().as_str())]);
    place(&fake, "tenant/raw/a.bin", reserved);
    let copied = handler.copy_plain(resolved.clone(), exact("raw/a.bin", version), old, None);
    let BlobEvent::PlainWritten { location, .. } = copied.await else {
        panic!("the copy did not land")
    };
    assert_eq!(
        body(&fake, "tenant/raw/a.bin").unwrap(),
        &b"renamed bytes"[..]
    );
    assert_eq!(read_back(&handler, location).await, b"renamed bytes");
    // A non-empty file is never removed as a reservation.
    let backend = resolved.backend.clone();
    let dropped = handler.drop_reservation(backend, "raw/a.bin".to_string(), version);
    assert!(matches!(dropped.await, BlobEvent::DeleteFinished));
    assert!(body(&fake, "tenant/raw/a.bin").is_some());
}

#[tokio::test]
async fn failed_copy_keeps() {
    // Without versions a failed copy keeps its marked reservation, as no delete names only it.
    let (_context, handler, resolved, fake) = tenant().await;
    let old = source(&handler, &resolved, b"renamed bytes").await;
    let stale = Some("0123456789abcdef0123456789abcdef".to_string());
    let copied = handler.copy_plain(resolved, exact("raw/a.bin", Ulid::generate()), old, stale);
    assert!(matches!(copied.await, BlobEvent::Error(_)));
    assert!(body(&fake, "tenant/raw/a.bin").unwrap().is_empty());
}

#[tokio::test]
async fn unpinned_copy_streams() {
    // A source pinned by neither version nor ETag proves its copied part by its MD5 before the
    // copy lands; changed bytes leave no copy at the new name and fail their hash in the node.
    let (_context, handler, resolved, fake) = tenant().await;
    let old = source(&handler, &resolved, b"renamed bytes").await;
    let mut changed = stored(b"changed bytes", &[]);
    changed.etag = "0123456789abcdef0123456789abcdef-2".to_string();
    place(&fake, "tenant/raw/7_a.bin", changed);
    let plain = exact("raw/a.bin", Ulid::generate());
    let copied = handler.copy_plain(resolved, plain, old, None).await;
    assert!(matches!(copied, BlobEvent::Error(_)), "{copied:?}");
    assert!(body(&fake, "tenant/raw/a.bin").unwrap().is_empty());
}

#[tokio::test]
async fn unreserve_keeps_replacement() {
    // A foreign empty file that replaced the reservation after it was looked at has its ETag;
    // without versions nothing is deleted, so it survives.
    let (_context, handler, resolved, fake) = tenant().await;
    let old = source(&handler, &resolved, b"renamed bytes").await;
    let foreign = stored(b"", &[]);
    fake.lock().unwrap().before_delete = Some(("tenant/raw/a.bin".to_string(), foreign));
    let stale = Some("0123456789abcdef0123456789abcdef".to_string());
    let plain = exact("raw/a.bin", Ulid::generate());
    let copied = handler.copy_plain(resolved, plain, old, stale);
    assert!(matches!(copied.await, BlobEvent::Error(_)));
    assert!(body(&fake, "tenant/raw/a.bin").unwrap().is_empty());
}

#[tokio::test]
async fn late_write_survives() {
    // A file that replaces a write's reservation before the write completes is never replaced,
    // whether the write streams through the node or copies large files in parts.
    let (_context, handler, resolved, fake) = tenant().await;
    let late = |key: &str| {
        let foreign = stored(b"foreign", &[]);
        fake.lock().unwrap().after_reserve = Some((key.to_string(), foreign));
    };
    late("tenant/raw/a.bin");
    let plain = PlainPath {
        exact: false,
        ..exact("raw/a.bin", Ulid::generate())
    };
    let blob = stream_from_bytes(b"uploaded bytes");
    let written = handler.write_plain(resolved.clone(), plain, test_user_id(), blob);
    assert!(matches!(
        written.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert_eq!(body(&fake, "tenant/raw/a.bin").unwrap(), &b"foreign"[..]);
    let mut old = source(&handler, &resolved, b"renamed bytes").await;
    let etag = Some(md5_hex(b"renamed bytes"));
    old.blob_size = 6 << 30;
    late("tenant/raw/b.bin");
    let plain = exact("raw/b.bin", Ulid::generate());
    let copied = handler.copy_plain(resolved, plain, old, etag);
    assert!(matches!(
        copied.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert_eq!(body(&fake, "tenant/raw/b.bin").unwrap(), &b"foreign"[..]);
}

#[tokio::test]
async fn hides_marked_reservations() {
    // A write's empty reservation is no file to a listing or a stat, whatever intents say; a
    // genuine empty file still is one.
    let (_context, handler, resolved, fake) = tenant().await;
    let marked = stored(b"", &[("aruna-reservation", "01K8")]);
    place(&fake, "tenant/raw/a.bin", stored(b"bytes", &[]));
    place(&fake, "tenant/raw/b.bin", marked);
    place(&fake, "tenant/raw/c.bin", stored(b"", &[]));
    let backend = resolved.backend.clone();
    let listed = handler.list_external(&backend, "raw/", None, 10).await;
    let BlobEvent::ExternalListed { objects, .. } = listed else {
        panic!("the listing failed: {listed:?}")
    };
    let keys: Vec<&str> = objects.iter().map(|object| object.key.as_str()).collect();
    assert_eq!(keys, ["raw/a.bin", "raw/c.bin"]);
    for (path, file) in [("raw/b.bin", false), ("raw/c.bin", true)] {
        let found = handler.stat_external(resolved.clone(), path.to_string(), test_user_id());
        let BlobEvent::ExternalStat { location, .. } = found.await else {
            panic!("the stat failed")
        };
        assert_eq!(location.is_some(), file, "{path}");
    }
}

#[tokio::test]
async fn hash_skips_reservations() {
    // A reservation is no file to hash, whether the stat shows it or only the read finds it in
    // place of an empty file; a genuine empty file still hashes.
    let (_context, handler, resolved, fake) = tenant().await;
    let marked = stored(b"", &[("aruna-reservation", "01K8")]);
    place(&fake, "tenant/raw/a.bin", marked.clone());
    place(&fake, "tenant/raw/b.bin", stored(b"", &[]));
    place(&fake, "tenant/raw/c.bin", stored(b"", &[]));
    fake.lock().unwrap().after_head = Some(("tenant/raw/b.bin".to_string(), marked));
    for (path, file) in [
        ("raw/a.bin", false),
        ("raw/b.bin", false),
        ("raw/c.bin", true),
    ] {
        let hashed = handler.hash_external(resolved.clone(), path.to_string(), test_user_id());
        let answer = match hashed.await {
            BlobEvent::ExternalHashed { .. } => Some(true),
            BlobEvent::Error(BlobError::Unreadable(_)) => Some(false),
            _ => None,
        };
        assert_eq!(answer, Some(file), "{path}");
    }
}

/// Opens a provider upload at the tenant path `path` and writes one part.
async fn opened(
    handler: &BlobHandler,
    resolved: &ResolvedBackend,
    path: &str,
) -> (BackendUpload, MultipartPart) {
    let id = Ulid::generate();
    let plain = PlainPath {
        path: path.to_string(),
        ulid: id,
        version_id: id,
        if_not_exists: false,
        exact: false,
    };
    let opened = handler.open_upload(
        Ulid::generate(),
        "bucket",
        "a.bin",
        resolved.clone(),
        test_user_id(),
        Some(plain),
    );
    let BlobEvent::UploadOpened {
        backend_upload: Some(upload),
    } = opened.await
    else {
        panic!("the upload did not open")
    };
    let part = super::in_place_part(handler, &upload, 1, b"uploaded bytes").await;
    (upload, part)
}

#[tokio::test]
async fn upload_keeps_taken() {
    // A completion never replaces a file that took its free path meanwhile; its abort keeps it.
    let (_context, handler, resolved, fake) = tenant().await;
    let (upload, part) = opened(&handler, &resolved, "raw/a.bin").await;
    place(&fake, "tenant/raw/a.bin", stored(b"foreign", &[]));
    let completed =
        handler.complete_upload(upload.clone(), vec![part], &[], Compression::Off, true);
    assert!(matches!(
        completed.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert!(matches!(
        handler.abort_upload(upload).await,
        BlobEvent::UploadAborted
    ));
    assert_eq!(body(&fake, "tenant/raw/a.bin").unwrap(), &b"foreign"[..]);
}

#[tokio::test]
async fn upload_late_write() {
    // A file that replaces the reservation before the completion stays; a retried completion
    // that finds its own completed object finishes from it.
    let (_context, handler, resolved, fake) = tenant().await;
    let (upload, part) = opened(&handler, &resolved, "raw/a.bin").await;
    let foreign = stored(b"foreign", &[]);
    fake.lock().unwrap().after_reserve = Some(("tenant/raw/a.bin".to_string(), foreign));
    let parts = vec![part.clone()];
    let completed = handler.complete_upload(upload, parts, &[], Compression::Off, true);
    assert!(matches!(
        completed.await,
        BlobEvent::Error(BlobError::PathTaken)
    ));
    assert_eq!(body(&fake, "tenant/raw/a.bin").unwrap(), &b"foreign"[..]);
    let (upload, part) = opened(&handler, &resolved, "raw/b.bin").await;
    for _ in 0..2 {
        let parts = vec![part.clone()];
        let completed = handler.complete_upload(upload.clone(), parts, &[], Compression::Off, true);
        let BlobEvent::PlainWritten { location, .. } = completed.await else {
            panic!("the completion did not finish")
        };
        assert_eq!(
            location.hashes,
            crate::hash::Hasher::new_with_bytes(b"uploaded bytes").to_map()
        );
    }
}

#[tokio::test]
async fn upload_reuses_reservation() {
    // A retried completion reuses its own reservation; without versions an abort keeps every
    // reservation.
    let (_context, handler, resolved, fake) = tenant().await;
    let (upload, part) = opened(&handler, &resolved, "raw/a.bin").await;
    let version = upload.location.ulid.to_string();
    let reserved = stored(b"", &[("aruna-reservation", version.as_str())]);
    place(&fake, "tenant/raw/a.bin", reserved.clone());
    let completed = handler.complete_upload(upload, vec![part], &[], Compression::Off, true);
    assert!(matches!(completed.await, BlobEvent::PlainWritten { .. }));
    assert_eq!(
        body(&fake, "tenant/raw/a.bin").unwrap(),
        &b"uploaded bytes"[..]
    );

    let (upload, _) = opened(&handler, &resolved, "raw/b.bin").await;
    let version = upload.location.ulid.to_string();
    place(
        &fake,
        "tenant/raw/b.bin",
        stored(b"", &[("aruna-reservation", version.as_str())]),
    );
    place(&fake, "tenant/raw/c.bin", reserved);
    assert!(matches!(
        handler.abort_upload(upload).await,
        BlobEvent::UploadAborted
    ));
    assert!(body(&fake, "tenant/raw/b.bin").unwrap().is_empty());
    assert!(body(&fake, "tenant/raw/c.bin").is_some());
}
