//! Tests upload attempts against a local server: the pinned version is sent as the body, a POST
//! goes once and its answer decides a rerun, a PUT goes again, and a marked source never leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use aruna_core::structs::execution::job::UploadForm;
use aruna_core::structs::execution::job::{JobErrorKind, PluginWorkAction};
use axum::http::{HeaderMap, StatusCode};
use serde_json::json;

use super::*;
use crate::driver::DriverContext;
use crate::jobs::plugin_work::run_plugin_work;
use crate::jobs::plugin_work::tests::job_context;
use crate::plugins::install::tests::Fixture;
use crate::plugins::work::tests::edit_work;
use crate::plugins::work::{
    Identity, RequestContext, SendBoundary, WorkKind, read_work, reserve, update_work,
};
use crate::s3::object::copy::test::{put_config, seed_bucket, with_blob};
use crate::s3::object::put::PutObjectOperation;

const CONTENT: &[u8] = b"ACGTACGT";

/// What the server got: the request count, and the last body and content type.
#[derive(Default)]
struct Seen {
    hits: AtomicUsize,
    body: Mutex<Vec<u8>>,
    kind: Mutex<String>,
}

/// A server that answers `status` with a small JSON reply and keeps what it got.
async fn server(status: u16) -> (String, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let kept = seen.clone();
    let router =
        axum::Router::new().fallback(move |headers: HeaderMap, body: axum::body::Bytes| {
            let kept = kept.clone();
            async move {
                kept.hits.fetch_add(1, Ordering::SeqCst);
                *kept.body.lock().unwrap() = body.to_vec();
                let kind = headers
                    .get("content-type")
                    .and_then(|value| value.to_str().ok());
                *kept.kind.lock().unwrap() = kind.unwrap_or_default().to_string();
                let reply = r#"{"checksum":"md5:1"}"#;
                (
                    StatusCode::from_u16(status).unwrap(),
                    [("content-type", "application/json")],
                    reply,
                )
            }
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}/deposit/"), seen)
}

/// An install of a version that requests `transfers.upload` through a service that takes PUT
/// and POST, granted the group's data.
async fn uploader(fixture: &Fixture) -> PluginInstall {
    let upload = json!({
        "permissions": { "required": ["transfers.upload"] },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "methods": ["PUT", "POST"] } }
    });
    let digest = fixture.publish("0.2.0", "cd", upload).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    request.grants = std::collections::BTreeMap::from([("read".into(), vec![data])]);
    fixture.install(&request).await.unwrap()
}

/// A blob context with the object `in/a.txt` of the install's group.
async fn ready(fixture: &Fixture, install: &PluginInstall) -> (tempfile::TempDir, DriverContext) {
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    seed_bucket(&blob, "in", install.group_id, install.account, Vec::new()).await;
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let put = put_config(realm, install.group_id, node, "in", "a.txt", CONTENT);
    drive(PutObjectOperation::new(put), &blob).await.unwrap();
    (root, blob)
}

/// A recorded upload of `in/a.txt` with `method` to `base`, and the spec of its job.
async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    (base, method): (&str, &str),
) -> (WorkRow, PluginWorkSpec) {
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, install.group_id);
    let source = format!("{data}/{}/in/a.txt", fixture.actor.node_id);
    let mut row = WorkRow::reserved(install, WorkKind::Upload, Ulid::generate(), 1);
    row.source = Some(source.clone());
    row.context = Some(RequestContext {
        connection: "catalog".into(),
        base: base.into(),
        method: method.into(),
        path: "a.txt".into(),
        query: Vec::new(),
        headers: Vec::new(),
    });
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let (row, _) = drive(
        reserve((install, &identity), row, None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let spec = PluginWorkSpec {
        realm_id: fixture.actor.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action: PluginWorkAction::Upload {
            source,
            version: None,
            form: None,
        },
    };
    (row, spec)
}

fn name(outcome: &JobRunOutcome) -> (&'static str, Option<JobErrorKind>) {
    match outcome {
        JobRunOutcome::Succeeded(_) => ("succeeded", None),
        JobRunOutcome::Failed(error) => ("failed", Some(error.kind)),
        JobRunOutcome::Undecided(_) => ("undecided", None),
        JobRunOutcome::Deferred(_) => ("deferred", None),
        _ => ("other", None),
    }
}

async fn boundary(fixture: &Fixture, (install_id, work_id): (Ulid, Ulid)) -> Option<SendBoundary> {
    let found = drive(read_work(install_id, work_id), &fixture.ctx).await;
    found.unwrap().unwrap().boundary
}

#[tokio::test]
async fn post_sent_once() {
    // A POST sends the object once; its answer is kept, and a rerun decides by it without a
    // second request.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (row, spec) = recorded(&fixture, &install, (&base, "POST")).await;
    let ctx = job_context(&blob, &fixture);
    for _ in 0..2 {
        let outcome = run_plugin_work(&ctx, &spec).await;
        let JobRunOutcome::Succeeded(JobResultPayload::PluginWork { bytes, .. }) = outcome else {
            panic!("the upload did not succeed: {:?}", name(&outcome));
        };
        assert_eq!(bytes, CONTENT.len() as u64);
    }
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
    assert_eq!(*seen.body.lock().unwrap(), CONTENT);
    let Some(SendBoundary::Sent { status, reply, .. }) =
        boundary(&fixture, (install.install_id, row.work_id)).await
    else {
        panic!("the answer is kept");
    };
    assert_eq!(status, 201);
    assert!(reply.unwrap().contains(r#""status":201"#));
}

#[tokio::test]
async fn open_post_undecided() {
    // An attempt that finds a POST begun but unanswered never sends again; its outcome stays
    // unknown.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (row, spec) = recorded(&fixture, &install, (&base, "POST")).await;
    let begun = |row: &mut WorkRow| row.boundary = Some(SendBoundary::Sending { attempt: 1 });
    edit_work(&fixture.ctx, (install.install_id, row.work_id), begun)
        .await
        .unwrap();
    let ctx = job_context(&blob, &fixture);
    assert_eq!(name(&run_plugin_work(&ctx, &spec).await).0, "undecided");
    assert_eq!(seen.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn put_sent_again() {
    // A PUT answered with a server error is tried again and sent again; a POST so answered fails
    // and is not sent again.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let ctx = job_context(&blob, &fixture);
    let (base, seen) = server(503).await;
    let (_, put) = recorded(&fixture, &install, (&base, "PUT")).await;
    for _ in 0..2 {
        let outcome = run_plugin_work(&ctx, &put).await;
        assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Retryable)));
    }
    assert_eq!(seen.hits.load(Ordering::SeqCst), 2);
    let (base, seen) = server(503).await;
    let (_, post) = recorded(&fixture, &install, (&base, "POST")).await;
    for _ in 0..2 {
        let outcome = run_plugin_work(&ctx, &post).await;
        assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Permanent)));
    }
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn marked_source_kept() {
    // A version a job with a secret wrote never leaves the node.
    use crate::plugins::marks::{Mark, mark_row};
    use crate::plugins::rows::write_rows;
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (_, spec) = recorded(&fixture, &install, (&base, "PUT")).await;
    let version = crate::plugins::broker::unmarked(&blob, ("in", "a.txt"), None).await;
    let version = version.unwrap().unwrap();
    let mark = Mark {
        install_id: Ulid::from_parts(8, 8),
        work_id: Ulid::from_parts(8, 9),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let marked = mark_row(("in", "a.txt", version), &mark).unwrap();
    drive(write_rows(vec![marked]), &blob).await.unwrap();
    let ctx = job_context(&blob, &fixture);
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Permanent)));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn refused_connect_reopens() {
    // Nothing reached a service that refused the connection, so a POST may be sent later.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    let base = format!("http://{address}/deposit/");
    let (row, spec) = recorded(&fixture, &install, (&base, "POST")).await;
    let ctx = job_context(&blob, &fixture);
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Retryable)));
    assert_eq!(
        boundary(&fixture, (install.install_id, row.work_id)).await,
        None
    );
}

#[tokio::test]
async fn multipart_form() {
    // A form sends the object as its named file part beside the text fields.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (_, mut spec) = recorded(&fixture, &install, (&base, "POST")).await;
    let form = UploadForm {
        file_field: "file".into(),
        file_name: "reads.fq".into(),
        fields: vec![("title".into(), "Run 7".into())],
    };
    if let PluginWorkAction::Upload { form: sent, .. } = &mut spec.action {
        *sent = Some(form);
    }
    let ctx = job_context(&blob, &fixture);
    assert_eq!(name(&run_plugin_work(&ctx, &spec).await).0, "succeeded");
    assert!(
        seen.kind
            .lock()
            .unwrap()
            .starts_with("multipart/form-data; boundary=")
    );
    let body = String::from_utf8(seen.body.lock().unwrap().clone()).unwrap();
    assert!(
        body.contains(r#"name="title""#) && body.contains("Run 7"),
        "{body}"
    );
    let file = r#"name="file"; filename="reads.fq""#;
    assert!(body.contains(file) && body.contains("ACGTACGT"), "{body}");
}

#[tokio::test]
async fn denied_connection() {
    // A policy that denies the connection after submission stops the upload before any request.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (_, spec) = recorded(&fixture, &install, (&base, "PUT")).await;
    fixture
        .deny(&["operation == 'plugin.connections.send'"])
        .await;
    let ctx = job_context(&blob, &fixture);
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Permanent)));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 0);
}

/// A server that takes each request and closes the connection without an answer.
async fn dropping() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buffer = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buffer).await;
        }
    });
    format!("http://{address}/deposit/")
}

#[tokio::test]
async fn dropped_answer() {
    // A PUT whose answer never came is tried again; a POST so cut off stays undecided.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let base = dropping().await;
    let ctx = job_context(&blob, &fixture);
    let (_, put) = recorded(&fixture, &install, (&base, "PUT")).await;
    let outcome = run_plugin_work(&ctx, &put).await;
    assert_eq!(name(&outcome), ("failed", Some(JobErrorKind::Retryable)));
    let (_, post) = recorded(&fixture, &install, (&base, "POST")).await;
    assert_eq!(name(&run_plugin_work(&ctx, &post).await).0, "undecided");
}

#[tokio::test]
async fn put_answer_reused() {
    // A PUT whose answer was kept decides a rerun by it, without a second request.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (_, spec) = recorded(&fixture, &install, (&base, "PUT")).await;
    let ctx = job_context(&blob, &fixture);
    for _ in 0..2 {
        assert_eq!(name(&run_plugin_work(&ctx, &spec).await).0, "succeeded");
    }
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovers_without_source() {
    // A POST begun or answered decides a rerun by its boundary, even with the source gone; an
    // answered one keeps the bytes and version it sent.
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (row, mut spec) = recorded(&fixture, &install, (&base, "POST")).await;
    if let PluginWorkAction::Upload { version, .. } = &mut spec.action {
        *version = Some(Ulid::from_parts(5, 1));
    }
    let ids = (install.install_id, row.work_id);
    let begun = |row: &mut WorkRow| row.boundary = Some(SendBoundary::Sending { attempt: 1 });
    edit_work(&fixture.ctx, ids, begun).await.unwrap();
    let ctx = job_context(&blob, &fixture);
    assert_eq!(name(&run_plugin_work(&ctx, &spec).await).0, "undecided");
    let answered = |row: &mut WorkRow| {
        row.boundary = Some(SendBoundary::Sent {
            status: 201,
            reply: None,
            bytes: 8,
            version: Ulid::from_parts(5, 2),
        })
    };
    edit_work(&fixture.ctx, ids, answered).await.unwrap();
    let outcome = run_plugin_work(&ctx, &spec).await;
    let JobRunOutcome::Succeeded(JobResultPayload::PluginWork { bytes, version_id }) = outcome
    else {
        panic!("the answered upload did not succeed: {:?}", name(&outcome));
    };
    assert_eq!((bytes, version_id), (8, Some(Ulid::from_parts(5, 2))));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 0);
}
