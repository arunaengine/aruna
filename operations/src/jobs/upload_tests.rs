//! Tests upload attempts against a local server: the pinned version is sent as the body, a POST
//! goes once and its answer decides a rerun, a PUT goes again, and a marked source never leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use aruna_core::structs::execution::job::{JobErrorKind, PluginWorkAction};
use axum::http::StatusCode;
use serde_json::json;

use super::*;
use crate::driver::DriverContext;
use crate::jobs::plugin_work::run_plugin_work;
use crate::jobs::plugin_work::tests::job_context;
use crate::plugins::install::tests::Fixture;
use crate::plugins::work::{
    Identity, RequestContext, SendBoundary, WorkKind, read_work, reserve, update_work,
};
use crate::s3::object::copy::test::{put_config, seed_bucket, with_blob};
use crate::s3::object::put::PutObjectOperation;

const CONTENT: &[u8] = b"ACGTACGT";

/// What the server got: the request count and the last body.
#[derive(Default)]
struct Seen {
    hits: AtomicUsize,
    body: Mutex<Vec<u8>>,
}

/// A server that answers `status` with a small JSON reply and keeps what it got.
async fn server(status: u16) -> (String, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let kept = seen.clone();
    let router = axum::Router::new().fallback(move |body: axum::body::Bytes| {
        let kept = kept.clone();
        async move {
            kept.hits.fetch_add(1, Ordering::SeqCst);
            *kept.body.lock().unwrap() = body.to_vec();
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
    let (row, _) = reserve(&fixture.ctx, (install, &identity), row)
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
    let found = read_work(&fixture.ctx, install_id, work_id).await;
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
    let Some(SendBoundary::Sent { status, reply }) =
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
    update_work(&fixture.ctx, (install.install_id, row.work_id), begun)
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
    use crate::plugins::marks::{Mark, mark_in};
    use crate::plugins::store::{Txn, transact};
    let fixture = Fixture::new().await;
    let install = uploader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, seen) = server(201).await;
    let (_, spec) = recorded(&fixture, &install, (&base, "PUT")).await;
    let version = crate::plugins::marks::unmarked(&blob, ("in", "a.txt"), None).await;
    let version = version.unwrap().unwrap();
    let mark = Mark {
        install_id: Ulid::from_parts(8, 8),
        work_id: Ulid::from_parts(8, 9),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let marked = async |txn: &Txn<'_>| mark_in(txn, ("in", "a.txt", version), &mark).await;
    transact(&blob, marked).await.unwrap();
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
