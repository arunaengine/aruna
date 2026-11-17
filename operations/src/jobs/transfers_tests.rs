//! Tests download attempts against a local server: the answer becomes the version once, a
//! redirect or client error fails, a server error is retried, a bound or narrowed rule refuses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use std::sync::atomic::AtomicUsize;

use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::job::{JobErrorKind, PluginWorkAction};
use axum::http::{HeaderMap, StatusCode};
use serde_json::json;

use super::*;
use crate::driver::DriverContext;
use crate::jobs::plugin_work::run_plugin_work;
use crate::jobs::plugin_work::tests::job_context;
use crate::plugins::install::tests::Fixture;
use crate::plugins::work::{Identity, RequestContext, WorkKind, reserve_marked};
use crate::s3::object::copy::test::{seed_bucket, with_blob};
use crate::s3::object::get::{GetObjectInput, GetObjectOperation};

const BODY: &[u8] = b"ACGTACGT";

/// A server that answers `status` with `BODY` at every path and counts the requests.
async fn server(status: u16) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let router = axum::Router::new().fallback(move || {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "text/plain".parse().unwrap());
            headers.insert("location", "http://127.0.0.1:9/elsewhere".parse().unwrap());
            (StatusCode::from_u16(status).unwrap(), headers, BODY)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}/files/"), hits)
}

/// An install of a version that requests `transfers.download`, granted the group's data.
async fn downloader(fixture: &Fixture) -> PluginInstall {
    let download = json!({ "permissions": { "required": ["transfers.download"] } });
    let digest = fixture.publish("0.2.0", "cd", download).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    request.grants = std::collections::BTreeMap::from([
        ("read".into(), vec![data.clone()]),
        ("write".into(), vec![data]),
    ]);
    fixture.install(&request).await.unwrap()
}

/// A recorded download of `install` from `base` into `in/reads.fq`, and the spec of its job.
async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    base: &str,
    bounds: (Vec<(ChecksumAlgorithm, Vec<u8>)>, Option<u64>),
) -> (WorkRow, PluginWorkSpec) {
    reserved(fixture, install, base, bounds, false).await
}

/// The same, with the version it will write marked in the reservation when `marked`.
async fn reserved(
    fixture: &Fixture,
    install: &PluginInstall,
    base: &str,
    (checksums, max_bytes): (Vec<(ChecksumAlgorithm, Vec<u8>)>, Option<u64>),
    marked: bool,
) -> (WorkRow, PluginWorkSpec) {
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, install.group_id);
    let target = format!("{data}/{}/in/reads.fq", fixture.actor.node_id);
    let mut row = WorkRow::reserved(install, WorkKind::Download, Ulid::generate(), 1);
    row.version_id = Some(Ulid::generate());
    row.target = Some(target.clone());
    row.context = Some(RequestContext {
        connection: "catalog".into(),
        base: base.into(),
        method: "GET".into(),
        path: "reads.fq".into(),
        query: Vec::new(),
        headers: Vec::new(),
    });
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let marked = marked.then_some(("in", "reads.fq"));
    let (row, _) = drive(reserve((install, &identity), row, marked).unwrap(), &fixture.ctx).await
        .unwrap();
    let action = PluginWorkAction::Download {
        target,
        version_id: row.version_id.unwrap(),
        checksums,
        max_bytes,
    };
    let spec = PluginWorkSpec {
        realm_id: fixture.actor.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action,
    };
    (row, spec)
}

/// A blob context with the bucket `in` of the install's group.
async fn ready(fixture: &Fixture, install: &PluginInstall) -> (tempfile::TempDir, DriverContext) {
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    seed_bucket(&blob, "in", install.group_id, install.account, Vec::new()).await;
    (root, blob)
}

fn failed(outcome: JobRunOutcome) -> (JobErrorKind, String) {
    match outcome {
        JobRunOutcome::Failed(error) => (error.kind, error.message),
        JobRunOutcome::Succeeded(_) => panic!("expected a failure, got a success"),
        JobRunOutcome::Deferred(error) => {
            panic!("expected a failure, got deferred: {}", error.message)
        }
        _ => panic!("expected a failure"),
    }
}

/// Signals of a held answer: the request arrived, and the test lets the answer go.
#[derive(Default)]
struct Gate {
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

/// A server that answers `BODY` once the test releases the request.
async fn held() -> (String, Arc<Gate>) {
    let gate = Arc::new(Gate::default());
    let held = gate.clone();
    let router = axum::Router::new().fallback(move || {
        let held = held.clone();
        async move {
            held.arrived.notify_one();
            held.release.notified().await;
            (StatusCode::OK, BODY)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}/files/"), gate)
}

async fn stored(ctx: &DriverContext, fixture: &Fixture, install: &PluginInstall) -> Vec<u8> {
    let read = GetObjectInput {
        bucket: "in".to_string(),
        key: "reads.fq".to_string(),
        version_id: None,
        range: None,
        group_id: install.group_id,
        user_identity: install.account,
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
    };
    let mut blob = drive(GetObjectOperation::new(read), ctx)
        .await
        .unwrap()
        .blob;
    let mut bytes = Vec::new();
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    bytes
}

#[tokio::test]
async fn downloads_once() {
    // The answer becomes the minted version; a rerun after a lost answer replays that version
    // without a second request.
    let fixture = Fixture::new().await;
    let install = downloader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, hits) = server(200).await;
    let (row, spec) = recorded(&fixture, &install, &base, (Vec::new(), Some(64))).await;
    let ctx = job_context(&blob, &fixture);
    for attempt in 0..2 {
        let outcome = run_plugin_work(&ctx, &spec).await;
        let JobRunOutcome::Succeeded(JobResultPayload::PluginWork { bytes, version_id }) = outcome
        else {
            panic!("attempt {attempt} did not succeed");
        };
        assert_eq!((bytes, version_id), (BODY.len() as u64, row.version_id));
    }
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(stored(&blob, &fixture, &install).await, BODY);
}

#[tokio::test]
async fn answers_classified() {
    // A redirect is not followed and a client error fails; a server error is tried again.
    let fixture = Fixture::new().await;
    let install = downloader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let ctx = job_context(&blob, &fixture);
    for (status, kind) in [
        (302, JobErrorKind::Permanent),
        (404, JobErrorKind::Permanent),
        (429, JobErrorKind::Retryable),
        (503, JobErrorKind::Retryable),
    ] {
        let (base, hits) = server(status).await;
        let (_, spec) = recorded(&fixture, &install, &base, (Vec::new(), None)).await;
        let (got, message) = failed(run_plugin_work(&ctx, &spec).await);
        assert_eq!(got, kind, "{status}: {message}");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "{status}: one request only");
    }
}

#[tokio::test]
async fn bound_and_checksum() {
    // An answer past its bound or with another digest than expected never becomes a version.
    let fixture = Fixture::new().await;
    let install = downloader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let ctx = job_context(&blob, &fixture);
    let (base, _) = server(200).await;
    let (_, small) = recorded(&fixture, &install, &base, (Vec::new(), Some(4))).await;
    let (kind, message) = failed(run_plugin_work(&ctx, &small).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("size limit of 4 bytes"), "{message}");
    let wrong = vec![(ChecksumAlgorithm::Md5, vec![0u8; 16])];
    let (_, mismatch) = recorded(&fixture, &install, &base, (wrong, None)).await;
    let (kind, message) = failed(run_plugin_work(&ctx, &mismatch).await);
    assert_eq!(kind, JobErrorKind::Permanent, "{message}");
    let missing = GetObjectInput {
        bucket: "in".to_string(),
        key: "reads.fq".to_string(),
        version_id: None,
        range: None,
        group_id: install.group_id,
        user_identity: install.account,
        realm_id: fixture.actor.realm_id,
        node_id: fixture.actor.node_id,
    };
    assert!(
        drive(GetObjectOperation::new(missing), &blob)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn ungranted_target() {
    // A target outside the install's write patterns ends the work before any request.
    let fixture = Fixture::new().await;
    let install = downloader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let ctx = job_context(&blob, &fixture);
    let (base, hits) = server(200).await;
    let (_, spec) = recorded(&fixture, &install, &base, (Vec::new(), None)).await;
    crate::plugins::bound::tests::edit_config(&fixture, |config| {
        config.installs[0].grants.insert("write".into(), Vec::new());
    })
    .await;
    let (kind, _) = failed(run_plugin_work(&ctx, &spec).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn narrowed_rule() {
    // A secret whose rule shows the status only would hide what the bucket keeps, so the work
    // ends after the release and before any request.
    use crate::plugins::bound::tests::{ORIGIN, TOKEN, bound, edit_config, keys, secret_with};
    use crate::plugins::install::read_config;
    use aruna_core::secrets::ResponseExposure;
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    edit_config(&fixture, |config| {
        config.installs[0].grants.insert("write".into(), vec![data]);
    })
    .await;
    secret_with(&fixture, &install, ResponseExposure::Status).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let install = config.installs[0].clone();
    let (_root, blob) = ready(&fixture, &install).await;
    let base = format!("{ORIGIN}/api/");
    let (row, spec) = recorded(&fixture, &install, &base, (Vec::new(), None)).await;
    let PluginWorkAction::Download {
        target, version_id, ..
    } = &spec.action
    else {
        unreachable!("a recorded work is a download")
    };
    let download = Download {
        target,
        version_id: *version_id,
        checksums: &[],
        max_bytes: None,
    };
    let ctx = job_context(&blob, &fixture);
    let outcome = download_work(&ctx, &spec, (&install, &config, &row), (download, &keys())).await;
    let (kind, message) = failed(outcome);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert_eq!(message, "this connection does not expose its whole answer");
    // The released token is in no row the work keeps.
    let kept = crate::plugins::work::read_work(&fixture.ctx, install.install_id, row.work_id);
    let kept = kept.await.unwrap().unwrap();
    assert!(!format!("{kept:?}").contains(TOKEN));
}

#[tokio::test]
async fn sweep_during_download() {
    // A sweep while the answer is still on its way keeps the mark of the version not yet
    // written; once written, the version keeps it too.
    use crate::plugins::marks::is_marked;
    use crate::plugins::worker::sweep_marks;
    let fixture = Fixture::new().await;
    let install = downloader(&fixture).await;
    let (_root, blob) = ready(&fixture, &install).await;
    let (base, gate) = held().await;
    let (row, spec) = reserved(&fixture, &install, &base, (Vec::new(), None), true).await;
    let version = ("in", "reads.fq", row.version_id.unwrap());
    let ctx = job_context(&blob, &fixture);
    let sweep = async {
        gate.arrived.notified().await;
        let swept = sweep_marks(&blob).await;
        gate.release.notify_one();
        swept
    };
    let (outcome, swept) = tokio::join!(run_plugin_work(&ctx, &spec), sweep);
    assert_eq!(swept, Ok(0), "the download may still write");
    let JobRunOutcome::Succeeded(JobResultPayload::PluginWork { version_id, .. }) = outcome else {
        panic!("the held download did not succeed");
    };
    assert_eq!(version_id, row.version_id);
    assert_eq!(sweep_marks(&blob).await, Ok(0), "the version exists");
    assert!(drive(is_marked(version).unwrap(), &blob).await.unwrap());
    assert_eq!(stored(&blob, &fixture, &install).await, BODY);
}
