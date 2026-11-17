//! Tests starting copies from runs: a repeated call answers the same work and job, the same call
//! with other arguments conflicts, and refused paths or a paused install start nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;

use super::*;
use crate::driver::DriverContext;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::plugin_work::tests::copier;
use crate::plugins::runs::tests::queued;
use crate::plugins::work::read_work;
use crate::s3::object::copy::test::seed_bucket;
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;

pub(crate) fn scope(fixture: &Fixture, install: &PluginInstall) -> RunScope {
    let mut run = queued(install.install_id, 1);
    (run.digest, run.group_id) = (install.digest.clone(), install.group_id);
    RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run,
    }
}

pub(crate) fn path(fixture: &Fixture, install: &PluginInstall, rest: &str) -> String {
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    format!("/{realm}/g/{}/data/{node}/{rest}", install.group_id)
}

pub(crate) fn request(fixture: &Fixture, install: &PluginInstall, target: &str) -> CopyRequest {
    CopyRequest {
        source: path(fixture, install, "in/a.txt"),
        source_version: None,
        target: path(fixture, install, target),
        ordinal: 0,
    }
}

pub(crate) async fn ready(fixture: &Fixture) -> PluginInstall {
    let install = copier(fixture).await;
    let account = install.account;
    for bucket in ["in", "out"] {
        seed_bucket(&fixture.ctx, bucket, install.group_id, account, Vec::new()).await;
    }
    install
}

#[tokio::test]
async fn repeat_finds_work() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let scope = scope(&fixture, &install);
    let copy = request(&fixture, &install, "out/a.txt");
    let first = start_copy(&fixture.ctx, &scope, copy.clone())
        .await
        .unwrap();
    let again = start_copy(&fixture.ctx, &scope, copy.clone())
        .await
        .unwrap();
    assert_eq!(first, again);
    let row = drive(read_work(install.install_id, first), &fixture.ctx).await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.state, WorkState::Dispatched);
    assert!(row.job.is_some() && row.version_id.is_some());
    let paths = (row.source.as_deref(), row.target.as_deref());
    assert_eq!(
        paths,
        (Some(copy.source.as_str()), Some(copy.target.as_str()))
    );
    // The same call with other arguments conflicts; the run's next call starts its own copy.
    let mut other = request(&fixture, &install, "out/b.txt");
    let refused = start_copy(&fixture.ctx, &scope, other.clone()).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::Conflict)
    );
    other.ordinal = 1;
    let next = start_copy(&fixture.ctx, &scope, other).await.unwrap();
    assert_ne!(next, first);
}

#[tokio::test]
async fn refusals_start_nothing() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let scope = scope(&fixture, &install);
    let kind = async |copy: CopyRequest| {
        let started = start_copy(&fixture.ctx, &scope, copy).await;
        started.map_err(|error| error.kind)
    };
    let mut elsewhere = request(&fixture, &install, "out/a.txt");
    elsewhere.target = elsewhere.target.replace("/data/", "/meta/");
    assert_eq!(kind(elsewhere).await, Err(ErrorKind::NotGranted));
    let missing = request(&fixture, &install, "nowhere/a.txt");
    assert_eq!(kind(missing).await, Err(ErrorKind::NotFound));
    edit_config(&fixture, |config| config.installs[0].paused = true).await;
    let paused = request(&fixture, &install, "out/a.txt");
    assert_eq!(kind(paused).await, Err(ErrorKind::Denied));
}

#[tokio::test]
async fn read_source_suffices() {
    // A copy reads its source and writes its target: read on the source is enough, and the
    // target needs write.
    use crate::plugins::lifecycle::set_grants;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let (incoming, outgoing) = (
        path(&fixture, &install, "in/**"),
        path(&fixture, &install, "out/**"),
    );
    let choices = std::collections::BTreeMap::from([
        ("read".to_string(), vec![incoming.clone()]),
        ("write".to_string(), vec![outgoing]),
    ]);
    let (actor, auth) = (&fixture.actor, fixture.auth());
    set_grants(&fixture.ctx, actor, &auth, install.install_id, &choices)
        .await
        .unwrap();
    let scope = scope(&fixture, &install);
    let copy = request(&fixture, &install, "out/a.txt");
    assert!(start_copy(&fixture.ctx, &scope, copy).await.is_ok());
    let mut back = request(&fixture, &install, "in/b.txt");
    back.source = path(&fixture, &install, "out/a.txt");
    let refused = start_copy(&fixture.ctx, &scope, back).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::NotGranted)
    );
}

#[tokio::test]
async fn broker_starts_copies() {
    // A run names objects by bucket path; the broker expands them, pins a given version, and a
    // rerun of the call gets the same job id.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Intent, JobCopy};

    use crate::plugins::broker::RunBroker;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let copy = |version: Option<&str>| JobCopy {
        source: "in/a.txt".into(),
        version: version.map(str::to_string),
        target: "out/a.txt".into(),
        intent: Intent::new(3, &[b"copy"]),
    };
    let id = broker.jobs_copy(&cx, copy(None)).await.unwrap();
    assert_eq!(broker.jobs_copy(&cx, copy(None)).await.unwrap(), id);
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.source, Some(path(&fixture, &install, "in/a.txt")));
    let bad = broker.jobs_copy(&cx, copy(Some("v1"))).await;
    assert_eq!(bad.map_err(|error| error.kind), Err(ErrorKind::Invalid));
}

#[tokio::test]
async fn shifted_copy_refused() {
    // A retried run that reaches its copy at the ordinal of an earlier state write changed: the
    // call is refused before any work is reserved, and the run fails as changed.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Intent, JobCopy, StatePut};

    use crate::plugins::broker::RunBroker;
    use crate::plugins::work::list_work;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let put = StatePut {
        key: "init".into(),
        value: b"1".to_vec(),
        expected: None,
        intent: Intent::new(1, &[b"init"]),
    };
    broker.state_put(&cx, put).await.unwrap();
    let copy = JobCopy {
        source: "in/a.txt".into(),
        version: None,
        target: "out/a.txt".into(),
        intent: Intent::new(1, &[b"copy"]),
    };
    let refused = broker.jobs_copy(&cx, copy).await;
    assert_eq!(
        refused.map_err(|error| error.kind),
        Err(ErrorKind::Conflict)
    );
    assert!(broker.changed());
    let (rows, _) = drive(list_work(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// An install granted the group's data whose `catalog` connection injects the slot `api_key`.
async fn fetcher(fixture: &Fixture) -> PluginInstall {
    let install = crate::plugins::bound::tests::bound(fixture).await;
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    edit_config(fixture, |config| {
        config.installs[0].grants.insert("write".into(), vec![data]);
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = (config.await.unwrap().installs.into_iter())
        .find(|found| found.install_id == install.install_id)
        .unwrap();
    seed_bucket(
        &fixture.ctx,
        "in",
        install.group_id,
        install.account,
        Vec::new(),
    )
    .await;
    install
}

fn fetch(fixture: &Fixture, install: &PluginInstall, secret: bool) -> DownloadRequest {
    DownloadRequest {
        context: RequestContext {
            connection: "catalog".into(),
            base: "https://api.example.org/api/".into(),
            method: "GET".into(),
            path: "reads.fq".into(),
            query: Vec::new(),
            headers: Vec::new(),
        },
        target: path(fixture, install, "in/reads.fq"),
        checksums: Vec::new(),
        max_bytes: None,
        secret,
        ordinal: 0,
    }
}

/// The marks of the work keyspace, by their key.
async fn marks(fixture: &Fixture) -> usize {
    use crate::plugins::marks::MARK_ROW;
    use crate::plugins::store::Txn;
    let txn = Txn::direct(&fixture.ctx);
    let rows = txn.scan(PLUGIN_WORK_KEYSPACE, &[MARK_ROW]).await.unwrap();
    rows.len()
}

#[tokio::test]
async fn download_marked() {
    // A download through a connection with a secret marks the version it will write in the
    // reservation, before any byte exists; the same call again finds the same work and mark.
    use crate::plugins::marks::is_marked;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let scope = scope(&fixture, &install);
    let request = fetch(&fixture, &install, true);
    let work_id = start_download(&fixture.ctx, &scope, request.clone())
        .await
        .unwrap();
    let row = drive(read_work(install.install_id, work_id), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    let version = ("in", "reads.fq", row.version_id.unwrap());
    assert!(
        drive(is_marked(version).unwrap(), &fixture.ctx)
            .await
            .unwrap()
    );
    let again = start_download(&fixture.ctx, &scope, request.clone()).await;
    assert_eq!(again, Ok(work_id));
    assert_eq!(marks(&fixture).await, 1);
    let mut other = request;
    other.max_bytes = Some(5);
    let changed = start_download(&fixture.ctx, &scope, other)
        .await
        .unwrap_err();
    assert_eq!(changed.kind, ErrorKind::Conflict);
}

#[tokio::test]
async fn download_unmarked() {
    // Without a secret the answer is the service's public data, so nothing is marked.
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let scope = scope(&fixture, &install);
    start_download(&fixture.ctx, &scope, fetch(&fixture, &install, false))
        .await
        .unwrap();
    assert_eq!(marks(&fixture).await, 0);
}

#[tokio::test]
async fn marks_once() {
    // Two reservations of the same call at once, the loser tried again, leave one mark.
    use crate::plugins::work::Reserve;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let identity = Identity::Call {
        run_id: Ulid::from_parts(9, 9),
        ordinal: 0,
    };
    let proposed = || {
        let mut row = WorkRow::reserved(&install, WorkKind::Download, Ulid::from_parts(9, 9), 1);
        (row.digest, row.version_id) = ([3; 32], Some(Ulid::generate()));
        row
    };
    let marked = Some(("in", "reads.fq"));
    let reserve = async |row| {
        drive(
            reserve((&install, &identity), row, marked).unwrap(),
            &fixture.ctx,
        )
        .await
    };
    let (first, second) = tokio::join!(reserve(proposed()), reserve(proposed()));
    let mut found = Vec::new();
    for outcome in [first, second] {
        let outcome = match outcome {
            Ok(found) => found,
            Err(_) => reserve(proposed()).await.unwrap(),
        };
        found.push(outcome);
    }
    let fresh = found
        .iter()
        .filter(|(_, how)| *how == Reserve::Fresh)
        .count();
    assert_eq!(fresh, 1, "one reservation wrote the row");
    assert_eq!(found[0].0.work_id, found[1].0.work_id);
    assert_eq!(marks(&fixture).await, 1);
}

#[tokio::test]
async fn retired_unmarked() {
    // A download that was never submitted, retired with its install, loses its mark at the next
    // sweep: its row is gone and the version it would have written never existed.
    use crate::plugins::jobs::retire_work;
    use crate::plugins::worker::sweep_marks;
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let identity = Identity::Call {
        run_id: Ulid::from_parts(9, 9),
        ordinal: 0,
    };
    let mut row = WorkRow::reserved(&install, WorkKind::Download, Ulid::from_parts(9, 9), 1);
    row.version_id = Some(Ulid::generate());
    let marked = Some(("in", "reads.fq"));
    drive(
        reserve((&install, &identity), row, marked).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        sweep_marks(&fixture.ctx).await,
        Ok(0),
        "its work may still write"
    );
    edit_config(&fixture, |config| {
        config.installs[0].state = InstallState::Removing
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    retire_work(&fixture.ctx, &config.await.unwrap(), 5)
        .await
        .unwrap();
    assert_eq!(sweep_marks(&fixture.ctx).await, Ok(1));
    assert_eq!(marks(&fixture).await, 0);
}

#[tokio::test]
async fn broker_starts_downloads() {
    // The broker keeps the request with the address from the run's settings, and a rerun of the
    // call gets the same job; a checksum it cannot check is invalid input.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Field, Intent, JobDownload};

    use crate::plugins::broker::RunBroker;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = r#"{"catalog_url":"https://catalog.example.org"}"#.into();
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let download = |checksums: Vec<Field>| JobDownload {
        service: "catalog".into(),
        path: "reads.fq".into(),
        query: Vec::new(),
        headers: Vec::new(),
        target: "in/reads.fq".into(),
        size_limit: Some(9),
        checksums,
        intent: Intent::new(4, &[b"download"]),
    };
    let md5 = |value: &str| Field {
        name: "MD5".into(),
        value: value.into(),
    };
    let digest = "0123456789abcdef0123456789abcdef";
    let id = broker
        .jobs_download(&cx, download(vec![md5(digest)]))
        .await
        .unwrap();
    let again = broker.jobs_download(&cx, download(vec![md5(digest)])).await;
    assert_eq!(again, Ok(id.clone()));
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.target, Some(path(&fixture, &install, "in/reads.fq")));
    let context = row.context.unwrap();
    assert_eq!(
        (context.base.as_str(), context.path.as_str()),
        ("https://catalog.example.org/api/", "reads.fq")
    );
    for wrong in [
        md5("00"),
        md5("not hex"),
        Field {
            name: "md4".into(),
            value: digest.into(),
        },
    ] {
        let refused = broker.jobs_download(&cx, download(vec![wrong])).await;
        assert_eq!(refused.map_err(|error| error.kind), Err(ErrorKind::Invalid));
    }
}

#[tokio::test]
async fn narrowed_download_refused() {
    // A rule that shows only the status refuses the download at the call; no work is recorded.
    use std::sync::Arc;

    use aruna_core::secrets::ResponseExposure;
    use aruna_plugins::broker::{Broker, CallContext, Intent, JobDownload};

    use crate::plugins::bound::tests::{ORIGIN, hold_keys, secret_with};
    use crate::plugins::broker::RunBroker;
    use crate::plugins::work::list_work;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    secret_with(&fixture, &install, ResponseExposure::Status).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = format!(r#"{{"catalog_url":"{ORIGIN}"}}"#);
    hold_keys(&fixture.ctx);
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let download = JobDownload {
        service: "catalog".into(),
        path: "reads.fq".into(),
        query: Vec::new(),
        headers: Vec::new(),
        target: "in/reads.fq".into(),
        size_limit: None,
        checksums: Vec::new(),
        intent: Intent::new(4, &[b"download"]),
    };
    let refused = broker.jobs_download(&cx, download).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Denied);
    assert_eq!(
        refused.message,
        "this connection does not expose its whole answer"
    );
    let (works, _) = drive(list_work(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    assert!(works.is_empty());
}

/// A blob context with the object `in/a.txt` of the fetcher's group.
async fn stored_object(
    fixture: &Fixture,
    install: &PluginInstall,
) -> (tempfile::TempDir, DriverContext) {
    use crate::driver::drive;
    use crate::s3::object::copy::test::{put_config, with_blob};
    use crate::s3::object::put::PutObjectOperation;
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let put = put_config(realm, install.group_id, node, "in", "a.txt", b"ACGT");
    drive(PutObjectOperation::new(put), &blob).await.unwrap();
    (root, blob)
}

fn send(fixture: &Fixture, install: &PluginInstall, version: Option<Ulid>) -> UploadRequest {
    UploadRequest {
        context: RequestContext {
            connection: "catalog".into(),
            base: "https://api.example.org/api/".into(),
            method: "PUT".into(),
            path: "a.txt".into(),
            query: Vec::new(),
            headers: Vec::new(),
        },
        source: path(fixture, install, "in/a.txt"),
        version,
        form: None,
        ordinal: 0,
    }
}

#[tokio::test]
async fn upload_started() {
    // An upload needs a readable, unmarked source; the same call finds the same work.
    use crate::plugins::broker::unmarked;
    use crate::plugins::marks::{Mark, mark_row};
    use crate::plugins::rows::write_rows;
    let fixture = Fixture::keyed().await;
    let install = fetcher(&fixture).await;
    let (_root, blob) = stored_object(&fixture, &install).await;
    let scope = scope(&fixture, &install);
    let work_id = start_upload(&blob, &scope, send(&fixture, &install, None))
        .await
        .unwrap();
    assert_eq!(
        start_upload(&blob, &scope, send(&fixture, &install, None)).await,
        Ok(work_id)
    );
    let version = unmarked(&blob, ("in", "a.txt"), None)
        .await
        .unwrap()
        .unwrap();
    let pinned = start_upload(&blob, &scope, send(&fixture, &install, Some(version))).await;
    assert_eq!(pinned.unwrap_err().kind, ErrorKind::Conflict);
    let mut missing = send(&fixture, &install, None);
    (missing.source, missing.ordinal) = (path(&fixture, &install, "in/none"), 1);
    assert_eq!(
        start_upload(&blob, &scope, missing).await.unwrap_err().kind,
        ErrorKind::NotFound
    );
    let mark = Mark {
        install_id: Ulid::from_parts(8, 8),
        work_id: Ulid::from_parts(8, 9),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let marked = mark_row(("in", "a.txt", version), &mark).unwrap();
    drive(write_rows(vec![marked]), &blob).await.unwrap();
    let mut later = send(&fixture, &install, None);
    later.ordinal = 2;
    assert_eq!(
        start_upload(&blob, &scope, later).await.unwrap_err().kind,
        ErrorKind::Denied
    );
}

#[tokio::test]
async fn reply_rechecked() {
    // A kept upload answer is shown while the secret's rule shows whole answers, and dropped,
    // with the state kept, once the rule shows the status only.
    use std::sync::Arc;

    use aruna_core::secrets::{ResponseExposure, SecretName};
    use aruna_plugins::broker::{Broker, CallContext};

    use crate::plugins::bound::tests::{ORIGIN, hold_keys, rule_for, secret_with};
    use crate::plugins::broker::RunBroker;
    use crate::plugins::rules::{RuleGuard, consumer, edit_rules};
    use crate::plugins::work::{WorkChange, reserve, update_work};
    let fixture = Fixture::keyed().await;
    fetcher(&fixture).await;
    let puts = serde_json::json!({
        "secrets": { "api_key": { "title": "API key", "kind": "bearer" } },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "auth": "api_key",
            "methods": ["GET", "PUT"] } }
    });
    let digest = fixture.publish("0.3.0", "ef", puts).await;
    edit_config(&fixture, |config| {
        (config.installs[0].digest, config.installs[0].version) = (digest, "0.3.0".into());
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = config.await.unwrap().installs.remove(0);
    secret_with(&fixture, &install, ResponseExposure::Full).await;
    let mut row = WorkRow::reserved(&install, WorkKind::Upload, Ulid::from_parts(9, 9), 1);
    let mut context = send(&fixture, &install, None).context;
    context.base = format!("{ORIGIN}/api/");
    row.context = Some(context);
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let (row, _) = drive(
        reserve((&install, &identity), row, None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let reply = r#"{"status":201,"headers":[["etag","e1"]],"body":"eyJpZCI6N30="}"#;
    let ids = (install.install_id, row.work_id);
    let answer = (201, Some(reply.into()));
    let sent = WorkChange::Sent {
        status: answer.0,
        reply: answer.1,
        bytes: 8,
        version: Ulid::nil(),
    };
    drive(update_work(ids, sent), &fixture.ctx).await.unwrap();
    hold_keys(&fixture.ctx);
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope(&fixture, &install));
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let id = row.work_id.to_string();
    let shown = broker.jobs_get(&cx, &id).await.unwrap();
    assert_eq!(shown.reply_status, Some(201));
    assert_eq!(shown.reply_body.as_deref(), Some(&br#"{"id":7}"#[..]));
    assert_eq!(shown.reply_headers[0].value, "e1");
    let name = SecretName::try_from("api-key".to_string()).unwrap();
    let guard = RuleGuard::Revision {
        install_id: install.install_id,
        revision: install.revision,
    };
    let rule = rule_for(&install, ResponseExposure::Status);
    let target = (install.group_id, &name);
    edit_rules(
        &fixture.ctx,
        &fixture.actor,
        target,
        (&consumer(&install), vec![rule]),
        guard,
    )
    .await
    .unwrap();
    let narrowed = broker.jobs_get(&cx, &id).await.unwrap();
    assert_eq!((narrowed.reply_status, narrowed.reply_body), (None, None));
    assert_eq!(narrowed.state, shown.state);
}

#[tokio::test]
async fn broker_starts_uploads() {
    // An upload is a PUT or a POST; a form names its file part after the source's file name.
    use std::sync::Arc;

    use aruna_plugins::broker::{Broker, CallContext, Field, Intent, JobUpload, Method};

    use crate::plugins::broker::RunBroker;
    use crate::plugins::work::read_work;
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let puts = serde_json::json!({
        "permissions": { "required": ["objects.copy"] },
        "connections": { "catalog": { "url": "{catalog_url}/api/", "methods": ["GET", "POST"] } }
    });
    let digest = fixture.publish("0.3.0", "ef", puts).await;
    edit_config(&fixture, |config| {
        (config.installs[0].digest, config.installs[0].version) = (digest, "0.3.0".into());
    })
    .await;
    let config = crate::plugins::install::read_config(&fixture.ctx, fixture.actor.realm_id);
    let install = (config.await.unwrap().installs.into_iter())
        .find(|found| found.install_id == install.install_id)
        .unwrap();
    let (_root, blob) = stored_object(&fixture, &install).await;
    let mut scope = scope(&fixture, &install);
    scope.run.settings = r#"{"catalog_url":"https://catalog.example.org"}"#.into();
    let broker = RunBroker::new(Arc::new(blob), scope);
    let cx = CallContext {
        run: "run".into(),
        install: install.install_id.to_string(),
        max_bytes: 1 << 20,
    };
    let upload = |method| JobUpload {
        service: "catalog".into(),
        method,
        path: "deposit".into(),
        query: Vec::new(),
        headers: Vec::new(),
        source: "in/a.txt".into(),
        version: None,
        file_field: Some("file".into()),
        fields: vec![Field {
            name: "title".into(),
            value: "Run 7".into(),
        }],
        intent: Intent::new(5, &[b"upload"]),
    };
    let refused = broker
        .jobs_upload(&cx, upload(Method::Get))
        .await
        .unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Invalid);
    let id = broker.jobs_upload(&cx, upload(Method::Post)).await.unwrap();
    let row = drive(
        read_work(install.install_id, id.parse().unwrap()),
        &fixture.ctx,
    )
    .await;
    let row = row.unwrap().unwrap();
    assert_eq!(row.source, Some(path(&fixture, &install, "in/a.txt")));
    assert_eq!(row.context.unwrap().method, "POST");
}
