//! Tests starting copies from runs: a repeated call answers the same work and job, the same call
//! with other arguments conflicts, and refused paths or a paused install start nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;

use super::*;
use crate::jobs::plugin_work::tests::copier;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::install::tests::Fixture;
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
    let row = read_work(&fixture.ctx, install.install_id, first).await;
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
    let row = read_work(&fixture.ctx, install.install_id, id.parse().unwrap()).await;
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
    let (rows, _) = list_work(&fixture.ctx, install.install_id, None, 10)
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
    let row = read_work(&fixture.ctx, install.install_id, work_id)
        .await
        .unwrap()
        .unwrap();
    let version = ("in", "reads.fq", row.version_id.unwrap());
    assert!(is_marked(&fixture.ctx, version).await.unwrap());
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
    let reserve = |row| reserve_marked(&fixture.ctx, (&install, &identity), row, marked);
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
    use crate::plugins::marks::sweep_marks;
    use crate::plugins::work::retire_work;
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
    reserve_marked(&fixture.ctx, (&install, &identity), row, marked)
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
