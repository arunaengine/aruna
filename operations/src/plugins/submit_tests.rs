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
