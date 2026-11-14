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

fn scope(fixture: &Fixture, install: &PluginInstall) -> RunScope {
    let mut run = queued(install.install_id, 1);
    run.digest = install.digest.clone();
    RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run,
    }
}

fn path(fixture: &Fixture, install: &PluginInstall, rest: &str) -> String {
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    format!("/{realm}/g/{}/data/{node}/{rest}", install.group_id)
}

fn request(fixture: &Fixture, install: &PluginInstall, target: &str) -> CopyRequest {
    CopyRequest {
        source: path(fixture, install, "in/a.txt"),
        source_version: None,
        target: path(fixture, install, target),
        ordinal: 0,
    }
}

async fn ready(fixture: &Fixture) -> PluginInstall {
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
