//! Tests the worker with the runtime fixture: a run that succeeds, one left with an unknown
//! call, the per install bound, and a stop that leaves a run to start again from the top.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::driver::drive;
use crate::plugins::attempt::invocation;
use crate::plugins::calls::*;
use crate::plugins::grants::short_path;
use crate::plugins::host_calls::call_intent;
use crate::plugins::install::InstallRequest;
use crate::plugins::install::tests::Fixture;
use crate::plugins::intents::unknown_row;
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::plugins::rows::write_rows;
use crate::plugins::runs::event_run;
use crate::plugins::runs::tests::queued;
use crate::plugins::runs::{admit, conclude, ended, read_run, save_run, unfinished};
#[allow(unused_imports)]
pub(crate) use crate::plugins::test_support::{
    connected, created, echoed_sync, install_artifact, installed, job_guest, publish_artifact,
    scoped, secured, synced,
};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{
    MAX_ATTEMPTS, RUNS_PARALLEL, RunState, RunTrigger, SourceEvent, SourceKind,
};
use aruna_core::plugins::PublishedPlugin;
use aruna_plugins::artifact::{ArtifactDigest, attach_manifest, read_manifest};
use aruna_plugins::broker::Intent;
use aruna_plugins::{InvokeError, Trigger};
use std::time::Duration;
use ulid::Ulid;

/// A run of `install` that reaches the fixture's `handler` and is due now: `echo` through a
/// dataset event, `spin` through the timer.
async fn admitted(
    fixture: &Fixture,
    install: &PluginInstall,
    seq: u64,
    handler: &str,
) -> PluginRun {
    let mut run = queued(install.install_id, seq);
    run.digest = install.digest.clone();
    run.settings = install.settings.clone();
    run.handler = "timer".into();
    if handler == "echo" {
        run.trigger = RunTrigger::Event(created(install, "D1").0);
        run.handler = "dataset_created".into();
    }
    run.due_ms = 0;
    drive(admit(run.clone()), &fixture.ctx).await.unwrap();
    run
}

/// A worker whose guests may spin until they are cancelled, however slow the machine is.
pub(crate) fn worker(fixture: &Fixture) -> Arc<Worker> {
    worker_on(&fixture.ctx, fixture)
}

/// The same worker on `ctx`, such as one with a blob store.
pub(crate) fn worker_on(ctx: &DriverContext, fixture: &Fixture) -> Arc<Worker> {
    let ctx = Arc::new(ctx.clone());
    let mut config = RuntimeConfig::default();
    config.ceiling.fuel = u64::MAX;
    config.ceiling.wall = Duration::from_secs(3600);
    let limits = Limits {
        fuel: u64::MAX,
        wall: Duration::from_secs(3600),
        ..Limits::default()
    };
    let node = (fixture.actor.realm_id, fixture.actor.node_id);
    Worker::with_budget(ctx, node, config, limits).unwrap()
}

#[tokio::test]
async fn finishes_runs() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ok = admitted(&fixture, &install, 1, "echo").await;
    let unsure = admitted(&fixture, &install, 2, "echo").await;
    let intent = Intent::new(0, &[b"patch"]);
    let unknown = unknown_row(unsure.run_id, &call_intent(&intent), "timed out").unwrap();
    drive(write_rows(vec![unknown]), &fixture.ctx)
        .await
        .unwrap();
    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 2);
    worker.settle().await;
    let done = drive(read_run(install.install_id, ok.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    assert!(
        done.output
            .unwrap_or_default()
            .starts_with(&format!("\"{}", ok.run_id))
    );
    assert_eq!(done.attempts, 1);
    let open = drive(read_run(install.install_id, unsure.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(open.state, RunState::NeedsAttention);
    assert!(drive(unfinished(), &fixture.ctx).await.unwrap().is_empty());
}

#[tokio::test]
async fn event_input() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let (event, _) = created(&install, "D1");
    let run = event_run(&config, &install, &event, 0).unwrap();
    assert_eq!(run.handler, "dataset_created");
    let call = invocation(
        &run,
        (&install, fixture.actor.node_id),
        Limits::default(),
        CancellationToken::new(),
    );
    let Trigger::Event(given) = call.trigger else {
        panic!("an event run calls its handler with the event")
    };
    let fields = [given.kind, given.path, given.resource, given.revision];
    assert_eq!(fields, ["dataset_created", event.path.as_str(), "D1", "r1"]);

    drive(admit(run.clone()), &fixture.ctx).await.unwrap();
    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 1);
    worker.settle().await;
    let done = drive(read_run(install.install_id, run.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    let output = done.output.unwrap_or_default();
    assert!(
        output.ends_with("change dataset_created D1 r1\""),
        "{output}"
    );
}

#[tokio::test]
async fn sync_input() {
    // A sync event starts the `sync_completed` handler; the guest gets the kind, the source
    // path, the relationship and the job.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let event = synced(&install, "raw/", Ulid::from_parts(3, 3));
    let run = event_run(&config, &install, &event, 0).unwrap();
    assert_eq!(run.handler, "sync_completed");
    let call = invocation(
        &run,
        (&install, fixture.actor.node_id),
        Limits::default(),
        CancellationToken::new(),
    );
    let Trigger::Event(given) = call.trigger else {
        panic!("an event run calls its handler with the event")
    };
    let fields = [given.kind, given.path, given.resource, given.revision];
    let expected = [
        &event.kind.name().to_string(),
        &event.path,
        &event.resource,
        &event.revision,
    ];
    assert_eq!(fields.each_ref(), expected);
    assert_eq!(event.resource, Ulid::from_parts(3, 3).to_string());
}

#[tokio::test]
async fn event_rechecked() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let (event, base) = created(&install, "D1");
    // An earlier attempt passed the check and was cut short; its grant is removed since.
    let mut run = event_run(&config, &install, &event, 0).unwrap();
    drive(admit(run.clone()), &fixture.ctx).await.unwrap();
    (run.state, run.attempts) = (RunState::Running, 1);
    drive(save_run(&run).unwrap(), &fixture.ctx).await.unwrap();
    let narrowed = scoped(vec![format!("{base}/D2/**")]);
    let auth = fixture.auth();
    set_grants(
        &fixture.ctx,
        &fixture.actor,
        &auth,
        install.install_id,
        &narrowed,
    )
    .await
    .unwrap();

    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 1);
    worker.settle().await;
    let done = drive(read_run(install.install_id, run.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!((done.state, done.attempts), (RunState::Cancelled, 2));
    let message = done.message.unwrap_or_default();
    assert!(message.contains("is not granted on"), "{message}");
    // The guest never ran: the echo handler would have stored its output.
    assert_eq!(done.output, None);
}

#[tokio::test]
async fn bounded_and_restarted() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let run = admitted(&fixture, &install, 1, "spin").await;
    let first = worker(&fixture);
    let busy = |n| Ulid::from_parts(900, n);
    for n in 0..RUNS_PARALLEL as u128 {
        first.supervisor.admit(busy(n), install.install_id);
    }
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 0);
    for n in 0..RUNS_PARALLEL as u128 {
        first.supervisor.finish(busy(n));
    }
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 1);
    first.stop().await;
    let cut = drive(read_run(install.install_id, run.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!((cut.state, cut.attempts), (RunState::Running, 1));

    // The next start runs it again from the top.
    let second = worker(&fixture);
    assert_eq!(second.claim(unix_timestamp_millis()).await.unwrap(), 1);
    second.stop().await;
    let again = drive(read_run(install.install_id, run.run_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(again.attempts, 2);
}

/// Marks this node as draining in the realm config.
async fn drain(fixture: &Fixture) {
    let mut config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let node = fixture.actor.node_id;
    let entry = config
        .placement_map
        .iter_mut()
        .find(|entry| entry.node_id == node);
    entry.unwrap().draining = true;
    let target = aruna_core::document::DocumentTarget::RealmConfig {
        realm_id: fixture.actor.realm_id,
    };
    let value = config.to_bytes(&fixture.actor).unwrap();
    let key = target.storage_key().to_vec();
    fixture
        .write_row(target.storage_keyspace(), key, value)
        .await;
}

#[tokio::test]
async fn revoked_runs() {
    use crate::plugins::dispatch::StartError;
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let install = installed(&fixture).await;
    let state = async |run: &PluginRun| {
        let run = drive(read_run(install.install_id, run.run_id), ctx)
            .await
            .unwrap();
        (run.state, run.message.unwrap_or_default())
    };
    let worker = worker(&fixture);
    let paused = admitted(&fixture, &install, 1, "spin").await;
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 1);
    pause_install(ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    let cancelled = (RunState::Cancelled, "the install was paused".to_string());
    assert_eq!(state(&paused).await, cancelled);

    // A draining receiver cancels its guests and refuses new runs, without a record write.
    resume_install(ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    let lost = admitted(&fixture, &install, 2, "spin").await;
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 1);
    drain(&fixture).await;
    worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    assert_eq!(state(&lost).await.1, "the node that runs it is draining");
    let (realm, node) = (actor.realm_id, actor.node_id);
    let refused = start_run(ctx, &auth, realm, node, install.install_id, "{}", 1).await;
    assert!(matches!(refused, Err(StartError::Lost { receiver, .. }) if receiver == node));
    let before = read_config(ctx, realm).await.unwrap().installs;
    assert!(
        before
            .iter()
            .any(|record| record.revision == install.revision + 2)
    );

    // Queued runs of a removed install end as cancelled.
    let queued = admitted(&fixture, &install, 3, "echo").await;
    remove_install(ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    worker.claim(unix_timestamp_millis()).await.unwrap();
    assert_eq!(state(&queued).await.0, RunState::Cancelled);
    assert!(drive(unfinished(), ctx).await.unwrap().is_empty());
}

#[test]
fn intents_decide_first() {
    let mut run = queued(Ulid::from_parts(1, 1), 1);
    run.attempts = MAX_ATTEMPTS;
    let unknown = Ok(Some("call 0 has an unknown outcome".to_string()));
    // A changed call does not hide an unknown one.
    let mut changed = run.clone();
    conclude(
        &mut changed,
        Ended::Failed("x".into()),
        true,
        unknown.clone(),
        9,
    );
    assert_eq!(changed.state, RunState::NeedsAttention);
    let mut cancelled = run.clone();
    conclude(
        &mut cancelled,
        Ended::Cancelled("removed".into()),
        false,
        unknown,
        9,
    );
    assert_eq!(cancelled.state, RunState::NeedsAttention);
    // Unreadable intents never end a run, also after its last attempt.
    for ended in [
        Ended::Retry("x".into()),
        Ended::Failed("x".into()),
        Ended::Output(None),
    ] {
        let mut held = run.clone();
        let unreadable = Err(StoreError("read failed".into()));
        conclude(&mut held, ended, false, unreadable, 9);
        assert_eq!(
            (held.state, held.attempts),
            (RunState::Queued, MAX_ATTEMPTS - 1)
        );
    }
    let mut last = run.clone();
    conclude(&mut last, Ended::Retry("x".into()), false, Ok(None), 9);
    assert_eq!(last.state, RunState::Failed);
}

#[tokio::test]
async fn removed_with_intents() {
    // A queued run of a removed install whose call may have had its effect needs attention.
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let install = installed(&fixture).await;
    let open = admitted(&fixture, &install, 1, "echo").await;
    let clean = admitted(&fixture, &install, 2, "echo").await;
    let intent = Intent::new(0, &[b"patch"]);
    let unknown = unknown_row(open.run_id, &call_intent(&intent), "timed out").unwrap();
    drive(write_rows(vec![unknown]), ctx).await.unwrap();
    remove_install(ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    worker(&fixture)
        .claim(unix_timestamp_millis())
        .await
        .unwrap();
    let state = async |run: &PluginRun| {
        drive(read_run(install.install_id, run.run_id), ctx)
            .await
            .unwrap()
            .state
    };
    assert_eq!(state(&open).await, RunState::NeedsAttention);
    assert_eq!(state(&clean).await, RunState::Cancelled);
}

#[test]
fn busy_keeps_attempts() {
    // A full runtime never entered the guest, so the run waits without using an attempt.
    let busy = ended(Err(InvokeError::Overloaded), false);
    assert!(matches!(busy, Ended::Busy(_)), "{busy:?}");
    let mut run = queued(Ulid::from_parts(1, 1), 1);
    run.attempts = MAX_ATTEMPTS;
    for _ in 0..3 * MAX_ATTEMPTS {
        conclude(&mut run, Ended::Busy("busy".into()), false, Ok(None), 9);
        assert_eq!(run.state, RunState::Queued);
        run.attempts += 1;
    }
    assert_eq!(run.attempts, MAX_ATTEMPTS);
    assert!(matches!(
        ended(Err(InvokeError::Cancelled), true),
        Ended::Stopped
    ));
}

#[tokio::test]
async fn retries_hide_values() {
    use crate::plugins::runs::{read_run, save_run};
    use aruna_core::compute::Secret;
    use aruna_core::secrets::SecretFields;
    const VALUE: &str = "canary-value-7b1e";
    let fixture = Fixture::new().await;
    let mut run = queued(Ulid::from_parts(3, 3), 1);
    drive(admit(run.clone()), &fixture.ctx).await.unwrap();
    let scope = RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run: run.clone(),
    };
    let broker = RunBroker::new(Arc::new(fixture.ctx.clone()), scope);
    broker.shown.lock().unwrap().push(SecretFields::new(
        [("token".to_string(), Secret::new(VALUE))].into(),
    ));
    // A retryable error that names a value handed to the code keeps the run without it.
    let ended = scrub_ended(&broker, Ended::Retry(format!("upstream said {VALUE}")));
    conclude(&mut run, ended, false, Ok(None), 5);
    drive(save_run(&run).unwrap(), &fixture.ctx).await.unwrap();
    let stored = drive(read_run(run.install_id, run.run_id), &fixture.ctx).await;
    let stored = stored.unwrap();
    assert_eq!(stored.state, RunState::Queued);
    assert_eq!(stored.message.as_deref(), Some("upstream said [redacted]"));
}

#[tokio::test]
async fn event_paths_shortened() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let local = fixture.actor.node_id;
    let realm = fixture.actor.realm_id;
    let full = format!("/{realm}/g/{}/data/{local}/raw/a.fq", install.group_id);
    assert_eq!(short_path(&full, (&install, local)), "raw/a.fq");
    // An object of another node keeps its full path, so the plugin never names it ambiguously.
    let other = iroh::SecretKey::from_bytes(&[9; 32]).public();
    let remote = full.replace(&local.to_string(), &other.to_string());
    assert_eq!(short_path(&remote, (&install, local)), remote);
    assert_eq!(short_path("D1", (&install, local)), "D1");
}
