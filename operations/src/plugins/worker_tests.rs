//! Tests the worker with the runtime fixture: a run that succeeds, one left with an unknown
//! call, the per install bound, and a stop that leaves a run to start again from the top.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::*;
use crate::driver::drive;
use crate::plugins::copies::store_local;
use crate::plugins::dispatch::event_run;
use crate::plugins::install::InstallRequest;
use crate::plugins::install::tests::Fixture;
use crate::plugins::intents::mark_unknown;
use crate::plugins::lifecycle::set_grants;
use crate::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use crate::plugins::runs::admit;
use crate::plugins::runs::tests::queued;
use aruna_core::plugin_runs::{SourceEvent, SourceKind};
use aruna_core::plugins::PublishedPlugin;
use aruna_plugins::artifact::{ArtifactDigest, attach_manifest, read_manifest};
use aruna_plugins::broker::Intent;

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

fn leb(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        match value {
            0 => return out.push(byte),
            _ => out.push(byte | 0x80),
        }
    }
}

/// The fixture without its secret and connection, which installs refuse.
fn runnable() -> Vec<u8> {
    let manifest = read_manifest(FIXTURE).unwrap();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut content = Vec::new();
    leb("aruna-plugin".len(), &mut content);
    content.extend_from_slice(b"aruna-plugin");
    content.extend_from_slice(&json);
    let mut section = vec![0u8];
    leb(content.len(), &mut section);
    section.extend_from_slice(&content);
    let component = FIXTURE.strip_suffix(section.as_slice()).unwrap();
    let mut narrow = manifest;
    narrow.secrets.clear();
    narrow.connections.clear();
    attach_manifest(component, &narrow).unwrap()
}

/// An install of the runnable fixture whose artifact this node keeps.
pub(crate) async fn installed(fixture: &Fixture) -> PluginInstall {
    let bytes = runnable();
    let manifest = read_manifest(&bytes).unwrap();
    let digest = ArtifactDigest::of(&bytes).to_string();
    let plugin = PublishedPlugin {
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        digest: digest.clone(),
        size: bytes.len() as u64,
        manifest: serde_json::to_string(&manifest).unwrap(),
        published_by: fixture.actor.user_id,
        published_at_ms: 1,
    };
    let publish = PublishEntryOperation::new(PublishEntryConfig {
        actor: fixture.actor.clone(),
        auth_context: fixture.auth(),
        plugin,
        copies: BTreeSet::from([fixture.actor.node_id]),
    });
    drive(publish, &fixture.ctx).await.unwrap();
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    store_local(&fixture.ctx, &config, &digest, &bytes, 1)
        .await
        .unwrap();
    let settings = serde_json::json!({ "api_url": "https://api.example.org" });
    let request = InstallRequest {
        plugin: manifest.name,
        group_id: fixture.group_id,
        version: Some(digest),
        settings: settings.as_object().cloned().unwrap_or_default(),
        grants: BTreeMap::new(),
        install_id: None,
        secrets: BTreeMap::new(),
    };
    fixture.install(&request).await.unwrap()
}

/// A run of `install` that calls `handler` and is due now.
async fn admitted(
    fixture: &Fixture,
    install: &PluginInstall,
    seq: u64,
    handler: &str,
) -> PluginRun {
    let mut run = queued(install.install_id, seq);
    run.digest = install.digest.clone();
    run.settings = install.settings.clone();
    run.handler = handler.into();
    run.due_ms = 0;
    admit(&fixture.ctx, &run).await.unwrap();
    run
}

/// A worker whose guests may spin until they are cancelled, however slow the machine is.
pub(crate) fn worker(fixture: &Fixture) -> Arc<Worker> {
    let ctx = Arc::new(fixture.ctx.clone());
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
    let intent = Intent::new(0, None, &[b"patch"]);
    mark_unknown(&fixture.ctx, unsure.run_id, &intent, "timed out")
        .await
        .unwrap();
    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 2);
    worker.settle().await;
    let done = read_run(&fixture.ctx, install.install_id, ok.run_id)
        .await
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    assert!(
        done.output
            .unwrap_or_default()
            .starts_with(&ok.run_id.to_string())
    );
    assert_eq!(done.attempts, 1);
    let open = read_run(&fixture.ctx, install.install_id, unsure.run_id)
        .await
        .unwrap();
    assert_eq!(open.state, RunState::NeedsAttention);
    assert!(unfinished(&fixture.ctx).await.unwrap().is_empty());
}

/// A created dataset `resource` under the install's whole metadata grant, and that grant's base.
pub(crate) fn created(install: &PluginInstall, resource: &str) -> (SourceEvent, String) {
    let whole = &install.grants["metadata.read"][0];
    let base = whole.strip_suffix("/**").unwrap().to_string();
    let event = SourceEvent {
        kind: SourceKind::DatasetCreated,
        group_id: install.group_id,
        path: format!("{base}/{resource}"),
        resource: resource.into(),
        revision: "r1".into(),
        source: format!("dataset_created/{resource}"),
        anchor_ms: 1,
        journaled_ms: 1,
    };
    (event, base)
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
    assert_eq!(run.handler, "echo");
    let call = invocation(&run, &install, Limits::default(), CancellationToken::new());
    let Trigger::Event(given) = call.trigger else {
        panic!("an event run calls its handler with the event")
    };
    let fields = [given.kind, given.path, given.resource, given.revision];
    assert_eq!(fields, ["dataset_created", event.path.as_str(), "D1", "r1"]);

    admit(&fixture.ctx, &run).await.unwrap();
    let worker = worker(&fixture);
    assert_eq!(worker.claim(unix_timestamp_millis()).await.unwrap(), 1);
    worker.settle().await;
    let done = read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap();
    assert_eq!(done.state, RunState::Succeeded, "{:?}", done.message);
    let output = done.output.unwrap_or_default();
    assert!(output.ends_with("change dataset_created D1 r1"), "{output}");
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
    admit(&fixture.ctx, &run).await.unwrap();
    (run.state, run.attempts) = (RunState::Running, 1);
    save_run(&fixture.ctx, &run).await.unwrap();
    let narrowed = BTreeMap::from([("metadata.read".into(), vec![format!("{base}/D2/**")])]);
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
    let done = read_run(&fixture.ctx, install.install_id, run.run_id)
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
    for n in 0..RUNS_PARALLEL as u128 {
        let busy = (install.install_id, CancellationToken::new());
        first.running().insert(Ulid::from_parts(900, n), busy);
    }
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 0);
    first.running().clear();
    assert_eq!(first.claim(unix_timestamp_millis()).await.unwrap(), 1);
    first.stop().await;
    let cut = read_run(&fixture.ctx, install.install_id, run.run_id)
        .await
        .unwrap();
    assert_eq!((cut.state, cut.attempts), (RunState::Running, 1));

    // The next start runs it again from the top.
    let second = worker(&fixture);
    assert_eq!(second.claim(unix_timestamp_millis()).await.unwrap(), 1);
    second.stop().await;
    let again = read_run(&fixture.ctx, install.install_id, run.run_id)
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
    use crate::plugins::dispatch::{StartError, start_run};
    use crate::plugins::lifecycle::{pause_install, remove_install, resume_install};
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let install = installed(&fixture).await;
    let state = async |run: &PluginRun| {
        let run = read_run(ctx, install.install_id, run.run_id).await.unwrap();
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
    assert!(unfinished(ctx).await.unwrap().is_empty());
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
    use crate::plugins::lifecycle::remove_install;
    let fixture = Fixture::new().await;
    let (ctx, actor, auth) = (&fixture.ctx, &fixture.actor, fixture.auth());
    let install = installed(&fixture).await;
    let open = admitted(&fixture, &install, 1, "echo").await;
    let clean = admitted(&fixture, &install, 2, "echo").await;
    let intent = Intent::new(0, None, &[b"patch"]);
    mark_unknown(ctx, open.run_id, &intent, "timed out")
        .await
        .unwrap();
    remove_install(ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    worker(&fixture)
        .claim(unix_timestamp_millis())
        .await
        .unwrap();
    let state = async |run: &PluginRun| {
        read_run(ctx, install.install_id, run.run_id)
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
