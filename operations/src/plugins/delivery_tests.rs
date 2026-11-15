//! Tests a copy through the worker and the test guest: a run that failed after its job id holds
//! the job's end; a retry gets the same id and delivers it once, a drop delivers nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{PluginRun, RunState, RunTrigger, SourceKind};
use aruna_core::structs::execution::job::JobPayload;
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{Intent, StatePut, StateRemove};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::plugin_work::run_plugin_work;
use crate::jobs::plugin_work::tests::job_context;
use crate::jobs::store::{insert_job, read_job_record};
use crate::plugins::finished::{drop_completion, notify_work, retry_run, waits_for};
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::tests::queued;
use crate::plugins::runs::{admit, list_runs, read_run};
use crate::plugins::state::{MAX_STATE_BYTES, MAX_STATE_VALUE, put_state, remove_state};
use crate::plugins::work::{list_work, settle_work};
use crate::plugins::worker::Worker;
use crate::plugins::worker::tests::{install_artifact, job_guest, worker_on};
use crate::s3::object::copy::test::{put_config, seed_bucket, with_blob};
use crate::s3::object::put::PutObjectOperation;

/// The state key that leaves the install too few bytes for a mapping.
const PAD: &str = "pad";

/// What a held completion leaves: the node with a blob store, the install and its worker, the
/// failed run and the job it started.
struct Held {
    _root: tempfile::TempDir,
    blob: DriverContext,
    install: PluginInstall,
    worker: Arc<Worker>,
    run_id: Ulid,
    job: String,
}

/// Fills the install's state, so the guest's mapping write after its copy fails.
async fn fill(ctx: &DriverContext, install_id: Ulid) {
    let value = vec![0u8; MAX_STATE_VALUE];
    let count = MAX_STATE_BYTES as usize / MAX_STATE_VALUE - 1;
    let mut used = 0;
    for n in 0..count {
        let key = format!("k{n}");
        used += key.len() + value.len();
        let put = StatePut {
            key,
            value: value.clone(),
            expected: None,
            intent: Intent::new(n as u32, &[b"fill"]),
        };
        put_state(ctx, Ulid::from_parts(77, 1), install_id, &put)
            .await
            .unwrap();
    }
    let left = MAX_STATE_BYTES as usize - used - PAD.len() - 8;
    let pad = StatePut {
        key: PAD.into(),
        value: vec![0u8; left],
        expected: None,
        intent: Intent::new(9_000, &[b"pad"]),
    };
    put_state(ctx, Ulid::from_parts(77, 1), install_id, &pad)
        .await
        .unwrap();
}

/// Runs every due run to its end.
async fn drain(worker: &Arc<Worker>) -> usize {
    let started = worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    started
}

/// Runs the install's one copy job on this node and records how it ended.
async fn run_copy(fixture: &Fixture, blob: &DriverContext, install: &PluginInstall) {
    let (rows, _) = list_work(blob, install.install_id, None, 10).await.unwrap();
    let job = rows[0].job.unwrap();
    let record = read_job_record(&blob.storage_handle, job, None).await;
    let record = record.unwrap().unwrap();
    let JobPayload::PluginWork(spec) = record.payload.clone() else {
        panic!("a plugin copy runs as plugin work");
    };
    let JobRunOutcome::Succeeded(result) =
        run_plugin_work(&job_context(blob, fixture), &spec).await
    else {
        panic!("the copy runs");
    };
    // The jobs runtime is not running here, so the finished record is written as it would.
    let mut finished = record;
    finished.state = aruna_core::structs::execution::job::JobState::Succeeded;
    (finished.result, finished.finished_at_ms) = (Some(result), Some(1));
    insert_job(&blob.storage_handle, &finished).await.unwrap();
    assert_eq!(settle_work(blob, 2).await.unwrap(), 1);
}

async fn notify(fixture: &Fixture, blob: &DriverContext) -> usize {
    let config = read_config(blob, fixture.actor.realm_id).await.unwrap();
    notify_work(blob, &config, fixture.actor.node_id, 3)
        .await
        .unwrap()
}

/// The finished runs of `install` that `job_finished` started, with their output.
async fn finish_outputs(blob: &DriverContext, install: &PluginInstall) -> Vec<PluginRun> {
    let (runs, _) = list_runs(blob, install.install_id, None, 50).await.unwrap();
    (runs.into_iter())
        .filter(|run| {
            matches!(&run.trigger, RunTrigger::Event(event) if event.kind == SourceKind::JobFinished)
        })
        .collect()
}

/// A copy run whose mapping write failed for a full state store, its job done and settled.
async fn held(fixture: &Fixture) -> Held {
    let install = install_artifact(fixture, job_guest()).await;
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    for bucket in ["in", "out"] {
        seed_bucket(&blob, bucket, install.group_id, install.account, Vec::new()).await;
    }
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let source = put_config(realm, install.group_id, node, "in", "a.txt", b"bytes");
    drive(PutObjectOperation::new(source), &blob).await.unwrap();
    fill(&blob, install.install_id).await;
    let mut run = queued(install.install_id, 1);
    (run.digest, run.settings) = (install.digest.clone(), install.settings.clone());
    run.group_id = install.group_id;
    let input =
        serde_json::json!({"call": "copy_mapped", "source": "in/a.txt", "target": "out/a.txt"});
    let by = fixture.actor.user_id;
    run.trigger = RunTrigger::Manual {
        by,
        input: input.to_string(),
    };
    (run.handler, run.due_ms) = ("manual".into(), 0);
    admit(&blob, &run).await.unwrap();
    let worker = worker_on(&blob, fixture);
    assert_eq!(drain(&worker).await, 1);
    let failed = read_run(&blob, install.install_id, run.run_id)
        .await
        .unwrap();
    let why = failed.message.clone().unwrap_or_default();
    assert_eq!(failed.state, RunState::Failed, "{why}");
    assert!(why.contains("would exceed 16 MiB"), "{why}");
    run_copy(fixture, &blob, &install).await;
    let (rows, _) = list_work(&blob, install.install_id, None, 10)
        .await
        .unwrap();
    assert_eq!(waits_for(&rows[0], Some(failed.state)), Some(run.run_id));
    // The job ended, but its starting run did not succeed: no `job_finished` run yet.
    assert_eq!(notify(fixture, &blob).await, 0);
    let job = rows[0].work_id.to_string();
    Held {
        _root: root,
        blob,
        install,
        worker,
        run_id: run.run_id,
        job,
    }
}

fn audit(fixture: &Fixture, action: AdminAuditAction) -> AdminAuditRecord {
    let actor = fixture.actor.user_id;
    AdminAuditRecord::new(action, actor, actor)
}

#[tokio::test]
async fn retry_delivers_once() {
    let fixture = Fixture::new().await;
    let held = held(&fixture).await;
    let (blob, install) = (&held.blob, &held.install);
    let free = StateRemove {
        key: PAD.into(),
        expected: 1,
        intent: Intent::new(9_001, &[b"free"]),
    };
    let freed = remove_state(blob, Ulid::from_parts(77, 1), install.install_id, &free);
    freed.await.unwrap();
    let action = audit(&fixture, AdminAuditAction::PluginRunRetried);
    let ids = (install.install_id, held.run_id);
    retry_run(blob, ids, &action, 0).await.unwrap();
    assert_eq!(drain(&held.worker).await, 1);
    // The rerun reached the same copy call and got the same job, then wrote its mapping.
    let rerun = read_run(blob, install.install_id, held.run_id)
        .await
        .unwrap();
    assert_eq!(rerun.state, RunState::Succeeded, "{:?}", rerun.message);
    assert_eq!(rerun.output, Some(format!("\"{}\"", held.job)));
    let (rows, _) = list_work(blob, install.install_id, None, 10).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(notify(&fixture, blob).await, 1);
    assert_eq!(drain(&held.worker).await, 1);
    assert_eq!(notify(&fixture, blob).await, 0);
    let finished = finish_outputs(blob, install).await;
    assert_eq!(finished.len(), 1);
    let output: serde_json::Value =
        serde_json::from_str(finished[0].output.as_ref().unwrap()).unwrap();
    let expected = serde_json::json!({"job": held.job, "state": "succeeded", "mapping": "mapped"});
    assert_eq!(output, expected);
}

#[tokio::test]
async fn drop_delivers_none() {
    let fixture = Fixture::new().await;
    let held = held(&fixture).await;
    let (blob, install) = (&held.blob, &held.install);
    let action = audit(&fixture, AdminAuditAction::PluginCompletionDropped);
    let work_id = Ulid::from_string(&held.job).unwrap();
    drop_completion(blob, (install.install_id, work_id), &action)
        .await
        .unwrap();
    assert_eq!(notify(&fixture, blob).await, 0);
    assert_eq!(drain(&held.worker).await, 0);
    assert!(finish_outputs(blob, install).await.is_empty());
}
