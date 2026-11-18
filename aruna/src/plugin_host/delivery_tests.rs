//! Tests finished work delivered to a guest, through the plugin host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::plugin_host::plugin_work::run_plugin_work;
use crate::plugin_host::worker::Worker;
use crate::plugin_host::worker::tests::worker_on;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::PluginRun;
use aruna_core::plugin_runs::RunState;
use aruna_core::plugin_runs::RunTrigger;
use aruna_core::plugin_runs::SourceKind;
use aruna_core::plugin_runs::StateRemoval;
use aruna_core::plugin_runs::StateWrite;
use aruna_core::structs::execution::job::JobPayload;
use aruna_core::structs::identity::admin_audit::AdminAuditAction;
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::DriverContext;
use aruna_operations::driver::drive;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::store::row_calls;
use aruna_operations::plugins::calls::notify_work;
use aruna_operations::plugins::calls::read_config;
use aruna_operations::plugins::calls::settle_work;
use aruna_operations::plugins::finished::drop_completion;
use aruna_operations::plugins::finished::retry_run;
use aruna_operations::plugins::finished::waits_for;
use aruna_operations::plugins::host_calls::call_intent;
use aruna_operations::plugins::runs::admit;
use aruna_operations::plugins::runs::list_runs;
use aruna_operations::plugins::runs::read_run;
use aruna_operations::plugins::state::MAX_STATE_BYTES;
use aruna_operations::plugins::state::MAX_STATE_VALUE;
use aruna_operations::plugins::state::put_state;
use aruna_operations::plugins::state::remove_state;
use aruna_operations::plugins::test_support::Fixture;
use aruna_operations::plugins::test_support::install_artifact;
use aruna_operations::plugins::test_support::job_context;
use aruna_operations::plugins::test_support::job_guest;
use aruna_operations::plugins::test_support::queued;
use aruna_operations::plugins::work::list_work;
use aruna_operations::s3::object::put::PutObjectOperation;
use aruna_operations::testing_objects::put_config;
use aruna_operations::testing_objects::seed_bucket;
use aruna_operations::testing_objects::with_blob;
use aruna_plugins::broker::Intent;
use std::sync::Arc;
use ulid::Ulid;

/// What a held completion leaves: the node with a blob store, the install and its worker, the
/// failed run and the job it started.
struct Held {
    _root: tempfile::TempDir,
    blob: DriverContext,
    install: PluginInstall,
    worker: Arc<Worker>,
    run_id: Ulid,
    job: String,
    /// The version of the value that fills the state.
    pad: u64,
}

/// The state key that leaves the install too few bytes for a mapping.
const PAD: &str = "pad";

fn audit(fixture: &Fixture, action: AdminAuditAction) -> AdminAuditRecord {
    let actor = fixture.actor.user_id;
    AdminAuditRecord::new(action, actor, actor)
}

/// Runs every due run to its end.
async fn drain(worker: &Arc<Worker>) -> usize {
    let started = worker.claim(unix_timestamp_millis()).await.unwrap();
    worker.settle().await;
    started
}

/// Fills the install's state, so the guest's mapping write after its copy fails; answers the
/// version of the value that takes the last bytes.
async fn fill(ctx: &DriverContext, install_id: Ulid) -> u64 {
    let value = vec![0u8; MAX_STATE_VALUE];
    let count = MAX_STATE_BYTES as usize / MAX_STATE_VALUE - 1;
    let mut used = 0;
    for n in 0..count {
        let key = format!("k{n}");
        used += key.len() + value.len();
        let put = StateWrite {
            key,
            value: value.clone(),
            expected: None,
            intent: call_intent(&Intent::new(n as u32, &[b"fill"])),
        };
        drive(
            put_state(Ulid::from_parts(77, 1), install_id, put.clone()),
            ctx,
        )
        .await
        .unwrap();
    }
    let left = MAX_STATE_BYTES as usize - used - PAD.len() - 8;
    let pad = StateWrite {
        key: PAD.into(),
        value: vec![0u8; left],
        expected: None,
        intent: call_intent(&Intent::new(9_000, &[b"pad"])),
    };
    drive(
        put_state(Ulid::from_parts(77, 1), install_id, pad.clone()),
        ctx,
    )
    .await
    .unwrap()
}

/// The finished runs of `install` that `job_finished` started, with their output.
async fn finish_outputs(blob: &DriverContext, install: &PluginInstall) -> Vec<PluginRun> {
    let (runs, _) = drive(list_runs(install.install_id, None, 50), blob)
        .await
        .unwrap();
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
    let pad = fill(&blob, install.install_id).await;
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
    drive(admit(run.clone()), &blob).await.unwrap();
    let worker = worker_on(&blob, fixture);
    assert_eq!(drain(&worker).await, 1);
    let failed = drive(read_run(install.install_id, run.run_id), &blob)
        .await
        .unwrap();
    let why = failed.message.clone().unwrap_or_default();
    assert_eq!(failed.state, RunState::Failed, "{why}");
    assert!(why.contains("would exceed 16 MiB"), "{why}");
    run_copy(fixture, &blob, &install).await;
    let (rows, _) = drive(list_work(install.install_id, None, 10), &blob)
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
        pad,
    }
}

async fn notify(fixture: &Fixture, blob: &DriverContext) -> usize {
    let config = read_config(blob, fixture.actor.realm_id).await.unwrap();
    notify_work(blob, &config, fixture.actor.node_id, 3)
        .await
        .unwrap()
}

/// Runs the install's one copy job on this node and records how it ended.
async fn run_copy(fixture: &Fixture, blob: &DriverContext, install: &PluginInstall) {
    let (rows, _) = drive(list_work(install.install_id, None, 10), blob)
        .await
        .unwrap();
    let job = rows[0].job.unwrap();
    let record = drive(row_calls::read_job_record(job), blob).await;
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
    drive(row_calls::insert_job(&finished), blob).await.unwrap();
    assert_eq!(settle_work(blob, 2).await.unwrap(), 1);
}

#[tokio::test]
async fn retry_delivers_once() {
    let fixture = Fixture::new().await;
    let held = held(&fixture).await;
    let (blob, install) = (&held.blob, &held.install);
    let free = StateRemoval {
        key: PAD.into(),
        expected: held.pad,
        intent: call_intent(&Intent::new(9_001, &[b"free"])),
    };
    let freed = drive(
        remove_state(Ulid::from_parts(77, 1), install.install_id, free),
        blob,
    );
    freed.await.unwrap();
    let action = audit(&fixture, AdminAuditAction::PluginRunRetried);
    let ids = (install.install_id, held.run_id);
    drive(retry_run(ids, &action, 0).unwrap(), blob)
        .await
        .unwrap();
    assert_eq!(drain(&held.worker).await, 1);
    // The rerun reached the same copy call and got the same job, then wrote its mapping.
    let rerun = drive(read_run(install.install_id, held.run_id), blob)
        .await
        .unwrap();
    assert_eq!(rerun.state, RunState::Succeeded, "{:?}", rerun.message);
    assert_eq!(rerun.output, Some(format!("\"{}\"", held.job)));
    let (rows, _) = drive(list_work(install.install_id, None, 10), blob)
        .await
        .unwrap();
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
    drive(
        drop_completion((install.install_id, work_id), &action).unwrap(),
        blob,
    )
    .await
    .unwrap();
    assert_eq!(notify(&fixture, blob).await, 0);
    assert_eq!(drain(&held.worker).await, 0);
    assert!(finish_outputs(blob, install).await.is_empty());
}
