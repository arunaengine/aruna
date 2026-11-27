//! Tests JSON template escaping, fail-closed outside inputs, slot caps and cancel/missing recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::applet::record::{Access, Applet, SubmissionFile, applet_key};
use aruna_core::automation::firing::{Firing, FiringState, StageState, TriggerPayload};
use aruna_core::automation::keys::active_key;
use aruna_core::automation::record::{Spec, Stage, StageKind, Trigger, When};
use aruna_core::automation::scope::{Audience, Scope};
use aruna_core::calls::{ChildOutcome, IoResult};
use aruna_core::keyspaces::{
    APPLETS_KEYSPACE, AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE, REALM_CONFIG_KEYSPACE,
    RUN_IDEMPOTENCY_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::{WorkspaceMode, user_dedup_key};
use aruna_core::structs::execution::workflow_run::{
    RunKeyRecord, RunState, WorkflowRunRecord, WorkflowRunSpec, run_key, run_key_row,
};
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_runner::driver::drive;
use serde_json::json;
use ulid::Ulid;

use super::AutoError;
use super::check::{CheckAutoOperation, render_sample_stage};
use super::control::{AckCancelsOperation, CancelFiringOperation};
use super::copy_stage::{call_intent, stage_intent_id};
use super::firing_reads::GetFiringOperation;
use super::flow_tests::Env;
use super::run_stage::{StartStageRun, parse_inputs, render_json, run_idempotency_key};
use super::runner::StepFiringsOperation;
use super::start::new_firing_rows;
use crate::applets::AppletError;
use crate::jobs::submit::SubmitJobSpec;
use crate::jobs::submit::local::SubmitLocalOperation;
use crate::plugins::intents::{IntentRecord, IntentState, intent_key, settled_row_in};

#[tokio::test]
async fn runner_guards_recovery() {
    let env = Env::new().await;
    let wf_id = Ulid::from_bytes([63; 16]);
    let run_stage = Stage {
        name: "run".into(),
        needs: Some(vec![]),
        when: When::Passed,
        kind: StageKind::Run {
            workflow_id: wf_id,
            version: 1,
            values_json: r#"{"file": "{{trigger.filename}}", "note": "k={{trigger.key}}"}"#.into(),
            inputs_json: Some(
                r#"{"reads": {"bucket": "{{trigger.bucket}}", "key": "{{trigger.key}}", "version_id": "{{trigger.version_id}}"}}"#
                    .into(),
            ),
            output_bucket: Some("out".into()),
            output_prefix: Some("runs".into()),
            placement_site: None,
            secrets: BTreeMap::new(),
            provenance_crate: false,
        },
    };
    let mut spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        stages: vec![run_stage],
        scope: Scope {
            workflows: vec![(wf_id, 1)],
            ..Scope::default()
        },
    };
    spec.scope.limits.max_concurrent_workflows = 1;
    let auto = env.create_active(spec.clone()).await;

    // Quotes, backslashes and newlines escape safely; malformed inputs_json is rejected.
    let ver = Ulid::from_parts(20, 1);
    let quoted = TriggerPayload::DataUploaded {
        bucket: "raw".into(),
        key: "dir/a\"b\\c\nd.csv".into(),
        version_id: ver,
        path: "/p".into(),
        size_bytes: 5,
    };
    let qf = Firing::new(
        Ulid::from_bytes([104; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        quoted,
        10,
    );
    let rendered = render_json(r#"{"name": "{{trigger.filename}}"}"#, &qf).unwrap();
    assert_eq!(rendered["name"], "a\"b\\c\nd.csv");
    // Unquoted whole-value templates preserve JSON types without double-substituting values (#10).
    let manual_f = Firing::new(
        Ulid::from_bytes([115; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: r#"{"sample": "{{trigger.key}}", "count": 7}"#.into(),
            actor: env.owner.user_id,
        },
        10,
    );
    let whole = render_json("{{trigger.values}}", &manual_f).unwrap();
    assert_eq!(whole["sample"], "{{trigger.key}}");
    assert_eq!(whole["count"], 7);
    let num = render_json(r#"{"sz": {{trigger.size_bytes}}}"#, &qf).unwrap();
    assert_eq!(num["sz"], 5);
    assert!(StartStageRun::new(auto.clone(), qf.clone(), 0, (env.node, env.realm)).is_ok());
    // Submission files with inputs_json=None and explicit source_node_id stay identical across pins/JSON (#9).
    let sub_f = Firing::new(
        Ulid::from_bytes([116; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::AppletSubmission {
            applet_id: Ulid::from_bytes([117; 16]),
            version_id: ver,
            pseudonym: "u1".into(),
            guest: false,
            outside_input: false,
            bucket: "raw".into(),
            key: "sub/meta.json".into(),
            files: vec![SubmissionFile {
                name: "reads".into(),
                bucket: "raw".into(),
                key: "sub/r1.fq".into(),
                version_id: ver,
            }],
        },
        10,
    );
    let (sub_pins, sub_json) = parse_inputs(None, &sub_f, env.node).unwrap();
    assert_eq!(sub_pins.len(), 1);
    let parsed_sub: serde_json::Value = serde_json::from_str(&sub_json.unwrap()).unwrap();
    assert_eq!(parsed_sub["reads"]["key"], "sub/r1.fq");
    // Stage validation merges pinned submission files into values_json before coordinator check.
    let mut sub_spec = spec.clone();
    if let StageKind::Run {
        values_json,
        inputs_json,
        ..
    } = &mut sub_spec.stages[0].kind
    {
        *values_json = "{}".into();
        *inputs_json = None;
    }
    let sub_val_f = Firing::new(
        Ulid::from_bytes([122; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &sub_spec,
        sub_f.trigger.clone(),
        10,
    );
    let stage_op = StartStageRun::new(auto.clone(), sub_val_f, 0, (env.node, env.realm)).unwrap();
    let merged_vals = stage_op.validation_values().unwrap();
    assert_eq!(
        merged_vals["reads"],
        format!("aruna://raw/sub/r1.fq?version={ver}")
    );
    let remote_node = iroh::SecretKey::from_bytes(&[12; 32]).public();
    let explicit = format!(
        r#"{{"reads": {{"bucket": "raw", "key": "x", "version_id": "{ver}", "source_node_id": "{remote_node}"}}}}"#
    );
    let (rem_pins, rem_json) = parse_inputs(Some(&explicit), &qf, env.node).unwrap();
    assert_eq!(rem_pins[0].source_node_id, Some(remote_node));
    let parsed_rem: serde_json::Value = serde_json::from_str(&rem_json.unwrap()).unwrap();
    assert_eq!(
        parsed_rem["reads"]["source_node_id"],
        remote_node.to_string()
    );
    // Check renderer agrees with execution on whole-value templates and reports render errors.
    let mut tmpl_spec = spec.clone();
    if let StageKind::Run {
        values_json,
        inputs_json,
        ..
    } = &mut tmpl_spec.stages[0].kind
    {
        *values_json = "{{trigger.values}}".into();
        *inputs_json = None;
    }
    let sample_obj = json!({"count": 7});
    let checked_vals = render_sample_stage(
        "{{trigger.values}}",
        None,
        &tmpl_spec,
        Some(&sample_obj),
        env.node,
    )
    .unwrap();
    assert_eq!(checked_vals, sample_obj);
    let mut bad_tmpl_spec = tmpl_spec.clone();
    if let StageKind::Run { values_json, .. } = &mut bad_tmpl_spec.stages[0].kind {
        *values_json = "{broken".into();
    }
    let bad_auto = env.create_active(bad_tmpl_spec).await;
    let bad_report = drive(
        CheckAutoOperation::new(&env.owner, bad_auto.automation_id, env.node, None),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(!bad_report.ok);
    assert_eq!(bad_report.workflows_checked, 0);
    assert!(
        bad_report
            .diagnostics
            .iter()
            .any(|d| d.contains("invalid template JSON")),
        "{:?}",
        bad_report.diagnostics
    );
    let mut bad_spec = spec.clone();
    if let StageKind::Run { inputs_json, .. } = &mut bad_spec.stages[0].kind {
        *inputs_json =
            Some(r#"{"reads": {"bucket": "raw", "key": "x", "version_id": "bad"}}"#.into());
    }
    let bad_f = Firing::new(
        Ulid::from_bytes([105; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &bad_spec,
        qf.trigger.clone(),
        10,
    );
    assert!(matches!(
        StartStageRun::new(auto.clone(), bad_f, 0, (env.node, env.realm)),
        Err(AutoError::Invalid(_))
    ));

    // outside_input fails closed when applet turns group-only, disappears, or errors.
    let app_id = Ulid::from_bytes([106; 16]);
    let mut group_applet = crate::applets::change::tests::applet();
    group_applet.applet_id = app_id;
    group_applet.group_id = env.group;
    group_applet.host_node = env.node;
    group_applet.access = Access::GroupMembers;
    group_applet.guests = None;
    env.write_row(
        APPLETS_KEYSPACE,
        applet_key(app_id),
        group_applet.to_bytes().unwrap(),
    )
    .await;
    let outside_trigger = TriggerPayload::AppletSubmission {
        applet_id: app_id,
        version_id: ver,
        pseudonym: "g1".into(),
        guest: true,
        outside_input: true,
        bucket: "out".into(),
        key: "in/sub.json".into(),
        files: Vec::new(),
    };
    let of = Firing::new(
        Ulid::from_bytes([107; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        outside_trigger,
        10,
    );
    let err = drive(
        StartStageRun::new(auto.clone(), of.clone(), 0, (env.node, env.realm)).unwrap(),
        &env.ctx,
    )
    .await;
    assert_eq!(err, Err(AutoError::Forbidden));

    let mut err_op = StartStageRun::new(auto.clone(), of, 0, (env.node, env.realm)).unwrap();
    err_op.first().unwrap();
    let failed_lookup = err_op.next(IoResult::Child(ChildOutcome::new(Err::<
        Option<Applet>,
        AppletError,
    >(
        AppletError::Storage("io".into()),
    ))));
    assert!(matches!(failed_lookup, Err(AutoError::Storage(_))));

    // Cross-firing workflow cap and node-disabled Workflows leave stage Ready (not Failed).
    let live_child = Ulid::from_bytes([108; 16]);
    let child_rec = env.sample_run(live_child, wf_id, RunState::Running, None);
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(live_child),
        child_rec.to_bytes().unwrap(),
    )
    .await;
    let mut f1 = Firing::new(
        Ulid::from_bytes([109; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        10,
    );
    f1.stages[0].state = StageState::Running;
    f1.stages[0].receipt.workflow_run_id = Some(live_child);
    let f2 = Firing::new(
        Ulid::from_bytes([110; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        11,
    );
    for (ks, k, v) in new_firing_rows(&f1)
        .unwrap()
        .into_iter()
        .chain(new_firing_rows(&f2).unwrap())
    {
        env.write_row(&ks, k, v).await;
    }
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (f2_after, _) = drive(
        GetFiringOperation::new(&env.owner, f2.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(f2_after.stages[0].state, StageState::Ready);

    // Child cancel failure persists cancel_sent=false and retries on worker tick.
    let remote_child_id = Ulid::from_bytes([111; 16]);
    let mut remote_child = env.sample_run(remote_child_id, wf_id, RunState::Running, None);
    remote_child.node_id = iroh::SecretKey::from_bytes(&[8; 32]).public();
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(remote_child_id),
        remote_child.to_bytes().unwrap(),
    )
    .await;
    let mut f3 = Firing::new(
        Ulid::from_bytes([112; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        12,
    );
    f3.stages[0].state = StageState::Running;
    f3.stages[0].receipt.workflow_run_id = Some(remote_child_id);
    for (ks, k, v) in new_firing_rows(&f3).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let cancelling = drive(
        CancelFiringOperation::new(&env.owner, f3.firing_id, (env.node, env.realm)),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(!cancelling.firing.stages[0].receipt.cancel_sent);
    assert!(cancelling.firing.stages[0].error.is_some());
    remote_child.node_id = env.node;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(remote_child_id),
        remote_child.to_bytes().unwrap(),
    )
    .await;
    let step_rep = drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    assert!(step_rep.wake_runs.contains(&remote_child_id));
    assert!(
        step_rep
            .cancel_acks
            .iter()
            .any(|a| a.firing_id == f3.firing_id && a.run_id == remote_child_id)
    );
    // cancel_sent stays false in storage until the caller delivers signals and commits acks.
    let (unacked, _) = drive(
        GetFiringOperation::new(&env.owner, f3.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(!unacked.stages[0].receipt.cancel_sent);
    drive(AckCancelsOperation::new(step_rep.cancel_acks), &env.ctx)
        .await
        .unwrap();
    let (retried, _) = drive(
        GetFiringOperation::new(&env.owner, f3.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(retried.stages[0].receipt.cancel_sent);

    // Stranded claimed Run recovers existing child via idempotency key even when Cancelling (#1, #2).
    let recovered_child_id = Ulid::from_bytes([118; 16]);
    let recovered_child = env.sample_run(recovered_child_id, wf_id, RunState::Running, None);
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(recovered_child_id),
        recovered_child.to_bytes().unwrap(),
    )
    .await;
    let mut stranded_run = Firing::new(
        Ulid::from_bytes([119; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        14,
    );
    stranded_run.state = FiringState::Cancelling;
    stranded_run.stages[0].state = StageState::Cancelling;
    let idem_key = run_idempotency_key(stranded_run.firing_id, "run", stranded_run.attempt);
    let key_rec = RunKeyRecord {
        run_id: recovered_child_id,
        request_digest: [0; 32],
    };
    env.write_row(
        RUN_IDEMPOTENCY_KEYSPACE,
        run_key_row(auto.group_id, auto.account.unwrap(), &idem_key),
        key_rec.to_bytes().unwrap(),
    )
    .await;
    for (ks, k, v) in new_firing_rows(&stranded_run).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let rec_rep = drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    assert!(rec_rep.wake_runs.contains(&recovered_child_id));
    assert!(
        rec_rep
            .cancel_acks
            .iter()
            .any(|a| a.firing_id == stranded_run.firing_id && a.run_id == recovered_child_id)
    );
    drive(AckCancelsOperation::new(rec_rep.cancel_acks), &env.ctx)
        .await
        .unwrap();
    let (rec_f, _) = drive(
        GetFiringOperation::new(&env.owner, stranded_run.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        rec_f.stages[0].receipt.workflow_run_id,
        Some(recovered_child_id)
    );
    assert!(rec_f.stages[0].receipt.cancel_sent);

    // Restart recovery reconciles missing parent_job_id on queued runs (both missing and deduped jobs).
    let mut cfg = RealmConfigDocument::default_for_realm(env.realm, Vec::new());
    cfg.seed_default_placement();
    cfg.seed_job_control(env.node, 0);
    let on = FeatureMode::Scoped {
        default: true,
        groups: BTreeMap::new(),
    };
    cfg.features.insert(Feature::Automations, on.clone());
    cfg.features.insert(Feature::Applets, on);
    env.write_row(
        REALM_CONFIG_KEYSPACE,
        env.realm.as_bytes().to_vec(),
        cfg.to_bytes(&env.actor()).unwrap(),
    )
    .await;
    let acct = auto.account.unwrap();
    // Case A: Queued run committed before SubmitLocalOperation -> recovery submits job and sets parent_job_id.
    let no_job_id = Ulid::from_bytes([123; 16]);
    let mut no_job_run = env.sample_run(no_job_id, wf_id, RunState::Queued, None);
    no_job_run.created_by = acct;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(no_job_id),
        no_job_run.to_bytes().unwrap(),
    )
    .await;
    let mut stranded_q = Firing::new(
        Ulid::from_bytes([124; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        17,
    );
    stranded_q.stages[0].state = StageState::Running;
    let q_idem = run_idempotency_key(stranded_q.firing_id, "run", stranded_q.attempt);
    env.write_row(
        RUN_IDEMPOTENCY_KEYSPACE,
        run_key_row(auto.group_id, acct, &q_idem),
        RunKeyRecord {
            run_id: no_job_id,
            request_digest: [0; 32],
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
    // Case B: Queued run whose workflow-run/{id} job exists -> recovery reuses that exact job_id.
    let dedup_run_id = Ulid::from_bytes([125; 16]);
    let mut dedup_run = env.sample_run(dedup_run_id, wf_id, RunState::Queued, None);
    dedup_run.created_by = acct;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(dedup_run_id),
        dedup_run.to_bytes().unwrap(),
    )
    .await;
    let dedup_spec = SubmitJobSpec {
        payload: JobPayload::WorkflowRun(Box::new(WorkflowRunSpec {
            run_id: dedup_run_id,
            group_id: env.group,
            created_by: acct,
            realm_id: env.realm,
        })),
        created_by: acct,
        owner_node_id: env.node,
        dedup_key: Some(user_dedup_key(
            acct,
            &format!("workflow-run/{dedup_run_id}"),
        )),
        now_ms: 17,
        retention_ms: RETENTION_MS,
        workspace_mode: WorkspaceMode::None,
        workspace_bucket: None,
        active_cap: None,
    };
    let existing_job = drive(
        SubmitLocalOperation::new(dedup_spec, env.realm, Some((env.node, env.realm))),
        &env.ctx,
    )
    .await
    .unwrap();
    let mut stranded_d = Firing::new(
        Ulid::from_bytes([126; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        18,
    );
    stranded_d.stages[0].state = StageState::Running;
    let d_idem = run_idempotency_key(stranded_d.firing_id, "run", stranded_d.attempt);
    env.write_row(
        RUN_IDEMPOTENCY_KEYSPACE,
        run_key_row(auto.group_id, acct, &d_idem),
        RunKeyRecord {
            run_id: dedup_run_id,
            request_digest: [0; 32],
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
    // Case C: Cancelling firing with never-submitted Queued run -> marks run Cancelled & ends firing.
    let cancel_q_id = Ulid::from_bytes([127; 16]);
    let mut cancel_q_run = env.sample_run(cancel_q_id, wf_id, RunState::Queued, None);
    cancel_q_run.created_by = acct;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(cancel_q_id),
        cancel_q_run.to_bytes().unwrap(),
    )
    .await;
    let mut stranded_cq = Firing::new(
        Ulid::from_bytes([128; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        19,
    );
    stranded_cq.state = FiringState::Cancelling;
    stranded_cq.stages[0].state = StageState::Cancelling;
    let cq_idem = run_idempotency_key(stranded_cq.firing_id, "run", stranded_cq.attempt);
    env.write_row(
        RUN_IDEMPOTENCY_KEYSPACE,
        run_key_row(auto.group_id, acct, &cq_idem),
        RunKeyRecord {
            run_id: cancel_q_id,
            request_digest: [0; 32],
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
    for (ks, k, v) in new_firing_rows(&stranded_q)
        .unwrap()
        .into_iter()
        .chain(new_firing_rows(&stranded_d).unwrap())
        .chain(new_firing_rows(&stranded_cq).unwrap())
    {
        env.write_row(&ks, k, v).await;
    }
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let rec_a = WorkflowRunRecord::from_bytes(
        &env.read_row(WORKFLOW_RUN_KEYSPACE, &run_key(no_job_id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(rec_a.parent_job_id.is_some());
    let rec_b = WorkflowRunRecord::from_bytes(
        &env.read_row(WORKFLOW_RUN_KEYSPACE, &run_key(dedup_run_id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rec_b.parent_job_id, Some(existing_job.job_id));
    let (cq_done, _) = drive(
        GetFiringOperation::new(&env.owner, stranded_cq.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(cq_done.state, FiringState::Cancelled);
    let rec_c = WorkflowRunRecord::from_bytes(
        &env.read_row(WORKFLOW_RUN_KEYSPACE, &run_key(cancel_q_id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(rec_c.state, RunState::Cancelled);

    // Multi-child API cancel defers cancel_sent until caller delivers signals and runs AckCancels.
    let two_stage_spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        stages: vec![
            spec.stages[0].clone(),
            Stage {
                name: "run2".into(),
                ..spec.stages[0].clone()
            },
        ],
        scope: spec.scope.clone(),
    };
    let two_auto = env.create_active(two_stage_spec.clone()).await;
    let (c1, c2) = (Ulid::from_bytes([129; 16]), Ulid::from_bytes([130; 16]));
    for cid in [c1, c2] {
        let rec = env.sample_run(cid, wf_id, RunState::Running, None);
        env.write_row(WORKFLOW_RUN_KEYSPACE, run_key(cid), rec.to_bytes().unwrap())
            .await;
    }
    let mut two_f = Firing::new(
        Ulid::from_bytes([131; 16]),
        two_auto.automation_id,
        two_auto.group_id,
        two_auto.revision,
        &two_stage_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        20,
    );
    two_f.stages[0].state = StageState::Running;
    two_f.stages[0].receipt.workflow_run_id = Some(c1);
    two_f.stages[1].state = StageState::Running;
    two_f.stages[1].receipt.workflow_run_id = Some(c2);
    for (ks, k, v) in new_firing_rows(&two_f).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let multi = drive(
        CancelFiringOperation::new(&env.owner, two_f.firing_id, (env.node, env.realm)),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(multi.woken_runs.len(), 2);
    assert_eq!(multi.pending_acks.len(), 2);
    let (before_ack, _) = drive(
        GetFiringOperation::new(&env.owner, two_f.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(!before_ack.stages[0].receipt.cancel_sent);
    assert!(!before_ack.stages[1].receipt.cancel_sent);
    let acked = drive(AckCancelsOperation::new(multi.pending_acks), &env.ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(acked.stages[0].receipt.cancel_sent);
    assert!(acked.stages[1].receipt.cancel_sent);

    // Stranded claimed Notify recovers its settled intent instead of stalling (#1).
    let notify_spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        stages: vec![Stage {
            name: "ping".into(),
            needs: Some(vec![]),
            when: When::Passed,
            kind: StageKind::Notify {
                audience: Audience::GroupAdmins,
                message: "done".into(),
            },
        }],
        scope: Scope {
            audiences: vec![Audience::GroupAdmins],
            ..Scope::default()
        },
    };
    let notify_auto = env.create_active(notify_spec.clone()).await;
    let mut stranded_notify = Firing::new(
        Ulid::from_bytes([120; 16]),
        notify_auto.automation_id,
        notify_auto.group_id,
        notify_auto.revision,
        &notify_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        15,
    );
    stranded_notify.stages[0].state = StageState::Running;
    let n_stage_id = stage_intent_id(stranded_notify.firing_id, "ping");
    let n_intent = call_intent(0, &[b"group_admins", b"done"]);
    let (_, nk, nv) = settled_row_in(
        AUTOMATION_INTENTS_KEYSPACE,
        n_stage_id,
        &n_intent,
        "3".into(),
    )
    .unwrap();
    env.write_row(AUTOMATION_INTENTS_KEYSPACE, nk, nv).await;
    for (ks, k, v) in new_firing_rows(&stranded_notify).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (done_notify, _) = drive(
        GetFiringOperation::new(&env.owner, stranded_notify.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(done_notify.state, FiringState::Succeeded);
    assert_eq!(done_notify.stages[0].receipt.notified, Some(3));
    assert!(
        env.read_row(
            AUTOMATION_RUNS_KEYSPACE,
            &active_key(stranded_notify.firing_id)
        )
        .await
        .is_none()
    );
    // Stranded Pending intent on a Cancelling notify stage turns Unknown and finishes Cancelled.
    let mut stranded_cancel = Firing::new(
        Ulid::from_bytes([121; 16]),
        notify_auto.automation_id,
        notify_auto.group_id,
        notify_auto.revision,
        &notify_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        16,
    );
    stranded_cancel.state = FiringState::Cancelling;
    stranded_cancel.stages[0].state = StageState::Cancelling;
    let c_stage_id = stage_intent_id(stranded_cancel.firing_id, "ping");
    let pend_rec = IntentRecord {
        digest: n_intent.digest,
        state: IntentState::Pending,
    };
    env.write_row(
        AUTOMATION_INTENTS_KEYSPACE,
        intent_key(c_stage_id, 0),
        postcard::to_allocvec(&pend_rec).unwrap(),
    )
    .await;
    for (ks, k, v) in new_firing_rows(&stranded_cancel).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (fin_cancel, _) = drive(
        GetFiringOperation::new(&env.owner, stranded_cancel.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(fin_cancel.state, FiringState::Cancelled);
    let unk_after: IntentRecord = postcard::from_bytes(
        &env.read_row(AUTOMATION_INTENTS_KEYSPACE, &intent_key(c_stage_id, 0))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(unk_after.state, IntentState::Unknown { .. }));

    // Missing child workflow row moves Running stage to NeedsAttention instead of stalling.
    let mut f4 = Firing::new(
        Ulid::from_bytes([113; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        13,
    );
    f4.stages[0].state = StageState::Running;
    f4.stages[0].receipt.workflow_run_id = Some(Ulid::from_bytes([114; 16]));
    for (ks, k, v) in new_firing_rows(&f4).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    drive(
        StepFiringsOperation::new((env.node, env.realm))
            .with_disabled_features(BTreeSet::from([Feature::Workflows])),
        &env.ctx,
    )
    .await
    .unwrap();
    let (missing, _) = drive(
        GetFiringOperation::new(&env.owner, f4.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(missing.state, FiringState::NeedsAttention);
    assert_eq!(missing.stages[0].state, StageState::NeedsAttention);
}
