//! Tests JSON template escaping, fail-closed outside inputs, slot caps and cancel/missing recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::applet::record::{Access, Applet, applet_key};
use aruna_core::automation::firing::{Firing, FiringState, StageState, TriggerPayload};
use aruna_core::automation::record::{Spec, Stage, StageKind, Trigger, When};
use aruna_core::automation::scope::Scope;
use aruna_core::calls::{ChildOutcome, IoResult};
use aruna_core::keyspaces::{APPLETS_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{RunState, run_key};
use aruna_core::structs::identity::features::Feature;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::AutoError;
use super::control::CancelFiringOperation;
use super::firing_reads::GetFiringOperation;
use super::flow_tests::Env;
use super::run_stage::{StartStageRun, render_json};
use super::runner::StepFiringsOperation;
use super::start::new_firing_rows;
use crate::applets::AppletError;

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
    assert!(StartStageRun::new(auto.clone(), qf.clone(), 0, (env.node, env.realm)).is_ok());
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
    assert!(!cancelling.stages[0].receipt.cancel_sent);
    assert!(cancelling.stages[0].error.is_some());
    remote_child.node_id = env.node;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(remote_child_id),
        remote_child.to_bytes().unwrap(),
    )
    .await;
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (retried, _) = drive(
        GetFiringOperation::new(&env.owner, f3.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(retried.stages[0].receipt.cancel_sent);

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
