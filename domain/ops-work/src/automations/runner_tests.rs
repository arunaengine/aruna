//! Tests DAG stage execution, approval gates, edge cases and upkeep pruning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::automation::firing::{Firing, FiringState, StageState, TriggerPayload};
use aruna_core::automation::keys::{active_key, kept_key};
use aruna_core::automation::record::{KeptEvent, Spec, Stage, StageKind, Trigger, When};
use aruna_core::automation::scope::{Audience, Scope};
use aruna_core::keyspaces::{
    AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE, NOTIFICATION_OUTBOX_KEYSPACE,
    WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::execution::workflow_run::{RunState, run_key};
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::AutoError;
use super::control::{
    CancelFiringOperation, DecideStageOperation, ResolveStageOperation, ResumeFiringOperation,
};
use super::firing_reads::GetFiringOperation;
use super::flow_tests::Env;
use super::runner::StepFiringsOperation;
use super::start::{ManualStart, StartManualOperation, new_firing_rows};
use super::upkeep::AutoUpkeepOperation;

#[tokio::test]
async fn stages_and_edges() {
    let env = Env::new().await;
    let wf_id = Ulid::from_bytes([61; 16]);
    let spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![
            Stage {
                name: "gate".into(),
                needs: Some(vec![]),
                when: When::Passed,
                kind: StageKind::Approval {
                    message: "approve".into(),
                    timeout_ms: None,
                },
            },
            Stage {
                name: "alert".into(),
                needs: Some(vec!["gate".into()]),
                when: When::Passed,
                kind: StageKind::Notify {
                    audience: Audience::GroupAdmins,
                    message: "approved".into(),
                },
            },
        ],
        scope: Scope {
            audiences: vec![Audience::GroupAdmins],
            ..Scope::default()
        },
    };
    let auto = env.create_active(spec).await;
    let ManualStart::Started(started) = drive(
        StartManualOperation::new(&env.owner, auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await
    .unwrap() else {
        panic!("expected started firing");
    };
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (waiting, _) = drive(
        GetFiringOperation::new(&env.owner, started.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(waiting.stages[0].state, StageState::WaitingForApproval);

    // Non-admin cannot approve a stage; human admin approves and notify writes outbox.
    let deny = DecideStageOperation::new(
        &env.member,
        started.firing_id,
        "gate".into(),
        true,
        None,
        env.node,
    );
    assert_eq!(drive(deny, &env.ctx).await, Err(AutoError::Forbidden));
    let ok = DecideStageOperation::new(
        &env.owner,
        started.firing_id,
        "gate".into(),
        true,
        None,
        env.node,
    );
    drive(ok, &env.ctx).await.unwrap();
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (done, _) = drive(
        GetFiringOperation::new(&env.owner, started.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(done.state, FiringState::Succeeded);
    assert_eq!(done.stages[1].receipt.notified, Some(1));
    assert!(!env.scan_rows(NOTIFICATION_OUTBOX_KEYSPACE).await.is_empty());

    // Edge cases 1 & 2: NeedsAttention refuses resume until audited resolve; resume keeps receipts.
    let mut branch = env.base_spec();
    branch.stages = vec![
        Stage {
            name: "work".into(),
            needs: Some(vec![]),
            when: When::Passed,
            kind: StageKind::Notify {
                audience: Audience::GroupAdmins,
                message: "ok".into(),
            },
        },
        Stage {
            name: "quarantine".into(),
            needs: Some(vec!["work".into()]),
            when: When::Failed,
            kind: StageKind::Notify {
                audience: Audience::GroupAdmins,
                message: "bad".into(),
            },
        },
    ];
    let branch_auto = env.create_active(branch).await;
    let mut firing = Firing::new(
        Ulid::from_bytes([95; 16]),
        branch_auto.automation_id,
        branch_auto.group_id,
        branch_auto.revision,
        &branch_auto.spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        20,
    );
    firing.stages[0].state = StageState::NeedsAttention;
    firing.stages[1].state = StageState::Succeeded;
    firing.stages[1].receipt.notified = Some(3);
    firing.state = FiringState::NeedsAttention;
    for (ks, k, v) in new_firing_rows(&firing).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    assert!(matches!(
        drive(
            ResumeFiringOperation::new(&env.owner, firing.firing_id, env.node),
            &env.ctx,
        )
        .await,
        Err(AutoError::Conflict(_))
    ));
    let resolved = drive(
        ResolveStageOperation::new(
            &env.owner,
            firing.firing_id,
            "work".into(),
            false,
            Some("retry".into()),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        (resolved.resolutions.len(), resolved.state),
        (1, FiringState::Failed)
    );
    let resumed = drive(
        ResumeFiringOperation::new(&env.owner, firing.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(resumed.stages[1].receipt.notified, Some(3));
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (repaired, _) = drive(
        GetFiringOperation::new(&env.owner, firing.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(repaired.state, FiringState::Succeeded);
    assert_eq!(repaired.stages[1].receipt.notified, Some(3));

    // Edge case 4: Async Cancelling propagates to child run and finalizes when it terminates.
    let child_id = Ulid::from_bytes([96; 16]);
    let mut child = env.sample_run(child_id, wf_id, RunState::Running, None);
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(child_id),
        child.to_bytes().unwrap(),
    )
    .await;
    let cancel_spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![Stage {
            name: "run".into(),
            needs: Some(vec![]),
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id: wf_id,
                version: 1,
                values_json: "{}".into(),
                inputs_json: None,
                output_bucket: None,
                output_prefix: None,
                placement_site: None,
                secrets: BTreeMap::new(),
                provenance_crate: false,
            },
        }],
        scope: Scope {
            workflows: vec![(wf_id, 1)],
            ..Scope::default()
        },
    };
    let cancel_auto = env.create_active(cancel_spec).await;
    let mut cf = Firing::new(
        Ulid::from_bytes([97; 16]),
        cancel_auto.automation_id,
        cancel_auto.group_id,
        cancel_auto.revision,
        &cancel_auto.spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        30,
    );
    cf.stages[0].state = StageState::Running;
    cf.stages[0].receipt.workflow_run_id = Some(child_id);
    for (ks, k, v) in new_firing_rows(&cf).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let cancelling = drive(
        CancelFiringOperation::new(&env.owner, cf.firing_id, (env.node, env.realm)),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(cancelling.firing.state, FiringState::Cancelling);
    assert_eq!(cancelling.woken_runs, vec![child_id]);
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &active_key(cf.firing_id))
            .await
            .is_some()
    );
    child.state = RunState::Cancelled;
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(child_id),
        child.to_bytes().unwrap(),
    )
    .await;
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (final_f, _) = drive(
        GetFiringOperation::new(&env.owner, cf.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(final_f.state, FiringState::Cancelled);
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &active_key(cf.firing_id))
            .await
            .is_none()
    );

    // Upkeep prunes expired kept events.
    let old_kept = KeptEvent {
        event_id: Ulid::from_bytes([98; 16]),
        automation_id: auto.automation_id,
        event: SourceEvent::data_uploaded(
            (env.realm, env.group, env.node),
            ("raw", "in/old.csv"),
            Ulid::from_bytes([99; 16]),
            None,
            1,
        ),
        reason: "paused".into(),
        kept_at_ms: 1,
        payload: TriggerPayload::DataUploaded {
            bucket: "raw".into(),
            key: "in/old.csv".into(),
            version_id: Ulid::from_bytes([99; 16]),
            path: "/r/g/raw/in/old.csv".into(),
            size_bytes: Some(10),
        },
    };
    env.write_row(
        AUTOMATIONS_KEYSPACE,
        kept_key(auto.automation_id, old_kept.event_id),
        old_kept.to_bytes().unwrap(),
    )
    .await;
    let far_future = 100 * 86_400_000u64;
    let upkeep = drive(
        AutoUpkeepOperation::new((env.node, env.realm), far_future),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(upkeep.pruned_kept >= 1);
}

#[tokio::test]
async fn pinned_spec_cancel() {
    use super::change::AutoChange;
    use super::control::{FiringDelta, FiringTxOp};
    use super::copy_stage::StageOutcome;
    use super::lifecycle::ChangeAutoOperation;
    use aruna_core::automation::firing::CopiedFile;

    let env = Env::new().await;
    let spec_v1 = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![
            Stage {
                name: "gate".into(),
                needs: Some(vec![]),
                when: When::Passed,
                kind: StageKind::Approval {
                    message: "approve".into(),
                    timeout_ms: Some(100),
                },
            },
            Stage {
                name: "alert".into(),
                needs: Some(vec!["gate".into()]),
                when: When::Passed,
                kind: StageKind::Notify {
                    audience: Audience::GroupAdmins,
                    message: "done".into(),
                },
            },
        ],
        scope: Scope {
            audiences: vec![Audience::GroupAdmins],
            ..Scope::default()
        },
    };
    let auto = env.create_active(spec_v1.clone()).await;
    let ManualStart::Started(started) = drive(
        StartManualOperation::new(&env.owner, auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await
    .unwrap() else {
        panic!("expected started firing");
    };
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();

    // Mutate parent automation to a reordered 3-stage spec and approve it.
    let mut spec_v2 = spec_v1.clone();
    spec_v2.stages.reverse();
    spec_v2.stages.push(Stage {
        name: "extra".into(),
        needs: Some(vec![]),
        when: When::Passed,
        kind: StageKind::Notify {
            audience: Audience::GroupAdmins,
            message: "extra".into(),
        },
    });
    let staged = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, Some(auto.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(spec_v2),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, Some(staged.revision)),
            AutoChange::Approve,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();

    // Expired approval is refused inside DecideStageOperation even before worker ticks.
    let mut expired = Firing::new(
        Ulid::from_bytes([101; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &spec_v1,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        1,
    );
    expired.stages[0].state = StageState::WaitingForApproval;
    expired.stages[0].receipt.expires_at_ms = Some(1);
    expired.state = FiringState::WaitingForApproval;
    for (ks, k, v) in new_firing_rows(&expired).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    let late = drive(
        DecideStageOperation::new(
            &env.owner,
            expired.firing_id,
            "gate".into(),
            true,
            None,
            env.node,
        ),
        &env.ctx,
    )
    .await;
    assert!(matches!(late, Err(AutoError::Conflict(_))), "{late:?}");

    // Existing firing still finishes cleanly against its pinned 2-stage spec.
    drive(
        DecideStageOperation::new(
            &env.owner,
            started.firing_id,
            "gate".into(),
            true,
            None,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (done, _) = drive(
        GetFiringOperation::new(&env.owner, started.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(done.state, FiringState::Succeeded);
    assert_eq!(done.stages.len(), 2);

    // Concurrent cancel during copy preserves committed file receipt and ends Cancelled.
    let copy_spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![Stage {
            name: "copy".into(),
            needs: Some(vec![]),
            when: When::Passed,
            kind: StageKind::Copy {
                from_stage: "run".into(),
                file: None,
                dest_bucket: "out".into(),
                dest_template: "d/{filename}".into(),
                allow_downgrade: false,
            },
        }],
        scope: Scope::default(),
    };
    let mut cf = Firing::new(
        Ulid::from_bytes([102; 16]),
        auto.automation_id,
        auto.group_id,
        auto.revision,
        &copy_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        10,
    );
    let file = |k: &str, v: u64| CopiedFile {
        source_key: k.into(),
        source_version: Ulid::from_parts(v, 1),
        dest_bucket: "out".into(),
        dest_key: format!("d/{k}"),
        dest_version: Ulid::from_parts(v, 2),
        bytes: 16,
    };
    cf.stages[0].state = StageState::Running;
    cf.stages[0].receipt.copied.push(file("a.dat", 1));
    for (ks, k, v) in new_firing_rows(&cf).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    drive(
        CancelFiringOperation::new(&env.owner, cf.firing_id, (env.node, env.realm)),
        &env.ctx,
    )
    .await
    .unwrap();
    let after = drive(
        FiringTxOp::new(
            auto.automation_id,
            cf.firing_id,
            FiringDelta::StageOutcome {
                idx: 0,
                outcome: StageOutcome::Progress(file("b.dat", 2)),
            },
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(after.firing.state, FiringState::Cancelled);
    assert_eq!(after.firing.stages[0].state, StageState::Cancelled);
    assert_eq!(after.firing.stages[0].receipt.copied.len(), 2);
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &active_key(cf.firing_id))
            .await
            .is_none()
    );

    // A cancelling firing whose last child succeeds stays Cancelled across re-evaluation (#6).
    let wf_id = Ulid::from_bytes([66; 16]);
    let run_spec = Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![Stage {
            name: "run".into(),
            needs: Some(vec![]),
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id: wf_id,
                version: 1,
                values_json: "{}".into(),
                inputs_json: None,
                output_bucket: None,
                output_prefix: None,
                placement_site: None,
                secrets: BTreeMap::new(),
                provenance_crate: false,
            },
        }],
        scope: Scope {
            workflows: vec![(wf_id, 1)],
            ..Scope::default()
        },
    };
    let run_auto = env.create_active(run_spec.clone()).await;
    let race_child_id = Ulid::from_bytes([103; 16]);
    let mut race_child = env.sample_run(race_child_id, wf_id, RunState::Succeeded, None);
    race_child.finished_at_ms = Some(50);
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(race_child_id),
        race_child.to_bytes().unwrap(),
    )
    .await;
    let mut rf = Firing::new(
        Ulid::from_bytes([104; 16]),
        run_auto.automation_id,
        run_auto.group_id,
        run_auto.revision,
        &run_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        20,
    );
    rf.state = FiringState::Cancelling;
    rf.stages[0].state = StageState::Cancelling;
    rf.stages[0].receipt.workflow_run_id = Some(race_child_id);
    rf.stages[0].receipt.cancel_sent = true;
    for (ks, k, v) in new_firing_rows(&rf).unwrap() {
        env.write_row(&ks, k, v).await;
    }
    drive(StepFiringsOperation::new((env.node, env.realm)), &env.ctx)
        .await
        .unwrap();
    let (raced, _) = drive(
        GetFiringOperation::new(&env.owner, rf.firing_id, env.node),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(raced.state, FiringState::Cancelled);
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &active_key(rf.firing_id))
            .await
            .is_none()
    );

    // Resuming a failed firing respects max_active_firings (#7).
    let mut cap_spec = spec_v1.clone();
    cap_spec.scope.limits.max_active_firings = Some(1);
    let cap_auto = env.create_active(cap_spec.clone()).await;
    let mut failed_f = Firing::new(
        Ulid::from_bytes([105; 16]),
        cap_auto.automation_id,
        cap_auto.group_id,
        cap_auto.revision,
        &cap_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        30,
    );
    failed_f.stages[0].state = StageState::Failed;
    failed_f.evaluate(31);
    assert_eq!(failed_f.state, FiringState::Failed);
    let active_f = Firing::new(
        Ulid::from_bytes([106; 16]),
        cap_auto.automation_id,
        cap_auto.group_id,
        cap_auto.revision,
        &cap_spec,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: env.owner.user_id,
        },
        32,
    );
    for (ks, k, v) in new_firing_rows(&failed_f)
        .unwrap()
        .into_iter()
        .chain(new_firing_rows(&active_f).unwrap())
    {
        env.write_row(&ks, k, v).await;
    }
    let capped_resume = drive(
        ResumeFiringOperation::new(&env.owner, failed_f.firing_id, env.node),
        &env.ctx,
    )
    .await;
    assert!(
        matches!(capped_resume, Err(AutoError::Limited(_))),
        "{capped_resume:?}"
    );
    assert!(
        env.read_row(AUTOMATION_RUNS_KEYSPACE, &active_key(failed_f.firing_id))
            .await
            .is_none()
    );
}
