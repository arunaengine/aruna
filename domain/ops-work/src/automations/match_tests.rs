//! Tests automation journal matching, self-loop suppression, kept-event replay and chain depth.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{SubmissionReceipt, submission_key};
use aruna_core::automation::firing::TriggerPayload;
use aruna_core::automation::record::{Lifecycle, PauseReason, Trigger};
use aruna_core::automation::scope::Limits;
use aruna_core::keyspaces::{APPLETS_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::execution::workflow_run::{AutomationOrigin, RunState, run_key};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::firing_reads::ListEventsOperation;
use super::flow_tests::Env;
use super::matcher::MatchEventsOperation;
use super::reads::LoadAutoOperation;
use super::start::StartEventsOperation;

#[tokio::test]
async fn matcher_and_replay() {
    let env = Env::new().await;
    let applet_id = Ulid::from_bytes([50; 16]);
    let wf_id = Ulid::from_bytes([60; 16]);
    let mut spec = env.base_spec();
    spec.triggers = vec![
        Trigger::DataUploaded {
            bucket: "raw".into(),
            prefix: "in/".into(),
            pattern: Some("*.csv".into()),
        },
        Trigger::AppletSubmission {
            applet_id: Some(applet_id),
        },
        Trigger::RunFinished {
            workflow_id: wf_id,
            status: None,
            applet_id: None,
        },
    ];
    spec.scope.limits = Limits {
        max_chain_depth: Some(1),
        ..Limits::default()
    };
    let auto = env.create_active(spec).await;

    // Self-upload is skipped; matching user upload fires once; cursor prevents duplicates.
    let loc = (env.realm, env.group, env.node);
    let own = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/a.csv"),
        Ulid::from_bytes([71; 16]),
        auto.account,
        10,
    );
    let ext = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/b.csv"),
        Ulid::from_bytes([72; 16]),
        Some(env.owner.user_id),
        11,
    );
    env.journal(&own).await;
    env.journal(&ext).await;
    let rep = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((rep.scanned, rep.fired, rep.kept), (2, 1, 0));
    let again = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((again.scanned, again.fired), (0, 0));

    // Applet submission while applets switch is off gets kept, then replayed once enabled.
    env.set_features(true, false).await;
    let sub_id = Ulid::from_bytes([80; 16]);
    let receipt = SubmissionReceipt {
        applet_id,
        version_id: sub_id,
        pseudonym: "vis-1".into(),
        guest: true,
        outside_input: true,
        bucket: "uploads".into(),
        key: "in/vis-1/sub.json".into(),
        files: Vec::new(),
        created_at_ms: 12,
    };
    env.write_row(
        APPLETS_KEYSPACE,
        submission_key(applet_id, sub_id),
        receipt.to_bytes().unwrap(),
    )
    .await;
    let path = group_permission_path(env.realm, env.group, env.node);
    let sub_ev =
        SourceEvent::applet_submission(env.group, path.clone(), (applet_id, sub_id), None, 12);
    env.journal(&sub_ev).await;
    let rep2 = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((rep2.fired, rep2.kept), (0, 1));
    let kept = drive(
        ListEventsOperation::new(&env.owner, auto.automation_id, env.node, 10),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].reason, "applets_disabled");
    env.set_features(true, true).await;
    let replayed = drive(
        StartEventsOperation::new(
            &env.owner,
            auto.automation_id,
            env.node,
            vec![kept[0].event_id],
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(replayed.len(), 1);
    assert!(matches!(
        &replayed[0].trigger,
        TriggerPayload::AppletSubmission { pseudonym, .. } if pseudonym == "vis-1"
    ));

    // Self-origin RunFinished is skipped; cross-automation chain-depth overflow auto-pauses.
    let self_run_id = Ulid::from_bytes([89; 16]);
    let self_origin = AutomationOrigin {
        automation_id: auto.automation_id,
        firing_id: Ulid::from_bytes([91; 16]),
        stage: "run".into(),
        applet_id: None,
        outside_input: false,
        depth: 0,
    };
    let self_run = env.sample_run(self_run_id, wf_id, RunState::Succeeded, Some(self_origin));
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(self_run_id),
        self_run.to_bytes().unwrap(),
    )
    .await;
    let self_fin = SourceEvent::run_finished(
        (env.group, path.clone()),
        (wf_id, self_run_id),
        "succeeded",
        env.owner.user_id,
        13,
    );
    env.journal(&self_fin).await;
    let rep_self = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((rep_self.fired, rep_self.paused), (0, 0));

    let run_id = Ulid::from_bytes([90; 16]);
    let origin = AutomationOrigin {
        automation_id: Ulid::from_bytes([92; 16]),
        firing_id: Ulid::from_bytes([93; 16]),
        stage: "run".into(),
        applet_id: None,
        outside_input: false,
        depth: 1,
    };
    let run = env.sample_run(run_id, wf_id, RunState::Succeeded, Some(origin));
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(run_id),
        run.to_bytes().unwrap(),
    )
    .await;
    let fin = SourceEvent::run_finished(
        (env.group, path),
        (wf_id, run_id),
        "succeeded",
        env.owner.user_id,
        14,
    );
    env.journal(&fin).await;
    let rep3 = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!(rep3.paused, 1);
    let paused = drive(LoadAutoOperation::new(auto.automation_id), &env.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paused.lifecycle, Lifecycle::Paused);
    assert_eq!(paused.pause_reason, Some(PauseReason::ChainDepth));
}

#[tokio::test]
async fn out_of_order() {
    use aruna_core::structs::execution::workflow_run::AppletOrigin;
    use aruna_core::structs::identity::features::Feature;
    use std::collections::BTreeSet;

    let env = Env::new().await;
    let applet_id = Ulid::from_bytes([51; 16]);
    let wf_id = Ulid::from_bytes([61; 16]);
    let mut spec = env.base_spec();
    spec.triggers = vec![
        Trigger::DataUploaded {
            bucket: "raw".into(),
            prefix: "in/".into(),
            pattern: Some("*.csv".into()),
        },
        Trigger::RunFinished {
            workflow_id: wf_id,
            status: Some(aruna_core::automation::record::RunStatus::Succeeded),
            applet_id: Some(applet_id),
        },
    ];
    let auto = env.create_active(spec).await;
    let loc = (env.realm, env.group, env.node);

    // Later-stamped event commits and is matched first; slower earlier event still matches.
    let later = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/late.csv"),
        Ulid::from_bytes([73; 16]),
        Some(env.owner.user_id),
        200,
    );
    env.journal(&later).await;
    let rep1 = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((rep1.scanned, rep1.fired), (1, 1));

    let earlier = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/early.csv"),
        Ulid::from_bytes([74; 16]),
        Some(env.owner.user_id),
        100,
    );
    env.journal(&earlier).await;
    let rep2 = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((rep2.scanned, rep2.fired), (1, 1));

    // Node-disabled Automations keeps events even when realm enables Automations.
    let blocked = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/disabled.csv"),
        Ulid::from_bytes([75; 16]),
        Some(env.owner.user_id),
        300,
    );
    env.journal(&blocked).await;
    let disabled = BTreeSet::from([Feature::Automations]);
    let rep3 = drive(
        MatchEventsOperation::new(env.realm).with_disabled_features(disabled),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!((rep3.fired, rep3.kept), (0, 1));

    // Duplicate IDs and repeat replay requests resolve idempotently to the same firing.
    let kept = drive(
        ListEventsOperation::new(&env.owner, auto.automation_id, env.node, 10),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(kept.len(), 1);
    let eid = kept[0].event_id;
    let first = drive(
        StartEventsOperation::new(&env.owner, auto.automation_id, env.node, vec![eid, eid]),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(first.len(), 1);
    let again = drive(
        StartEventsOperation::new(&env.owner, auto.automation_id, env.node, vec![eid]),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].firing_id, first[0].firing_id);

    // Retry of an already-replayed event succeeds even after the automation is paused (#12).
    use super::change::AutoChange;
    use super::lifecycle::ChangeAutoOperation;
    use aruna_core::automation::record::PauseReason;

    drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, None),
            AutoChange::Pause(PauseReason::Owner),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let paused_retry = drive(
        StartEventsOperation::new(&env.owner, auto.automation_id, env.node, vec![eid]),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(paused_retry.len(), 1);
    assert_eq!(paused_retry[0].firing_id, first[0].firing_id);
    drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, None),
            AutoChange::Resume,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();

    // Journal admission rejects events whose trigger was removed in a newer revision (#8).
    let mut edited_spec = auto.spec.clone();
    edited_spec
        .triggers
        .retain(|t| !matches!(t, Trigger::DataUploaded { .. }));
    let edited = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, None),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(edited_spec),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(edited.revision > auto.revision);
    let stale = drive(
        super::admit::AdmitFiringOp::journal(
            auto.automation_id,
            Ulid::from_bytes([87; 16]),
            "data_uploaded/stale".into(),
            auto.revision,
            Some(env.owner.user_id),
            TriggerPayload::DataUploaded {
                bucket: "raw".into(),
                key: "in/late.csv".into(),
                version_id: Ulid::from_bytes([86; 16]),
                path: "/p".into(),
                size_bytes: 10,
            },
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(stale, super::admit::AdmitOutcome::Unmatched);

    // Lowering max_chain_depth in a newer revision returns ChainDepth (not Unmatched) and pauses.
    let mut lowered_spec = edited.spec.clone();
    lowered_spec.scope.limits.max_chain_depth = Some(1);
    let lowered = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, Some(edited.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(lowered_spec),
                workflow_id: None,
            },
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(lowered.revision > edited.revision);
    let depth_hit = drive(
        super::admit::AdmitFiringOp::journal(
            auto.automation_id,
            Ulid::from_bytes([85; 16]),
            "run_finished/depth-hit".into(),
            edited.revision,
            Some(env.owner.user_id),
            TriggerPayload::RunFinished {
                workflow_id: wf_id,
                run_id: Ulid::from_bytes([84; 16]),
                status: aruna_core::automation::record::RunStatus::Succeeded,
                outputs: Vec::new(),
                applet_id: Some(applet_id),
                outside_input: false,
                depth: 1,
            },
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(depth_hit, super::admit::AdmitOutcome::ChainDepth);
    // Restore higher chain depth for the direct applet run_finished check below.
    let mut restored_spec = lowered.spec.clone();
    restored_spec.scope.limits.max_chain_depth = Some(3);
    let staged_up = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto.automation_id, Some(lowered.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(restored_spec),
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
            (auto.automation_id, Some(staged_up.revision)),
            AutoChange::Approve,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();

    // Direct applet workflow run matches applet-filtered run_finished and propagates outside_input.
    let direct_id = Ulid::from_bytes([88; 16]);
    let mut direct_run = env.sample_run(direct_id, wf_id, RunState::Succeeded, None);
    direct_run.applet = Some(AppletOrigin {
        applet_id,
        outside_input: true,
    });
    env.write_row(
        WORKFLOW_RUN_KEYSPACE,
        run_key(direct_id),
        direct_run.to_bytes().unwrap(),
    )
    .await;
    let path = group_permission_path(env.realm, env.group, env.node);
    let fin = SourceEvent::run_finished(
        (env.group, path),
        (wf_id, direct_id),
        "succeeded",
        env.owner.user_id,
        400,
    );
    env.journal(&fin).await;
    let rep4 = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!(rep4.fired, 1);
    let firings = drive(
        super::firing_reads::ListFiringsOperation::new(
            &env.owner,
            (None, Some(auto.automation_id)),
            None,
            env.node,
            None,
            10,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(firings.items.iter().any(|f| matches!(
        &f.trigger,
        TriggerPayload::RunFinished {
            run_id,
            applet_id: Some(aid),
            outside_input: true,
            ..
        } if *run_id == direct_id && *aid == applet_id
    )));
}
