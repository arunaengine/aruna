//! Tests schedule ticking, missed slots, pause/resume reset and condition enforcement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::automation::firing::{FiringState, TriggerPayload};
use aruna_core::automation::keys::{active_key, kept_key, schedule_key};
use aruna_core::automation::record::{Lifecycle, PauseReason, Trigger};
use aruna_core::automation::rules::{
    Condition, ConditionMode, Conditions, ScheduleEvery, ScheduleSpec,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use byteview::ByteView;
use ulid::Ulid;

use super::{TickTimersOperation, TimerReport};
use crate::automations::admit::AdmitOutcome;
use crate::automations::change::AutoChange;
use crate::automations::firing_reads::{ListEventsOperation, ListFiringsOperation};
use crate::automations::flow_tests::Env;
use crate::automations::lifecycle::ChangeAutoOperation;
use crate::automations::matcher::MatchEventsOperation;
use crate::automations::start::{StartEventsOperation, StartManualOperation};
use crate::automations::{AutoError, schedule_row};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn utc_clock_window(ms: u64) -> (String, String) {
    let total_minutes = (ms / 60_000) % 1440;
    let hour = total_minutes / 60;
    let minute = total_minutes % 60;
    let other_hour = (hour + 12) % 24;
    let other_end = (other_hour + 1) % 24;
    (
        format!("{hour:02}:{minute:02}"),
        format!("{other_hour:02}:00-{other_end:02}:00"),
    )
}

#[test]
fn steps_timer_phases() {
    let realm = aruna_core::structs::identity::realm::RealmId([3; 32]);
    let mut operation = TickTimersOperation::new(realm);
    operation.stamp(1_700_000_000_000);
    operation.seed(42);

    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));

    let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    config.features.insert(
        Feature::Automations,
        aruna_core::structs::identity::features::FeatureMode::Scoped {
            default: true,
            groups: Default::default(),
        },
    );
    let page_call = operation
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            config,
        ))))
        .unwrap();
    assert!(matches!(page_call, Next::Call(IoCall::Page { .. })));

    let done = operation
        .next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        })
        .unwrap();
    assert_eq!(done, Next::Done(TimerReport::default()));

    // Verify a scheduled index entry loads the automation and admits due slots.
    let mut active_op = TickTimersOperation::new(realm);
    let now = 1_764_300_000_000u64;
    active_op.stamp(now);
    active_op.seed(99);
    assert!(matches!(
        active_op.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let mut enabled = RealmConfigDocument::default_for_realm(realm, Vec::new());
    enabled.features.insert(
        Feature::Automations,
        aruna_core::structs::identity::features::FeatureMode::Scoped {
            default: true,
            groups: Default::default(),
        },
    );
    assert!(matches!(
        active_op.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled
        )))),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let auto_id = Ulid::from_bytes([12; 16]);
    let (_, row_key, _) = schedule_row(auto_id, now - 7_200_000);
    let read_call = active_op
        .next(IoResult::Page {
            rows: vec![(
                row_key,
                ByteView::from((now - 7_200_000).to_be_bytes().to_vec()),
            )],
            next: None,
        })
        .unwrap();
    assert!(matches!(read_call, Next::Call(IoCall::Read(_))));

    let mut spec = aruna_core::automation::record::Spec::default();
    spec.triggers.push(Trigger::Schedule(ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: "00:00".into(),
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    }));
    let automation = aruna_core::automation::record::Automation {
        automation_id: auto_id,
        group_id: Ulid::from_bytes([13; 16]),
        name: "hourly".into(),
        description: String::new(),
        host_node: iroh::SecretKey::from_bytes(&[14; 32]).public(),
        lifecycle: Lifecycle::Active,
        pause_reason: None,
        spec,
        pending: None,
        account: None,
        role_id: None,
        approved_by: None,
        approved_at_ms: None,
        created_by: aruna_core::user::id::UserId::nil(realm),
        created_at_ms: 1,
        updated_at_ms: 1,
        revision: 1,
        workflow_id: None,
    };
    let admit_call = active_op
        .next(IoResult::Values(vec![Some(
            automation.to_bytes().unwrap().into(),
        )]))
        .unwrap();
    assert!(matches!(admit_call, Next::Call(IoCall::Child(_))));

    let skipped = aruna_core::automation::firing::Firing::skipped(
        Ulid::from_bytes([15; 16]),
        auto_id,
        automation.group_id,
        1,
        &automation.spec,
        TriggerPayload::Schedule {
            at_ms: now - 3_600_000,
        },
        "Missed scheduled start while the node was unavailable.".into(),
        Vec::new(),
        now,
    );
    let next_call = active_op
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
            AdmitOutcome::Skipped(skipped),
        ))))
        .unwrap();
    assert!(matches!(
        next_call,
        Next::Call(IoCall::Child(_)) | Next::Done(_)
    ));
}

#[tokio::test]
async fn fires_and_skips() {
    let env = Env::new().await;
    let now = now_ms();
    let (live_at, _) = utc_clock_window(now.saturating_sub(30_000));

    let mut spec = env.base_spec();
    spec.triggers = vec![Trigger::Schedule(ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: live_at,
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    })];
    spec.scope.limits.per_hour = Some(1);
    let active = env.create_active(spec).await;
    let auto_id = active.automation_id;

    // Approval seeds the schedule index row at now_ms.
    assert!(
        env.read_row(AUTOMATIONS_KEYSPACE, &schedule_key(auto_id))
            .await
            .is_some()
    );

    // Rewind the schedule cursor 90 seconds so one live slot is due.
    let (space, key, value) = schedule_row(auto_id, now.saturating_sub(90_000));
    env.write_row(&space, key, value).await;
    let first = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((first.checked, first.fired, first.skipped), (1, 1, 0));

    // Second tick immediately after sees no new slot.
    let idle = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((idle.fired, idle.skipped), (0, 0));

    // Rewind 2.5 hours so missed slots are recorded as skipped without making them up.
    let (space, key, value) = schedule_row(auto_id, now.saturating_sub(9_000_000));
    env.write_row(&space, key, value).await;
    let missed = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert!(missed.skipped >= 2, "{missed:?}");
    let skipped_page = drive(
        ListFiringsOperation::new(
            &env.owner,
            (Some(env.group), Some(auto_id)),
            Some(FiringState::Skipped),
            env.node,
            None,
            20,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(skipped_page.items.iter().any(|firing| {
        firing.reason.as_deref() == Some("Missed scheduled start while the node was unavailable.")
    }));
    // Skipped runs never enter the active set.
    for skipped_firing in &skipped_page.items {
        assert!(
            env.read_row(
                AUTOMATION_RUNS_KEYSPACE,
                &active_key(skipped_firing.firing_id)
            )
            .await
            .is_none()
        );
    }

    // Schedule hitting per_hour cap records a skipped firing rather than a kept event.
    let (space, key, value) = schedule_row(auto_id, now.saturating_sub(90_000));
    env.write_row(&space, key, value).await;
    let capped = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((capped.fired, capped.skipped), (0, 1));
    let kept = drive(
        ListEventsOperation::new(&env.owner, auto_id, env.node, 10),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(kept.is_empty());

    // Pausing removes the schedule index row; resuming resets it to now without backfill.
    let paused = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto_id, Some(active.revision)),
            AutoChange::Pause(PauseReason::Owner),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(
        env.read_row(AUTOMATIONS_KEYSPACE, &schedule_key(auto_id))
            .await
            .is_none()
    );
    let resumed = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto_id, Some(paused.revision)),
            AutoChange::Resume,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(resumed.lifecycle, Lifecycle::Active);
    assert!(
        env.read_row(AUTOMATIONS_KEYSPACE, &schedule_key(auto_id))
            .await
            .is_some()
    );
    let after_resume = drive(
        TickTimersOperation::new(env.realm).with_disabled_features(BTreeSet::new()),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!((after_resume.fired, after_resume.skipped), (0, 0));
}

#[tokio::test]
async fn enforces_conditions() {
    let env = Env::new().await;
    let now = now_ms();
    let (_, outside_window) = utc_clock_window(now);
    let (from, to) = outside_window.split_once('-').unwrap();

    // Manual start with failing time_of_day condition returns ConditionsNotMet with skipped firing.
    let mut manual_spec = env.base_spec();
    manual_spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::TimeOfDay {
            from: from.to_string(),
            to: to.to_string(),
            zone: "UTC".into(),
        }],
    };
    manual_spec.scope.limits.per_hour = Some(1);
    let manual_auto = env.create_active(manual_spec).await;

    let refused = drive(
        StartManualOperation::new(&env.owner, manual_auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await;
    let Err(AutoError::ConditionsNotMet(skipped)) = refused else {
        panic!("expected ConditionsNotMet, got {refused:?}");
    };
    assert_eq!(skipped.state, FiringState::Skipped);
    assert!(
        skipped
            .reason
            .as_deref()
            .is_some_and(|text| text.contains("outside"))
    );
    assert_eq!(skipped.conditions_checked.len(), 1);
    assert!(!skipped.conditions_checked[0].held);

    // Repeating manual start still refuses on conditions (rate cap of 1 was not consumed).
    let again = drive(
        StartManualOperation::new(&env.owner, manual_auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await;
    assert!(matches!(again, Err(AutoError::ConditionsNotMet(_))));

    // File condition on DataUploaded skips small files and keeps payload on paused replay.
    let mut file_spec = env.base_spec();
    file_spec.triggers = vec![Trigger::DataUploaded {
        bucket: "raw".into(),
        prefix: "in/".into(),
        pattern: None,
    }];
    file_spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::File {
            pattern: Some("*.fastq.gz".into()),
            min_bytes: Some(100),
            max_bytes: None,
        }],
    };
    let file_auto = env.create_active(file_spec).await;
    let loc = (env.realm, env.group, env.node);
    let small_upload = SourceEvent::data_uploaded(
        loc,
        ("raw", "in/tiny.fastq.gz"),
        Ulid::from_bytes([74; 16]),
        Some(env.owner.user_id),
        now,
    );
    env.journal(&small_upload).await;
    let report = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((report.fired, report.skipped), (0, 1));

    // Replaying a kept upload whose file fails conditions returns a skipped firing and keeps row.
    let kept_id = Ulid::from_bytes([75; 16]);
    let kept = aruna_core::automation::record::KeptEvent {
        event_id: kept_id,
        automation_id: file_auto.automation_id,
        event: small_upload,
        reason: "paused".into(),
        kept_at_ms: now,
        payload: Some(TriggerPayload::DataUploaded {
            bucket: "raw".into(),
            key: "in/tiny.fastq.gz".into(),
            version_id: Ulid::from_bytes([74; 16]),
            path: "/r/g/raw/in/tiny.fastq.gz".into(),
            size_bytes: 12,
        }),
    };
    env.write_row(
        AUTOMATIONS_KEYSPACE,
        kept_key(file_auto.automation_id, kept_id),
        kept.to_bytes().unwrap(),
    )
    .await;
    let replayed = drive(
        StartEventsOperation::new(&env.owner, file_auto.automation_id, env.node, vec![kept_id]),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].state, FiringState::Skipped);
    assert!(
        env.read_row(
            AUTOMATIONS_KEYSPACE,
            &kept_key(file_auto.automation_id, kept_id)
        )
        .await
        .is_some()
    );
}
