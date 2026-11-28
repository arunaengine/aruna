//! Tests due-indexed schedule ticking, missed-range summaries, replay checks and conditions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::firing::{Firing, FiringState, SkippedFiring, TriggerPayload};
use aruna_core::automation::keys::{active_key, kept_key, schedule_cursor_key, schedule_due_key};
use aruna_core::automation::record::{Automation, KeptEvent, Lifecycle, PauseReason, Trigger};
use aruna_core::automation::rules::{Condition, ConditionCheckKind, ConditionMode, Conditions};
use aruna_core::automation::schedule::{
    ClockTime, ScheduleAnchor, ScheduleCursor, ScheduleEvery, ScheduleSpec,
};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::structs::execution::workflow_run::{AutomationOrigin, RunState, run_key};
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use ulid::Ulid;

use super::{TickTimersOperation, TimerReport};
use crate::automations::admit::{AdmitFiringOp, AdmitOutcome};
use crate::automations::change::AutoChange;
use crate::automations::firing_reads::{ListEventsOperation, ListFiringsOperation};
use crate::automations::flow_tests::Env;
use crate::automations::lifecycle::ChangeAutoOperation;
use crate::automations::matcher::MatchEventsOperation;
use crate::automations::reads::LoadAutoOperation;
use crate::automations::start::{ManualStart, StartEventsOperation, StartManualOperation};
use crate::automations::{AutoError, schedule_cursor_row, schedule_due_row};

fn utc_clock(ms: u64) -> ClockTime {
    let total_minutes = (ms / 60_000) % 1440;
    ClockTime::new((total_minutes / 60) as u8, (total_minutes % 60) as u8).unwrap()
}

fn outside_utc_window(ms: u64) -> (ClockTime, ClockTime) {
    let hour = ((ms / 60_000) / 60) % 24;
    let start_hour = ((hour + 12) % 24) as u8;
    let end_hour = ((hour + 13) % 24) as u8;
    (
        ClockTime::new(start_hour, 0).unwrap(),
        ClockTime::new(end_hour, 0).unwrap(),
    )
}

fn enabled_config(realm: RealmId, on: bool) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    config.features.insert(
        Feature::Automations,
        FeatureMode::Scoped {
            default: on,
            groups: Default::default(),
        },
    );
    config
}

fn hourly_auto(realm: RealmId, auto_id: Ulid, schedule: ScheduleSpec) -> Automation {
    let mut spec = aruna_core::automation::record::Spec::default();
    spec.triggers.push(Trigger::Schedule(schedule));
    Automation {
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
        identity_synced_revision: 1,
        workflow_id: None,
    }
}

#[test]
fn steps_timer_phases() {
    let realm = RealmId([3; 32]);
    let now = 1_764_300_000_000u64;
    let auto_id = Ulid::from_bytes([12; 16]);
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: ClockTime::new(0, 0).unwrap(),
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    };

    let automation = hourly_auto(realm, auto_id, schedule);
    let group_bytes: aruna_core::types::Value = automation.group_id.to_bytes().to_vec().into();

    // 1. Future due_ms stops immediately without reading any automation record.
    let mut idle = TickTimersOperation::new(realm);
    idle.stamp(now);
    assert!(matches!(idle.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        idle.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm, true)
        )))),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let done = idle
        .next(IoResult::Page {
            rows: vec![(schedule_due_key(now + 60_000, auto_id), group_bytes.clone())],
            next: Some(schedule_due_key(now + 60_000, auto_id)),
        })
        .unwrap();
    assert_eq!(done, Next::Done(TimerReport::default()));

    // 2. Disabled feature skips due automation right in index scan with zero record reads.
    let mut gated = TickTimersOperation::new(realm);
    gated.stamp(now);
    gated.first().unwrap();
    gated
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm, false),
        ))))
        .unwrap();
    let skipped_off = gated
        .next(IoResult::Page {
            rows: vec![(schedule_due_key(now - 1_000, auto_id), group_bytes.clone())],
            next: None,
        })
        .unwrap();
    assert_eq!(skipped_off, Next::Done(TimerReport::default()));

    // 3. Missed range followed by a live slot triggers a second Admit call on the same tick.
    let exact_hour = (now / 3_600_000) * 3_600_000;
    let tick_now = exact_hour + 10_000;
    let mut active = TickTimersOperation::new(realm);
    active.stamp(tick_now);
    active.seed(99);
    active.first().unwrap();
    active
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm, true),
        ))))
        .unwrap();
    let first_admit = active
        .next(IoResult::Page {
            rows: vec![(
                schedule_due_key(exact_hour - 7_200_000, auto_id),
                group_bytes.clone(),
            )],
            next: None,
        })
        .unwrap();
    assert!(matches!(first_admit, Next::Call(IoCall::Child(_))));

    let missed = Firing::skipped(SkippedFiring {
        firing_id: Ulid::from_bytes([15; 16]),
        automation_id: auto_id,
        group_id: automation.group_id,
        revision: 1,
        spec: &automation.spec,
        trigger: TriggerPayload::Schedule {
            at_ms: exact_hour - 3_600_000,
        },
        reason: "Missed 2 scheduled starts.".into(),
        conditions_checked: Vec::new(),
        now_ms: tick_now,
    });
    let second_admit = active
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
            AdmitOutcome::Skipped(missed),
        ))))
        .unwrap();
    assert!(matches!(second_admit, Next::Call(IoCall::Child(_))));

    let live = Firing::new(
        Ulid::from_bytes([16; 16]),
        auto_id,
        automation.group_id,
        1,
        &automation.spec,
        TriggerPayload::Schedule { at_ms: exact_hour },
        tick_now,
    );
    let finished = active
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
            AdmitOutcome::Created(live),
        ))))
        .unwrap();
    assert_eq!(
        finished,
        Next::Done(TimerReport {
            checked: 1,
            fired: 1,
            skipped: 1,
        })
    );

    // 4. Conflict or NotFound from Admit moves cleanly to Done without failing the tick.
    let mut conflict = TickTimersOperation::new(realm);
    conflict.stamp(tick_now);
    conflict.first().unwrap();
    conflict
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm, true),
        ))))
        .unwrap();
    let admit_call = conflict
        .next(IoResult::Page {
            rows: vec![(schedule_due_key(exact_hour, auto_id), group_bytes)],
            next: None,
        })
        .unwrap();
    assert!(matches!(admit_call, Next::Call(IoCall::Child(_))));
    let handled = conflict
        .next(IoResult::Child(ChildOutcome::new(Err::<
            AdmitOutcome,
            AutoError,
        >(
            AutoError::NotFound
        ))))
        .unwrap();
    assert_eq!(
        handled,
        Next::Done(TimerReport {
            checked: 1,
            fired: 0,
            skipped: 0,
        })
    );
}

#[test]
fn steps_admit_phases() {
    let realm = RealmId([5; 32]);
    let auto_id = Ulid::from_bytes([21; 16]);
    let firing_id = Ulid::from_bytes([22; 16]);
    let exact_hour = 1_764_302_400_000u64;
    let tick_now = exact_hour + 10_000;
    let schedule = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: ClockTime::new(0, 0).unwrap(),
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    };
    let mut auto = hourly_auto(realm, auto_id, schedule.clone());
    auto.spec.scope.limits.per_hour = Some(1);

    // 1. Live schedule hitting per_hour writes a Skipped firing and advances the due row.
    let cursor = ScheduleCursor {
        due_ms: exact_hour,
        anchors: vec![ScheduleAnchor {
            schedule,
            last_ms: exact_hour - 3_600_000,
        }],
    };
    let mut capped = AdmitFiringOp::schedule(auto_id, firing_id, tick_now);
    capped.stamp(tick_now);
    assert!(matches!(
        capped.first(),
        Ok(Next::Call(IoCall::Read(ref keys))) if keys.len() == 4
    ));
    let write = capped
        .next(IoResult::Values(vec![
            Some(auto.to_bytes().unwrap().into()),
            Some(1u64.to_be_bytes().to_vec().into()),
            Some(1u64.to_be_bytes().to_vec().into()),
            Some(cursor.to_bytes().unwrap().into()),
        ]))
        .unwrap();
    assert!(matches!(write, Next::Call(IoCall::Write(_))));
    let delete_old_due = capped.next(IoResult::Done).unwrap();
    assert!(matches!(delete_old_due, Next::Call(IoCall::Delete(ref keys)) if keys.len() == 1));
    let Next::Done(AdmitOutcome::Skipped(skipped)) = capped.next(IoResult::Done).unwrap() else {
        panic!("expected Skipped");
    };
    assert_eq!(
        skipped.conditions_checked[0].kind,
        ConditionCheckKind::Starts
    );

    // 2. Removed schedule in fresh Automation clears cursor+due rows and returns Unmatched.
    let mut removed_auto = auto.clone();
    removed_auto.spec.triggers = vec![Trigger::Manual { form_json: None }];
    let mut stale = AdmitFiringOp::schedule(auto_id, firing_id, tick_now);
    stale.stamp(tick_now);
    stale.first().unwrap();
    let cleanup = stale
        .next(IoResult::Values(vec![
            Some(removed_auto.to_bytes().unwrap().into()),
            None,
            None,
            Some(cursor.to_bytes().unwrap().into()),
        ]))
        .unwrap();
    assert!(matches!(cleanup, Next::Call(IoCall::Delete(ref keys)) if keys.len() == 2));
    assert_eq!(
        stale.next(IoResult::Done).unwrap(),
        Next::Done(AdmitOutcome::Unmatched)
    );

    // 3. Rate-window keys stay pinned across a tx conflict retry crossing midnight UTC.
    let utc_midnight = 1_764_374_400_000u64;
    let initial_template = AdmitFiringOp::manual(
        auto_id,
        firing_id,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: aruna_core::user::id::UserId::nil(realm),
        },
        utc_midnight - 1_000,
    );
    let mut attempt_one = initial_template.clone();
    attempt_one.stamp(utc_midnight - 1_000);
    let Next::Call(IoCall::Read(first_reads)) = attempt_one.first().unwrap() else {
        panic!("expected Read");
    };
    let initial_hour_key = first_reads[1].1.clone();
    let initial_day_key = first_reads[2].1.clone();

    // Retry clones the pre-first() op and stamps after midnight UTC; keys must not change.
    let mut attempt_two = initial_template.clone();
    attempt_two.stamp(utc_midnight + 1_000);
    let Next::Call(IoCall::Read(retry_reads)) = attempt_two.first().unwrap() else {
        panic!("expected Read");
    };
    assert_eq!(retry_reads[1].1, initial_hour_key);
    assert_eq!(retry_reads[2].1, initial_day_key);

    let mut manual_auto = auto;
    manual_auto.spec.triggers = vec![Trigger::Manual { form_json: None }];
    manual_auto.spec.scope.limits.per_hour = Some(5);
    manual_auto.spec.scope.limits.per_day = Some(10);
    assert!(matches!(
        attempt_two.next(IoResult::Values(vec![
            Some(manual_auto.to_bytes().unwrap().into()),
            None,
            None,
        ])),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let Next::Call(IoCall::Write(writes)) = attempt_two
        .next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        })
        .unwrap()
    else {
        panic!("expected Write");
    };
    assert!(writes.iter().any(|(_, key, _)| *key == initial_hour_key));
    assert!(writes.iter().any(|(_, key, _)| *key == initial_day_key));

    // 4. Retry-After reflects the latest exceeded pinned window, floored at 1s after expiry.
    let mut expired_retry = initial_template;
    expired_retry.stamp(utc_midnight + 1_000);
    expired_retry.first().unwrap();
    let expired_limit = expired_retry.next(IoResult::Values(vec![
        Some(manual_auto.to_bytes().unwrap().into()),
        None,
        Some(10u64.to_be_bytes().to_vec().into()),
    ]));
    assert!(matches!(
        expired_limit,
        Err(AutoError::Limited { retry_after: 1, .. })
    ));

    let mid_morning = utc_midnight + 10 * 3_600_000 + 30 * 60_000;
    let mut both_caps = AdmitFiringOp::manual(
        auto_id,
        firing_id,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: aruna_core::user::id::UserId::nil(realm),
        },
        mid_morning,
    );
    both_caps.stamp(mid_morning);
    both_caps.first().unwrap();
    let both_err = both_caps.next(IoResult::Values(vec![
        Some(manual_auto.to_bytes().unwrap().into()),
        Some(5u64.to_be_bytes().to_vec().into()),
        Some(10u64.to_be_bytes().to_vec().into()),
    ]));
    assert!(matches!(
        both_err,
        Err(AutoError::Limited {
            retry_after: 48_600,
            ..
        })
    ));

    let mut hour_only = AdmitFiringOp::manual(
        auto_id,
        firing_id,
        TriggerPayload::Manual {
            values_json: "{}".into(),
            actor: aruna_core::user::id::UserId::nil(realm),
        },
        mid_morning,
    );
    hour_only.stamp(mid_morning);
    hour_only.first().unwrap();
    let hour_err = hour_only.next(IoResult::Values(vec![
        Some(manual_auto.to_bytes().unwrap().into()),
        Some(5u64.to_be_bytes().to_vec().into()),
        Some(5u64.to_be_bytes().to_vec().into()),
    ]));
    assert!(matches!(
        hour_err,
        Err(AutoError::Limited {
            retry_after: 1_800,
            ..
        })
    ));
}

#[tokio::test]
async fn fires_and_skips() {
    let env = Env::new().await;
    // Create with a placeholder schedule and active cap = 1 (immune to hour boundaries),
    // then align `at` using the driver-stamped timestamp.
    let initial_schedule = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: ClockTime::new(0, 0).unwrap(),
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    };
    let mut spec = env.base_spec();
    spec.triggers = vec![Trigger::Schedule(initial_schedule)];
    spec.scope.limits.max_active_firings = Some(1);
    let active = env.create_active(spec).await;
    let auto_id = active.automation_id;

    // Approval seeds both the schedule cursor row and the due-time index row.
    let stored_bytes = env
        .read_row(AUTOMATIONS_KEYSPACE, &schedule_cursor_key(auto_id))
        .await
        .expect("cursor row seeded");
    let seeded_cursor = ScheduleCursor::from_bytes(&stored_bytes).unwrap();
    assert!(
        env.read_row(
            AUTOMATIONS_KEYSPACE,
            &schedule_due_key(seeded_cursor.due_ms, auto_id)
        )
        .await
        .is_some()
    );

    // Rewind the schedule cursor right at the driver-stamped UTC minute so one live slot is due.
    let live_slot_ms = (active.updated_at_ms / 60_000) * 60_000;
    let aligned = ScheduleSpec {
        every: ScheduleEvery::Hours,
        at: utc_clock(live_slot_ms),
        days: Vec::new(),
        hours: Some(1),
        zone: "UTC".into(),
    };
    let mut updated_auto = active.clone();
    updated_auto.spec.triggers = vec![Trigger::Schedule(aligned.clone())];
    env.write_row(
        AUTOMATIONS_KEYSPACE,
        aruna_core::automation::keys::record_key(auto_id),
        updated_auto.to_bytes().unwrap(),
    )
    .await;
    let rewind = ScheduleCursor {
        due_ms: live_slot_ms,
        anchors: vec![ScheduleAnchor {
            schedule: aligned.clone(),
            last_ms: live_slot_ms.saturating_sub(3_600_000),
        }],
    };
    let (space, key, value) = schedule_cursor_row(auto_id, &rewind).unwrap();
    env.write_row(&space, key, value).await;
    let (due_space, due_key, due_value) = schedule_due_row(rewind.due_ms, auto_id, env.group);
    env.write_row(&due_space, due_key, due_value).await;

    let first = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((first.checked, first.fired, first.skipped), (1, 1, 0));

    // Second tick immediately after stops on future due_ms with zero checks.
    let idle = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((idle.checked, idle.fired, idle.skipped), (0, 0, 0));

    // Rewind 48.5 hours so 48 missed starts collapse into ONE summary skipped firing and the live
    // slot is capped by max_active_firings = 1 as a second skipped firing in the same tick.
    let long_outage = ScheduleCursor {
        due_ms: live_slot_ms.saturating_sub(48 * 3_600_000),
        anchors: vec![ScheduleAnchor {
            schedule: aligned.clone(),
            last_ms: live_slot_ms.saturating_sub(49 * 3_600_000),
        }],
    };
    let (space, key, value) = schedule_cursor_row(auto_id, &long_outage).unwrap();
    env.write_row(&space, key, value).await;
    let (due_space, due_key, due_value) = schedule_due_row(long_outage.due_ms, auto_id, env.group);
    env.write_row(&due_space, due_key, due_value).await;

    let missed = drive(TickTimersOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((missed.fired, missed.skipped), (0, 2));

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
        firing
            .reason
            .as_deref()
            .is_some_and(|text| text.contains("Missed 48 scheduled starts"))
    }));
    // Skipped runs never enter the active set, and schedule rate limits never write kept events.
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
    let kept = drive(
        ListEventsOperation::new(&env.owner, auto_id, env.node, 10),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(kept.is_empty());

    // Approving an unrelated edit on an active scheduled automation preserves an already-due slot.
    let preserve_cursor = ScheduleCursor {
        due_ms: live_slot_ms,
        anchors: vec![ScheduleAnchor {
            schedule: aligned,
            last_ms: live_slot_ms.saturating_sub(3_600_000),
        }],
    };
    let (space, key, value) = schedule_cursor_row(auto_id, &preserve_cursor).unwrap();
    env.write_row(&space, key, value).await;
    let approved_again = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto_id, Some(active.revision)),
            AutoChange::Approve,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let preserved_bytes = env
        .read_row(AUTOMATIONS_KEYSPACE, &schedule_cursor_key(auto_id))
        .await
        .unwrap();
    assert_eq!(
        ScheduleCursor::from_bytes(&preserved_bytes).unwrap().due_ms,
        live_slot_ms
    );

    // Pausing removes the schedule cursor row; resuming seeds it at now without backfill.
    let paused = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (auto_id, Some(approved_again.revision)),
            AutoChange::Pause(PauseReason::Owner),
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(
        env.read_row(AUTOMATIONS_KEYSPACE, &schedule_cursor_key(auto_id))
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
        env.read_row(AUTOMATIONS_KEYSPACE, &schedule_cursor_key(auto_id))
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
async fn enforces_condition_guards() {
    let env = Env::new().await;
    // Seed one active record to obtain the driver's stamped timestamp without SystemTime::now().
    let anchor = env.create_active(env.base_spec()).await;
    let now = anchor.updated_at_ms;
    let (from, to) = outside_utc_window(now);

    // Manual start with failing time_of_day condition returns ManualStart::Skipped.
    let mut manual_spec = env.base_spec();
    manual_spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::TimeOfDay {
            from,
            to,
            zone: "UTC".into(),
        }],
    };
    manual_spec.scope.limits.per_hour = Some(1);
    let manual_auto = env.create_active(manual_spec).await;

    let refused = drive(
        StartManualOperation::new(&env.owner, manual_auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await
    .unwrap();
    let ManualStart::Skipped(skipped) = refused else {
        panic!("expected ManualStart::Skipped, got {refused:?}");
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

    // Repeating manual start still skips on conditions (rate cap of 1 was not consumed).
    let again = drive(
        StartManualOperation::new(&env.owner, manual_auto.automation_id, env.node, "{}".into()),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(matches!(again, ManualStart::Skipped(_)));

    // Run-finished event failing conditions skips WITHOUT pausing for chain depth.
    let workflow_id = Ulid::from_bytes([60; 16]);
    let mut chain_spec = env.base_spec();
    chain_spec.triggers = vec![Trigger::RunFinished {
        workflow_id,
        status: None,
        applet_id: None,
    }];
    chain_spec.conditions = Conditions {
        mode: ConditionMode::All,
        items: vec![Condition::TimeOfDay {
            from,
            to,
            zone: "UTC".into(),
        }],
    };
    chain_spec.scope.limits.max_chain_depth = Some(1);
    let chain_auto = env.create_active(chain_spec).await;
    let run_id = Ulid::from_bytes([61; 16]);
    let origin = AutomationOrigin {
        automation_id: Ulid::from_bytes([62; 16]),
        firing_id: Ulid::from_bytes([63; 16]),
        stage: "run".into(),
        applet_id: None,
        outside_input: false,
        depth: 1,
    };
    let run = env.sample_run(run_id, workflow_id, RunState::Succeeded, Some(origin));
    env.write_row(
        aruna_core::keyspaces::WORKFLOW_RUN_KEYSPACE,
        run_key(run_id),
        run.to_bytes().unwrap(),
    )
    .await;
    let path = group_permission_path(env.realm, env.group, env.node);
    env.journal(&SourceEvent::run_finished(
        (env.group, path),
        (workflow_id, run_id),
        "succeeded",
        env.owner.user_id,
        now,
    ))
    .await;
    let chain_report = drive(MatchEventsOperation::new(env.realm), &env.ctx)
        .await
        .unwrap();
    assert_eq!((chain_report.skipped, chain_report.paused), (1, 0));
    let still_active = drive(LoadAutoOperation::new(chain_auto.automation_id), &env.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(still_active.lifecycle, Lifecycle::Active);

    // File condition on DataUploaded skips small files; narrowing trigger blocks replay.
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
    let file_auto = env.create_active(file_spec.clone()).await;
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
    let kept = KeptEvent {
        event_id: kept_id,
        automation_id: file_auto.automation_id,
        event: small_upload,
        reason: "paused".into(),
        kept_at_ms: now,
        payload: TriggerPayload::DataUploaded {
            bucket: "raw".into(),
            key: "in/tiny.fastq.gz".into(),
            version_id: Ulid::from_bytes([74; 16]),
            path: "/r/g/raw/in/tiny.fastq.gz".into(),
            size_bytes: Some(12),
        },
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

    // Switching the active upload prefix refuses replaying the older kept upload and preserves it.
    let mut narrowed = file_spec;
    narrowed.triggers = vec![Trigger::DataUploaded {
        bucket: "raw".into(),
        prefix: "in/other/".into(),
        pattern: None,
    }];
    let updated = drive(
        ChangeAutoOperation::new(
            &env.owner,
            (file_auto.automation_id, Some(file_auto.revision)),
            AutoChange::Update {
                name: None,
                description: None,
                spec: Some(narrowed),
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
            (updated.automation_id, Some(updated.revision)),
            AutoChange::Approve,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let refused_replay = drive(
        StartEventsOperation::new(&env.owner, file_auto.automation_id, env.node, vec![kept_id]),
        &env.ctx,
    )
    .await;
    assert!(matches!(refused_replay, Err(AutoError::Conflict(_))));
    assert!(
        env.read_row(
            AUTOMATIONS_KEYSPACE,
            &kept_key(file_auto.automation_id, kept_id)
        )
        .await
        .is_some()
    );
}
