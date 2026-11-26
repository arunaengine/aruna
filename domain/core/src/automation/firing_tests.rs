//! Unit tests covering all four mandatory DAG state machine edge cases.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::*;
use crate::automation::record::{Stage, Trigger};
use crate::automation::scope::Audience;
use crate::structs::identity::realm::RealmId;

fn run_stage(name: &str, needs: Vec<&str>, when: When) -> Stage {
    Stage {
        name: name.into(),
        needs: Some(needs.into_iter().map(String::from).collect()),
        when,
        kind: StageKind::Run {
            workflow_id: Ulid::from_parts(2, 1),
            version: 1,
            values_json: "{}".into(),
            inputs_json: None,
            output_bucket: None,
            output_prefix: None,
            placement_site: None,
            secrets: BTreeMap::new(),
            provenance_crate: false,
        },
    }
}

fn copy_stage(name: &str, from: &str, when: When) -> Stage {
    Stage {
        name: name.into(),
        needs: Some(vec![from.into()]),
        when,
        kind: StageKind::Copy {
            from_stage: from.into(),
            file: None,
            dest_bucket: "out".into(),
            dest_template: "d/{filename}".into(),
            allow_downgrade: false,
        },
    }
}

fn manual_trigger() -> TriggerPayload {
    TriggerPayload::Manual {
        values_json: "{}".into(),
        actor: UserId::new(Ulid::from_parts(1, 1), RealmId([1; 32])),
    }
}

#[test]
fn enforces_dag_edges() {
    let actor = UserId::new(Ulid::from_parts(1, 9), RealmId([1; 32]));
    let mut spec = Spec::default();
    spec.triggers.push(Trigger::Manual { form_json: None });
    spec.scope.limits.max_concurrent_workflows = 2;
    spec.stages.push(run_stage("a", vec![], When::Passed));
    spec.stages
        .push(copy_stage("quarantine", "a", When::Failed));
    spec.stages.push(copy_stage("deliver", "a", When::Passed));
    spec.stages.push(Stage {
        name: "alert".into(),
        needs: Some(vec!["deliver".into()]),
        when: When::Passed,
        kind: StageKind::Notify {
            audience: Audience::GroupAdmins,
            message: "delivered".into(),
        },
    });

    let mut firing = Firing::new(
        Ulid::from_parts(5, 1),
        Ulid::from_parts(5, 2),
        Ulid::from_parts(5, 3),
        1,
        &spec,
        manual_trigger(),
        100,
    );
    // Edge case 1: NeedsAttention is non-terminal; resume refused until resolved.
    firing.stages[0].state = StageState::NeedsAttention;
    firing.evaluate(110);
    assert_eq!(firing.state, FiringState::NeedsAttention);
    assert_eq!(firing.stages[1].state, StageState::Pending);
    assert_eq!(firing.stages[2].state, StageState::Pending);
    assert!(firing.prepare_resume(120).is_err());

    // Resolve as failed -> quarantine becomes Ready while deliver is Skipped.
    firing
        .resolve("a", false, actor, Some("retry".into()), 130)
        .unwrap();
    assert_eq!(firing.resolutions.len(), 1);
    assert_eq!(firing.stages[1].state, StageState::Ready);
    assert_eq!(firing.stages[2].state, StageState::Skipped);
    assert_eq!(firing.next_copy(), Some(1));

    // Quarantine commits a receipt and succeeds; firing ends Failed (since `a` failed).
    firing.stages[1].receipt.copied.push(CopiedFile {
        source_key: "in.dat".into(),
        source_version: Ulid::from_parts(1, 2),
        dest_bucket: "out".into(),
        dest_key: "d/in.dat".into(),
        dest_version: Ulid::from_parts(1, 3),
        bytes: 10,
    });
    firing.stages[1].state = StageState::Succeeded;
    firing.evaluate(140);
    assert_eq!(firing.state, FiringState::Failed);

    // Edge case 2: Resume preserves attempt-1 quarantine receipt and finishes Succeeded.
    firing.prepare_resume(150).unwrap();
    assert_eq!(firing.attempt, 2);
    assert_eq!(firing.stages[0].state, StageState::Ready);
    assert_eq!(firing.stages[1].state, StageState::Succeeded);
    assert_eq!(firing.stages[1].receipt.copied.len(), 1);
    assert_eq!(firing.stages[2].state, StageState::Pending);

    firing.stages[0].state = StageState::Succeeded;
    firing.evaluate(160);
    assert_eq!(firing.stages[2].state, StageState::Ready);
    firing.stages[2].state = StageState::Succeeded;
    firing.evaluate(170);
    firing.stages[3].state = StageState::Succeeded;
    firing.evaluate(180);
    assert_eq!(firing.state, FiringState::Succeeded);
    assert_eq!(firing.stages[1].receipt.copied.len(), 1);
}

#[test]
fn enforces_caps_cancel() {
    let mut spec = Spec::default();
    spec.triggers.push(Trigger::Manual { form_json: None });
    spec.scope.limits.max_concurrent_workflows = 2;
    spec.stages.push(run_stage("r1", vec![], When::Passed));
    spec.stages.push(run_stage("r2", vec![], When::Passed));
    spec.stages.push(run_stage("r3", vec![], When::Passed));
    spec.stages.push(Stage {
        name: "gate".into(),
        needs: Some(vec!["r1".into()]),
        when: When::Passed,
        kind: StageKind::Approval {
            message: "approve".into(),
            timeout_ms: None,
        },
    });

    let mut firing = Firing::new(
        Ulid::from_parts(6, 1),
        Ulid::from_parts(6, 2),
        Ulid::from_parts(6, 3),
        1,
        &spec,
        manual_trigger(),
        100,
    );
    // Edge case 3: ready_runs caps fan-out at max_concurrent_workflows and available slots.
    assert_eq!(firing.ready_runs(2), vec![0, 1]);
    assert_eq!(firing.ready_runs(1), vec![0]);
    let child_id = Ulid::from_parts(7, 1);
    firing.stages[0].state = StageState::Running;
    firing.stages[0].receipt.workflow_run_id = Some(child_id);
    assert_eq!(firing.ready_runs(2), vec![1]);

    // Edge case 4: async Cancelling stops pending/ready stages and waits for active child.
    let cancel_ids = firing.begin_cancel(200);
    assert_eq!(cancel_ids, vec![(0, child_id)]);
    assert_eq!(firing.state, FiringState::Cancelling);
    assert_eq!(firing.stages[0].state, StageState::Cancelling);
    assert!(!firing.stages[0].receipt.cancel_sent);
    assert_eq!(firing.stages[1].state, StageState::Cancelled);
    assert_eq!(firing.stages[2].state, StageState::Cancelled);
    assert_eq!(firing.stages[3].state, StageState::Cancelled);

    firing.stages[0].state = StageState::Cancelled;
    firing.evaluate(250);
    assert_eq!(firing.state, FiringState::Cancelled);
    assert_eq!(firing.finished_at_ms, Some(250));
}

#[test]
fn resolves_copy_item() {
    let actor = UserId::new(Ulid::from_parts(1, 9), RealmId([1; 32]));
    let mut spec = Spec::default();
    spec.triggers.push(Trigger::Manual { form_json: None });
    spec.stages.push(run_stage("a", vec![], When::Passed));
    spec.stages.push(copy_stage("deliver", "a", When::Passed));
    let mut firing = Firing::new(
        Ulid::from_parts(8, 1),
        Ulid::from_parts(8, 2),
        Ulid::from_parts(8, 3),
        1,
        &spec,
        manual_trigger(),
        100,
    );
    firing.stages[0].state = StageState::Succeeded;
    firing.evaluate(110);
    let file = |k: &str, v: u64| CopiedFile {
        source_key: k.into(),
        source_version: Ulid::from_parts(v, 1),
        dest_bucket: "out".into(),
        dest_key: format!("d/{k}"),
        dest_version: Ulid::from_parts(v, 2),
        bytes: 42,
    };
    firing.stages[1].receipt.copied.push(file("f1", 1));
    firing.stages[1].receipt.pending_copy = Some(file("f2", 2));
    firing.stages[1].state = StageState::NeedsAttention;
    firing.evaluate(120);
    assert_eq!(firing.state, FiringState::NeedsAttention);

    // Resolving file 2 of 3 as done records file 2 and keeps the stage Running for file 3.
    firing
        .resolve_item("deliver", true, Some((None, 3)), actor, None, 130)
        .unwrap();
    assert_eq!(firing.stages[1].state, StageState::Running);
    assert_eq!(firing.state, FiringState::Running);
    assert_eq!(firing.stages[1].receipt.copied.len(), 2);
    assert_eq!(firing.stages[1].receipt.copied[1].source_key, "f2");

    // Resolving file 3 of 3 as done completes the stage.
    firing.stages[1].state = StageState::NeedsAttention;
    firing
        .resolve_item(
            "deliver",
            true,
            Some((Some(file("f3", 3)), 3)),
            actor,
            None,
            140,
        )
        .unwrap();
    assert_eq!(firing.stages[1].state, StageState::Succeeded);
    assert_eq!(firing.state, FiringState::Succeeded);
    assert_eq!(firing.stages[1].receipt.copied.len(), 3);
}
