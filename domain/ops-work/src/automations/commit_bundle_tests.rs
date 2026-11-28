//! Pure state-machine tests for `CommitBundleOperation` covering pairing, revision and schedule rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::automation::record::{
    Automation, Lifecycle, Spec, Stage, StageKind, Trigger, When,
};
use aruna_core::automation::schedule::{ClockTime, ScheduleCursor, ScheduleEvery, ScheduleSpec};
use aruna_core::automation::scope::{Audience, Scope};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::{
    StoredFile, StoredWorkflowLanguage, WorkflowRecord, WorkflowVisibility,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::user::id::UserId;
use serde_json::json;
use ulid::Ulid;

use super::{CommitBundleInput, CommitBundleOperation, SaveAutomation, SaveWorkflow};
use crate::automations::AutoError;
use crate::workflows::import::Prepared;

fn prepared_wir() -> Prepared {
    Prepared {
        language: Some(StoredWorkflowLanguage::Wir),
        entrypoint: "workflow.wir.json".into(),
        files: vec![StoredFile {
            path: "workflow.wir.json".into(),
            content: json!({"wir_version": "1"}).to_string().into_bytes(),
            executable: false,
        }],
        ..Prepared::default()
    }
}

fn base_spec(workflow_id: Ulid, version: u32) -> Spec {
    Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![Stage {
            name: "run".into(),
            needs: Some(Vec::new()),
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id,
                version,
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
            workflows: vec![(workflow_id, version)],
            audiences: vec![Audience::GroupAdmins],
            ..Scope::default()
        },
    }
}

fn sample_input(
    workflow_new: bool,
    automation_new: bool,
    spec: Spec,
    prepared: Option<Prepared>,
) -> CommitBundleInput {
    let realm_id = RealmId([2; 32]);
    CommitBundleInput {
        group_id: Ulid::from_bytes([10; 16]),
        local: iroh::SecretKey::from_bytes(&[3; 32]).public(),
        realm_id,
        actor: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        workflow_id: Ulid::from_bytes([20; 16]),
        workflow_new,
        automation_id: Ulid::from_bytes([30; 16]),
        automation_new,
        workflow: SaveWorkflow {
            workflow_id: (!workflow_new).then(|| Ulid::from_bytes([20; 16])),
            name: Some("qc".into()),
            description: None,
            language: Some(StoredWorkflowLanguage::Wir),
            visibility: Some(WorkflowVisibility::Group),
            message: Some("v1".into()),
            source: None,
        },
        automation: SaveAutomation {
            automation_id: (!automation_new).then(|| Ulid::from_bytes([30; 16])),
            expected_revision: (!automation_new).then_some(1),
            name: Some("qc-auto".into()),
            description: None,
            spec,
            pin_version: true,
        },
        prepared,
        now_ms: 1_764_300_000_000,
    }
}

#[test]
fn commits_new_pair() {
    let input = sample_input(true, true, base_spec(Ulid::nil(), 0), Some(prepared_wir()));
    let workflow_id = input.workflow_id;
    let automation_id = input.automation_id;
    let mut operation = CommitBundleOperation::new(input);

    let first = operation.first().unwrap();
    assert!(matches!(first, Next::Call(IoCall::Read(ref keys)) if keys.len() == 1));

    let check_version = operation.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(check_version, Next::Call(IoCall::Read(ref keys)) if keys.len() == 1));

    let write = operation.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(write, Next::Call(IoCall::Write(ref rows)) if rows.len() >= 4));

    let Next::Done(saved) = operation.next(IoResult::Done).unwrap() else {
        panic!("expected Done");
    };
    assert_eq!(saved.workflow.automation_id, Some(automation_id));
    assert_eq!(saved.automation.workflow_id, Some(workflow_id));
    assert_eq!(
        saved.automation.spec.scope.workflows,
        vec![(workflow_id, 1)]
    );
    assert!(saved.identity_synced);
}

#[test]
fn rejects_bundle_conflicts() {
    let input = sample_input(false, true, base_spec(Ulid::from_bytes([20; 16]), 1), None);
    let workflow_conflict = WorkflowRecord {
        workflow_id: input.workflow_id,
        group_id: input.group_id,
        name: "qc".into(),
        description: None,
        language: StoredWorkflowLanguage::Wir,
        visibility: WorkflowVisibility::Group,
        version_count: 1,
        edam_formats: Vec::new(),
        created_by: input.actor,
        created_at_ms: 1,
        updated_at_ms: 1,
        automation_id: Some(Ulid::from_bytes([99; 16])),
    };
    let mut conflict_op = CommitBundleOperation::new(input.clone());
    conflict_op.first().unwrap();
    let result = conflict_op.next(IoResult::Values(vec![Some(
        workflow_conflict.to_bytes().unwrap().into(),
    )]));
    assert!(matches!(result, Err(AutoError::Conflict(_))));

    // Existing automation linked to another workflow also conflicts.
    let both_existing = sample_input(
        false,
        false,
        base_spec(Ulid::from_bytes([20; 16]), 1),
        Some(prepared_wir()),
    );
    let mut wf = workflow_conflict.clone();
    wf.automation_id = None;
    let mut foreign_link_auto = Automation {
        automation_id: both_existing.automation_id,
        group_id: both_existing.group_id,
        name: "qc-auto".into(),
        description: String::new(),
        host_node: both_existing.local,
        lifecycle: Lifecycle::Draft,
        pause_reason: None,
        spec: both_existing.automation.spec.clone(),
        pending: None,
        account: None,
        role_id: None,
        approved_by: None,
        approved_at_ms: None,
        created_by: both_existing.actor,
        created_at_ms: 1,
        updated_at_ms: 1,
        revision: 1,
        identity_synced_revision: 0,
        workflow_id: Some(Ulid::from_bytes([88; 16])),
    };
    let mut auto_conflict = CommitBundleOperation::new(both_existing.clone());
    auto_conflict.first().unwrap();
    let err = auto_conflict.next(IoResult::Values(vec![
        Some(wf.to_bytes().unwrap().into()),
        Some(foreign_link_auto.to_bytes().unwrap().into()),
        None,
    ]));
    assert!(matches!(err, Err(AutoError::Conflict(_))));

    // Existing version collision at CheckVersion conflicts cleanly.
    foreign_link_auto.workflow_id = Some(both_existing.workflow_id);
    let mut version_collision = CommitBundleOperation::new(both_existing);
    version_collision.first().unwrap();
    let check = version_collision
        .next(IoResult::Values(vec![
            Some(wf.to_bytes().unwrap().into()),
            Some(foreign_link_auto.to_bytes().unwrap().into()),
            None,
        ]))
        .unwrap();
    assert!(matches!(check, Next::Call(IoCall::Read(_))));
    let collision = version_collision.next(IoResult::Values(vec![Some(vec![1].into())]));
    assert!(matches!(collision, Err(AutoError::Conflict(_))));
}

#[test]
fn drops_removed_schedules() {
    let mut spec_without_schedule = base_spec(Ulid::from_bytes([20; 16]), 1);
    spec_without_schedule.stages = vec![Stage {
        name: "alert".into(),
        needs: Some(Vec::new()),
        when: When::Passed,
        kind: StageKind::Notify {
            audience: Audience::GroupAdmins,
            message: "ok".into(),
        },
    }];
    spec_without_schedule.scope.workflows.clear();

    let mut spec_with_schedule = spec_without_schedule.clone();
    spec_with_schedule
        .triggers
        .push(Trigger::Schedule(ScheduleSpec {
            every: ScheduleEvery::Day,
            at: ClockTime::new(8, 0).unwrap(),
            days: Vec::new(),
            hours: None,
            zone: "UTC".into(),
        }));

    let input = sample_input(false, false, spec_without_schedule, None);
    let workflow = WorkflowRecord {
        workflow_id: input.workflow_id,
        group_id: input.group_id,
        name: "qc".into(),
        description: None,
        language: StoredWorkflowLanguage::Wir,
        visibility: WorkflowVisibility::Group,
        version_count: 1,
        edam_formats: Vec::new(),
        created_by: input.actor,
        created_at_ms: 1,
        updated_at_ms: 1,
        automation_id: Some(input.automation_id),
    };
    let active_auto = Automation {
        automation_id: input.automation_id,
        group_id: input.group_id,
        name: "qc-auto".into(),
        description: String::new(),
        host_node: input.local,
        lifecycle: Lifecycle::Active,
        pause_reason: None,
        spec: spec_with_schedule.clone(),
        pending: None,
        account: Some(input.actor),
        role_id: Some(input.automation_id),
        approved_by: Some(input.actor),
        approved_at_ms: Some(1),
        created_by: input.actor,
        created_at_ms: 1,
        updated_at_ms: 1,
        revision: 1,
        identity_synced_revision: 1,
        workflow_id: Some(input.workflow_id),
    };
    let cursor = ScheduleCursor::for_spec(&spec_with_schedule, None, false, input.now_ms).unwrap();

    let mut operation = CommitBundleOperation::new(input);
    let first = operation.first().unwrap();
    assert!(matches!(first, Next::Call(IoCall::Read(ref keys)) if keys.len() == 3));

    let write = operation
        .next(IoResult::Values(vec![
            Some(workflow.to_bytes().unwrap().into()),
            Some(active_auto.to_bytes().unwrap().into()),
            Some(cursor.to_bytes().unwrap().into()),
        ]))
        .unwrap();
    assert!(matches!(write, Next::Call(IoCall::Write(_))));

    // Removing the final schedule deletes both the due index row and the cursor row.
    let drop = operation.next(IoResult::Done).unwrap();
    assert!(matches!(drop, Next::Call(IoCall::Delete(ref keys)) if keys.len() == 2));

    let Next::Done(saved) = operation.next(IoResult::Done).unwrap() else {
        panic!("expected Done");
    };
    assert!(!saved.automation.spec.has_schedules());
    assert!(!saved.identity_synced);
}
