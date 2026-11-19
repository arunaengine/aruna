//! Tests the task check: the reserved tags, the named install, the paths a task job reads and
//! writes, and the calls and results of the screen and the receiver's admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{
    InputMode, InputSelection, InputSource, JobFamilyId, JobId, OutputDestination, OutputSelection,
};
use aruna_core::structs::identity::realm::RealmId;

use super::*;
use crate::plugins::rows::encode;
use crate::plugins::rows::tests::{child_result, values_result};
use crate::plugins::work::index_key;
use crate::tests::records::{node, payload};

const REALM: RealmId = RealmId([3; 32]);

fn tags(spec: &mut ExecutionSpec, install: &str, work: &str) {
    spec.tags
        .insert(PLUGIN_INSTALL_TAG.to_string(), install.to_string());
    spec.tags
        .insert(PLUGIN_WORK_TAG.to_string(), work.to_string());
}

/// A spec with one output, as every plugin task has.
fn with_output() -> ExecutionSpec {
    let mut spec = payload();
    spec.file_outputs.push(OutputSelection {
        container_path: "/out/".to_string(),
        path_prefix: None,
        destination_node_id: None,
        destination: OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "maps/".to_string(),
        },
        name: None,
        description: None,
    });
    spec
}

#[test]
fn refuses_outputless() {
    // A task without an output names no path its `jobs.run` grant is checked on, so no stage
    // admits it.
    let mut spec = payload();
    let (install_id, work_id) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
    tags(&mut spec, &install_id.to_string(), &work_id.to_string());
    assert!(matches!(tagged(&spec), Some(Err(_))));
}

#[test]
fn reads_task_tags() {
    let mut spec = with_output();
    assert_eq!(tagged(&spec), None);
    let (install_id, work_id) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
    tags(&mut spec, &install_id.to_string(), &work_id.to_string());
    assert_eq!(
        tagged(&spec),
        Some(Ok(Tagged {
            install_id,
            work_id
        }))
    );
    tags(&mut spec, &install_id.to_string(), "not a ulid");
    assert_eq!(tagged(&spec), Some(Err(ScreenError::Malformed)));
    spec.tags.remove(PLUGIN_INSTALL_TAG);
    assert_eq!(tagged(&spec), Some(Err(ScreenError::Malformed)));
}

#[test]
fn lists_task_paths() {
    let (ingress, other) = (node(1), node(2));
    let mut spec = payload();
    spec.inputs = vec![
        InputSelection {
            source: InputSource::S3 {
                bucket: "raw".to_string(),
                key: "reads.fastq".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "reads.fastq".to_string(),
            mode: InputMode::Snapshot,
            container_path: Some("/in/reads.fastq".to_string()),
            name: None,
            description: None,
        },
        InputSelection {
            source: InputSource::Inline {
                bytes: b"sample".to_vec(),
            },
            source_node_id: None,
            dest_key: "/in/samples.tsv".to_string(),
            mode: InputMode::Snapshot,
            container_path: Some("/in/samples.tsv".to_string()),
            name: None,
            description: None,
        },
    ];
    spec.file_outputs = vec![OutputSelection {
        container_path: "/out/results/".to_string(),
        path_prefix: None,
        destination_node_id: Some(other),
        destination: OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "run-7/".to_string(),
        },
        name: None,
        description: None,
    }];
    let group = spec.group_id;
    // Inline bytes name no object; inputs without a node are read at the ingress.
    assert_eq!(
        screen_targets(&spec, REALM, ingress),
        vec![
            (
                Permission::ObjectsRead,
                object_permission_path(REALM, group, ingress, "raw", "reads.fastq")
            ),
            (
                Permission::JobsRun,
                object_permission_path(REALM, group, other, "results", "run-7/")
            ),
        ]
    );
}

pub(crate) fn install() -> PluginInstall {
    PluginInstall {
        install_id: Ulid::from_bytes([1; 16]),
        group_id: Ulid::from_bytes([2; 16]),
        plugin: "map-reads".to_string(),
        version: "0.1.0".to_string(),
        digest: "sha256:aa".to_string(),
        state: InstallState::Active,
        paused: false,
        settings: "{}".to_string(),
        grants: Default::default(),
        account: UserId::local(Ulid::from_bytes([3; 16]), REALM),
        role_id: Ulid::from_bytes([4; 16]),
        receiver: node(1),
        granted_by: UserId::local(Ulid::from_bytes([5; 16]), REALM),
        request: "sha256:bb".to_string(),
        revision: 1,
        active_since_ms: Some(1),
        failed: None,
        prerequisites: Vec::new(),
        activated_by: None,
        widened: None,
        secrets: Default::default(),
    }
}

#[test]
fn fences_by_row() {
    let install = install();
    let family = JobFamilyId {
        submission_id: aruna_core::structs::execution::job::SubmissionId([5; 32]),
        request_digest: [6; 32],
    };
    let adopted = (Some(family), family);
    let mut row = WorkRow::reserved(&install, WorkKind::Recipe, (Ulid::nil(), Ulid::nil()), 1);
    row.job = Some(JobId::from_bytes([5; 16]));
    // A reserved row asks again; a launch before adoption is never refused for good.
    assert_eq!(
        work_admits(&install, Some(&row), adopted),
        Err(ScreenError::Pending)
    );
    row.state = WorkState::Dispatched;
    assert_eq!(work_admits(&install, Some(&row), adopted), Ok(()));
    // The row keeps another holder's alias of the same family: still this family's launch.
    row.job = Some(JobId::from_bytes([7; 16]));
    assert_eq!(work_admits(&install, Some(&row), adopted), Ok(()));
    let refused = |row: Option<&WorkRow>, adopted| {
        matches!(
            work_admits(&install, row, adopted),
            Err(ScreenError::Refused(_))
        )
    };
    assert!(refused(None, adopted));
    // A row that adopted another family, or none yet, never admits this launch.
    let other = JobFamilyId {
        request_digest: [8; 32],
        ..family
    };
    assert!(refused(Some(&row), (Some(other), family)));
    assert!(refused(Some(&row), (None, family)));
    let settled = WorkRow {
        state: WorkState::Settled,
        ..row.clone()
    };
    assert!(refused(Some(&settled), adopted));
    let mut updated = row.clone();
    updated.binding.digest = "sha256:cc".to_string();
    assert!(refused(Some(&updated), adopted));
    let copy = WorkRow {
        kind: WorkKind::Copy,
        ..row.clone()
    };
    assert!(refused(Some(&copy), adopted));
    let former = WorkRow {
        account: UserId::local(Ulid::from_bytes([9; 16]), REALM),
        ..row
    };
    assert!(refused(Some(&former), adopted));
}

#[test]
fn names_active_install() {
    let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
    let tagged = Tagged {
        install_id: Ulid::from_bytes([1; 16]),
        work_id: Ulid::nil(),
    };
    assert!(matches!(
        named_install(&config, tagged),
        Err(ScreenError::Install(_))
    ));
    config.installs.push(install());
    assert!(named_install(&config, tagged).is_ok());
    config.installs[0].paused = true;
    assert_eq!(
        named_install(&config, tagged),
        Err(ScreenError::Install("paused"))
    );
    config.installs[0].paused = false;
    config.installs[0].state = InstallState::Removing;
    assert!(matches!(
        named_install(&config, tagged),
        Err(ScreenError::Install(_))
    ));
}

fn family() -> JobFamilyId {
    JobFamilyId {
        submission_id: aruna_core::structs::execution::job::SubmissionId([5; 32]),
        request_digest: [6; 32],
    }
}

/// A tagged spec of `install` for work `work_id`, with its output.
fn task_spec(install: &PluginInstall, work_id: Ulid) -> ExecutionSpec {
    let mut spec = with_output();
    spec.group_id = install.group_id;
    tags(
        &mut spec,
        &install.install_id.to_string(),
        &work_id.to_string(),
    );
    spec
}

fn screen(install: &PluginInstall, created_by: UserId) -> ScreenTask {
    let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
    config.installs.push(install.clone());
    let spec = task_spec(install, Ulid::from_bytes([8; 16]));
    ScreenTask::new(Arc::new(config), (&spec, created_by, node(1)))
}

#[test]
fn screen_reads_account() {
    let install = install();
    // Another account than the install's is refused before anything is read.
    let mut other = screen(&install, install.granted_by);
    assert_eq!(other.first(), Err(ScreenError::Account));
    let mut task = screen(&install, install.account);
    assert!(matches!(task.first(), Ok(Next::Call(IoCall::Read(rows))) if rows.len() == 1));
    // A missing account is not the install's active account.
    assert_eq!(
        task.next(values_result(vec![None])),
        Err(ScreenError::Account)
    );
}

#[test]
fn admit_needs_receiver() {
    let install = install();
    let mut admit = AdmitTask::new(screen(&install, install.account), (node(2), family()));
    assert!(matches!(admit.first(), Ok(Next::Call(IoCall::Child(_)))));
    let screened = child_result(Ok::<_, ScreenError>(install.clone()));
    assert!(matches!(admit.next(screened), Err(ScreenError::Refused(_))));
}

#[test]
fn admit_reads_rows() {
    let install = install();
    let work_id = Ulid::from_bytes([8; 16]);
    let mut row = WorkRow::reserved(&install, WorkKind::Recipe, (Ulid::nil(), work_id), 1);
    row.binding.digest = install.digest.clone();
    row.state = WorkState::Dispatched;
    let admitted = |task: Option<TaskRow>| {
        let mut admit = AdmitTask::new(screen(&install, install.account), (node(1), family()));
        admit.first().unwrap();
        let screened = child_result(Ok::<_, ScreenError>(install.clone()));
        let Ok(Next::Call(IoCall::Read(index))) = admit.next(screened) else {
            panic!("the work row is looked up");
        };
        assert_eq!(&*index[0].1, &index_key(install.install_id, work_id)[..]);
        let key = b"row".to_vec();
        admit.next(values_result(vec![Some(key)])).unwrap();
        let found = values_result(vec![Some(encode(&row).unwrap())]);
        assert!(matches!(admit.next(found), Ok(Next::Call(IoCall::Read(_)))));
        let task = task.map(|task| encode(&task).unwrap());
        admit.next(values_result(vec![task]))
    };
    // The receiver adopted this family: the launch runs as the install.
    let adopted = TaskRow {
        family: Some(family()),
        ..TaskRow::default()
    };
    assert_eq!(admitted(Some(adopted)), Ok(Next::Done(install.clone())));
    // Without an adopted family the row is not this launch's.
    assert!(matches!(admitted(None), Err(ScreenError::Refused(_))));
}
