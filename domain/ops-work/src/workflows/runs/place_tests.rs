//! Tests where a workflow task runs and where its outputs stay: the child spec's GPUs, site and
//! node labels and run area, the placement the task records, and outputs on another node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::family::{
    ExecutionRole, OutputSet, PhysicalExecutionState, ProjectedExecution,
};
use aruna_core::structs::execution::job::local::OutputObject;
use aruna_workflow_contract::settings::StepPlacement;

use super::super::data::address_on;
use super::super::tests::{ingress, sample_run, start_task};
use super::*;
use crate::tests::records::{Family, captured_input, node};

fn label(key: &str) -> String {
    format!("{LABEL_TAG_PREFIX}{key}")
}

/// A projection whose canonical execution ran on `executor` and wrote `outputs`.
fn projection(fixture: &Family, executor: NodeId, outputs: Vec<OutputObject>) -> JobProjection {
    let family = fixture.family();
    let execution_id = Ulid::from_bytes([6; 16]);
    JobProjection {
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        canonical_job_id: fixture.spec().job_id,
        aliases: Vec::new(),
        state: LogicalJobState::Succeeded,
        canonical_execution_id: Some(execution_id),
        executions: vec![ProjectedExecution {
            execution_id,
            executor_node_id: executor,
            state: PhysicalExecutionState::Succeeded,
            role: ExecutionRole::Canonical,
            started_at_ms: Some(1),
            observed_at_ms: Some(2),
            result: None,
        }],
        outputs: OutputSet::canonical(outputs).expect("ordered outputs"),
        cancel_requested: false,
        expiry_reason: None,
    }
}

// The child asks for the task's GPUs, pins its site and node as required labels, reads an input
// on another node there, and keeps its work folder in the run area of the node that runs it.
#[test]
fn maps_task_placement() {
    let mut run = sample_run();
    run.placement_site = Some("eu-west".into());
    let holder = node(7);
    let mut request = start_task();
    request.resources.gpus = 2;
    let version = Ulid::from_bytes([8; 16]);
    request.direct_inputs[0].address = address_on("reads", "a.txt", Some(version), holder);
    request.step_settings = Some(ArunaStepSettings {
        placement: StepPlacement {
            node: Some(holder.to_string()),
            site: None,
        },
        ..ArunaStepSettings::default()
    });

    let spec = task_spec(&run, &request, "key").unwrap();
    assert_eq!(spec.resources.gpus, Some(2));
    assert_eq!(spec.tags[&label(LOCATION_LABEL_KEY)], "eu-west");
    assert_eq!(spec.tags[&label(NODE_LABEL_KEY)], holder.to_string());
    let required = aruna_core::job_request::required_labels(&spec).unwrap();
    assert_eq!(required.len(), 2);
    assert_eq!(spec.inputs[1].source_node_id, Some(holder));
    let [control, work] = spec.file_outputs.as_slice() else {
        panic!("a control and a work folder: {:?}", spec.file_outputs);
    };
    assert!(
        matches!(&control.destination, OutputDestination::S3 { bucket, .. } if bucket == "results")
    );
    let prefix = format!("{}/tasks/run-7-1/work", run.run_id);
    assert_eq!(work.destination, OutputDestination::RunArea { prefix });
    assert_eq!(work.destination_node_id, None);

    // The task's own site wins over the run's; no site leaves the task free.
    request.site = Some("eu-central".into());
    let spec = task_spec(&run, &request, "key").unwrap();
    assert_eq!(spec.tags[&label(LOCATION_LABEL_KEY)], "eu-central");
    let spec = task_spec(&sample_run(), &start_task(), "key").unwrap();
    assert!(
        aruna_core::job_request::required_labels(&spec)
            .unwrap()
            .is_empty()
    );
    assert_eq!(spec.resources.gpus, None);
}

// A child that ran where all its inputs were runs compute to data; one that staged an input
// from another node runs data to compute and counts the staged bytes.
#[test]
fn records_child_placement() {
    let fixture = Family::new([1; 32]);
    let (executor, holder) = (node(2), node(3));
    let mut spec = fixture.spec();
    let input = |key: &str| InputSelection {
        source: InputSource::S3 {
            bucket: "data".into(),
            key: key.into(),
            version_id: None,
        },
        source_node_id: None,
        dest_key: key.into(),
        mode: InputMode::Snapshot,
        container_path: Some(format!("/{key}")),
        name: None,
        description: None,
    };
    spec.captured_inputs = vec![captured_input(&input("local"), executor)];
    let ran = projection(&fixture, executor, Vec::new());
    let local = Placed::of(&ran, &spec).unwrap();
    assert_eq!(
        (local.node_id, local.mode, local.staged_bytes),
        (executor, "compute_to_data", 0)
    );

    let mut staged = captured_input(&input("remote"), holder);
    staged.bytes = 4_096;
    spec.captured_inputs.push(staged);
    let moved = Placed::of(&ran, &spec).unwrap();
    assert_eq!((moved.mode, moved.staged_bytes), ("data_to_compute", 4_096));

    let mut task = RunTaskOperation::new(sample_run(), ingress(), Box::new(start_task()));
    task.placed = Some(moved);
    let record = task.record(RunState::Running);
    assert_eq!(record.node_id, Some(executor));
    assert_eq!(record.mode.as_deref(), Some("data_to_compute"));
    assert_eq!(record.staged_bytes, Some(4_096));
}

// An output a task wrote on another node keeps its node and exact version, so its address
// reads it there and a later task stages it from there.
#[test]
fn reads_remote_outputs() {
    let fixture = Family::new([1; 32]);
    let run = sample_run();
    let executor = node(4);
    let bucket = run_area_bucket(run.group_id, executor);
    let key = format!("{}/tasks/run-7-1/work/out.txt", run.run_id);
    let version = Ulid::from_bytes([9; 16]);
    let written = OutputObject {
        node_id: executor,
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: version,
        execution_id: Ulid::from_bytes([6; 16]),
        container_path: "/uwe/work/out.txt".into(),
        size: 12,
        digest: Some("ab".into()),
    };
    let ran = projection(&fixture, executor, vec![written]);
    let mut task = RunTaskOperation::new(run.clone(), ingress(), Box::new(start_task()));
    task.placed = Placed::of(&ran, &fixture.spec());

    let file = task.output_file(&ChildSummary::from_projection(&ran), "out.txt");
    assert_eq!(file.node_id, Some(executor));
    assert_eq!(file.version_id, Some(version.to_string()));
    assert_eq!(
        (file.bucket.as_deref(), file.size),
        (Some(bucket.as_str()), Some(12))
    );
    let address = file_address(run.node_id, &file).unwrap();
    let target = resolve(&run, &address).unwrap();
    assert_eq!(target.remote(run.node_id), Some(executor));
    assert_eq!((target.key, target.version), (key, Some(version)));
    // A file of this node keeps the plain address.
    let mut here = file.clone();
    here.node_id = Some(run.node_id);
    assert!(!file_address(run.node_id, &here).unwrap().contains("node="));

    // A remote directory output expands into exact-version child addresses, including `.` for
    // the work root and paging when overlapping directories exceed the inline frame budget.
    let dir_child_key = format!("{}/tasks/run-7-1/work/outdir/sub/item.csv", run.run_id);
    let dir_child_ver = Ulid::from_bytes([11; 16]);
    let root_file_key = format!("{}/tasks/run-7-1/work/root.txt", run.run_id);
    let root_file_ver = Ulid::from_bytes([12; 16]);
    let dir_child = OutputObject {
        node_id: executor,
        bucket: bucket.clone(),
        key: dir_child_key.clone(),
        version_id: dir_child_ver,
        execution_id: Ulid::from_bytes([6; 16]),
        container_path: "/uwe/work/outdir/sub/item.csv".into(),
        size: 7,
        digest: None,
    };
    let root_file = OutputObject {
        node_id: executor,
        bucket: bucket.clone(),
        key: root_file_key.clone(),
        version_id: root_file_ver,
        execution_id: Ulid::from_bytes([6; 16]),
        container_path: "/uwe/work/root.txt".into(),
        size: 5,
        digest: None,
    };
    let dir_ran = projection(&fixture, executor, vec![dir_child, root_file]);
    let mut dir_task = RunTaskOperation::new(run.clone(), ingress(), Box::new(start_task()));
    dir_task.placed = Placed::of(&dir_ran, &fixture.spec());
    let summary = ChildSummary::from_projection(&dir_ran);
    let mut control_files = BTreeMap::new();
    control_files.insert("exit".into(), b"0\n".to_vec());
    control_files.insert(
        "manifest".into(),
        b"directory .\ndirectory ./outdir/\n".to_vec(),
    );
    let _ = dir_task.collected(summary, control_files).unwrap();
    let TaskStep::Pin(ref parts) = dir_task.step else {
        panic!("expected pinned task answer");
    };
    let (_, finished) = parts.as_ref();
    assert_eq!(finished.outputs.len(), 2);
    let dot_out = &finished.outputs[0];
    assert!(dot_out.directory && dot_out.address.is_empty() && dot_out.files_token.is_none());
    assert_eq!(dot_out.path, ".");
    assert_eq!(dot_out.files.len(), 2);
    assert_eq!(dot_out.files[0].path, "outdir/sub/item.csv");
    assert_eq!(dot_out.files[1].path, "root.txt");
    let sub_out = &finished.outputs[1];
    assert!(sub_out.directory && sub_out.address.is_empty() && sub_out.files_token.is_none());
    assert_eq!(sub_out.files.len(), 1);
    assert_eq!(sub_out.files[0].path, "sub/item.csv");
    assert_eq!(
        sub_out.files[0].address,
        address_on(&bucket, &dir_child_key, Some(dir_child_ver), executor)
    );

    // When inline budget is exceeded, remote directory entries spill into size-bounded pages.
    let summary = ChildSummary::from_projection(&dir_ran);
    let manifest = vec![(".".to_owned(), true), ("outdir".to_owned(), true)];
    let run_files: Vec<RunFile> = manifest
        .iter()
        .map(|(path, _)| dir_task.output_file(&summary, path))
        .collect();
    let (paged_outputs, writes) = dir_task
        .build_outputs(&summary, &manifest, &run_files, (1, 1))
        .unwrap();
    assert!(paged_outputs[0].files.is_empty());
    let token = paged_outputs[0].files_token.clone().expect("paged token");
    assert_eq!(writes.len(), 3, "2 pages for `.` plus 1 page for `outdir`");
    let mut reader = DirPageOperation::new(run.run_id, token.clone(), 0);
    let Next::Call(IoCall::Read(keys)) = reader.first().unwrap() else {
        panic!("expected read call");
    };
    let row0 = writes
        .iter()
        .find(|(_, k, _)| *k == keys[0].1)
        .expect("page 0 row")
        .2
        .clone();
    let Next::Done(HostRpcResult::TaskDirectoryPage { files, next_page }) =
        reader.next(IoResult::Values(vec![Some(row0)])).unwrap()
    else {
        panic!("expected directory page 0");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "outdir/sub/item.csv");
    assert_eq!(next_page, Some(1));
    let mut reader1 = DirPageOperation::new(run.run_id, token, 1);
    let Next::Call(IoCall::Read(keys1)) = reader1.first().unwrap() else {
        panic!("expected read call for page 1");
    };
    let row1 = writes
        .iter()
        .find(|(_, k, _)| *k == keys1[0].1)
        .expect("page 1 row")
        .2
        .clone();
    let Next::Done(HostRpcResult::TaskDirectoryPage { files, next_page }) =
        reader1.next(IoResult::Values(vec![Some(row1)])).unwrap()
    else {
        panic!("expected directory page 1");
    };
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "root.txt");
    assert_eq!(next_page, None);
}
