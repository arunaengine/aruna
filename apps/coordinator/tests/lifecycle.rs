//! Whole runs against a mocked node: the run start, image pins, the run log, task calls with
//! wrappers, uploads and step settings, live events, the end of the run and cancellation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_workflow_contract::message::{RunStartPayload, TaskState};
use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage, WorkflowLanguage};
use aruna_workflow_contract::{CoordinatorMode, HostToCoordinator, LiveWorkflowEvent};
use serde_json::{Map, Value, json};

use crate::host::{FakeHost, MockNode, TaskMode, drive_run};
use crate::workflows::{COMPOSED, REVERSE_TOOL, SHOUT_TOOL, SNAKEMAKE, source};

pub fn snakemake_start() -> RunStartPayload {
    RunStartPayload {
        source: source(&[("Snakefile", SNAKEMAKE)]),
        entrypoint: "Snakefile".into(),
        language: Some(WorkflowLanguage::Snakemake),
        inputs: Value::Null,
        params: Map::new(),
        pinned_images: BTreeMap::new(),
        cache_namespace: Some("group:test".into()),
        resume_lineage: None,
        secret_names: Vec::new(),
        network_isolated: true,
        max_pending: Some(4),
        prior_log_pages: Vec::new(),
        step_settings: BTreeMap::new(),
    }
}

/// Runs a workflow to its end against a node that finishes every task, and gives the node.
pub async fn finished_run(run_id: &str, start: RunStartPayload, node: MockNode) -> MockNode {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Run, run_id).await;
    let mut node = node;
    host.send(&HostToCoordinator::RunStart(start)).await;
    let finished = drive_run(&mut host, &mut node, None).await;
    assert!(finished.success, "the run failed: {:?}", finished.error);
    assert!(!finished.stopped);
    assert_eq!(finished.final_summary_json["run_id"], run_id);
    // A run session ends after its run.
    assert!(host.receive().await.is_none());
    host.finished().await.expect("a clean end");
    node
}

/// The run log of a node as pages of three entries, as the node pages it.
pub fn log_pages(node: &MockNode) -> Vec<RunLogPage> {
    node.pages(3)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runs_mocked_tasks() {
    let node = MockNode::new(TaskMode::Succeed);
    let node = finished_run("run-life", snakemake_start(), node).await;
    let steps: Vec<&str> = node
        .tasks
        .iter()
        .map(|task| task.step_id.as_str())
        .collect();
    assert!(
        steps.contains(&"greet") && steps.contains(&"collect"),
        "{steps:?}"
    );
    for task in &node.tasks {
        let wrapper = String::from_utf8_lossy(&task.command_wrapper_bytes);
        assert!(
            wrapper.starts_with("#!/bin/sh\n# The uwe task wrapper."),
            "{wrapper}"
        );
        assert!(task.name.starts_with("run-life-"), "{}", task.name);
        assert_eq!(task.attempt, 1);
        assert!(
            !task.network_allowed,
            "an isolated run gives no task the network"
        );
        assert!(
            task.image
                .as_deref()
                .is_some_and(|image| image.contains("@sha256:"))
        );
        assert!(!task.task_key_seed.is_empty());
        assert!(task.spec_json["format"].is_u64(), "{}", task.spec_json);
    }
    // The outputs of the greetings reach the collecting task through uploads.
    let collect = (node.tasks.iter())
        .find(|task| task.step_id == "collect")
        .expect("collect");
    let uploaded: Vec<_> = (collect.staged_files.iter())
        .filter_map(|file| file.address.as_deref())
        .collect();
    assert_eq!(uploaded.len(), 2, "{:?}", collect.staged_files);
    assert!(
        uploaded
            .iter()
            .all(|address| address.starts_with("aruna-staging://"))
    );
    assert!(
        node.pinned
            .iter()
            .any(|image| image.contains("debian:stable-slim"))
    );
    // The run log starts with the run and numbers its changes from 1 without gaps.
    assert!(matches!(node.log.first(), Some(RunLogEntry::Begin { .. })));
    let seqs: Vec<u64> = (node.log.iter())
        .filter_map(|entry| match entry {
            RunLogEntry::Change { seq, .. } => Some(*seq),
            RunLogEntry::Begin { .. } => None,
        })
        .collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    let ended = (node.log.iter()).any(|entry| {
        matches!(entry, RunLogEntry::Change { change_json, .. } if change_json["change"] == "ended")
    });
    assert!(ended, "the log records the end of the run");
    // Live events come in order: the start, task changes with counts, the end.
    assert!(node.event_seqs.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(matches!(
        node.events.first(),
        Some(LiveWorkflowEvent::RunStarted { .. })
    ));
    assert!(matches!(
        node.events.last(),
        Some(LiveWorkflowEvent::RunEnded { error: None, .. })
    ));
    let finished = (node.events.iter()).any(|event| {
        matches!(event, LiveWorkflowEvent::TaskChanged(change) if change.state == TaskState::Finished)
    });
    assert!(finished);
    let counts = (node.events.iter()).rev().find_map(|event| match event {
        LiveWorkflowEvent::Progress(counts) => Some(*counts),
        _ => None,
    });
    let counts = counts.expect("progress counts");
    assert_eq!((counts.running, counts.failed), (0, 0));
    assert_eq!(counts.finished, counts.requested);
}

fn composed_start(composed: &str) -> RunStartPayload {
    let mut start = snakemake_start();
    start.source = source(&[
        ("twice.uwe.yaml", composed),
        ("tools/reverse.yaml", REVERSE_TOOL),
        ("tools/shout.yaml", SHOUT_TOOL),
    ]);
    start.entrypoint = "twice.uwe.yaml".into();
    start.language = None;
    start.network_isolated = false;
    start.inputs = json!({"text": INPUT});
    start
}

const INPUT: &str = "aruna://object/input-text";

fn input_node() -> MockNode {
    let mut node = MockNode::new(TaskMode::Succeed);
    node.files
        .insert(INPUT.into(), b"first line\nsecond line\n".to_vec());
    node
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applies_step_settings() {
    let node = finished_run("run-composed", composed_start(COMPOSED), input_node()).await;
    let task = |step: &str| {
        (node.tasks.iter())
            .find(|task| task.step_id == step)
            .unwrap_or_else(|| panic!("a task of {step}"))
    };
    let first = task("reverse");
    let settings = first
        .step_settings
        .as_ref()
        .expect("the settings of step first");
    assert_eq!(settings.resources.cpus, Some(2.0));
    assert_eq!(first.resources.cpus, 2);
    assert_eq!(first.resources.memory_bytes, Some(1536 << 20));
    assert!(!first.network_allowed, "step first runs without network");
    assert_eq!(first.site, None);
    let second = task("shout");
    assert_eq!(second.site.as_deref(), Some("giessen"));
    assert!(second.network_allowed);
    // The node gives the first step the input itself; the second gets the first's output.
    let direct: Vec<_> = first
        .direct_inputs
        .iter()
        .map(|input| &input.address)
        .collect();
    assert_eq!(direct, [INPUT]);
    assert_eq!(
        first.direct_inputs[0].aruna_id.as_deref(),
        Some("input-text")
    );
    assert!(first.staged_files.is_empty(), "{:?}", first.staged_files);
    let staged: Vec<_> = second
        .staged_files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    assert_eq!(staged, ["/tmp/in/text.txt"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_ambiguous_settings() {
    let shared = COMPOSED
        .replace("tool: shout", "tool: reverse")
        .replace("second/shouted", "second/reversed");
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Run, "run-shared").await;
    let mut node = input_node();
    host.send(&HostToCoordinator::RunStart(composed_start(&shared)))
        .await;
    let finished = drive_run(&mut host, &mut node, None).await;
    assert!(!finished.success);
    let error = finished.error.unwrap_or_default();
    assert!(error.contains("distinct tools"), "{error}");
    assert!(
        node.tasks.is_empty(),
        "no task starts with settings it cannot place"
    );
    assert!(host.receive().await.is_none());
    host.finished().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancels_running_tasks() {
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Run, "run-cancel").await;
    let mut node = MockNode::new(TaskMode::Hold);
    host.send(&HostToCoordinator::RunStart(snakemake_start()))
        .await;
    let finished = drive_run(&mut host, &mut node, Some(1)).await;
    assert!(finished.stopped, "{finished:?}");
    assert!(!finished.success);
    assert!(!node.tasks.is_empty());
    let started: Vec<&str> = node.tasks.iter().map(|task| task.name.as_str()).collect();
    assert!(
        !node.cancelled.is_empty(),
        "the node is asked to cancel the running tasks"
    );
    assert!(
        node.cancelled
            .iter()
            .all(|name| started.contains(&name.as_str()))
    );
    assert_eq!(node.stopped_runs, ["run-cancel"]);
    assert!(host.receive().await.is_none());
    host.finished().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolates_step_network() {
    let composed = COMPOSED
        .replace("      execution:\n        network: false\n", "")
        .replace(
            "      placement:\n        site: giessen\n",
            "      placement:\n        site: giessen\n      execution:\n        network: true\n",
        );
    let mut start = composed_start(&composed);
    start.network_isolated = true;
    let node = finished_run("run-net", start, input_node()).await;
    let first = (node.tasks.iter())
        .find(|task| task.step_id == "reverse")
        .expect("reverse");
    let second = (node.tasks.iter())
        .find(|task| task.step_id == "shout")
        .expect("shout");
    assert!(
        !first.network_allowed,
        "isolated run keeps default steps offline"
    );
    assert!(second.network_allowed, "step opting into network gets it");
    let targets: Vec<&str> = node
        .delivered
        .iter()
        .map(|(_, path)| path.as_str())
        .collect();
    assert!(
        targets.iter().any(|path| path.ends_with("shouted.txt")),
        "declared workflow outputs are delivered through host: {targets:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivers_declared_outputs() {
    let node = finished_run("run-deliver", composed_start(COMPOSED), input_node()).await;
    assert_eq!(node.delivered.len(), 1, "{:?}", node.delivered);
    assert_eq!(node.delivered[0].1, "shouted.txt");
    assert!(
        node.delivered[0].0.starts_with("aruna://object/"),
        "task output is delivered by reference: {:?}",
        node.delivered
    );

    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Run, "run-deliver-fail").await;
    let mut failing = input_node();
    failing.fail_delivery = Some("output bucket denied".into());
    host.send(&HostToCoordinator::RunStart(composed_start(COMPOSED)))
        .await;
    let finished = drive_run(&mut host, &mut failing, None).await;
    assert!(!finished.success, "delivery refusal fails the run");
    let error = finished.error.unwrap_or_default();
    assert!(error.contains("output bucket denied"), "{error}");
    assert!(host.receive().await.is_none());
    host.finished().await.expect("a clean end");
}
