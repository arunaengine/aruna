//! Reports over recorded run logs: two real runs of one lineage go through a mocked node, then
//! a report session answers why, time, compare, changes, the run crate and the HTML report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage, WorkflowSource};
use aruna_workflow_contract::{ContractError, CoordinatorMode, PreflightKind, PreflightResult};

use crate::host::{FakeHost, MockNode, TaskMode};
use crate::lifecycle::{finished_run, log_pages, snakemake_start};
use crate::workflows::{SNAKEMAKE, source};

/// The pages of a first run and of a second run that resumes it.
async fn recorded_runs() -> (Vec<RunLogPage>, Vec<RunLogPage>) {
    let first = finished_run(
        "run-first",
        snakemake_start(),
        MockNode::new(TaskMode::Succeed),
    )
    .await;
    let first_pages = log_pages(&first);
    let mut start = snakemake_start();
    start.resume_lineage = Some("run-first".into());
    start.prior_log_pages = first_pages.clone();
    let second = finished_run("run-second", start, MockNode::new(TaskMode::Succeed)).await;
    // The node logs only what the second run adds; the first run is in its own log.
    let begun = (second.log.iter())
        .filter(|entry| matches!(entry, RunLogEntry::Begin { .. }))
        .count();
    assert_eq!(begun, 1);
    (first_pages, log_pages(&second))
}

fn snakefile() -> WorkflowSource {
    source(&[("Snakefile", SNAKEMAKE)])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_recorded_runs() {
    let (first, second) = recorded_runs().await;
    let lineage: Vec<RunLogPage> = first.iter().chain(&second).cloned().collect();
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Report, "run-second").await;

    let why = host
        .preflight(PreflightKind::ComputeWhy {
            run_id: "run-second".into(),
            task_id: 1,
            log_pages: lineage.clone(),
        })
        .await;
    let Ok(PreflightResult::ComputeWhy { why_json }) = why else {
        panic!("{why:?}");
    };
    assert!(why_json.is_some_and(|why| why.is_object()));

    let time = host
        .preflight(PreflightKind::ComputeTime {
            run_id: "run-first".into(),
            log_pages: first.clone(),
        })
        .await;
    let Ok(PreflightResult::ComputeTime { time_json }) = time else {
        panic!("{time:?}");
    };
    assert!(time_json.is_some_and(|time| time.is_object()));

    let compare = host
        .preflight(PreflightKind::ComputeCompare {
            first_run_id: "run-first".into(),
            second_run_id: "run-second".into(),
            first_log_pages: first.clone(),
            second_log_pages: second.clone(),
        })
        .await;
    let Ok(PreflightResult::ComputeCompare { comparison_json }) = compare else {
        panic!("{compare:?}");
    };
    assert!(comparison_json.is_some_and(|comparison| comparison.is_object()));

    let changes = host
        .preflight(PreflightKind::ComputeChanges {
            run_id: "run-first".into(),
            log_pages: first.clone(),
        })
        .await;
    let Ok(PreflightResult::ComputeChanges { changes_json }) = changes else {
        panic!("{changes:?}");
    };
    assert_eq!(changes_json["run"]["id"], "run-first");
    let images = changes_json["images"].as_array().expect("images");
    assert!(images.iter().any(|image| {
        image["ran"]
            .as_str()
            .is_some_and(|ran| ran.contains("@sha256:"))
    }));

    let crate_result = host
        .preflight(PreflightKind::BuildRunCrate {
            run_id: "run-first".into(),
            log_pages: first.clone(),
            base_uri: "https://w3id.org/aruna/data/".into(),
            source: snakefile(),
            entrypoint: "Snakefile".into(),
        })
        .await;
    let Ok(PreflightResult::BuildRunCrate {
        metadata_json,
        files,
    }) = crate_result
    else {
        panic!("{crate_result:?}");
    };
    assert!(metadata_json["@graph"].is_array(), "{metadata_json}");
    assert!(
        files.contains(&"ro-crate-metadata.json".to_owned()),
        "{files:?}"
    );
    assert!(files.contains(&"Snakefile".to_owned()), "{files:?}");

    let report = host
        .preflight(PreflightKind::BuildReport {
            run_id: "run-first".into(),
            log_pages: first.clone(),
        })
        .await;
    let Ok(PreflightResult::BuildReport { html }) = report else {
        panic!("{report:?}");
    };
    assert!(html.contains("<html"), "{}", &html[..html.len().min(200)]);
    assert!(html.contains("run-first"));
    host.shutdown().await.expect("a clean end");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_broken_logs() {
    let first = finished_run(
        "run-log",
        snakemake_start(),
        MockNode::new(TaskMode::Succeed),
    )
    .await;
    let pages = log_pages(&first);
    let (mut host, _) = FakeHost::greeted(CoordinatorMode::Report, "run-log").await;
    let unknown = host
        .preflight(PreflightKind::ComputeTime {
            run_id: "run-other".into(),
            log_pages: pages.clone(),
        })
        .await;
    assert!(
        matches!(unknown, Err(ContractError::NotFound { .. })),
        "{unknown:?}"
    );
    let mut reordered: Vec<RunLogEntry> =
        pages.iter().flat_map(|page| page.entries.clone()).collect();
    reordered.swap(1, 2);
    let reordered = vec![RunLogPage { entries: reordered }];
    let out_of_order = host
        .preflight(PreflightKind::BuildReport {
            run_id: "run-log".into(),
            log_pages: reordered,
        })
        .await;
    assert!(
        matches!(out_of_order, Err(ContractError::Invalid { .. })),
        "{out_of_order:?}"
    );
    let headless = vec![RunLogPage {
        entries: pages[0].entries[1..].to_vec(),
    }];
    let without_start = host
        .preflight(PreflightKind::ComputeTime {
            run_id: "run-log".into(),
            log_pages: headless,
        })
        .await;
    assert!(
        matches!(without_start, Err(ContractError::Invalid { .. })),
        "{without_start:?}"
    );
    host.shutdown().await.expect("a clean end");
}
