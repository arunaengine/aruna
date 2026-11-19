//! Tests of the staging lanes: their pure plans, their window and whole imports over a listing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::staging::initial_checkpoint;
use crate::jobs::staging::pure_tests::{child_answer, test_connector, test_run};
use crate::tests::staging::spec;
use aruna_core::events::GroupResult;
use aruna_core::structs::execution::job::{StagingJobError, StagingJobPrefix};

// Lanes only run when no two prefixes can write the same key.
#[test]
fn nested_targets_sequential() {
    assert!(disjoint(&spec(&["a", "b", "ab"])));
    assert!(!disjoint(&spec(&["a", "a/b"])));
    assert!(!disjoint(&spec(&["", "b"])));
}

// Open lanes count in the job's progress, and their prefixes read as not started.
#[test]
fn merged_counts_lanes() {
    let mut main = initial_checkpoint(&spec(&["a", "b", "c"]));
    main.prefixes_done = 3;
    main.items_current = 10;
    let mut lane = main.clone();
    lane.items_current = 5;
    lane.items_failed = 1;
    lane.errors = vec![StagingJobError {
        source_path: "src/b/x".to_string(),
        target_key: "b/x".to_string(),
        error: "denied".to_string(),
    }];

    let view = merged(main, vec![(4, lane.clone(), None), (5, lane, None)]);

    assert_eq!((view.prefixes_done, view.items_current), (1, 20));
    assert_eq!((view.items_failed, view.errors.len()), (2, 2));
}

/// The split points a plan finds over sorted `keys`.
fn split_of(keys: &[&str], source_path: &str, parts: usize) -> Vec<String> {
    let mut plan = SplitPlan::new(source_path, parts);
    let mut lower = plan.begin();
    while let Some(at) = lower {
        let first = keys
            .iter()
            .find(|key| **key >= at.as_str() && key.starts_with(source_path));
        lower = plan.answer(first.map(|key| key.to_string()));
    }
    plan.splits()
}

// The splits of a prefix refine the distinct characters after the start its keys share.
#[test]
fn splits_shared_start() {
    let keys = ["p/SAMD1", "p/SAMD2", "p/SAMEA1", "p/SAMN1", "p/SAMN2"];
    let splits = split_of(&keys, "p/", 4);
    assert_eq!(splits, ["p/SAMD2", "p/SAME", "p/SAMN1"]);
    let walk = StagingWalk {
        source_path: "p/".to_string(),
        target_prefix: "t".to_string(),
        cursor: None,
        queue_row: None,
        from: None,
    };
    assert_eq!(target_of(&walk, &splits[2]).as_deref(), Some("t/SAMN1"));
}

// Keys that all start with `SAM` and branch only into `SAMD` and `SAME` still split into the
// full number of lanes, inside both branches.
#[test]
fn splits_sample_space() {
    let keys: Vec<String> = (["SAMD", "SAMEA"].iter())
        .flat_map(|sample| (0..300).map(move |index| format!("p/{sample}{index:08}")))
        .collect();
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let splits = split_of(&keys, "p/", 4);
    assert_eq!(splits.len(), 3, "{splits:?}");
    assert!(splits.iter().any(|split| split.starts_with("p/SAMD")));
    assert!(splits.iter().any(|split| split.starts_with("p/SAME")));
}

// A single key gives no split, and the plan stops once its probes run out.
#[test]
fn plan_needs_forks() {
    assert!(split_of(&["p/only"], "p/", 4).is_empty());
    assert!(split_of(&[], "p/", 4).is_empty());
    assert!(SplitPlan::new("p/", 1).begin().is_none());
}

fn stops(cancelled: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown: false,
    };
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

fn walking_lane() -> OpenLane {
    let mut checkpoint = initial_checkpoint(&test_run().spec);
    checkpoint.walk = Some(StagingWalk {
        source_path: "a/".to_string(),
        target_prefix: "ta".to_string(),
        cursor: None,
        queue_row: None,
        from: None,
    });
    OpenLane {
        checkpoint,
        upper: None,
        ahead: None,
    }
}

// A lane page walks only while the job goes on and reports the items the page added.
#[test]
fn lane_page_reports() {
    let mut page = LanePageOperation::new(test_run(), 4, walking_lane());
    assert_eq!(page.first(), Ok(Next::Call(job_call(JobEffect::Stops))));
    assert!(matches!(
        page.next(stops(false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let mut checkpoint = walking_lane().checkpoint;
    checkpoint.items_current = 3;
    let walked = WalkedPage {
        checkpoint,
        ahead: None,
    };
    let report = JobReport {
        advance: 3,
        checkpoint: true,
        ..JobReport::default()
    };
    assert_eq!(
        page.next(child_answer(Ok::<_, PageFailure>(walked))),
        Ok(Next::Call(job_call(JobEffect::Report(report))))
    );
    let reported = IoResult::Event(Box::new(Event::Job(JobEvent::Reported)));
    assert!(matches!(
        page.next(reported),
        Ok(Next::Done(LanePage::Walked(_)))
    ));

    let mut stopped = LanePageOperation::new(test_run(), 4, walking_lane());
    stopped.first().unwrap();
    assert_eq!(stopped.next(stops(true)), Ok(Next::Done(LanePage::Stopped)));
}

fn opened_window(run: StagingRun) -> LanesOperation {
    let main = initial_checkpoint(&run.spec);
    let mut lanes = LanesOperation::new(run, main);
    assert!(matches!(lanes.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let page = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert!(matches!(
        lanes.next(page),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    lanes
}

fn member_event(index: usize, event: Event) -> IoResult {
    IoResult::Group(GroupEvent::Member {
        index,
        result: GroupResult::Done(Box::new(event)),
    })
}

fn opened(outcome: Result<Opened, PageFailure>) -> Event {
    Event::Child(aruna_core::calls::ChildOutcome::new(outcome))
}

// A fresh job opens its first prefix, runs the prefix's lane and folds it when it ends.
#[test]
fn window_runs_lanes() {
    let run = test_run();
    let mut lanes = opened_window(run.clone());
    let reported = IoResult::Event(Box::new(Event::Job(JobEvent::Reported)));
    let Ok(Next::Call(IoCall::Group(group))) = lanes.next(reported) else {
        panic!("expected the window");
    };
    assert_eq!(group.members.len(), 1, "only the opener starts");
    let lane = (0, walking_lane().checkpoint, None);
    let answer = opened(Ok(Opened::Lanes {
        lanes: vec![lane],
        prefixes_done: 1,
    }));
    let Ok(Next::Call(IoCall::Group(group))) = lanes.next(member_event(0, answer)) else {
        panic!("expected the lane page and the next opener");
    };
    assert_eq!(group.members.len(), 2);
    assert_eq!(lanes.members[1], Member::Page(0));
    assert_eq!(lanes.members[2], Member::Opener);

    let mut done = walking_lane();
    done.checkpoint.walk = None;
    done.checkpoint.items_current = 2;
    let page = Event::Child(aruna_core::calls::ChildOutcome::new(Ok::<_, PageFailure>(
        LanePage::Walked(done),
    )));
    assert_eq!(lanes.next(member_event(1, page)), Ok(Next::Wait));
    assert_eq!(lanes.main.items_current, 2);
    assert_eq!(lanes.finished, [0]);

    // No prefix is left; the finished lane's rows go with the job's checkpoint.
    let none = opened(Ok(Opened::Lanes {
        lanes: Vec::new(),
        prefixes_done: 1,
    }));
    let Ok(Next::Call(IoCall::Group(group))) = lanes.next(member_event(2, none)) else {
        panic!("expected the write of the job's rows");
    };
    assert_eq!(group.members.len(), 1);
    assert_eq!(lanes.members[3], Member::Writer);
    let written = Event::Child(aruna_core::calls::ChildOutcome::new(Ok::<
        (),
        JobMutationError,
    >(())));
    assert_eq!(lanes.next(member_event(3, written)), Ok(Next::Wait));
    let Ok(Next::Done(end)) = lanes.next(IoResult::Group(GroupEvent::Closed)) else {
        panic!("expected the end of the lanes");
    };
    assert!(end.finished);
    assert_eq!(end.main.items_current, 2);
}

// A failed page starts nothing new, lets the window close and ends the run with its failure.
#[test]
fn failure_ends_window() {
    let mut lanes = opened_window(test_run());
    let reported = IoResult::Event(Box::new(Event::Job(JobEvent::Reported)));
    lanes.next(reported).unwrap();
    lanes.opening = false;
    lanes.members.push(Member::Page(4));
    lanes.pages = 1;
    let failed = Event::Child(aruna_core::calls::ChildOutcome::new(Err::<LanePage, _>(
        PageFailure::Retry("listing failed".to_string()),
    )));
    assert_eq!(lanes.next(member_event(1, failed)), Ok(Next::Wait));
    assert_eq!(
        lanes.next(IoResult::Group(GroupEvent::Closed)),
        Err(PageFailure::Retry("listing failed".to_string()))
    );
}

// The opener writes one lane per part, each bounded by the next part's first key.
#[test]
fn opener_writes_lanes() {
    let mut spec = (*test_run().spec).clone();
    spec.prefixes = vec![StagingJobPrefix {
        source_prefix: "p".to_string(),
        target_prefix: "t".to_string(),
    }];
    let run = StagingRun {
        spec: std::sync::Arc::new(spec.clone()),
        ..test_run()
    };
    let mut open = OpenPrefixOperation::new(run, initial_checkpoint(&spec), 2);
    assert_eq!(open.first(), Ok(Next::Call(job_call(JobEffect::Stops))));
    assert!(matches!(
        open.next(stops(false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let listed = |path: Option<&str>, open: &mut OpenPrefixOperation| {
        let page = crate::staging::list_source::ListStagingResult {
            entries: path.into_iter().map(entry).collect(),
            next_cursor: None,
            connector: test_connector(),
            secret_fingerprint: None,
        };
        open.next(child_answer(Ok::<_, ListStagingError>(page)))
    };
    assert!(matches!(listed(Some("p/a1"), &mut open), Ok(Next::Call(_))));
    assert!(matches!(listed(Some("p/b1"), &mut open), Ok(Next::Call(_))));
    assert!(matches!(
        listed(None, &mut open),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let OpenStep::Commit(lanes) = &open.step else {
        panic!("expected the write of the lanes");
    };
    assert_eq!(lanes.len(), 2);
    assert_eq!(lanes[0].2.as_deref(), Some("t/b"));
    assert_eq!(lanes[1].2, None);
    let written = child_answer(Ok::<(), JobMutationError>(()));
    let Ok(Next::Done(Opened::Lanes { prefixes_done, .. })) = open.next(written) else {
        panic!("expected the opened lanes");
    };
    assert_eq!(prefixes_done, 1);
}

fn entry(path: &str) -> aruna_core::structs::execution::source_access::SourceEntry {
    aruna_core::structs::execution::source_access::SourceEntry {
        name: path.to_string(),
        path: path.to_string(),
        kind: aruna_core::structs::execution::source_access::SourceEntryKind::File,
        size: Some(1),
        modified: None,
        etag: None,
        stat: None,
    }
}
