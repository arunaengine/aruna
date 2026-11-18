//! Tests the attempt decisions as calls and results: what a claim starts, skips and ends, what
//! stops an attempt, and when a running guest is revoked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_plugins::broker::HostError;
use serde_json::json;

use super::*;
use crate::plugin_host::worker::tests::created;
use crate::plugins::fixtures::{self, config_result, is_child, node};
use crate::plugins::rows::tests::child_result;
use crate::plugins::runs::tests::queued;

fn config() -> Arc<RealmConfigDocument> {
    Arc::new(fixtures::config(&fixtures::install(), json!({})))
}

fn claim(active: BTreeMap<Ulid, Ulid>) -> ClaimRuns {
    ClaimRuns::new(config(), node(3), active, 50)
}

fn due_run(seq: u64) -> PluginRun {
    queued(fixtures::install().install_id, seq)
}

fn queue_of(runs: &[&PluginRun]) -> IoResult {
    let rows: Vec<(Ulid, Ulid)> = (runs.iter())
        .map(|run| (run.install_id, run.run_id))
        .collect();
    child_result(Ok::<_, RunError>(rows))
}

fn read(run: &PluginRun) -> IoResult {
    child_result(Ok::<_, RunError>(run.clone()))
}

fn saved() -> IoResult {
    child_result(Ok::<(), StoreError>(()))
}

fn claimed(next: Result<Next<ClaimedRuns>, RunError>) -> ClaimedRuns {
    let Ok(Next::Done(claimed)) = next else {
        panic!("the claim should be over");
    };
    claimed
}

#[test]
fn claim_marks_due() {
    let run = due_run(1);
    let mut claim = claim(BTreeMap::new());
    assert!(is_child(&claim.first()));
    assert!(is_child(&claim.next(queue_of(&[&run]))));
    assert!(is_child(&claim.next(read(&run))));
    let done = claimed(claim.next(saved()));
    assert_eq!(done.failure, None);
    assert_eq!(done.runs[0].state, RunState::Running);
    assert_eq!(done.runs[0].attempts, 1);
}

#[test]
fn claim_leaves_future() {
    let mut run = due_run(1);
    run.due_ms = 51;
    let mut claim = claim(BTreeMap::new());
    claim.first().unwrap();
    claim.next(queue_of(&[&run])).unwrap();
    assert!(claimed(claim.next(read(&run))).runs.is_empty());
}

#[test]
fn claim_respects_bound() {
    let run = due_run(1);
    let busy = (0..RUNS_PARALLEL as u128)
        .map(|seq| (Ulid::from_parts(9, seq), run.install_id))
        .collect();
    let mut claim = claim(busy);
    claim.first().unwrap();
    assert!(claimed(claim.next(queue_of(&[&run]))).runs.is_empty());
}

#[test]
fn claim_skips_running() {
    let run = due_run(1);
    let mut claim = claim(BTreeMap::from([(run.run_id, run.install_id)]));
    claim.first().unwrap();
    assert!(claimed(claim.next(queue_of(&[&run]))).runs.is_empty());
}

#[test]
fn claim_ends_orphans() {
    let run = queued(Ulid::from_parts(9, 9), 1);
    let mut claim = claim(BTreeMap::new());
    claim.first().unwrap();
    assert!(is_child(&claim.next(queue_of(&[&run]))));
    assert!(is_child(&claim.next(read(&run))));
    let open = child_result(Ok::<Option<String>, StoreError>(None));
    assert!(is_child(&claim.next(open)));
    assert!(claimed(claim.next(saved())).runs.is_empty());
}

#[test]
fn claim_keeps_earlier() {
    let (first, second) = (due_run(1), due_run(2));
    let mut claim = claim(BTreeMap::new());
    claim.first().unwrap();
    claim.next(queue_of(&[&first, &second])).unwrap();
    claim.next(read(&first)).unwrap();
    assert!(is_child(&claim.next(saved())));
    let failed = child_result(Err::<PluginRun, _>(RunError::NotFound));
    let done = claimed(claim.next(failed));
    assert_eq!(done.runs.len(), 1);
    assert_eq!(done.failure, Some(RunError::NotFound));
}

fn place() -> (RealmId, NodeId) {
    (fixtures::realm(), node(3))
}

#[test]
fn begin_reads_install() {
    let mut begin = BeginAttempt::new(place(), &due_run(1));
    assert!(is_child(&begin.first()));
    let Ok(Next::Done(start)) = begin.next(config_result((*config()).clone())) else {
        panic!("a timer run needs no check");
    };
    assert_eq!(start.install, fixtures::install());
    let mut gone = BeginAttempt::new(place(), &queued(Ulid::from_parts(9, 9), 1));
    gone.first().unwrap();
    let removed = Ended::Cancelled(INSTALL_REMOVED.to_string());
    assert_eq!(gone.next(config_result((*config()).clone())), Err(removed));
}

fn event_begin() -> BeginAttempt {
    let mut install = fixtures::install();
    let whole = format!("/{}/g/{}/data/**", fixtures::realm(), install.group_id);
    install.grants.insert("read".to_string(), vec![whole]);
    let (event, _) = created(&install, "D1");
    let mut run = queued(install.install_id, 1);
    run.trigger = RunTrigger::Event(event);
    let mut begin = BeginAttempt::new(place(), &run);
    begin.first().unwrap();
    let checked = begin.next(config_result(fixtures::config(&install, json!({}))));
    assert!(is_child(&checked));
    begin
}

#[test]
fn begin_checks_events() {
    let check = |kind, ended: Ended| {
        let failed = CallError(HostError::new(kind, "no"));
        let answer = child_result(Err::<PluginInstall, _>(failed));
        assert_eq!(event_begin().next(answer), Err(ended));
    };
    check(ErrorKind::Unavailable, Ended::Retry("no".into()));
    check(ErrorKind::Denied, Ended::Cancelled("no".into()));
    let passed = child_result(Ok::<_, CallError>(fixtures::install()));
    assert!(matches!(event_begin().next(passed), Ok(Next::Done(_))));
}

#[test]
fn finish_writes_end() {
    let mut finish = FinishRun::new(due_run(1), Ended::Output(None), false, 50);
    assert!(is_child(&finish.first()));
    let open = child_result(Ok::<Option<String>, StoreError>(None));
    assert!(is_child(&finish.next(open)));
    assert_eq!(finish.run.state, RunState::Succeeded);
    assert_eq!(finish.next(saved()), Ok(Next::Done(())));
}

#[test]
fn revokes_stopped_installs() {
    let mut install = fixtures::install();
    let id = install.install_id;
    let reason = |install: &PluginInstall, local| {
        revocation(&fixtures::config(install, json!({})), local, id)
    };
    assert_eq!(reason(&install, node(3)), None);
    assert_eq!(reason(&install, node(9)), lost_reason(&config(), node(9)));
    install.paused = true;
    assert_eq!(reason(&install, node(3)), Some("the install was paused"));
    install.state = InstallState::Removing;
    assert_eq!(reason(&install, node(3)), Some(INSTALL_REMOVED));
    let other = revocation(&config(), node(3), Ulid::from_parts(9, 9));
    assert_eq!(other, Some(INSTALL_REMOVED));
}
