//! Tests the settle decisions of one plugin task row as calls and results: absence only from
//! every target's answer, settlement from terminal states, and the decision per row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{OutputSet, ProjectedExecution};
use aruna_core::structs::identity::realm::RealmId;

use super::*;
use crate::jobs::lifecycle::ids::SubmissionRequest;
use crate::plugins::fixtures::is_child;
use crate::plugins::rows::encode;
use crate::plugins::rows::tests::{child_result, values_result};
use crate::plugins::screen::tests::install;
use crate::plugins::work::WorkKind;
use crate::tests::records::{Family, node, payload};

fn family() -> JobFamilyId {
    let family = Family::new([1; 32]);
    JobFamilyId {
        submission_id: family.submission_id,
        request_digest: family.request_digest,
    }
}

fn alias() -> JobId {
    Family::new([1; 32]).spec().job_id
}

#[test]
fn absence_needs_all() {
    let (targets, none) = ([node(1), node(2)], BTreeSet::new());
    let absent = || (node(1), ClosureAnswer::Absent);
    // One node not decided yet, or not answered at all, keeps the row.
    let undecided = (node(2), ClosureAnswer::Undecided);
    let waiting = absence(&targets, &[absent(), undecided], None, &none);
    assert_eq!(waiting, Absence::Waiting);
    assert_eq!(
        absence(&targets, &[absent()], None, &none),
        Absence::Waiting
    );
    let both = [absent(), (node(2), ClosureAnswer::Absent)];
    assert_eq!(absence(&targets, &both, None, &none), Absence::Proven);
    // The receiver's own claim wins over every answer.
    let claimed = Absence::Claimed {
        family: family(),
        job_id: alias(),
    };
    let local = Some((family(), alias()));
    assert_eq!(absence(&targets, &both, local, &none), claimed);
    let remote = ClosureAnswer::Claimed {
        family: family(),
        job_id: alias(),
    };
    let answers = [absent(), (node(2), remote)];
    assert_eq!(absence(&targets, &answers, None, &none), claimed);
    assert_eq!(absence(&[], &[], None, &none), Absence::Waiting);
}

#[test]
fn departed_target_absent() {
    // A target that left the realm never answers; it counts as absent.
    let targets = [node(1), node(2)];
    let gone = BTreeSet::from([node(2)]);
    let absent = (node(1), ClosureAnswer::Absent);
    assert_eq!(absence(&targets, &[absent], None, &gone), Absence::Proven);
    let all = BTreeSet::from([node(1), node(2)]);
    assert_eq!(absence(&targets, &[], None, &all), Absence::Proven);
    assert_eq!(absence(&[], &[], None, &all), Absence::Waiting);
}

#[test]
fn settles_terminal_states() {
    let projection = |state| JobProjection {
        submission_id: family().submission_id,
        request_digest: family().request_digest,
        canonical_job_id: alias(),
        aliases: vec![alias()],
        state,
        canonical_execution_id: None,
        executions: Vec::<ProjectedExecution>::new(),
        outputs: OutputSet::new(Vec::new()).unwrap(),
        cancel_requested: false,
        expiry_reason: None,
    };
    for waiting in [
        LogicalJobState::Queued,
        LogicalJobState::Running,
        LogicalJobState::Indeterminate,
    ] {
        assert_eq!(final_of(&projection(waiting)), None, "{waiting:?}");
    }
    let state = |state| final_of(&projection(state)).map(|summary| summary.state);
    let succeeded = state(LogicalJobState::Succeeded);
    assert_eq!(succeeded, Some(FinalState::Succeeded));
    assert_eq!(state(LogicalJobState::Failed), Some(FinalState::Failed));
    let cancelled = state(LogicalJobState::Cancelled);
    assert_eq!(cancelled, Some(FinalState::Cancelled));
}

fn row(state: WorkState) -> WorkRow {
    let ids = (Ulid::nil(), Ulid::from_parts(2, 2));
    let mut row = WorkRow::reserved(&install(), WorkKind::Recipe, ids, 1);
    row.state = state;
    row
}

/// A task row that sent its request to `targets` with a deadline of 1 000.
fn sent(targets: Vec<NodeId>) -> TaskRow {
    let request = SubmissionRequest {
        created_by: row(WorkState::Reserved).account,
        spec: payload(),
        scope: SubmissionScope::Keyed("plugin-work/2".into()),
        retention_ms: 1,
        ingress_node_id: node(1),
        captured_inputs: Vec::new(),
        output_policies: Vec::new(),
        input_pages: None,
        admit_by: Some(1_000),
    };
    TaskRow {
        admit_by: 1_000,
        targets,
        request: Some(request),
        digest: Some([7; 32]),
        ..TaskRow::default()
    }
}

/// A settle of `row` at `now_ms` that just read `task`; the install is active unless `state`.
fn settle(
    row: WorkRow,
    task: Option<TaskRow>,
    (now_ms, state): (u64, InstallState),
) -> (SettleTask, SettleNext) {
    let mut config = RealmConfigDocument::default_for_realm(RealmId([3; 32]), Vec::new());
    let mut install = install();
    install.state = state;
    config.installs.push(install.clone());
    let mut settle = SettleTask::new(Arc::new(config), node(1), (install.install_id, row));
    settle.stamp(now_ms);
    assert!(matches!(settle.first(), Ok(Next::Call(IoCall::Read(_)))));
    let task = task.map(|task| encode(&task).unwrap());
    let next = settle.next(values_result(vec![task]));
    (settle, next)
}

fn page(rows: Vec<(Key, Value)>) -> IoResult {
    IoResult::Page { rows, next: None }
}

#[test]
fn decides_from_rows() {
    let active = InstallState::Active;
    // A settled row stays while its install is kept, and goes with a removed one.
    let (_, kept) = settle(row(WorkState::Settled), None, (5, active));
    assert_eq!(
        kept,
        Ok(Next::Done(Settled {
            changed: false,
            followup: Followup::Nothing
        }))
    );
    let (settle_removed, next) = settle(row(WorkState::Settled), None, (5, InstallState::Removing));
    assert!(is_child(&next));
    assert_eq!(settle_removed.phase, SettlePhase::Change);
    // An unsent reservation goes once no call can still be running.
    let (_, fresh) = settle(row(WorkState::Reserved), None, (5, active));
    assert!(matches!(fresh, Ok(Next::Done(_))));
    let (_, stale) = settle(
        row(WorkState::Reserved),
        None,
        (ADMIT_WINDOW_MS + 2, active),
    );
    assert!(is_child(&stale));
    // A removed install's dispatched task is cancelled at its holders.
    let mut dispatched = row(WorkState::Dispatched);
    dispatched.job = Some(alias());
    let mut task = sent(vec![node(2)]);
    task.family = Some(family());
    let (_, cancel) = settle(dispatched, Some(task), (5, InstallState::Removing));
    let followup = Followup::Cancel {
        alias: alias(),
        family: family(),
    };
    assert_eq!(
        cancel,
        Ok(Next::Done(Settled {
            changed: false,
            followup
        }))
    );
}

#[test]
fn resends_without_claim() {
    let task = Some(sent(vec![node(2)]));
    let (mut settle, next) = settle(row(WorkState::Reserved), task, (500, InstallState::Active));
    assert!(matches!(next, Ok(Next::Call(IoCall::Page { .. }))));
    let resend = Settled {
        changed: false,
        followup: Followup::Resend,
    };
    assert_eq!(settle.next(page(Vec::new())), Ok(Next::Done(resend)));
}

#[test]
fn proves_absence_late() {
    // Past the deadline and its margin, every target is asked; a departed one counts as absent.
    let late = 1_000 + CLOCK_MARGIN_MS + 1;
    let task = Some(sent(vec![node(2)]));
    let (mut settle, next) = settle(row(WorkState::Reserved), task, (late, InstallState::Active));
    assert!(matches!(next, Ok(Next::Call(IoCall::Page { .. }))));
    assert!(is_child(&settle.next(page(Vec::new()))));
    let ended = child_result(Ok::<_, StoreError>(Changed {
        settled: true,
        marked: None,
    }));
    let settled = Settled {
        changed: true,
        followup: Followup::Nothing,
    };
    assert_eq!(settle.next(ended), Ok(Next::Done(settled)));
}

#[test]
fn waits_within_margin() {
    let within = 1_000 + CLOCK_MARGIN_MS;
    let task = Some(sent(vec![node(2)]));
    let (_, next) = settle(
        row(WorkState::Reserved),
        task,
        (within, InstallState::Active),
    );
    let waiting = Settled {
        changed: false,
        followup: Followup::Nothing,
    };
    assert_eq!(next, Ok(Next::Done(waiting)));
}
