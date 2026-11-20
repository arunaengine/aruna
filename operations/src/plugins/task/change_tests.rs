//! Tests the task row changes as calls and results: a send's mark and record, the drop of an
//! unsent reservation, the end of an absent submission, and the settle with its report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::types::{Key, Value};

use super::*;
use crate::plugins::rows::tests::values_result;
use crate::plugins::work::tests::sample_install;
use crate::tests::records::{node, payload, user};
use aruna_core::install_work::WorkKind;

fn ids() -> (Ulid, Ulid) {
    (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2))
}

fn reserved() -> WorkRow {
    let install = sample_install(ids().0);
    WorkRow::reserved(&install, WorkKind::Recipe, (Ulid::nil(), ids().1), 1)
}

/// The change after it read `row` and `task`: what it calls or answers next.
fn applied(change: TaskChange, row: &WorkRow, task: Option<TaskRow>) -> (ChangeTask, ChangeNext) {
    let mut edit = ChangeTask::new(ids(), change, 9);
    assert!(matches!(edit.first(), Ok(Next::Call(IoCall::Read(_)))));
    edit.next(values_result(vec![Some(b"row".to_vec())]))
        .unwrap();
    let read = edit.next(values_result(vec![Some(encode(row).unwrap())]));
    assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
    let task = task.map(|task| encode(&task).unwrap());
    let next = edit.next(values_result(vec![task]));
    (edit, next)
}

fn request() -> SubmissionRequest {
    use aruna_core::job_request::SubmissionScope;
    SubmissionRequest {
        created_by: user(),
        spec: payload(),
        scope: SubmissionScope::Keyed("plugin-work/1".to_string()),
        retention_ms: 1,
        ingress_node_id: node(1),
        captured_inputs: Vec::new(),
        output_policies: Vec::new(),
        input_pages: None,
        admit_by: Some(5),
    }
}

fn mark() -> TaskChange {
    TaskChange::Mark {
        targets: vec![node(2)],
        admit_by: 5,
        request: Box::new(request()),
        digest: [3; 32],
    }
}

#[test]
fn mark_counts_attempts() {
    let (mut edit, next) = applied(mark(), &reserved(), None);
    let Ok(Next::Call(IoCall::Write(rows))) = next else {
        panic!("the mark writes both rows");
    };
    assert_eq!(rows.len(), 2);
    let marked = Changed {
        settled: false,
        marked: Some((1, false)),
    };
    assert_eq!(edit.next(IoResult::Done), Ok(Next::Done(marked)));
    // A second send means the first one's answer was lost.
    let mut again = reserved();
    again.boundary = Some(SendBoundary::Sending { attempt: 1 });
    let (mut edit, _) = applied(mark(), &again, Some(TaskRow::default()));
    let marked = edit.next(IoResult::Done).unwrap();
    assert_eq!(
        marked,
        Next::Done(Changed {
            settled: false,
            marked: Some((2, true))
        })
    );
}

#[test]
fn record_needs_reserved() {
    let family = JobFamilyId {
        submission_id: SubmissionId([4; 32]),
        request_digest: [5; 32],
    };
    let alias = JobId::from_bytes([6; 16]);
    let dispatch = TaskChange::Record(SendOutcome::Dispatch { alias, family });
    let (_, next) = applied(dispatch.clone(), &reserved(), None);
    assert!(matches!(next, Ok(Next::Call(IoCall::Write(_)))));
    // A row that moved on keeps its state.
    let mut settled = reserved();
    settled.state = WorkState::Settled;
    let (_, next) = applied(dispatch, &settled, None);
    assert_eq!(next, Ok(Next::Done(Changed::default())));
}

#[test]
fn unsent_dropped_once() {
    let (mut edit, next) = applied(TaskChange::DropUnsent, &reserved(), None);
    let Ok(Next::Call(IoCall::Delete(keys))) = next else {
        panic!("an unsent reservation goes with its keys");
    };
    assert_eq!(keys.len(), 3);
    assert_eq!(
        edit.next(IoResult::Done).unwrap(),
        Next::Done(Changed::settled(true))
    );
    // A reservation a send marked stays.
    let mut marked = reserved();
    marked.boundary = Some(SendBoundary::Sending { attempt: 1 });
    let (_, next) = applied(TaskChange::DropUnsent, &marked, None);
    assert_eq!(next, Ok(Next::Done(Changed::settled(true))));
}

#[test]
fn end_keeps_claimed() {
    let end = TaskChange::End {
        active: true,
        submission: SubmissionId([4; 32]),
    };
    let mut edit = ChangeTask::new(ids(), end, 9);
    assert!(matches!(edit.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let found = IoResult::Page {
        rows: vec![(Key::from(&b"claim"[..]), Value::from(&b"v"[..]))],
        next: None,
    };
    assert_eq!(edit.next(found), Ok(Next::Done(Changed::settled(false))));
}

#[test]
fn settle_keeps_report() {
    let mut dispatched = reserved();
    dispatched.state = WorkState::Dispatched;
    let task = TaskRow {
        report: Some("mapped".to_string()),
        ..TaskRow::default()
    };
    let summary = Summary::of(FinalState::Succeeded);
    let settle = TaskChange::Settle(Box::new(summary));
    let (_, next) = applied(settle, &dispatched, Some(task));
    let Ok(Next::Call(IoCall::Write(rows))) = next else {
        panic!("a finished family settles the row");
    };
    let row: WorkRow = decode(&rows[0].2).unwrap();
    let summary = row.summary.unwrap();
    assert_eq!(summary.report.as_deref(), Some("mapped"));
    assert_eq!(row.state, WorkState::Settled);
}
