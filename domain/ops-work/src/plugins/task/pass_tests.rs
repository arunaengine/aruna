//! Tests the task pass as calls and results: only task rows are settled, a resend goes out for an
//! active install, and an accepted cancel marks the row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_plugin_contract::calls::{ErrorKind, HostError};
use serde_json::json;

use super::*;
use crate::plugins::fixtures::{self, config_result, is_child};
use crate::plugins::rows::encode;
use crate::plugins::rows::tests::child_result;
use crate::tests::records::{Family, local_ingress};
use aruna_core::install_work::{Identity, WorkState, row_key};

fn task(kind: WorkKind) -> WorkRow {
    let ids = (Ulid::from_parts(3, 3), Ulid::from_parts(4, 4));
    let mut row = WorkRow::reserved(&fixtures::install(), kind, ids, 1);
    row.state = WorkState::Dispatched;
    row
}

fn scanned(rows: &[WorkRow]) -> IoResult {
    let install_id = fixtures::install().install_id;
    let rows: Rows = (rows.iter())
        .map(|row| {
            let identity = Identity::Call {
                run_id: row.work_id,
                ordinal: 0,
            };
            (
                row_key(install_id, &identity).unwrap(),
                encode(row).unwrap(),
            )
        })
        .collect();
    child_result(Ok::<_, StoreError>(rows))
}

/// A pass that read the config and scanned `rows`.
fn scanning(rows: &[WorkRow]) -> (SettleTasks, PassNext) {
    let install = fixtures::install();
    let mut pass = SettleTasks::new(local_ingress());
    assert!(is_child(&pass.first()));
    let config = fixtures::config(&install, json!({}));
    assert!(is_child(&pass.next(config_result(config))));
    let next = pass.next(scanned(rows));
    (pass, next)
}

fn settled(followup: Followup) -> IoResult {
    let changed = followup == Followup::Nothing;
    child_result(Ok::<_, StoreError>(Settled { changed, followup }))
}

#[test]
fn settles_task_rows() {
    let (_, done) = scanning(&[task(WorkKind::Copy)]);
    assert_eq!(done, Ok(Next::Done(0)));
    let (mut pass, settle) = scanning(&[task(WorkKind::Recipe)]);
    assert!(is_child(&settle));
    assert_eq!(pass.next(settled(Followup::Nothing)), Ok(Next::Done(1)));
}

#[test]
fn resends_for_active() {
    let (mut pass, _) = scanning(&[task(WorkKind::Recipe)]);
    assert!(is_child(&pass.next(settled(Followup::Resend))));
    // A resend that did not go out only warns; the pass goes on.
    let refused = CallError(HostError::new(ErrorKind::Unavailable, "down"));
    let failed = child_result(Err::<(), _>(refused));
    assert_eq!(pass.next(failed), Ok(Next::Done(0)));
}

#[test]
fn accepted_cancel_marks() {
    let family = Family::new([4; 32]);
    let ids = JobFamilyId {
        submission_id: family.submission_id,
        request_digest: family.request_digest,
    };
    let (mut pass, _) = scanning(&[task(WorkKind::Recipe)]);
    let cancel = Followup::Cancel {
        alias: family.job_id,
        family: ids,
    };
    assert!(is_child(&pass.next(settled(cancel))));
    assert!(is_child(&pass.next(child_result(Ok::<_, IoError>(true)))));
    let marked = child_result(Ok::<Option<WorkRow>, StoreError>(None));
    assert_eq!(pass.next(marked), Ok(Next::Done(0)));
}
