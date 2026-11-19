//! Tests a task start as calls and results: a paused install, a task the manifest does not
//! declare, the task limit, and a refused send that frees its reservation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::json;

use super::*;
use crate::plugins::fixtures::{self, config_result, is_child, node};
use crate::plugins::rows::tests::child_result;
use crate::plugins::test_support::queued;
use crate::tests::records::local_ingress;

fn scope() -> RunScope {
    RunScope {
        realm_id: fixtures::realm(),
        local: node(3),
        run: queued(fixtures::install().install_id, 1),
    }
}

fn request() -> TaskRequest {
    TaskRequest {
        task: "map".to_string(),
        parameters: BTreeMap::new(),
        inputs: Vec::new(),
        files: Vec::new(),
        outputs: vec![("/out/".to_string(), "results/".to_string())],
        ordinal: 0,
    }
}

fn start() -> StartTask {
    let mut start = StartTask::new(&scope(), request(), local_ingress());
    start.seed(7);
    start
}

fn checked(install: PluginInstall) -> IoResult {
    child_result(Ok::<_, CallError>(install))
}

#[test]
fn paused_refused() {
    let mut start = start();
    assert!(is_child(&start.first()));
    let mut paused = fixtures::install();
    paused.paused = true;
    let refused = start.next(checked(paused));
    assert!(matches!(refused, Err(CallError(error)) if error.kind == ErrorKind::Denied));
}

#[test]
fn undeclared_task_invalid() {
    let mut start = start();
    start.first().unwrap();
    let install = fixtures::install();
    assert!(matches!(
        start.next(checked(install.clone())),
        Ok(Next::Call(_))
    ));
    let config = fixtures::config(&install, json!({}));
    let refused = start.next(config_result(config));
    assert!(matches!(refused, Err(CallError(error)) if error.kind == ErrorKind::Invalid));
}

#[test]
fn limit_refuses_task() {
    let limited = reserve_error(WorkError::Limit(TASK_ACTIVE));
    assert_eq!(limited.0.kind, ErrorKind::Limit);
    let conflict = reserve_error(WorkError::Conflict);
    assert_eq!(conflict.0.kind, ErrorKind::Conflict);
}

#[test]
fn refused_send_frees_slot() {
    let mut start = start();
    let work_id = Ulid::from_parts(9, 9);
    start.phase = StartPhase::Send(work_id);
    let refused = CallError(HostError::new(ErrorKind::Invalid, "no"));
    let failed = child_result(Err::<(), _>(refused.clone()));
    // The unsent reservation goes first, then the refusal is answered.
    assert!(is_child(&start.next(failed)));
    let dropped = child_result(Ok::<_, StoreError>(Changed::default()));
    assert_eq!(start.next(dropped), Err(refused));
}
