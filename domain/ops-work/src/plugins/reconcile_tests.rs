//! Tests the repair of settled installs as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::json;

use super::*;
use crate::plugins::fixtures::{self, config_result, is_child};
use crate::plugins::rows::tests::child_result;

fn place() -> RepairNode {
    (fixtures::realm(), fixtures::node(3))
}

#[test]
fn repair_skips_unsettled() {
    let mut preparing = fixtures::install();
    preparing.state = InstallState::Preparing;
    let mut repair = RepairSettled::new(place(), &[preparing], 5_000);
    assert_eq!(repair.first(), Ok(Next::Done(0)));
}

#[test]
fn repair_survives_failure() {
    let install = fixtures::install();
    let installs = [install.clone(), install];
    let mut repair = RepairSettled::new(place(), &installs, 5_000);
    assert!(is_child(&repair.first()));
    let failed = StepError {
        step: "role",
        error: "no".into(),
    };
    assert!(is_child(&repair.next(child_result(Err::<bool, _>(failed)))));
    let changed = child_result(Ok::<bool, StepError>(true));
    assert_eq!(repair.next(changed), Ok(Next::Done(1)));
}

#[test]
fn reconcile_needs_manager() {
    let mut reconcile = ReconcileInstalls::new((fixtures::realm(), fixtures::node(9)), 5_000);
    assert!(is_child(&reconcile.first()));
    let config = fixtures::config(&fixtures::install(), json!({}));
    assert_eq!(reconcile.next(config_result(config)), Ok(Next::Done(0)));
}

#[test]
fn reconcile_keeps_repairs() {
    let mut reconcile = ReconcileInstalls::new(place(), 5_000);
    reconcile.first().unwrap();
    let config = fixtures::config(&fixtures::install(), json!({}));
    assert!(is_child(&reconcile.next(config_result(config))));
    assert!(is_child(
        &reconcile.next(child_result(Ok::<usize, IoError>(2)))
    ));
    let failed = child_result(Err::<usize, _>(AccountError("no".into())));
    assert_eq!(reconcile.next(failed), Ok(Next::Done(2)));
}
