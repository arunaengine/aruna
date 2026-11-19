//! Tests that an admitted host call runs its plan only after the admission, at the run's time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_plugin_types::calls::{ErrorKind, HostError};

use super::*;
use crate::plugins::fixtures::{self, is_child, node};
use crate::plugins::rows::tests::child_result;
use aruna_test_support::plugins::queued;

fn scope() -> RunScope {
    RunScope {
        realm_id: fixtures::realm(),
        local: node(3),
        run: queued(fixtures::install().install_id, 1),
    }
}

fn admitted() -> IoResult {
    child_result(Ok::<_, CallError>(fixtures::install()))
}

#[test]
fn plan_after_admission() {
    let id = "01JBX7Y3S5M3Q8Y1V9K2T4R6P1".to_string();
    let mut cancel = Admitted::new(&scope(), None, CancelAsk { id: id.clone() });
    cancel.stamp(42);
    assert!(is_child(&cancel.first()), "the admission runs first");
    assert_eq!(cancel.plan, Some(CancelAsk { id: id.clone() }));
    assert!(is_child(&cancel.next(admitted())), "then the cancel");
    assert_eq!(cancel.plan, None);
    let expected = CancelWork::new(&fixtures::install(), &id, 42);
    assert_eq!(
        CancelAsk { id }.call(&fixtures::install(), 42),
        Ok(expected)
    );
    let done = cancel.next(child_result(Ok::<(), CallError>(())));
    assert_eq!(done, Ok(Next::Done(())));
}

#[test]
fn refusal_stops_call() {
    let ask = StateAsk { key: "k".into() };
    let mut read = Admitted::new(&scope(), None, ask);
    read.first().unwrap();
    let limit = CallError(HostError::new(ErrorKind::Limit, "too fast"));
    let refused = read.next(child_result(Err::<PluginInstall, _>(limit.clone())));
    assert_eq!(refused, Err(limit), "nothing is read");
    assert!(read.plan.is_some());
    let removal = aruna_core::plugin_runs::StateRemoval {
        key: "k".into(),
        expected: 1,
        intent: CallIntent {
            ordinal: 0,
            digest: [1; 32],
        },
    };
    let change = StateChange::Remove(removal);
    let ask = ChangeAsk {
        run_id: Ulid::from_parts(1, 1),
        change,
    };
    let mut remove = Admitted::new(&scope(), None, ask);
    remove.first().unwrap();
    let denied = CallError(HostError::new(ErrorKind::Denied, "no"));
    let answer = remove.next(child_result(Err::<PluginInstall, _>(denied.clone())));
    let failure = answer.unwrap_err();
    assert_eq!((failure.error, failure.changed), (denied.0, false));
}

#[test]
fn plans_check_arguments() {
    let place = (fixtures::realm(), node(3));
    let install = fixtures::install();
    let list = ListAsk {
        place,
        group_id: install.group_id,
        prefix: "/listed".into(),
        cursor: Some("zz".into()),
    };
    let bad = list.call(&install, 0).unwrap_err();
    assert_eq!(bad.error.kind, ErrorKind::Invalid);
    let info = InfoAsk {
        place,
        path: "/elsewhere".into(),
        version: Some("x".into()),
    };
    let bad = info.call(&install, 0).unwrap_err();
    assert_eq!(bad.error.kind, ErrorKind::Invalid);
}
