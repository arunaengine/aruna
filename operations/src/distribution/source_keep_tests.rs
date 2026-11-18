//! Tests keeping the source bytes of an external reference.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::UserId;
use aruna_core::structs::identity::realm::RealmId;

use super::*;

fn keep() -> SourceKeep {
    SourceKeep {
        bucket: "spire".to_string(),
        key: "x".to_string(),
        version_id: Ulid::from_bytes([1; 16]),
        group_id: Ulid::from_bytes([2; 16]),
        user_id: UserId::nil(RealmId::from_bytes([9; 32])),
        ranged: true,
    }
}

// A version with nothing to keep ends the work before any claim.
#[test]
fn no_plan_settles() {
    let mut first = KeepFirstOperation::new(keep());
    assert!(matches!(first.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = Ok::<Option<Plan>, PlanError>(None);
    let answer = IoResult::Child(ChildOutcome::new(none));
    assert_eq!(first.next(answer), Ok(Next::Done(KeepStart::Settled(false))));
}
