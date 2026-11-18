//! Tests the checks of distributed bucket configuration changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{member, realm, record, replicated};

#[test]
fn refusal_names_eligible() {
    // Member 2 keeps a copy of member 1's range; member 1 itself cannot take it.
    let (snapshot, _) = replicated(true, 1);
    let range = &snapshot.config.ranges[0];
    let (range_id, coordinator) = (range.range_id, range.coordinator);
    let other = if coordinator == member(1) {
        member(2)
    } else {
        member(1)
    };
    assert!(eligible_target(&snapshot, range_id, other).is_ok());
    let Err(CreateError::Invalid(message)) = eligible_target(&snapshot, range_id, coordinator)
    else {
        panic!("a transfer to the coordinator itself is refused");
    };
    assert!(message.contains(&other.to_string()), "{message}");
    let (bare, _) = replicated(true, 0);
    let Err(CreateError::Invalid(message)) =
        eligible_target(&bare, bare.config.ranges[0].range_id, other)
    else {
        panic!("a range without copies has no eligible member");
    };
    assert!(message.contains("none"), "{message}");
}

#[test]
fn split_checked_early() {
    // A dry run refuses what the publisher would refuse: a key outside the range.
    let (snapshot, _) = record(true);
    let lower = &snapshot.config.ranges[0];
    let plan = |key: &str| {
        let [_, lower_id, upper_id] = split_ids(snapshot.set_id(), lower.range_id, 0, key);
        SplitPlan {
            range_id: lower.range_id,
            split_key: key.to_string(),
            lower_id,
            upper_id,
        }
    };
    assert!(checked_split(&snapshot, realm(), &plan("f")).is_ok());
    let refused = checked_split(&snapshot, realm(), &plan("x"));
    assert!(
        matches!(
            refused,
            Err(CreateError::Publish(PublishRefusal::Rejected(_)))
        ),
        "{refused:?}"
    );
    let (pending, _) = record(false);
    let refused = checked_split(&pending, realm(), &plan("f"));
    assert!(
        matches!(
            refused,
            Err(CreateError::Publish(PublishRefusal::Rejected(_)))
        ),
        "{refused:?}"
    );
}
