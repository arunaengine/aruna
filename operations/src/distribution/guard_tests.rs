// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{binding, record, secret};

#[test]
fn coordinator_stamps_epoch() {
    let (snapshot, bytes) = record(true);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(write_epoch(&own, "a", Some(&bytes)), Ok(OriginalEpoch(1)));
    let Err(GuardError::NotCoordinator {
        node_id, range_id, ..
    }) = write_epoch(&own, "x", Some(&bytes))
    else {
        panic!("the upper range belongs to member 2");
    };
    assert_eq!(node_id, secret(2).public());
    assert_eq!(range_id, snapshot.config.ranges[1].range_id);
}

#[test]
fn unready_sets_refuse() {
    let (snapshot, bytes) = record(false);
    let own = binding(&snapshot, 1, 5);
    assert_eq!(
        write_epoch(&own, "a", Some(&bytes)),
        Err(GuardError::Route(RouteError::Pending(snapshot.set_id())))
    );
    assert_eq!(
        write_epoch(&own, "a", None),
        Err(GuardError::RecordMissing(snapshot.set_id()))
    );
}

#[test]
fn replaced_bucket_refused() {
    let (snapshot, bytes) = record(true);
    // A recreated local bucket of the name has another incarnation than the admitted one.
    let replaced = binding(&snapshot, 1, 6);
    assert_eq!(
        write_epoch(&replaced, "a", Some(&bytes)),
        Err(GuardError::Unbound(snapshot.set_id()))
    );
    let stranger = binding(&snapshot, 9, 5);
    assert_eq!(
        write_epoch(&stranger, "a", Some(&bytes)),
        Err(GuardError::Route(RouteError::NotMember(snapshot.set_id())))
    );
}
