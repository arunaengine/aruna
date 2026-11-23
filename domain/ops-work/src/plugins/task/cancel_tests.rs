//! Tests the task cancels as calls and results: the holders asked in order, a cancel published
//! here, the removal proof, and an orphan pass over an empty node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::lifecycle::screen::tests::install;
use crate::plugins::fixtures::is_child;
use crate::plugins::rows::tests::child_result;
use crate::tests::records::{Family, node, user};

fn cancel(family: &Family, local: NodeId) -> CancelTask {
    let task = (user(), family.job_id, family.family());
    let mut cancel = CancelTask::new(Arc::new(family.config.clone()), local, task);
    cancel.stamp(5);
    cancel
}

#[test]
fn holders_in_turn() {
    let family = Family::new([4; 32]);
    let mut cancel = cancel(&family, node(9));
    assert!(is_child(&cancel.first()));
    // Every refusal asks the next holder; none accepting still stops the local runs.
    while matches!(cancel.phase, CancelPhase::Holder(_)) {
        let refused = JobRouteOutcome::Remote(JobResponse::Forbidden);
        assert!(is_child(
            &cancel.next(child_result(Ok::<_, JobRouteError>(refused)))
        ));
    }
    assert_eq!(cancel.phase, CancelPhase::Local);
    let stopped = child_result(Ok::<_, IoError>(()));
    assert_eq!(cancel.next(stopped), Ok(Next::Done(false)));
}

#[test]
fn holder_publishes_here() {
    let family = Family::with_local([4; 32], Some(node(9)));
    let mut cancel = cancel(&family, node(9));
    assert!(is_child(&cancel.first()));
    assert_eq!(cancel.phase, CancelPhase::Family);
    let published = child_result(Ok::<_, JobRouteError>(Some(())));
    assert!(is_child(&cancel.next(published)));
    let stopped = child_result(Ok::<_, IoError>(()));
    assert_eq!(cancel.next(stopped), Ok(Next::Done(true)));
}

#[test]
fn removal_needs_settled() {
    let mut removing = install();
    removing.state = InstallState::Removing;
    let account = removing.account;
    let settled = InstallEntry::Settled(Some(Box::new(removing.clone())));
    assert!(removal_confirmed(&settled, account));
    assert!(removal_confirmed(&InstallEntry::Settled(None), account));
    // Another account, an active install, no entry or a conflict prove nothing.
    assert!(!removal_confirmed(&settled, user()));
    let active = InstallEntry::Settled(Some(Box::new(install())));
    assert!(!removal_confirmed(&active, account));
    assert!(!removal_confirmed(&InstallEntry::Absent, account));
    let conflict = InstallEntry::Conflict {
        deletion: true,
        installs: vec![removing],
    };
    assert!(!removal_confirmed(&conflict, account));
}

#[test]
fn reads_family_keys() {
    let family = Family::new([4; 32]).family();
    let mut key = family.submission_id.0.to_vec();
    key.extend(family.request_digest);
    assert_eq!(family_at(&key), Some(family));
    assert_eq!(family_at(&key[..40]), None);
}

#[test]
fn empty_orphan_pass() {
    let family = Family::new([4; 32]);
    let mut orphans = CancelOrphans::new(Arc::new(family.config), node(9));
    assert!(matches!(
        orphans.first(),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let empty = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(orphans.next(empty), Ok(Next::Done(0)));
}
