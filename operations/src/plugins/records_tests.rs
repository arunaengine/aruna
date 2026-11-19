//! Tests the install record decisions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmId;

use super::*;
use crate::plugins::work::tests::sample_install;

fn config(expected: Option<u64>) -> InstallEntryConfig {
    let install = sample_install(Ulid::from_parts(1, 1));
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let actor = Actor {
        node_id: install.receiver,
        user_id: UserId::local(Ulid::from_parts(1, 2), realm_id),
        realm_id,
    };
    InstallEntryConfig {
        auth_context: AuthContext::from(&actor),
        actor,
        install_id: install.install_id,
        group_id: install.group_id,
        expected,
        install: Some(install),
        audit: None,
        proven: BTreeSet::new(),
    }
}

fn allowed(allowed: Result<bool, AuthorizationError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(allowed))
}

#[test]
fn checks_before_reading() {
    // The record must raise the revision the caller read by one.
    let mut skipped = InstallEntryOperation::new(config(Some(4)));
    assert_eq!(skipped.first(), Err(RecordError::Mismatch));
    let mut write = InstallEntryOperation::new(config(None));
    assert_eq!(
        write.transaction(),
        aruna_core::operation::TransactionMode::Write
    );
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Child(_)))));
    let mut refused = InstallEntryOperation::new(config(None));
    refused.first().unwrap();
    assert_eq!(
        refused.next(allowed(Ok(false))),
        Err(RecordError::Unauthorized)
    );
    let Ok(Next::Call(IoCall::Read(reads))) = write.next(allowed(Ok(true))) else {
        panic!("an allowed caller reads the config and its reducer state");
    };
    assert_eq!(reads.len(), 2);
    let missing = IoResult::Values(vec![None, None]);
    assert_eq!(write.next(missing), Err(RecordError::ConfigMissing));
}

#[test]
fn conflict_stays_storage() {
    assert_eq!(
        RecordError::from(IoError::Conflict),
        RecordError::StorageError(StorageError::TransactionConflict)
    );
    assert!(InstallEntryOperation::is_conflict(&RecordError::from(
        IoError::Conflict
    )));
}

#[test]
fn drains_after_commit() {
    let config = config(None);
    let install = config.install.clone();
    let drain = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
    let finish = InstallEntryOperation::new(config).finish();
    assert_eq!(
        finish,
        Next::Then {
            output: install,
            calls: drain
        }
    );
}

#[test]
fn removed_path_refused() {
    let none = BTreeSet::new();
    assert_eq!(entry_admits(&InstallEntry::Absent, true, &none), Ok(()));
    let deleted = InstallEntry::Settled(None);
    // A delayed retry with the original id finds the deletion and writes nothing.
    assert_eq!(
        entry_admits(&deleted, true, &none),
        Err(RecordError::Retired)
    );
    let live = InstallEntry::Settled(Some(Box::new(sample_install(Ulid::from_parts(1, 1)))));
    assert_eq!(entry_admits(&live, true, &none), Ok(()));
}

#[test]
fn conflict_resolves_deletion() {
    let (a, mut b) = (
        sample_install(Ulid::from_parts(1, 1)),
        sample_install(Ulid::from_parts(1, 1)),
    );
    b.group_id = Ulid::from_bytes([8; 16]);
    let conflict = InstallEntry::Conflict {
        deletion: true,
        installs: vec![a.clone(), b.clone()],
    };
    // No write may settle the replacement over the deletion.
    let all = BTreeSet::from([a.group_id, b.group_id]);
    assert_eq!(
        entry_admits(&conflict, true, &all),
        Err(RecordError::Retired)
    );
    // The resolving deletion needs every candidate group proven.
    let one = BTreeSet::from([a.group_id]);
    assert_eq!(
        entry_admits(&conflict, false, &one),
        Err(RecordError::Unproven)
    );
    assert_eq!(entry_admits(&conflict, false, &all), Ok(()));
    let values = InstallEntry::Conflict {
        deletion: false,
        installs: vec![a, b],
    };
    assert_eq!(entry_admits(&values, true, &BTreeSet::new()), Ok(()));
}
