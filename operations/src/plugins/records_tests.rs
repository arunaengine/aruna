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
    assert!(write.transaction());
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
