//! Explicit grants: sealed while unlocked, pending while locked or without a key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::rows::authority_rows;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::format::Compression;
use aruna_core::vault_format::key_fingerprint;
use std::time::SystemTime;
use ulid::Ulid;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn input(lookup: KeyLookup) -> GrantInput {
    GrantInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        user_id: user(7),
        granted_by: user(1),
        lookup,
        now_ms: 9,
    }
}

fn keys() -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        realm_id: user(7).realm_id,
        user_id: user(7),
        record_id: Ulid::from_bytes([8; 16]),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: false,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
        signature: None,
    }])
}

/// Runs a grant by admin user(1) up to its seal or write step against an encrypted bucket, or a
/// plain one.
fn started(lookup: KeyLookup, encrypted: bool) -> (GrantHolderOperation, Step) {
    granted(lookup, encrypted, &[user(1)])
}

/// Runs a grant by user(1) while `admins` hold the group admin role in the transaction.
fn granted(lookup: KeyLookup, encrypted: bool, admins: &[UserId]) -> (GrantHolderOperation, Step) {
    let mut operation = GrantHolderOperation::new(input(lookup));
    assert_eq!(
        operation.transaction(),
        aruna_core::operation::TransactionMode::Write
    );
    let _ = operation.first();
    let info = BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: 2,
        ..Default::default()
    };
    let settings = encrypted.then_some(&settings);
    let values = authority_rows(&info, settings, admins);
    let values = values.into_iter().map(|(_, value)| value).collect();
    let turn = operation.next(IoResult::Values(values));
    if !encrypted || !admins.contains(&user(1)) {
        return (operation, turn);
    }
    let turn = operation.next(IoResult::Values(vec![None]));
    (operation, turn)
}

/// The grant row a write step stores.
fn written_grant(turn: &Step) -> (BucketHolder, usize) {
    let Ok(Next::Call(IoCall::Write(writes))) = turn else {
        panic!("expected the grant write, got {turn:?}");
    };
    let (space, _, value) = writes.last().unwrap();
    assert_eq!(space, BUCKET_HOLDER_KEYSPACE);
    // The audit record of the grant commits in the same batch.
    let (audit_space, _, audit) = &writes[writes.len() - 2];
    assert_eq!(audit_space, aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE);
    let record = BucketAuditRecord::from_bytes(audit).unwrap();
    assert_eq!(
        (record.action, record.actor, record.outcome),
        (
            AuditAction::HolderGrant,
            Some(user(1)),
            AuditOutcome::Applied
        )
    );
    (BucketHolder::from_bytes(value).unwrap(), writes.len() - 2)
}

fn committed(mut operation: GrantHolderOperation) -> GrantResult {
    match operation.next(IoResult::Done) {
        Ok(Next::Done(result)) => result,
        other => panic!("expected the grant, got {other:?}"),
    }
}

fn sealed(operation: &mut GrantHolderOperation, event: Event) -> Step {
    operation.next(IoResult::Event(Box::new(event)))
}

#[test]
fn unlocked_grant_seals() {
    let (mut operation, turn) = started(keys(), true);
    let Ok(Next::Call(IoCall::Effect(effect))) = &turn else {
        panic!("expected sealing with the unlocked key, got {turn:?}");
    };
    let Effect::Blob(BlobEffect::SealUnlocked { key, holders, .. }) = effect.as_ref() else {
        panic!("expected sealing with the unlocked key, got {turn:?}");
    };
    assert_eq!(*key, BucketKeyRef::new(BUCKET_ID, 2));
    let copy = SealedCopy {
        key: *key,
        user_id: holders[0].user_id,
        key_record: holders[0].key_record,
        key_id: holders[0].key_id.clone(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 9,
    };
    let copies = vec![copy];
    let turn = sealed(
        &mut operation,
        Event::Blob(BlobEvent::CopiesSealed { copies }),
    );
    let (grant, copies) = written_grant(&turn);
    assert_eq!(
        (grant.state, grant.origin, copies),
        (GrantState::Ready, HolderOrigin::Explicit, 1)
    );
    assert_eq!(committed(operation).state, HolderState::Ready);
}

#[test]
fn locked_grant_waits() {
    let (mut operation, _) = started(keys(), true);
    let locked = BlobError::BucketKey(BucketKeyError::Locked(BUCKET_ID));
    let turn = sealed(&mut operation, Event::Blob(BlobEvent::Error(locked)));
    let (grant, copies) = written_grant(&turn);
    assert_eq!((grant.state, copies), (GrantState::Pending, 0));
    assert_eq!(grant.granted_by, user(1));
    assert_eq!(committed(operation).state, HolderState::Pending);

    // A user without a published key is granted, but reported as missing a key.
    let (operation, turn) = started(KeyLookup::Missing, true);
    let (grant, _) = written_grant(&turn);
    assert_eq!(grant.state, GrantState::Pending);
    assert_eq!(committed(operation).state, HolderState::MissingKey);
}

#[test]
fn plain_bucket_refused() {
    let (_, turn) = started(keys(), false);
    assert_eq!(turn, Err(GrantError::NotEncrypted));
}

#[test]
fn revoked_admin_refused() {
    // The route saw user(1) as admin, but the role was revoked before the grant's transaction.
    let (_, turn) = granted(keys(), true, &[]);
    assert_eq!(turn, Err(GrantError::NotAdmin));
}
