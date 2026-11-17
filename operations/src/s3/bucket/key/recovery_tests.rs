//! Recovery notices: a lost admin role tells the current holders once, never the former admin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::rows::authority_rows;
use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
use aruna_core::structs::execution::notification::NotificationOutboxRecord;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::Compression;
use aruna_core::vault_format::key_fingerprint;
use std::time::SystemTime;
use ulid::Ulid;

const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn keys(user_id: UserId) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        realm_id: user_id.realm_id,
        user_id,
        record_id: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
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

fn copy(user_id: UserId) -> (Key, Value) {
    let copy = SealedCopy {
        key: BucketKeyRef::new(BUCKET_ID, 1),
        user_id,
        key_record: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    };
    (copy.key().into(), copy.to_bytes().unwrap().into())
}

fn rows(values: Vec<(Key, Value)>) -> IoResult {
    IoResult::Page {
        rows: values,
        next: None,
    }
}

/// Checks a bucket of creator user(1) where user(1) and user(2) hold copies, `admins` hold the
/// admin role now and `marker` is what the holders were told before. Answers the decision.
fn checked(admins: &[UserId], marker: Option<Vec<u8>>) -> (RecoveryNoticeOperation, Step) {
    let lookups = BTreeMap::from([(user(1), keys(user(1))), (user(2), keys(user(2)))]);
    let mut operation = RecoveryNoticeOperation::new(RecoveryInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        lookups,
        now_ms: 9,
    });
    assert_eq!(operation.transaction(), TransactionMode::Write);
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
        key_generation: 1,
        ..Default::default()
    };
    let values = authority_rows(&info, Some(&settings), admins);
    let values = values.into_iter().map(|(_, value)| value).collect();
    let _ = operation.next(IoResult::Values(values));
    let _ = operation.next(rows(Vec::new()));
    let _ = operation.next(rows(vec![copy(user(1)), copy(user(2))]));
    let turn = operation.next(IoResult::Values(vec![marker.map(Value::from)]));
    (operation, turn)
}

/// The notices and marker of a decision batch.
fn written(turn: &Step) -> (Vec<UserId>, Vec<u8>) {
    let Ok(Next::Call(IoCall::Write(writes))) = turn else {
        panic!("expected the notice batch, got {turn:?}");
    };
    let recipients = writes
        .iter()
        .filter(|(space, _, _)| space == NOTIFICATION_OUTBOX_KEYSPACE)
        .map(|(_, _, value)| NotificationOutboxRecord::from_bytes(value).unwrap())
        .map(|outbox| outbox.record.recipient)
        .collect();
    let (space, _, marker) = &writes[0];
    assert_eq!(space, BUCKET_RECOVERY_KEYSPACE);
    (recipients, marker.to_vec())
}

#[test]
fn revoked_admin_degrades() {
    // With both admins ready the rule is met and nobody is told.
    let (_, turn) = checked(&[user(2)], None);
    assert_eq!(turn, Ok(Next::Done(Vec::new())));

    // user(2) lost the admin role: one ready holder without a recovery code is left. Only the
    // current holder is told; the former admin is not.
    let (mut operation, turn) = checked(&[], None);
    let (recipients, marker) = written(&turn);
    assert_eq!(recipients, [user(1)]);
    // The drain is woken only after the notices committed.
    let drain = IoCall::Effect(Box::new(schedule_drain_effect()));
    assert_eq!(
        operation.next(IoResult::Done),
        Ok(Next::Then {
            output: vec![user(1)],
            calls: vec![drain],
        })
    );

    // The same weakened state checked again tells nobody a second time.
    let (_, turn) = checked(&[], Some(marker));
    let (recipients, _) = written(&turn);
    assert!(recipients.is_empty());
}
