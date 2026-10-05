//! Sealing missing copies after an unlock: pending grants and new implicit holders get copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::rows::authority_rows;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode, HolderOrigin};
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
        user_id,
        record_id: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: false,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
    }])
}

fn copy(user_id: UserId) -> SealedCopy {
    SealedCopy {
        key: BucketKeyRef::new(BUCKET_ID, 2),
        user_id,
        key_record: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0]; 16]),
        key_id: "slot".to_string(),
        enc: [0; 32],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    }
}

fn rows(values: Vec<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.into()))
            .collect(),
        next_start_after: None,
    })
}

#[test]
fn pending_holders_sealed() {
    // Creator user(1) has a copy; admin user(2) and the pending grant of user(3) have none.
    let lookups = [user(1), user(2), user(3)].map(|user| (user, keys(user)));
    let mut operation = SealMissingOperation::new(SealMissingInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        key: BucketKeyRef::new(BUCKET_ID, 2),
        lookups: BTreeMap::from(lookups),
    });
    operation.start();
    let txn_id = Ulid::from_bytes([9; 16]);
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let info = BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
    };
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(BUCKET_ID),
        key_generation: 2,
        ..Default::default()
    };
    let values = authority_rows(&info, Some(&settings), &[user(2)]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let grant = BucketHolder {
        bucket_id: BUCKET_ID,
        user_id: user(3),
        origin: HolderOrigin::Explicit,
        state: GrantState::Pending,
        granted_by: user(1),
        granted_at_ms: 1,
    };
    operation.step(rows(vec![grant.to_bytes().unwrap()]));
    let effects = operation.step(rows(vec![copy(user(1)).to_bytes().unwrap()]));
    let [Effect::Blob(BlobEffect::SealUnlocked { holders, .. })] = effects.as_slice() else {
        panic!("expected the seal step, got {effects:?}");
    };
    let targets: Vec<_> = holders.iter().map(|target| target.user_id).collect();
    assert_eq!(targets, [user(2), user(3)]);

    let copies = vec![copy(user(2)), copy(user(3))];
    let effects = operation.step(Event::Blob(BlobEvent::CopiesSealed { copies }));
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the copy writes, got {effects:?}");
    };
    let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [KEY_COPY_KEYSPACE, KEY_COPY_KEYSPACE, BUCKET_HOLDER_KEYSPACE]
    );
    let ready = BucketHolder::from_bytes(&writes[2].2).unwrap();
    assert_eq!(ready.state, GrantState::Ready);
    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert_eq!(operation.finalize(), Ok(vec![user(2), user(3)]));
}
