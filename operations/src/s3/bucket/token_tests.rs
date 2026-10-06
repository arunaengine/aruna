//! Token admission: holder rules, missing or stale copies, wrong tokens and promotion leases.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::blob::promote::PromotePendingOperation;
use crate::s3::bucket::key::rows::authority_rows;
use aruna_core::UserId;
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BucketInfo};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, EncryptionMode, GrantState, KeyState,
};
use aruna_core::structs::storage::format::{Compression, PithosLayout, StoredFormat};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use ulid::Ulid;

const GROUP: Ulid = Ulid::from_bytes([3; 16]);

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn key() -> BucketKeyRef {
    BucketKeyRef::new(Ulid::from_bytes([4; 16]), 2)
}

fn archive() -> ArchiveKey {
    ArchiveKey::new(Ulid::from_bytes([7; 16]), BackendRef::node_default())
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

fn token() -> SharedSecret {
    SharedSecret::new(SecretBytes::new(vec![9; 32]))
}

fn operation() -> AdmitTokenOperation {
    AdmitTokenOperation::new(TokenAdmitInput {
        bucket: "bucket".to_string(),
        group_id: GROUP,
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: node(),
        key: key(),
        archive: archive(),
        credential: TokenCredential {
            access_key: "TOKENKEY".to_string(),
            token: token(),
        },
    })
}

fn copy(created_by: UserId, generation: u64) -> TokenCopy {
    TokenCopy {
        key: BucketKeyRef::new(key().bucket_id, generation),
        access_key: "TOKENKEY".to_string(),
        created_by,
        nonce: [0; 12],
        ciphertext: vec![0; 48],
        created_at_ms: 1,
    }
}

/// The rows of a bucket of creator user(1) with `admins`, the token copy and the key record.
fn rows(admins: &[UserId], copy: Option<TokenCopy>, state: KeyState) -> Event {
    let info = BucketInfo {
        group_id: GROUP,
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
        bucket_id: Some(key().bucket_id),
        key_generation: key().generation,
        ..Default::default()
    };
    let mut values = authority_rows(&info, Some(&settings), admins);
    let mut record = BucketKeyRecord::new(key(), Ulid::from_bytes([8; 16]), [6; 32], 1);
    record.state = state;
    let copy = copy.map(|copy| Value::from(copy.to_bytes().unwrap()));
    values.push((Key::from(Vec::new()), copy));
    values.push((
        Key::from(Vec::new()),
        Some(record.to_bytes().unwrap().into()),
    ));
    Event::Storage(StorageEvent::BatchReadResult { values })
}

fn admitted(effects: &Effects) -> bool {
    matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::AdmitToken { key: admitted, archive: named, copy, public_key, token: sent, .. })]
            if *admitted == key() && *named == archive() && copy.key == key()
                && *public_key == [6; 32] && *sent == token()
    )
}

fn locked(operation: AdmitTokenOperation) -> bool {
    operation.finalize()
        == Err(TokenAdmitError::Key(BucketKeyError::Locked(
            key().bucket_id,
        )))
}

#[test]
fn creator_token_admitted() {
    let mut operation = operation();
    let effects = operation.start();
    let [
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the rows read, got {effects:?}");
    };
    let spaces: Vec<_> = reads[4..].iter().map(|(space, _)| space.as_str()).collect();
    assert_eq!(spaces, [KEY_COPY_KEYSPACE, BUCKET_KEY_KEYSPACE]);
    assert_eq!(
        reads[4].1.as_ref(),
        TokenCopy::copy_key(key(), "TOKENKEY").as_slice()
    );

    let effects = operation.step(rows(&[], Some(copy(user(1), 2)), KeyState::Active));
    assert!(admitted(&effects), "{effects:?}");
    let lease = ReadLease::new(key(), archive(), Ulid::nil(), Arc::new(()));
    operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease }));
    assert_eq!(operation.finalize().map(|lease| lease.key), Ok(key()));

    // A current admin's token opens without a grant read.
    let mut operation = self::operation();
    operation.start();
    let effects = operation.step(rows(&[user(2)], Some(copy(user(2), 2)), KeyState::Active));
    assert!(admitted(&effects));
}

#[test]
fn holder_loss_locks() {
    // user(3) created the token as an explicit holder; the grant decides now (D30).
    let grant = |origin| {
        let grant = BucketHolder {
            bucket_id: key().bucket_id,
            user_id: user(3),
            origin,
            state: GrantState::Ready,
            granted_by: user(1),
            granted_at_ms: 1,
        };
        Some(Value::from(grant.to_bytes().unwrap()))
    };
    for (row, opens) in [
        (grant(HolderOrigin::Explicit), true),
        (grant(HolderOrigin::Admin), false),
        (None, false),
    ] {
        let mut operation = operation();
        operation.start();
        let effects = operation.step(rows(&[], Some(copy(user(3), 2)), KeyState::Active));
        let [
            Effect::Storage(StorageEffect::Read {
                key_space,
                key: read,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the grant read, got {effects:?}");
        };
        assert_eq!(key_space, BUCKET_HOLDER_KEYSPACE);
        let named = [&key().bucket_id.to_bytes()[..], &user(3).to_storage_key()].concat();
        assert_eq!(read.as_ref(), named.as_slice());
        let read = Event::Storage(StorageEvent::ReadResult {
            key: named.into(),
            value: row,
        });
        let effects = operation.step(read);
        assert_eq!(admitted(&effects), opens);
        if !opens {
            assert!(effects.is_empty());
            assert!(locked(operation));
        }
    }
}

#[test]
fn stale_copies_lock() {
    // No copy of this generation, a retired generation or another credential's copy: no key
    // is ever opened, and the read fails like any locked read.
    let other = TokenCopy {
        access_key: "OTHERKEY".to_string(),
        ..copy(user(1), 2)
    };
    for (copy, state) in [
        (None, KeyState::Active),
        (Some(copy(user(1), 1)), KeyState::Active),
        (Some(copy(user(1), 2)), KeyState::Retired),
        (Some(other), KeyState::Active),
    ] {
        let mut operation = operation();
        operation.start();
        let effects = operation.step(rows(&[], copy, state));
        assert!(effects.is_empty(), "{effects:?}");
        assert!(locked(operation));
    }
}

#[test]
fn retiring_token_locks() {
    let retiring = BucketKeyRef::new(key().bucket_id, 1);
    let mut operation = operation();
    operation.input.key = retiring;
    operation.start();
    let Event::Storage(StorageEvent::BatchReadResult { mut values }) =
        rows(&[], Some(copy(user(1), 1)), KeyState::Retiring)
    else {
        panic!("expected bucket rows");
    };
    let mut record = BucketKeyRecord::new(retiring, Ulid::from_bytes([8; 16]), [6; 32], 1);
    record.state = KeyState::Retiring;
    values[5].1 = Some(record.to_bytes().unwrap().into());
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    assert!(effects.is_empty(), "{effects:?}");
    assert!(locked(operation));
}

#[test]
fn wrong_token_typed() {
    let mut operation = operation();
    operation.start();
    operation.step(rows(&[], Some(copy(user(1), 2)), KeyState::Active));
    let wrong = BlobError::BucketKey(BucketKeyError::InvalidToken);
    operation.step(Event::Blob(BlobEvent::Error(wrong)));
    assert_eq!(
        operation.finalize().err(),
        Some(TokenAdmitError::Key(BucketKeyError::InvalidToken))
    );

    // A lease of another archive is not taken.
    let mut operation = self::operation();
    operation.start();
    operation.step(rows(&[], Some(copy(user(1), 2)), KeyState::Active));
    let other = ArchiveKey::new(Ulid::generate(), BackendRef::node_default());
    let lease = ReadLease::new(key(), other, Ulid::nil(), Arc::new(()));
    operation.step(Event::Blob(BlobEvent::ReadAdmitted { lease }));
    assert!(matches!(
        operation.finalize(),
        Err(TokenAdmitError::InvalidStateEvent { .. })
    ));
}

#[test]
fn promotion_uses_lease() {
    let layout = PithosLayout {
        stored_size: 64,
        metadata_digest: [1; 32],
        storage_generation: 0,
    };
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "pending".to_string(),
        ulid: archive().archive_id,
        format: StoredFormat::pithos(layout, key()),
        created_by: user(1),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::new(),
    };
    let pending = || {
        Event::Storage(StorageEvent::ReadResult {
            key: archive().to_bytes().into(),
            value: Some(location.to_bytes().unwrap().into()),
        })
    };
    let promotion = |lease| {
        let limits = RoCrateLimits::default();
        let operation = PromotePendingOperation::new(archive(), user(1).realm_id, node(), limits);
        operation.with_lease(lease)
    };
    // A token lease of the pending archive hashes it without asking the unlock registry.
    let lease = ReadLease::new(key(), archive(), Ulid::nil(), Arc::new(()));
    let mut operation = promotion(lease);
    operation.start();
    let effects = operation.step(pending());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::HashArchive { .. })]
    ));
    // A lease of another archive is ignored; the registry decides as before.
    let other = ArchiveKey::new(Ulid::generate(), BackendRef::node_default());
    let mut operation = promotion(ReadLease::new(key(), other, Ulid::nil(), Arc::new(())));
    operation.start();
    let effects = operation.step(pending());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::AdmitRead { .. })]
    ));
}
