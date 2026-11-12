//! Tests which credentials the cutoff sweep deletes, how it pages, and when a cutoff is gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::auth::{MAX_TOKEN_LIFETIME, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::credential_encryption::EncryptedS3Secret;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::TokenRevocation;
use aruna_core::structs::storage::blob::never_expiry;
use aruna_core::types::Value;

const CUTOFF: u64 = 1_000_000;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), realm())
}

/// A never-expiring credential of `owner` issued at unix second `issued` by `issuer`.
fn credential(owner: UserId, issued: u64, issuer: NodeId, serial: u128) -> UserAccess {
    UserAccess {
        access_key: Ulid::from_parts(issued * 1000, serial).to_string(),
        user_identity: owner,
        group_id: Ulid::from_bytes([9; 16]),
        secret: EncryptedS3Secret::empty(),
        expiry: never_expiry(),
        path_restrictions: None,
        issued_by: *issuer.as_bytes(),
        revoked_at: None,
    }
}

fn row(access: &UserAccess) -> (Key, Value) {
    let key = access.access_key.as_bytes().to_vec().into();
    (key, access.to_bytes().unwrap().into())
}

/// The realm configuration with a cutoff of `cut` at `CUTOFF`.
fn cutoffs(cut: UserId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
    config.revoked_tokens.push(TokenRevocation {
        token_hash: user_cutoff_hash(&cut),
        expires_at: user_cutoff_expiry(CUTOFF),
    });
    config
}

fn sweep(now: u64) -> CutoffSweepOperation {
    CutoffSweepOperation::new(CutoffSweepConfig {
        realm_id: realm(),
        node_id: node(1),
        now,
    })
}

#[test]
fn deletes_cut_off() {
    // Only this node's credential issued before the owner's live cutoff is deleted.
    let config = cutoffs(user(2));
    let actor = Actor {
        node_id: node(1),
        user_id: user(2),
        realm_id: realm(),
    };
    let doomed = credential(user(2), CUTOFF - 10, node(1), 1);
    let rows = vec![
        row(&doomed),
        row(&credential(user(2), CUTOFF + 10, node(1), 2)),
        row(&credential(user(2), CUTOFF - 10, node(3), 3)),
        row(&credential(user(4), CUTOFF - 10, node(1), 4)),
    ];
    let mut operation = sweep(CUTOFF + 60);
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));
    let value = Some(config.to_bytes(&actor).unwrap().into());
    let key = Key::from(Vec::new());
    let effects = operation.step(Event::Storage(StorageEvent::ReadResult { key, value }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { start: None, .. })]
    ));

    let last = rows[3].0.clone();
    let page = StorageEvent::IterResult {
        values: rows,
        next_start_after: Some(last.clone()),
    };
    let effects = operation.step(Event::Storage(page));
    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    assert!(
        operation.pending.is_empty(),
        "exactly one credential is cut off"
    );

    let revoked = SubOperationEvent::TokenRevoked { result: Ok(()) };
    let effects = operation.step(Event::SubOperation(revoked));
    let [Effect::Storage(StorageEffect::Iter { start, .. })] = effects.as_slice() else {
        panic!("expected the next page, got {effects:?}");
    };
    assert_eq!(start, &Some(IterStart::After(last)));
    let empty = StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    };
    assert!(operation.step(Event::Storage(empty)).is_empty());
    assert!(operation.is_complete());
    assert_eq!(operation.finalize(), Ok(1));
}

#[test]
fn pruned_cutoff_keeps() {
    // Once the cutoff left the revocation set the sweep cannot see it; it must run before.
    let mut operation = sweep(CUTOFF + MAX_TOKEN_LIFETIME + 1);
    operation.cutoffs = Some(cutoffs(user(2)));
    assert!(!operation.cut_off(&credential(user(2), CUTOFF - 10, node(1), 1)));
    operation.config.now = CUTOFF + MAX_TOKEN_LIFETIME - 1;
    assert!(operation.cut_off(&credential(user(2), CUTOFF - 10, node(1), 1)));
}

#[test]
fn missing_config_finishes() {
    let mut operation = sweep(CUTOFF);
    operation.start();
    let read = StorageEvent::ReadResult {
        key: Key::from(Vec::new()),
        value: None,
    };
    assert!(operation.step(Event::Storage(read)).is_empty());
    assert_eq!(operation.finalize(), Ok(0));

    // An event that does not fit the state fails the sweep instead of being ignored.
    let mut operation = sweep(CUTOFF);
    operation.start();
    let revoked = SubOperationEvent::TokenRevoked { result: Ok(()) };
    operation.step(Event::SubOperation(revoked));
    assert!(matches!(
        operation.finalize(),
        Err(CutoffSweepError::UnexpectedEvent { .. })
    ));
}
