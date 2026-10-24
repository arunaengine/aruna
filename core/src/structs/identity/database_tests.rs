// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::admin_documents::AdminDocumentDot;

fn realm() -> RealmId {
    RealmId::from_bytes([2; 32])
}

fn entry(seed: u8) -> DatabaseEntry {
    DatabaseEntry {
        entry_id: Ulid::from_bytes([seed; 16]),
        dataset_id: Ulid::from_bytes([9; 16]),
        target: DatabaseTarget::Distributed {
            set_id: Ulid::from_bytes([4; 16]),
        },
        owner_group: Ulid::from_bytes([5; 16]),
    }
}

fn dot(seq: u64) -> AdminDocumentDot {
    AdminDocumentDot {
        event_id: Ulid::from_bytes([6; 16]),
        origin_node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        origin_seq: seq,
    }
}

fn consent(entry: DatabaseEntry, predecessor: Option<AdminDocumentDot>) -> DatabaseConsent {
    let key = iroh::SecretKey::from_bytes(&[8; 32]);
    let claim = ConsentClaim {
        realm_id: realm(),
        entry,
        predecessor,
        approver: crate::UserId::local(Ulid::from_bytes([3; 16]), realm()),
    };
    let signature = key.sign(&claim.signing_bytes().unwrap());
    DatabaseConsent {
        claim,
        signer: key.public(),
        signature,
    }
}

#[test]
fn consent_binds_entry_and_predecessor() {
    let signed = consent(entry(1), Some(dot(1)));
    assert_eq!(signed.verify(realm(), &entry(1), Some(&dot(1))), Ok(()));
    // A consent never carries over to another entry or a later register version.
    assert_eq!(
        signed.verify(realm(), &entry(2), Some(&dot(1))),
        Err(DatabaseError::ConsentMismatch)
    );
    assert_eq!(
        signed.verify(realm(), &entry(1), Some(&dot(2))),
        Err(DatabaseError::ConsentMismatch)
    );
    assert_eq!(
        signed.verify(realm(), &entry(1), None),
        Err(DatabaseError::ConsentMismatch)
    );
}

#[test]
fn forged_signer_is_refused() {
    let mut signed = consent(entry(1), None);
    signed.signer = iroh::SecretKey::from_bytes(&[1; 32]).public();
    assert_eq!(
        signed.verify(realm(), &entry(1), None),
        Err(DatabaseError::ConsentSignature)
    );
}

#[test]
fn list_is_bounded() {
    let mut entries: Vec<_> = (0..=MAX_DATABASES)
        .map(|seed| DatabaseEntry {
            entry_id: Ulid::from(seed as u128),
            ..entry(1)
        })
        .collect();
    assert_eq!(validate_databases(&entries), Err(DatabaseError::TooMany));
    entries.truncate(2);
    entries[1].entry_id = entries[0].entry_id;
    assert_eq!(
        validate_databases(&entries),
        Err(DatabaseError::DuplicateEntry)
    );
}

#[test]
fn ordinary_bucket_name_checked() {
    let mut ordinary = entry(1);
    ordinary.target = DatabaseTarget::Ordinary {
        node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        bucket: "a/b".to_string(),
    };
    assert_eq!(ordinary.validate(), Err(DatabaseError::InvalidBucket));
}

#[test]
fn removal_leaves_new_version() {
    // A removal must change the register version, so a consent given before it cannot be
    // replayed onto the empty entry.
    use crate::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use crate::reducer::AdminDocumentState;
    use crate::structs::identity::auth::Actor;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        user_id: crate::UserId::local(Ulid::from_bytes([3; 16]), realm()),
        realm_id: realm(),
    };
    let mut state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id: realm() });
    state
        .apply_operation(
            &actor,
            AdminDocumentOperation::DatabaseSet {
                entry: entry(1),
                predecessor: None,
                consent: None,
            },
        )
        .unwrap();
    assert_eq!(state.materialized_databases(), vec![entry(1)]);
    let marked = state.database_version(entry(1).entry_id);
    assert!(marked.is_some());
    state
        .apply_operation(
            &actor,
            AdminDocumentOperation::DatabaseRemoved {
                entry_id: entry(1).entry_id,
            },
        )
        .unwrap();
    assert!(state.materialized_databases().is_empty());
    let removed = state.database_version(entry(1).entry_id);
    assert!(removed.is_some());
    assert_ne!(removed, marked);
}
