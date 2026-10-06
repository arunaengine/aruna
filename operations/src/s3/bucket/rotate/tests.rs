//! Tests bucket key rotation and encryption settings changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::rows::authority_rows;
use aruna_core::compute::SecretBytes;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::transition::TransitionState;
use std::time::SystemTime;

const BUCKET_ID: Ulid = Ulid::from_bytes([7; 16]);

fn info() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([1; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::new(Ulid::from_bytes([2; 16]), RealmId::from_bytes([3; 32])),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Zstd { level: 3 },
    }
}

fn settings(mode: EncryptionMode) -> BucketEncryption {
    BucketEncryption {
        mode,
        bucket_id: Some(BUCKET_ID),
        key_generation: 1,
        storage_generation: 4,
        ..BucketEncryption::default()
    }
}

fn active(vault: bool) -> BucketKeyRecord {
    let key = BucketKeyRef::new(BUCKET_ID, 1);
    let mut record = BucketKeyRecord::new(key, Ulid::from_bytes([5; 16]), [9; 32], 1);
    record.vault_entry = vault.then_some(record.record_id);
    record
}

/// The group admin who asks for every change.
fn admin() -> UserId {
    UserId::new(Ulid::from_bytes([9; 16]), info().created_by.realm_id)
}

fn operation(change: KeyChange) -> ChangeEncryptionOperation {
    ChangeEncryptionOperation::new(ChangeInput {
        bucket: "b".to_string(),
        group_id: info().group_id,
        realm_id: info().created_by.realm_id,
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        caller: admin(),
        change,
        max_unlock_ms: None,
        expected_generation: 4,
        lookups: BTreeMap::new(),
        now_ms: 50,
    })
}

fn settings_change(mode: EncryptionMode) -> KeyChange {
    KeyChange::Settings {
        mode,
        cipher: BlockCipher::default(),
        block_keys: BlockKeys::default(),
    }
}

/// Answers the bucket, upload and active key reads; returns what the operation does next.
fn loaded(
    operation: &mut ChangeEncryptionOperation,
    mode: EncryptionMode,
    uploads: Vec<(Key, Value)>,
) -> Effects {
    loaded_with(operation, mode, uploads, None, true)
}

/// Answers the reads up to the change itself: `transition` is the stored transition row and
/// `unlocked` decides whether the active generation is unlocked on this node.
fn loaded_with(
    operation: &mut ChangeEncryptionOperation,
    mode: EncryptionMode,
    uploads: Vec<(Key, Value)>,
    transition: Option<EncryptionTransition>,
    unlocked: bool,
) -> Effects {
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TxnId::default(),
    }));
    let current = settings(mode);
    let values = authority_rows(&info(), Some(&current), &[admin()]);
    operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let effects = operation.step(Event::Storage(StorageEvent::IterResult {
        values: uploads,
        next_start_after: None,
    }));
    if !matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ) {
        return effects;
    }
    let record = active(mode == EncryptionMode::NodeManaged);
    let transition = transition.map(|record| Value::from(record.to_bytes().unwrap()));
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                Key::from(Vec::new()),
                Some(record.to_bytes().unwrap().into()),
            ),
            (Key::from(Vec::new()), transition),
        ],
    }));
    if !matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReadKeyStatus { .. })]
    ) {
        return effects;
    }
    let status = UnlockStatus {
        key: record.key,
        session_id: Ulid::from_bytes([4; 16]),
        sequence: ulid::Ulid::from_parts(1, 1),
        deadline_ms: None,
        active: true,
        unlocked_at: SystemTime::UNIX_EPOCH,
        remaining: None,
        max_remaining: None,
    };
    let generations = if unlocked { vec![status] } else { Vec::new() };
    operation.step(Event::Blob(BlobEvent::KeyStatus { generations }))
}

fn rows(effects: &Effects) -> Vec<(String, Key, Value)> {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected the row writes, got {effects:?}")
    };
    writes.clone()
}

fn row(rows: &[(String, Key, Value)], key_space: &str) -> Vec<u8> {
    let found = rows.iter().find(|(space, _, _)| space == key_space);
    found.map(|(_, _, value)| value.to_vec()).unwrap()
}

fn generated(operation: &mut ChangeEncryptionOperation) -> Effects {
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    let private = SecretBytes::new(vec![4; 32]);
    let public_key = aruna_core::structs::storage::encryption::public_key_of(&private).unwrap();
    let effects = operation.step(Event::Blob(BlobEvent::BucketKeyGenerated {
        public_key,
        private_key: SharedSecret::new(private),
    }));
    crate::s3::bucket::key::abe::admit_parameters(operation, effects)
}

#[test]
fn rotation_journals_generations() {
    let mut operation = operation(KeyChange::Rotate);
    let effects = loaded(&mut operation, EncryptionMode::NodeManaged, Vec::new());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, .. })] if key_space == BUCKET_HOLDER_KEYSPACE
    ));

    let rows = rows(&generated(&mut operation));

    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Rotate);
    assert_eq!(transition.source, Some(BucketKeyRef::new(BUCKET_ID, 1)));
    assert_eq!(transition.target.plan.unwrap().key.generation, 2);
    assert_eq!(transition.source_vault, active(true).vault_entry);
    let stored = BucketEncryption::from_bytes(&row(&rows, BUCKET_ENCRYPTION_KEYSPACE)).unwrap();
    assert_eq!((stored.key_generation, stored.storage_generation), (2, 5));
    let old = rows.iter().find(|(space, key, _)| {
        space == BUCKET_KEY_KEYSPACE && key.as_ref() == BucketKeyRef::new(BUCKET_ID, 1).key()
    });
    let old = BucketKeyRecord::from_bytes(&old.unwrap().2).unwrap();
    assert_eq!(old.state, KeyState::Retiring);

    // A node-managed rotation keeps a node copy of the new generation.
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::VaultWrite { .. })]
    ));
    operation.step(Event::Storage(StorageEvent::WriteResult {
        key: Vec::new().into(),
    }));
    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TxnId::default(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Task(TaskEffect::ShortenTimer { .. })]
    ));
    let result = operation.finalize().unwrap();
    assert_eq!(result.key.unwrap().0.key.generation, 2);
}

#[test]
fn locking_needs_recovery() {
    // Without ready holders a vault_locked generation would be lost, and the node copy is
    // never written for it.
    let mut operation = operation(settings_change(EncryptionMode::VaultLocked));
    loaded(&mut operation, EncryptionMode::NodeManaged, Vec::new());

    let effects = generated(&mut operation);

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(ChangeError::RecoveryUnmet));
}

#[test]
fn decrypting_keeps_source() {
    let mut operation = operation(settings_change(EncryptionMode::Off));

    let rows = rows(&loaded(
        &mut operation,
        EncryptionMode::VaultLocked,
        Vec::new(),
    ));

    let stored = BucketEncryption::from_bytes(&row(&rows, BUCKET_ENCRYPTION_KEYSPACE)).unwrap();
    assert_eq!(stored.mode, EncryptionMode::Off);
    assert_eq!(stored.storage_generation, 5);
    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Decrypt);
    assert_eq!(transition.target.plan, None);
    assert_eq!(transition.target.compression, info().compression);
    let old = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    assert_eq!(old.state, KeyState::Retiring);
}

#[test]
fn cipher_change_reencodes() {
    let mut operation = operation(KeyChange::Settings {
        mode: EncryptionMode::NodeManaged,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::default(),
    });

    let rows = rows(&loaded(
        &mut operation,
        EncryptionMode::NodeManaged,
        Vec::new(),
    ));

    let transition = EncryptionTransition::from_bytes(&row(&rows, TRANSITION_KEYSPACE)).unwrap();
    assert_eq!(transition.kind, TransitionKind::Reencode);
    let plan = transition.target.plan.unwrap();
    assert_eq!(
        (plan.key.generation, plan.cipher),
        (1, BlockCipher::Aes256Gcm)
    );
    let old = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    assert_eq!(old.state, KeyState::Active);
}

#[test]
fn combined_change_reencodes() {
    use aruna_core::structs::identity::user::vault::UserKeyRecord;
    use aruna_core::structs::placement::record::PlacementRef;
    for (cipher, block_keys) in [
        (BlockCipher::Aes256Gcm, BlockKeys::ContentDerived),
        (BlockCipher::ChaCha20Poly1305, BlockKeys::Unique),
        (BlockCipher::Aes256Gcm, BlockKeys::Unique),
    ] {
        let mut operation = operation(KeyChange::Settings {
            mode: EncryptionMode::VaultLocked,
            cipher,
            block_keys,
        });
        let holder = UserKeyRecord {
            user_id: admin(),
            record_id: Ulid::from_bytes([8; 16]),
            key_id: "slot".to_string(),
            public_key: [7; 32],
            fingerprint: aruna_core::vault_format::key_fingerprint(&[7; 32]),
            has_recovery: true,
            node_id: operation.input.node_id,
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        };
        operation
            .input
            .lookups
            .insert(admin(), KeyLookup::Keys(vec![holder.clone()]));
        loaded(&mut operation, EncryptionMode::NodeManaged, Vec::new());
        let effects = generated(&mut operation);
        let [Effect::Blob(BlobEffect::SealHolderCopies { key, .. })] = effects.as_slice() else {
            panic!("expected holder copies")
        };
        let copy = SealedCopy {
            key: *key,
            user_id: holder.user_id,
            key_record: holder.record_id,
            key_id: holder.key_id,
            enc: [0; 32],
            ciphertext: vec![0; 48],
            created_at_ms: 50,
        };
        let writes =
            rows(&operation.step(Event::Blob(BlobEvent::CopiesSealed { copies: vec![copy] })));
        let transition =
            EncryptionTransition::from_bytes(&row(&writes, TRANSITION_KEYSPACE)).unwrap();
        assert_eq!(transition.kind, TransitionKind::Reencode);
        let plan = transition.target.plan.unwrap();
        assert_eq!(
            (plan.key.generation, plan.cipher, plan.block_keys),
            (2, cipher, block_keys)
        );
        assert_eq!(plan.storage_generation, 5);
        assert_eq!(transition.source, Some(active(true).key));
    }
}

#[test]
fn concurrent_put_converted() {
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    for written_ms in [50, 60] {
        let mut change = operation(KeyChange::Settings {
            mode: EncryptionMode::NodeManaged,
            cipher: BlockCipher::Aes256Gcm,
            block_keys: BlockKeys::default(),
        });
        let old = settings(EncryptionMode::NodeManaged);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: String::new(),
            ulid: Ulid::nil(),
            format: StoredFormat::pithos(
                PithosLayout {
                    stored_size: 1,
                    metadata_digest: [1; 32],
                    storage_generation: old.storage_generation,
                },
                old.active_key().unwrap(),
            ),
            created_by: admin(),
            created_at: SystemTime::UNIX_EPOCH + Duration::from_millis(written_ms),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: Default::default(),
        };
        let writes = rows(&loaded(
            &mut change,
            EncryptionMode::NodeManaged,
            Vec::new(),
        ));
        let transition =
            EncryptionTransition::from_bytes(&row(&writes, TRANSITION_KEYSPACE)).unwrap();
        assert!(transition.needs(&location));
        let mut rewrite = crate::blob::migration::rewrite::RewriteVersionOperation::new(
            aruna_core::structs::storage::blob::VersionKey::new("b", "put", Ulid::nil()),
            transition,
            SystemTime::UNIX_EPOCH,
        );
        rewrite.start();
        let version = aruna_core::structs::storage::blob::BlobVersion::materialized(
            [1; 32],
            location.backend.clone(),
            location.format.encoding(),
            location.created_at,
            admin(),
            None,
        );
        rewrite.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        let effects = rewrite.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(location.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::AdmitRead { .. })]
        ));
    }
}

#[test]
fn combined_settings_atomic() {
    let change = KeyChange::Settings {
        mode: EncryptionMode::NodeManaged,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::default(),
    };
    let mut operation = operation(change);
    operation.input.max_unlock_ms = Some(Some(60_000));
    let writes = rows(&loaded(
        &mut operation,
        EncryptionMode::NodeManaged,
        Vec::new(),
    ));
    let stored = BucketEncryption::from_bytes(&row(&writes, BUCKET_ENCRYPTION_KEYSPACE)).unwrap();
    assert_eq!(stored.cipher, BlockCipher::Aes256Gcm);
    assert_eq!(stored.max_unlock_ms, Some(60_000));
    assert_eq!(stored.storage_generation, 5);
}

#[test]
fn combined_settings_refused() {
    for (max, generation, error) in [
        (Some(0), 4, BucketKeyError::InvalidDuration),
        (
            Some(60_000),
            3,
            BucketKeyError::StaleGeneration {
                requested: 3,
                current: 4,
            },
        ),
    ] {
        let mut operation = operation(settings_change(EncryptionMode::Off));
        operation.input.max_unlock_ms = Some(max);
        operation.input.expected_generation = generation;
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let values = authority_rows(
            &info(),
            Some(&settings(EncryptionMode::NodeManaged)),
            &[admin()],
        );
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(operation.finalize(), Err(ChangeError::Key(error)));
    }
}

#[test]
fn unlocking_adds_copy() {
    let mut operation = operation(settings_change(EncryptionMode::NodeManaged));
    let effects = loaded(&mut operation, EncryptionMode::VaultLocked, Vec::new());
    let key = BucketKeyRef::new(BUCKET_ID, 1);
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReadUnlockedKey { key })]
    );

    let private_key = SharedSecret::new(SecretBytes::new(vec![4; 32]));
    let rows = rows(&operation.step(Event::Blob(BlobEvent::UnlockedKeyRead { key, private_key })));

    assert!(
        rows.iter()
            .all(|(space, _, _)| space != TRANSITION_KEYSPACE)
    );
    let record = BucketKeyRecord::from_bytes(&row(&rows, BUCKET_KEY_KEYSPACE)).unwrap();
    let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::VaultWrite { entry, .. })]
            if Some(entry.id) == record.vault_entry
    ));
}

#[test]
fn locked_bucket_stays() {
    let mut operation = operation(settings_change(EncryptionMode::NodeManaged));
    loaded(&mut operation, EncryptionMode::VaultLocked, Vec::new());

    let locked = BucketKeyError::Locked(BUCKET_ID);
    operation.step(Event::Blob(BlobEvent::Error(locked.clone().into())));

    assert_eq!(
        operation.finalize(),
        Err(ChangeError::Blob(BlobError::BucketKey(locked)))
    );
}

#[test]
fn running_transition_rejects() {
    // Each unfinished state still owns copies of its source generation.
    let target = TransitionTarget {
        compression: Compression::Off,
        plan: None,
    };
    let source = Some(BucketKeyRef::new(BUCKET_ID, 1));
    for state in [
        TransitionState::Running,
        TransitionState::AwaitingKey,
        TransitionState::Cleanup,
        TransitionState::Blocked,
    ] {
        let mut transition =
            EncryptionTransition::new(TransitionKind::Rotate, source, target, 4, 1);
        transition.state = state;
        for change in [KeyChange::Rotate, settings_change(EncryptionMode::Off)] {
            let mut operation = operation(change);
            let mode = EncryptionMode::NodeManaged;
            let effects = loaded_with(
                &mut operation,
                mode,
                Vec::new(),
                Some(transition.clone()),
                true,
            );
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ));
            assert_eq!(operation.finalize(), Err(ChangeError::TransitionRunning));
        }
    }

    let mut finished = EncryptionTransition::new(TransitionKind::Rotate, source, target, 4, 1);
    finished.state = TransitionState::Finished;
    finished.finished_at_ms = Some(9);
    let mut operation = operation(KeyChange::Rotate);
    let effects = loaded_with(
        &mut operation,
        EncryptionMode::NodeManaged,
        Vec::new(),
        Some(finished),
        true,
    );
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { .. })]
    ));
}

#[test]
fn locked_source_refuses() {
    // No row is written: the operation stops before any batch write and aborts.
    let changes = [
        (KeyChange::Rotate, EncryptionMode::VaultLocked),
        (
            settings_change(EncryptionMode::Off),
            EncryptionMode::VaultLocked,
        ),
        (
            KeyChange::Settings {
                mode: EncryptionMode::NodeManaged,
                cipher: BlockCipher::Aes256Gcm,
                block_keys: BlockKeys::default(),
            },
            EncryptionMode::NodeManaged,
        ),
    ];
    for (change, mode) in changes {
        let mut operation = operation(change);

        let effects = loaded_with(&mut operation, mode, Vec::new(), None, false);

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(ChangeError::Key(BucketKeyError::Locked(BUCKET_ID)))
        );
    }
}

#[test]
fn former_admin_refused() {
    let mut operation = operation(KeyChange::Rotate);
    operation.start();
    operation.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TxnId::default(),
    }));
    let current = settings(EncryptionMode::NodeManaged);
    let values = authority_rows(&info(), Some(&current), &[]);

    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(operation.finalize(), Err(ChangeError::NotAdmin));
}

#[test]
fn change_writes_audit() {
    use aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE;
    use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
    let mut change = operation(KeyChange::Rotate);
    loaded(&mut change, EncryptionMode::NodeManaged, Vec::new());
    let rotation = rows(&generated(&mut change));
    let record = BucketAuditRecord::from_bytes(&row(&rotation, BUCKET_AUDIT_KEYSPACE)).unwrap();
    assert_eq!(record.action, AuditAction::Rotation);
    assert_eq!(record.actor, Some(admin()));
    assert_eq!(record.generation, Some(2));
    assert_eq!(record.outcome, AuditOutcome::Applied);

    let mut change = operation(settings_change(EncryptionMode::Off));
    let decrypt = rows(&loaded(
        &mut change,
        EncryptionMode::VaultLocked,
        Vec::new(),
    ));
    let record = BucketAuditRecord::from_bytes(&row(&decrypt, BUCKET_AUDIT_KEYSPACE)).unwrap();
    assert_eq!(record.action, AuditAction::ModeChange);
    assert_eq!(record.generation, Some(1));
}
