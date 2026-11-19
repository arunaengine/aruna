//! Enabling encryption step by step: holders, recovery, open uploads and stale generations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::key::test_rows::{authority_rows, woke};
use aruna_core::compute::SecretBytes;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::public_key_of;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::vault_format::key_fingerprint;
use std::time::SystemTime;

type Turn = Result<Next<EnableResult>, EnableError>;

fn row(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

fn keyed_rows(values: Vec<(Key, Option<Value>)>) -> IoResult {
    IoResult::Values(values.into_iter().map(|(_, value)| value).collect())
}

fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

fn blob(event: BlobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Blob(event)))
}

/// The answer to the node vault write.
fn vault_written() -> IoResult {
    let written = StorageEvent::WriteResult {
        key: Vec::new().into(),
    };
    IoResult::Event(Box::new(Event::Storage(written)))
}

fn asked(turn: &Turn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

fn user(seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
}

fn keys(user_id: UserId, recovery: bool) -> KeyLookup {
    KeyLookup::Keys(vec![UserKeyRecord {
        realm_id: user_id.realm_id,
        user_id,
        record_id: Ulid::from_bytes([user_id.user_ulid.to_bytes()[0] + 100; 16]),
        key_id: "slot".to_string(),
        public_key: [7; 32],
        fingerprint: key_fingerprint(&[7; 32]),
        has_recovery: recovery,
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        placement: PlacementRef::NIL,
        created_at_ms: 1,
        signature: None,
    }])
}

fn input(mode: EncryptionMode, lookups: BTreeMap<UserId, KeyLookup>) -> EnableInput {
    EnableInput {
        bucket: "bucket".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: RealmId::from_bytes([1; 32]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        caller: user(2),
        mode,
        cipher: BlockCipher::Aes256Gcm,
        block_keys: BlockKeys::ContentDerived,
        max_unlock_ms: None,
        expected_generation: 0,
        lookups,
        now_ms: 5,
    }
}

fn bucket() -> BucketInfo {
    BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: user(1),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

/// Runs the operation up to the generated key, with no uploads and no stored settings.
fn generated(operation: &mut EnableEncryptionOperation) -> (Turn, [u8; 32]) {
    assert_eq!(operation.transaction(), TransactionMode::Write);
    operation.first().unwrap();
    let values = authority_rows(&bucket(), None, &[user(2)]);
    operation.next(keyed_rows(values)).unwrap();
    no_versions(operation).unwrap();
    operation.next(page(Vec::new(), None)).unwrap();
    let effects = no_versions(operation);
    assert_eq!(
        asked(&effects),
        &Effect::Blob(BlobEffect::GenerateBucketKey)
    );
    let private = SecretBytes::new(vec![4; 32]);
    let public = public_key_of(&private).unwrap();
    let effects = operation.next(blob(BlobEvent::BucketKeyGenerated {
        public_key: public,
        private_key: SharedSecret::new(private),
    }));
    let effects = crate::s3::bucket::key::abe::tests::admit_parameters(operation, effects);
    (effects, public)
}

/// Answers the scan of the bucket's versions with an empty page.
fn no_versions(operation: &mut EnableEncryptionOperation) -> Turn {
    operation.next(page(Vec::new(), None))
}

fn sealed(effects: &Turn) -> Vec<SealedCopy> {
    let Effect::Blob(BlobEffect::SealHolderCopies { key, holders, .. }) = asked(&effects) else {
        panic!("expected sealing, got {effects:?}");
    };
    holders
        .iter()
        .map(|target| SealedCopy {
            key: *key,
            user_id: target.user_id,
            key_record: target.key_record,
            key_id: target.key_id.clone(),
            enc: [0; 32],
            ciphertext: vec![0; 48],
            created_at_ms: 5,
        })
        .collect()
}

#[test]
fn managed_enable_writes() {
    let lookups = BTreeMap::from([
        (user(1), keys(user(1), false)),
        (user(2), KeyLookup::Missing),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, lookups));
    let (effects, public) = generated(&mut operation);
    let copies = sealed(&effects);
    // Only the creator has a published key; the admin without one gets no copy.
    assert_eq!(
        copies.iter().map(|copy| copy.user_id).collect::<Vec<_>>(),
        vec![user(1)]
    );

    let effects = operation.next(blob(BlobEvent::CopiesSealed { copies }));
    let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
        panic!("expected the settings write, got {effects:?}");
    };
    let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    // The same batch starts the encrypt transition of copies stored in plain form.
    assert_eq!(
        spaces,
        [
            BUCKET_ENCRYPTION_KEYSPACE,
            BUCKET_KEY_KEYSPACE,
            KEY_COPY_KEYSPACE,
            aruna_core::keyspaces::TRANSITION_KEYSPACE,
            aruna_core::keyspaces::TRANSITION_QUEUE_KEYSPACE,
            aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE,
            aruna_core::keyspaces::GROUP_ENCRYPTED_KEYSPACE
        ]
    );
    // The enable records its mode change in the same batch.
    let audit = BucketAuditRecord::from_bytes(&writes[5].2).unwrap();
    assert_eq!(
        (audit.action, audit.actor, audit.outcome),
        (
            AuditAction::ModeChange,
            Some(user(2)),
            AuditOutcome::Applied
        )
    );
    let settings = BucketEncryption::from_bytes(&writes[0].2).unwrap();
    assert_eq!(
        (settings.mode, settings.key_generation),
        (EncryptionMode::NodeManaged, 1)
    );
    assert_eq!(
        (settings.storage_generation, settings.cipher),
        (1, BlockCipher::Aes256Gcm)
    );

    // The node copy of a managed bucket rides the same transaction.
    let effects = operation.next(IoResult::Done);
    let Effect::Storage(StorageEffect::VaultWrite { entry, .. }) = asked(&effects) else {
        panic!("expected the node copy, got {effects:?}");
    };
    assert_eq!(entry.purpose, VaultPurpose::BucketKey);
    // The transition runs at once after the commit.
    let result = woke(operation.next(vault_written()));
    // The operation hands its only key handle on and keeps none.
    assert!(operation.private_key.is_none());
    assert_eq!(result.key.public_key, public);
    assert_eq!(public_key_of(result.private_key.bytes()), Some(public));
}

#[test]
fn locked_needs_recovery() {
    // One ready holder without a recovery code does not meet the rule.
    let lookups = BTreeMap::from([
        (user(1), keys(user(1), false)),
        (user(2), KeyLookup::Unavailable),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::VaultLocked, lookups));
    let (effects, _) = generated(&mut operation);
    let effects = operation.next(blob(BlobEvent::CopiesSealed {
        copies: sealed(&effects),
    }));
    assert_eq!(effects, Err(EnableError::RecoveryUnmet));
    assert!(operation.abort().is_empty());
    assert!(operation.private_key.is_none());

    let lookups = BTreeMap::from([
        (user(1), keys(user(1), true)),
        (user(2), KeyLookup::Missing),
    ]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::VaultLocked, lookups));
    let (effects, _) = generated(&mut operation);
    let effects = operation.next(blob(BlobEvent::CopiesSealed {
        copies: sealed(&effects),
    }));
    let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
        panic!("a holder with a recovery code meets the rule, got {effects:?}");
    };
    // A vault-locked bucket keeps no node copy.
    let record = BucketKeyRecord::from_bytes(&writes[1].2).unwrap();
    assert_eq!(record.vault_entry, None);
}

/// Starts the operation and answers the bucket read with `settings` and the link scan with no
/// rules.
fn bucket_read(
    operation: &mut EnableEncryptionOperation,
    settings: Option<BucketEncryption>,
) -> Turn {
    operation.first().unwrap();
    let values = authority_rows(&bucket(), settings.as_ref(), &[user(2)]);
    let effects = operation.next(keyed_rows(values));
    match operation.state {
        EnableState::CheckLinks => no_versions(operation),
        _ => effects,
    }
}

#[test]
fn mapped_refuses_encryption() {
    use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    operation.first().unwrap();
    let values = authority_rows(&bucket(), None, &[user(2)]);
    operation.next(keyed_rows(values)).unwrap();
    let rule = BackendImport {
        import_id: Ulid::from_bytes([5; 16]),
        group_id: Ulid::from_bytes([3; 16]),
        backend_id: Ulid::from_bytes([4; 16]),
        backend_prefix: "team-a-bucket/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: user(1),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: PLAIN_PATTERN.to_string(),
    };
    let effects = operation.next(page(
        vec![(rule.key(), rule.to_bytes().unwrap().into())],
        None,
    ));
    assert_eq!(effects, Err(EnableError::Mapped));
}

#[test]
fn decryption_blocks_enable() {
    use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, TRANSITION_KEYSPACE};
    use aruna_core::structs::storage::transition::{
        EncryptionTransition, TransitionKind, TransitionTarget,
    };
    // An `off` bucket that was encrypted keeps its id while the decryption still runs.
    let decrypted = BucketEncryption {
        bucket_id: Some(Ulid::from_bytes([6; 16])),
        key_generation: 1,
        ..Default::default()
    };
    let target = TransitionTarget {
        compression: Compression::Off,
        plan: None,
    };
    let source = BucketKeyRef::new(Ulid::from_bytes([6; 16]), 1);
    let kind = TransitionKind::Decrypt;
    let mut decrypt = EncryptionTransition::new(kind, Some(source), target, 1, 1);
    let read = |operation: &mut EnableEncryptionOperation, transition: &EncryptionTransition| {
        bucket_read(operation, Some(decrypted.clone())).unwrap();
        operation.next(page(Vec::new(), None)).unwrap();
        let effects = no_versions(operation);
        assert!(matches!(
            &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == TRANSITION_KEYSPACE
        ));
        operation.next(row(Some(transition.to_bytes().unwrap().into())))
    };

    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let effects = read(&mut operation, &decrypt);
    assert_eq!(effects, Err(EnableError::TransitionRunning));

    // Once the decryption finished, the bucket enables again with its next generation.
    decrypt.finished_at_ms = Some(9);
    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let effects = read(&mut operation, &decrypt);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Page { space, .. })) if space == BUCKET_HOLDER_KEYSPACE
    ));
}

#[test]
fn stale_generation_refused() {
    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    let stale = BucketEncryption {
        storage_generation: 4,
        ..Default::default()
    };
    // The caller read generation 0, so a concurrent change refuses the enable.
    let effects = bucket_read(&mut operation, Some(stale));
    assert!(matches!(
        effects,
        Err(EnableError::Key(BucketKeyError::StaleGeneration { .. }))
    ));
}

#[test]
fn open_uploads_conflict() {
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::multipart::MultipartUploadStatus;
    use std::collections::HashMap;

    let mut operation =
        EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
    bucket_read(&mut operation, None).unwrap();
    let upload = MultipartUpload {
        upload_id: Ulid::generate(),
        backend: BackendRef::node_default(),
        storage_class: None,
        bucket: "bucket".to_string(),
        key: "object".to_string(),
        group_id: Ulid::from_bytes([3; 16]),
        created_by: user(1),
        created_at: SystemTime::UNIX_EPOCH,
        status: MultipartUploadStatus::Open,
        checksum_hint: None,
        metadata: HashMap::new(),
        placement_policies: Vec::new(),
        subject_generation: 0,
        completing_since_ms: None,
        backend_upload: None,
        encryption: None,
    };
    let values = vec![(b"upload".to_vec().into(), upload.to_bytes().unwrap().into())];
    let effects = operation.next(page(values, None));
    assert_eq!(effects, Err(EnableError::OpenUploads));
}

#[test]
fn revoked_enabler_refused() {
    // The route saw user(2) as admin, but the role was revoked before the enable's transaction.
    let lookups = BTreeMap::from([(user(1), keys(user(1), true))]);
    let mut operation = EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, lookups));
    operation.first().unwrap();
    let values = authority_rows(&bucket(), None, &[]);
    let effects = operation.next(keyed_rows(values));
    assert!(matches!(effects, Err(EnableError::NotAdmin)));
}

#[test]
fn oversized_copy_refused() {
    use aruna_blob::blob::pithos::MAX_SIZE;
    use aruna_core::errors::BlobError;
    use aruna_core::keyspaces::BLOB_LOCATIONS_KEYSPACE;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobVersion, VersionKey,
    };
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
    use std::collections::HashMap;

    // A plain copy larger than any Pithos copy could never be moved by the encryption transition.
    let run = |size: u64| {
        let mut operation =
            EnableEncryptionOperation::new(input(EncryptionMode::NodeManaged, BTreeMap::new()));
        bucket_read(&mut operation, None).unwrap();
        operation.next(page(Vec::new(), None)).unwrap();
        let hash = [7u8; 32];
        let version = BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            user(1),
            None,
        );
        let key = VersionKey::new("bucket", "large", Ulid::from_bytes([8; 16]));
        let values = vec![(
            key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        )];
        let effects = operation.next(page(values, None));
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the size reads, got {effects:?}")
        };
        assert_eq!(reads[0].0, BLOB_LOCATIONS_KEYSPACE);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "large".to_string(),
            ulid: Ulid::from_bytes([9; 16]),
            format: StoredFormat::default(),
            created_by: user(1),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
        };
        let values = vec![(
            reads[0].1.clone(),
            Some(location.to_bytes().unwrap().into()),
        )];
        let effects = operation.next(keyed_rows(values));
        (operation, effects)
    };

    let (_, effects) = run(MAX_SIZE + 1);
    assert_eq!(
        effects,
        Err(EnableError::Blob(BlobError::SizeLimitExceeded {
            limit: MAX_SIZE
        }))
    );
    // A copy at the supported size still lets the bucket enable.
    let (_, effects) = run(MAX_SIZE);
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::GenerateBucketKey)
    ));
}

#[test]
fn managed_refuses_limit() {
    let mut limited = input(EncryptionMode::NodeManaged, BTreeMap::new());
    limited.max_unlock_ms = Some(60_000);
    let mut operation = EnableEncryptionOperation::new(limited);

    assert_eq!(
        operation.first(),
        Err(EnableError::Key(BucketKeyError::ManagedLimit))
    );
}
