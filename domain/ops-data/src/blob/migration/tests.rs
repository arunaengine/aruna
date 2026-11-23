//! Tests version re-encoding and storage moves against settings and version changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{
    COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE, MIGRATION_AHEAD_KEYSPACE,
    MOVE_HANDOVER_KEYSPACE, RENAME_MOVE_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::RenameRequest;
use aruna_core::structs::storage::format::{CompressionMigration, MigrationAhead};

use super::drain::{
    EndAbortedOperation, HandOverOperation, Handover, MIGRATION_TIME, MIGRATION_WORK,
    MigratePageOperation, MigrationRun, ProcessMigrationsOperation, RETRY_BASE, RETRY_MAX,
    RenameAccessOperation, SettleMoveOperation, decode_handover, decode_request, granted,
    handover_call, release_key,
};
use aruna_core::structs::storage::backend_import::WriteIntent;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_ops_identity::auth::request_authorization::BucketAccess;
use aruna_runner::driver::{drive, node_routing};

async fn row_value(
    context: &aruna_runner::driver::DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, String> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(other.label().to_string()),
    }
}

async fn process_migrations(
    context: &aruna_runner::driver::DriverContext,
    after: Option<Key>,
) -> Result<MigrationRun, String> {
    let run =
        ProcessMigrationsOperation::new(node_routing(context), context.node_and_realm(), after);
    drive(run, context).await.map_err(|error| error.to_string())
}

async fn read_progress(
    context: &aruna_runner::driver::DriverContext,
    key: &Key,
) -> Result<Option<CompressionMigration>, String> {
    let value = row_value(context, COMPRESSION_MIGRATION_KEYSPACE, key.to_vec()).await?;
    value
        .map(|value| CompressionMigration::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|error| error.to_string())
}

async fn read_ahead(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
) -> Result<Option<MigrationAhead>, String> {
    let key = bucket.as_bytes().to_vec();
    let value = row_value(context, MIGRATION_AHEAD_KEYSPACE, key).await?;
    value
        .map(|value| MigrationAhead::from_bytes(&value))
        .transpose()
        .map_err(|error| error.to_string())
}

async fn read_handover(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
) -> Result<Option<Handover>, String> {
    let key = bucket.as_bytes().to_vec();
    let value = row_value(context, MOVE_HANDOVER_KEYSPACE, key).await?;
    decode_handover(value).map_err(|error| error.to_string())
}

/// Writes or deletes the hand-over row, as a hand-over run does.
#[derive(Debug, PartialEq)]
struct StoreHandover(Option<IoCall>);

impl Operation for StoreHandover {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        self.0.take().map(Next::Call).ok_or(IoError::Unfinished)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

async fn store_handover(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
    handover: Option<&Handover>,
) -> Result<(), String> {
    let call = handover_call(bucket, handover).map_err(|error| error.to_string())?;
    drive(StoreHandover(Some(call)), context)
        .await
        .map_err(|error| error.to_string())
}

async fn rename_request(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
    record: &CompressionMigration,
) -> Result<Option<RenameRequest>, String> {
    let key = bucket.as_bytes().to_vec();
    let value = row_value(context, RENAME_MOVE_KEYSPACE, key).await?;
    decode_request(value, (record.moving, record.started_at_ms)).map_err(|error| error.to_string())
}

async fn rename_access(
    context: &aruna_runner::driver::DriverContext,
    request: &RenameRequest,
    routing: &RoutingSnapshot,
    bucket: &str,
) -> Result<Option<BucketAccess>, String> {
    let access = RenameAccessOperation::new(request, routing.group_id, bucket);
    drive(access, context)
        .await
        .map_err(|error| error.to_string())
}

async fn migrate_page(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
    record: CompressionMigration,
    budget: Duration,
) -> Result<Option<Duration>, String> {
    let page = MigratePageOperation::new(node_routing(context), bucket, record, budget);
    drive(page, context)
        .await
        .map_err(|error| error.to_string())
}

async fn end_aborted(
    context: &aruna_runner::driver::DriverContext,
    bucket: &str,
    record: CompressionMigration,
) -> Result<Option<Duration>, String> {
    let end = EndAbortedOperation::new(node_routing(context), bucket, record);
    drive(end, context).await.map_err(|error| error.to_string())
}

async fn settle_move(
    context: &aruna_runner::driver::DriverContext,
    version: &VersionKey,
    intents: &[(Vec<u8>, WriteIntent)],
) -> Result<bool, String> {
    let settle = SettleMoveOperation::new(node_routing(context), version, intents.to_vec());
    drive(settle, context)
        .await
        .map_err(|error| error.to_string())
}

#[test]
fn handover_reads_routing() {
    let mut hand = HandOverOperation::new(Default::default(), "b", 1);
    assert!(matches!(hand.first(), Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn retries_unavailable_policy() {
    use aruna_ops_identity::auth::error::AuthorizeError;
    use aruna_ops_identity::auth::error::PolicyEnforcementError;
    let unavailable = PolicyEnforcementError::Unavailable("down".to_string());
    assert!(granted::<()>(Err(AuthorizeError::Policy(unavailable))).is_err());
    let denied = PolicyEnforcementError::Denied {
        name: "p".to_string(),
        reason: "no".to_string(),
    };
    assert_eq!(granted::<()>(Err(AuthorizeError::Policy(denied))), Ok(None));
    assert_eq!(
        granted::<()>(Err(AuthorizeError::PermissionDenied)),
        Ok(None)
    );
    assert_eq!(granted(Ok(1)), Ok(Some(1)));
}
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendRef, ManagedCopyState};
use aruna_core::structs::storage::format::{FrameLayout, StoredFormat, StoredLayout};
use std::collections::HashMap;
use ulid::Ulid;

const ZSTD: Compression = Compression::Zstd { level: 3 };

fn location(framed: bool) -> BackendLocation {
    let mut format = StoredFormat::default();
    if framed {
        format.layout = StoredLayout::Frames(Box::new(FrameLayout {
            level: 3,
            frames: 1,
            stored_size: 20,
            index_hash: [0; 32],
        }));
    }
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "store".to_string(),
        backend_path: format!("b/k_{}", Ulid::generate()),
        ulid: Ulid::generate(),
        format,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 40,
        hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![4u8; 32])]),
    }
}

fn read_result(value: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![value.map(Into::into)])
}

fn blob_event(event: BlobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::blob(event)))
}

fn asked(turn: &Turn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

/// Turns `operation`, which waits for its publication, into that child and starts it.
fn enter_publish(operation: &mut MigrateVersionOperation) -> Turn {
    assert_eq!(operation.state, MigrateState::Publish);
    *operation = operation.publish_child();
    operation.first()
}

/// The calls a run that skipped the version makes after its end.
fn skipped_calls(turn: Turn) -> Vec<IoCall> {
    match turn {
        Ok(Next::Then {
            output: MigrateOutcome::Skipped,
            calls,
        }) => calls,
        Ok(Next::Done(MigrateOutcome::Skipped)) => Vec::new(),
        other => panic!("expected a skipped version, got {other:?}"),
    }
}

/// The written copy is released, and without an owning commit goes to the cleanup queue.
fn leaves_copy(calls: &[IoCall], cleanup: bool) {
    let IoCall::Effect(release) = &calls[0] else {
        panic!("expected the release, got {calls:?}")
    };
    assert!(matches!(
        (**release).as_blob(),
        Some(BlobEffect::ReleaseReservation { .. })
    ));
    assert_eq!(calls.len(), 1 + usize::from(cleanup));
    if cleanup {
        assert_eq!(calls[1], IoCall::from(schedule_cleanup_effect()));
    }
}

/// The publication's read of the bucket and its encryption settings.
fn bucket_read(compression: Compression, settings: Option<BucketEncryption>) -> IoResult {
    let settings = settings.map(|settings| settings.to_bytes().unwrap().into());
    IoResult::Values(vec![Some(bucket(compression).into()), settings])
}

fn bucket(compression: Compression) -> Vec<u8> {
    BucketInfo {
        group_id: Ulid::from_bytes([1; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression,
        distribution: None,
    }
    .to_bytes()
    .unwrap()
}

/// Drives the operation up to the transaction that publishes the new copy.
fn written(operation: &mut MigrateVersionOperation, version: &BlobVersion) -> Turn {
    operation.first().unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();
    operation
        .next(read_result(Some(location(false).to_bytes().unwrap())))
        .unwrap();
    operation.next(read_result(None)).unwrap();
    let blob = BackendStream::new(futures_util::stream::empty::<
        Result<bytes::Bytes, std::io::Error>,
    >());
    let turn = operation.next(blob_event(BlobEvent::ReadFinished {
        blob,
        stream_size: 40,
    }));
    let Some(BlobEffect::Write { resolved, .. }) = asked(&turn).as_blob() else {
        panic!("expected one write, got {turn:?}")
    };
    assert_eq!(resolved.compression, ZSTD);
    operation
        .next(blob_event(BlobEvent::WriteFinished {
            location: location(true),
        }))
        .unwrap();
    enter_publish(operation)
}

fn raw_version() -> BlobVersion {
    BlobVersion::materialized(
        [4u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        Default::default(),
        None,
    )
}

fn operation() -> MigrateVersionOperation {
    let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
    MigrateVersionOperation::new(key, ZSTD, SystemTime::UNIX_EPOCH)
}

#[test]
fn skips_encoded_version() {
    let mut operation = operation();
    let mut version = raw_version();
    if let BlobVersionState::Materialized { encoding, .. } = &mut version.state {
        *encoding = EncodingClass::Zstd { level: 3 };
    }
    operation.first().unwrap();

    let turn = operation.next(read_result(Some(version.to_bytes().unwrap())));

    assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
}

#[test]
fn publishes_new_copy() {
    let mut operation = operation();
    let version = raw_version();
    written(&mut operation, &version).unwrap();
    operation.next(bucket_read(ZSTD, None)).unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();

    let effects = operation.next(read_result(None));

    let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
        panic!("expected the row writes, got {effects:?}")
    };
    let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [
            BLOB_LOCATIONS_KEYSPACE,
            BLOB_VERSIONS_KEYSPACE,
            BLOB_RECLAIM_KEYSPACE
        ]
    );
    let stored = BlobVersion::from_bytes(&writes[1].2).unwrap();
    let class = stored.location_key().unwrap().encoding;
    assert_eq!(class, EncodingClass::Zstd { level: 3 });
    let old = ReclaimCandidateKey::from_bytes(&writes[2].1).unwrap();
    assert_eq!(old.encoding, EncodingClass::Raw);

    // The commit makes the copy the row's owner, so it is only released afterwards.
    operation.usage = None;
    let Ok(Next::Then { output, calls }) = operation.update_usage() else {
        panic!("expected the result with its release")
    };
    assert_eq!(output, MigrateOutcome::Migrated);
    leaves_copy(&calls, false);
}

#[test]
fn failed_publish_releases() {
    // A failed publication leaves the copy to the parent, which releases it once; the
    // copy no commit owns goes to the cleanup queue.
    let mut parent = operation();
    let version = raw_version();
    parent.first().unwrap();
    parent
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();
    parent
        .next(read_result(Some(location(false).to_bytes().unwrap())))
        .unwrap();
    parent.state = MigrateState::Publish;
    parent.new = Some(location(true));
    let mut child = parent.publish_child();
    assert_eq!(child.transaction(), TransactionMode::Write);
    assert!(child.abort().is_empty());
    let error = MigrateVersionError::TargetMissing;
    let failed = IoResult::Child(ChildOutcome::new(Err::<MigrateOutcome, _>(error)));
    assert_eq!(parent.next(failed), Err(MigrateVersionError::TargetMissing));
    leaves_copy(&parent.abort(), true);
    assert!(parent.abort().is_empty());

    // After a published version the child already released the copy.
    let mut parent = operation();
    parent.state = MigrateState::Publish;
    parent.new = Some(location(true));
    let done = IoResult::Child(ChildOutcome::new(Ok::<_, MigrateVersionError>(
        MigrateOutcome::Migrated,
    )));
    let turn = parent.next(done);
    assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Migrated)));
    assert!(parent.abort().is_empty());
}

#[test]
fn adopts_existing_copy() {
    // Another version already moved this content: nothing is read or written again.
    let mut operation = operation();
    let version = raw_version();
    operation.first().unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();
    operation
        .next(read_result(Some(location(false).to_bytes().unwrap())))
        .unwrap();

    let turn = operation.next(read_result(Some(location(true).to_bytes().unwrap())));

    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    enter_publish(&mut operation).unwrap();
    operation.next(bucket_read(ZSTD, None)).unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();
    let effects = operation.next(read_result(Some(location(true).to_bytes().unwrap())));
    let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
        panic!("expected the row writes, got {effects:?}")
    };
    let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(spaces, [BLOB_VERSIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE]);
}

#[test]
fn governed_moves_registration() {
    // The registration follows the copy in the same transaction as the version.
    let mut operation = operation();
    let version = raw_version()
        .with_policies(vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([3; 16]),
            digest: [4; 32],
        }])
        .unwrap();
    written(&mut operation, &version).unwrap();
    operation.next(bucket_read(ZSTD, None)).unwrap();
    let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
    let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
        panic!("expected the registration read, got {effects:?}")
    };
    assert_eq!(reads[0].0, MANAGED_COPY_KEYSPACE);
    let old = operation.old.clone().unwrap();
    let copy = ManagedCopyRecord::new(
        operation.version_key.clone(),
        iroh::SecretKey::from_bytes(&[9; 32]).public(),
        old,
        version.placement_policies.clone(),
        7,
        ManagedCopyState::Registered,
    )
    .unwrap();
    operation
        .next(read_result(Some(copy.to_bytes().unwrap())))
        .unwrap();

    let effects = operation.next(read_result(None));

    let Ok(Next::Call(IoCall::Write(writes))) = &effects else {
        panic!("expected the row writes, got {effects:?}")
    };
    let (space, _, value) = &writes[1];
    assert_eq!(space, MANAGED_COPY_KEYSPACE);
    let moved = ManagedCopyRecord::from_bytes(value).unwrap();
    assert!(moved.location.same_object(operation.new.as_ref().unwrap()));
    assert_eq!(moved.policies, copy.policies);
}

#[test]
fn unserveable_copy_fails() {
    // A copy waiting for revalidation is a failure, so a later retry pass moves it.
    let mut operation = operation();
    let version = raw_version()
        .with_policies(vec![PlacementPolicyRef {
            policy_id: Ulid::from_bytes([3; 16]),
            digest: [4; 32],
        }])
        .unwrap();
    written(&mut operation, &version).unwrap();
    operation.next(bucket_read(ZSTD, None)).unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();

    let turn = operation.next(read_result(None));

    assert_eq!(
        turn,
        Err(MigrateVersionError::ManagedCopy(
            ManagedCopyError::Unregistered
        ))
    );
}

#[test]
fn retry_waits_double() {
    let retry_delay = |retries| capped_backoff(RETRY_BASE, RETRY_MAX, retries, 0);
    assert_eq!(retry_delay(0), RETRY_BASE);
    assert_eq!(retry_delay(1), 2 * RETRY_BASE);
    assert_eq!(retry_delay(6), RETRY_MAX);
    assert_eq!(retry_delay(u32::MAX), RETRY_MAX);
}

#[test]
fn stale_setting_discards() {
    // The setting changed again while the copy was written: nothing is published
    // and the written copy goes back to the cleanup queue.
    let mut operation = operation();
    written(&mut operation, &raw_version()).unwrap();

    let turn = operation.next(bucket_read(Compression::Off, None));

    leaves_copy(&skipped_calls(turn), true);
}

#[test]
fn encrypting_bucket_skips() {
    // Encryption was enabled while this compression pass waited: a plain copy is never
    // published for the version, and the written copy goes back to the cleanup queue.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let mut operation = operation();
    written(&mut operation, &raw_version()).unwrap();
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_bytes([3; 16])),
        key_generation: 1,
        ..Default::default()
    };

    let turn = operation.next(bucket_read(ZSTD, Some(settings)));

    leaves_copy(&skipped_calls(turn), true);
}

#[test]
fn encrypting_adoption_skips() {
    // A matching plain copy appeared after encryption was enabled: the retry never adopts
    // it for this version.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let mut operation = operation();
    let version = raw_version();
    operation.first().unwrap();
    operation
        .next(read_result(Some(version.to_bytes().unwrap())))
        .unwrap();
    operation
        .next(read_result(Some(location(false).to_bytes().unwrap())))
        .unwrap();
    operation
        .next(read_result(Some(location(true).to_bytes().unwrap())))
        .unwrap();
    enter_publish(&mut operation).unwrap();
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::from_bytes([3; 16])),
        key_generation: 1,
        ..Default::default()
    };

    let turn = operation.next(bucket_read(ZSTD, Some(settings)));

    assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
}

#[tokio::test]
async fn encryption_retires_compression() {
    // A compression pass waiting for its retry is finished once its bucket encrypts.
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::driver::DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let mut record = CompressionMigration::new(ZSTD, 1);
    record.failed = 2;
    record.retry_at_ms = Some(2);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(Ulid::from_bytes([3; 16])),
        key_generation: 1,
        ..Default::default()
    };
    for (key_space, value) in [
        (COMPRESSION_QUEUE_KEYSPACE, Vec::new()),
        (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
        (BUCKET_ENCRYPTION_KEYSPACE, settings.to_bytes().unwrap()),
        (RENAME_MOVE_KEYSPACE, b"1".to_vec()),
    ] {
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: b"b".to_vec().into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }

    // Resuming after "a" skips the transition queue, which this test does not seed.
    process_migrations(&context, Some(b"a".to_vec().into()))
        .await
        .unwrap();

    let stored = read_progress(&context, &b"b".to_vec().into())
        .await
        .unwrap()
        .unwrap();
    assert!(stored.finished_at_ms.is_some());
    assert_eq!(stored.retry_at_ms, None);
    // The finished move takes its rename request along.
    let marker = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: RENAME_MOVE_KEYSPACE.to_string(),
            key: b"b".to_vec().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        marker,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[tokio::test]
async fn checks_rename_caller() {
    // The rename's caller is read with its move, and each key is checked against them.
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::object_permission_path;
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let (realm, group) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(1, 1));
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let secret = object_permission_path(realm, group, node, "b", "secret/**");
    let auth = aruna_runner::testing::seed_access(&context, (realm, group, node), &secret).await;
    let mut record = CompressionMigration::new(Compression::Off, 1);
    (record.moving, record.started_at_ms) = (true, 7);
    let grant = aruna_core::structs::storage::backend_import::TokenGrant {
        token_hash: "hash".to_string(),
        issuer: realm,
        issued_at_secs: 1,
        expires_at_secs: u64::MAX,
        via: None,
    };
    let request = RenameRequest {
        started_at_ms: 7,
        auth,
        node_id: node,
        grant,
        paused: false,
        aborted_at_ms: None,
    };
    let write = StorageEffect::Write {
        key_space: RENAME_MOVE_KEYSPACE.to_string(),
        key: b"b".to_vec().into(),
        value: request.to_bytes().unwrap().into(),
        txn_id: None,
    };
    storage.send_storage_effect(write).await;
    let found = rename_request(&context, "b", &record).await.unwrap();
    assert_eq!(found.as_ref(), Some(&request));
    // A later move of the bucket is not the one the caller asked to rename.
    record.started_at_ms = 8;
    assert_eq!(rename_request(&context, "b", &record).await.unwrap(), None);
    let routing = RoutingSnapshot::single(group);
    let access = rename_access(&context, &request, &routing, "b").await;
    let access = access.unwrap().unwrap();
    let write = aruna_core::structs::identity::auth::Permission::WRITE;
    assert!(access.allows(Some("in/a.csv"), &write));
    assert!(!access.allows(Some("secret/a.csv"), &write));
    // An expired token renames nothing more.
    let mut expired = request.clone();
    expired.grant.expires_at_secs = 1;
    let access = rename_access(&context, &expired, &routing, "b").await;
    assert!(access.unwrap().is_none());
}

#[tokio::test]
async fn passes_skipped_versions() {
    // Versions with nothing to do do not use up a run; only the others count toward it, and
    // rows left go on at once while a failure pass waits for its retry.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let record = CompressionMigration::new(ZSTD, 1);
    let put = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let progress = record.to_bytes().unwrap();
    let seeded = put(COMPRESSION_MIGRATION_KEYSPACE, b"b".to_vec(), progress);
    storage.send_storage_effect(seeded).await;
    let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let row = |index: usize| VersionKey::new("b", format!("k{index:03}"), Ulid::from_parts(1, 1));
    for index in 0..200 + MIGRATION_WORK + 44 {
        // Deleted versions are skipped; the later rows cannot be read and fail.
        let value = match index < 200 {
            true => deleted.to_bytes().unwrap(),
            false => b"unreadable".to_vec(),
        };
        let write = put(
            BLOB_VERSIONS_KEYSPACE,
            row(index).to_bytes().unwrap(),
            value,
        );
        storage.send_storage_effect(write).await;
    }

    let after = migrate_page(&context, "b", record, Duration::MAX).await;
    let after = after.unwrap();

    assert_eq!(after, Some(Duration::ZERO));
    let key: Key = b"b".to_vec().into();
    let stored = read_progress(&context, &key).await.unwrap().unwrap();
    assert_eq!(
        (stored.skipped, stored.failed),
        (200, MIGRATION_WORK as u64)
    );
    let last = row(200 + MIGRATION_WORK - 1).to_bytes().unwrap();
    assert_eq!(stored.cursor, Some(last));
    let after = migrate_page(&context, "b", stored, Duration::MAX).await;
    let after = after.unwrap();
    assert_eq!(after, Some(capped_backoff(RETRY_BASE, RETRY_MAX, 0, 0)));
    let stored = read_progress(&context, &key).await.unwrap().unwrap();
    assert_eq!(stored.failed, MIGRATION_WORK as u64 + 44);
    assert!(stored.retry_at_ms.is_some());
}

#[tokio::test]
async fn stores_progress_often() {
    // A run that used up its time stores its progress after the version it started, and
    // the next run goes on at once after it.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let record = CompressionMigration::new(ZSTD, 1);
    let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
    let rows = [0, 1, 2].map(|index| (row(index).to_bytes().unwrap(), deleted.to_bytes()));
    let progress = [(b"b".to_vec(), record.to_bytes().unwrap())];
    let writes = rows.into_iter().map(|(key, value)| (key, value.unwrap()));
    for (key_space, (key, value)) in std::iter::repeat_n(BLOB_VERSIONS_KEYSPACE, 3)
        .zip(writes)
        .chain([(COMPRESSION_MIGRATION_KEYSPACE, progress[0].clone())])
    {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
    }
    let key: Key = b"b".to_vec().into();
    let mut stored = record;
    for index in 0..3 {
        let after = migrate_page(&context, "b", stored, Duration::ZERO).await;
        assert_eq!(after.unwrap(), (index < 2).then_some(Duration::ZERO));
        stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(stored.skipped, index + 1);
        assert_eq!(stored.cursor, Some(row(index).to_bytes().unwrap()));
    }
}

#[tokio::test]
async fn counts_rows_once() {
    // Rows a run finished after its cursor count once: the next run of the same pass
    // passes over them, and a row of another pass or cursor is not trusted.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
    let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    for index in 0..3 {
        let version = row(index).to_bytes().unwrap();
        let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
        storage.send_storage_effect(effect).await;
    }
    let key: Key = b"b".to_vec().into();
    for (started_at_ms, skipped) in [(1, 2), (2, 3)] {
        let record = CompressionMigration::new(ZSTD, 1);
        let progress = record.to_bytes().unwrap();
        let effect = write(COMPRESSION_MIGRATION_KEYSPACE, b"b".to_vec(), progress);
        storage.send_storage_effect(effect).await;
        let ahead = MigrationAhead {
            started_at_ms,
            rows: vec![row(1).to_bytes().unwrap()],
            ..MigrationAhead::default()
        };
        let ahead = ahead.to_bytes().unwrap();
        storage
            .send_storage_effect(write(MIGRATION_AHEAD_KEYSPACE, b"b".to_vec(), ahead))
            .await;
        let after = migrate_page(&context, "b", record, Duration::MAX).await;
        assert_eq!(after.unwrap(), None);
        let stored = read_progress(&context, &key).await.unwrap().unwrap();
        assert_eq!(stored.skipped, skipped);
        assert_eq!(stored.cursor, Some(row(2).to_bytes().unwrap()));
        assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
    }
}

#[tokio::test]
async fn survives_lost_progress() {
    // Rows ahead are stored with the progress: a run whose progress is lost leaves them as
    // they were, so the run that repeats it counts each row once.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let row = |index: u64| VersionKey::new("b", format!("k{index}"), Ulid::from_parts(1, 1));
    let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    for index in 0..3 {
        let version = row(index).to_bytes().unwrap();
        let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
        storage.send_storage_effect(effect).await;
    }
    // An earlier run counted k0 and k2 while k1 still ran.
    let mut record = CompressionMigration::new(ZSTD, 1);
    (record.cursor, record.skipped) = (Some(row(0).to_bytes().unwrap()), 2);
    let ahead = MigrationAhead {
        started_at_ms: 1,
        cursor: record.cursor.clone(),
        rows: vec![row(2).to_bytes().unwrap()],
        ..MigrationAhead::default()
    };
    let ahead = ahead.to_bytes().unwrap();
    storage
        .send_storage_effect(write(MIGRATION_AHEAD_KEYSPACE, b"b".to_vec(), ahead))
        .await;
    // Without its progress row the next run stores nothing, as if it stopped before storing.
    let key: Key = b"b".to_vec().into();
    let lost = migrate_page(&context, "b", record.clone(), Duration::MAX).await;
    assert_eq!(lost.unwrap(), None);
    assert_eq!(read_progress(&context, &key).await.unwrap(), None);
    let progress = record.to_bytes().unwrap();
    storage
        .send_storage_effect(write(
            COMPRESSION_MIGRATION_KEYSPACE,
            b"b".to_vec(),
            progress,
        ))
        .await;
    let after = migrate_page(&context, "b", record, Duration::MAX).await;
    assert_eq!(after.unwrap(), None);
    let stored = read_progress(&context, &key).await.unwrap().unwrap();
    assert_eq!(stored.skipped, 3);
    assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
}

#[tokio::test]
async fn resumes_without_gap() {
    // The version a run left unfinished is deleted during the pause; the two versions of one
    // key it finished after it now open the next run and still count once.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let deleted = BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default());
    let row = |at: u64| VersionKey::new("b", "k1", Ulid::from_parts(at, 1));
    let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    for at in [1, 2] {
        let version = row(at).to_bytes().unwrap();
        let effect = write(BLOB_VERSIONS_KEYSPACE, version, deleted.to_bytes().unwrap());
        storage.send_storage_effect(effect).await;
    }
    let mut record = CompressionMigration::new(ZSTD, 1);
    record.skipped = 2;
    let ahead = MigrationAhead {
        started_at_ms: 1,
        rows: [1, 2].map(|at| row(at).to_bytes().unwrap()).to_vec(),
        ..MigrationAhead::default()
    };
    let writes = [
        (MIGRATION_AHEAD_KEYSPACE, ahead.to_bytes().unwrap()),
        (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
    ];
    for (key_space, value) in writes {
        storage
            .send_storage_effect(write(key_space, b"b".to_vec(), value))
            .await;
    }
    let after = migrate_page(&context, "b", record, Duration::MAX).await;
    assert_eq!(after.unwrap(), None);
    let key: Key = b"b".to_vec().into();
    let stored = read_progress(&context, &key).await.unwrap().unwrap();
    assert_eq!(stored.skipped, 2);
    assert_eq!(read_ahead(&context, "b").await.unwrap(), None);
}

#[tokio::test]
async fn ends_aborted_rename() {
    // An aborted rename ends its move with its counts and keeps the request as the abort.
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let mut record = CompressionMigration::new(Compression::Off, 7);
    (record.moving, record.migrated) = (true, 3);
    let info = BucketInfo {
        group_id: Ulid::from_parts(1, 1),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let request = RenameRequest {
        started_at_ms: 7,
        auth: aruna_core::structs::identity::auth::AuthContext {
            user_id: Default::default(),
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            path_restrictions: None,
            session: None,
        },
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        grant: aruna_core::structs::storage::backend_import::TokenGrant {
            token_hash: "hash".to_string(),
            issuer: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        },
        paused: true,
        aborted_at_ms: Some(9),
    };
    for (key_space, value) in [
        (S3_BUCKET_KEYSPACE, info.to_bytes().unwrap()),
        (COMPRESSION_QUEUE_KEYSPACE, Vec::new()),
        (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
        (RENAME_MOVE_KEYSPACE, request.to_bytes().unwrap()),
    ] {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: b"b".to_vec().into(),
            value: value.into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
    }

    let after = migrate_page(&context, "b", record, MIGRATION_TIME).await;
    assert_eq!(after.unwrap(), None);

    let key: Key = b"b".to_vec().into();
    let stored = read_progress(&context, &key).await.unwrap().unwrap();
    assert!(stored.finished_at_ms.is_some());
    assert_eq!(stored.migrated, 3);
    let read = |key_space: &str| StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.clone(),
        txn_id: None,
    };
    let kept = storage
        .send_storage_effect(read(RENAME_MOVE_KEYSPACE))
        .await;
    assert!(matches!(
        kept,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
    let queued = storage
        .send_storage_effect(read(COMPRESSION_QUEUE_KEYSPACE))
        .await;
    assert!(matches!(
        queued,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
}

#[test]
fn keys_copy_releases() {
    // A replayed settlement writes the release of one copy version at one key only.
    let row = intent_key(
        Ulid::from_parts(2, 2),
        "raw/a.csv~X",
        Ulid::from_parts(9, 9),
    );
    assert_eq!(release_key(&row, "v1"), release_key(&row, "v1"));
    assert_ne!(release_key(&row, "v1"), release_key(&row, "v2"));
    assert_ne!(release_key(&row, "v1"), row.to_vec());
}

#[tokio::test]
async fn settles_before_abort() {
    // An aborted rename hands each version with a move intent to the cleanup queue, which
    // runs it again until its intents are settled.
    use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
    let storage = aruna_storage::storage::memory::MemoryStorage::open();
    let context = aruna_runner::testing::test_context(storage.clone());
    let group = Ulid::from_parts(1, 1);
    let rule = BackendImport {
        import_id: Ulid::from_parts(2, 2),
        group_id: group,
        backend_id: Ulid::from_parts(3, 3),
        backend_prefix: "raw/".to_string(),
        bucket: "b".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: DEFAULT_PATTERN.to_string(),
    };
    let info = BucketInfo {
        group_id: group,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let id = Ulid::from_parts(9, 9);
    let intent = WriteIntent {
        key: "a.csv".to_string(),
        version_id: id,
        backend: BackendRef::Group(rule.backend_id),
        created_by: Default::default(),
        written: Some([1; 32]),
        mover: Some(7),
    };
    let write = |key_space: &str, key: Vec<u8>, value: Vec<u8>| StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let intent_row = intent_key(rule.import_id, "raw/a.csv~X", id).to_vec();
    let version = VersionKey::new("b", "a.csv", id).to_bytes().unwrap();
    let backend = aruna_core::structs::storage::group_backend::GroupStorage {
        backend_id: rule.backend_id,
        group_id: group,
        name: "tenant".to_string(),
        kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
        public_config: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        disabled: false,
        cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
    };
    for effect in [
        write(
            aruna_core::keyspaces::BACKEND_INDEX_KEYSPACE,
            aruna_ops_identity::groups::backends::index_key(group, rule.backend_id).to_vec(),
            backend.to_bytes().unwrap(),
        ),
        write(S3_BUCKET_KEYSPACE, b"b".to_vec(), info.to_bytes().unwrap()),
        write(
            BACKEND_IMPORT_KEYSPACE,
            rule.key().to_vec(),
            rule.to_bytes().unwrap(),
        ),
        write(
            IMPORT_INTENT_KEYSPACE,
            intent_row.clone(),
            intent.to_bytes().unwrap(),
        ),
        // A version row that cannot be read makes its run fail.
        write(BLOB_VERSIONS_KEYSPACE, version.clone(), vec![0xff; 3]),
    ] {
        storage.send_storage_effect(effect).await;
    }
    let mut record = CompressionMigration::new(Compression::Off, 7);
    record.moving = true;
    let key: Key = b"b".to_vec().into();
    let stored = write(
        COMPRESSION_MIGRATION_KEYSPACE,
        b"b".to_vec(),
        record.to_bytes().unwrap(),
    );
    storage.send_storage_effect(stored).await;

    // An upload's intent with its hash is not the move's and stays where it is.
    let upload = WriteIntent {
        mover: None,
        ..intent.clone()
    };
    let upload_row = intent_key(rule.import_id, "raw/b.csv", id).to_vec();
    let stored = write(
        IMPORT_INTENT_KEYSPACE,
        upload_row.clone(),
        upload.to_bytes().unwrap(),
    );
    storage.send_storage_effect(stored).await;
    // A hand-over stopped in a rule removed since goes on with the next rule.
    let place = Handover {
        started_ms: 7,
        import_id: Some(Ulid::from_parts(1, 1)),
        after: Some(b"zz".to_vec()),
    };
    store_handover(&context, "b", Some(&place)).await.unwrap();
    // The abort hands the version over to the cleanup queue and ends the move.
    assert_eq!(end_aborted(&context, "b", record).await.unwrap(), None);
    assert!(read_handover(&context, "b").await.unwrap().is_none());
    let read = StorageEffect::Read {
        key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
        key: upload_row.into(),
        txn_id: None,
    };
    assert!(matches!(
        storage.send_storage_effect(read).await,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));
    let ended = read_progress(&context, &key).await.unwrap().unwrap();
    assert!(ended.finished_at_ms.is_some());
    let read = StorageEffect::Read {
        key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
        key: intent_row.clone().into(),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(work), ..
    }) = storage.send_storage_effect(read).await
    else {
        panic!("expected the handed over move")
    };
    let BlobCleanupWork::SettleMove {
        version: moved,
        intents,
    } = BlobCleanupWork::from_bytes(&work).unwrap()
    else {
        panic!("expected a move to settle")
    };
    assert_eq!(moved, VersionKey::new("b", "a.csv", id));
    // A version that cannot run keeps its intents for a later pass.
    assert!(!settle_move(&context, &moved, &intents).await.unwrap());
    // Once the version is gone, its copy must be checked before its intents go; without
    // a blob storage to ask, the move stays unsettled and keeps them.
    let delete = StorageEffect::Delete {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: version.into(),
        txn_id: None,
    };
    storage.send_storage_effect(delete).await;
    assert!(!settle_move(&context, &moved, &intents).await.unwrap());
    let read = StorageEffect::Read {
        key_space: IMPORT_INTENT_KEYSPACE.to_string(),
        key: intents[0].0.clone().into(),
        txn_id: None,
    };
    assert!(matches!(
        storage.send_storage_effect(read).await,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
}

#[test]
fn rejects_wrong_event() {
    let mut operation = operation();
    operation.state = MigrateState::ProbeTarget;

    let turn = operation.next(blob_event(BlobEvent::DeleteFinished));

    assert!(matches!(
        turn,
        Err(MigrateVersionError::InvalidStateEvent { .. })
    ));
}

mod moving {
    use super::*;
    use aruna_core::keyspaces::IMPORTED_OBJECT_KEYSPACE;
    use aruna_core::structs::storage::backend_import::{DEFAULT_PATTERN, tagged_path};
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingTarget,
    };
    use std::collections::BTreeSet;

    const GROUP: Ulid = Ulid::from_parts(1, 1);
    const STORAGE: Ulid = Ulid::from_parts(5, 5);
    const IMPORT: Ulid = Ulid::from_parts(4, 4);

    fn storage() -> BackendRef {
        BackendRef::Group(STORAGE)
    }

    fn rule() -> BackendImport {
        BackendImport {
            import_id: IMPORT,
            group_id: GROUP,
            backend_id: STORAGE,
            backend_prefix: "raw/".to_string(),
            bucket: "b".to_string(),
            key_prefix: "in/".to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
        }
    }

    /// A group whose default rule names its own storage, with or without a linked rule.
    fn routing(imports: Vec<BackendImport>) -> RoutingSnapshot {
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        RoutingSnapshot::new(GROUP, catalog).with_group_inputs(GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(storage())),
            backend_ids: BTreeSet::from([STORAGE]),
            imports,
            ..Default::default()
        })
    }

    fn operation(key: &str, imports: Vec<BackendImport>) -> MigrateVersionOperation {
        let key = VersionKey::new("b", key, Ulid::from_bytes([2; 16]));
        MigrateVersionOperation::new(key, Compression::Off, SystemTime::UNIX_EPOCH)
            .with_routing(routing(imports))
    }

    fn on_storage(mut location: BackendLocation) -> BackendLocation {
        location.backend = storage();
        location
    }

    fn empty_blob() -> IoResult {
        let blob = BackendStream::new(futures_util::stream::empty::<
            Result<bytes::Bytes, std::io::Error>,
        >());
        blob_event(BlobEvent::ReadFinished {
            blob,
            stream_size: 40,
        })
    }

    /// Answers the publication's rule reads: bucket, encryption, group default, then the
    /// group's import rules; returns what follows them.
    fn rules_read(
        operation: &mut MigrateVersionOperation,
        default: Option<RoutingTarget>,
        rule: impl IntoIterator<Item = BackendImport>,
    ) -> Turn {
        let group = GroupStorageRouting {
            group_id: GROUP,
            default_target: default,
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: Default::default(),
        };
        let values = vec![
            Some(bucket(Compression::Off).into()),
            None,
            Some(group.to_bytes().unwrap().into()),
        ];
        let turn = operation.next(IoResult::Values(values));
        let Ok(Next::Call(IoCall::Page { space, .. })) = &turn else {
            panic!("expected the import rule scan, got {turn:?}")
        };
        assert_eq!(space, BACKEND_IMPORT_KEYSPACE);
        let rows = (rule.into_iter())
            .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
            .collect();
        operation.next(IoResult::Page { rows, next: None })
    }

    fn storage_record(disabled: bool) -> IoResult {
        let record = aruna_core::structs::storage::group_backend::GroupStorage {
            backend_id: STORAGE,
            group_id: GROUP,
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled,
            cleanup: Default::default(),
        };
        read_result(Some(record.to_bytes().unwrap()))
    }

    /// Answers the rule reads and the target storage fence, up to the version read.
    fn published(operation: &mut MigrateVersionOperation, rule: Option<BackendImport>) {
        let default = Some(RoutingTarget::Backend(storage()));
        let turn = rules_read(operation, default, rule);
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the storage fence, got {turn:?}")
        };
        assert_eq!(reads[0].0, aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE);
        operation.next(storage_record(false)).unwrap();
    }

    /// Answers the file's write with the location the adapter reports.
    fn plain_written(plain: &PlainPath) -> IoResult {
        let mut written = on_storage(location(false));
        (written.storage_bucket, written.backend_path) = (String::new(), plain.path.clone());
        written.ulid = plain.ulid;
        written.format.layout = StoredLayout::Plain(plain.ulid);
        blob_event(BlobEvent::PlainWritten {
            location: written,
            version: None,
            etag: None,
        })
    }

    /// Answers the stored and synced intent before a linked write, and returns the write.
    fn past_intent(operation: &mut MigrateVersionOperation, turn: Turn) -> Turn {
        let Ok(Next::Call(IoCall::Child(_))) = &turn else {
            return turn;
        };
        assert_eq!(operation.state, MigrateState::WriteIntent);
        // The source bytes are known, so the intent proves the copy before it is written.
        let target = operation.plain.clone().unwrap();
        let intent = operation.write_intent(&target, Default::default());
        let source = operation.old.as_ref().and_then(|old| old.get_blake3());
        assert_eq!(intent.written.as_ref().map(|hash| &hash[..]), source);
        let stored = Ok::<_, StorageError>(Fenced::Written);
        let turn = operation.next(IoResult::Child(ChildOutcome::new(stored)));
        assert_eq!(turn, Ok(Next::Call(IoCall::Sync)));
        operation.next(IoResult::Done)
    }

    /// Drives a move of `version` from Aruna storage up to its publication reads; a linked
    /// target finds its path taken when `taken`.
    fn copied(operation: &mut MigrateVersionOperation, version: &BlobVersion, taken: bool) -> Turn {
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        let turn = operation.next(read_result(Some(location(false).to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the target row read, got {turn:?}")
        };
        let key = BlobLocationKey::from_bytes(&reads[0].1).unwrap();
        assert_eq!(key.backend, storage());
        let mut turn = operation.next(read_result(None));
        if let Ok(Next::Call(IoCall::Read(reads))) = &turn {
            assert_eq!(reads[0].0, IMPORTED_OBJECT_KEYSPACE);
            turn = operation.next(read_result(None));
        }
        if let Some(BlobEffect::StatPlain { .. }) = asked(&turn).as_blob() {
            turn = operation.next(blob_event(BlobEvent::PlainStat {
                exists: taken,
                version: None,
            }));
        }
        assert!(matches!(
            (asked(&turn)).as_blob(),
            Some(BlobEffect::Read { .. })
        ));
        let turn = operation.next(empty_blob());
        let turn = past_intent(operation, turn);
        let new = match asked(&turn).as_blob() {
            Some(BlobEffect::Write { resolved, .. }) => {
                assert_eq!(resolved.backend, storage());
                blob_event(BlobEvent::WriteFinished {
                    location: on_storage(location(false)),
                })
            }
            Some(BlobEffect::WritePlain { plain, .. }) => plain_written(plain),
            other => panic!("expected a write, got {other:?}"),
        };
        operation.next(new).unwrap();
        enter_publish(operation)
    }

    /// Answers the publication's read of the target row, with the rows the rules keep of the
    /// path absent.
    fn answer_target(
        operation: &mut MigrateVersionOperation,
        turn: Turn,
        value: Option<Vec<u8>>,
    ) -> Turn {
        let Ok(Next::Call(IoCall::Read(reads))) = &turn else {
            panic!("expected the target read, got {turn:?}")
        };
        let mut values = vec![None; reads.len()];
        values[0] = value.map(Into::into);
        operation.next(IoResult::Values(values))
    }

    fn batch(turn: &Turn) -> &Vec<(String, Key, Value)> {
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected the row writes, got {turn:?}")
        };
        writes
    }

    #[test]
    fn moves_to_storage() {
        // The version names the copy on the group's storage; the old copy is reclaimed.
        let mut operation = operation("k", Vec::new());
        let version = raw_version();
        copied(&mut operation, &version, false).unwrap();
        published(&mut operation, None);
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, None);

        let writes = batch(&effects);
        let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        let expected = [
            BLOB_LOCATIONS_KEYSPACE,
            BLOB_VERSIONS_KEYSPACE,
            BLOB_RECLAIM_KEYSPACE,
        ];
        assert_eq!(spaces, expected);
        let moved = BlobVersion::from_bytes(&writes[1].2).unwrap();
        assert_eq!(moved.location_key().unwrap().backend, storage());
        let old = ReclaimCandidateKey::from_bytes(&writes[2].1).unwrap();
        assert_eq!(old.backend, BackendRef::node_default());
    }

    #[test]
    fn skips_placed_version() {
        let mut operation = operation("k", Vec::new());
        let mut version = raw_version();
        if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
            *backend = storage();
        }
        operation.first().unwrap();

        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));

        assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    #[test]
    fn discards_changed_rule() {
        // The default rule went back to Aruna storage during the copy.
        let mut operation = operation("k", Vec::new());
        copied(&mut operation, &raw_version(), false).unwrap();

        let turn = rules_read(&mut operation, None, None);

        // The unpublished copy goes to the cleanup queue.
        leaves_copy(&skipped_calls(turn), true);
    }

    #[test]
    fn disabled_target_discards() {
        // A storage disabled during the copy never takes the version, so nothing is reclaimed.
        let mut operation = operation("k", Vec::new());
        copied(&mut operation, &raw_version(), false).unwrap();
        rules_read(
            &mut operation,
            Some(RoutingTarget::Backend(storage())),
            None,
        )
        .unwrap();

        let turn = operation.next(storage_record(true));

        leaves_copy(&skipped_calls(turn), true);
    }

    #[test]
    fn skips_missing_target() {
        // A row on the target whose bytes are gone is never adopted.
        let mut operation = operation("k", Vec::new());
        operation.first().unwrap();
        operation
            .next(read_result(Some(raw_version().to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        let target = on_storage(location(false));

        let effects = operation.next(read_result(Some(target.to_bytes().unwrap())));
        let Some(BlobEffect::StatCopy { location }) = asked(&effects).as_blob() else {
            panic!("expected the target probe, got {effects:?}")
        };
        assert!(location.same_object(&target));
        let turn = operation.next(blob_event(BlobEvent::CopyStat { exists: false }));

        assert_eq!(turn, Err(MigrateVersionError::TargetMissing));
    }

    #[test]
    fn probed_target_revalidated() {
        // The adopted row must still be the probed copy when the version moves to it.
        let mut operation = operation("k", Vec::new());
        let version = raw_version();
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(
                on_storage(location(false)).to_bytes().unwrap(),
            )))
            .unwrap();
        let turn = operation.next(blob_event(BlobEvent::CopyStat { exists: true }));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        enter_publish(&mut operation).unwrap();
        published(&mut operation, None);
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();

        let replaced = on_storage(location(false));
        let turn = operation.next(read_result(Some(replaced.to_bytes().unwrap())));

        assert_eq!(turn, Err(MigrateVersionError::TargetMissing));
    }

    #[test]
    fn writes_linked_file() {
        // The file lands where an upload goes and its importer skips it later.
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        let version = raw_version();
        let effects = copied(&mut operation, &version, false);
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the publication reads, got {effects:?}")
        };
        assert_eq!(reads.len(), 3);
        let plain = operation.plain_path.clone().unwrap();
        assert_eq!(
            plain.path,
            format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]))
        );
        // Only a free path is taken, so an outside file is never replaced.
        assert!(plain.if_not_exists);
        published(&mut operation, Some(rule()));
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, None);

        let writes = batch(&effects);
        let (_, key, _) = (writes.iter())
            .find(|(space, ..)| space == IMPORTED_OBJECT_KEYSPACE)
            .expect("the import row");
        assert_eq!(key, &object_key(IMPORT, &plain.path));
        let (_, _, moved) = (writes.iter())
            .find(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
            .unwrap();
        let moved = BlobVersion::from_bytes(moved).unwrap();
        assert_eq!(moved.location_key().unwrap().backend, storage());
    }

    #[test]
    fn records_move_intents() {
        // The move stores its identity at both paths it may reach and retires it on publish.
        let mut moved = operation("in/x/one.csv", vec![rule()]);
        let version = raw_version();
        copied(&mut moved, &version, false).unwrap();
        let plain = moved.plain_path.clone().unwrap();
        let id = Ulid::from_bytes([2; 16]);
        let tagged = tagged_path(&plain.path, plain.ulid);
        let expected = [&plain.path, &tagged].map(|path| intent_key(IMPORT, path, id));
        assert_eq!(moved.written_intent, expected.to_vec());
        assert_eq!(moved.intent_keys(plain.ulid), expected.to_vec());
        published(&mut moved, Some(rule()));
        let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
        answer_target(&mut moved, effects, None).unwrap();
        let turn = moved.next(IoResult::Done);
        let Ok(Next::Call(IoCall::Delete(deletes))) = &turn else {
            panic!("expected the intent retirement, got {turn:?}")
        };
        let retired: Vec<Key> = deletes.iter().map(|(_, key)| key.clone()).collect();
        assert_eq!(retired, expected.to_vec());

        // A refused write moved no byte, so its intents go before the move fails.
        let mut refused = operation("in/x/one.csv", vec![rule()]);
        refused.first().unwrap();
        refused
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        refused
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        refused.next(read_result(None)).unwrap();
        refused.next(read_result(None)).unwrap();
        refused
            .next(blob_event(BlobEvent::PlainStat {
                exists: false,
                version: None,
            }))
            .unwrap();
        let effects = refused.next(empty_blob());
        past_intent(&mut refused, effects).unwrap();
        let turn = refused.next(blob_event(BlobEvent::Error(BlobError::PathTaken)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(refused.next(IoResult::Done), Ok(Next::Call(IoCall::Sync)));
        let turn = refused.next(IoResult::Done);
        assert!(refused.written_intent.is_empty());
        assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
    }

    #[test]
    fn drops_orphan_intents() {
        // The rule went before the intents were stored, so none are and the move stops.
        let mut moved = operation("in/x/one.csv", vec![rule()]);
        let version = raw_version();
        moved.first().unwrap();
        moved
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        moved
            .next(read_result(Some(location(false).to_bytes().unwrap())))
            .unwrap();
        moved.next(read_result(None)).unwrap();
        moved.next(read_result(None)).unwrap();
        moved
            .next(blob_event(BlobEvent::PlainStat {
                exists: false,
                version: None,
            }))
            .unwrap();
        moved.next(empty_blob()).unwrap();
        let gone = Ok::<_, StorageError>(Fenced::RuleGone);
        let turn = moved.next(IoResult::Child(ChildOutcome::new(gone)));
        assert!(moved.written_intent.is_empty());
        assert_eq!(turn, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    #[test]
    fn keeps_shared_target() {
        // Another rule of the folder recorded the new file, so the move does not take it.
        let other = BackendImport {
            import_id: Ulid::from_parts(9, 9),
            key_prefix: "other/".to_string(),
            ..rule()
        };
        let version = raw_version();
        let at_target = || {
            let mut moved = operation("in/x/one.csv", vec![rule(), other.clone()]);
            copied(&mut moved, &version, false).unwrap();
            let default = Some(RoutingTarget::Backend(storage()));
            rules_read(&mut moved, default, [rule(), other.clone()]).unwrap();
            moved.next(storage_record(false)).unwrap();
            let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
            let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
                panic!("expected the target and shared reads, got {effects:?}")
            };
            let path = moved.new.as_ref().unwrap().backend_path.clone();
            // The rule's own record of the path comes first, then the other rule's.
            assert_eq!(reads[1].1, object_key(IMPORT, &path));
            assert_eq!(reads[2].1, object_key(other.import_id, &path));
            (moved, path)
        };
        let row = |other: Option<Value>| IoResult::Values(vec![None, None, other]);
        let taken = || Err(MigrateVersionError::Blob(BlobError::PathTaken));
        let (mut moved, _) = at_target();
        assert_eq!(moved.next(row(Some(vec![1].into()))), taken());
        // Nor while the other rule has an unfinished write of the path.
        let (mut moved, path) = at_target();
        let turn = moved.next(row(None));
        let Ok(Next::Call(IoCall::Page { prefix, .. })) = &turn else {
            panic!("expected the peer intents, got {turn:?}")
        };
        assert_eq!(*prefix, intent_prefix(other.import_id, &path));
        let rows = vec![(Key::from(vec![1]), Value::from(vec![1]))];
        assert_eq!(moved.next(IoResult::Page { rows, next: None }), taken());
    }

    #[test]
    fn keeps_owned_file() {
        // The rule recorded the new name for another key, as an earlier pattern's write of
        // equal bytes; the move does not take that file over.
        let mut moved = operation("in/x/one.csv", vec![rule()]);
        let version = raw_version();
        copied(&mut moved, &version, false).unwrap();
        published(&mut moved, Some(rule()));
        let effects = moved.next(read_result(Some(version.to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the target and own record reads, got {effects:?}")
        };
        assert_eq!(reads.len(), 2);
        let other =
            ImportedObject::written(Ulid::from_parts(8, 8), "in/y/one.csv".into(), 40, None);
        let values = vec![None, Some(other.to_bytes().unwrap().into())];
        let turn = moved.next(IoResult::Values(values));
        assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
    }

    #[test]
    fn never_overwrites_path() {
        // A file an earlier attempt or a user left at the path is never overwritten.
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        copied(&mut operation, &raw_version(), true).unwrap();

        let plain = operation.plain_path.clone().unwrap();
        let path = format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]));
        assert_eq!(plain.path, tagged_path(&path, plain.ulid));
        assert_ne!(plain.ulid, Ulid::from_bytes([2; 16]));
        assert!(plain.if_not_exists);
    }

    #[test]
    fn new_rule_discards() {
        // A linked rule created during an ordinary move is read in the publication.
        let mut operation = operation("in/x/one.csv", Vec::new());
        copied(&mut operation, &raw_version(), false).unwrap();
        let default = Some(RoutingTarget::Backend(storage()));

        let effects = rules_read(&mut operation, default, Some(rule()));

        leaves_copy(&skipped_calls(effects), true);
    }

    #[test]
    fn removed_rule_discards() {
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        copied(&mut operation, &raw_version(), false).unwrap();
        let default = Some(RoutingTarget::Backend(storage()));

        let effects = rules_read(&mut operation, default, None);

        leaves_copy(&skipped_calls(effects), true);
    }

    /// A version whose copy is in Aruna's layout on the rule's storage.
    fn in_place() -> (BlobVersion, BackendLocation) {
        let mut version = raw_version();
        if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
            *backend = storage();
        }
        (version, on_storage(location(false)))
    }

    #[test]
    fn links_existing_copy() {
        // A copy already on the rule's storage still gets its file, as an upload does,
        // and the version keeps the copy it names.
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        let (version, old) = in_place();
        operation.first().unwrap();
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the import row read, got {effects:?}")
        };
        assert_eq!(reads[0].0, IMPORTED_OBJECT_KEYSPACE);
        operation.next(read_result(None)).unwrap();
        operation
            .next(read_result(Some(old.to_bytes().unwrap())))
            .unwrap();
        let effects = operation.next(read_result(None));
        assert!(matches!(
            (asked(&effects)).as_blob(),
            Some(BlobEffect::StatPlain { .. })
        ));
        operation
            .next(blob_event(BlobEvent::PlainStat {
                exists: false,
                version: None,
            }))
            .unwrap();
        let effects = operation.next(empty_blob());
        let effects = past_intent(&mut operation, effects);
        let Some(BlobEffect::WritePlain { plain, .. }) = asked(&effects).as_blob() else {
            panic!("expected the file write, got {effects:?}")
        };
        let written = plain_written(plain);
        operation.next(written).unwrap();
        enter_publish(&mut operation).unwrap();
        published(&mut operation, Some(rule()));
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));

        let spaces: Vec<&str> = batch(&effects).iter().map(|(s, ..)| s.as_str()).collect();
        assert_eq!(spaces, [IMPORTED_OBJECT_KEYSPACE, BLOB_VERSIONS_KEYSPACE]);
    }

    #[test]
    fn linked_copy_done() {
        // Once the version's file is recorded, a later move leaves it alone.
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        let (version, old) = in_place();
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        operation
            .next(read_result(Some(old.to_bytes().unwrap())))
            .unwrap();
        let row =
            ImportedObject::written(Ulid::from_bytes([2; 16]), "in/x/one.csv".into(), 40, None);

        let effects = operation.next(read_result(Some(row.to_bytes().unwrap())));

        assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    #[test]
    fn keeps_linked_file() {
        // A file found in a linked folder stays even when the rules point elsewhere.
        let mut operation = operation("k", Vec::new());
        operation.routing = Some(routing(Vec::new()).with_group_default(None));
        let mut version = raw_version();
        if let BlobVersionState::Materialized { backend, .. } = &mut version.state {
            *backend = storage();
        }
        let mut external = on_storage(location(false));
        external.storage_bucket = String::new();
        operation.first().unwrap();
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, Some(external.to_bytes().unwrap()));

        assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
    }

    /// A version whose own linked file has the name of the earlier prefix pattern.
    fn prefixed() -> (BlobVersion, BackendLocation) {
        let id = Ulid::from_bytes([2; 16]);
        let mut old = on_storage(location(false));
        old.storage_bucket = String::new();
        old.backend_path = format!("raw/x/{id}_one.csv");
        old.format.layout = StoredLayout::Plain(old.ulid);
        let mut version = raw_version();
        if let BlobVersionState::Materialized {
            backend, encoding, ..
        } = &mut version.state
        {
            (*backend, *encoding) = (storage(), old.format.encoding());
        }
        (version, old)
    }

    /// A move that knows the rule's earlier prefix pattern, up to the read of the rule's
    /// record at the new name.
    fn named(
        version: &BlobVersion,
        old: &BackendLocation,
        (source, etag): (Option<&[u8]>, Option<&str>),
    ) -> MigrateVersionOperation {
        let earlier = Earlier::from([(IMPORT, vec!["{dir}{id}_{name}".to_string()])]);
        let mut operation = operation("in/x/one.csv", vec![rule()]).with_earlier(earlier);
        operation.first().unwrap();
        operation
            .next(read_result(Some(version.to_bytes().unwrap())))
            .unwrap();
        let effects = operation.next(read_result(Some(old.to_bytes().unwrap())));
        // The old file's backend version binds a later delete to the renamed bytes, and the
        // rule's record of the file names the ETag an unversioned copy needs.
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the reads of the old file, got {effects:?}")
        };
        let id = operation.version_key.version_id;
        let etag = etag.map(str::to_string);
        let row = ImportedObject::written(id, "in/x/one.csv".to_string(), 4, etag);
        assert_eq!(reads.len(), 2);
        let values = vec![
            source.map(|source| source.to_vec().into()),
            Some(row.to_bytes().unwrap().into()),
        ];
        operation.next(IoResult::Values(values)).unwrap();
        operation.next(read_result(None)).unwrap();
        operation
    }

    /// A move that knows the rule's earlier prefix pattern, up to its copy to the new name.
    fn renaming(
        version: &BlobVersion,
        old: &BackendLocation,
        (source, recorded): (Option<&[u8]>, Option<&str>),
    ) -> (MigrateVersionOperation, PlainPath) {
        let mut operation = named(version, old, (source, recorded));
        // Nothing records the new name for another key, and no provider is asked first.
        let effects = operation.next(read_result(None));
        let effects = past_intent(&mut operation, effects);
        let Some(BlobEffect::CopyPlain {
            plain,
            source: copied,
            etag,
            ..
        }) = asked(&effects).as_blob()
        else {
            panic!("expected the copy to the new name, got {effects:?}")
        };
        assert_eq!(**copied, *old);
        assert_eq!(etag.as_deref(), recorded);
        // A rename takes its exact new name or nothing.
        assert!(plain.if_not_exists && plain.exact);
        let name = format!("raw/x/one.csv~{}", Ulid::from_bytes([2; 16]));
        assert_eq!(plain.path, name);
        (operation, plain.clone())
    }

    #[test]
    fn keeps_recorded_name() {
        // A new name the rule records for another key is taken before any provider request.
        let (version, old) = prefixed();
        let mut operation = named(&version, &old, (Some(b"v1"), Some("e1")));
        let id = Ulid::from_bytes([9; 16]);
        let other = ImportedObject::written(id, "in/x/other.csv".to_string(), 4, None);
        let effects = operation.next(read_result(Some(other.to_bytes().unwrap())));
        assert_eq!(
            effects,
            Err(MigrateVersionError::Blob(BlobError::PathTaken))
        );
    }

    #[test]
    fn refuses_unconditional_rename() {
        // A storage that cannot create only a free name refuses the rename; no intent stays.
        let (version, old) = prefixed();
        let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
        let refused = || BlobError::ConditionalUnsupported;
        let effects = operation.next(blob_event(BlobEvent::Error(refused())));
        // Such a storage may hold an earlier attempt at the tagged name.
        let Some(BlobEffect::StatExternal { path, .. }) = asked(&effects).as_blob() else {
            panic!("expected the stat of the tagged name, got {effects:?}")
        };
        assert_eq!(path, &tagged_path(&plain.path, plain.ulid));
        let turn = operation.next(blob_event(BlobEvent::ExternalStat {
            location: None,
            version: None,
            etag: None,
        }));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        operation.next(IoResult::Done).unwrap();
        let turn = operation.next(IoResult::Done);
        assert!(operation.written_intent.is_empty());
        assert_eq!(turn, Err(MigrateVersionError::Blob(refused())));
    }

    /// Drives a rename to its publication rows.
    fn renamed(
        source: (Option<&[u8]>, Option<&str>),
    ) -> (Vec<(String, Key, Value)>, PlainPath, BackendLocation) {
        let (version, old) = prefixed();
        let (mut operation, plain) = renaming(&version, &old, source);
        operation.next(plain_written(&plain)).unwrap();
        enter_publish(&mut operation).unwrap();
        published(&mut operation, Some(rule()));
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, None);
        (batch(&effects).clone(), plain, old)
    }

    #[test]
    fn renames_linked_file() {
        // The version names the new file; the old one goes only through the cleanup queue.
        let (writes, plain, old) = renamed((Some(b"v1"), Some("e1")));
        let find = |space: &str| writes.iter().find(|(found, ..)| found == space).unwrap();
        let moved = BlobVersion::from_bytes(&find(BLOB_VERSIONS_KEYSPACE).2).unwrap();
        let encoding = EncodingClass::Plain {
            location: plain.ulid,
        };
        assert_eq!(moved.location_key().unwrap().encoding, encoding);
        assert_eq!(
            find(IMPORTED_OBJECT_KEYSPACE).1,
            object_key(IMPORT, &plain.path)
        );
        let work = BlobCleanupWork::from_bytes(&find(BLOB_CLEANUP_KEYSPACE).2).unwrap();
        let BlobCleanupWork::ReleaseRenamed {
            location,
            import_id,
            version_id,
            version,
        } = work
        else {
            panic!("expected the release of the old file")
        };
        assert_eq!(
            (location, import_id, version),
            (old, IMPORT, "v1".to_string())
        );
        assert_eq!(version_id, Ulid::from_bytes([2; 16]));
        // Without a backend version the old file goes only while its recorded ETag holds.
        let release = |writes: &[(String, Key, Value)]| {
            let found = writes
                .iter()
                .find(|(space, ..)| space == BLOB_CLEANUP_KEYSPACE);
            found.map(|(.., work)| BlobCleanupWork::from_bytes(work).unwrap())
        };
        for source in [None, Some(&b"null"[..])] {
            let (writes, ..) = renamed((source, Some("e1")));
            let Some(BlobCleanupWork::ReleaseRenamed { version, .. }) = release(&writes) else {
                panic!("expected the release of the old file")
            };
            assert_eq!(version, "");
        }
        // Without a recorded ETag nothing proves which bytes are there, so the file stays.
        let (writes, ..) = renamed((None, None));
        assert!(release(&writes).is_none());
        assert!(
            writes
                .iter()
                .any(|(space, ..)| space == BLOB_VERSIONS_KEYSPACE)
        );
    }

    /// The old file of `prefixed` with a known MD5, and the provider file `size` and `etag`
    /// found at the name a refused rename wanted.
    fn taken_name(
        (old_size, size): (Option<u64>, u64),
        etag: &str,
    ) -> (MigrateVersionOperation, BlobVersion, BackendLocation, Turn) {
        let (version, mut old) = prefixed();
        old.hashes.insert(HASH_MD5.to_string(), vec![7; 16]);
        if let Some(old_size) = old_size {
            old.blob_size = old_size;
            let md5 = md5::compute(b"").0.to_vec();
            old.hashes.insert(HASH_MD5.to_string(), md5);
        }
        let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
        let effects = operation.next(blob_event(BlobEvent::Error(BlobError::PathTaken)));
        let Some(BlobEffect::StatExternal { path, .. }) = asked(&effects).as_blob() else {
            panic!("expected the stat of the taken name, got {effects:?}")
        };
        assert_eq!(path, &plain.path);
        let mut found = on_storage(location(false));
        (found.storage_bucket, found.backend_path) = (String::new(), path.clone());
        (found.blob_size, found.hashes) = (size, HashMap::new());
        let effects = operation.next(blob_event(BlobEvent::ExternalStat {
            location: Some(found),
            version: None,
            etag: Some(format!("\"{etag}\"")),
        }));
        (operation, version, old, effects)
    }

    #[test]
    fn resumes_taken_name() {
        // An interrupted rename left its copy at the new name; only checksums decide.
        let size = prefixed().1.blob_size;
        let (single, copied) = (
            hex::encode([7; 16]),
            format!("{:x}-1", md5::compute([7; 16])),
        );
        // A provider copy may keep the old file's recorded ETag, and an empty old file's
        // copy is proved like any other; a reservation never shows here as a file.
        let empty = hex::encode(md5::compute(b"").0);
        for (size, etag, adopted) in [
            ((None, size), single.as_str(), true),
            ((None, size), copied.as_str(), true),
            ((None, size), "e1", true),
            ((None, size), "0123", false),
            ((None, size + 1), copied.as_str(), false),
            ((Some(0), 0), empty.as_str(), true),
        ] {
            let (mut operation, _, old, effects) = taken_name(size, etag);
            if adopted {
                // The upload the earlier attempt recorded is settled before the adoption.
                let attempt = Ulid::from(u128::from(old.ulid) ^ 1);
                let settle = BlobEffect::SettleCopy {
                    attempt,
                    upload: None,
                };
                assert_eq!(asked(&effects), &Effect::blob(settle));
                let turn = operation.next(blob_event(BlobEvent::UploadAborted));
                assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
                // The intents the earlier attempt stored go with the publication.
                assert_eq!(operation.written_intent, operation.intent_keys(attempt));
                assert_eq!(operation.written_intent.len(), 2);
                // The proved copy names the old file's hashes.
                assert_eq!(operation.new.as_ref().unwrap().hashes, old.hashes);
                continue;
            }
            // Another file at the new name keeps it; the intents go before the move fails.
            assert!(matches!(effects, Ok(Next::Call(IoCall::Delete(_)))));
            operation.next(IoResult::Done).unwrap();
            let error = MigrateVersionError::Blob(BlobError::PathTaken);
            assert_eq!(operation.next(IoResult::Done), Err(error));
        }
    }

    #[test]
    fn skips_deleted_version() {
        // A copy at the new name whose backend version a release deleted is not adopted.
        let (version, mut old) = prefixed();
        old.hashes.insert(HASH_MD5.to_string(), vec![7; 16]);
        let (mut operation, plain) = renaming(&version, &old, (Some(b"v1"), Some("e1")));
        operation
            .next(blob_event(BlobEvent::Error(BlobError::PathTaken)))
            .unwrap();
        let mut found = on_storage(location(false));
        (found.storage_bucket, found.backend_path) = (String::new(), plain.path.clone());
        found.blob_size = old.blob_size;
        operation
            .next(blob_event(BlobEvent::ExternalStat {
                location: Some(found),
                version: Some("p9".to_string()),
                etag: Some(hex::encode([7; 16])),
            }))
            .unwrap();
        operation
            .next(blob_event(BlobEvent::UploadAborted))
            .unwrap();
        enter_publish(&mut operation).unwrap();
        published(&mut operation, Some(rule()));
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the target reads, got {effects:?}")
        };
        assert_eq!(reads.last().unwrap().0, DELETE_CLAIM_KEYSPACE);
        let mut values = vec![None; reads.len()];
        *values.last_mut().unwrap() = Some(Vec::new().into());
        let turn = operation.next(IoResult::Values(values));
        assert_eq!(turn, Err(MigrateVersionError::Blob(BlobError::PathTaken)));
    }

    #[test]
    fn keeps_unknown_name() {
        // Without the earlier pattern the name tells no version, so the file stays.
        let (version, old) = prefixed();
        let mut operation = operation("in/x/one.csv", vec![rule()]);
        operation.first().unwrap();
        let effects = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let effects = answer_target(&mut operation, effects, Some(old.to_bytes().unwrap()));
        assert_eq!(effects, Ok(Next::Done(MigrateOutcome::Skipped)));
    }
}
