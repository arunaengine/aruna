//! Tests the inbound version operation call by call without a running driver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    EnqueueClock, IncomingVersionError, IncomingVersionOperation, IncomingVersionResult,
    IncomingVersionState, QuotaProbe, ReceivedBlob,
};
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeEvent, AbeParameters, EnvelopePlan, ObjectEnvelope, create_envelope,
    create_parameters,
};
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::StoredFormat;

use crate::replication::protocol::{
    MAX_VALUE_BYTES, MaterializedBlobInfo, VersionReplicationManifest, VersionReplicationMessage,
};
use crate::s3::purge_fence::PurgeFenceError;
use crate::s3::usage::{QuotaGate, QuotaGateError};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE,
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    OBJECT_METADATA_KEYSPACE, PATHS_INDEX_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{EffectOperation, Operation, Runnable, TransactionMode};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{QuotaConfig, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CurrentVersionPointer, HashIndex, WriteOwner,
};
use aruna_core::structs::storage::cleanup::ReclaimCandidateKey;
use aruna_core::structs::storage::multipart::MultipartObjectKey;
use aruna_core::structs::storage::replication::{
    ReplicationItemKind, ReplicationNegotiationResult,
};
use aruna_core::structs::storage::replication_queue::LiveObligationRecord;
use aruna_core::structs::storage::replication_queue::{ReferenceAdvance, SyncOrigin};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingTarget, StorageRoutingRule,
};
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, UserId};
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, SystemTime};
use ulid::Ulid;

pub(super) type Turn = super::Turn;

pub(super) fn row(value: Option<Value>) -> IoResult {
    IoResult::Values(vec![value])
}

pub(super) fn rows(values: Vec<Option<Value>>) -> IoResult {
    IoResult::Values(values)
}

pub(super) fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

pub(super) fn event_result(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

pub(super) fn blob(event: BlobEvent) -> IoResult {
    event_result(Event::Blob(event))
}

pub(super) fn asked(turn: &Turn) -> &Effect {
    match turn {
        Ok(Next::Call(IoCall::Effect(effect))) => effect,
        other => panic!("expected an effect call, got {other:?}"),
    }
}

/// The rows of a write call.
fn written(turn: &Turn) -> &Vec<(String, Key, Value)> {
    match turn {
        Ok(Next::Call(IoCall::Write(rows))) => rows,
        other => panic!("expected a write call, got {other:?}"),
    }
}

/// Turns `op`, which starts its apply, into that child and runs its first call.
fn enter_apply(op: &mut IncomingVersionOperation) -> Turn {
    assert_eq!(op.state, IncomingVersionState::Apply);
    *op = op.apply_child();
    op.first()
}

/// The parent's answer for an apply child that committed.
fn applied(result: IncomingVersionResult) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, IncomingVersionError>(result)))
}

/// The parent's answer for an apply child that failed.
fn apply_failed(error: IncomingVersionError) -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<IncomingVersionResult, _>(error)))
}

/// A parent whose apply child is running, holding `received` when bytes arrived.
fn applying(
    kind: ReplicationItemKind,
    seed: u8,
    negotiated: Option<ReplicationNegotiationResult>,
    received: Option<BackendLocation>,
) -> IncomingVersionOperation {
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(u64::from(seed), 1),
        iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        test_realm_id(),
        make_manifest(kind),
    );
    op.state = IncomingVersionState::Apply;
    op.negotiation_result = negotiated;
    op.received_blob = received.map(ReceivedBlob::reserved);
    op
}

/// What one call of a failed run's cleanup does.
#[derive(Debug, PartialEq)]
enum Tail {
    Reconcile(Key),
    Release(Ulid),
    Reject(String),
    Delete(BackendLocation),
    Close,
}

fn tail(calls: &[IoCall]) -> Vec<Tail> {
    let step = |call: &IoCall| match call {
        IoCall::Write(rows) if rows.len() == 1 && rows[0].0 == BLOB_CLEANUP_KEYSPACE => {
            Tail::Reconcile(rows[0].1.clone())
        }
        IoCall::Effect(effect) => match effect.as_ref() {
            Effect::Blob(BlobEffect::ReleaseReservation { id }) => Tail::Release(*id),
            Effect::Blob(BlobEffect::Delete { location }) => Tail::Delete(location.clone()),
            Effect::Blob(BlobEffect::CloseConnection { .. }) => Tail::Close,
            sent => match message_from_effect(sent) {
                VersionReplicationMessage::VersionApplyRejected(reason) => Tail::Reject(reason),
                other => panic!("unexpected message in the cleanup: {other:?}"),
            },
        },
        other => panic!("unexpected cleanup call: {other:?}"),
    };
    calls.iter().map(step).collect()
}

/// The envelope rows among the version rows of one write call.
fn envelope_rows(turn: &Turn) -> Vec<&(String, Key, Value)> {
    let spaces = [
        ABE_ENVELOPE_KEYSPACE,
        ABE_VERSION_KEYSPACE,
        ABE_ARCHIVE_KEYSPACE,
    ];
    let is_envelope = |row: &&(String, Key, Value)| spaces.contains(&row.0.as_str());
    written(turn).iter().filter(is_envelope).collect()
}

#[tokio::test]
async fn refused_copy_absent() {
    use crate::replication::protocol::VersionReplicationRequest;
    use crate::replication::version_replication::{ReplicateObjectError, ReplicateObjectOperation};
    use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
    use aruna_core::structs::storage::blob::VersionKey;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRef, EncryptionMode,
    };
    use aruna_core::structs::storage::format::PithosLayout;
    use aruna_core::structs::storage::replication_queue::ReplicationMode;
    let (_source_dir, source_storage) = crate::tests::s3::test_storage();
    let (target_dir, target_storage) = crate::tests::s3::test_storage();
    let target_node = iroh::SecretKey::from_bytes(&[42; 32]).public();
    let stream_id = Ulid::from_parts(42, 42);
    let data = [7u8; 42];
    let hash = *blake3::hash(&data).as_bytes();
    let mut target_location = make_location();
    target_location.root = target_dir.path().to_str().unwrap().to_string();
    target_location
        .hashes
        .insert("blake3".to_string(), hash.to_vec());
    let path = std::path::PathBuf::from(target_location.get_full_path().unwrap());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, data).unwrap();
    let mut location = target_location.clone();
    let key = BucketKeyRef::new(Ulid::from_parts(43, 43), 1);
    location.format = StoredFormat::pithos(
        PithosLayout {
            stored_size: 99,
            metadata_digest: [2; 32],
            storage_generation: 1,
        },
        key,
    );
    let version = BlobVersion::materialized(
        hash,
        location.backend.clone(),
        location.format.encoding(),
        fixed_created_at(),
        test_user_id(),
        None,
    );
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(key.bucket_id),
        key_generation: key.generation,
        ..Default::default()
    };
    let version_key = VersionKey::new("bucket", "dir/file.txt", trace_version_id())
        .to_bytes()
        .unwrap();
    for (storage, space, row, value) in [
        (
            &source_storage,
            BLOB_VERSIONS_KEYSPACE,
            version_key.clone(),
            version.to_bytes().unwrap(),
        ),
        (
            &source_storage,
            BLOB_HEAD_KEYSPACE,
            aruna_core::structs::storage::blob::BlobHeadKey::new("bucket", "dir/file.txt")
                .to_bytes()
                .unwrap(),
            CurrentVersionPointer::new(trace_version_id())
                .to_bytes()
                .unwrap(),
        ),
        (
            &source_storage,
            BLOB_LOCATIONS_KEYSPACE,
            location.location_key().unwrap().to_bytes(),
            location.to_bytes().unwrap(),
        ),
        (
            &source_storage,
            BUCKET_ENCRYPTION_KEYSPACE,
            b"bucket".to_vec(),
            settings.to_bytes().unwrap(),
        ),
        (
            &target_storage,
            S3_BUCKET_KEYSPACE,
            b"bucket".to_vec(),
            make_bucket_info(test_group_id()).to_bytes().unwrap(),
        ),
        (
            &target_storage,
            BLOB_LOCATIONS_KEYSPACE,
            target_location.location_key().unwrap().to_bytes(),
            target_location.to_bytes().unwrap(),
        ),
    ] {
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: space.to_string(),
                    key: row.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
    let mut source = ReplicateObjectOperation::new(VersionReplicationRequest {
        bucket: "bucket".to_string(),
        key: "dir/file.txt".to_string(),
        version_id: trace_version_id(),
        source_group_id: test_group_id(),
        target_node_id: target_node,
        auth_context: make_manifest(ReplicationItemKind::Materialized).auth_context,
        mode: ReplicationMode::OnDemand,
    })
    .into_machine();
    let mut effects = source.start();
    while matches!(effects.as_slice(), [Effect::Storage(_)]) {
        let Effect::Storage(effect) = effects.remove(0) else {
            unreachable!()
        };
        effects = source.step(source_storage.send_storage_effect(effect).await);
    }
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::OpenConnection { .. })]
    ));
    let effects = source.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
    let VersionReplicationMessage::VersionManifest(mut manifest) = message_from_effect(&effects[0])
    else {
        panic!("expected source manifest");
    };
    assert!(manifest.current_version);
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    source.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    let mut target =
        IncomingVersionOperation::new(stream_id, target_node, test_realm_id(), manifest);
    target.manifest_policy = Some(target.target_authorization_path(test_group_id()));
    target.writer_policy = Some(target.target_authorization_path(test_group_id()));
    let mut target = target.into_machine();
    let mut effects = target.start();
    loop {
        match effects.as_slice() {
            [Effect::Storage(_)] => {
                let Effect::Storage(effect) = effects.remove(0) else {
                    unreachable!()
                };
                effects = target.step(target_storage.send_storage_effect(effect).await)
            }
            [Effect::SubOperation(_)] => {
                effects = target.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
                    result: Ok(GroupRoutingInputs::default()),
                }));
            }
            _ => break,
        }
    }
    let negotiation = message_from_effect(&effects[0]);
    assert_eq!(
        negotiation,
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    );
    let waiting = target.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert!(matches!(
        waiting.as_slice(),
        [Effect::Blob(BlobEffect::ReadMessage { .. })]
    ));
    let mut effects = source.step(Event::Blob(BlobEvent::MessageReceived {
        stream_id,
        payload: negotiation.to_bytes().unwrap(),
    }));
    let Effect::Storage(effect) = effects.remove(0) else {
        panic!("source must check encryption");
    };
    let effects = source.step(source_storage.send_storage_effect(effect).await);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::CloseConnection { .. })]
    ));
    assert_eq!(
        source.finalize(),
        Err(ReplicateObjectError::PlaintextRefused)
    );
    target.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
    for space in [
        BLOB_VERSIONS_KEYSPACE,
        BLOB_HEAD_KEYSPACE,
        PATHS_INDEX_KEYSPACE,
    ] {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = target_storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: space.to_string(),
                prefix: None,
                start: None,
                limit: 2,
                txn_id: None,
            })
            .await
        else {
            panic!("target index read failed");
        };
        assert!(values.is_empty(), "refused copy wrote {space}");
    }
}

pub(super) fn test_realm_id() -> RealmId {
    RealmId::from_bytes([7u8; 32])
}

fn test_user_id() -> UserId {
    UserId::nil(test_realm_id())
}

fn test_group_id() -> Ulid {
    Ulid::from_parts(7, 7)
}

/// Fixed wall clock for the traces; the receiver samples its enqueue clock
/// when it writes a reclaim candidate.
fn trace_now() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

fn fixed_trace_clock() -> SystemTime {
    trace_now()
}

thread_local! {
    /// The enqueue-phase clock for the reclaim regression: the test advances
    /// it while the operation is mid-apply to model a long transfer.
    static CONTROLLED_CLOCK: Cell<SystemTime> = const { Cell::new(SystemTime::UNIX_EPOCH) };
}

fn controlled_clock() -> SystemTime {
    CONTROLLED_CLOCK.with(Cell::get)
}

fn set_controlled_clock(now: SystemTime) {
    CONTROLLED_CLOCK.with(|clock| clock.set(now));
}

/// Named fixed identities, one seed per role, so a persisted value in a
/// trace is always traceable to the input that produced it.
fn trace_stream_id() -> Ulid {
    Ulid::from_bytes([0x51; 16])
}

fn trace_version_id() -> Ulid {
    Ulid::from_bytes([0x53; 16])
}

fn fixed_created_at() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000)
}

fn make_location() -> BackendLocation {
    let mut hashes = HashMap::new();
    hashes.insert("blake3".to_string(), vec![1u8; 32]);
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "blob-bucket".to_string(),
        backend_path: "bucket/key".to_string(),
        ulid: Ulid::from_bytes([0x21; 16]),
        format: StoredFormat::default(),
        created_by: test_user_id(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes,
    }
}

pub(super) fn make_bucket_info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test_user_id(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

pub(super) fn make_manifest(kind: ReplicationItemKind) -> VersionReplicationManifest {
    let blob = match kind {
        ReplicationItemKind::Materialized => {
            let location = make_location();
            Some(MaterializedBlobInfo {
                hash: [1u8; 32],
                size: location.blob_size,
                location,
            })
        }
        ReplicationItemKind::DeleteMarker => None,
    };

    VersionReplicationManifest {
        bucket: "bucket".to_string(),
        key: "dir/file.txt".to_string(),
        version_id: trace_version_id(),
        group_id: test_group_id(),
        kind,
        created_at: fixed_created_at(),
        created_by: test_user_id(),
        current_version: true,
        current_version_generation: Some(1),
        auth_context: AuthContext {
            user_id: test_user_id(),
            realm_id: test_realm_id(),
            path_restrictions: None,
            session: None,
        },
        blob,
        source: None,
        multipart: None,
        reference_intent: false,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: Some(AuthContext {
            user_id: test_user_id(),
            realm_id: test_realm_id(),
            path_restrictions: None,
            session: None,
        }),
        reference_metadata: None,
        metadata: HashMap::new(),
        reference_advance: None,
        reference_advance_count: None,
        placement_policies: Vec::new(),
        original_epoch: Default::default(),
        version_generation: 0,
        logical_identity: None,
        distribution: None,
        version_origin: None,
        version_hop: None,
        sync_source: None,
    }
}

fn make_source_binding() -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "https://example.org".to_string(),
            )]),
            source_path: "dir/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::from_bytes([0x22; 16])),
        mirrors: Vec::new(),
    }
}

pub(super) fn make_reference_manifest() -> VersionReplicationManifest {
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut source = make_source_binding();
    source.descriptor.kind = SourceConnectorKind::ArunaNative;
    source.descriptor.origin_node_id = Some(iroh::SecretKey::from_bytes(&[8u8; 32]).public());
    source.connector_id = None;
    manifest.blob = None;
    manifest.source = Some(source);
    manifest.reference_intent = true;
    manifest.reference_metadata = Some(SourceMetadata {
        content_length: 1_000_000,
        content_type: Some("application/octet-stream".to_string()),
        etag: None,
        last_modified: Some(manifest.created_at),
        source_version: None,
        digests: Default::default(),
    });
    manifest.reference_advance_count = Some(0);
    manifest
}

fn advance_fixture() -> (VersionReplicationManifest, BlobVersion, NodeId) {
    let predecessor = Ulid::from_bytes([21u8; 16]);
    let version_id = Ulid::from_bytes([22u8; 16]);
    let publisher = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
    let mut manifest = make_reference_manifest();
    manifest.version_id = version_id;
    manifest.created_at = SystemTime::UNIX_EPOCH;
    manifest.current_version_generation = Some(8);
    manifest.auth_context = AuthContext::anonymous(test_realm_id());
    manifest.writer_auth_context = None;
    manifest.metadata = HashMap::from([("s3-key".to_string(), "value".to_string())]);
    manifest.origin = Some(SyncOrigin {
        relationship_id: Ulid::from_bytes([26u8; 16]),
        hop_count: 1,
    });
    manifest.reference_advance = Some(ReferenceAdvance {
        generation: 8,
        predecessor,
    });
    manifest.reference_advance_count = Some(1);
    manifest
        .source
        .as_mut()
        .unwrap()
        .descriptor
        .version_selector = Some(format!("version:{version_id}"));

    let mut previous_source = manifest.source.clone().unwrap();
    previous_source.descriptor.version_selector = Some(format!("version:{predecessor}"));
    let mut previous_metadata = manifest.reference_metadata.clone().unwrap();
    previous_metadata.content_length = 42;
    let previous = BlobVersion::reference(
        previous_source,
        previous_metadata,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        manifest.created_by,
        SystemTime::UNIX_EPOCH + Duration::from_secs(2),
    )
    .with_metadata(manifest.metadata.clone())
    .with_publisher(publisher);

    (manifest, previous, publisher)
}

fn advance_operation(
    manifest: VersionReplicationManifest,
    publisher: NodeId,
) -> IncomingVersionOperation {
    IncomingVersionOperation::new(
        Ulid::from_bytes([24u8; 16]),
        iroh::SecretKey::from_bytes(&[25u8; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(publisher)
}

fn assert_advance_invalid(
    manifest: VersionReplicationManifest,
    publisher: NodeId,
    previous: BlobVersion,
) {
    let op = advance_operation(manifest, publisher);
    assert_eq!(
        op.validate_advance(&previous),
        Err(IncomingVersionError::InvalidReferenceAdvance)
    );
}

fn message_from_effect(effect: &Effect) -> VersionReplicationMessage {
    let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = effect else {
        panic!("expected blob send message effect")
    };
    VersionReplicationMessage::from_bytes(payload).unwrap()
}

fn expect_rejected_negotiation(effect: &Effect, expected_reason: &str) {
    match message_from_effect(effect) {
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::Rejected(reason),
        ) => assert_eq!(reason, expected_reason),
        other => panic!("expected rejected negotiation response, got {other:?}"),
    }
}

/// Answers the routing load that follows every destination bucket read.
pub(super) fn load_routing(op: &mut IncomingVersionOperation, inputs: GroupRoutingInputs) -> Turn {
    assert_eq!(op.state, IncomingVersionState::LoadDestinationRouting);
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::GroupRoutingLoaded { result: Ok(inputs) },
    )))
}

fn advance_version_lookup(op: &mut IncomingVersionOperation, group_id: Ulid) -> Turn {
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));
    let effects = op.first();
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    op.next(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )))
    .unwrap();
    let effects = load_routing(op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    effects
}

fn advance_blob_lookup(op: &mut IncomingVersionOperation) -> Turn {
    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_LOCATIONS_KEYSPACE
    ));
    effects
}

/// The apply transaction's drift re-check answered with an absent bucket
/// and an absent subject, which an ungoverned replica passes.
fn no_drift() -> IoResult {
    rows(vec![None, None, None])
}

/// The destination bucket read with no encryption settings row.
pub(super) fn bucket_read(bucket: Option<aruna_core::types::Value>) -> IoResult {
    rows(vec![bucket, None])
}

/// The drift re-check echoing the bucket the negotiation read, which a
/// trace that really read a bucket must answer.
fn bucket_drift(bucket_info: &BucketInfo) -> IoResult {
    rows(vec![
        Some(bucket_info.to_bytes().unwrap().into()),
        None,
        None,
    ])
}

fn start_apply_transaction(op: &mut IncomingVersionOperation) {
    start_apply_with(op, None)
}

/// Turns `op` into its apply child, up to the head read. `bucket` must echo what negotiation
/// stored, or None when no bucket was read; the drift re-check compares the two.
fn start_apply_with(op: &mut IncomingVersionOperation, bucket: Option<Value>) {
    op.state = IncomingVersionState::Apply;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(2, 2));

    let effects = enter_apply(op);
    assert_eq!(op.transaction(), TransactionMode::Write);
    assert_eq!(op.state, IncomingVersionState::CheckPurgeFence);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));
    op.next(row(None)).unwrap();
    assert_eq!(op.state, IncomingVersionState::CheckDrift);
    // The apply transaction re-reads the destination default and the local
    // subject before it exposes anything.
    let effects = op.next(rows(vec![bucket, None, None]));
    assert_eq!(op.state, IncomingVersionState::VerifyReplaced);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    let value = op
        .replaced_version
        .as_ref()
        .map(|version| version.to_bytes().unwrap().into());
    let effects = op.next(row(value));
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_HEAD_KEYSPACE
    ));
}

#[test]
fn purge_fence_rejects() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(3, 3),
        iroh::SecretKey::from_bytes(&[65; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    op.state = IncomingVersionState::Apply;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(4, 4));
    enter_apply(&mut op).unwrap();
    let fence = StoragePurgeFence {
        job_id: JobId::from_bytes([12; 16]),
        scope: StoragePurgeScope::File {
            bucket: manifest.bucket,
            key: manifest.key,
        },
    };

    let turn = op.next(row(Some(fence.to_bytes().unwrap().into())));

    assert!(matches!(
        turn,
        Err(IncomingVersionError::PurgeFence(PurgeFenceError::Suspended))
    ));
}

#[test]
fn existing_version_skips() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(6, 6),
        iroh::SecretKey::from_bytes(&[66; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );

    let _effects = advance_version_lookup(&mut op, Ulid::from_parts(7, 7));

    let version = BlobVersion::materialized(
        manifest.blob.as_ref().unwrap().hash,
        BackendRef::node_default(),
        EncodingClass::Raw,
        manifest.created_at,
        manifest.created_by,
        None,
    );
    let effects = op.next(row(Some(version.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::AlreadyReplicatedVersion
        )
    ));
}

#[test]
fn existing_delete_skips() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(8, 8),
        iroh::SecretKey::from_bytes(&[67; 32]).public(),
        test_realm_id(),
        manifest.clone(),
    );

    let _effects = advance_version_lookup(&mut op, test_group_id());
    let effects = op.next(row(Some(
        BlobVersion::deleted(manifest.created_at, manifest.created_by)
            .to_bytes()
            .unwrap()
            .into(),
    )));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::AlreadyReplicatedVersion
        )
    ));
}

#[test]
fn reference_requests_metadata() {
    let manifest = make_reference_manifest();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(9, 9),
        iroh::SecretKey::from_bytes(&[68; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let _effects = advance_version_lookup(&mut op, test_group_id());
    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(None));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
}

#[test]
fn reference_writes_version() {
    let manifest = make_reference_manifest();
    let expected_source = manifest.source.clone().unwrap();
    let expected_metadata = manifest.reference_metadata.clone().unwrap();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(10, 10),
        iroh::SecretKey::from_bytes(&[69; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.write_blob_version();
    let (_, _, value) = &written(&effects)[0];
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();

    assert!(matches!(
        version.state,
        BlobVersionState::Reference {
            source,
            cached_metadata,
            ..
        } if source == expected_source && cached_metadata == expected_metadata
    ));
    let usage = op.usage_delta().unwrap();
    assert_eq!(usage.logical_bytes, 0);
    assert_eq!(usage.referenced_bytes, 1_000_000);
}

#[test]
fn version_binds_publisher() {
    // A forged manifest cannot forge attribution: the persisted version is
    // bound to the authenticated publisher, never to its self-asserted user.
    let publisher = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let forged = UserId::local(Ulid::from_bytes([9u8; 16]), test_realm_id());
    let mut manifest = make_reference_manifest();
    manifest.created_by = forged;
    manifest.auth_context.user_id = forged;
    manifest.writer_auth_context = None;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(12, 12),
        iroh::SecretKey::from_bytes(&[70; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(publisher);

    let effects = op.write_blob_version();
    let (_, _, value) = &written(&effects)[0];
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(version.published_by, Some(publisher));
    assert_eq!(version.created_by, forged);
}

#[test]
fn epoch_needs_binding() {
    // An ordinary bucket orders by generation alone, whatever epoch a manifest claims.
    let stored_epoch = |bound: bool| {
        let mut manifest = make_reference_manifest();
        manifest.writer_auth_context = None;
        manifest.original_epoch = OriginalEpoch(4);
        let mut op = IncomingVersionOperation::new(
            Ulid::from_parts(12, 12),
            iroh::SecretKey::from_bytes(&[70; 32]).public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(iroh::SecretKey::from_bytes(&[42u8; 32]).public());
        op.destination_bound = bound;
        let effects = op.write_blob_version();
        let (_, _, value) = &written(&effects)[0];
        BlobVersion::from_bytes(value.as_ref())
            .unwrap()
            .original_epoch
    };
    assert_eq!(stored_epoch(false), OriginalEpoch(0));
    assert_eq!(stored_epoch(true), OriginalEpoch(4));
}

#[test]
fn history_keeps_generation() {
    // A historical version keeps the generation it was written as inside a distributed bucket.
    let stored = |bound: bool| {
        let mut manifest = make_reference_manifest();
        manifest.writer_auth_context = None;
        manifest.current_version = false;
        manifest.current_version_generation = None;
        manifest.version_generation = 5;
        let mut op = IncomingVersionOperation::new(
            Ulid::from_parts(12, 12),
            iroh::SecretKey::from_bytes(&[70; 32]).public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(iroh::SecretKey::from_bytes(&[42u8; 32]).public());
        op.destination_bound = bound;
        let effects = op.write_blob_version();
        let (_, _, value) = &written(&effects)[0];
        BlobVersion::from_bytes(value.as_ref()).unwrap().generation
    };
    assert_eq!((stored(false), stored(true)), (0, 5));
}

#[test]
fn keeps_logical_identity() {
    // The identity the set fixed travels with the version, whatever form its bytes take here.
    let mut manifest = make_reference_manifest();
    manifest.writer_auth_context = None;
    manifest.logical_identity = Some([9; 32]);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(12, 12),
        iroh::SecretKey::from_bytes(&[70; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(iroh::SecretKey::from_bytes(&[42u8; 32]).public());
    op.destination_bound = true;
    let effects = op.write_blob_version();
    let (_, _, value) = &written(&effects)[0];
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(version.identity, Some([9; 32]));
    assert_eq!(op.written_identity, Some([9; 32]));
}

#[test]
fn valid_advance() {
    let (manifest, previous, publisher) = advance_fixture();
    let advance = manifest.reference_advance.unwrap();
    let version_id = manifest.version_id;
    assert!(manifest.writer_auth_context.is_none());
    let mut op = advance_operation(manifest, publisher);

    advance_version_lookup(&mut op, test_group_id()).unwrap();
    let existing = op.reference_version().unwrap();
    let effects = op.next(row(Some(existing.to_bytes().unwrap().into())));
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
    start_apply_with(
        &mut op,
        Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
    );
    op.next(row(Some(
        CurrentVersionPointer::new_with_generation(advance.predecessor, advance.generation - 1)
            .to_bytes()
            .unwrap()
            .into(),
    )))
    .unwrap();

    let effects = op.next(row(Some(previous.to_bytes().unwrap().into())));
    let (key_space, _, value) = &written(&effects)[0];
    assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(
        CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(version_id, advance.generation)
    );
    assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

    let effects = op.next(IoResult::Done);
    let (key_space, ..) = &written(&effects)[0];
    assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);
}

#[test]
fn advance_retry_matches() {
    // A retried advance rebuilds the reference exactly as the first apply stored it, epoch,
    // generation and identity included, so the stored version is not refused.
    let (mut manifest, _, publisher) = advance_fixture();
    manifest.original_epoch = OriginalEpoch(3);
    let mut op = advance_operation(manifest, publisher);
    op.destination_bound = true;
    let rebuilt = op.reference_version().unwrap();
    let effects = op.write_blob_version();
    let (_, _, value) = &written(&effects)[0];
    let stored = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(
        (stored.generation, stored.original_epoch),
        (8, OriginalEpoch(3))
    );
    assert_eq!(rebuilt, stored);
}

#[test]
fn refreshed_advance_matches() {
    // A local refresh after the first apply moves only `last_refresh`, so a retry still matches.
    let (manifest, _, publisher) = advance_fixture();
    let op = advance_operation(manifest, publisher);
    let incoming = op.reference_version().unwrap();
    let mut refreshed = incoming.clone();
    let BlobVersionState::Reference { last_refresh, .. } = &mut refreshed.state else {
        panic!("expected a reference")
    };
    *last_refresh += Duration::from_secs(60);
    assert!(super::same_reference(&refreshed, &incoming));
    let mut other = refreshed.clone();
    other
        .metadata
        .insert("changed".to_string(), "yes".to_string());
    assert!(
        !super::same_reference(&other, &incoming),
        "other fields still count"
    );
}

#[test]
fn advance_replay_orders() {
    // A head written at a later epoch already supersedes a replayed advance, whatever its
    // generation; comparing generations alone would refuse the replay as invalid lineage.
    let (mut manifest, _, publisher) = advance_fixture();
    manifest.original_epoch = OriginalEpoch(1);
    let mut op = advance_operation(manifest, publisher);
    op.destination_bound = true;
    op.advance_version_exists = true;
    let later = CurrentVersionPointer::new_with_generation(Ulid::from_bytes([30u8; 16]), 1)
        .with_epoch(OriginalEpoch(2));
    op.write_compared_object(Some(&later.to_bytes().unwrap()))
        .unwrap();
    assert_eq!(op.state, IncomingVersionState::WriteLiveObligation);
    assert_eq!(op.existing_current_pointer, Some(later));
    assert_eq!(op.pending_head, None);
}

// One advance mints exactly one successor: a repeated, skipped or overflowing
// count would let a publisher reset the cap by replaying advances.
#[test]
fn advance_rejects_counts() {
    let (manifest, previous, publisher) = advance_fixture();
    for count in [Some(0), Some(2), None] {
        let mut replayed = manifest.clone();
        replayed.reference_advance_count = count;
        let op = advance_operation(replayed, publisher);
        assert!(op.validate_advance(&previous).is_err());
    }

    let mut exhausted = previous.clone();
    exhausted.state = BlobVersionState::Reference {
        source: manifest.source.clone().unwrap(),
        cached_metadata: manifest.reference_metadata.clone().unwrap(),
        last_refresh: SystemTime::UNIX_EPOCH,
        advance_count: u16::MAX,
    };
    assert_advance_invalid(manifest, publisher, exhausted);
}

// Repair and snapshot replication reconstruct the reference with the cap the
// manifest carries, and refuse a manifest that omits it.
#[test]
fn reference_keeps_count() {
    let mut manifest = make_reference_manifest();
    manifest.reference_advance_count = Some(12);
    let publisher = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
    let op = advance_operation(manifest.clone(), publisher);
    assert_eq!(op.reference_version().unwrap().advance_count(), Some(12));

    manifest.reference_advance_count = None;
    let op = advance_operation(manifest, publisher);
    assert_eq!(
        op.reference_version().unwrap_err(),
        IncomingVersionError::ReferenceCountMissing
    );
}

#[test]
fn advance_needs_bucket() {
    let (manifest, _, publisher) = advance_fixture();
    let mut op = advance_operation(manifest, publisher);
    op.manifest_policy = Some(op.target_authorization_path(test_group_id()));

    op.first().unwrap();
    let effects = op.next(bucket_read(None));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(!op.create_attempted);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::DestinationNotFound
            .to_string()
            .as_str(),
    );
}

#[test]
fn advance_requires_predecessor() {
    let (manifest, _, publisher) = advance_fixture();
    let advance = manifest.reference_advance.unwrap();
    let other = Ulid::from_bytes([33u8; 16]);
    let pointers = [
        None,
        Some(CurrentVersionPointer::new_with_generation(
            advance.predecessor,
            advance.generation - 2,
        )),
        Some(CurrentVersionPointer::new_with_generation(
            other,
            advance.generation - 1,
        )),
        Some(CurrentVersionPointer::new_with_generation(
            advance.predecessor,
            advance.generation,
        )),
    ];

    for pointer in pointers {
        let mut op = advance_operation(manifest.clone(), publisher);
        start_apply_transaction(&mut op);
        let turn = op.next(row(
            pointer.map(|pointer| pointer.to_bytes().unwrap().into())
        ));
        assert!(matches!(
            turn,
            Err(IncomingVersionError::InvalidReferenceAdvance)
        ));
    }

    let mut op = advance_operation(manifest, publisher);
    start_apply_transaction(&mut op);
    op.next(row(Some(
        CurrentVersionPointer::new_with_generation(advance.predecessor, advance.generation - 1)
            .to_bytes()
            .unwrap()
            .into(),
    )))
    .unwrap();
    let turn = op.next(row(None));
    assert!(matches!(
        turn,
        Err(IncomingVersionError::InvalidReferenceAdvance)
    ));
}

#[test]
fn advance_checks_publisher() {
    let (manifest, mut previous, publisher) = advance_fixture();
    previous.published_by = Some(iroh::SecretKey::from_bytes(&[36u8; 32]).public());

    assert_advance_invalid(manifest, publisher, previous);
}

#[test]
fn advance_preserves_identity() {
    let (manifest, previous, publisher) = advance_fixture();

    let mut changed_creator = previous.clone();
    changed_creator.created_by = UserId::local(Ulid::from_bytes([37u8; 16]), test_realm_id());
    assert_advance_invalid(manifest.clone(), publisher, changed_creator);

    let mut changed_metadata = previous.clone();
    changed_metadata
        .metadata
        .insert("s3-key".to_string(), "changed".to_string());
    assert_advance_invalid(manifest.clone(), publisher, changed_metadata);

    let mut changed_binding = previous.clone();
    let BlobVersionState::Reference { source, .. } = &mut changed_binding.state else {
        panic!("expected reference predecessor")
    };
    source.descriptor.source_path = "changed/path".to_string();
    assert_advance_invalid(manifest.clone(), publisher, changed_binding);

    let mut non_native_manifest = manifest;
    non_native_manifest.source.as_mut().unwrap().descriptor.kind = SourceConnectorKind::Http;
    let mut non_native_previous = previous;
    let BlobVersionState::Reference { source, .. } = &mut non_native_previous.state else {
        panic!("expected reference predecessor")
    };
    source.descriptor.kind = SourceConnectorKind::Http;
    assert_advance_invalid(non_native_manifest, publisher, non_native_previous);
}

#[test]
fn advance_rejects_collision() {
    let (manifest, _, publisher) = advance_fixture();
    let mut op = advance_operation(manifest, publisher);
    let mut collision = op.reference_version().unwrap();
    collision
        .metadata
        .insert("collision".to_string(), "true".to_string());
    advance_version_lookup(&mut op, test_group_id()).unwrap();

    let effects = op.next(row(Some(collision.to_bytes().unwrap().into())));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::InvalidReferenceAdvance
            .to_string()
            .as_str(),
    );
}

#[test]
fn later_head_noop() {
    let (manifest, _, publisher) = advance_fixture();
    let advance = manifest.reference_advance.unwrap();
    let mut op = advance_operation(manifest, publisher);
    let duplicate = op.reference_version().unwrap();
    advance_version_lookup(&mut op, test_group_id()).unwrap();

    let effects = op.next(row(Some(duplicate.to_bytes().unwrap().into())));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
    start_apply_with(
        &mut op,
        Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
    );
    let effects = op.next(row(Some(
        CurrentVersionPointer::new_with_generation(
            Ulid::from_bytes([50u8; 16]),
            advance.generation + 1,
        )
        .to_bytes()
        .unwrap()
        .into(),
    )));
    let (key_space, _, value) = &written(&effects)[0];
    assert_eq!(key_space, REPLICATION_OBLIGATION_KEYSPACE);
    let obligation = LiveObligationRecord::from_bytes(value).unwrap();
    assert_eq!(obligation.reference_advance, Some(advance));
    assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

    let effects = op.next(IoResult::Done);
    assert!(matches!(effects, Ok(Next::Done(_))));
}

#[test]
fn replacement_cleans_metadata() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let version_id = manifest.version_id;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(14, 14),
        iroh::SecretKey::from_bytes(&[71; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.destination_group_id = Some(test_group_id());
    op.replaced_version = Some(BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1600000060),
        test_user_id(),
        None,
    ));

    let effects = op.read_replaced_metadata();
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Page { space, .. })) if space == OBJECT_METADATA_KEYSPACE
    ));

    let part_key = MultipartObjectKey::part(version_id, 3).to_bytes().unwrap();
    let effects = op.next(page(
        vec![(part_key.clone().into(), vec![1u8].into())],
        None,
    ));
    let Ok(Next::Call(IoCall::Delete(deletes))) = effects else {
        panic!("expected replacement metadata batch delete")
    };
    let summary_key = MultipartObjectKey::summary(version_id).to_bytes().unwrap();
    assert!(deletes.iter().any(|(key_space, key)| {
        key_space == OBJECT_METADATA_KEYSPACE && key.as_ref() == summary_key
    }));
    assert!(deletes.iter().any(|(key_space, key)| {
        key_space == OBJECT_METADATA_KEYSPACE && key.as_ref() == part_key
    }));
    assert!(deletes.iter().any(|(key_space, key)| {
        key_space == PATHS_INDEX_KEYSPACE
            && HashIndex::from_bytes(key.as_ref()).is_ok_and(|index| index.blake3_hash == [9u8; 32])
    }));

    let effects = op.next(IoResult::Done);
    assert_eq!(op.state, IncomingVersionState::WriteReclaimCandidate);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Write(rows))) if rows[0].0 == BLOB_RECLAIM_KEYSPACE
    ));

    let effects = op.next(IoResult::Done);
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_HEAD_KEYSPACE
    ));
}

#[test]
fn replacement_queues_reclaim() {
    // The copy a replaced materialized version named is unreferenced once
    // the replacement names a different one, and only this enqueue frees it.
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(16, 16),
        iroh::SecretKey::from_bytes(&[72; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.replaced_version = Some(BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1600000120),
        test_user_id(),
        None,
    ));

    assert_eq!(
        op.replaced_reclaim_key(),
        Some(ReclaimCandidateKey::new(
            BackendRef::node_default(),
            EncodingClass::Raw,
            [9u8; 32]
        ))
    );

    // The replacement adopting the very same copy must not queue it.
    let mut adopted = make_location();
    adopted.backend = BackendRef::node_default();
    adopted
        .hashes
        .insert("blake3".to_string(), [9u8; 32].to_vec());
    op.received_blob = Some(ReceivedBlob::reserved(adopted));
    assert_eq!(op.replaced_reclaim_key(), None);
}

#[test]
fn replaced_version_fenced() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(18, 18),
        iroh::SecretKey::from_bytes(&[73; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    let prior = BlobVersion::deleted(manifest.created_at, manifest.created_by);
    op.replaced_version = Some(prior);
    op.state = IncomingVersionState::Apply;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(19, 19));

    enter_apply(&mut op).unwrap();
    op.next(row(None)).unwrap();
    let effects = op.next(no_drift());
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    let current = BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        manifest.created_at,
        manifest.created_by,
        None,
    );
    let turn = op.next(row(Some(current.to_bytes().unwrap().into())));

    assert_eq!(
        turn,
        Err(IncomingVersionError::StorageError(
            StorageError::TransactionConflict
        ))
    );
}

#[test]
fn changed_reference_updates() {
    let manifest = make_reference_manifest();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(21, 21),
        iroh::SecretKey::from_bytes(&[74; 32]).public(),
        test_realm_id(),
        manifest.clone(),
    );
    let _effects = advance_version_lookup(&mut op, test_group_id());
    let mut metadata = manifest.reference_metadata.clone().unwrap();
    metadata.etag = Some("old-etag".to_string());
    let existing = BlobVersion::reference(
        manifest.source.clone().unwrap(),
        metadata,
        manifest.created_at,
        manifest.created_by,
        manifest.created_at,
    );

    let effects = op.next(row(Some(existing.to_bytes().unwrap().into())));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
}

// A sync destination's hop names the sender's bucket and key, never its own.
#[test]
fn hop_names_source() {
    use aruna_core::structs::storage::origin::OriginVersion;
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let relationship_id = Ulid::from_parts(22, 22);
    manifest.origin = Some(SyncOrigin {
        relationship_id,
        hop_count: 1,
    });
    manifest.sync_source = Some(("source-bucket".to_string(), "raw/a.txt".to_string()));
    let publisher = iroh::SecretKey::from_bytes(&[75; 32]).public();
    let op = IncomingVersionOperation::new(
        Ulid::from_parts(23, 23),
        publisher,
        test_realm_id(),
        manifest,
    );

    let hop = op.hop().unwrap();
    assert_eq!(hop.relationship_id, relationship_id);
    let OriginVersion::Node(arn) = hop.from else {
        panic!("an ordinary source is named by its node ARN")
    };
    assert_eq!(
        (arn.bucket.as_str(), arn.key.as_str()),
        ("source-bucket", "raw/a.txt")
    );
}

#[test]
fn hop_limit_rejects() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.origin = Some(SyncOrigin {
        relationship_id: Ulid::from_parts(22, 22),
        hop_count: 5,
    });
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(23, 23),
        iroh::SecretKey::from_bytes(&[75; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.first();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::HopLimitExceeded.to_string().as_str(),
    );
}

#[test]
fn rejects_manifest_size() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest
        .metadata
        .insert("metadata".to_string(), "x".repeat(MAX_VALUE_BYTES + 1));
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(24, 24),
        iroh::SecretKey::from_bytes(&[76; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.first();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        "Failed to convert from str: replication manifest entry is too large",
    );
}

#[test]
fn checks_serving_realm() {
    // A user of another realm is served here; a session of another realm, or an anonymous
    // identity of another realm, is refused.
    let foreign = RealmId::from_bytes([8u8; 32]);
    let user = |realm_id| UserId::local(Ulid::from_parts(25, 25), realm_id);
    for (user_id, serving_realm, accepted) in [
        (user(foreign), test_realm_id(), true),
        (user(test_realm_id()), foreign, false),
        (UserId::nil(foreign), test_realm_id(), false),
    ] {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.auth_context.user_id = user_id;
        manifest.auth_context.realm_id = serving_realm;
        if let Some(writer) = manifest.writer_auth_context.as_mut() {
            writer.user_id = manifest.auth_context.user_id;
        }
        let mut op = IncomingVersionOperation::new(
            Ulid::from_parts(26, 26),
            iroh::SecretKey::from_bytes(&[77; 32]).public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.first();

        if accepted {
            assert_ne!(op.state, IncomingVersionState::SendNegotiation);
        } else {
            expect_rejected_negotiation(
                asked(&effects),
                IncomingVersionError::RealmMismatch.to_string().as_str(),
            );
        }
    }
}

#[test]
fn obligation_keeps_origin() {
    let origin = SyncOrigin {
        relationship_id: Ulid::from_parts(27, 27),
        hop_count: 2,
    };
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.origin = Some(origin.clone());
    manifest.upstream_sources.push(
        aruna_core::structs::storage::replication::ArunaArn::s3_bucket(
            test_realm_id(),
            iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            "source",
        )
        .unwrap(),
    );
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(28, 28),
        iroh::SecretKey::from_bytes(&[78; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.manifest.writer_auth_context = Some(op.manifest.auth_context.clone());

    let effects = op.write_live_obligation();

    let (_, _, value) = &written(&effects)[0];
    let obligation = LiveObligationRecord::from_bytes(value).unwrap();
    assert_eq!(obligation.origin, Some(origin));
    assert_eq!(obligation.upstream_sources, op.manifest.upstream_sources);
}

#[test]
fn obligation_keeps_lineage() {
    let (mut manifest, _, publisher) = advance_fixture();
    let reader = manifest.auth_context.clone();
    let advance = manifest.reference_advance.unwrap();
    let origin = SyncOrigin {
        relationship_id: Ulid::from_bytes([44u8; 16]),
        hop_count: 2,
    };
    let source = aruna_core::structs::storage::replication::ArunaArn::s3_bucket(
        test_realm_id(),
        iroh::SecretKey::from_bytes(&[45u8; 32]).public(),
        "source",
    )
    .unwrap();
    manifest.origin = Some(origin.clone());
    manifest.upstream_sources = vec![source.clone()];
    let mut op = advance_operation(manifest, publisher);

    let effects = op.write_live_obligation();

    let (_, _, value) = &written(&effects)[0];
    let obligation = LiveObligationRecord::from_bytes(value).unwrap();
    assert_eq!(obligation.auth_context, reader);
    assert_eq!(obligation.reference_advance, Some(advance));
    assert_eq!(obligation.origin, Some(origin));
    assert_eq!(obligation.upstream_sources, vec![source]);
}

#[test]
fn quota_excess_rejects() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let group_id = test_group_id();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(29, 29),
        iroh::SecretKey::from_bytes(&[79; 32]).public(),
        test_realm_id(),
        manifest,
    );
    let _effects = advance_version_lookup(&mut op, group_id);
    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
    config.quota = QuotaConfig {
        default_quota_bytes: Some(1),
        grace_factor_percent: 100,
        ..QuotaConfig::default()
    };
    let config_bytes = postcard::to_allocvec(&config).unwrap();
    let effects = op.next(row(Some(config_bytes.into())));
    assert_eq!(op.state, IncomingVersionState::EnforceQuota);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Child(_)))));

    let exceeded = ChildOutcome::new(Ok::<bool, QuotaGateError>(true));
    let effects = op.next(IoResult::Child(exceeded));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    match message_from_effect(asked(&effects)) {
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::Rejected(reason),
        ) => assert_eq!(reason, "quota"),
        other => panic!("expected quota rejection, got {other:?}"),
    }
}

#[test]
fn probe_reads_snapshot() {
    // The probe sums the usage in one read snapshot and reports the excess to its parent.
    let local = iroh::SecretKey::from_bytes(&[80; 32]).public();
    let gate = QuotaGate::new_for_realm(1, 42, test_group_id(), local, test_realm_id());
    let mut probe = QuotaProbe { gate };
    assert_eq!(probe.transaction(), TransactionMode::ReadSnapshot);

    let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
    config.quota.default_quota_bytes = Some(1);
    let config = postcard::to_allocvec(&config).unwrap();
    assert!(matches!(probe.first(), Ok(Next::Call(IoCall::Read(_)))));
    assert!(matches!(probe.next(rows(vec![None])), Ok(Next::Call(_))));
    assert!(matches!(
        probe.next(rows(vec![Some(config.into())])),
        Ok(Next::Call(_))
    ));
    assert!(matches!(probe.next(rows(vec![None])), Ok(Next::Call(_))));
    assert!(matches!(
        probe.next(page(Vec::new(), None)),
        Ok(Next::Done(true))
    ));
}

#[test]
fn copies_skip_quota() {
    // A copy into a distributed bucket passes a full quota unless the group counts replicas.
    let cases = [
        (false, IncomingVersionState::ReadExistingBlob),
        (true, IncomingVersionState::EnforceQuota),
    ];
    for (count_replicas, expected) in cases {
        let mut op = IncomingVersionOperation::new(
            Ulid::from_parts(33, 33),
            iroh::SecretKey::from_bytes(&[82; 32]).public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        let _effects = advance_version_lookup(&mut op, test_group_id());
        op.next(row(None)).unwrap();
        assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
        // Stands in for a bound bucket, which admits only its coordinator's publications.
        op.destination_bound = true;
        let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
        config.quota = QuotaConfig {
            default_quota_bytes: Some(1),
            grace_factor_percent: 100,
            ..QuotaConfig::default()
        };
        config.replica_quota.count = count_replicas;
        op.next(row(Some(postcard::to_allocvec(&config).unwrap().into())))
            .unwrap();
        assert_eq!(op.state, expected);
        assert_eq!(
            op.quota_ceiling.is_some(),
            count_replicas,
            "no commit check either"
        );
    }
}

#[test]
fn full_backend_rejects() {
    // Replication now routes through the quota-marked catalog, so a full
    // destination backend owes the sender a reason before any transfer.
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let group_id = test_group_id();
    let mut routing = NodeRouting::default();
    routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(31, 31),
        iroh::SecretKey::from_bytes(&[80; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_routing(routing);

    let _effects = advance_version_lookup(&mut op, group_id);
    let effects = advance_blob_lookup(&mut op);
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));
    let effects = op.next(row(None));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    match message_from_effect(asked(&effects)) {
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::Rejected(reason),
        ) => assert!(reason.contains("quota"), "unexpected reason: {reason}"),
        other => panic!("expected a rejected negotiation, got {other:?}"),
    }
}

#[test]
fn full_backend_dedupes() {
    // A blob the destination already holds stores no bytes, so its cap has
    // nothing left to protect.
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let existing = manifest
        .blob
        .as_ref()
        .map(|blob| blob.location.clone())
        .unwrap();
    let group_id = test_group_id();
    let mut routing = NodeRouting::default();
    routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(32, 32),
        iroh::SecretKey::from_bytes(&[81; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_routing(routing);

    let _effects = advance_version_lookup(&mut op, group_id);
    advance_blob_lookup(&mut op).unwrap();
    let effects = op.next(row(Some(existing.to_bytes().unwrap().into())));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
}

#[test]
fn marker_ignores_quota() {
    // A delete marker stores no bytes, so a full destination must still let
    // the tombstone converge.
    let group_id = test_group_id();
    let mut routing = NodeRouting::default();
    routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(33, 33),
        iroh::SecretKey::from_bytes(&[82; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::DeleteMarker),
    )
    .with_routing(routing);

    let _effects = advance_version_lookup(&mut op, group_id);
    let effects = op.next(row(None));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
}

#[test]
fn head_releases_observation() {
    // An offered directory observation is charged only as the head, so a
    // replicated head over it releases its bytes.
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.current_version_generation = Some(10);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(34, 34),
        iroh::SecretKey::from_bytes(&[83; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::from_parts(35, 35), 5);
    op.next(row(Some(existing_pointer.to_bytes().unwrap().into())))
        .unwrap();
    assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);

    let mut source = make_source_binding();
    source.connector_id = None;
    source.descriptor.kind = SourceConnectorKind::LocalDirectory;
    source.descriptor.source_path = manifest.key.clone();
    source.descriptor.public_config = HashMap::from([(
        aruna_core::structs::execution::offered_directory::OFFERED_DIRECTORY_BUCKET.to_string(),
        manifest.bucket.clone(),
    )]);
    let metadata = SourceMetadata {
        content_length: 7,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let now = fixed_created_at();
    let head = BlobVersion::reference(source, metadata, now, test_user_id(), now);
    op.next(row(Some(head.to_bytes().unwrap().into()))).unwrap();

    let delta = op.usage_delta().unwrap();
    assert_eq!((delta.objects, delta.referenced_bytes), (-1, -7));
}

#[test]
fn stale_pointer_skips() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.current_version_generation = Some(10);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(34, 34),
        iroh::SecretKey::from_bytes(&[83; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::from_parts(35, 35), 20);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Write(rows))) if rows[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    assert_eq!(op.object_delta, 0);
    assert_eq!(op.usage_delta().unwrap().objects, 0);
}

#[test]
fn rejects_missing_generation() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.current_version_generation = None;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(36, 36),
        iroh::SecretKey::from_bytes(&[84; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.state = IncomingVersionState::Apply;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);

    enter_apply(&mut op).unwrap();
    op.next(row(None)).unwrap();
    let effects = op.next(no_drift());
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    let effects = op.next(row(None));

    assert!(matches!(
        effects,
        Err(IncomingVersionError::MissingVersionGeneration)
    ));
}

#[test]
fn rejects_bad_pointer() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(38, 38),
        iroh::SecretKey::from_bytes(&[85; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    start_apply_transaction(&mut op);

    let effects = op.next(row(Some(vec![255, 255, 255].into())));

    assert!(matches!(
        effects,
        Err(IncomingVersionError::ConversionError(_))
    ));
}

#[test]
fn stale_pointer_writes() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.current_version_generation = Some(1);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(39, 39),
        iroh::SecretKey::from_bytes(&[86; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::from_parts(40, 40), 2);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    let (_, _, value) = &written(&effects)[0];
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert!(version.is_deleted());
    assert_eq!(version.created_by, manifest.created_by);
}

#[test]
fn preserves_source_binding() {
    let source = make_source_binding();
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    manifest.source = Some(source.clone());
    manifest
        .metadata
        .insert("mtime".to_string(), "1753272000.123456789".to_string());
    let expected_metadata = manifest.metadata.clone();
    manifest.current_version = false;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(41, 41),
        iroh::SecretKey::from_bytes(&[87; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.destination_group_id = Some(Ulid::from_parts(43, 43));
    op.existing_blob_location = Some(make_location());

    let effects = op.write_version();

    let (_, _, value) = &written(&effects)[0];
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    // The binding is stored compact, with its root record written in the same transaction.
    let stored = version.source_binding().unwrap();
    assert!(stored.descriptor.root.is_some());
    assert!(
        aruna_core::structs::storage::distribution::source_root::RootEntry::same_source(
            stored, &source
        )
    );
    let root_space = aruna_core::keyspaces::ROOT_RECORD_KEYSPACE;
    assert!(written(&effects).iter().any(|row| row.0 == root_space));
    assert_eq!(version.metadata, expected_metadata);
}

#[test]
fn rewrite_journals_nothing() {
    // Only a version new to this node is an upload; rewriting one already here journals nothing.
    let journaled = |existing: Option<BlobVersion>| {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let mut op = IncomingVersionOperation::new(
            Ulid::from_parts(45, 45),
            iroh::SecretKey::from_bytes(&[88; 32]).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.destination_group_id = Some(Ulid::from_parts(44, 44));
        op.existing_blob_location = Some(make_location());
        op.replaced_version = existing;
        let turn = op.write_version();
        usize::from(written(&turn).iter().any(|row| row.0 == "event_journal"))
    };
    assert_eq!(journaled(None), 1);
    let existing = BlobVersion::materialized(
        [1u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1600000060),
        test_user_id(),
        None,
    );
    assert_eq!(journaled(Some(existing)), 0);
}

#[test]
fn indexes_noncurrent_version() {
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    manifest.current_version = false;
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    let group_id = Ulid::from_parts(44, 44);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(45, 45),
        iroh::SecretKey::from_bytes(&[88; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    op.destination_group_id = Some(group_id);
    op.existing_blob_location = Some(make_location());

    // The version, its hash index, managed copy and journal rows commit in one write.
    let effects = op.write_version();
    let writes = written(&effects);
    assert_eq!(writes[0].0, BLOB_VERSIONS_KEYSPACE);
    let position = |space: &str| writes.iter().position(|row| row.0 == space).unwrap();
    let index = position(PATHS_INDEX_KEYSPACE);
    let copy = position(aruna_core::keyspaces::MANAGED_COPY_KEYSPACE);
    let journal = position("event_journal");
    assert!(index < copy && copy < journal);

    let index_key = HashIndex::from_bytes(writes[index].1.as_ref()).unwrap();
    assert_eq!(index_key.blake3_hash, [1u8; 32]);
    assert_eq!(index_key.version_id, manifest.version_id);
    assert_eq!(index_key.group_id, group_id);
    assert_eq!(index_key.bucket, manifest.bucket);
    assert_eq!(index_key.key, manifest.key);

    assert_eq!(
        aruna_core::structs::storage::blob::ManagedCopyRecord::from_bytes(writes[copy].2.as_ref())
            .unwrap()
            .version,
        aruna_core::structs::storage::blob::VersionKey::new(
            &manifest.bucket,
            &manifest.key,
            manifest.version_id
        )
    );

    // The version's journal rows come last.
    assert!(writes[journal..].iter().all(|row| row.0 == "event_journal"));
    let effects = op.next(IoResult::Done);
    assert_eq!(op.state, IncomingVersionState::WriteLiveObligation);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Write(rows))) if rows[0].0 == REPLICATION_OBLIGATION_KEYSPACE
    ));

    let effects = op.next(IoResult::Done);
    assert_eq!(op.state, IncomingVersionState::UpdateUsage);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::AddUsage { .. }))));

    let effects = op.next(IoResult::Done);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Write(_)))));
    let effects = op.next(IoResult::Done);
    assert!(matches!(effects, Ok(Next::Done(_))));
}

#[test]
fn newer_generation_rollback() {
    let existing_version_id = Ulid::from_bytes([9u8; 16]);
    let incoming_version_id = Ulid::from_bytes([1u8; 16]);
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.version_id = incoming_version_id;
    manifest.current_version_generation = Some(20);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(47, 47),
        iroh::SecretKey::from_bytes(&[89; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest.clone(),
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 10);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
    let effects = op.next(row(Some(
        BlobVersion::materialized(
            [2u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1600000180),
            test_user_id(),
            None,
        )
        .to_bytes()
        .unwrap()
        .into(),
    )));

    let (key_space, _, value) = &written(&effects)[0];
    assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(
        CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(
            incoming_version_id,
            manifest.current_version_generation.unwrap()
        )
    );
    assert_eq!(op.object_delta, -1);
    assert_eq!(op.usage_delta().unwrap().objects, -1);
}

#[test]
fn materialized_restores_object() {
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    manifest.current_version_generation = Some(2);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(48, 48),
        iroh::SecretKey::from_bytes(&[90; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::from_parts(49, 49), 1);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_VERSIONS_KEYSPACE
    ));

    op.next(row(Some(
        BlobVersion::deleted(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1600000240),
            test_user_id(),
        )
        .to_bytes()
        .unwrap()
        .into(),
    )))
    .unwrap();

    let delta = op.usage_delta().unwrap();
    assert_eq!(delta.objects, 1);
    assert_eq!(delta.logical_bytes, 42);
}

#[test]
fn higher_ulid_skips() {
    // A same-generation incoming version cannot replace the node-local head.
    let existing_version_id = Ulid::from_bytes([1u8; 16]);
    let incoming_version_id = Ulid::from_bytes([9u8; 16]);
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.version_id = incoming_version_id;
    manifest.current_version_generation = Some(7);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(50, 50),
        iroh::SecretKey::from_bytes(&[91; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Write(rows))) if rows[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
}

#[test]
fn lower_ulid_skips() {
    let existing_version_id = Ulid::from_bytes([9u8; 16]);
    let incoming_version_id = Ulid::from_bytes([1u8; 16]);
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.version_id = incoming_version_id;
    manifest.current_version_generation = Some(7);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(51, 51),
        iroh::SecretKey::from_bytes(&[92; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

    let effects = op.next(row(Some(existing_pointer.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Write(rows))) if rows[0].0 == BLOB_VERSIONS_KEYSPACE
    ));
}

#[test]
fn canonical_auth_path() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.bucket = "bucket-a".to_string();
    manifest.key = "nested/file.txt".to_string();
    let local_node_id = iroh::SecretKey::from_bytes(&[93; 32]).public();
    let local_realm_id = RealmId::from_bytes([7u8; 32]);
    let op = IncomingVersionOperation::new(
        Ulid::from_parts(52, 52),
        local_node_id,
        local_realm_id,
        manifest,
    );
    let group_id = Ulid::from_bytes([4u8; 16]);

    assert_eq!(
        op.target_authorization_path(group_id),
        aruna_core::structs::storage::blob::object_permission_path(
            local_realm_id,
            group_id,
            local_node_id,
            "bucket-a",
            "nested/file.txt",
        )
    );
}

#[test]
fn mismatch_requests_transfer() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(53, 53),
        iroh::SecretKey::from_bytes(&[94; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );

    let _effects = advance_version_lookup(&mut op, Ulid::from_parts(54, 54));
    let effects = advance_blob_lookup(&mut op);
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_LOCATIONS_KEYSPACE
    ));

    let mut mismatched_location = make_location();
    mismatched_location.blob_size += 1;
    let effects = op.next(row(Some(mismatched_location.to_bytes().unwrap().into())));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedBlobVersion
        )
    ));
}

#[test]
fn missing_blob_location() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(55, 55),
        iroh::SecretKey::from_bytes(&[95; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );

    let _effects = advance_version_lookup(&mut op, Ulid::from_parts(56, 56));
    let effects = advance_blob_lookup(&mut op);

    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_LOCATIONS_KEYSPACE
    ));
}

/// Drives an incoming materialized version to the existing-copy probe under
/// the given bucket rules and group inputs.
fn probe_backend(
    rules: Vec<StorageRoutingRule>,
    inputs: GroupRoutingInputs,
) -> (IncomingVersionOperation, Turn) {
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(57, 57),
        iroh::SecretKey::from_bytes(&[96; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    let mut bucket_info = make_bucket_info(test_group_id());
    bucket_info.storage_routing = rules;
    op.manifest_policy = Some(op.target_authorization_path(bucket_info.group_id));
    op.writer_policy = Some(op.target_authorization_path(bucket_info.group_id));

    op.first().unwrap();
    op.next(bucket_read(Some(bucket_info.to_bytes().unwrap().into())))
        .unwrap();
    load_routing(&mut op, inputs).unwrap();
    let effects = advance_blob_lookup(&mut op);
    (op, effects)
}

fn group_backend_key(backend_id: Ulid) -> Vec<u8> {
    BlobLocationKey::new([1u8; 32], EncodingClass::Raw, BackendRef::Group(backend_id)).to_bytes()
}

#[test]
fn metadata_waits_approval() {
    let (mut op, _) = probe_backend(Vec::new(), GroupRoutingInputs::default());
    op.next(row(Some(make_location().to_bytes().unwrap().into())))
        .unwrap();
    let stream_id = op.stream_id;
    let effects = op.next(blob(BlobEvent::MessageSent { stream_id }));
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::ReadMessage { .. })
    ));
    let effects = op.next(blob(BlobEvent::MessageReceived {
        stream_id,
        payload: VersionReplicationMessage::VersionApplyApproved
            .to_bytes()
            .unwrap(),
    }));
    assert_eq!(op.state, IncomingVersionState::Apply);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Child(_)))));
}

fn probed_key(effects: &Turn) -> Vec<u8> {
    let Ok(Next::Call(IoCall::Read(reads))) = effects else {
        panic!("expected one location read, got {effects:?}")
    };
    reads[0].1.to_vec()
}

#[test]
fn refuses_vanished_copy() {
    // The adopted copy is re-read in the transaction, so a sweep that
    // removed it in between must fail the apply instead of committing.
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(58, 58),
        iroh::SecretKey::from_bytes(&[97; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.destination_group_id = Some(test_group_id());
    op.existing_blob_location = Some(make_location());

    let effects = op.begin_blob_location();
    assert_eq!(op.state, IncomingVersionState::VerifyExistingBlob);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == BLOB_LOCATIONS_KEYSPACE
    ));

    let effects = op.next(row(None));

    assert!(matches!(
        effects,
        Err(IncomingVersionError::ExistingBlobChanged)
    ));
}

#[test]
fn probes_rule_backend() {
    // The probe must ask about the backend the bucket rule names.
    let backend_id = Ulid::from_bytes([4u8; 16]);
    let (op, effects) = probe_backend(
        vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Group(backend_id)),
        }],
        GroupRoutingInputs {
            default_target: None,
            backend_ids: BTreeSet::from([backend_id]),
            ..Default::default()
        },
    );

    assert_eq!(
        op.resolve_destination().unwrap().backend,
        BackendRef::Group(backend_id)
    );
    assert_eq!(probed_key(&effects), group_backend_key(backend_id));
}

#[test]
fn probes_group_default() {
    let backend_id = Ulid::from_bytes([5u8; 16]);
    let (_op, effects) = probe_backend(
        Vec::new(),
        GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            backend_ids: BTreeSet::from([backend_id]),
            ..Default::default()
        },
    );

    assert_eq!(probed_key(&effects), group_backend_key(backend_id));
}

#[test]
fn keeps_loaded_inputs() {
    // The version-only path resolves from the same inputs the probe used.
    let backend_id = Ulid::from_bytes([6u8; 16]);
    let (mut op, _effects) = probe_backend(
        Vec::new(),
        GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            backend_ids: BTreeSet::from([backend_id]),
            ..Default::default()
        },
    );
    let mut location = make_location();
    location.backend = BackendRef::Group(backend_id);

    op.next(row(Some(location.to_bytes().unwrap().into())))
        .unwrap();

    assert_eq!(
        op.negotiation_result,
        Some(ReplicationNegotiationResult::NeedVersionOnly)
    );
    assert_eq!(
        op.resolve_destination().unwrap().backend,
        BackendRef::Group(backend_id)
    );
}

#[test]
fn rejects_mismatched_blob() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let stream_id = Ulid::from_parts(60, 60);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[98; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    let mut mismatched_location = make_location();
    mismatched_location.blob_size += 1;

    op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobVersion);
    op.state = IncomingVersionState::ReceiveBlob;

    let effects = op.next(blob(BlobEvent::ReplicationFinished {
        location: mismatched_location.clone(),
    }));
    let reason = effects.unwrap_err().to_string();
    assert_eq!(
        tail(&op.abort()),
        [
            Tail::Reject(reason),
            Tail::Delete(mismatched_location),
            Tail::Close
        ]
    );
}

#[test]
fn write_cleanup_rejects() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let stream_id = Ulid::from_parts(61, 61);
    let received = make_location();
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[99; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobVersion);
    op.state = IncomingVersionState::ReceiveBlob;

    let effects = op.next(blob(BlobEvent::Error(BlobError::WriteCleanup {
        location: received.clone(),
        message: "marker write failed".to_string(),
    })));
    assert_eq!(
        op.received_blob.as_ref().map(|blob| blob.location.clone()),
        Some(received.clone())
    );
    assert!(op.received_blob.as_ref().unwrap().cleanup_on_abort);
    let reason = effects.unwrap_err().to_string();
    assert_eq!(
        tail(&op.abort()),
        [Tail::Reject(reason), Tail::Delete(received), Tail::Close]
    );
}

#[test]
fn unbuildable_bucket_rejects() {
    // One create attempt, still missing, then reject and close the stream.
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let stream_id = Ulid::from_parts(62, 62);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[100; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.manifest_policy = Some(op.target_authorization_path(test_group_id()));
    op.writer_policy = Some(op.target_authorization_path(test_group_id()));

    op.first().unwrap();
    op.next(bucket_read(None)).unwrap();
    assert_eq!(op.state, IncomingVersionState::CreateDestinationBucket);
    op.next(event_result(Event::SubOperation(
        SubOperationEvent::BucketCreated {
            result: Err("boom".to_string()),
        },
    )))
    .unwrap();

    let effects = op.next(bucket_read(None));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::DestinationNotFound
            .to_string()
            .as_str(),
    );

    let effects = op.next(blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
}

#[test]
fn rejects_denied_writer() {
    // A replica whose original writer lacks WRITE on the destination path
    // is refused during negotiation.
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    let stream_id = Ulid::from_parts(63, 63);
    let group_id = Ulid::from_parts(64, 64);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[101; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    )
    .with_writer_policy(None);
    op.manifest_policy = Some(op.target_authorization_path(group_id));

    op.first().unwrap();
    op.next(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )))
    .unwrap();
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::WriterPermissionDenied
            .to_string()
            .as_str(),
    );

    let effects = op.next(blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
}

#[test]
fn rejects_missing_policy() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    let group_id = Ulid::from_parts(65, 65);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(66, 66),
        iroh::SecretKey::from_bytes(&[102; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.manifest_policy = Some(op.target_authorization_path(group_id));

    op.first().unwrap();
    op.next(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )))
    .unwrap();
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::WriterPermissionDenied
            .to_string()
            .as_str(),
    );
}

#[test]
fn rejects_manifest_policy() {
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(67, 67),
        iroh::SecretKey::from_bytes(&[103; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::DeleteMarker),
    )
    .with_manifest_policy(None);
    let group_id = Ulid::from_parts(68, 68);
    op.first().unwrap();
    op.next(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )))
    .unwrap();
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::ManifestPermissionDenied
            .to_string()
            .as_str(),
    );
}

#[test]
fn rejects_missing_writer() {
    // Ordinary replication must carry its durable original writer.
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.writer_auth_context = None;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(69, 69),
        iroh::SecretKey::from_bytes(&[104; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.first();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::WriterPermissionDenied
            .to_string()
            .as_str(),
    );
}

#[test]
fn rejects_relationship_writer() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.origin = Some(SyncOrigin {
        relationship_id: Ulid::from_parts(70, 70),
        hop_count: 0,
    });
    manifest.writer_auth_context = None;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(71, 71),
        iroh::SecretKey::from_bytes(&[105; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.first();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        asked(&effects),
        IncomingVersionError::WriterPermissionDenied
            .to_string()
            .as_str(),
    );
}

#[test]
fn allows_writer_policy() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(72, 72),
        iroh::SecretKey::from_bytes(&[106; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    let group_id = Ulid::from_parts(73, 73);
    let path = op.target_authorization_path(group_id);
    op = op
        .with_manifest_policy(Some(path.clone()))
        .with_writer_policy(Some(path));

    let _effects = advance_version_lookup(&mut op, group_id);
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);
}

#[test]
fn delete_marker_only() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(74, 74),
        iroh::SecretKey::from_bytes(&[107; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );

    let _effects = advance_version_lookup(&mut op, Ulid::from_parts(75, 75));
    let effects = op.next(row(None));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
}

#[test]
fn missing_blob_transfer() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(76, 76),
        iroh::SecretKey::from_bytes(&[108; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );

    let _effects = advance_version_lookup(&mut op, Ulid::from_parts(77, 77));
    let effects = advance_blob_lookup(&mut op);
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedBlobVersion
        )
    ));
}

#[test]
fn failure_rejects_first() {
    let negotiated = Some(ReplicationNegotiationResult::NeedVersionOnly);
    let mut op = applying(ReplicationItemKind::DeleteMarker, 109, negotiated, None);
    let error = IncomingVersionError::RealmMismatch;
    let reason = error.to_string();

    let effects = op.next(apply_failed(error));

    assert!(matches!(effects, Err(IncomingVersionError::RealmMismatch)));
    assert_eq!(tail(&op.abort()), [Tail::Reject(reason), Tail::Close]);
}

#[test]
fn failure_deletes_blobs() {
    let received = make_location();
    let negotiated = Some(ReplicationNegotiationResult::NeedBlobVersion);
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 110, negotiated, Some(received.clone()));
    let error = IncomingVersionError::StalePlan;
    let reason = error.to_string();

    assert!(op.next(apply_failed(error)).is_err());

    assert_eq!(
        tail(&op.abort()),
        [Tail::Reject(reason), Tail::Delete(received), Tail::Close]
    );
}

#[test]
fn unknown_commit_preserves() {
    // The apply queues its reconciliation row, and a commit of unknown outcome keeps the copy.
    let received = make_location();
    let negotiated = Some(ReplicationNegotiationResult::NeedBlobVersion);
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 111, negotiated, Some(received.clone()));
    let release_id = received.ulid;
    let reconcile_key: Key = received.ulid.to_bytes().to_vec().into();

    let mut child = op.apply_child();
    let effects = child.commit_or_cleanup();
    let (_, key, value) = &written(&effects)[0];
    assert_eq!(key, &reconcile_key);
    assert!(matches!(
        BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
        BlobCleanupWork::ReconcileWrite {
            owner: WriteOwner::Blob {
                blake3,
                realm_id,
                ..
            },
            ..
        } if blake3 == [1u8; 32] && realm_id == test_realm_id()
    ));
    assert!(matches!(child.next(IoResult::Done), Ok(Next::Done(_))));

    let error = IncomingVersionError::StorageError(StorageError::CommitFailed);
    let reason = error.to_string();
    assert!(op.next(apply_failed(error)).is_err());

    assert!(!op.received_blob.as_ref().unwrap().cleanup_on_abort);
    assert_eq!(
        tail(&op.abort()),
        [
            Tail::Reconcile(reconcile_key),
            Tail::Release(release_id),
            Tail::Reject(reason),
            Tail::Close
        ]
    );
}

#[test]
fn release_after_commit() {
    let received = make_location();
    let id = received.ulid;
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 112, None, Some(received));

    let effects = op.next(applied(op.result(true)));
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::ReleaseReservation { id: observed }) if *observed == id
    ));
    assert_eq!(op.state, IncomingVersionState::ReleaseReservation);
    assert!(op.apply_committed);
    assert!(!op.received_blob.as_ref().unwrap().cleanup_on_abort);

    op.next(blob(BlobEvent::ReservationReleased { id }))
        .unwrap();
    assert_eq!(op.state, IncomingVersionState::ScheduleUsage);
}

#[test]
fn conflict_commit_deletes() {
    // A refused commit proves nothing landed, so the received copy is deleted.
    let received = make_location();
    let negotiated = Some(ReplicationNegotiationResult::NeedBlobVersion);
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 113, negotiated, Some(received.clone()));
    let error = IncomingVersionError::StorageError(StorageError::TransactionConflict);
    let reason = error.to_string();

    assert!(op.next(apply_failed(error)).is_err());

    assert_eq!(
        tail(&op.abort()),
        [Tail::Reject(reason), Tail::Delete(received), Tail::Close]
    );
}

#[test]
fn commit_abort_preserves() {
    // A run cut off during the apply cannot know the commit outcome and never deletes.
    let received = make_location();
    let id = received.ulid;
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 114, None, Some(received));

    assert_eq!(
        tail(&op.abort()),
        [
            Tail::Reconcile(id.to_bytes().to_vec().into()),
            Tail::Release(id),
            Tail::Close
        ]
    );
}

#[test]
fn failure_without_delete() {
    // Without received bytes an unknown commit leaves nothing to keep or delete.
    let negotiated = Some(ReplicationNegotiationResult::NeedVersionOnly);
    let mut op = applying(ReplicationItemKind::DeleteMarker, 115, negotiated, None);
    let error = IncomingVersionError::StorageError(StorageError::CommitFailed);
    let reason = error.to_string();

    assert!(op.next(apply_failed(error)).is_err());

    assert_eq!(tail(&op.abort()), [Tail::Reject(reason), Tail::Close]);
}

#[test]
fn commit_preserves_blob() {
    // A failure after the commit neither rejects the item nor deletes the published copy.
    let received = make_location();
    let negotiated = Some(ReplicationNegotiationResult::NeedBlobVersion);
    let kind = ReplicationItemKind::Materialized;
    let mut op = applying(kind, 116, negotiated, Some(received));
    op.next(applied(op.result(true))).unwrap();

    let effects = op.next(blob(BlobEvent::DeleteFinished));

    assert!(matches!(
        effects,
        Err(IncomingVersionError::InvalidStateEvent { .. })
    ));
    assert_eq!(tail(&op.abort()), [Tail::Close]);
}

fn missing_bucket_op() -> IncomingVersionOperation {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(95, 95),
        iroh::SecretKey::from_bytes(&[117; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.manifest_policy = Some(op.target_authorization_path(test_group_id()));
    op.writer_policy = Some(op.target_authorization_path(test_group_id()));
    op.first().unwrap();
    op.next(bucket_read(None)).unwrap();
    op
}

#[test]
fn missing_bucket_autocreates() {
    let op = missing_bucket_op();
    assert_eq!(op.state, IncomingVersionState::CreateDestinationBucket);
    assert!(op.create_attempted);
    let info = op.destination_bucket_info();
    assert_eq!(info.group_id, test_group_id());
    assert_eq!(info.created_by, test_user_id());
    assert!(info.cors_configuration.is_none());
}

#[test]
fn autocreate_rereads_bucket() {
    let mut op = missing_bucket_op();
    let effects = op.next(event_result(Event::SubOperation(
        SubOperationEvent::BucketCreated { result: Ok(()) },
    )));
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(
        &effects,
        Ok(Next::Call(IoCall::Read(reads)))
            if reads[0].0 == S3_BUCKET_KEYSPACE
    ));
}

#[test]
fn create_invalid_event() {
    let mut op = missing_bucket_op();
    let effects = op.next(blob(BlobEvent::DeleteFinished));
    assert!(matches!(
        effects,
        Err(IncomingVersionError::InvalidStateEvent { .. })
    ));
}

/// Constructor-to-finalize trace of a materialized replica: every real
/// transition runs, from the bucket probe through the blob transfer, the
/// apply transaction and the committed apply acknowledgement.
#[test]
fn materialized_trace() {
    let stream_id = trace_stream_id();
    let group_id = test_group_id();
    let received = make_location();
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(iroh::SecretKey::from_bytes(&[0x31; 32]).public());
    op.clock = EnqueueClock(fixed_trace_clock);
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));

    let effects = op.first();
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    let effects = op.next(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )));
    assert_eq!(op.state, IncomingVersionState::LoadDestinationRouting);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Child(_)))));

    op.next(event_result(Event::SubOperation(
        SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        },
    )))
    .unwrap();
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);

    op.next(row(None)).unwrap();
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);

    op.next(row(None)).unwrap();
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);

    let effects = op.next(row(None));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(asked(&effects)),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedBlobVersion
        )
    ));

    let effects = op.next(blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::ReceiveBlob);
    assert!(matches!(
        asked(&effects),
        Effect::Blob(BlobEffect::HandleReplication { stream_id: id, .. }) if *id == stream_id
    ));

    let effects = op.next(blob(BlobEvent::ReplicationFinished {
        location: received.clone(),
    }));
    assert_eq!(op.state, IncomingVersionState::Apply);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Child(_)))));

    let mut child = op.apply_child();
    assert_eq!(child.transaction(), TransactionMode::Write);
    let effects = child.first();
    assert_eq!(child.state, IncomingVersionState::CheckPurgeFence);
    assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));

    child.next(row(None)).unwrap();
    assert_eq!(child.state, IncomingVersionState::CheckDrift);

    child
        .next(bucket_drift(&make_bucket_info(group_id)))
        .unwrap();
    assert_eq!(child.state, IncomingVersionState::VerifyReplaced);

    child.next(row(None)).unwrap();
    assert_eq!(child.state, IncomingVersionState::WriteBlobLocation);
    assert_eq!(child.received_blob.as_ref().unwrap().location, received);

    child.next(IoResult::Done).unwrap();
    assert_eq!(child.state, IncomingVersionState::ReadObjectLookup);

    child.next(row(None)).unwrap();
    assert_eq!(child.state, IncomingVersionState::ApplyHeadTransition);

    child.next(IoResult::Done).unwrap();
    assert_eq!(child.state, IncomingVersionState::WriteBlobVersion);

    child.next(IoResult::Done).unwrap();
    assert_eq!(child.state, IncomingVersionState::WriteLiveObligation);

    child.next(IoResult::Done).unwrap();
    assert_eq!(child.state, IncomingVersionState::UpdateUsage);

    child.next(IoResult::Done).unwrap();
    child.next(IoResult::Done).unwrap();
    assert_eq!(child.state, IncomingVersionState::WriteCleanupRow);

    let Ok(Next::Done(result)) = child.next(IoResult::Done) else {
        panic!("the apply finishes after its cleanup row");
    };
    op.next(applied(result)).unwrap();
    assert_eq!(op.state, IncomingVersionState::ReleaseReservation);

    op.next(blob(BlobEvent::ReservationReleased { id: received.ulid }))
        .unwrap();
    assert_eq!(op.state, IncomingVersionState::ScheduleUsage);

    op.next(event_result(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::PublishUsageSnapshots,
        after: Duration::from_secs(1),
    })))
    .unwrap();
    assert_eq!(op.state, IncomingVersionState::ScheduleLiveDrain);

    op.next(event_result(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainReplicationQueue,
        after: Duration::from_secs(1),
    })))
    .unwrap();
    assert_eq!(op.state, IncomingVersionState::RegisterBlobDht);

    op.next(event_result(Event::Net(NetEvent::Dht(
        DhtEvent::PutComplete {
            key: DhtKeyId::from_bytes([1u8; 32]),
            remote_attempt_count: 0,
            remote_store_count: 0,
        },
    ))))
    .unwrap();
    assert_eq!(op.state, IncomingVersionState::SendApplyComplete);

    op.next(blob(BlobEvent::MessageSent { stream_id })).unwrap();
    assert_eq!(op.state, IncomingVersionState::CloseConnection);

    let Ok(Next::Done(result)) = op.next(blob(BlobEvent::ConnectionClosed { stream_id })) else {
        panic!("the trace commits successfully");
    };
    assert!(result.applied);
    assert_eq!(result.group_id, Some(group_id));
}

/// The existing row of the version id is read with its marker status, which the bucket's
/// version and marker counts follow when the incoming row replaces it.
#[test]
fn replaced_row_kind() {
    use crate::s3::usage::RowChange;

    let group_id = test_group_id();
    type Row = fn(&VersionReplicationManifest) -> BlobVersion;
    let marker: Row = |manifest| BlobVersion::deleted(manifest.created_at, manifest.created_by);
    let version: Row = |manifest| {
        BlobVersion::materialized(
            [3; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            manifest.created_at,
            manifest.created_by,
            None,
        )
    };
    let cases = [
        (
            ReplicationItemKind::DeleteMarker,
            version,
            RowChange::VersionToMarker,
        ),
        (
            ReplicationItemKind::Materialized,
            marker,
            RowChange::MarkerToVersion,
        ),
        (ReplicationItemKind::DeleteMarker, marker, RowChange::Kept),
        (ReplicationItemKind::Materialized, version, RowChange::Kept),
    ];
    for (kind, existing, expected) in cases {
        let manifest = make_manifest(kind.clone());
        let existing = existing(&manifest);
        let mut op = IncomingVersionOperation::new(
            trace_stream_id(),
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            manifest,
        );
        op.manifest_policy = Some(op.target_authorization_path(group_id));
        op.writer_policy = Some(op.target_authorization_path(group_id));
        op.first().unwrap();
        op.next(bucket_read(Some(
            make_bucket_info(group_id).to_bytes().unwrap().into(),
        )))
        .unwrap();
        op.next(event_result(Event::SubOperation(
            SubOperationEvent::GroupRoutingLoaded {
                result: Ok(GroupRoutingInputs::default()),
            },
        )))
        .unwrap();
        assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);
        op.next(row(Some(existing.to_bytes().unwrap().into())))
            .unwrap();
        let incoming = kind == ReplicationItemKind::DeleteMarker;
        assert_eq!(op.existing_marker, Some(existing.is_deleted()));
        assert_eq!(RowChange::written(op.existing_marker, incoming), expected);

        // The commit books the version and marker change of the replacement.
        op.destination_group_id = Some(group_id);
        let effects = op.start_commit_quota();
        // A replacement of the same kind may book nothing at all.
        let booked = match &effects {
            Ok(Next::Call(IoCall::AddUsage { buckets, .. })) => buckets.as_slice(),
            _ => &[],
        };
        let booked = booked
            .iter()
            .fold((0, 0), |(versions, markers), (_, delta)| {
                (versions + delta.versions, markers + delta.delete_markers)
            });
        let wanted = match expected {
            RowChange::VersionToMarker => (-1, 1),
            RowChange::MarkerToVersion => (1, -1),
            _ => (0, 0),
        };
        assert_eq!(booked, wanted, "{expected:?}: {effects:?}");
    }
}

/// An incoming operation that replaces the row of its version id, which was a marker or not.
fn replacing(
    kind: ReplicationItemKind,
    current: bool,
    was_marker: bool,
) -> IncomingVersionOperation {
    let mut manifest = make_manifest(kind);
    manifest.current_version = current;
    let mut op = IncomingVersionOperation::new(
        trace_stream_id(),
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.existing_marker = Some(was_marker);
    op.destination_group_id = Some(test_group_id());
    op.existing_blob_location = Some(make_location());
    op
}

fn head(version_id: Ulid, generation: u64) -> Vec<u8> {
    let pointer = CurrentVersionPointer::new_with_generation(version_id, generation);
    pointer.to_bytes().unwrap()
}

/// A historical item that turns the head's row into the other kind moves the object count,
/// though the head stays; a row the head does not name moves nothing.
#[test]
fn historical_replacement_liveness() {
    let other = Ulid::from_parts(9, 9);
    let cases = [
        (
            ReplicationItemKind::DeleteMarker,
            false,
            trace_version_id(),
            -1,
        ),
        (
            ReplicationItemKind::Materialized,
            true,
            trace_version_id(),
            1,
        ),
        (ReplicationItemKind::DeleteMarker, false, other, 0),
    ];
    for (kind, was_marker, named, delta) in cases {
        let mut op = replacing(kind, false, was_marker);
        let effects = op.write_object_lookup();
        let Ok(Next::Call(IoCall::Read(reads))) = &effects else {
            panic!("expected the head read, got {effects:?}");
        };
        assert_eq!(reads[0].0, BLOB_HEAD_KEYSPACE);
        assert_eq!(op.state, IncomingVersionState::ReadNamedHead);
        op.next(row(Some(head(named, 3).into()))).unwrap();
        assert_eq!(op.object_delta, delta);
    }
    // A row of the same kind changes no liveness, so the head is not read.
    let mut op = replacing(ReplicationItemKind::DeleteMarker, false, true);
    op.write_object_lookup().unwrap();
    assert_ne!(op.state, IncomingVersionState::ReadNamedHead);
    assert_eq!(op.object_delta, 0);
}

/// A current item stale against a newer head generation leaves the head, yet still moves the
/// object count when the head names the replaced row; a replay at the head's own generation
/// rewrites the head and books the change from the old row it reads.
#[test]
fn stalled_head_liveness() {
    let other = Ulid::from_parts(9, 9);
    let stale = [
        (
            ReplicationItemKind::DeleteMarker,
            false,
            trace_version_id(),
            -1,
        ),
        (
            ReplicationItemKind::Materialized,
            true,
            trace_version_id(),
            1,
        ),
        (ReplicationItemKind::DeleteMarker, false, other, 0),
        (
            ReplicationItemKind::DeleteMarker,
            true,
            trace_version_id(),
            0,
        ),
    ];
    for (kind, was_marker, named, delta) in stale {
        let mut op = replacing(kind, true, was_marker);
        op.write_compared_object(Some(&head(named, 5))).unwrap();
        assert!(op.pending_head.is_none());
        assert_eq!(op.object_delta, delta);
        assert_eq!(op.usage_delta().unwrap().objects, delta);
    }
    let replayed = [
        (ReplicationItemKind::DeleteMarker, false, -1),
        (ReplicationItemKind::Materialized, true, 1),
    ];
    for (kind, was_marker, delta) in replayed {
        let mut op = replacing(kind, true, was_marker);
        let effects = op.write_compared_object(Some(&head(trace_version_id(), 1)));
        assert!(op.pending_head.is_some());
        assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);
        assert!(matches!(&effects, Ok(Next::Call(IoCall::Read(_)))));
        let old = match was_marker {
            true => BlobVersion::deleted(fixed_created_at(), test_user_id()),
            false => BlobVersion::materialized(
                [3; 32],
                BackendRef::node_default(),
                EncodingClass::Raw,
                fixed_created_at(),
                test_user_id(),
                None,
            ),
        };
        op.next(row(Some(old.to_bytes().unwrap().into()))).unwrap();
        assert_eq!(op.object_delta, delta);
    }
}

/// Constructor-to-finalize trace of a delete marker: no blob is
/// transferred, the head is cleared and the apply acknowledgement closes
/// the stream.
#[test]
fn delete_marker_trace() {
    let stream_id = trace_stream_id();
    let group_id = test_group_id();
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_clock(fixed_trace_clock);
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));

    op.start();
    op.step(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )));
    op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
        result: Ok(GroupRoutingInputs::default()),
    }));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));

    op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::StartTransaction);
    op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    op.step(bucket_drift(&make_bucket_info(group_id)));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ApplyHeadTransition);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"head".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"version".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteLiveObligation);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"obligation".to_vec().into(),
    }));
    // The marker moves no group counter, but its bucket counts the new marker row.
    assert_eq!(op.state, IncomingVersionState::UpdateUsage);
    op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert_eq!(op.state, IncomingVersionState::CommitTransaction);

    op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert_eq!(op.state, IncomingVersionState::ScheduleUsage);
    op.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::PublishUsageSnapshots,
        after: Duration::from_secs(1),
    }));
    assert_eq!(op.state, IncomingVersionState::ScheduleLiveDrain);
    op.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainReplicationQueue,
        after: Duration::from_secs(1),
    }));
    assert_eq!(op.state, IncomingVersionState::SendApplyComplete);

    op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
    assert_eq!(op.state, IncomingVersionState::Finish);

    let result = op.finalize().expect("delete marker commits successfully");
    assert!(result.applied);
    assert_eq!(result.group_id, Some(group_id));
}

/// A refused negotiation runs constructor-to-finalize without entering the
/// apply at all and reports `applied: false`.
#[test]
fn rejected_trace() {
    let stream_id = trace_stream_id();
    let group_id = test_group_id();
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    )
    .with_clock(fixed_trace_clock);
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some("/other/path".to_string());

    op.start();
    op.step(bucket_read(Some(
        make_bucket_info(group_id).to_bytes().unwrap().into(),
    )));
    let effects = op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
        result: Ok(GroupRoutingInputs::default()),
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(&effects[0], "writer_access_denied");

    op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
    assert_eq!(op.state, IncomingVersionState::Finish);

    let result = op.finalize().expect("rejection is a clean result");
    assert!(!result.applied);
    assert_eq!(result.group_id, Some(group_id));
}

/// A long apply must stamp the reclaim candidate at the enqueue phase, not at
/// construction: a grace starting at operation construction could make the sweep
/// treat an already-old copy as immediately reclaimable.
#[test]
fn reclaim_uses_enqueue() {
    let started_at = trace_now();
    let enqueued_at = started_at + Duration::from_secs(6 * 60 * 60);
    set_controlled_clock(started_at);
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let version_id = manifest.version_id;
    let mut op = IncomingVersionOperation::new(
        trace_stream_id(),
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_clock(controlled_clock);
    op.txn_id = Some(trace_txn_id());
    op.destination_group_id = Some(test_group_id());
    op.replaced_version = Some(BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        fixed_created_at(),
        test_user_id(),
        None,
    ));

    op.read_replaced_metadata();
    let part_key = MultipartObjectKey::part(version_id, 3).to_bytes().unwrap();
    op.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(part_key.into(), vec![1u8].into())],
        next_start_after: None,
    }));

    // The blob transfer and the transactional apply ran for hours.
    set_controlled_clock(enqueued_at);
    let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteReclaimCandidate);
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected reclaim candidate write");
    };
    assert_eq!(key_space, BLOB_RECLAIM_KEYSPACE);
    let candidate =
        aruna_core::structs::storage::cleanup::ReclaimCandidate::from_bytes(value.as_ref())
            .unwrap();
    assert_eq!(candidate.enqueued_at, enqueued_at);
    assert_ne!(candidate.enqueued_at, started_at);
}

// Finalization before a terminal state is an explicit error. A fresh receiver
// and every in-flight negotiation, transfer, and commit phase must not report
// the applied default.
#[test]
fn rejects_early_finalize() {
    for state in [
        IncomingVersionState::Init,
        IncomingVersionState::SendNegotiation,
        IncomingVersionState::ReceiveBlob,
        IncomingVersionState::CommitTransaction,
        IncomingVersionState::ReleaseReservation,
        IncomingVersionState::CloseConnection,
    ] {
        let mut op = IncomingVersionOperation::new(
            trace_stream_id(),
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        )
        .with_clock(fixed_trace_clock);
        op.state = state.clone();
        assert_eq!(
            op.finalize(),
            Err(IncomingVersionError::NotFinished),
            "{state:?} must reject finalization"
        );
    }
}

// A failure recorded through the operation's own failure path reaches
// finalization as the operation error, not as a successful default.
#[test]
fn failure_survives_finalize() {
    let mut op = IncomingVersionOperation::new(
        trace_stream_id(),
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    )
    .with_clock(fixed_trace_clock);
    op.fail(IncomingVersionError::RealmMismatch);
    assert_eq!(op.finalize(), Err(IncomingVersionError::RealmMismatch));
}

#[test]
fn stale_encoding_rejects() {
    // The bucket switched to zstd after this raw replica was stored: publishing it
    // would leave a copy a finished migration never revisits.
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(91, 91),
        iroh::SecretKey::from_bytes(&[91; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.state = IncomingVersionState::CheckDrift;
    op.txn_id = Some(Ulid::from_parts(92, 92));
    op.existing_blob_location = Some(make_location());
    let mut bucket = make_bucket_info(Ulid::from_parts(93, 93));
    bucket.compression = aruna_core::structs::storage::format::Compression::Zstd { level: 3 };

    op.step(bucket_drift(&bucket));

    assert_eq!(op.state, IncomingVersionState::Error);
    assert!(matches!(
        op.output,
        Some(Err(IncomingVersionError::StorageError(
            aruna_core::errors::StorageError::TransactionConflict
        )))
    ));
}

/// An encrypting destination with its active key, and the plan it captures from both.
fn encrypting_target() -> (
    aruna_core::structs::storage::encryption::BucketEncryption,
    aruna_core::structs::storage::encryption::BucketKeyRecord,
    aruna_core::structs::storage::encryption::SealPlan,
) {
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRecord, BucketKeyRef, EncryptionMode, SealPlan,
    };
    let key = BucketKeyRef::new(Ulid::from_parts(95, 95), 1);
    let settings = BucketEncryption {
        mode: EncryptionMode::NodeManaged,
        bucket_id: Some(key.bucket_id),
        key_generation: 1,
        storage_generation: 3,
        ..Default::default()
    };
    let record = BucketKeyRecord::new(key, Ulid::from_parts(96, 96), [4; 32], 0);
    let plan = SealPlan::capture(&settings, &record).unwrap().unwrap();
    (settings, record, plan)
}

fn destination_rows(group_id: Ulid, settings: &[u8]) -> Event {
    let bucket = make_bucket_info(group_id).to_bytes().unwrap();
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (b"bucket".to_vec().into(), Some(bucket.into())),
            (b"bucket".to_vec().into(), Some(settings.to_vec().into())),
        ],
    })
}

/// An encrypting target that has read its plan and now reads its ABE parameters.
fn reading_abe() -> IncomingVersionOperation {
    use aruna_core::keyspaces::{ABE_PARAMETERS_KEYSPACE, BUCKET_KEY_KEYSPACE};
    let (settings, record, _) = encrypting_target();
    let group_id = test_group_id();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(94, 94),
        iroh::SecretKey::from_bytes(&[94; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));
    op.start();
    let effects = op.step(destination_rows(group_id, &settings.to_bytes().unwrap()));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == BUCKET_KEY_KEYSPACE
    ));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: Some(record.to_bytes().unwrap().into()),
    }));
    load_routing(&mut op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: None,
    }));
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::new().into(),
        value: None,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, txn_id: None })]
            if reads[0].0 == ABE_PARAMETERS_KEYSPACE
    ));
    op
}

fn abe_rows(parameters: Option<&AbeParameters>, epoch: u64) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                vec![0u8].into(),
                parameters.map(|p| p.to_bytes().unwrap().into()),
            ),
            (vec![1u8].into(), Some(epoch.to_be_bytes().to_vec().into())),
        ],
    })
}

fn target_parameters() -> AbeParameters {
    let (_, _, plan) = encrypting_target();
    let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
    let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
    create_parameters(&secret, test_realm_id(), node, plan.key).unwrap()
}

#[test]
fn encrypting_target_negotiates() {
    // Without ABE parameters the plan alone rides on the reply, and no plain or foreign copy is
    // probed or adopted.
    let (_, _, plan) = encrypting_target();
    let mut op = reading_abe();
    let effects = op.step(abe_rows(None, 1));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert_eq!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedSealedBlob(plan)
        )
    );
    let effects = op.step(Event::Blob(BlobEvent::MessageSent {
        stream_id: Ulid::from_parts(94, 94),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::HandleReplication { resolved, object: None, .. })]
            if resolved.encryption == Some(plan)
    ));
    assert_eq!(op.envelope, None);
}

#[test]
fn abe_target_negotiates() {
    // Admitted parameters give the replica its own object key and envelope before the transfer,
    // and the reply carries that object key with the plan.
    let (_, _, plan) = encrypting_target();
    let parameters = target_parameters();
    let mut op = reading_abe();
    let effects = op.step(abe_rows(Some(&parameters), 2));
    let [Effect::Blob(BlobEffect::Abe(effect))] = effects.as_slice() else {
        panic!("expected the envelope effect, got {effects:?}")
    };
    let AbeEffect::Envelope(envelope_plan) = effect.as_ref() else {
        panic!("expected an envelope plan, got {effect:?}")
    };
    assert_eq!(envelope_plan.epoch, 2);
    assert_eq!(envelope_plan.bucket_public, plan.public_key);
    assert_eq!(envelope_plan.object_key, op.manifest.key);
    let (envelope, _) = create_envelope(envelope_plan.clone()).unwrap();
    let object = envelope.context.public_key;
    let event = Box::new(AbeEvent::Envelope(envelope));
    let effects = op.step(Event::Blob(BlobEvent::Abe(event)));

    assert_eq!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedAbeBlob(plan, object)
        )
    );
    let effects = op.step(Event::Blob(BlobEvent::MessageSent {
        stream_id: Ulid::from_parts(94, 94),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::HandleReplication { object: Some(sent), .. })]
            if *sent == object
    ));
}

#[test]
fn target_refuses_references() {
    // A reference would serve bytes the destination never sealed.
    let (settings, _, _) = encrypting_target();
    let group_id = test_group_id();
    let mut manifest = make_manifest(ReplicationItemKind::Materialized);
    manifest.reference_intent = true;
    manifest.blob = None;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(94, 94),
        iroh::SecretKey::from_bytes(&[94; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.state = IncomingVersionState::ReadDestinationBucket;
    let effects = op.step(destination_rows(group_id, &settings.to_bytes().unwrap()));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        &IncomingVersionError::EncryptedDestination.to_string(),
    );
}

/// An operation inside its apply transaction, at the drift check of a sealed receipt.
fn sealed_drift(
    plan: Option<aruna_core::structs::storage::encryption::SealPlan>,
) -> IncomingVersionOperation {
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(96, 96),
        iroh::SecretKey::from_bytes(&[96; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.state = IncomingVersionState::CheckDrift;
    op.txn_id = Some(Ulid::from_parts(97, 97));
    op.seal_plan = plan;
    op.existing_blob_location = Some(make_location());
    op
}

fn drift_rows(settings: &[u8]) -> Event {
    let bucket = make_bucket_info(Ulid::from_parts(93, 93))
        .to_bytes()
        .unwrap();
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (vec![0u8; 4].into(), Some(bucket.into())),
            (vec![1u8; 4].into(), None),
            (vec![2u8; 4].into(), Some(settings.to_vec().into())),
        ],
    })
}

#[test]
fn stale_plan_retries() {
    // A plan captured at negotiation must still hold at publication; a stale one fails the item
    // with a reason the sender retries, and plain bytes never land in a bucket that now encrypts.
    use aruna_core::structs::storage::replication::{ReplicationFailure, ReplicationItemError};
    let (mut settings, _, plan) = encrypting_target();
    settings.storage_generation += 1;
    for mut op in [sealed_drift(Some(plan)), sealed_drift(None)] {
        op.step(drift_rows(&settings.to_bytes().unwrap()));
        assert_eq!(op.state, IncomingVersionState::Error);
        assert_eq!(op.output, Some(Err(IncomingVersionError::StalePlan)));
    }
    let reason = IncomingVersionError::StalePlan.to_string();
    assert_eq!(
        ReplicationItemError::from_peer_reason(&reason).failure,
        ReplicationFailure::Other
    );

    // The unchanged plan passes on to the replaced-version check.
    settings.storage_generation -= 1;
    let mut op = sealed_drift(Some(plan));
    let effects = op.step(drift_rows(&settings.to_bytes().unwrap()));
    assert_eq!(op.state, IncomingVersionState::VerifyReplaced);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == BLOB_VERSIONS_KEYSPACE
    ));
}

#[test]
fn received_archive_pends() {
    // An archive kept as the sender granted it is published pending with the claimed hash, and
    // writes no hash alias: only promotion with the target key may register it.
    use aruna_core::keyspaces::{
        COPY_OWNER_KEYSPACE, PENDING_CLAIM_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    };
    use aruna_core::structs::storage::format::PithosLayout;
    let (_, _, plan) = encrypting_target();
    let mut location = make_location();
    location.hashes.clear();
    let layout = PithosLayout {
        stored_size: 300,
        metadata_digest: [6; 32],
        storage_generation: plan.storage_generation,
    };
    location.format = StoredFormat::pithos(layout, plan.key);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(98, 98),
        iroh::SecretKey::from_bytes(&[98; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.seal_plan = Some(plan);
    op.destination_group_id = Some(test_group_id());
    op.txn_id = Some(Ulid::from_parts(99, 99));
    op.received_blob = Some(ReceivedBlob::reserved(location));

    let effects = op.write_hash_lookup();
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("the archive rows are written together: {effects:?}")
    };
    let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [
            COPY_OWNER_KEYSPACE,
            PENDING_LOCATION_KEYSPACE,
            PENDING_CLAIM_KEYSPACE
        ]
    );
    assert_eq!(writes[2].2.as_ref(), &[1u8; 32]);

    let mut version = None;
    let mut effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    for _ in 0..8 {
        let event = match effects.first() {
            Some(Effect::Storage(StorageEffect::Read { key, .. })) => StorageEvent::ReadResult {
                key: key.clone(),
                value: None,
            },
            Some(Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                ..
            })) => {
                assert_ne!(
                    key_space, PATHS_INDEX_KEYSPACE,
                    "no hash alias for a pending replica"
                );
                if key_space == BLOB_VERSIONS_KEYSPACE {
                    version = Some(BlobVersion::from_bytes(value).unwrap());
                }
                StorageEvent::WriteResult { key: key.clone() }
            }
            _ => break,
        };
        effects = op.step(Event::Storage(event));
    }
    assert!(matches!(
        version.map(|version| version.state),
        Some(BlobVersionState::PendingContent { .. })
    ));
}

fn target_envelope(epoch: u64) -> ObjectEnvelope {
    let (_, _, plan) = encrypting_target();
    let plan = EnvelopePlan {
        parameters: target_parameters(),
        epoch,
        write_id: Ulid::from_parts(90, 90),
        object_key: make_manifest(ReplicationItemKind::Materialized).key,
        bucket_public: plan.public_key,
    };
    create_envelope(plan).unwrap().0
}

#[test]
fn stale_epoch_refused() {
    // An epoch raised since the negotiation refuses the envelope; nothing of it was stored.
    let (settings, _, plan) = encrypting_target();
    let settings = settings.to_bytes().unwrap();
    let parameters = target_parameters();
    let with_abe = |epoch: u64| {
        let Event::Storage(StorageEvent::BatchReadResult { mut values }) = drift_rows(&settings)
        else {
            unreachable!()
        };
        let Event::Storage(StorageEvent::BatchReadResult { values: abe }) =
            abe_rows(Some(&parameters), epoch)
        else {
            unreachable!()
        };
        values.extend(abe);
        Event::Storage(StorageEvent::BatchReadResult { values })
    };
    let mut op = sealed_drift(Some(plan));
    op.envelope = Some(target_envelope(1));
    let effects = op.check_drift();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == 5
    ));
    let effects = op.step(with_abe(2));
    assert_eq!(op.output, Some(Err(IncomingVersionError::StalePlan)));
    assert!(effects.iter().all(|effect| !matches!(
        effect,
        Effect::Storage(StorageEffect::Write { .. } | StorageEffect::BatchWrite { .. })
    )));

    let mut op = sealed_drift(Some(plan));
    op.envelope = Some(target_envelope(1));
    op.step(with_abe(1));
    assert_eq!(op.state, IncomingVersionState::VerifyReplaced);
}

#[test]
fn replica_publishes_envelope() {
    // The envelope, its mappings and its charge are written in the version's transaction.
    use aruna_core::keyspaces::{
        ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    };
    use aruna_core::structs::storage::abe::EnvelopeArchive;
    use aruna_core::structs::storage::blob::ArchiveKey;
    use aruna_core::structs::storage::format::PithosLayout;
    let (_, _, plan) = encrypting_target();
    let mut location = make_location();
    location.hashes.clear();
    let layout = PithosLayout {
        stored_size: 300,
        metadata_digest: [6; 32],
        storage_generation: plan.storage_generation,
    };
    location.format = StoredFormat::pithos(layout, plan.key);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(98, 98),
        iroh::SecretKey::from_bytes(&[98; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    let envelope = target_envelope(1);
    op.seal_plan = Some(plan);
    op.envelope = Some(envelope.clone());
    op.destination_group_id = Some(test_group_id());
    op.txn_id = Some(Ulid::from_parts(99, 99));
    op.received_blob = Some(ReceivedBlob::reserved(location.clone()));

    let mut effects = op.write_blob_version();
    let mut writes = None;
    while let Some(effect) = effects.first() {
        if let Effect::Storage(StorageEffect::BatchWrite { writes: rows, .. }) = effect {
            writes = Some(rows.clone());
            break;
        }
        effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
    }
    let writes = writes.expect("the envelope rows are written");
    let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
    assert_eq!(
        spaces,
        [
            ABE_ENVELOPE_KEYSPACE,
            ABE_VERSION_KEYSPACE,
            ABE_ARCHIVE_KEYSPACE
        ]
    );
    assert_eq!(ObjectEnvelope::from_bytes(&writes[0].2).unwrap(), envelope);
    let archive: EnvelopeArchive = postcard::from_bytes(&writes[2].2).unwrap();
    assert_eq!(archive.archive, ArchiveKey::of(&location));
    assert!(archive.location_key.is_empty());
    assert!(op.envelope_bytes > 0);
    let delta = op.usage_delta().unwrap();
    let size = op.manifest.blob.as_ref().unwrap().size;
    assert_eq!(
        delta.logical_bytes,
        i128::from(size + op.envelope_bytes) - i128::from(op.replaced_logical_bytes)
    );
}

#[test]
fn replacement_drops_envelope() {
    // Replacing an ABE version deletes its old envelope rows and returns their charge.
    use aruna_core::keyspaces::{
        ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    };
    use aruna_core::structs::storage::abe::envelope_charge;
    use aruna_core::structs::storage::format::PithosLayout;
    let (_, _, plan) = encrypting_target();
    let mut location = make_location();
    location.hashes.clear();
    location.format = StoredFormat::pithos(
        PithosLayout {
            stored_size: 300,
            metadata_digest: [6; 32],
            storage_generation: plan.storage_generation,
        },
        plan.key,
    );
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(97, 97),
        iroh::SecretKey::from_bytes(&[97; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    let txn_id = Ulid::from_parts(99, 99);
    op.seal_plan = Some(plan);
    op.envelope = Some(target_envelope(1));
    op.destination_group_id = Some(test_group_id());
    op.txn_id = Some(txn_id);
    op.received_blob = Some(ReceivedBlob::reserved(location));
    let replaced = BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        EncodingClass::Raw,
        SystemTime::UNIX_EPOCH,
        test_user_id(),
        None,
    );
    op.replaced_version = Some(replaced.clone());
    op.replaced_logical_bytes = 42;
    op.state = IncomingVersionState::VerifyReplaced;

    let version = op.version_key_bytes().unwrap();
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: version.clone().into(),
        value: Some(replaced.to_bytes().unwrap().into()),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, txn_id: Some(id) })]
            if reads[0] == (ABE_VERSION_KEYSPACE.to_string(), version.clone().into())
                && *id == txn_id
    ));
    let old_id = vec![7u8; 16];
    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (version.clone().into(), Some(old_id.clone().into())),
            (b"bucket".to_vec().into(), None),
        ],
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads == &vec![
            (ABE_ENVELOPE_KEYSPACE.to_string(), old_id.clone().into()),
            (ABE_ARCHIVE_KEYSPACE.to_string(), old_id.clone().into()),
        ]
    ));
    op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (old_id.clone().into(), Some(vec![1; 40].into())),
            (old_id.clone().into(), Some(vec![2; 9].into())),
        ],
    }));
    assert_eq!(op.state, IncomingVersionState::ReadReplacedMetadata);
    let effects = op.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice() else {
        panic!("the replaced rows are deleted together: {effects:?}")
    };
    for row in [
        (ABE_VERSION_KEYSPACE.to_string(), version.clone().into()),
        (ABE_ENVELOPE_KEYSPACE.to_string(), old_id.clone().into()),
        (ABE_ARCHIVE_KEYSPACE.to_string(), old_id.into()),
    ] {
        assert!(deletes.contains(&row), "missing delete {row:?}");
    }

    // The new envelope maps the same version after the old rows are gone.
    let mut effects = op.write_blob_version();
    let writes = loop {
        match effects.first() {
            Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) => {
                break writes.clone();
            }
            Some(_) => {}
            None => panic!("the envelope rows are written"),
        }
        effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
    };
    let new_id = Ulid::from_parts(90, 90).to_bytes().to_vec();
    assert_eq!(writes[1].0, ABE_VERSION_KEYSPACE);
    assert_eq!(writes[1].1.as_ref(), version.as_slice());
    assert_eq!(writes[1].2.as_ref(), new_id.as_slice());
    let size = op.manifest.blob.as_ref().unwrap().size;
    let old = 42 + envelope_charge(&[1; 40], &[2; 9]);
    assert_eq!(
        op.usage_delta().unwrap().logical_bytes,
        i128::from(size + op.envelope_bytes) - i128::from(old)
    );
}

#[test]
fn encrypted_replacement_quota() {
    // One payload byte more replaces a larger old envelope at the exact ceiling.
    let (_, _, plan) = encrypting_target();
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(98, 98),
        iroh::SecretKey::from_bytes(&[98; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.seal_plan = Some(plan);
    op.destination_group_id = Some(test_group_id());
    op.state = IncomingVersionState::ReadReplacedBlob;
    let mut old = make_location();
    old.blob_size -= 1;
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(old.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);

    let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
    config.quota = QuotaConfig {
        default_quota_bytes: Some(1),
        grace_factor_percent: 100,
        ..QuotaConfig::default()
    };
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(postcard::to_allocvec(&config).unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadAbe);
    assert_eq!(op.quota_ceiling, Some(1));

    // The transaction adds the old envelope charge, so the net charge needs no quota.
    let mut location = make_location();
    location.hashes.clear();
    location.format = StoredFormat::pithos(
        aruna_core::structs::storage::format::PithosLayout {
            stored_size: 300,
            metadata_digest: [6; 32],
            storage_generation: plan.storage_generation,
        },
        plan.key,
    );
    op.received_blob = Some(ReceivedBlob::reserved(location));
    op.txn_id = Some(Ulid::from_parts(99, 99));
    op.envelope_bytes = 10;
    op.replaced_logical_bytes += 11;
    op.start_commit_quota();
    assert_eq!(op.state, IncomingVersionState::UpdateUsage);
}

#[test]
fn mapped_refuses_replicas() {
    use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
    let rule = BackendImport {
        import_id: Ulid::from_parts(58, 58),
        group_id: test_group_id(),
        backend_id: Ulid::from_parts(59, 59),
        backend_prefix: "team-a-bucket/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: String::new(),
        schedule: None,
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        updated_at: std::time::SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: PLAIN_PATTERN.to_string(),
    };
    let inputs = GroupRoutingInputs {
        imports: vec![rule],
        ..Default::default()
    };
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(57, 57),
        iroh::SecretKey::from_bytes(&[96; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    let bucket_info = make_bucket_info(test_group_id());
    op.manifest_policy = Some(op.target_authorization_path(bucket_info.group_id));
    op.writer_policy = Some(op.target_authorization_path(bucket_info.group_id));
    op.start();
    op.step(bucket_read(Some(bucket_info.to_bytes().unwrap().into())));
    load_routing(&mut op, inputs);
    let mut effects = Default::default();
    for _ in 0..2 {
        effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
    }
    // The refusal comes before any copy is probed or any byte is invited.
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        "A bucket stored 1:1 in a provider bucket cannot receive replicas yet.",
    );
}

// A member receiving a pulled marker of a distributed bucket keeps its receipt in the apply.
#[test]
fn keeps_pull_receipt() {
    use aruna_core::keyspaces::SYNC_RECEIPT_KEYSPACE;
    use aruna_core::structs::storage::distribution::DistributionBinding;
    use aruna_core::structs::{ORIGIN_DEVICE_TAG, SYNC_VERSION_TAG, tagged_receipt};
    let device = iroh::SecretKey::from_bytes(&[9; 32]).public();
    let pulled = Ulid::from_bytes([2; 16]);
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.metadata = HashMap::from([
        (SYNC_VERSION_TAG.to_string(), pulled.to_string()),
        (ORIGIN_DEVICE_TAG.to_string(), device.to_string()),
    ]);
    let target = (manifest.bucket.clone(), manifest.key.clone());
    let version_id = manifest.version_id;
    let local = iroh::SecretKey::from_bytes(&[65; 32]).public();
    let mut operation =
        IncomingVersionOperation::new(trace_stream_id(), local, test_realm_id(), manifest);
    assert_eq!(
        operation.pull_receipt(),
        None,
        "an ordinary bucket keeps none"
    );
    operation.txn_id = Some(trace_txn_id());
    operation.destination_binding = Some(DistributionBinding {
        set_id: Ulid::from_bytes([3; 16]),
        member_id: Ulid::from_bytes([4; 16]),
        incarnation: 5,
        revision: Default::default(),
    });
    let key = tagged_receipt((&target.0, &target.1), &operation.manifest.metadata, true);
    assert_eq!(
        operation.pull_receipt(),
        Some(Effect::Storage(StorageEffect::Write {
            key_space: SYNC_RECEIPT_KEYSPACE.to_string(),
            key: key.unwrap().into(),
            value: version_id.to_bytes().to_vec().into(),
            txn_id: Some(trace_txn_id()),
        }))
    );
}
