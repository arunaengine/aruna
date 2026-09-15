use super::{IncomingVersionError, IncomingVersionOperation, IncomingVersionState, ReceivedBlob};

use crate::replication::protocol::{
    MAX_VALUE_BYTES, MaterializedBlobInfo, ReferenceAdvance, SyncOrigin,
    VersionReplicationManifest, VersionReplicationMessage,
};
use crate::replication::queue::LiveObligationRecord;
use crate::s3::purge_fence::PurgeFenceError;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    OBJECT_METADATA_KEYSPACE, PATHS_INDEX_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
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
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingTarget, StorageRoutingRule,
};
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::{NodeId, UserId};
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, SystemTime};
use ulid::Ulid;

fn test_realm_id() -> RealmId {
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

fn trace_txn_id() -> Ulid {
    Ulid::from_bytes([0x52; 16])
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
        compressed: false,
        encrypted: false,
        created_by: test_user_id(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 42,
        hashes,
    }
}

fn make_bucket_info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test_user_id(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    }
}

pub(super) fn make_manifest(kind: ReplicationItemKind) -> VersionReplicationManifest {
    let blob = match kind {
        ReplicationItemKind::Materialized => {
            let location = make_location();
            Some(MaterializedBlobInfo {
                hash: [1u8; 32],
                size: location.blob_size,
                compressed: location.compressed,
                encrypted: location.encrypted,
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
        },
        connector_id: Some(Ulid::from_bytes([0x22; 16])),
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
fn load_routing(
    op: &mut IncomingVersionOperation,
    inputs: GroupRoutingInputs,
) -> aruna_core::types::Effects {
    assert_eq!(op.state, IncomingVersionState::LoadDestinationRouting);
    op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
        result: Ok(inputs),
    }))
}

fn advance_version_lookup(op: &mut IncomingVersionOperation, group_id: Ulid) -> Effect {
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));
    let effects = op.start();
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::Read { .. })
    ));

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
    let mut effects = load_routing(op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));
    effects.remove(0)
}

fn advance_blob_lookup(op: &mut IncomingVersionOperation) -> aruna_core::types::Effects {
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_LOCATIONS_KEYSPACE
    ));
    effects
}

/// The apply transaction's drift re-check answered with an absent bucket
/// and an absent subject, which an ungoverned replica passes.
fn no_drift() -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(vec![0u8; 4].into(), None), (vec![1u8; 4].into(), None)],
    })
}

/// The drift re-check echoing the bucket the negotiation read, which a
/// trace that really read a bucket must answer.
fn bucket_drift(bucket_info: &BucketInfo) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                vec![0u8; 4].into(),
                Some(bucket_info.to_bytes().unwrap().into()),
            ),
            (vec![1u8; 4].into(), None),
        ],
    })
}

fn start_apply_transaction(op: &mut IncomingVersionOperation) -> Ulid {
    start_apply_with(op, None)
}

/// `bucket` must echo what negotiation stored, or None when no bucket was
/// read; the drift re-check compares the two.
fn start_apply_with(
    op: &mut IncomingVersionOperation,
    bucket: Option<aruna_core::types::Value>,
) -> Ulid {
    let txn_id = Ulid::from_parts(1, 1);
    op.state = IncomingVersionState::StartTransaction;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(2, 2));

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CheckPurgeFence);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { txn_id: read_txn_id, .. })]
            if *read_txn_id == Some(txn_id)
    ));
    let _effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::CheckDrift);
    // The apply transaction re-reads the destination default and the local
    // subject before it exposes anything.
    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(vec![0u8; 4].into(), bucket), (vec![1u8; 4].into(), None)],
    }));
    assert_eq!(op.state, IncomingVersionState::VerifyReplaced);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE && *read_txn_id == Some(txn_id)
    ));
    let value = op
        .replaced_version
        .as_ref()
        .map(|version| version.to_bytes().unwrap().into());
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
            if key_space == BLOB_HEAD_KEYSPACE && *read_txn_id == Some(txn_id)
    ));
    txn_id
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
    op.state = IncomingVersionState::StartTransaction;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(4, 4));
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_parts(5, 5),
    }));
    let fence = StoragePurgeFence {
        job_id: JobId::from_bytes([12; 16]),
        scope: StoragePurgeScope::File {
            bucket: manifest.bucket,
            key: manifest.key,
        },
    };

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(fence.to_bytes().unwrap().into()),
    }));

    assert!(matches!(
        op.output,
        Some(Err(IncomingVersionError::PurgeFence(
            PurgeFenceError::Suspended
        )))
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
        manifest.created_at,
        manifest.created_by,
        None,
    );
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(version.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        message_from_effect(&effects[0]),
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
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            BlobVersion::deleted(manifest.created_at, manifest.created_by)
                .to_bytes()
                .unwrap()
                .into(),
        ),
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
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
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

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
    op.txn_id = Some(Ulid::from_parts(11, 11));

    let effects = op.write_blob_version();
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected reference version write")
    };
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
    op.txn_id = Some(Ulid::from_parts(13, 13));

    let effects = op.write_blob_version();
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected reference version write")
    };
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(version.published_by, Some(publisher));
    assert_eq!(version.created_by, forged);
}

#[test]
fn valid_advance() {
    let (manifest, previous, publisher) = advance_fixture();
    let advance = manifest.reference_advance.unwrap();
    let version_id = manifest.version_id;
    assert!(manifest.writer_auth_context.is_none());
    let mut op = advance_operation(manifest, publisher);

    advance_version_lookup(&mut op, test_group_id());
    let existing = op.reference_version().unwrap();
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing.to_bytes().unwrap().into()),
    }));
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
    let txn_id = start_apply_with(
        &mut op,
        Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
    );
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            CurrentVersionPointer::new_with_generation(advance.predecessor, advance.generation - 1)
                .to_bytes()
                .unwrap()
                .into(),
        ),
    }));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(previous.to_bytes().unwrap().into()),
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            value,
            txn_id: write_txn_id,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected head advance write")
    };
    assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(*write_txn_id, Some(txn_id));
    assert_eq!(
        CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(version_id, advance.generation)
    );
    assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    let [Effect::Storage(StorageEffect::Write { key_space, .. })] = effects.as_slice() else {
        panic!("expected successor version write")
    };
    assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);
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

    op.start();
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: None,
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(!op.create_attempted);
    expect_rejected_negotiation(
        &effects[0],
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
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: pointer.map(|pointer| pointer.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            op.output,
            Some(Err(IncomingVersionError::InvalidReferenceAdvance))
        ));
    }

    let mut op = advance_operation(manifest, publisher);
    start_apply_transaction(&mut op);
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            CurrentVersionPointer::new_with_generation(advance.predecessor, advance.generation - 1)
                .to_bytes()
                .unwrap()
                .into(),
        ),
    }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert!(matches!(
        op.output,
        Some(Err(IncomingVersionError::InvalidReferenceAdvance))
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
    advance_version_lookup(&mut op, test_group_id());

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(collision.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
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
    advance_version_lookup(&mut op, test_group_id());

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(duplicate.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
    let txn_id = start_apply_with(
        &mut op,
        Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
    );
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            CurrentVersionPointer::new_with_generation(
                Ulid::from_bytes([50u8; 16]),
                advance.generation + 1,
            )
            .to_bytes()
            .unwrap()
            .into(),
        ),
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            value,
            txn_id: write_txn_id,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected downstream obligation write")
    };
    assert_eq!(key_space, REPLICATION_OBLIGATION_KEYSPACE);
    assert_eq!(*write_txn_id, Some(txn_id));
    let obligation = LiveObligationRecord::from_bytes(value).unwrap();
    assert_eq!(obligation.reference_advance, Some(advance));
    assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
            if *commit_txn == txn_id
    ));
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
    let txn_id = Ulid::from_parts(15, 15);
    op.txn_id = Some(txn_id);
    op.destination_group_id = Some(test_group_id());
    op.replaced_version = Some(BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1600000060),
        test_user_id(),
        None,
    ));

    let effects = op.read_replaced_metadata();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Iter { key_space, txn_id: effect_txn, .. })]
            if key_space == OBJECT_METADATA_KEYSPACE
                && *effect_txn == Some(txn_id)
    ));

    let part_key = MultipartObjectKey::part(version_id, 3).to_bytes().unwrap();
    let mut effects = op.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(part_key.clone().into(), vec![1u8].into())],
        next_start_after: None,
    }));
    let Effect::Storage(StorageEffect::BatchDelete {
        deletes,
        txn_id: effect_txn,
    }) = effects.remove(0)
    else {
        panic!("expected replacement metadata batch delete")
    };
    assert_eq!(effect_txn, Some(txn_id));
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

    let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: deletes,
    }));
    assert_eq!(op.state, IncomingVersionState::WriteReclaimCandidate);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_RECLAIM_KEYSPACE
    ));

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_HEAD_KEYSPACE
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
    op.txn_id = Some(Ulid::from_parts(17, 17));
    op.replaced_version = Some(BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1600000120),
        test_user_id(),
        None,
    ));

    assert_eq!(
        op.replaced_reclaim_key(),
        Some(ReclaimCandidateKey::new(
            BackendRef::node_default(),
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
    op.state = IncomingVersionState::StartTransaction;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.destination_group_id = Some(Ulid::from_parts(19, 19));

    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_parts(20, 20),
    }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    let effects = op.step(no_drift());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));
    let current = BlobVersion::materialized(
        [9u8; 32],
        BackendRef::node_default(),
        manifest.created_at,
        manifest.created_by,
        None,
    );
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(current.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        op.output,
        Some(Err(IncomingVersionError::StorageError(
            StorageError::TransactionConflict
        )))
    ));
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));
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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedVersionOnly
        )
    ));
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

    let effects = op.start();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
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

    let effects = op.start();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        "Failed to convert from str: replication manifest entry is too large",
    );
}

#[test]
fn rejects_user_realm() {
    let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    manifest.auth_context.user_id =
        UserId::local(Ulid::from_parts(25, 25), RealmId::from_bytes([8u8; 32]));
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(26, 26),
        iroh::SecretKey::from_bytes(&[77; 32]).public(),
        test_realm_id(),
        manifest,
    );

    let effects = op.start();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        IncomingVersionError::RealmMismatch.to_string().as_str(),
    );
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

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected live replication obligation write")
    };
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
    let txn_id = Ulid::from_bytes([47u8; 16]);
    let mut op = advance_operation(manifest, publisher);
    op.txn_id = Some(txn_id);

    let effects = op.write_live_obligation();

    let [
        Effect::Storage(StorageEffect::Write {
            value,
            txn_id: write_txn_id,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected live replication obligation write")
    };
    assert_eq!(*write_txn_id, Some(txn_id));
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
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

    let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
    config.quota = QuotaConfig {
        default_quota_bytes: Some(1),
        grace_factor_percent: 100,
        ..QuotaConfig::default()
    };
    let config_bytes = postcard::to_allocvec(&config).unwrap();
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(config_bytes.clone().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::StartQuotaCheck);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    ));

    let txn_id = Ulid::from_parts(30, 30);
    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::EnforceQuota);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { txn_id: read_txn_id, .. })]
            if *read_txn_id == Some(txn_id)
    ));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(config_bytes.into()),
    }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    let effects = op.step(Event::Storage(StorageEvent::IterResult {
        values: Vec::new(),
        next_start_after: None,
    }));
    assert_eq!(op.state, IncomingVersionState::FinishQuotaCheck);
    assert_eq!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id })
    );

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    match message_from_effect(&effects[0]) {
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::Rejected(reason),
        ) => assert_eq!(reason, "quota"),
        other => panic!("expected quota rejection, got {other:?}"),
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
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    match message_from_effect(&effects[0]) {
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
    advance_blob_lookup(&mut op);
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing.to_bytes().unwrap().into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
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
    let txn_id = start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::from_parts(35, 35), 20);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, txn_id: write_txn_id, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE && *write_txn_id == Some(txn_id)
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
    let txn_id = Ulid::from_parts(37, 37);
    op.state = IncomingVersionState::StartTransaction;
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);

    op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    let effects = op.step(no_drift());
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));

    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        &op.output,
        Some(Err(IncomingVersionError::MissingVersionGeneration))
    ));
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(vec![255, 255, 255].into()),
    }));

    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        &op.output,
        Some(Err(IncomingVersionError::ConversionError(_)))
    ));
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected blob version write")
    };
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
    op.txn_id = Some(Ulid::from_parts(42, 42));
    op.destination_group_id = Some(Ulid::from_parts(43, 43));
    op.existing_blob_location = Some(make_location());

    let effects = op.write_version();

    let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
        panic!("expected blob version write")
    };
    let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
    assert_eq!(version.source_binding(), Some(&source));
    assert_eq!(version.metadata, expected_metadata);
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
    op.txn_id = Some(Ulid::from_parts(46, 46));
    op.destination_group_id = Some(group_id);
    op.existing_blob_location = Some(make_location());

    let effects = op.write_version();
    let [Effect::Storage(StorageEffect::Write { key_space, .. })] = effects.as_slice() else {
        panic!("expected blob version write")
    };
    assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    let [Effect::Storage(StorageEffect::Write { key_space, key, .. })] = effects.as_slice() else {
        panic!("expected hash path index write")
    };
    assert_eq!(key_space, PATHS_INDEX_KEYSPACE);
    let index_key = HashIndex::from_bytes(key.as_ref()).unwrap();
    assert_eq!(index_key.blake3_hash, [1u8; 32]);
    assert_eq!(index_key.version_id, manifest.version_id);
    assert_eq!(index_key.group_id, group_id);
    assert_eq!(index_key.bucket, manifest.bucket);
    assert_eq!(index_key.key, manifest.key);

    // The replica registers its managed copy in the same transaction.
    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    let [
        Effect::Storage(StorageEffect::Write {
            key_space, value, ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected the managed-copy registration")
    };
    assert_eq!(key_space, aruna_core::keyspaces::MANAGED_COPY_KEYSPACE);
    assert_eq!(
        aruna_core::structs::storage::blob::ManagedCopyRecord::from_bytes(value.as_ref())
            .unwrap()
            .version,
        aruna_core::structs::storage::blob::VersionKey::new(
            &manifest.bucket,
            &manifest.key,
            manifest.version_id
        )
    );

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteLiveObligation);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == REPLICATION_OBLIGATION_KEYSPACE
    ));

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: vec![0u8; 4].into(),
    }));
    assert_eq!(op.state, IncomingVersionState::UpdateUsage);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchRead { .. })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(vec![0].into(), None), (vec![1].into(), None)],
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::BatchWrite { .. })]
    ));
    let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert_eq!(op.state, IncomingVersionState::CommitTransaction);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { .. })]
    ));
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
    let txn_id = start_apply_transaction(&mut op);
    let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 10);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            BlobVersion::materialized(
                [2u8; 32],
                BackendRef::node_default(),
                SystemTime::UNIX_EPOCH + Duration::from_secs(1600000180),
                test_user_id(),
                None,
            )
            .to_bytes()
            .unwrap()
            .into(),
        ),
    }));

    let [
        Effect::Storage(StorageEffect::Write {
            key_space,
            value,
            txn_id: write_txn_id,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected blob head write")
    };
    assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
    assert_eq!(*write_txn_id, Some(txn_id));
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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadCurrentVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
    ));

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(
            BlobVersion::deleted(
                SystemTime::UNIX_EPOCH + Duration::from_secs(1600000240),
                test_user_id(),
            )
            .to_bytes()
            .unwrap()
            .into(),
        ),
    }));

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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
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

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(existing_pointer.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { key_space, .. })]
            if key_space == BLOB_VERSIONS_KEYSPACE
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
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_LOCATIONS_KEYSPACE
    ));

    let mut mismatched_location = make_location();
    mismatched_location.blob_size += 1;
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: Some(mismatched_location.to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
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
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == BLOB_LOCATIONS_KEYSPACE
    ));
}

/// Drives an incoming materialized version to the existing-copy probe under
/// the given bucket rules and group inputs.
fn probe_backend(
    rules: Vec<StorageRoutingRule>,
    inputs: GroupRoutingInputs,
) -> (IncomingVersionOperation, aruna_core::types::Effects) {
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

    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(bucket_info.to_bytes().unwrap().into()),
    }));
    load_routing(&mut op, inputs);
    let effects = advance_blob_lookup(&mut op);
    (op, effects)
}

fn group_backend_key(backend_id: Ulid) -> Vec<u8> {
    BlobLocationKey::new([1u8; 32], BackendRef::Group(backend_id)).to_bytes()
}

fn probed_key(effects: &aruna_core::types::Effects) -> Vec<u8> {
    let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
        panic!("expected one location read, got {effects:?}")
    };
    key.to_vec()
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
    let txn_id = Ulid::from_parts(59, 59);
    op.txn_id = Some(txn_id);
    op.destination_group_id = Some(test_group_id());
    op.existing_blob_location = Some(make_location());

    let effects = op.begin_blob_location();
    assert_eq!(op.state, IncomingVersionState::VerifyExistingBlob);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn, .. })]
            if key_space == BLOB_LOCATIONS_KEYSPACE && *read_txn == Some(txn_id)
    ));

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));

    assert_eq!(
        op.output,
        Some(Err(IncomingVersionError::ExistingBlobChanged))
    );
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
        },
    );
    let mut location = make_location();
    location.backend = BackendRef::Group(backend_id);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: group_backend_key(backend_id).into(),
        value: Some(location.to_bytes().unwrap().into()),
    }));

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

    let effects = op.step(Event::Blob(BlobEvent::ReplicationFinished {
        location: mismatched_location.clone(),
    }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CleanupReceivedBlob);
    assert_eq!(
        effects[0],
        Effect::Blob(BlobEffect::Delete {
            location: mismatched_location
        })
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

    let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
        location: received.clone(),
        message: "marker write failed".to_string(),
    })));
    assert_eq!(
        op.received_blob.as_ref().map(|blob| blob.location.clone()),
        Some(received.clone())
    );
    assert!(op.received_blob.as_ref().unwrap().cleanup_on_abort);
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CleanupReceivedBlob);
    assert_eq!(
        effects[0],
        Effect::Blob(BlobEffect::Delete { location: received })
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

    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::CreateDestinationBucket);
    op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
        result: Err("boom".to_string()),
    }));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        IncomingVersionError::DestinationNotFound
            .to_string()
            .as_str(),
    );

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        effects[0],
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

    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
        IncomingVersionError::WriterPermissionDenied
            .to_string()
            .as_str(),
    );

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        effects[0],
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

    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
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
    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
    let effects = load_routing(&mut op, GroupRoutingInputs::default());
    expect_rejected_negotiation(
        &effects[0],
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

    let effects = op.start();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
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

    let effects = op.start();

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    expect_rejected_negotiation(
        &effects[0],
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
    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));

    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
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
    assert!(matches!(
        effects[0],
        Effect::Storage(StorageEffect::Read { .. })
    ));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedBlobVersion
        )
    ));
}

#[test]
fn failure_rejects_first() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let stream_id = Ulid::from_parts(78, 78);
    let txn_id = Ulid::from_parts(79, 79);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[109; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );

    op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
    op.state = IncomingVersionState::ApplyHeadTransition;
    op.txn_id = Some(txn_id);

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::AbortTransaction);
    assert_eq!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id })
    );

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        effects[0],
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
}

#[test]
fn failure_deletes_blobs() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let stream_id = Ulid::from_parts(80, 80);
    let received = make_location();
    let txn_id = Ulid::from_parts(81, 81);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[110; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.negotiation_result = Some(
        aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedBlobVersion,
    );
    op.state = IncomingVersionState::WriteBlobLocation;
    op.txn_id = Some(txn_id);
    op.received_blob = Some(ReceivedBlob::reserved(received.clone()));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::AbortTransaction);
    assert_eq!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id })
    );

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CleanupReceivedBlob);
    assert_eq!(
        effects[0],
        Effect::Blob(BlobEffect::Delete { location: received })
    );

    let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        effects[0],
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
}

#[test]
fn unknown_commit_preserves() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let received = make_location();
    let txn_id = Ulid::from_parts(82, 82);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(83, 83),
        iroh::SecretKey::from_bytes(&[111; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobVersion);
    op.txn_id = Some(txn_id);
    op.received_blob = Some(ReceivedBlob::reserved(received.clone()));
    let release_id = received.ulid;

    let effects = op.commit_or_cleanup();
    let [
        Effect::Storage(StorageEffect::Write {
            key,
            value,
            txn_id: write_txn,
            ..
        }),
    ] = effects.as_slice()
    else {
        panic!("expected transactional reconciliation row, got {effects:?}")
    };
    assert_eq!(*write_txn, Some(txn_id));
    assert_eq!(key.as_ref(), received.ulid.to_bytes().as_slice());
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

    let effects = op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"cleanup".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::CommitTransaction);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { txn_id: id })] if *id == txn_id
    ));
    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::CommitFailed,
    }));
    assert_eq!(op.state, IncomingVersionState::ReleaseReservation);
    assert_eq!(op.txn_id, None);
    assert!(!op.received_blob.as_ref().unwrap().cleanup_on_abort);
    assert_eq!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation {
            id: release_id
        })]
    );
    let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
        id: release_id,
    }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));
    assert!(
        !effects
            .iter()
            .any(|effect| { matches!(effect, Effect::Storage(StorageEffect::Write { .. })) })
    );
    let effects = op.abort();
    assert!(
        !effects
            .iter()
            .any(|effect| { matches!(effect, Effect::Blob(BlobEffect::Delete { .. })) })
    );
}

#[test]
fn release_after_commit() {
    let received = make_location();
    let id = received.ulid;
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(84, 84),
        iroh::SecretKey::from_bytes(&[112; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.state = IncomingVersionState::CommitTransaction;
    op.txn_id = Some(Ulid::from_parts(85, 85));
    op.received_blob = Some(ReceivedBlob::reserved(received));

    let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_parts(86, 86),
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::ReleaseReservation { id: observed })] if *observed == id
    ));
    assert_eq!(op.state, IncomingVersionState::ReleaseReservation);

    let effects = op.step(Event::Blob(BlobEvent::ReservationReleased { id }));
    assert_eq!(op.state, IncomingVersionState::ScheduleUsage);
    assert_eq!(effects.len(), 1);
}

#[test]
fn conflict_commit_deletes() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let stream_id = Ulid::from_parts(87, 87);
    let received = make_location();
    let txn_id = Ulid::from_parts(88, 88);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[113; 32]).public(),
        test_realm_id(),
        manifest,
    );
    op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobVersion);
    op.state = IncomingVersionState::CommitTransaction;
    op.txn_id = Some(txn_id);
    op.received_blob = Some(ReceivedBlob::reserved(received.clone()));

    let effects = op.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::AbortTransaction);
    assert_eq!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id })
    );
    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CleanupReceivedBlob);
    assert_eq!(
        effects[0],
        Effect::Blob(BlobEffect::Delete { location: received })
    );
}

#[test]
fn commit_abort_preserves() {
    let received = make_location();
    let txn_id = Ulid::from_parts(89, 89);
    let mut op = IncomingVersionOperation::new(
        Ulid::from_parts(90, 90),
        iroh::SecretKey::from_bytes(&[114; 32]).public(),
        test_realm_id(),
        make_manifest(ReplicationItemKind::Materialized),
    );
    op.state = IncomingVersionState::CommitTransaction;
    op.txn_id = Some(txn_id);
    op.received_blob = Some(ReceivedBlob::reserved(received));

    let effects = op.abort();
    assert!(
        !effects
            .iter()
            .any(|effect| { matches!(effect, Effect::Blob(BlobEffect::Delete { .. })) })
    );
    assert!(effects.contains(&Effect::Storage(StorageEffect::AbortTransaction { txn_id })));
}

#[test]
fn failure_without_delete() {
    let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
    let stream_id = Ulid::from_parts(91, 91);
    let txn_id = Ulid::from_parts(92, 92);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[115; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.negotiation_result = Some(
        aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedVersionOnly,
    );
    op.state = IncomingVersionState::ApplyHeadTransition;
    op.txn_id = Some(txn_id);

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::SendApplyRejected);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionApplyRejected(_)
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::AbortTransaction);
    assert_eq!(
        effects[0],
        Effect::Storage(StorageEffect::AbortTransaction { txn_id })
    );

    let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);
    assert!(matches!(
        effects[0],
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
}

#[test]
fn commit_preserves_blob() {
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let stream_id = Ulid::from_parts(93, 93);
    let received = make_location();
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[116; 32]).public(),
        RealmId::from_bytes([7u8; 32]),
        manifest,
    );
    op.negotiation_result = Some(
        aruna_core::structs::storage::replication::ReplicationNegotiationResult::NeedBlobVersion,
    );
    op.state = IncomingVersionState::RegisterBlobDht;
    op.received_blob = Some(ReceivedBlob::owned(received));
    op.apply_committed = true;

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_parts(94, 94),
    }));
    assert_eq!(op.state, IncomingVersionState::Error);
    assert_eq!(effects.len(), 1);
    assert!(matches!(
        effects[0],
        Effect::Blob(BlobEffect::CloseConnection { .. })
    ));
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
    op.start();
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: None,
    }));
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
    let effects = op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
        result: Ok(()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { key_space, .. })]
            if key_space == S3_BUCKET_KEYSPACE
    ));
}

#[test]
fn create_invalid_event() {
    let mut op = missing_bucket_op();
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: Ulid::from_parts(96, 96),
    }));
    assert_eq!(op.state, IncomingVersionState::Error);
}

/// Constructor-to-finalize trace of a materialized replica: every real
/// transition runs, from the bucket probe through the blob transfer, the
/// apply transaction and the committed apply acknowledgement.
#[test]
fn materialized_trace() {
    let stream_id = trace_stream_id();
    let txn_id = trace_txn_id();
    let group_id = test_group_id();
    let received = make_location();
    let manifest = make_manifest(ReplicationItemKind::Materialized);
    let mut op = IncomingVersionOperation::new(
        stream_id,
        iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
        test_realm_id(),
        manifest,
    )
    .with_publisher_node(iroh::SecretKey::from_bytes(&[0x31; 32]).public())
    .with_clock(fixed_trace_clock);
    op.manifest_policy = Some(op.target_authorization_path(group_id));
    op.writer_policy = Some(op.target_authorization_path(group_id));

    let effects = op.start();
    assert_eq!(op.state, IncomingVersionState::ReadDestinationBucket);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
    assert_eq!(op.state, IncomingVersionState::LoadDestinationRouting);
    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

    op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
        result: Ok(GroupRoutingInputs::default()),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadExistingVersion);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadQuotaConfig);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ReadExistingBlob);

    let effects = op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::SendNegotiation);
    assert!(matches!(
        message_from_effect(&effects[0]),
        VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedBlobVersion
        )
    ));

    let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::ReceiveBlob);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Blob(BlobEffect::HandleReplication { stream_id: id, .. })] if *id == stream_id
    ));

    let effects = op.step(Event::Blob(BlobEvent::ReplicationFinished {
        location: received.clone(),
    }));
    assert_eq!(op.state, IncomingVersionState::StartTransaction);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));

    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    assert_eq!(op.state, IncomingVersionState::CheckPurgeFence);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { txn_id: Some(id), .. })] if *id == txn_id
    ));

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::CheckDrift);

    op.step(bucket_drift(&make_bucket_info(group_id)));
    assert_eq!(op.state, IncomingVersionState::VerifyReplaced);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobLocation);
    assert_eq!(op.received_blob.as_ref().unwrap().location, received);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"location".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::ReadObjectLookup);

    op.step(Event::Storage(StorageEvent::ReadResult {
        key: vec![0u8; 4].into(),
        value: None,
    }));
    assert_eq!(op.state, IncomingVersionState::ApplyHeadTransition);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"head-index".to_vec().into(),
    }));
    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"version".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteBlobVersion);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"index".to_vec().into(),
    }));
    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"copy".to_vec().into(),
    }));
    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"obligation".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteLiveObligation);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"obligation".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::UpdateUsage);

    op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(vec![0].into(), None), (vec![1].into(), None)],
    }));
    op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    assert_eq!(op.state, IncomingVersionState::WriteCleanupRow);

    op.step(Event::Storage(StorageEvent::WriteResult {
        key: b"cleanup".to_vec().into(),
    }));
    assert_eq!(op.state, IncomingVersionState::CommitTransaction);

    op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert_eq!(op.state, IncomingVersionState::ReleaseReservation);

    op.step(Event::Blob(BlobEvent::ReservationReleased {
        id: received.ulid,
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
    assert_eq!(op.state, IncomingVersionState::RegisterBlobDht);

    op.step(Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
        key: DhtKeyId::from_bytes([1u8; 32]),
        remote_attempt_count: 0,
        remote_store_count: 0,
    })));
    assert_eq!(op.state, IncomingVersionState::SendApplyComplete);

    op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
    assert_eq!(op.state, IncomingVersionState::CloseConnection);

    op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
    assert_eq!(op.state, IncomingVersionState::Finish);
    assert!(op.is_complete());

    let result = op.finalize().expect("trace commits successfully");
    assert!(result.applied);
    assert_eq!(result.group_id, Some(group_id));
}

/// Constructor-to-finalize trace of a delete marker: no blob is
/// transferred, the head is cleared and the apply acknowledgement closes
/// the stream.
#[test]
fn delete_marker_trace() {
    let stream_id = trace_stream_id();
    let txn_id = trace_txn_id();
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
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
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
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: b"bucket".to_vec().into(),
        value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
    }));
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
