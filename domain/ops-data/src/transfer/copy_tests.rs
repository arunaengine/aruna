//! Tests object copies: source conditions, references and the copied state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::get::GetObjectOperation;
use crate::testing_objects::put_config;
use crate::tests::policy::{seed_gate, subject};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::PortableSourceDescriptor;
use aruna_core::structs::storage::blob::{BlobHeadKey, BlobVersion, VersionKey};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication_queue::MAX_AUTO_ADVANCES;
use aruna_runner::driver::{DriverContext, drive, node_routing};
use aruna_runner::testing::seed_bucket;
use aruna_test_harness::s3::{full_context, spawn_reference_server, write_version};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn test_node_id() -> aruna_core::id::NodeId {
    iroh::SecretKey::from_bytes(&[7; 32]).public()
}

async fn copy_object(
    context: &DriverContext,
    input: CopyObjectInput,
) -> Result<CopyResultData, CopyObjectError> {
    drive(
        CopyObjectOperation::new(input, node_routing(context)),
        context,
    )
    .await
}

async fn copy_object_at(
    context: &DriverContext,
    input: CopyObjectInput,
    version_id: Ulid,
) -> Result<CopyResultData, CopyObjectError> {
    let copy = CopyObjectOperation::new(input, node_routing(context)).at_version(version_id);
    drive(copy, context).await
}

async fn copy_object_tracked(
    context: &DriverContext,
    input: CopyObjectInput,
    pulled: Option<Arc<AtomicU64>>,
) -> Result<CopyResultData, CopyObjectError> {
    let copy = CopyObjectOperation::new(input, node_routing(context));
    match pulled {
        Some(pulled) => drive(copy.with_progress(pulled), context).await,
        None => drive(copy, context).await,
    }
}

fn step_input(conditions: CopySourceConditions) -> CopyObjectInput {
    let realm_id = RealmId::from_bytes([1; 32]);
    let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
    CopyObjectInput {
        source_bucket: "source".to_string(),
        source_key: "a".to_string(),
        source_version_id: None,
        source_group_id: Ulid::from_bytes([3; 16]),
        source_auth_context: auth_context(user_id),
        dest_bucket: "dest".to_string(),
        dest_key: "b".to_string(),
        user_id,
        group_id: Ulid::from_bytes([3; 16]),
        realm_id,
        node_id: test_node_id(),
        quota_ceiling: None,
        conditions,
        metadata: None,
        restrictions: None,
        references: CopyReferences::Materialize,
        job_id: None,
    }
}

fn step_head() -> HeadObjectResult {
    HeadObjectResult {
        location: None,
        metadata: HashMap::new(),
        source_metadata: None,
        last_refresh: None,
        version_created_at: None,
        version_id: None,
        resolved_version_id: None,
        checksum_type: aruna_core::structs::storage::multipart::MultipartChecksumType::FullObject,
        composite_hashes: HashMap::new(),
        part_count: None,
        source_policies: Vec::new(),
        source_binding: None,
    }
}

fn step_copy(conditions: CopySourceConditions) -> CopyObjectOperation {
    CopyObjectOperation::new(step_input(conditions), NodeRouting::default())
}

fn child<T, E>(outcome: Result<T, E>) -> IoResult
where
    T: std::fmt::Debug + PartialEq + Send + 'static,
    E: std::fmt::Debug + PartialEq + Send + 'static,
{
    IoResult::Child(aruna_core::calls::ChildOutcome::new(outcome))
}

#[test]
fn failed_condition_stops() {
    let mut copy = step_copy(CopySourceConditions {
        if_match: Some("other".to_string()),
        ..Default::default()
    });
    assert!(matches!(copy.first(), Ok(Next::Call(IoCall::Child(_)))));
    let fenced = copy.next(child::<(), PurgeFenceError>(Ok(())));
    assert!(matches!(fenced, Ok(Next::Call(IoCall::Child(_)))));
    let answer = copy.next(child::<_, HeadObjectError>(Ok(step_head())));
    assert_eq!(answer, Err(CopyObjectError::PreconditionFailed));
}

#[test]
fn missing_source_fails() {
    let mut copy = step_copy(CopySourceConditions::default());
    copy.first().unwrap();
    copy.next(child::<(), PurgeFenceError>(Ok(()))).unwrap();
    let missing = child::<HeadObjectResult, _>(Err(HeadObjectError::NoSuchKey));
    let answer = copy.next(missing);
    assert_eq!(answer, Err(CopyObjectError::Get(GetObjectError::NoSuchKey)));
}

#[test]
fn committed_version_answers() {
    let version = Ulid::from_bytes([4; 16]);
    let mut copy = step_copy(CopySourceConditions::default()).at_version(version);
    copy.first().unwrap();
    let answer = copy.next(child::<_, HeadObjectError>(Ok(step_head())));
    let Ok(Next::Done(done)) = answer else {
        panic!("a committed version answers the copy");
    };
    assert_eq!(done.version_id, version);
    assert_eq!(done.source_version_id, None);
}

#[test]
fn absent_version_copies() {
    let version = Ulid::from_bytes([4; 16]);
    let mut copy = step_copy(CopySourceConditions::default()).at_version(version);
    copy.first().unwrap();
    let missing = child::<HeadObjectResult, _>(Err(HeadObjectError::NoSuchVersion));
    assert!(matches!(
        copy.next(missing),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(copy.step, CopyStep::Fence);
}

#[test]
fn unbound_reference_fails() {
    let input = step_input(CopySourceConditions::default());
    let stamp = CopyStamp {
        version: None,
        fresh: Ulid::from_bytes([5; 16]),
        now_ms: 1,
    };
    let mut keep = KeepReferenceOperation::new(input, step_head(), None, stamp);
    assert!(matches!(keep.first(), Ok(Next::Call(IoCall::Child(_)))));
    let answer = keep.next(child::<(), CopyObjectError>(Ok(())));
    assert_eq!(
        answer,
        Err(CopyObjectError::Get(GetObjectError::GetObjectFailed))
    );
}

#[test]
fn plain_source_admitted() {
    let mut admit = AdmitSourceOperation {
        bucket: "source".to_string(),
        admitting: false,
    };
    assert!(matches!(admit.first(), Ok(Next::Call(IoCall::Read(_)))));
    let answer = admit.next(IoResult::Values(vec![None]));
    assert_eq!(answer, Ok(Next::Done(())));
}

#[test]
fn locked_source_refused() {
    let settings = BucketEncryption {
        mode: aruna_core::structs::storage::encryption::EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::from_bytes([6; 16])),
        key_generation: 1,
        ..Default::default()
    };
    let mut admit = AdmitSourceOperation {
        bucket: "source".to_string(),
        admitting: false,
    };
    admit.first().unwrap();
    let row = settings.to_bytes().unwrap();
    let read = admit.next(IoResult::Values(vec![Some(row.into())]));
    assert!(matches!(read, Ok(Next::Call(IoCall::Effect(_)))));
    let error = aruna_core::structs::storage::encryption::BucketKeyError::InvalidToken;
    let event = Event::blob(BlobEvent::Error(BlobError::BucketKey(error)));
    let answer = admit.next(IoResult::Event(Box::new(event)));
    assert!(matches!(
        answer,
        Err(CopyObjectError::Get(GetObjectError::ConversionError(
            ConversionError::BucketKey(_)
        )))
    ));
}

fn auth_context(user_id: UserId) -> AuthContext {
    AuthContext::new(user_id, user_id.realm_id)
}

async fn read_dest_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
) -> BlobVersion {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing blob version entry");
    };
    BlobVersion::from_bytes(value.expect("missing blob version").as_ref()).unwrap()
}

/// A rule that admits exactly this node, so a governed write is allowed.
pub(crate) fn admits(
    node_id: NodeId,
    seed: u8,
) -> aruna_core::structs::placement::policy::VerifiedPolicy {
    let policy = aruna_core::structs::placement::policy::PlacementPolicy::new(
        Ulid::from_bytes([seed; 16]),
        "residency".to_string(),
        vec![aruna_core::structs::placement::policy::PlacementSelector {
            node_id: Some(node_id),
            location: None,
            labels: Vec::new(),
            executor_kind: None,
        }],
    )
    .expect("policy is valid");
    aruna_core::structs::placement::policy::VerifiedPolicy::verify(policy).expect("policy verifies")
}

#[tokio::test]
async fn copy_unions_refs() {
    // Union tightens: the copy carries both its source's and its
    // destination's refs.
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let source = admits(node_id, 1);
    let dest = admits(node_id, 2);
    let (source_ref, dest_ref) = (source.policy_ref(), dest.policy_ref());
    seed_gate(
        &context,
        realm_id,
        user_id,
        subject(node_id, "eu-west"),
        &[source.clone(), dest.clone()],
    )
    .await;
    seed_bucket(&context, "source", group_id, user_id, vec![source_ref]).await;
    seed_bucket(&context, "dest", group_id, user_id, vec![dest_ref]).await;
    let gate = drive(GateOperation::new(realm_id, 1_000), &context)
        .await
        .expect("subject reads")
        .expect("gate present");
    drive(
        PutObjectOperation::new(put_config(
            realm_id,
            group_id,
            node_id,
            "source",
            "object.txt",
            b"payload",
        ))
        .with_gate(gate),
        &context,
    )
    .await
    .unwrap();

    let result = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "source".to_string(),
            source_key: "object.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "dest".to_string(),
            dest_key: "copy.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap();

    let copied = read_dest_version(&context, "dest", "copy.txt", result.version_id).await;
    assert_eq!(copied.placement_policies, vec![source_ref, dest_ref]);
}

#[tokio::test]
async fn materialized_copy_deduplicates() {
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);

    let source = drive(
        PutObjectOperation::new(put_config(
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"hello, world!",
        )),
        &context,
    )
    .await
    .unwrap();

    let copy_started_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let result = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "source.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap();

    let source_version =
        read_dest_version(&context, "bucket", "source.txt", source.version_id).await;
    assert_eq!(result.location, Some(source.location));
    assert_eq!(result.source_version_id, Some(source.version_id));
    assert_eq!(result.source_last_modified, Some(source_version.created_at));
    // The copy dedups onto the source location, but its last-modified must
    // come from the fresh destination version, not the source's timestamp.
    assert_eq!(
        result.created_at,
        UNIX_EPOCH + Duration::from_millis(result.version_id.timestamp_ms())
    );
    assert!(result.version_id.timestamp_ms() >= copy_started_ms);

    let dest_version = read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
    assert!(dest_version.is_materialized());
    assert_eq!(dest_version.created_at, result.created_at);
}

#[tokio::test]
async fn given_version_replays() {
    // A retry with the minted version answers that version instead of writing another.
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let config = put_config(realm_id, group_id, node_id, "bucket", "a.txt", b"bytes");
    drive(PutObjectOperation::new(config), &context)
        .await
        .unwrap();
    let input = || CopyObjectInput {
        source_bucket: "bucket".to_string(),
        source_key: "a.txt".to_string(),
        source_version_id: None,
        source_group_id: group_id,
        source_auth_context: auth_context(user_id),
        dest_bucket: "bucket".to_string(),
        dest_key: "b.txt".to_string(),
        user_id,
        group_id,
        realm_id,
        node_id,
        quota_ceiling: None,
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: None,
        references: CopyReferences::Materialize,
        job_id: None,
    };
    let version = Ulid::generate();
    let first = copy_object_at(&context, input(), version).await.unwrap();
    let again = copy_object_at(&context, input(), version).await.unwrap();
    assert_eq!((first.version_id, again.version_id), (version, version));
    let head = HeadObjectInput {
        bucket: "bucket".to_string(),
        key: "b.txt".to_string(),
        version_id: None,
    };
    let head = drive(HeadObjectOperation::new(head), &context)
        .await
        .unwrap();
    assert_eq!(head.resolved_version_id, Some(version));
}

#[tokio::test]
async fn replays_without_source() {
    // The copy committed and its answer was lost; the source was deleted since, yet the
    // retry answers the committed version instead of failing on the missing source.
    use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let config = put_config(realm_id, group_id, node_id, "bucket", "a.txt", b"bytes");
    drive(PutObjectOperation::new(config), &context)
        .await
        .unwrap();
    let input = || CopyObjectInput {
        source_bucket: "bucket".to_string(),
        source_key: "a.txt".to_string(),
        source_version_id: None,
        source_group_id: group_id,
        source_auth_context: auth_context(user_id),
        dest_bucket: "bucket".to_string(),
        dest_key: "b.txt".to_string(),
        user_id,
        group_id,
        realm_id,
        node_id,
        quota_ceiling: None,
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: None,
        references: CopyReferences::Materialize,
        job_id: None,
    };
    let version = Ulid::generate();
    let first = copy_object_at(&context, input(), version).await;
    assert_eq!(first.unwrap().size, 5);
    let delete = DeleteObjectInput {
        bucket: "bucket".to_string(),
        key: "a.txt".to_string(),
        version_id: None,
        group_id,
        realm_id,
        node_id,
        deleted_by: user_id,
    };
    drive(DeleteObjectOperation::new(delete), &context)
        .await
        .unwrap();
    let fresh = copy_object_at(&context, input(), Ulid::generate()).await;
    assert!(fresh.is_err(), "a new copy of the deleted source must fail");
    let again = copy_object_at(&context, input(), version).await.unwrap();
    assert_eq!((again.version_id, again.size), (version, 5));
}

#[tokio::test]
async fn reference_copy_materializes() {
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);

    let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::generate()),
        mirrors: Vec::new(),
    };
    write_version(
        &context,
        "bucket",
        "ref.txt",
        BlobVersion::reference(
            source,
            SourceMetadata {
                content_length: 15,
                content_type: Some("text/plain".to_string()),
                etag: Some("etag-1".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
                source_version: None,
                digests: Default::default(),
            },
            SystemTime::UNIX_EPOCH,
            UserId::local(Ulid::generate(), realm_id),
            SystemTime::UNIX_EPOCH,
        ),
    )
    .await;

    let result = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "ref.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap();

    let dest_version = read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
    assert!(dest_version.is_materialized());
    assert_eq!(
        dest_version
            .source_binding()
            .map(|binding| binding.strategy.clone()),
        Some(StagingStrategy::Snapshot)
    );

    server.abort();
    let _ = server.await;

    let mut blob = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "bucket".to_string(),
            key: "dest.txt".to_string(),
            version_id: None,
            range: None,
            group_id,
            user_identity: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            node_id: test_node_id(),
        }),
        &context,
    )
    .await
    .unwrap()
    .blob;
    let mut read_buffer = Vec::new();
    while let Some(Ok(bytes)) = blob.next().await {
        read_buffer.extend_from_slice(&bytes);
    }
    assert_eq!(read_buffer, b"reference-bytes");
}

// A copy reading a capped reference must surface the exact variant, so the
// caller learns that only an explicit rebind heals it.
#[tokio::test]
async fn copy_preserves_reference() {
    // A kept reference is cloned from the source version alone: the
    // connector is never contacted, so the source may be unreachable.
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([6u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
    let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
    server.abort();
    let _ = server.await;
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    };
    write_version(
        &context,
        "bucket",
        "ref.txt",
        BlobVersion::reference(
            source.clone(),
            SourceMetadata {
                content_length: 15,
                content_type: Some("text/plain".to_string()),
                etag: Some("etag-1".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
                source_version: None,
                digests: Default::default(),
            },
            SystemTime::UNIX_EPOCH,
            user_id,
            SystemTime::UNIX_EPOCH,
        ),
    )
    .await;

    let result = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "ref.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Preserve,
            job_id: None,
        },
    )
    .await
    .unwrap();
    assert!(result.location.is_none());
    assert_eq!(result.size, 15);
    assert!(result.source_version_id.is_some());

    let dest_version = read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
    assert!(!dest_version.is_materialized());
    let binding = dest_version.source_binding().expect("a reference binding");
    assert_eq!(binding.strategy, StagingStrategy::Reference);
    assert_eq!(binding.descriptor, source.descriptor);
}

#[tokio::test]
async fn locked_reference_refused() {
    // A reference of a locked encrypting bucket is not preserved into another bucket, where
    // its external bytes would be readable without the source's key.
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::structs::execution::job::input::RoCrateLimits;
    use aruna_core::structs::storage::encryption::{
        BucketKeyError, EncryptionMode, TokenCredential,
    };
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([6u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    seed_bucket(&context, "locked", group_id, user_id, Vec::new()).await;
    seed_bucket(&context, "plain", group_id, user_id, Vec::new()).await;
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::generate()),
        key_generation: 1,
        ..Default::default()
    };
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: b"locked".to_vec().into(),
            value: settings.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([(
                "endpoint".to_string(),
                "http://127.0.0.1:9".to_string(),
            )]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
    };
    let metadata = SourceMetadata {
        content_length: 15,
        content_type: None,
        etag: Some("etag-1".to_string()),
        last_modified: Some(SystemTime::UNIX_EPOCH),
        source_version: None,
        digests: Default::default(),
    };
    let version = BlobVersion::reference(
        source,
        metadata,
        SystemTime::UNIX_EPOCH,
        user_id,
        SystemTime::UNIX_EPOCH,
    );
    write_version(&context, "locked", "ref.txt", version).await;
    let request = |dest_bucket: &str| CopyObjectInput {
        source_bucket: "locked".to_string(),
        source_key: "ref.txt".to_string(),
        source_version_id: None,
        source_group_id: group_id,
        source_auth_context: auth_context(user_id),
        dest_bucket: dest_bucket.to_string(),
        dest_key: "dest.txt".to_string(),
        user_id,
        group_id,
        realm_id,
        node_id,
        quota_ceiling: None,
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: None,
        references: CopyReferences::Preserve,
        job_id: None,
    };

    let refused = copy_object(&context, request("plain")).await;

    assert!(matches!(
        refused,
        Err(CopyObjectError::Get(GetObjectError::ConversionError(
            ConversionError::BucketKey(_)
        )))
    ));
    let head = BlobHeadKey::new("plain", "dest.txt").to_bytes().unwrap();
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: head.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event");
    };
    assert!(value.is_none(), "nothing is published in the plain bucket");
    // Within its own bucket the reference stays behind the same lock, so it is kept.
    copy_object(&context, request("locked")).await.unwrap();

    // A token opens single objects only, so it admits no reference copy of a locked bucket.
    let token = Some(TokenRead {
        credential: TokenCredential {
            access_key: "HOLDER".to_string(),
            token: SharedSecret::new(SecretBytes::new(vec![9; 32])),
        },
        limits: RoCrateLimits::default(),
    });
    let copy = CopyObjectOperation::new(request("plain"), node_routing(&context));
    let refused = drive(copy.with_token(token), &context).await;
    assert!(matches!(
        refused,
        Err(CopyObjectError::Get(GetObjectError::ConversionError(
            ConversionError::BucketKey(BucketKeyError::Locked(_))
        )))
    ));
}

/// Stores the realm and group documents, so `owner` holds the group's roles.
pub(crate) async fn seed_authority(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    owner: UserId,
) {
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    };
    let group = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: owner,
        realm_id,
    };
    for (key, value) in [
        (
            realm_id.as_bytes().to_vec(),
            realm.to_bytes(&actor).unwrap(),
        ),
        (
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        ),
    ] {
        let write = StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
    }
}

#[tokio::test]
async fn backend_copy_adopts() {
    // Bytes the destination's backend already holds are adopted without a
    // stream: nothing passes the progress counter and the location is shared.
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
    let data: &'static [u8] = b"adopt these bytes";
    let source = drive(
        PutObjectOperation::new(put_config(
            realm_id, group_id, node_id, "bucket", "src.txt", data,
        )),
        &context,
    )
    .await
    .unwrap();

    let pulled = Arc::new(AtomicU64::new(0));
    let result = copy_object_tracked(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "src.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
        Some(pulled.clone()),
    )
    .await
    .unwrap();
    assert_eq!(pulled.load(Ordering::Relaxed), 0, "no byte streamed");
    assert_eq!(result.location, Some(source.location.clone()));
    assert_eq!(result.size, data.len() as u64);
    let dest_version = read_dest_version(&context, "bucket", "dest.txt", result.version_id).await;
    assert!(dest_version.is_materialized());
}

#[tokio::test]
async fn copy_keeps_encodings() {
    // A destination that compresses never adopts the source's raw copy.
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    seed_bucket(&context, "bucket", group_id, user_id, Vec::new()).await;
    let packed = aruna_core::structs::storage::blob::BucketInfo {
        group_id,
        created_at: std::time::UNIX_EPOCH,
        created_by: user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 1,
        compression: Compression::Zstd { level: 3 },
        distribution: None,
    };
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: b"packed".to_vec().into(),
            value: packed.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let data: &'static [u8] = b"bytes that need their own copy";
    let source = drive(
        PutObjectOperation::new(put_config(
            realm_id, group_id, node_id, "bucket", "src.txt", data,
        )),
        &context,
    )
    .await
    .unwrap();

    let pulled = Arc::new(AtomicU64::new(0));
    let result = copy_object_tracked(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "src.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "packed".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
        Some(pulled.clone()),
    )
    .await
    .unwrap();

    assert_eq!(pulled.load(Ordering::Relaxed), data.len() as u64);
    let location = result.location.expect("a materialized copy");
    assert_ne!(location, source.location);
    assert_eq!(location.format.encoding(), EncodingClass::Zstd { level: 3 });
}

#[tokio::test]
async fn copy_reports_exhausted() {
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([6u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);

    let (endpoint, server) = spawn_reference_server(b"reference-bytes").await;
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::generate()),
        mirrors: Vec::new(),
    };
    write_version(
        &context,
        "bucket",
        "capped.txt",
        BlobVersion::reference(
            source,
            SourceMetadata {
                content_length: 1,
                content_type: Some("text/plain".to_string()),
                etag: Some("stale-etag".to_string()),
                last_modified: Some(SystemTime::UNIX_EPOCH),
                source_version: None,
                digests: Default::default(),
            },
            SystemTime::UNIX_EPOCH,
            UserId::local(Ulid::generate(), realm_id),
            SystemTime::UNIX_EPOCH,
        )
        .with_advance_count(MAX_AUTO_ADVANCES),
    )
    .await;

    let error = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "capped.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap_err();

    server.abort();
    let _ = server.await;
    assert_eq!(
        error,
        CopyObjectError::Get(GetObjectError::ReferenceAdvanceExhausted)
    );
}

#[tokio::test]
async fn delete_marker_errors() {
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);

    let version_id = Ulid::generate();
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("failed to start transaction");
    };
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("bucket", "gone.txt", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: BlobVersion::deleted(
                SystemTime::now(),
                UserId::local(Ulid::generate(), realm_id),
            )
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    let error = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "gone.txt".to_string(),
            source_version_id: Some(version_id),
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error, CopyObjectError::Get(GetObjectError::DeleteMarker));
}

#[test]
fn matching_etag_required() {
    let conditions = CopySourceConditions {
        if_match: Some("abc".to_string()),
        ..Default::default()
    };
    assert!(evaluate_source_conditions(&conditions, Some("abc"), None, true).is_ok());
    assert!(evaluate_source_conditions(&conditions, Some("\"abc\""), None, true).is_ok());
    assert_eq!(
        evaluate_source_conditions(&conditions, Some("def"), None, true),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert_eq!(
        evaluate_source_conditions(&conditions, None, None, true),
        Err(CopyObjectError::PreconditionFailed)
    );
}

#[test]
fn wildcard_uses_existence() {
    let if_match = CopySourceConditions {
        if_match: Some("*".to_string()),
        ..Default::default()
    };
    assert!(evaluate_source_conditions(&if_match, None, None, true).is_ok());
    assert_eq!(
        evaluate_source_conditions(&if_match, None, None, false),
        Err(CopyObjectError::PreconditionFailed)
    );

    let if_none_match = CopySourceConditions {
        if_none_match: Some("*".to_string()),
        ..Default::default()
    };
    assert_eq!(
        evaluate_source_conditions(&if_none_match, None, None, true),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert!(evaluate_source_conditions(&if_none_match, None, None, false).is_ok());
}

#[test]
fn matching_etag_rejected() {
    let conditions = CopySourceConditions {
        if_none_match: Some("abc".to_string()),
        ..Default::default()
    };
    assert_eq!(
        evaluate_source_conditions(&conditions, Some("abc"), None, true),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert!(evaluate_source_conditions(&conditions, Some("def"), None, true).is_ok());
}

#[test]
fn checks_unmodified_precision() {
    let threshold = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let conditions = CopySourceConditions {
        if_unmodified_since: Some(threshold),
        ..Default::default()
    };
    assert_eq!(
        evaluate_source_conditions(
            &conditions,
            None,
            Some(threshold + Duration::from_secs(1)),
            true,
        ),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert!(evaluate_source_conditions(&conditions, None, Some(threshold), true).is_ok());
    assert!(
        evaluate_source_conditions(
            &conditions,
            None,
            Some(threshold + Duration::from_millis(500)),
            true,
        )
        .is_ok()
    );
}

#[test]
fn checks_modified_precision() {
    let threshold = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let conditions = CopySourceConditions {
        if_modified_since: Some(threshold),
        ..Default::default()
    };
    assert_eq!(
        evaluate_source_conditions(&conditions, None, Some(threshold), true),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert_eq!(
        evaluate_source_conditions(
            &conditions,
            None,
            Some(threshold + Duration::from_millis(500)),
            true,
        ),
        Err(CopyObjectError::PreconditionFailed)
    );
    assert!(
        evaluate_source_conditions(
            &conditions,
            None,
            Some(threshold + Duration::from_secs(1)),
            true,
        )
        .is_ok()
    );
}

#[tokio::test]
async fn precondition_preserves_versions() {
    let (_temp, context) = full_context().await;
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);

    drive(
        PutObjectOperation::new(put_config(
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"payload",
        )),
        &context,
    )
    .await
    .unwrap();

    let error = copy_object(
        &context,
        CopyObjectInput {
            source_bucket: "bucket".to_string(),
            source_key: "source.txt".to_string(),
            source_version_id: None,
            source_group_id: group_id,
            source_auth_context: auth_context(user_id),
            dest_bucket: "bucket".to_string(),
            dest_key: "dest.txt".to_string(),
            user_id,
            group_id,
            realm_id,
            node_id,
            quota_ceiling: None,
            conditions: CopySourceConditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
            metadata: None,
            restrictions: None,
            references: CopyReferences::Materialize,
            job_id: None,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error, CopyObjectError::PreconditionFailed);

    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("bucket", "dest.txt")
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage read result");
    };
    assert!(value.is_none());
}
