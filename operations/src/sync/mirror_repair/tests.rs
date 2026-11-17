use super::drain::ensure_target_write;
use super::settle::{RollbackClaim, SettleReconcileOperation};
use super::*;
use crate::driver::{DriverContext, drive};
use crate::sync::sync_relationship::{
    CreateRelationshipOperation, GetRelationshipOperation, StoreRelationshipOperation,
    SyncRelationshipDirection,
};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::request_policy::{PolicyKind, RequestPolicy};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::sync_relationship_key;
use aruna_core::structs::{ReferenceHandling, SyncMode, SyncState, SyncStatusSnapshot};
use aruna_storage::FjallStorage;
use std::time::SystemTime;
use ulid::Ulid;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn relationship() -> SyncRelationship {
    let realm_id = RealmId::from_bytes([1; 32]);
    SyncRelationship {
        id: Ulid::from_bytes([2; 16]),
        source: ArunaArn::s3_bucket(realm_id, node(3), "source").unwrap(),
        target: ArunaArn::s3_bucket(realm_id, node(4), "target").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: UserId::local(Ulid::from_bytes([5; 16]), realm_id),
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    }
}

fn policy(expression: &str) -> RequestPolicy {
    RequestPolicy {
        policy_id: Ulid::from_bytes([7; 16]),
        name: "mirror-test".to_string(),
        kind: PolicyKind::Deny,
        when: None,
        expression: expression.to_string(),
        enabled: true,
    }
}

async fn setup_auth(expression: Option<&str>) -> (tempfile::TempDir, DriverContext) {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let relationship = relationship();
    let realm_id = relationship.source.realm_id;
    let group_id = Ulid::from_bytes([6; 16]);
    let actor = Actor {
        node_id: relationship.source.node_id,
        user_id: relationship.created_by,
        realm_id,
    };
    let auth_doc =
        GroupAuthorizationDocument::default_group_doc(relationship.created_by, realm_id, group_id);
    let group = Group {
        display_name: "mirror-test".to_string(),
        group_id,
        realm_id,
        roles: auth_doc.roles.keys().copied().collect(),
        owner: relationship.created_by,
    };
    let mut realm = RealmConfigDocument::new(realm_id, Vec::new(), 1);
    realm.request_policies = expression.map(policy).into_iter().collect();
    let source_bucket = relationship.source.bucket().unwrap();
    let bucket = relationship.target.bucket().unwrap();
    let bucket_info = BucketInfo {
        group_id,
        created_at: relationship.created_at,
        created_by: relationship.created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let event = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: vec![
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    realm.to_bytes(&actor).unwrap().into(),
                ),
                (
                    GROUP_KEYSPACE.to_string(),
                    group_id.to_bytes().to_vec().into(),
                    group.to_bytes(&actor).unwrap().into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    realm_id.as_bytes().to_vec().into(),
                    RealmAuthorizationDocument::default_realm_doc(realm_id)
                        .to_bytes(&actor)
                        .unwrap()
                        .into(),
                ),
                (
                    AUTH_KEYSPACE.to_string(),
                    group_id.to_bytes().to_vec().into(),
                    auth_doc.to_bytes(&actor).unwrap().into(),
                ),
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    source_bucket.as_bytes().into(),
                    bucket_info.to_bytes().unwrap().into(),
                ),
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    bucket.as_bytes().into(),
                    bucket_info.to_bytes().unwrap().into(),
                ),
            ],
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    (tempdir, context)
}

fn storage_context() -> (tempfile::TempDir, DriverContext) {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (tempdir, context)
}

async fn read_value(context: &DriverContext, effect: Effect) -> Option<Value> {
    let Effect::Storage(effect) = effect else {
        panic!("expected a storage read");
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected read event: {other:?}"),
    }
}

async fn repair_record(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Option<SyncMirrorRecord> {
    let key = repair_key(relationship);
    let value = read_value(context, repair_read(&key, None)).await;
    decode_record(&key, value).unwrap()
}

async fn stage(context: &DriverContext, relationship: &SyncRelationship, intent: SyncMirrorIntent) {
    drive(
        MirrorRecordOperation::stage(relationship.clone(), intent),
        context,
    )
    .await
    .unwrap();
}

async fn store_record(context: &DriverContext, record: SyncMirrorRecord) {
    drive(MirrorRecordOperation::store(record), context)
        .await
        .unwrap();
}

async fn settle(context: &DriverContext, record: &SyncMirrorRecord) -> RollbackClaim {
    drive(SettleReconcileOperation::new(record.clone()), context)
        .await
        .unwrap()
}

async fn store_status(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<bool, SyncMirrorError> {
    drive(StoreStatusOperation::new(relationship.clone()), context).await
}

async fn staged_record(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> SyncMirrorRecord {
    stage(context, relationship, SyncMirrorIntent::Reconcile).await;
    stored_record(context, relationship).await
}

async fn stored_record(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> SyncMirrorRecord {
    repair_record(context, relationship).await.unwrap()
}

async fn outgoing(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Option<SyncRelationship> {
    let key = sync_relationship_key(relationship.source.bucket().unwrap(), relationship.id);
    read_value(context, outgoing_read(&key.into(), None))
        .await
        .map(|value| SyncRelationship::from_bytes(&value).unwrap())
}

/// Runs `operation` against storage until it asks to commit, and returns that commit.
async fn run_to_commit<O: Operation>(context: &DriverContext, operation: &mut O) -> StorageEffect {
    let mut queue: std::collections::VecDeque<Effect> = operation.start().into_iter().collect();
    while let Some(effect) = queue.pop_front() {
        let Effect::Storage(effect) = effect else {
            panic!("expected storage effects only");
        };
        if matches!(effect, StorageEffect::CommitTransaction { .. }) {
            return effect;
        }
        let event = context.storage_handle.send_storage_effect(effect).await;
        queue.extend(operation.step(event));
    }
    panic!("operation finished without a commit");
}

#[tokio::test]
async fn rollback_blocks_activation() {
    let (_tempdir, context) = storage_context();
    let relationship = relationship();
    let record = staged_record(&context, &relationship).await;

    assert_eq!(settle(&context, &record).await, RollbackClaim::Claimed);
    assert_eq!(
        drive(
            CreateRelationshipOperation::new(relationship.clone()),
            &context
        )
        .await,
        Err(SyncRelationshipError::RolledBack)
    );
    assert_eq!(
        drive(
            CreateRelationshipOperation::activate(relationship.clone()),
            &context
        )
        .await,
        Err(SyncRelationshipError::RolledBack)
    );

    assert_eq!(outgoing(&context, &relationship).await, None);
    let claimed = stored_record(&context, &relationship).await;
    assert_eq!(claimed.intent, SyncMirrorIntent::Delete);
    assert!(claimed.rollback);
}

#[tokio::test]
async fn activation_blocks_rollback() {
    let (_tempdir, context) = storage_context();
    let relationship = relationship();
    let record = staged_record(&context, &relationship).await;

    drive(
        CreateRelationshipOperation::new(relationship.clone()),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(
        settle(&context, &record).await,
        RollbackClaim::Active(relationship.clone())
    );
    assert_eq!(stored_record(&context, &relationship).await, record);
    assert_eq!(outgoing(&context, &relationship).await, Some(relationship));
}

#[tokio::test]
async fn claims_conflict() {
    // Both orders: the claim that commits second must fail and change nothing.
    for activation_first in [true, false] {
        let (_tempdir, context) = storage_context();
        let relationship = relationship();
        let record = staged_record(&context, &relationship).await;
        let mut activation = CreateRelationshipOperation::activate(relationship.clone());
        let mut rollback = SettleReconcileOperation::new(record.clone());
        let activation_commit = run_to_commit(&context, &mut activation).await;
        let rollback_commit = run_to_commit(&context, &mut rollback).await;
        let (first, second) = match activation_first {
            true => (activation_commit, rollback_commit),
            false => (rollback_commit, activation_commit),
        };
        assert!(matches!(
            context.storage_handle.send_storage_effect(first).await,
            Event::Storage(StorageEvent::TransactionCommitted { .. })
        ));
        assert_eq!(
            context.storage_handle.send_storage_effect(second).await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict
            })
        );
        let stored = stored_record(&context, &relationship).await;
        if activation_first {
            assert_eq!(stored, record);
            assert_eq!(outgoing(&context, &relationship).await, Some(relationship));
        } else {
            assert!(stored.rollback);
            assert_eq!(outgoing(&context, &relationship).await, None);
        }
    }
}

#[tokio::test]
async fn delete_keeps_rollback() {
    let (_tempdir, context) = storage_context();
    let relationship = relationship();
    let record = staged_record(&context, &relationship).await;
    settle(&context, &record).await;

    stage(&context, &relationship, SyncMirrorIntent::Delete).await;

    let stored = stored_record(&context, &relationship).await;
    assert_eq!(stored.intent, SyncMirrorIntent::Delete);
    assert!(stored.rollback);
}

#[tokio::test]
async fn rollback_skips_stub() {
    // A reference-serving create that never activated must not leave a detached stub.
    for mode in [SyncMode::Reference, SyncMode::Continuous] {
        let (_tempdir, context) = storage_context();
        let mut relationship = relationship();
        relationship.mode = mode;
        relationship.set_reference_handling(ReferenceHandling::Preserve);
        let record = staged_record(&context, &relationship).await;
        settle(&context, &record).await;

        let result = process_mirror_repairs(&context, relationship.source.node_id)
            .await
            .unwrap();

        assert_eq!(result.failed, 1);
        assert_eq!(outgoing(&context, &relationship).await, None);
        assert_eq!(
            stored_record(&context, &relationship).await.last_error,
            Some("target unreachable".to_string())
        );
    }
}

#[test]
fn repair_record_roundtrips() {
    let record = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Reconcile);
    let key = record.relationship.id.to_bytes();

    assert_eq!(
        SyncMirrorRecord::from_bytes(&key, &record.to_bytes().unwrap()).unwrap(),
        record
    );
}

#[test]
fn serving_changes_identity() {
    let left = relationship();
    let mut right = left.clone();
    right.reference_serving = true;

    assert!(!same_relationship(&left, &right));
}
