//! Runs the mirror repair operations and the drain against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{EffectOperation, Runnable};
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    (tempdir, context)
}

async fn read_value(context: &DriverContext, space: &str, key: Key) -> Option<Value> {
    let read = StorageEffect::Read {
        key_space: space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected read event: {other:?}"),
    }
}

async fn repair_record(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Option<SyncMirrorRecord> {
    let key = repair_key(relationship);
    let value = read_value(context, MIRROR_REPAIR_KEYSPACE, key.clone()).await;
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
    read_value(context, RELATIONSHIP_OUT_KEYSPACE, key.into())
        .await
        .map(|value| SyncRelationship::from_bytes(&value).unwrap())
}

/// Runs `operation` against storage until it asks to commit, and returns that commit.
async fn run_to_commit<O: EffectOperation>(
    context: &DriverContext,
    operation: &mut O,
) -> StorageEffect {
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
        let mut activation =
            CreateRelationshipOperation::activate(relationship.clone()).into_machine();
        let mut rollback = SettleReconcileOperation::new(record.clone()).into_machine();
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
    let record = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Reconcile, 1);
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

#[tokio::test]
async fn delete_supersedes_reconcile() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let relationship = relationship();

    stage(&context, &relationship, SyncMirrorIntent::Reconcile).await;
    stage(&context, &relationship, SyncMirrorIntent::Delete).await;
    stage(&context, &relationship, SyncMirrorIntent::Reconcile).await;

    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: relationship.id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing mirror repair record");
    };
    let record = SyncMirrorRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
    assert_eq!(record.intent, SyncMirrorIntent::Delete);
}

#[tokio::test]
async fn delete_blocks_status() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let relationship = relationship();
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    stage(&context, &relationship, SyncMirrorIntent::Delete).await;
    let mut updated = relationship.clone();
    updated.status.last_error = Some("failed".to_string());

    assert!(!store_status(&context, &updated).await.unwrap());
    assert_eq!(
        drive(
            GetRelationshipOperation::new(relationship.id, SyncRelationshipDirection::Outgoing,),
            &context,
        )
        .await
        .unwrap(),
        relationship
    );
}

#[tokio::test]
async fn status_stages_reconcile() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let mut relationship = relationship();
    relationship.state = SyncState::Failed {
        reason: "failed".to_string(),
    };
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    relationship.state = SyncState::Enabled;
    relationship.status.last_error = None;

    assert!(store_status(&context, &relationship).await.unwrap());
    assert_eq!(
        drive(
            GetRelationshipOperation::new(relationship.id, SyncRelationshipDirection::Outgoing,),
            &context,
        )
        .await
        .unwrap(),
        relationship
    );
    assert_eq!(
        repair_record(&context, &relationship)
            .await
            .map(|record| record.relationship),
        Some(relationship)
    );
}

/// The plugin journal rows this node has not matched yet.
async fn journaled(context: &DriverContext) -> Vec<SourceEvent> {
    crate::plugins::journal::pending_events(context).await
}

async fn stored(context: &DriverContext, relationship: &SyncRelationship) {
    let direction = SyncRelationshipDirection::Outgoing;
    let store = StoreRelationshipOperation::new(relationship.clone(), direction);
    drive(store, context).await.unwrap();
}

#[tokio::test]
async fn failure_journals_once() {
    // A relationship that stops as failed journals one `sync_failed` row with its status;
    // storing it failed again writes none, and a failure after a resume writes a second.
    let (_dir, context) = setup_auth(None).await;
    let mut relationship = relationship();
    stored(&context, &relationship).await;
    let failed = SyncState::Failed {
        reason: "access_denied".to_string(),
    };
    relationship.state = failed.clone();
    assert!(store_status(&context, &relationship).await.unwrap());
    relationship.status.counters.failures = 2;
    assert!(store_status(&context, &relationship).await.unwrap());

    let events = journaled(&context).await;
    assert_eq!(events.len(), 1);
    let source = &relationship.source;
    let group_id = Ulid::from_bytes([6; 16]);
    let path = object_permission_path(source.realm_id, group_id, source.node_id, "source", "");
    assert_eq!(
        (events[0].kind, &events[0].path),
        (aruna_core::plugin_runs::SourceKind::SyncFailed, &path)
    );
    assert_eq!(events[0].resource, relationship.id.to_string());
    let prefix = format!("sync_failed/{}/{}/", source.node_id, relationship.id);
    assert!(events[0].source.starts_with(&prefix));

    relationship.state = SyncState::Enabled;
    assert!(store_status(&context, &relationship).await.unwrap());
    relationship.state = failed;
    assert!(store_status(&context, &relationship).await.unwrap());
    let events = journaled(&context).await;
    assert_eq!(events.len(), 2);
    assert_ne!(events[0].source, events[1].source);
}

#[tokio::test]
async fn failure_needs_commit() {
    // An aborted status transaction, a detached stub and a deleted source bucket journal
    // nothing.
    let failed = SyncState::Failed {
        reason: "access_denied".to_string(),
    };
    let (_dir, context) = setup_auth(None).await;
    let relationship = relationship();
    stored(&context, &relationship).await;
    let mut changed = relationship.clone();
    changed.replicate_deletes = false;
    changed.state = failed.clone();
    assert!(store_status(&context, &changed).await.is_err());
    let mut detached = relationship.clone();
    detached.state = SyncState::Detached;
    stored(&context, &detached).await;
    detached.state = failed.clone();
    assert!(!store_status(&context, &detached).await.unwrap());
    assert!(journaled(&context).await.is_empty());

    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let mut relationship = relationship;
    stored(&context, &relationship).await;
    relationship.state = failed;
    assert!(store_status(&context, &relationship).await.unwrap());
    assert!(journaled(&context).await.is_empty());
}

#[tokio::test]
async fn status_stays_handling() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let mut current = relationship();
    let stale = current.clone();
    current.set_reference_handling(ReferenceHandling::Preserve);
    drive(
        StoreRelationshipOperation::new(current.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();

    assert!(matches!(
        store_status(&context, &stale).await,
        Err(SyncMirrorError::Mirror(_))
    ));
    assert_eq!(
        drive(
            GetRelationshipOperation::new(current.id, SyncRelationshipDirection::Outgoing,),
            &context,
        )
        .await
        .unwrap(),
        current
    );
}

#[tokio::test]
async fn stale_reconcile_survives() {
    let (_tempdir, context) = storage_context();
    let relationship = relationship();
    let mut stale = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile, 1);
    stale.due_at_ms = 0;
    store_record(&context, stale.clone()).await;

    let mut current = stale.clone();
    current.relationship.status.last_error = Some("newer".to_string());
    current.due_at_ms = 10;
    store_record(&context, current.clone()).await;

    drive(
        MirrorRecordOperation::clear_scanned(stale.clone()),
        &context,
    )
    .await
    .unwrap();
    let reschedule = MirrorRecordOperation::reschedule(stale, "stale".to_string());
    drive(reschedule, &context).await.unwrap();

    assert_eq!(repair_record(&context, &relationship).await, Some(current));
}

#[tokio::test]
async fn newer_reconcile_survives() {
    let (_tempdir, context) = storage_context();
    let stale = relationship();
    let mut current = stale.clone();
    current.status.last_error = Some("newer".to_string());
    stage(&context, &current, SyncMirrorIntent::Reconcile).await;

    let clear = MirrorRecordOperation::clear(stale, SyncMirrorIntent::Reconcile);
    drive(clear, &context).await.unwrap();

    assert_eq!(
        repair_record(&context, &current)
            .await
            .map(|record| record.relationship),
        Some(current)
    );
}

#[tokio::test]
async fn delete_clear_preserved() {
    let (_tempdir, context) = storage_context();
    let stale = relationship();
    let mut current = stale.clone();
    current.status.last_error = Some("newer".to_string());
    stage(&context, &current, SyncMirrorIntent::Delete).await;

    let clear = MirrorRecordOperation::clear(stale.clone(), SyncMirrorIntent::Delete);
    drive(clear, &context).await.unwrap();

    assert!(repair_record(&context, &stale).await.is_none());
}

#[tokio::test]
async fn repair_backs_off() {
    // Authorized repair reaches the missing metadata handle and retries.
    let (_tempdir, context) = setup_auth(None).await;
    let storage = context.storage_handle.clone();
    let relationship = relationship();
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    stage(&context, &relationship, SyncMirrorIntent::Reconcile).await;

    let mut record = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile, 1);
    record.due_at_ms = 0;
    let (key_space, key, value) = repair_entry(record).unwrap();
    assert!(matches!(
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let result = process_mirror_repairs(&context, relationship.source.node_id)
        .await
        .unwrap();
    assert_eq!(result.failed, 1);

    let Event::Storage(StorageEvent::ReadResult {
        value: Some(value), ..
    }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: MIRROR_REPAIR_KEYSPACE.to_string(),
            key: relationship.id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing mirror repair retry");
    };
    let record = SyncMirrorRecord::from_bytes(&relationship.id.to_bytes(), &value).unwrap();
    assert_eq!(record.attempts, 1);
    assert_eq!(record.last_error.as_deref(), Some("target unreachable"));
}

#[tokio::test]
async fn policy_denies_write() {
    let (_tempdir, context) = setup_auth(Some("operation == 's3.PutBucketReplication'")).await;

    let result = ensure_target_write(&context, &relationship()).await;

    assert!(matches!(
        result,
        Err(SyncMirrorError::Mirror(error)) if error == "access_denied"
    ));
}

#[tokio::test]
async fn policy_allows_write() {
    let (_tempdir, context) = setup_auth(Some("operation == 's3.GetObject'")).await;

    assert!(ensure_target_write(&context, &relationship()).await.is_ok());
}

#[tokio::test]
async fn policy_retry_survives() {
    let (_tempdir, context) = setup_auth(Some("operation == 's3.PutBucketReplication'")).await;
    let mut relationship = relationship();
    relationship.target = ArunaArn::s3_bucket(
        relationship.target.realm_id,
        relationship.source.node_id,
        "target",
    )
    .unwrap();
    drive(
        StoreRelationshipOperation::new(relationship.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    let mut record = SyncMirrorRecord::new(relationship.clone(), SyncMirrorIntent::Reconcile, 1);
    record.due_at_ms = 0;
    store_record(&context, record).await;

    let result = process_mirror_repairs(&context, relationship.source.node_id)
        .await
        .unwrap();

    assert_eq!(result.failed, 1);
    assert_eq!(
        repair_record(&context, &relationship)
            .await
            .and_then(|record| record.last_error),
        Some("access_denied".to_string())
    );
}

#[test]
fn routes_mirror_changes() {
    let relationship = relationship();
    let (source, target) = (relationship.source.node_id, relationship.target.node_id);
    assert_eq!(
        create_peer(&relationship, source),
        Ok(MirrorPeer::Remote(target))
    );
    assert!(create_peer(&relationship, target).is_err());
    assert_eq!(
        delete_peer(&relationship, source, true),
        Ok(MirrorPeer::Remote(target))
    );
    assert_eq!(
        delete_peer(&relationship, target, false),
        Ok(MirrorPeer::Remote(source))
    );
    assert!(delete_peer(&relationship, target, true).is_err());
    assert!(delete_peer(&relationship, node(9), false).is_err());

    let mut local = relationship.clone();
    local.target.node_id = source;
    assert_eq!(create_peer(&local, source), Ok(MirrorPeer::Local));
    assert_eq!(delete_peer(&local, source, true), Ok(MirrorPeer::Local));
}
