//! Runs the quarantine operations against real storage and holds their shared fixtures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use aruna_core::NodeId;
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::{
    SyncQuarantineCapacity, SyncQuarantineEvidence, SyncQuarantineIdentity, SyncQuarantineInput,
    build_quarantine_entries, quarantine_row_entry, quarantine_usage_entry,
};
use aruna_core::types::{Key, Value};
use aruna_storage::storage;
use ulid::Ulid;

fn context() -> (DriverContext, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (context, dir)
}

fn event(index: u8) -> DocumentEvent {
    let node = NodeId::from_bytes(&[1u8; 32]).unwrap();
    DocumentEvent::Upsert {
        event_id: Ulid::from_bytes([index; 16]),
        target: DocumentTarget::RealmConfig {
            realm_id: RealmId([9; 32]),
        },
        bytes: vec![index; 8],
        change: DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_bytes([index; 16]),
                actor: node,
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        },
    }
}

fn identity(index: u8) -> SyncQuarantineIdentity {
    SyncQuarantineIdentity::from_parts([7; 32], [8; 32], u64::from(index) + 1)
}

fn input(index: u8, reason: &str) -> SyncQuarantineInput<'_> {
    SyncQuarantineInput {
        identity: identity(index),
        evidence: SyncQuarantineEvidence::from_event(&event(index)),
        reason,
        quarantined_at_ms: 42,
        replaced_bytes: None,
    }
}

/// One quarantine row for operation tests.
pub(super) fn record(index: u8, acknowledged: bool) -> SyncQuarantineRecord {
    let write = build_quarantine_entries(
        input(index, "invalid"),
        SyncQuarantineUsage::default(),
        SyncQuarantineCapacity::default(),
    )
    .unwrap();
    let mut record = write.record;
    record.acknowledged = acknowledged;
    record
}

pub(super) fn record_bytes(record: &SyncQuarantineRecord) -> Value {
    quarantine_row_entry(record).unwrap().2
}

pub(super) fn usage_bytes(usage: SyncQuarantineUsage) -> Value {
    quarantine_usage_entry(usage).unwrap().2
}

async fn write_rows(ctx: &DriverContext, writes: Vec<(String, Key, Value)>) {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

async fn stored_usage(ctx: &DriverContext) -> SyncQuarantineUsage {
    list(ctx, QuarantinePageRequest::default()).await.usage
}

async fn list(ctx: &DriverContext, request: QuarantinePageRequest) -> QuarantinePage {
    drive(ListQuarantineOperation::new(request), ctx)
        .await
        .unwrap()
}

async fn read(ctx: &DriverContext, index: u8) -> Option<SyncQuarantineRecord> {
    drive(ReadQuarantineOperation::new(row_key(index)), ctx)
        .await
        .unwrap()
}

async fn acknowledge(ctx: &DriverContext, index: u8) -> Option<SyncQuarantineRecord> {
    drive(AcknowledgeQuarantineOperation::new(row_key(index)), ctx)
        .await
        .unwrap()
}

async fn prune(ctx: &DriverContext, request: QuarantinePageRequest) -> QuarantinePruneResult {
    drive(PruneQuarantineOperation::new(request), ctx)
        .await
        .unwrap()
}

async fn seed_rows(ctx: &DriverContext, count: u8) -> SyncQuarantineUsage {
    let mut usage = SyncQuarantineUsage::default();
    for index in 0..count {
        let write = build_quarantine_entries(
            input(index, "invalid"),
            usage,
            SyncQuarantineCapacity::default(),
        )
        .unwrap();
        usage = write.usage;
        write_rows(ctx, vec![write.row, quarantine_usage_entry(usage).unwrap()]).await;
    }
    usage
}

fn row_key(index: u8) -> Vec<u8> {
    identity(index).storage_key()
}

#[tokio::test]
async fn list_pages_records() {
    let (ctx, _dir) = context();
    let usage = seed_rows(&ctx, 5).await;
    let first = list(
        &ctx,
        QuarantinePageRequest {
            topic: None,
            start_after: None,
            limit: Some(2),
        },
    )
    .await;
    assert_eq!(first.records.len(), 2);
    assert_eq!(first.usage, usage);
    assert_eq!(first.usage.records, 5);

    let second = list(
        &ctx,
        QuarantinePageRequest {
            topic: None,
            start_after: first.next_start_after.clone(),
            limit: Some(2),
        },
    )
    .await;
    assert_eq!(second.records.len(), 2);
    assert_ne!(second.records[0].event_id(), first.records[0].event_id());
}

#[tokio::test]
async fn inspect_reads_record() {
    let (ctx, _dir) = context();
    seed_rows(&ctx, 2).await;
    let record = read(&ctx, 1).await.unwrap();
    assert_eq!(record.decoded_event().unwrap(), event(1));
    assert!(read(&ctx, 9).await.is_none());
}

#[tokio::test]
async fn acknowledge_is_idempotent() {
    let (ctx, _dir) = context();
    let usage = seed_rows(&ctx, 2).await;
    assert!(acknowledge(&ctx, 0).await.is_some());
    assert!(acknowledge(&ctx, 0).await.is_some());
    assert!(acknowledge(&ctx, 9).await.is_none());
    let stored = read(&ctx, 0).await.unwrap();
    assert!(stored.acknowledged);
    assert_eq!(stored_usage(&ctx).await, usage);
}

#[tokio::test]
async fn prune_skips_unacknowledged() {
    let (ctx, _dir) = context();
    let usage = seed_rows(&ctx, 3).await;
    acknowledge(&ctx, 1).await;

    let result = prune(&ctx, QuarantinePageRequest::default()).await;
    assert_eq!(result.pruned, 1);
    assert_eq!(result.scanned, 3);
    assert_eq!(result.usage.records, 2);
    assert!(result.usage.bytes < usage.bytes);
    assert_eq!(stored_usage(&ctx).await, result.usage);
    assert!(read(&ctx, 1).await.is_none());
    assert!(read(&ctx, 0).await.is_some());
}

#[tokio::test]
async fn prune_is_resumable() {
    let (ctx, _dir) = context();
    seed_rows(&ctx, 4).await;
    for index in 0..4u8 {
        acknowledge(&ctx, index).await;
    }

    let mut cursor = None;
    let mut pruned = 0;
    loop {
        let result = prune(
            &ctx,
            QuarantinePageRequest {
                topic: None,
                start_after: cursor,
                limit: Some(1),
            },
        )
        .await;
        assert!(result.scanned <= 1);
        pruned += result.pruned;
        cursor = result.next_start_after;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pruned, 4);
    assert_eq!(stored_usage(&ctx).await, SyncQuarantineUsage::default());
}

#[tokio::test]
async fn prune_reclaims_capacity() {
    let (ctx, _dir) = context();
    let capacity = SyncQuarantineCapacity {
        max_records: 2,
        max_bytes: u64::MAX,
    };
    let usage = seed_rows(&ctx, 2).await;
    let error = build_quarantine_entries(
        SyncQuarantineInput {
            identity: identity(2),
            evidence: SyncQuarantineEvidence::from_event(&event(2)),
            reason: "invalid",
            quarantined_at_ms: 42,
            replaced_bytes: None,
        },
        usage,
        capacity,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        aruna_core::structs::SyncQuarantineError::AtCapacity { .. }
    ));

    acknowledge(&ctx, 0).await;
    let reclaimed = prune(&ctx, QuarantinePageRequest::default()).await;
    assert!(
        build_quarantine_entries(
            SyncQuarantineInput {
                identity: identity(2),
                evidence: SyncQuarantineEvidence::from_event(&event(2)),
                reason: "invalid",
                quarantined_at_ms: 42,
                replaced_bytes: None,
            },
            reclaimed.usage,
            capacity,
        )
        .is_ok()
    );
}
