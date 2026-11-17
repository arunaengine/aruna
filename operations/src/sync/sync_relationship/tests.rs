//! Runs the sync relationship operations against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive, drive_retry};
use crate::sync::mirror_repair::{MirrorRecordOperation, SyncMirrorIntent};
use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot};
use aruna_core::{NodeId, UserId};
use aruna_storage::storage::FjallStorage;
use byteview::ByteView;
use std::time::SystemTime;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

fn test_context() -> (TempDir, DriverContext) {
    let tempdir = tempdir().unwrap();
    let storage_handle = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle,
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

fn test_node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn relationship(id: u8, source: &str, target: &str) -> SyncRelationship {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let mut id_bytes = [0u8; 16];
    id_bytes[15] = id;
    SyncRelationship {
        id: Ulid::from_bytes(id_bytes),
        source: ArunaArn::s3_object_prefix(realm_id, test_node(2), source, "selected/").unwrap(),
        target: ArunaArn::s3_object_prefix(realm_id, test_node(3), target, "replica/").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: UserId::local(Ulid::from_bytes([4u8; 16]), realm_id),
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    }
}

#[tokio::test]
async fn outgoing_crud() {
    let (_tempdir, context) = test_context();
    let first = relationship(1, "source-a", "target-a");
    let second = relationship(2, "source-b", "target-a");

    assert_eq!(
        drive(
            StoreRelationshipOperation::new(first.clone(), SyncRelationshipDirection::Outgoing,),
            &context,
        )
        .await
        .unwrap(),
        first
    );
    drive(
        StoreRelationshipOperation::new(second.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();

    let listed = drive(
        ListRelationshipsOperation::new(
            SyncRelationshipDirection::Outgoing,
            Some("source-a".to_string()),
        ),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(listed, vec![first.clone()]);

    let fetched = drive(
        GetRelationshipOperation::new(first.id, SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(fetched, first);

    drive(
        DeleteRelationshipOperation::new(first.clone(), SyncRelationshipDirection::Outgoing),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(
        drive(
            GetRelationshipOperation::new(first.id, SyncRelationshipDirection::Outgoing,),
            &context,
        )
        .await,
        Err(SyncRelationshipError::NotFound)
    );

    let remaining = drive(
        ListRelationshipsOperation::new(SyncRelationshipDirection::Outgoing, None),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(remaining, vec![second]);
}

#[tokio::test]
async fn incoming_crud() {
    let (_tempdir, context) = test_context();
    let relationships = (1..=129)
        .map(|id| relationship(id, "source", "target"))
        .collect::<Vec<_>>();

    for relationship in &relationships {
        drive(
            StoreRelationshipOperation::new(
                relationship.clone(),
                SyncRelationshipDirection::Incoming,
            ),
            &context,
        )
        .await
        .unwrap();
    }

    let listed = drive(
        ListRelationshipsOperation::new(
            SyncRelationshipDirection::Incoming,
            Some("target".to_string()),
        ),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(listed, relationships);

    let last = relationships.last().unwrap();
    let fetched = drive(
        GetRelationshipOperation::new(last.id, SyncRelationshipDirection::Incoming),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(&fetched, last);

    drive(
        DeleteRelationshipOperation::new(last.clone(), SyncRelationshipDirection::Incoming),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(
        drive(
            GetRelationshipOperation::new(last.id, SyncRelationshipDirection::Incoming,),
            &context,
        )
        .await,
        Err(SyncRelationshipError::NotFound)
    );
}

#[tokio::test]
async fn create_is_atomic() {
    // Every duplicate race must commit one row and reject the losing transaction.
    let (_tempdir, context) = test_context();

    for attempt in 0..16u8 {
        let source = format!("source-{attempt}");
        let target = format!("target-{attempt}");
        let first = relationship(attempt * 2 + 1, &source, &target);
        let second = relationship(attempt * 2 + 2, &source, &target);
        for relationship in [&first, &second] {
            let stage =
                MirrorRecordOperation::stage(relationship.clone(), SyncMirrorIntent::Reconcile);
            drive(stage, &context).await.unwrap();
        }

        let (first_seed, second_seed) = (first.id.to_bytes(), second.id.to_bytes());
        let (first_result, second_result) = tokio::join!(
            drive_retry(
                || CreateRelationshipOperation::new(first.clone()),
                &context,
                &first_seed
            ),
            drive_retry(
                || CreateRelationshipOperation::new(second.clone()),
                &context,
                &second_seed
            ),
        );
        assert!(matches!(
            (&first_result, &second_result),
            (Ok(_), Err(SyncRelationshipError::Duplicate))
                | (Err(SyncRelationshipError::Duplicate), Ok(_))
        ));

        let stored = drive(
            ListRelationshipsOperation::new(SyncRelationshipDirection::Outgoing, Some(source)),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(stored.len(), 1);
    }
}

/// The page call the operations should make, built here independently of `pages`.
pub(super) fn page_call(
    direction: SyncRelationshipDirection,
    bucket: Option<&str>,
    after: Option<Key>,
) -> IoCall {
    IoCall::Page {
        space: direction.keyspace().to_string(),
        prefix: ByteView::from(bucket.map(sync_relationship_prefix).unwrap_or_default()),
        after,
        limit: RELATIONSHIP_PAGE_SIZE,
    }
}

fn page(rows: Vec<&SyncRelationship>, next: Option<Key>) -> IoResult {
    let rows = rows
        .into_iter()
        .map(|relationship| {
            let key = storage_key(SyncRelationshipDirection::Incoming, relationship).unwrap();
            (key, ByteView::from(relationship.to_bytes().unwrap()))
        })
        .collect();
    IoResult::Page { rows, next }
}

#[test]
fn writes_and_deletes() {
    let record = relationship(1, "source-a", "target-a");
    let incoming = SyncRelationshipDirection::Incoming;
    let mut store = StoreRelationshipOperation::new(record.clone(), incoming);
    assert_eq!(
        store.first(),
        Ok(Next::Call(write_call(incoming, &record).unwrap()))
    );
    assert_eq!(store.next(IoResult::Done), Ok(Next::Done(record.clone())));
    let mut delete = DeleteRelationshipOperation::new(record.clone(), incoming);
    assert_eq!(
        delete.first(),
        Ok(Next::Call(delete_call(incoming, &record).unwrap()))
    );
    assert_eq!(delete.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn reads_pages() {
    let (first, second) = (relationship(1, "s", "t"), relationship(2, "s", "t"));
    let incoming = SyncRelationshipDirection::Incoming;
    let after = Some(storage_key(incoming, &first).unwrap());
    let mut list = ListRelationshipsOperation::new(incoming, Some("t".to_string()));
    assert_eq!(
        list.first(),
        Ok(Next::Call(page_call(incoming, Some("t"), None)))
    );
    assert_eq!(
        list.next(page(vec![&first], after.clone())),
        Ok(Next::Call(page_call(incoming, Some("t"), after.clone())))
    );
    assert_eq!(
        list.next(page(vec![&second], None)),
        Ok(Next::Done(vec![first.clone(), second.clone()]))
    );
    let mut get = GetRelationshipOperation::new(second.id, incoming);
    assert_eq!(get.first(), Ok(Next::Call(page_call(incoming, None, None))));
    assert_eq!(
        get.next(page(vec![&first], after.clone())),
        Ok(Next::Call(page_call(incoming, None, after)))
    );
    assert_eq!(get.next(page(vec![&second], None)), Ok(Next::Done(second)));
    let mut missing = GetRelationshipOperation::new(Ulid::nil(), incoming);
    assert_eq!(
        missing.next(page(vec![&first], None)),
        Err(SyncRelationshipError::NotFound)
    );
}

#[test]
fn refuses_bad_results() {
    let record = relationship(1, "source-a", "target-a");
    let incoming = SyncRelationshipDirection::Incoming;
    let unexpected = |result: Result<_, SyncRelationshipError>| {
        matches!(
            result,
            Err(SyncRelationshipError::Io(IoError::Unexpected { .. }))
        )
    };
    let mut store = StoreRelationshipOperation::new(record.clone(), incoming);
    assert!(unexpected(
        store.next(IoResult::Values(Vec::new())).map(drop)
    ));
    let mut list = ListRelationshipsOperation::new(incoming, None);
    assert!(unexpected(list.next(IoResult::Done).map(drop)));
    let mut get = GetRelationshipOperation::new(record.id, incoming);
    assert!(unexpected(get.next(IoResult::Done).map(drop)));
    // A row stored under another relationship's key is refused, not returned.
    let moved = IoResult::Page {
        rows: vec![(
            storage_key(incoming, &relationship(2, "s", "t")).unwrap(),
            ByteView::from(record.to_bytes().unwrap()),
        )],
        next: None,
    };
    assert!(matches!(
        list.next(moved),
        Err(SyncRelationshipError::Conversion(_))
    ));
    let storage = SyncRelationshipError::from(IoError::Storage(StorageError::Timeout));
    assert_eq!(
        storage,
        SyncRelationshipError::Storage(StorageError::Timeout)
    );
}
