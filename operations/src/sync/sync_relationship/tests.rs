//! Runs the sync relationship operations against real storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::sync::mirror_repair::{MirrorRecordOperation, SyncMirrorIntent};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{rejects_events, sample_events};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncState, SyncStatusSnapshot};
use aruna_core::{NodeId, UserId};
use aruna_storage::storage::FjallStorage;
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

        let (first_result, second_result) = tokio::join!(
            drive(CreateRelationshipOperation::new(first), &context),
            drive(CreateRelationshipOperation::new(second), &context),
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

// A state that expects no event must reject one instead of ignoring it.
#[test]
fn terminal_rejects_event() {
    let stray = || {
        Event::Storage(StorageEvent::DeleteResult {
            key: Vec::<u8>::new().into(),
        })
    };
    let page = || {
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        })
    };
    let record = relationship(1, "source-a", "target-a");
    let store = || {
        let mut store =
            StoreRelationshipOperation::new(record.clone(), SyncRelationshipDirection::Outgoing);
        store.start();
        store.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        }));
        store
    };
    for (_, effects, error) in rejects_events(store, sample_events, |_| false) {
        assert!(effects.is_empty());
        assert!(
            matches!(
                error,
                SyncRelationshipError::UnexpectedEvent { .. } | SyncRelationshipError::Storage(_)
            ),
            "{error:?}"
        );
    }

    let list = || {
        let mut list = ListRelationshipsOperation::new(SyncRelationshipDirection::Outgoing, None);
        list.start();
        list.step(page());
        list
    };
    for (_, effects, error) in rejects_events(list, sample_events, |_| false) {
        assert!(effects.is_empty());
        assert!(
            matches!(
                error,
                SyncRelationshipError::UnexpectedEvent { .. } | SyncRelationshipError::Storage(_)
            ),
            "{error:?}"
        );
    }

    let get = || {
        let mut get = GetRelationshipOperation::new(record.id, SyncRelationshipDirection::Outgoing);
        get.start();
        get.step(page());
        get
    };
    for (_, effects, error) in rejects_events(get, sample_events, |_| false) {
        assert!(effects.is_empty());
        assert!(
            matches!(
                error,
                SyncRelationshipError::UnexpectedEvent { .. } | SyncRelationshipError::Storage(_)
            ),
            "{error:?}"
        );
    }

    let delete = || {
        let mut delete =
            DeleteRelationshipOperation::new(record.clone(), SyncRelationshipDirection::Outgoing);
        delete.start();
        delete.step(stray());
        delete
    };
    for (_, effects, error) in rejects_events(delete, sample_events, |_| false) {
        assert!(effects.is_empty());
        assert!(
            matches!(
                error,
                SyncRelationshipError::UnexpectedEvent { .. } | SyncRelationshipError::Storage(_)
            ),
            "{error:?}"
        );
    }
}
