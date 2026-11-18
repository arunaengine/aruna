//! Drives the update entry and its readiness check through their calls without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::StorageEvent;
use aruna_core::storage_entries::event_log_key;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;

use crate::metadata::update_document::UpdateDocumentConfig;

fn document() -> Ulid {
    Ulid::from_parts(3, 3)
}

fn expected() -> Ulid {
    Ulid::from_parts(4, 4)
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn newest(event_id: Ulid) -> IoResult {
    let key = event_log_key(document(), event_id);
    event(Event::Storage(StorageEvent::IterResult {
        values: vec![(key, Vec::new().into())],
        next_start_after: None,
    }))
}

fn status(event_id: Ulid, state: MaterializationState) -> IoResult {
    let status = MaterializationStatusRecord {
        document_id: document(),
        event_id,
        graph_iri: String::new(),
        context_digest: None,
        dataset_digest: None,
        state,
        attempts: 0,
        failures: 0,
        last_error: None,
        updated_at_ms: 1,
    };
    IoResult::Values(vec![Some(postcard::to_allocvec(&status).unwrap().into())])
}

fn page(keys: Vec<Key>) -> IoResult {
    IoResult::Page {
        rows: keys
            .into_iter()
            .map(|key| (key, Vec::new().into()))
            .collect(),
        next: None,
    }
}

fn ready_at_markers() -> ReadyBaseOperation {
    let mut ready = ReadyBaseOperation::new(document(), expected());
    ready.first().unwrap();
    ready.next(newest(expected())).unwrap();
    let Ok(Next::Call(IoCall::Page { space, .. })) =
        ready.next(status(expected(), MaterializationState::Materialized))
    else {
        panic!("expected the marker page");
    };
    assert_eq!(space, PENDING_PROJECTION_KEYSPACE);
    ready
}

#[test]
fn stale_base_conflicts() {
    let mut ready = ReadyBaseOperation::new(document(), expected());
    assert!(matches!(ready.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let newer = Ulid::from_parts(5, 5);
    assert_eq!(
        ready.next(newest(newer)),
        Err(UpdateDocumentError::RevisionConflict {
            expected: expected(),
            current: newer,
        })
    );
}

#[test]
fn unapplied_base_pends() {
    let mut ready = ReadyBaseOperation::new(document(), expected());
    ready.first().unwrap();
    ready.next(newest(expected())).unwrap();
    assert_eq!(
        ready.next(status(expected(), MaterializationState::Pending)),
        Err(UpdateDocumentError::GraphPending {
            expected: expected()
        })
    );
}

#[test]
fn applied_base_ready() {
    let mut ready = ready_at_markers();
    let Ok(Next::Call(IoCall::Page { space, limit, .. })) = ready.next(page(Vec::new())) else {
        panic!("expected the job page");
    };
    assert_eq!((space.as_str(), limit), (DOCUMENT_JOB_KEYSPACE, 1));
    assert_eq!(ready.next(page(Vec::new())), Ok(Next::Done(())));

    let mut queued = ready_at_markers();
    queued.next(page(Vec::new())).unwrap();
    assert_eq!(
        queued.next(page(vec![Key::from(vec![1])])),
        Err(UpdateDocumentError::GraphPending {
            expected: expected()
        })
    );
}

#[test]
fn unprojected_marker_pends() {
    // A marker whose event is no applied batch is a received change not handed on yet.
    let mut ready = ready_at_markers();
    let marker = event_log_key(document(), Ulid::from_parts(6, 6));
    assert_eq!(
        ready.next(page(vec![marker.clone()])),
        Ok(Next::Call(read_row(EVENT_LOG_KEYSPACE, marker)))
    );
    assert_eq!(
        ready.next(IoResult::Values(vec![None])),
        Err(UpdateDocumentError::GraphPending {
            expected: expected()
        })
    );
}

fn update(
    mutation: UpdateDocumentMutation,
    expected_revision: Option<Ulid>,
) -> UpdateEntryOperation {
    let realm_id = RealmId::from_bytes([1; 32]);
    let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
        actor: Actor {
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        },
        group_id: Ulid::from_parts(2, 2),
        document_id: document(),
        public: false,
        mutation,
        expected_revision,
    });
    UpdateEntryOperation::new(operation, None)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn record() -> MetadataRegistryRecord {
    MetadataRegistryRecord {
        realm_id: RealmId::from_bytes([1; 32]),
        group_id: Ulid::from_parts(2, 2),
        document_id: document(),
        document_path: "datasets/entry".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document()),
        public: false,
        permission_path: String::new(),
        placement: Default::default(),
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 2,
        establishing_event_id: Ulid::from_parts(1, 1),
        last_event_id: expected(),
    }
}

#[test]
fn update_holds_document() {
    let mut entry = update(UpdateDocumentMutation::Checkpoint, None);
    assert_eq!(
        entry.hold_scope(),
        Some(HoldScope::DocumentWrite(document()))
    );
    // A checkpoint plans on no base and carries no Profile verdict.
    assert_eq!(
        entry.first(),
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            MetadataEffect::CacheGeneration
        )))))
    );
    let generation = event(Event::Metadata(MetadataEvent::CacheGeneration {
        generation: 7,
    }));
    assert!(matches!(
        entry.next(generation),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let updated = child(Ok::<_, UpdateDocumentError>(record()));
    assert_eq!(
        entry.next(updated),
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            MetadataEffect::CacheRecords {
                records: vec![record()],
                generation: Some(7),
            }
        )))))
    );
    let cached = event(Event::Metadata(MetadataEvent::CacheGeneration {
        generation: 8,
    }));
    assert_eq!(entry.next(cached), Ok(Next::Done(record())));
}

#[test]
fn uncached_update_done() {
    let mut entry = update(UpdateDocumentMutation::Checkpoint, None);
    entry.first().unwrap();
    let missing = event(Event::Metadata(MetadataEvent::Error {
        graph_iri: None,
        error: MetadataError::HandleMissing,
    }));
    entry.next(missing).unwrap();
    let updated = child(Ok::<_, UpdateDocumentError>(record()));
    assert_eq!(entry.next(updated), Ok(Next::Done(record())));
}

#[test]
fn checked_base_first() {
    let jsonld = r#"{"@id":"./data/file.txt","@type":"File"}"#.to_string();
    let mut entry = update(
        UpdateDocumentMutation::UpsertDataEntity { jsonld },
        Some(expected()),
    );
    assert!(matches!(entry.first(), Ok(Next::Call(IoCall::Child(_)))));
    let pending = || UpdateDocumentError::GraphPending {
        expected: expected(),
    };
    assert_eq!(entry.next(child(Err::<(), _>(pending()))), Err(pending()));
}
