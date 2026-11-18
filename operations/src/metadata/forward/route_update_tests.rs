//! Drives the routed update through its config, local attempt and holder answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmId;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn document() -> Ulid {
    Ulid::from_parts(3, 3)
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
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id: document(),
        document_path: "datasets/routed".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document()),
        public: false,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node(1)],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(5, 5),
        last_event_id: Ulid::from_parts(5, 5),
    }
}

fn update() -> RouteUpdateOperation {
    let input = RouteUpdateInput {
        actor: Actor {
            node_id: node(1),
            user_id: UserId::local(Ulid::from_parts(8, 8), realm()),
            realm_id: realm(),
        },
        record: None,
        document_id: document(),
        public: None,
        mutation: UpdateDocumentMutation::ReplaceRoCrate {
            jsonld: "{}".to_string(),
        },
        expected_revision: Some(Ulid::from_parts(6, 6)),
        auth_token: None,
        commit_message: None,
    };
    RouteUpdateOperation::new(input, Some(node(1)))
}

fn at(stage: UpdateStage) -> RouteUpdateOperation {
    let mut update = update();
    update.stage = stage;
    update.holders = vec![node(1), node(2)];
    update
}

fn holder_said(message: MetadataTransportMessage) -> IoResult {
    child(Ok::<_, HolderWriteError>(message))
}

// The update starts at the realm config and stops when it is missing.
#[test]
fn update_needs_config() {
    let mut update = update();
    assert!(matches!(update.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert!(matches!(
        update.next(missing),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}

// The local update's record is the result.
#[test]
fn local_update_ends() {
    let mut update = at(UpdateStage::Local);
    let done = child(Ok::<_, UpdateDocumentError>(record()));
    assert_eq!(update.next(done), Ok(Next::Done(record())));
}

// A full local history moves the update on to the other holders.
#[test]
fn full_history_forwards() {
    let mut update = at(UpdateStage::Local);
    let full = child(Err::<MetadataRegistryRecord, _>(
        UpdateDocumentError::RawLimit,
    ));
    assert!(matches!(
        update.next(full),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(update.stage, UpdateStage::Forward);
}

// A local refusal is final and reaches no other holder.
#[test]
fn local_conflict_fails() {
    let (expected, current) = (Ulid::from_parts(6, 6), Ulid::from_parts(7, 7));
    let conflict = UpdateDocumentError::RevisionConflict { expected, current };
    let mut update = at(UpdateStage::Local);
    let refused = child(Err::<MetadataRegistryRecord, _>(conflict));
    assert_eq!(
        update.next(refused),
        Err(MetadataWriteError::Update(
            UpdateDocumentError::RevisionConflict { expected, current }
        ))
    );
}

// A holder's revision conflict comes back as the same error a local update gives.
#[test]
fn holder_conflict_fails() {
    let (expected, current) = (Ulid::from_parts(6, 6), Ulid::from_parts(7, 7));
    let mut update = at(UpdateStage::Forward);
    let answer = MetadataTransportMessage::ForwardedRevisionConflict { expected, current };
    assert_eq!(
        update.next(holder_said(answer)),
        Err(MetadataWriteError::Update(
            UpdateDocumentError::RevisionConflict { expected, current }
        ))
    );
}

// A record the routing cannot match to this document is never accepted.
#[test]
fn foreign_record_fails() {
    let mut update = at(UpdateStage::Forward);
    let answer = MetadataTransportMessage::ForwardedRecord {
        record: Box::new(record()),
    };
    assert!(matches!(
        update.next(holder_said(answer)),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}

// A holder that refuses the forward fails the update with its reason.
#[test]
fn holder_refusal_fails() {
    let mut update = at(UpdateStage::Forward);
    let refused = child(Err::<MetadataTransportMessage, _>(
        HolderWriteError::Forbidden,
    ));
    assert_eq!(update.next(refused), Err(MetadataWriteError::Forbidden));
}
