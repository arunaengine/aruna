//! Drives the routed update through its config, device and holder answers without I/O.
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

fn actor() -> Actor {
    Actor {
        node_id: node(1),
        user_id: UserId::local(Ulid::from_parts(8, 8), realm()),
        realm_id: realm(),
    }
}

fn update(expected_revision: Option<Ulid>) -> DocumentUpdate {
    DocumentUpdate {
        public: None,
        mutation: UpdateDocumentMutation::ReplaceRoCrate {
            jsonld: "{}".to_string(),
        },
        expected_revision,
        commit_message: None,
    }
}

fn route(expected_revision: Option<Ulid>) -> RouteUpdateOperation {
    let document = (None, Ulid::from_parts(3, 3));
    RouteUpdateOperation::new(
        (actor(), Some(node(1))),
        document,
        update(expected_revision),
        None,
    )
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn unreadable_config_undeliverable() {
    let mut update = route(None);
    assert!(matches!(update.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert!(matches!(
        update.next(missing),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}

#[test]
fn device_reads_replica_first() {
    // A node outside the sync set edits its own replica when it may.
    let mut update = route(None);
    update.first().unwrap();
    let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let turn = update.next(child(Ok::<_, GetConfigError>(config)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(update.stage, UpdateStage::Replica);
}

#[test]
fn expected_revision_skips_device() {
    // Only a holder can check an expected revision, so the change is routed at once.
    let mut update = route(Some(Ulid::from_parts(4, 4)));
    update.first().unwrap();
    let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let _ = update.next(child(Ok::<_, GetConfigError>(config)));
    assert_ne!(update.stage, UpdateStage::Replica);
}

#[test]
fn device_edit_errors_mapped() {
    let mut update = route(None);
    update.stage = UpdateStage::Device;
    let missing = child(Err::<MetadataRegistryRecord, _>(DeviceEditError::NoReplica));
    assert_eq!(update.next(missing), Err(MetadataWriteError::NotFound));
}

#[test]
fn full_history_forwards() {
    let mut update = route(None);
    update.config = Some(RealmConfigDocument::default_for_realm(realm(), Vec::new()));
    update.holders = vec![node(1), node(2)];
    update.stage = UpdateStage::Local;
    let full = child(Err::<MetadataRegistryRecord, _>(
        UpdateDocumentError::RawLimit,
    ));
    assert!(matches!(
        update.next(full),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(update.stage, UpdateStage::Forward);
}

#[test]
fn holder_conflict_reported() {
    let mut update = route(None);
    update.config = Some(RealmConfigDocument::default_for_realm(realm(), Vec::new()));
    update.placement = Some(PlacementRef::NIL);
    update.stage = UpdateStage::Forward;
    let (expected, current) = (Ulid::from_parts(4, 4), Ulid::from_parts(5, 5));
    let conflict = MetadataTransportMessage::ForwardedRevisionConflict { expected, current };
    let answer = child(Ok::<_, HolderWriteError>(conflict));
    let error =
        MetadataWriteError::from(UpdateDocumentError::RevisionConflict { expected, current });
    assert_eq!(update.next(answer), Err(error));
}
