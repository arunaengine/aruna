//! Drives the Profile export through its child calls and peer answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::metadata::{MaterializationState, MetadataRawRevision};

fn realm() -> RealmId {
    RealmId::from_bytes([3; 32])
}

fn profile_id() -> Ulid {
    Ulid::from_parts(7, 7)
}

fn revision() -> Ulid {
    Ulid::from_parts(9, 9)
}

fn record() -> MetadataRegistryRecord {
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id: profile_id(),
        document_path: format!("{PROFILE_DOCUMENT_PREFIX}example"),
        graph_iri: MetadataRegistryRecord::graph_iri_for(profile_id()),
        public: true,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: revision(),
        last_event_id: revision(),
    }
}

fn view(winning: Ulid) -> MetadataRawView {
    MetadataRawView {
        revision: MetadataRawRevision {
            jsonld: "{}".to_string(),
            winning_event_id: winning,
            context_digest: [1; 32],
            dataset_digest: Some([2; 32]),
            merged: None,
        },
        projection_state: MaterializationState::Materialized,
        projected_event_id: Some(winning),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn served() -> ExportMetadataResult {
    ExportMetadataResult::Raw {
        record: record(),
        raw: view(revision()),
        dataset_digest: Some([2; 32]),
    }
}

#[test]
fn local_serves_profile() {
    let mut local = ProfileLocalOperation::new(realm(), profile_id(), revision());
    assert!(matches!(local.first(), Ok(Next::Call(IoCall::Child(_)))));
    let found = child(Ok::<_, LiveRecordsError>(Some(record())));
    assert!(matches!(
        local.next(found),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let raw = child(Ok::<_, RawReadError>(Some(view(revision()))));
    assert_eq!(local.next(raw), Ok(Next::Done(served())));
}

#[test]
fn local_fences_profile() {
    // Only a live Profile document at exactly the asked revision is served.
    let answer = |stored: Option<MetadataRegistryRecord>| {
        let mut local = ProfileLocalOperation::new(realm(), profile_id(), revision());
        local.first().unwrap();
        local.next(child(Ok::<_, LiveRecordsError>(stored)))
    };
    assert_eq!(answer(None), Err(MetadataReadError::NotFound));
    let mut outside = record();
    outside.document_path = "datasets/example".to_string();
    assert_eq!(answer(Some(outside)), Err(MetadataReadError::NotFound));
    let mut newer = record();
    newer.last_event_id = Ulid::from_parts(10, 10);
    assert_eq!(answer(Some(newer)), Err(MetadataReadError::Unavailable));

    let mut lagging = ProfileLocalOperation::new(realm(), profile_id(), revision());
    lagging.first().unwrap();
    lagging
        .next(child(Ok::<_, LiveRecordsError>(Some(record()))))
        .unwrap();
    let raw = child(Ok::<_, RawReadError>(Some(view(Ulid::from_parts(8, 8)))));
    assert_eq!(lagging.next(raw), Err(MetadataReadError::Unavailable));
}

#[test]
fn offline_serves_local() {
    let mut export = ProfileExportOperation::new(None, realm(), profile_id(), revision());
    assert!(matches!(export.first(), Ok(Next::Call(IoCall::Child(_)))));
    let local = child(Ok::<_, MetadataReadError>(served()));
    assert_eq!(export.next(local), Ok(Next::Done(served())));
}

#[test]
fn missing_config_unavailable() {
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let mut export = ProfileExportOperation::new(Some(node), realm(), profile_id(), revision());
    export.first().unwrap();
    let config = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(export.next(config), Err(MetadataReadError::Unavailable));
}

#[test]
fn peer_answers_collect() {
    // Any holder serving the exact revision wins over a lagging holder's not-found.
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let peer = iroh::SecretKey::from_bytes(&[5; 32]).public();
    let mut export = ProfileExportOperation::new(Some(node), realm(), profile_id(), revision());
    export.stage = ExportStage::Peers;
    export.holder_count = 2;
    export
        .answers
        .push((node, Err(MetadataReadError::NotFound)));
    let answer = MetadataTransportMessage::ExportAnswer {
        result: Ok(Box::new(served())),
    };
    let answers = child(Ok::<_, FetchError>(vec![(peer, Ok::<_, String>(answer))]));
    assert_eq!(export.next(answers), Ok(Next::Done(served())));
    assert_eq!(
        export_answer(Err("no answer".to_string())),
        Err(MetadataReadError::Unavailable)
    );
}
