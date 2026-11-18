//! Drives the routed create through its local attempt and holder answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use crate::metadata::create_document::CreateDocumentPayload;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn route() -> RouteCreateOperation {
    let realm_id = RealmId::from_bytes([3; 32]);
    let create = CreateDocumentOperation::new(CreateDocumentConfig {
        actor: Actor {
            node_id: node(1),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        },
        group_id: Ulid::from_parts(2, 2),
        document_id: Ulid::from_parts(9, 9),
        document_path: "datasets/routed".to_string(),
        public: false,
        payload: CreateDocumentPayload::Scaffold {
            name: "Routed".to_string(),
            description: String::new(),
            date_published: "2026-01-01".to_string(),
            license: None,
        },
    });
    RouteCreateOperation::new(create, Some(node(1)), None)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn created() -> CreateDocumentResult {
    let document_id = Ulid::from_parts(9, 9);
    CreateDocumentResult {
        record: MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([3; 32]),
            group_id: Ulid::from_parts(2, 2),
            document_id,
            document_path: "datasets/routed".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: aruna_core::structs::placement::record::PlacementRef::NIL,
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(5, 5),
            last_event_id: Ulid::from_parts(5, 5),
        },
        event_id: Ulid::from_parts(5, 5),
    }
}

/// A route that has reached the holder forward, asking `holders` in order.
fn forwarding(holders: &[NodeId]) -> RouteCreateOperation {
    let mut route = route();
    route.stage = RouteStage::Forward;
    route.frame = vec![1, 2, 3];
    route.holders = holders.iter().copied().collect();
    route.asked = holders.len();
    route
}

fn answered(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        MetadataAnswer {
            peer,
            elapsed: Duration::ZERO,
            result,
        },
    )))))
}

fn reply(message: MetadataTransportMessage) -> Result<Vec<u8>, PeerRequestError> {
    Ok(encode_message(&message).unwrap())
}

#[test]
fn holder_creates_locally() {
    let mut route = route();
    assert!(matches!(route.first(), Ok(Next::Call(IoCall::Child(_)))));
    let done = child(Ok::<_, CreateDocumentError>(created()));
    assert_eq!(route.next(done), Ok(Next::Done(created())));
}

#[test]
fn non_holder_reads_config() {
    let mut route = route();
    route.first().unwrap();
    let refused = child(Err::<CreateDocumentResult, _>(
        CreateDocumentError::HoldsNoBucket,
    ));
    assert!(matches!(
        route.next(refused),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert!(matches!(
        route.next(missing),
        Err(MetadataWriteError::Undeliverable(_))
    ));

    let mut failed = self::route();
    failed.first().unwrap();
    let denied = child(Err::<CreateDocumentResult, _>(
        CreateDocumentError::RawLimit,
    ));
    assert_eq!(
        failed.next(denied),
        Err(MetadataWriteError::Create(CreateDocumentError::RawLimit))
    );
}

#[test]
fn unsent_tries_next() {
    // A request that never left this node asks the next holder; a failed one stops.
    let mut route = forwarding(&[node(2), node(3)]);
    let unsent = answered(
        node(2),
        Err(PeerRequestError::NotSent(MetadataError::Backend(
            "no stream".into(),
        ))),
    );
    let Ok(Next::Call(IoCall::Effect(next))) = route.next(unsent) else {
        panic!("expected the next holder");
    };
    assert!(matches!(
        *next,
        Effect::Net(NetEffect::MetadataRequest(ref request)) if request.peer == node(3)
    ));
    let failed = answered(
        node(3),
        Err(PeerRequestError::Failed(MetadataError::Backend(
            "reset".into(),
        ))),
    );
    let Err(MetadataWriteError::Undeliverable(detail)) = route.next(failed) else {
        panic!("expected the terminal refusal");
    };
    assert!(detail.contains("refusing to replay"));
}

#[test]
fn unavailable_holders_exhaust() {
    let mut route = forwarding(&[node(2)]);
    let unavailable = answered(
        node(2),
        reply(MetadataTransportMessage::ForwardedWriteUnavailable),
    );
    let Err(MetadataWriteError::Undeliverable(detail)) = route.next(unavailable) else {
        panic!("expected the undeliverable create");
    };
    assert!(detail.contains("placement view is unavailable"));
    assert!(matches!(
        forwarding(&[]).ask_next(),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}

#[test]
fn denial_and_findings_map() {
    let mut route = forwarding(&[node(2)]);
    let denied = reply(MetadataTransportMessage::ForwardedWriteDenied {
        error: WriteAuthError::Forbidden,
    });
    assert_eq!(
        route.next(answered(node(2), denied)),
        Err(MetadataWriteError::Forbidden)
    );
    let mut route = forwarding(&[node(2)]);
    let findings = reply(MetadataTransportMessage::ForwardedProfileValidation {
        findings: Vec::new(),
    });
    assert_eq!(
        route.next(answered(node(2), findings)),
        Err(
            CreateDocumentError::MetadataError(MetadataError::ProfileValidation(Vec::new())).into()
        )
    );
}

#[test]
fn foreign_record_refused() {
    let mut route = forwarding(&[node(2)]);
    route.placement = Some(PlacementRef::NIL);
    route.document_id = Ulid::from_parts(9, 9);
    let mut other = created().record;
    other.document_id = Ulid::from_parts(8, 8);
    let record = reply(MetadataTransportMessage::ForwardedRecord {
        record: Box::new(other),
    });
    assert!(matches!(
        route.next(answered(node(2), record)),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}
