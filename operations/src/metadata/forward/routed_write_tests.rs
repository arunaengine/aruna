//! Drives the routed delete and the device edit forward through their answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::events::{MetadataAnswer, NetEvent};
use aruna_core::metadata::MetadataError;

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

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn answer(peer: NodeId, result: Result<MetadataTransportMessage, PeerRequestError>) -> IoResult {
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: result.map(|message| encode_message(&message).unwrap()),
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

fn delete() -> RouteDeleteOperation {
    RouteDeleteOperation::new((actor(), Some(node(1))), None, Ulid::from_parts(3, 3), None)
}

fn batch() -> ApplyBatchOperation {
    let batch = MetadataBatch {
        graph_iri: "urn:graph".to_string(),
        actor: [4; 32],
        counter: 1,
        base_clock: Default::default(),
        ops: Vec::new(),
        timestamp_millis: 1,
    };
    let authored = MetadataBatchSource::ReplaceRoCrate {
        jsonld: "{}".to_string(),
    };
    let token = AuthToken::bearer("token").unwrap();
    ApplyBatchOperation::new(
        (realm(), Ulid::from_parts(3, 3)),
        (Box::new(batch), authored),
        token,
    )
}

#[test]
fn delete_needs_config() {
    let mut delete = delete();
    assert!(matches!(delete.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert!(matches!(
        delete.next(missing),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}

#[test]
fn local_delete_uncaches() {
    let mut delete = delete();
    delete.stage = DeleteStage::Local;
    let uncache = MetadataEffect::UncacheRecord {
        document_id: Ulid::from_parts(3, 3),
    };
    assert_eq!(
        delete.next(child(Ok::<_, DeleteDocumentError>(()))),
        Ok(Next::Then {
            output: (),
            calls: vec![IoCall::Effect(Box::new(Effect::Metadata(uncache)))],
        })
    );
}

#[test]
fn forwarded_delete_confirmed() {
    let mut delete = delete();
    delete.stage = DeleteStage::Forward;
    let deleted = child(Ok::<_, HolderWriteError>(
        MetadataTransportMessage::ForwardedDelete,
    ));
    assert_eq!(delete.next(deleted), Ok(Next::Done(())));
    delete.stage = DeleteStage::Forward;
    let refused = child(Err::<MetadataTransportMessage, _>(
        HolderWriteError::Forbidden,
    ));
    assert_eq!(delete.next(refused), Err(MetadataWriteError::Forbidden));
}

#[test]
fn unsent_tries_next() {
    let mut apply = batch();
    apply.holders = VecDeque::from([node(3)]);
    apply.asked = Some(node(2));
    let unsent = PeerRequestError::NotSent(MetadataError::HandleMissing);
    let turn = apply.next(answer(node(2), Err(unsent)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(apply.asked, Some(node(3)));
}

#[test]
fn ambiguous_edit_stops() {
    let mut apply = batch();
    apply.holders = VecDeque::from([node(3)]);
    apply.asked = Some(node(2));
    let turn = apply.next(answer(node(2), Err(PeerRequestError::TimedOut)));
    assert!(matches!(turn, Err(MetadataWriteError::Undeliverable(_))));
}

#[test]
fn refused_edit_classified() {
    let mut apply = batch();
    apply.asked = Some(node(2));
    let refused = MetadataTransportMessage::ForwardedApplyBatch {
        result: Err(SyncRefusal::Forbidden),
    };
    let turn = apply.next(answer(node(2), Ok(refused)));
    assert_eq!(turn, Err(MetadataWriteError::Forbidden));
}

#[test]
fn no_holder_left() {
    let mut apply = batch();
    assert!(matches!(
        apply.ask(),
        Err(MetadataWriteError::Undeliverable(_))
    ));
}
