//! Tests how a metadata write walks its holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::peers::{failed_answer, peer_answer};
use aruna_core::calls::IoCall;
use aruna_core::effects::{Effect, NetEffect};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn delete() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardDeleteDocument {
        auth_token: None,
        config_digest: [0; 32],
        document_id: Ulid::from_bytes([1; 16]),
    }
}

fn create_like() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardCreatePolicy {
        auth_token: None,
        policy: Box::new(
            aruna_core::structs::placement::policy::PlacementPolicy::new(
                Ulid::from_bytes([2; 16]),
                "rule".to_string(),
                vec![aruna_core::structs::placement::policy::PlacementSelector {
                    node_id: None,
                    location: Some("eu".to_string()),
                    labels: Vec::new(),
                    executor_kind: None,
                }],
            )
            .unwrap(),
        ),
        created_at_ms: 1,
    }
}

fn asked(turn: HolderTurn) -> NodeId {
    let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
        panic!("expected a holder request");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
        panic!("expected a metadata request");
    };
    request.peer
}

fn walk(holders: &[NodeId], message: &MetadataTransportMessage) -> ForwardHoldersOperation {
    ForwardHoldersOperation::new(Some(node(9)), holders, message, None, false)
}

#[test]
fn skips_local_duplicates() {
    // This node and repeated holders are not asked.
    let holders = [node(9), node(1), node(1), node(2)];
    let mut operation = walk(&holders, &delete());
    assert_eq!(asked(operation.first()), node(1));
    let unavailable = MetadataTransportMessage::ForwardedWriteUnavailable;
    assert_eq!(
        asked(operation.next(peer_answer(node(1), &unavailable))),
        node(2)
    );
    let done = MetadataTransportMessage::ForwardedDelete;
    assert_eq!(
        operation.next(peer_answer(node(2), &done)),
        Ok(Next::Done(done))
    );
}

#[test]
fn denial_ends_walk() {
    let denied = |error| MetadataTransportMessage::ForwardedWriteDenied { error };
    for (error, expected) in [
        (WriteAuthError::Unauthorized, HolderWriteError::Unauthorized),
        (WriteAuthError::Forbidden, HolderWriteError::Forbidden),
    ] {
        let mut operation = walk(&[node(1), node(2)], &delete());
        operation.first().unwrap();
        let answer = peer_answer(node(1), &denied(error));
        assert_eq!(operation.next(answer), Err(expected));
    }
}

#[test]
fn every_holder_missed() {
    // A local miss counts as one of the holders that does not have the document.
    let holders = [node(9), node(1)];
    let message = delete();
    let mut operation =
        ForwardHoldersOperation::new(None, &holders, &message, Some(node(9)), false);
    assert_eq!(asked(operation.first()), node(1));
    let miss = peer_answer(node(1), &MetadataTransportMessage::WriteNotFound);
    assert_eq!(operation.next(miss), Err(HolderWriteError::NotFound));
}

#[test]
fn create_miss_undeliverable() {
    // A create has no "not found": the miss is one holder's failure.
    let mut operation = walk(&[node(1)], &create_like());
    operation.first().unwrap();
    let miss = peer_answer(node(1), &MetadataTransportMessage::WriteNotFound);
    assert!(matches!(
        operation.next(miss),
        Err(HolderWriteError::Undeliverable(detail)) if detail.contains("not found")
    ));
}

#[test]
fn unsent_tries_next() {
    let mut operation = walk(&[node(1), node(2)], &delete());
    operation.first().unwrap();
    let unsent = failed_answer(node(1), PeerRequestError::NotSent(aruna_core::metadata::MetadataError::Backend(
            "no route".to_string(),
        )));
    assert_eq!(asked(operation.next(unsent)), node(2));
}

#[test]
fn possible_send_stops() {
    // A holder that may have applied the write is never followed by a replay.
    for error in [
        PeerRequestError::Failed(aruna_core::metadata::MetadataError::Backend(
            "reset".to_string(),
        )),
        PeerRequestError::TimedOut,
    ] {
        let mut operation = walk(&[node(1), node(2)], &delete());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(failed_answer(node(1), error)),
            Err(HolderWriteError::Undeliverable(detail)) if detail.contains("refusing to replay")
        ));
        assert!(!operation.remaining.is_empty());
    }
}

#[test]
fn rejection_stops() {
    let mut operation = walk(&[node(1), node(2)], &delete());
    operation.first().unwrap();
    let rejected = peer_answer(
        node(1),
        &MetadataTransportMessage::Reject("bad".to_string()),
    );
    assert!(matches!(
        operation.next(rejected),
        Err(HolderWriteError::Undeliverable(detail)) if detail.contains("rejected")
    ));
}

#[test]
fn no_holder_undeliverable() {
    let mut operation = walk(&[], &delete());
    assert_eq!(
        operation.first(),
        Err(HolderWriteError::Undeliverable(
            "the document's bucket has no reachable holder".to_string()
        ))
    );
}
