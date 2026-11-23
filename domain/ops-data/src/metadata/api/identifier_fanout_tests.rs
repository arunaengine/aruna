//! Drives the identifier lookup through the fanout and the merge of every node's matches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::secondary_id::{IdentifierOrigin, SecondaryIdKind};
use ulid::Ulid;

use super::super::MetadataFanoutStats;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn lookup() -> IdentifierFanoutOperation {
    let query = IdentifierQuery {
        kind: SecondaryIdKind::Doi,
        value: "10.1/x".to_string(),
        endpoint: None,
    };
    lookup_identifier_distributed((realm(), node(1)), (None, None), query)
}

fn found(seed: u64, origin: IdentifierOrigin) -> IdentifierMatch {
    IdentifierMatch {
        document_id: Ulid::from_parts(seed, 1),
        pid: None,
        origin,
    }
}

fn fanout(parts: Vec<(NodeId, Matches)>, nodes_failed: usize) -> IoResult {
    let stats = MetadataFanoutStats {
        nodes_failed,
        ..MetadataFanoutStats::default()
    };
    let answer: Result<FanoutParts<Matches>, MetadataApiError> = Ok((parts, stats));
    IoResult::Child(ChildOutcome::new(answer))
}

#[test]
fn matches_merge() {
    let mut lookup = lookup();
    assert!(matches!(lookup.first(), Ok(Next::Call(IoCall::Child(_)))));
    let parts = vec![
        (node(1), vec![found(1, IdentifierOrigin::Imported)]),
        (
            node(2),
            vec![
                found(1, IdentifierOrigin::Published),
                found(2, IdentifierOrigin::Imported),
            ],
        ),
    ];
    let Ok(Next::Done((matches, incomplete))) = lookup.next(fanout(parts, 0)) else {
        panic!("expected the merged matches");
    };
    assert_eq!(matches.len(), 2);
    assert!(!incomplete);
}

#[test]
fn failed_node_incomplete() {
    let mut lookup = lookup();
    lookup.first().unwrap();
    let Ok(Next::Done((matches, incomplete))) = lookup.next(fanout(Vec::new(), 1)) else {
        panic!("expected an incomplete answer");
    };
    assert!(matches.is_empty());
    assert!(incomplete);
}
