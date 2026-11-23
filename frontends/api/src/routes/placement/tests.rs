//! Tests that policy reference and selector bodies round trip and reject short digests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{PolicyRefBody, SelectorBody};
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PlacementSelector};
use ulid::Ulid;

#[test]
fn ref_round_trips() {
    // A ref must survive the transport form exactly: a truncated digest
    // would silently name another definition.
    let policy_ref = PlacementPolicyRef {
        policy_id: Ulid::from_bytes([5u8; 16]),
        digest: [7u8; 32],
    };
    let body: PolicyRefBody = policy_ref.into();
    assert_eq!(body.digest.len(), 64);
    assert_eq!(
        PlacementPolicyRef::try_from(body).expect("ref parses"),
        policy_ref
    );
}

#[test]
fn rejects_short_digest() {
    let body = PolicyRefBody {
        policy_id: Ulid::from_bytes([5u8; 16]).to_string(),
        digest: "00".to_string(),
        name: None,
        owner_group_id: None,
    };
    assert!(PlacementPolicyRef::try_from(body).is_err());
}

#[test]
fn selector_round_trips() {
    let selector = PlacementSelector {
        node_id: None,
        location: Some("eu-west".to_string()),
        labels: Vec::new(),
        executor_kind: None,
    };
    let body: SelectorBody = selector.clone().into();
    assert_eq!(
        PlacementSelector::try_from(body).expect("selector parses"),
        selector
    );
}
