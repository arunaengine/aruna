//! Tests fetching a co-holder's shard manifest as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{node, shard_effect, shard_event};
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_bytes([9; 16]),
        shard: 0,
    }
}

fn fetching() -> FetchManifestOperation {
    let mut operation = FetchManifestOperation::new(node(2), realm(), placement());
    assert_eq!(
        shard_effect(operation.first()),
        ShardNetEffect::FetchManifest {
            peer: node(2),
            realm_id: realm(),
            placement: placement(),
        }
    );
    operation
}

#[test]
fn manifest_returned() {
    let manifest = ShardManifest {
        placement: placement(),
        holder: node(2),
        entries: Vec::new(),
        cursor: Vec::new(),
        digest: [1; 32],
        updated_at_ms: 3,
    };
    let answer = ShardNetEvent::Manifest(Ok(Box::new(manifest.clone())));
    assert_eq!(
        fetching().next(shard_event(answer)),
        Ok(Next::Done(manifest))
    );
}

#[test]
fn peer_rejection_kept() {
    let answer = ShardNetEvent::Manifest(Err("foreign realm".to_string()));
    assert_eq!(
        fetching().next(shard_event(answer)),
        Err(ManifestError("foreign realm".to_string()))
    );
}

#[test]
fn network_required() {
    assert!(
        fetching()
            .next(shard_event(ShardNetEvent::NoNetwork))
            .is_err()
    );
}
