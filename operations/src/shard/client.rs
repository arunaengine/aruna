//! Asks a co-holder for its shard manifest and checks that the answer is the one requested.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoResult, Next};
use aruna_core::document::ShardManifest;
use aruna_core::effects::ShardNetEffect;
use aruna_core::events::ShardNetEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;

use super::{ManifestError, shard_answer, shard_call};

pub const SHARD_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetches a co-holder's paged manifest for one shard. A `Reject` from the co-holder
/// (foreign realm, untrusted peer, or a shard it does not hold) surfaces as an error.
#[derive(Debug, PartialEq)]
pub struct FetchManifestOperation {
    peer: NodeId,
    realm_id: RealmId,
    placement: PlacementRef,
}

impl FetchManifestOperation {
    pub fn new(peer: NodeId, realm_id: RealmId, placement: PlacementRef) -> Self {
        Self {
            peer,
            realm_id,
            placement,
        }
    }
}

impl Operation for FetchManifestOperation {
    type Output = ShardManifest;
    type Error = ManifestError;

    fn first(&mut self) -> Result<Next<ShardManifest>, ManifestError> {
        Ok(Next::Call(shard_call(ShardNetEffect::FetchManifest {
            peer: self.peer,
            realm_id: self.realm_id,
            placement: self.placement,
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ShardManifest>, ManifestError> {
        match shard_answer(result)? {
            ShardNetEvent::Manifest(manifest) => Ok(Next::Done(*manifest.map_err(ManifestError)?)),
            ShardNetEvent::NoNetwork => {
                Err(ManifestError("shard fetch needs a net handle".to_string()))
            }
            other => Err(ManifestError(format!("unexpected shard answer {other:?}"))),
        }
    }
}

pub(crate) fn validate_manifest_response(
    manifest: ShardManifest,
    node_id: NodeId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    if manifest.placement != placement {
        return Err(format!(
            "shard manifest placement mismatch: requested {placement:?}, received {:?}",
            manifest.placement
        ));
    }
    if manifest.holder != node_id {
        return Err(format!(
            "shard manifest holder mismatch: requested {node_id}, received {}",
            manifest.holder
        ));
    }
    Ok(manifest)
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9; 16]),
            shard,
        }
    }

    fn manifest(holder: NodeId, placement: PlacementRef) -> ShardManifest {
        ShardManifest {
            placement,
            holder,
            entries: Vec::new(),
            cursor: Vec::new(),
            digest: [0; 32],
            updated_at_ms: 0,
        }
    }

    #[test]
    fn response_rejects_placement() {
        let holder = node_id(1);
        let requested = placement(7);
        let received = manifest(holder, placement(8));

        let error = validate_manifest_response(received, holder, requested)
            .expect_err("wrong placement must be rejected");

        assert!(
            error.contains("placement mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn response_rejects_holder() {
        let requested_holder = node_id(1);
        let placement = placement(7);
        let received = manifest(node_id(2), placement);

        let error = validate_manifest_response(received, requested_holder, placement)
            .expect_err("wrong holder must be rejected");

        assert!(
            error.contains("holder mismatch"),
            "unexpected error: {error}"
        );
    }
}
