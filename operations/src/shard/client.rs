//! Asks a co-holder for its shard manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoResult, Next};
use aruna_core::document::ShardManifest;
use aruna_core::effects::ShardNetEffect;
use aruna_core::events::ShardNetEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;

use super::{ManifestError, shard_answer, shard_call};

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
