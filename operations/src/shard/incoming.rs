//! Decides what a peer asking for a shard manifest gets: realm, trust and holder checks first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::ShardManifest;
use aruna_core::operation::Operation;
use aruna_core::placement::{bucket_membership, resolve_shard_holders};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;

use super::protocol::ShardTransportMessage;
use super::{AssembleManifestOperation, ManifestError};
use crate::document_repository::{GetConfigError, GetConfigOperation};

/// What a manifest request is answered with.
#[derive(Debug, PartialEq)]
pub(crate) enum ManifestAnswer {
    Reject(String),
    Manifest(Box<ShardManifest>),
}

fn reject(reason: String) -> Result<Next<ManifestAnswer>, IoError> {
    Ok(Next::Done(ManifestAnswer::Reject(reason)))
}

/// Serves one manifest request after checking the realm, the peer and this node's holdership.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeManifestOperation {
    local: (NodeId, RealmId),
    peer: NodeId,
    realm_id: RealmId,
    placement: PlacementRef,
    now_ms: u64,
    assembling: bool,
}

impl ServeManifestOperation {
    pub(crate) fn new(
        local: (NodeId, RealmId),
        peer: NodeId,
        message: ShardTransportMessage,
    ) -> Self {
        let ShardTransportMessage::ManifestRequest {
            realm_id,
            placement,
        } = message;
        Self {
            local,
            peer,
            realm_id,
            placement,
            now_ms: 0,
            assembling: false,
        }
    }

    fn check(&mut self, config: &RealmConfigDocument) -> Result<Next<ManifestAnswer>, IoError> {
        let (peer, realm_id, placement) = (self.peer, self.realm_id, self.placement);
        // Trust gate: only sync-eligible (server-class) realm nodes may fetch a
        // manifest, mirroring the notification/metadata peer checks.
        match config.sync_eligible_nodes() {
            Ok(eligible) if eligible.contains(&peer) => {}
            Ok(_) => {
                return reject(format!(
                    "shard peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
                ));
            }
            Err(error) => return reject(error.to_string()),
        }
        // Only a current holder can answer authoritatively for a shard.
        if !resolve_shard_holders(config, &placement).contains(&self.local.0) {
            return reject(format!(
                "node does not hold shard {}/{} in realm `{realm_id}`",
                placement.strategy_id, placement.shard
            ));
        }
        // A transition target is not a holder yet and still has to verify the copy
        // it pulled against this holder, so the derived membership decides here.
        let membership = bucket_membership(config, &placement, self.now_ms);
        if !membership.members.contains(&peer) {
            return reject(format!(
                "shard peer `{peer}` is not a holder for shard {}/{} in realm `{realm_id}`",
                placement.strategy_id, placement.shard
            ));
        }
        self.assembling = true;
        let assemble = AssembleManifestOperation::new(realm_id, placement);
        Ok(Next::Call(IoCall::child(assemble)))
    }
}

impl Operation for ServeManifestOperation {
    type Output = ManifestAnswer;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ManifestAnswer>, IoError> {
        let (peer, realm_id) = (self.peer, self.realm_id);
        if realm_id != self.local.1 {
            return reject(format!(
                "shard peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ManifestAnswer>, IoError> {
        if self.assembling {
            return Ok(Next::Done(
                match result.child::<ShardManifest, ManifestError>()? {
                    Ok(manifest) => ManifestAnswer::Manifest(Box::new(manifest)),
                    Err(ManifestError(error)) => ManifestAnswer::Reject(error),
                },
            ));
        }
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => self.check(&config),
            Err(GetConfigError::DocumentNotFound) => {
                reject(format!("realm `{}` config unavailable", self.realm_id))
            }
            Err(error) => reject(error.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "incoming_tests.rs"]
mod tests;
