//! Decides whether a metadata write runs locally or goes to the holders of its placement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentConfig;
use crate::metadata::create_document::resolve_metadata_id;
use crate::placement::holds_placement;
use crate::placement::resolve_shard_holders;
use aruna_core::NodeId;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use std::sync::Arc;
use ulid::Ulid;

/// Where a metadata write must be applied: topic membership equals the bucket's
/// holder set, so a non-holder cannot publish and the mutation goes to a holder.
/// Widening membership to admit origins would dissolve sharding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataWriteRoute {
    Local,
    /// Holders of the document's bucket in rank order (rank-0 first).
    Forward(Vec<NodeId>),
}

/// Route for a write against `placement`, from the local node's point of view.
/// [`PlacementRef::NIL`] has no governing strategy, so the local node stays the
/// authority. A non-NIL placement without config fails closed as undeliverable.
pub fn write_route(
    config: Option<&RealmConfigDocument>,
    placement: &PlacementRef,
    local_node_id: NodeId,
) -> MetadataWriteRoute {
    let Some(config) = config else {
        return if *placement == PlacementRef::NIL {
            MetadataWriteRoute::Local
        } else {
            MetadataWriteRoute::Forward(Vec::new())
        };
    };
    if holds_placement(config, placement, local_node_id) {
        return MetadataWriteRoute::Local;
    }
    MetadataWriteRoute::Forward(resolve_shard_holders(config, placement))
}

/// Whether the local node is a device. Devices cache the realm's documents, so
/// their local checks are real, but they hold no metadata bucket: an effectful
/// write goes to an ingress, which stays the authority for what it applies.
pub async fn is_user_origin(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::realm::get_config::GetConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let node = config
        .nodes
        .into_iter()
        .find(|node| node.node_id == local_node_id.to_string())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    Ok(!node.kind.is_sync_eligible())
}

/// Whether the origin currently holds a structured metadata document's bucket.
pub async fn origin_holds_document(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Result<bool, MetadataApiError> {
    let config = drive(
        crate::realm::get_config::GetConfigOperation::new(realm_id),
        context.as_ref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if !config.has_node(local_node_id) {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = resolve_metadata_id(&config, realm_id, None, document_id)
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    Ok(holds_placement(&config, &placement, local_node_id))
}

pub(crate) fn holds_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> bool {
    resolve_metadata_id(config, realm_id, None, document_id)
        .is_ok_and(|placement| holds_placement(config, &placement, local_node_id))
}

/// Holders of the document's blind-hashed bucket: candidates for a create the
/// origin cannot place. Every candidate holds that one bucket and a forwarded
/// create stamps exactly it, so the answering candidate cannot change placement.
pub(crate) fn create_forward_holders(
    realm_config: &RealmConfigDocument,
    config: &CreateDocumentConfig,
    document_id: Ulid,
) -> Option<(PlacementRef, Vec<NodeId>)> {
    let placement = resolve_metadata_id(
        realm_config,
        config.actor.realm_id,
        Some(config.group_id),
        document_id,
    )
    .ok()?;
    let holders = resolve_shard_holders(realm_config, &placement);
    Some((placement, holders))
}

pub(crate) fn distinct_holders(holders: &[NodeId]) -> Vec<NodeId> {
    let mut distinct = Vec::with_capacity(holders.len());
    for holder in holders.iter().copied() {
        if !distinct.contains(&holder) {
            distinct.push(holder);
        }
    }
    distinct
}

pub(crate) fn holder_intersection(current: &[NodeId], frozen: &[NodeId]) -> Vec<NodeId> {
    let holders = current
        .iter()
        .copied()
        .filter(|holder| frozen.contains(holder))
        .collect::<Vec<_>>();
    distinct_holders(&holders)
}
