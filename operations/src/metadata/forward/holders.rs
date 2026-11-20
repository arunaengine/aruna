//! Says which nodes hold a metadata document's bucket and whether this node is one of them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::placement::{holds_placement, resolve_shard_holders};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use ulid::Ulid;

use crate::metadata::create_document::{CreateDocumentConfig, resolve_metadata_id};

/// Whether the origin currently holds a structured metadata document's bucket.
/// `None` when the config does not list the local node or the id has no placement.
pub fn origin_holds_document(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    local_node_id: NodeId,
    document_id: Ulid,
) -> Option<bool> {
    if !config.has_node(local_node_id) {
        return None;
    }
    let placement = resolve_metadata_id(config, realm_id, None, document_id).ok()?;
    Some(holds_placement(config, &placement, local_node_id))
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
