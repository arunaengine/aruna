//! The node that runs an install: a server or management node that is not draining and holds
//! the bucket of the group's root metadata path. It is picked once, when the record is created.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::document::DocumentTarget;
use aruna_core::id::NodeId;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::{PlacementRef, shard_for_subject};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use ulid::Ulid;

use crate::realm::mutate_placement::node_kind;
use aruna_core::placement::resolver::{
    PlacementResolutionContext, meta_bucket_subject, strategy_for_target,
};
use aruna_core::placement::try_resolve_holders;

/// The first eligible holder of the group's root metadata bucket, in holder order. A plugin
/// with tasks passes the nodes that advertise an executor: then an eligible holder with one
/// comes first, else any eligible node with one in node id order, else a holder as before.
pub(crate) fn pick_receiver(
    config: &RealmConfigDocument,
    (realm_id, group_id): (RealmId, GroupId),
    executors: &BTreeSet<NodeId>,
) -> Option<NodeId> {
    let holders = metadata_holders(config, realm_id, group_id).unwrap_or_default();
    let eligible = |node: &&NodeId| eligible(config, **node);
    let executing = |node: &&NodeId| executors.contains(*node);
    (holders.iter().filter(eligible).find(executing))
        .or_else(|| executors.iter().find(eligible))
        .or_else(|| holders.iter().find(eligible))
        .copied()
}

fn metadata_holders(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
) -> Option<Vec<NodeId>> {
    let path = MetadataRegistryRecord::normalize_document_path("/");
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: Ulid::nil(),
    };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: Some(&path),
    };
    let (strategy, _) = strategy_for_target(config, &target, context)?;
    let subject = meta_bucket_subject(realm_id, group_id, &path);
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: shard_for_subject(&subject, strategy.shard_count),
    };
    try_resolve_holders(config, &placement).ok()
}

/// Whether `node` is a server or management node of the realm that is not draining.
pub fn eligible(config: &RealmConfigDocument, node: NodeId) -> bool {
    lost_reason(config, node).is_none()
}

/// Why `node` can no longer run installs: it left the realm, is no longer a server or
/// management node, or is draining. Its installs show as blocked; nothing moves them.
pub fn lost_reason(config: &RealmConfigDocument, node: NodeId) -> Option<&'static str> {
    match node_kind(config, node) {
        None => Some("the node that runs it left the realm"),
        Some(RealmNodeKind::User { .. }) => Some("the node that runs it is no longer a server"),
        Some(_)
            if config
                .placement_entry(node)
                .is_some_and(|entry| entry.draining) =>
        {
            Some("the node that runs it is draining")
        }
        Some(_) => None,
    }
}
