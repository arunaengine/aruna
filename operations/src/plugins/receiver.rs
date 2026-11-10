//! The node that runs an install: a server or management node that is not draining and holds
//! the bucket of the group's root metadata path. It is picked once, when the record is created.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::{PlacementRef, shard_for_subject};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use ulid::Ulid;

use crate::placement::{
    PlacementResolutionContext, meta_bucket_subject, strategy_for_target, try_resolve_holders,
};
use crate::realm::mutate_placement::node_kind;

/// The first eligible holder of the group's root metadata bucket, in holder order.
pub fn pick_receiver(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
) -> Option<NodeId> {
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
    let holders = try_resolve_holders(config, &placement).ok()?;
    holders.into_iter().find(|node| eligible(config, *node))
}

/// Whether `node` is a server or management node of the realm that is not draining.
pub fn eligible(config: &RealmConfigDocument, node: NodeId) -> bool {
    let infrastructure = matches!(
        node_kind(config, node),
        Some(RealmNodeKind::Server | RealmNodeKind::Management)
    );
    let draining = config
        .placement_entry(node)
        .is_some_and(|entry| entry.draining);
    infrastructure && !draining
}
