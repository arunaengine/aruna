//! Holds the registry row of a metadata document: path claim, placement and holder nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::types::GroupId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRegistryRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub permission_path: String,
    /// Bucket chosen by the create-receiving node from the buckets it holds. Recorded once, never
    /// re-derived: re-choosing under a changed config would fork the document across two sync topics.
    /// Holders stay derived from `(placement, config)`, so a rebalance moves buckets, not documents.
    pub placement: PlacementRef,
    pub holder_node_ids: Vec<NodeId>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    /// Immutable event that established this document's path claim.
    pub establishing_event_id: Ulid,
    pub last_event_id: Ulid,
}

impl MetadataRegistryRecord {
    pub fn graph_iri_for(document_id: Ulid) -> String {
        format!("https://w3id.org/aruna/{document_id}")
    }

    /// The document whose graph IRI [`Self::graph_iri_for`] gives `iri`.
    pub fn graph_document(iri: &str) -> Option<Ulid> {
        let id = iri.strip_prefix("https://w3id.org/aruna/")?;
        Ulid::from_string(id).ok()
    }

    pub fn normalize_document_path(path: &str) -> String {
        path.trim().trim_matches('/').to_string()
    }

    pub fn permission_path_for(
        realm_id: &RealmId,
        group_id: GroupId,
        path: &str,
        document_id: Ulid,
    ) -> String {
        format!(
            "/{realm_id}/g/{group_id}/meta/{}@{document_id}",
            Self::normalize_document_path(path)
        )
    }

    /// Whether both records describe the same document claim: realm, group, document, path,
    /// graph, permission path, placement, creation and establishing event.
    pub fn same_identity(&self, other: &Self) -> bool {
        self.realm_id == other.realm_id
            && self.group_id == other.group_id
            && self.document_id == other.document_id
            && self.document_path == other.document_path
            && self.graph_iri == other.graph_iri
            && self.permission_path == other.permission_path
            && self.placement == other.placement
            && self.created_at_ms == other.created_at_ms
            && self.establishing_event_id == other.establishing_event_id
    }

    /// Whether the path, graph and permission path are the ones derived from the identity.
    pub fn identity_valid(&self) -> bool {
        let normalized_path = Self::normalize_document_path(&self.document_path);
        self.establishing_event_id != Ulid::nil()
            && self.document_path == normalized_path
            && self.graph_iri == Self::graph_iri_for(self.document_id)
            && self.permission_path
                == Self::permission_path_for(
                    &self.realm_id,
                    self.group_id,
                    &normalized_path,
                    self.document_id,
                )
    }

    /// The order of record versions: a later update, then a later event id, is newer.
    pub fn freshness(&self) -> (u64, Ulid) {
        (self.updated_at_ms, self.last_event_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataAuditOperation {
    Create,
    ReplaceRoCrate,
    UpsertDataEntity,
    UpsertContextualEntity,
    Delete,
    SetVisibility,
    PlaceReplicas,
    WithdrawPersistentId,
    Checkpoint,
    RemoveDataEntity,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataAuditRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub graph_iri: String,
    pub user_id: UserId,
    pub node_id: NodeId,
    pub operation: MetadataAuditOperation,
    pub occurred_at_ms: u64,
    pub details: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_names_document() {
        let document_id = Ulid::from_bytes([3; 16]);
        let iri = MetadataRegistryRecord::graph_iri_for(document_id);
        assert_eq!(
            MetadataRegistryRecord::graph_document(&iri),
            Some(document_id)
        );
        assert_eq!(
            MetadataRegistryRecord::graph_document("https://w3id.org/aruna/data/abc"),
            None
        );
        assert_eq!(
            MetadataRegistryRecord::graph_document("https://example.org/x"),
            None
        );
    }
}
