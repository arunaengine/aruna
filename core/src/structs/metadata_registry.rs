use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::RealmId;
use crate::types::{GroupId, UserId};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRegistryRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub permission_path: String,
    pub holder_node_ids: Vec<NodeId>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl MetadataRegistryRecord {
    pub fn graph_iri_for(document_id: Ulid) -> String {
        format!("https://w3id.org/aruna/{document_id}")
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
