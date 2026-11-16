//! Holds the reference preflight request, target and coverage types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    ARUNA_DATA_PREFIX, ApiQueryMode, AuthContext, ConversionError, Deserialize, HashSet, NodeId,
    RealmConfigDocument, Serialize, Ulid,
};

pub(super) fn preflight_fingerprint(
    targets: &[MetadataResolvedTarget],
    mode: Option<ApiQueryMode>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"metadata-reference-preflight\0");
    hasher.update(&[match mode {
        None => 0,
        Some(ApiQueryMode::Local) => 1,
        Some(ApiQueryMode::Distributed) => 2,
    }]);
    hasher.update(
        &postcard::to_allocvec(targets).expect("preflight cursor fingerprint payload serializes"),
    );
    *hasher.finalize().as_bytes()
}

#[derive(Debug, Clone)]
pub enum ReferenceTarget {
    ContentW3ids {
        content_w3ids: Vec<String>,
        remove_resolvable_locations: bool,
    },
    BucketPrefix {
        bucket: String,
        prefix: Option<String>,
        operation: MetadataStorageOperation,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataStorageOperation {
    #[default]
    LatestVersionTombstone,
    AllVersionsPurge,
}

#[derive(Debug, Clone)]
pub struct ReferenceRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub target: ReferenceTarget,
    pub s3_endpoint: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<ApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightLocation {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataResolvedTarget {
    /// The content w3id, or the `s3://` location IRI of a location target; it keys the target.
    pub content_w3id: String,
    /// All zeros for a location target; a node of an earlier build then finds no copy by it and
    /// still looks up the target's IRIs.
    pub content_hash: [u8; 32],
    pub queried_iris: Vec<String>,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub removed_locations: Vec<MetadataPreflightLocation>,
    #[serde(rename = "remove_all_resolvable_locations")]
    pub remove_resolvable_locations: bool,
}

impl MetadataResolvedTarget {
    /// The location IRI of a target named by its location, as its bytes were never read.
    pub fn location(&self) -> Option<&str> {
        (!self.content_w3id.starts_with(ARUNA_DATA_PREFIX)).then_some(self.content_w3id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeRequest {
    pub targets: Vec<MetadataResolvedTarget>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataVisibleReference {
    pub content_w3id: String,
    pub document_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataIndexState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataNodeFreshness {
    pub node_id: NodeId,
    pub index_state: MetadataIndexState,
    #[serde(rename = "oldest_status_updated_at_ms")]
    pub oldest_status_updated: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeTarget {
    pub content_w3id: String,
    pub hidden_references_exist: bool,
    pub resolvable_location_found: bool,
    #[serde(rename = "resolvable_location_after_operation")]
    pub location_after_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeExecution {
    pub visible_references: Vec<MetadataVisibleReference>,
    pub targets: Vec<ReferenceNodeTarget>,
    pub freshness: MetadataNodeFreshness,
    #[serde(rename = "path_style_endpoint_available")]
    pub path_style_available: bool,
    pub saturated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceTargetExecution {
    /// The content identity of stored bytes; `None` for a location target.
    pub content_w3id: Option<String>,
    /// The `s3://` location of reference or pending versions, whose bytes were never read.
    pub location_iri: Option<String>,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub visible_references: Vec<MetadataVisibleReference>,
    pub hidden_references_exist: bool,
    pub would_remove_location: bool,
    pub location_impact_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExcludedForm {
    pub form: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceCoverage {
    pub queried_scope: &'static str,
    pub queried_forms: Vec<&'static str>,
    pub excluded_forms: Vec<MetadataExcludedForm>,
    pub node_freshness: Vec<MetadataNodeFreshness>,
    pub target_resolution_complete: bool,
    pub path_style_complete: bool,
    pub realm_coverage_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceExecution {
    pub targets: Vec<ReferenceTargetExecution>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub complete: bool,
    pub failed_partitions: Vec<NodeId>,
    pub coverage: ReferenceCoverage,
}

pub(super) fn authorized_realm_nodes(
    config: &RealmConfigDocument,
    nodes: HashSet<NodeId>,
) -> Result<HashSet<NodeId>, ConversionError> {
    let authorized = config
        .sync_eligible_nodes()?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(nodes
        .into_iter()
        .filter(|node_id| authorized.contains(node_id))
        .collect())
}
