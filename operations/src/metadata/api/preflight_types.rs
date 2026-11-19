//! Holds the reference preflight request, target and coverage types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::hidden_token;
use super::{
    ApiQueryMode, AuthContext, ConversionError, Deserialize, HashSet, NodeId, RealmConfigDocument,
    Serialize,
};
use aruna_core::metadata_api::{
    MetadataNodeFreshness, MetadataPreflightLocation, MetadataResolvedTarget,
    MetadataVisibleReference,
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Clone, PartialEq)]
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

/// Shows whether a bearer is forwarded, never its value.
impl std::fmt::Debug for ReferenceRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceRequest")
            .field("auth", &self.auth)
            .field("bearer_token", &hidden_token(&self.bearer_token))
            .field("target", &self.target)
            .field("limit", &self.limit)
            .field("cursor", &self.cursor)
            .field("mode", &self.mode)
            .field("target_nodes", &self.target_nodes)
            .field("allow_partial", &self.allow_partial)
            .finish()
    }
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
