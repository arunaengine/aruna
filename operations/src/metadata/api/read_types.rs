//! Holds the metadata query and reference request and result types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{AuthContext, MetadataFanoutStats, MetadataQueryResults, NodeId, Permission, Ulid};

/// The canonical `metadata.read` policy request for one record path and caller,
/// shared by the single-record and bulk visibility seams.
pub(crate) fn metadata_read_request(
    permission_path: &str,
    auth: Option<&AuthContext>,
) -> aruna_core::request_policy::PolicyRequest {
    crate::auth::request_policy::policy_request_with(
        permission_path,
        &Permission::READ,
        auth,
        crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentQueryRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub query: String,
    pub mode: Option<ApiQueryMode>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub mode: Option<ApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataQueryExecution {
    pub results: MetadataQueryResults,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataReferencesRequest {
    pub auth: Option<AuthContext>,
    pub iri: String,
    pub predicate: Option<String>,
    pub limit: Option<usize>,
    pub resolve: bool,
    /// Documents after this id; pass the previous page's `next_after`.
    pub after: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataReferenceEntry {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub predicate: Option<String>,
    pub subject_iris: Vec<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetadataReferencesExecution {
    pub references: Vec<MetadataReferenceEntry>,
    /// The last document read when more may follow; `None` on the last page.
    pub next_after: Option<Ulid>,
}
