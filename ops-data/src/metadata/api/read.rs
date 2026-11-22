//! Builds a caller's metadata query and maps live record failures to API errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::query_fanout::DistributedQueryOperation;
use super::read_types::MetadataQueryRequest;
use super::{MetadataApiError, MetadataFanoutScope, NodeId, RealmId, query_fingerprint};
use crate::metadata::live_records::LiveRecordsError;

/// The query a caller asked for, across the realm or on this node as its mode says.
pub fn query_metadata(
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataQueryRequest,
) -> DistributedQueryOperation {
    let subject = query_fingerprint(
        &request.query,
        request.graph_iris.as_deref(),
        request.mode,
        None,
        None,
    );
    let scope = MetadataFanoutScope::new(request.mode, request.target_nodes, request.allow_partial)
        .with_subject(subject);
    DistributedQueryOperation::request(
        (realm_id, local_node_id),
        (request.auth, request.bearer_token),
        (request.graph_iris, request.query),
        scope,
    )
}

impl From<LiveRecordsError> for MetadataApiError {
    fn from(error: LiveRecordsError) -> Self {
        match error {
            LiveRecordsError::TooMany => Self::ServiceUnavailable,
            other => Self::Internal(other.to_string()),
        }
    }
}
