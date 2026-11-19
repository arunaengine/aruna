//! Checks the mode and form of metadata queries and opens the span of each node a fanout asks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::local_query::QUERY_MAX_BYTES;
use super::fanout_types::{MetadataFanoutOperation, pattern_contains_service};
use super::{ApiQueryMode, MetadataApiError, NodeId, QUERY_MAX_ROWS, Span, debug_span, field};

pub(super) fn ensure_query_mode(mode: &Option<ApiQueryMode>) {
    match mode {
        None | Some(ApiQueryMode::Local) | Some(ApiQueryMode::Distributed) => {}
    }
}

pub(super) fn ensure_query_form(query: &str) -> Result<(), MetadataApiError> {
    if query.len() > QUERY_MAX_BYTES {
        return Err(MetadataApiError::BadRequest);
    }
    let parsed = spargebra::SparqlParser::new()
        .parse_query(query)
        .map_err(|_| MetadataApiError::BadRequest)?;
    let pattern = match &parsed {
        spargebra::Query::Select { pattern, .. } | spargebra::Query::Ask { pattern, .. } => pattern,
        _ => return Err(MetadataApiError::BadRequest),
    };
    if pattern_contains_service(pattern) {
        return Err(MetadataApiError::BadRequest);
    }
    if matches!(
        pattern,
        spargebra::algebra::GraphPattern::Slice {
            length: Some(length),
            ..
        } if *length > QUERY_MAX_ROWS
    ) {
        return Err(MetadataApiError::BadRequest);
    }
    Ok(())
}

pub(super) fn fanout_node_span(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
) -> Span {
    match operation {
        MetadataFanoutOperation::Query => debug_span!(
            "metadata.operation.query_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::Search => debug_span!(
            "metadata.operation.search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::BucketSearch => debug_span!(
            "metadata.operation.bucket_search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::ObjectSearch => debug_span!(
            "metadata.operation.object_search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::ReferencePreflight => debug_span!(
            "metadata.operation.reference_preflight_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::IdentifierLookup => debug_span!(
            "metadata.operation.identifier_lookup_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
    }
}
