//! Holds the distributed query form and the pure merges of node query results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    HashSet, MAX_RESULT_BYTES, MetadataApiError, MetadataQueryResults, MetadataReadError,
    ObjectKeyMatch, ObjectQueryMode, QUERY_MAX_ROWS, RealmId, ReferenceNodeExecution,
    SearchNodePage, Span,
};

pub(super) fn object_search_fingerprint(
    realm_id: RealmId,
    query: &str,
    key_match: ObjectKeyMatch,
    bucket: Option<&str>,
    mode: ObjectQueryMode,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna.object.search.v1\0");
    hasher.update(realm_id.as_bytes());
    hasher.update(query.as_bytes());
    hasher.update(&[0]);
    hasher.update(&[match key_match {
        ObjectKeyMatch::Substring => 1,
        ObjectKeyMatch::Prefix => 2,
        ObjectKeyMatch::Metadata => 3,
    }]);
    match bucket {
        Some(bucket) => {
            hasher.update(&[1]);
            hasher.update(bucket.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[match mode {
        ObjectQueryMode::Local => 1,
        ObjectQueryMode::DistributedBestEffort => 2,
        ObjectQueryMode::DistributedStrict => 3,
    }]);
    *hasher.finalize().as_bytes()
}

pub(super) fn record_object_result(
    span: &Span,
    result: &Result<SearchNodePage, MetadataReadError>,
) {
    match result {
        Ok(page) => {
            span.record("result", "ok");
            span.record("hit_count", page.hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_preflight_node(
    span: &Span,
    result: &Result<ReferenceNodeExecution, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", "ok");
            span.record("hit_count", result.visible_references.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub fn aggregate_query_results(
    results: Vec<MetadataQueryResults>,
    query_form: MetadataQueryForm,
    select_limit: Option<usize>,
) -> Result<MetadataQueryResults, MetadataApiError> {
    match query_form {
        MetadataQueryForm::Ask => {
            Ok(MetadataQueryResults::Boolean(results.into_iter().any(
                |result| matches!(result, MetadataQueryResults::Boolean(true)),
            )))
        }
        MetadataQueryForm::Select => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            let mut merged_bytes = 32usize;
            let row_limit = select_limit.unwrap_or(QUERY_MAX_ROWS).min(QUERY_MAX_ROWS);
            if row_limit == 0 {
                return Ok(MetadataQueryResults::Solutions(Vec::new()));
            }
            for result in results {
                let MetadataQueryResults::Solutions(rows) = result else {
                    continue;
                };
                for row in rows {
                    let key = serde_json::to_string(&row)
                        .map_err(|err| MetadataApiError::Internal(err.to_string()))?;
                    if seen.insert(key) {
                        merged_bytes = merged_bytes.saturating_add(
                            serde_json::to_vec(&row)
                                .map_err(|err| MetadataApiError::Internal(err.to_string()))?
                                .len()
                                .saturating_add(1),
                        );
                        if merged_bytes > MAX_RESULT_BYTES {
                            return Err(MetadataApiError::BadRequest);
                        }
                        merged.push(row);
                        if merged.len() >= row_limit {
                            return Ok(MetadataQueryResults::Solutions(merged));
                        }
                    }
                }
            }
            Ok(MetadataQueryResults::Solutions(merged))
        }
    }
}

pub fn query_select_limit(query: &str) -> Option<usize> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = parsed else {
        return None;
    };
    let spargebra::algebra::GraphPattern::Slice { length, .. } = pattern else {
        return None;
    };
    length
}

pub fn query_form(query: &str) -> Option<MetadataQueryForm> {
    match spargebra::SparqlParser::new().parse_query(query).ok()? {
        spargebra::Query::Select { .. } => Some(MetadataQueryForm::Select),
        spargebra::Query::Ask { .. } => Some(MetadataQueryForm::Ask),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataQueryForm {
    Select,
    Ask,
}
