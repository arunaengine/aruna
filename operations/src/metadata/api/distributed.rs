//! Notes node results of metadata fanouts on their spans and plans ordered partition queries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::local_query::QUERY_MAX_BYTES;
use super::distributed_types::query_select_limit;
use super::{
    BucketPage, MetadataApiError, MetadataQueryResults, MetadataReadError, MetadataSearchHit,
    QUERY_MAX_ROWS, Span,
};

pub(super) fn record_bucket_result(span: &Span, result: &Result<BucketPage, MetadataReadError>) {
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

pub(super) fn record_query_result(
    span: &Span,
    result: &Result<MetadataQueryResults, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_search_node(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataReadError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

/// Returns the query each partition runs, ordered by its `ORDER BY` keys, every other projected
/// variable, then each variable's term parts, so each `LIMIT` cuts a total order.
pub(super) fn ordered_query(query: &str) -> Result<Option<String>, MetadataApiError> {
    use spargebra::algebra::{Expression, Function, GraphPattern, OrderExpression};

    let Ok(mut parsed) = spargebra::SparqlParser::new().parse_query(query) else {
        return Ok(None);
    };
    let spargebra::Query::Select { pattern, .. } = &mut parsed else {
        return Ok(None);
    };
    let mut pattern = pattern;
    let mut projected = Vec::new();
    loop {
        pattern = match pattern {
            GraphPattern::Slice { inner, .. } | GraphPattern::Distinct { inner } => inner,
            GraphPattern::Project { inner, variables } => {
                projected = variables.clone();
                inner
            }
            GraphPattern::OrderBy { expression, .. } => {
                for variable in &projected {
                    let keyed = expression.iter().any(|order| {
                        matches!(order, OrderExpression::Asc(Expression::Variable(key))
                            | OrderExpression::Desc(Expression::Variable(key)) if key == variable)
                    });
                    if !keyed {
                        expression
                            .push(OrderExpression::Asc(Expression::Variable(variable.clone())));
                    }
                }
                // Value-equal terms such as 1 and 1.0 still differ in these term parts.
                for variable in &projected {
                    for function in [Function::Str, Function::Datatype, Function::LangDir] {
                        let argument = Expression::Variable(variable.clone());
                        expression.push(OrderExpression::Asc(Expression::FunctionCall(
                            function,
                            vec![argument],
                        )));
                    }
                }
                break;
            }
            _ => return Ok(None),
        };
    }
    let partition = parsed.to_string();
    // Tie fetches may raise `LIMIT` up to the row limit, which lengthens the partition text.
    let widest = query_select_limit(query).map_or(0, |limit| {
        QUERY_MAX_ROWS.max(limit).to_string().len() - limit.to_string().len()
    });
    if partition.len() + widest > QUERY_MAX_BYTES {
        return Err(MetadataApiError::InvalidQuery(
            "This ordered query is too long to run across the realm; shorten its variable names \
             or run it locally"
                .to_string(),
        ));
    }
    Ok(Some(partition))
}
