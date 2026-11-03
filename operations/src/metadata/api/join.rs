//! Plans and joins one bounded cross-shard SPARQL form: two triple patterns that share a
//! variable, each fetched across shards as its own union-safe query and joined here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};

use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};

use super::{MAX_RESULT_BYTES, MetadataApiError, MetadataFanoutStats, QUERY_MAX_ROWS};

/// Rows one side of a join may hold; a side with more refuses the query.
pub(super) const JOIN_SIDE_ROWS: usize = 5_000;
/// Row pairs one join may combine before it refuses the query.
const JOIN_MAX_PAIRS: usize = 1_000_000;

type Row = BTreeMap<String, String>;

/// A `SELECT DISTINCT` over two triple patterns that share a variable, with an optional
/// `LIMIT` and no other modifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JoinPlan {
    /// The union-safe query of each pattern, bounded to one row past [`JOIN_SIDE_ROWS`].
    pub(super) sides: [String; 2],
    shared: Vec<String>,
    projected: Vec<String>,
    limit: Option<usize>,
}

/// The join plan of `query`, or `None` for every other form.
pub(super) fn join_plan(query: &str) -> Option<JoinPlan> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select {
        pattern,
        dataset: None,
        ..
    } = parsed
    else {
        return None;
    };
    let (pattern, limit) = match pattern {
        GraphPattern::Slice {
            inner,
            start: 0,
            length,
        } => (*inner, length),
        GraphPattern::Slice { .. } => return None,
        pattern => (pattern, None),
    };
    let GraphPattern::Distinct { inner } = pattern else {
        return None;
    };
    let GraphPattern::Project { inner, variables } = *inner else {
        return None;
    };
    let GraphPattern::Bgp { patterns } = *inner else {
        return None;
    };
    let [left, right] = patterns.as_slice() else {
        return None;
    };
    let (left_names, right_names) = (pattern_names(left)?, pattern_names(right)?);
    let shared: Vec<String> = (left_names.iter())
        .filter(|name| right_names.contains(name))
        .cloned()
        .collect();
    if shared.is_empty() {
        return None;
    }
    Some(JoinPlan {
        sides: [
            side_query(left, &left_names),
            side_query(right, &right_names),
        ],
        shared,
        projected: variables
            .iter()
            .map(|name| name.as_str().to_string())
            .collect(),
        limit,
    })
}

/// The variables of `pattern` in order, or `None` if it holds a blank node or quoted triple.
fn pattern_names(pattern: &TriplePattern) -> Option<Vec<String>> {
    let predicate = match &pattern.predicate {
        NamedNodePattern::Variable(variable) => Some(variable),
        NamedNodePattern::NamedNode(_) => None,
    };
    let mut names: Vec<String> = Vec::new();
    for variable in [
        term_variable(&pattern.subject)?,
        predicate,
        term_variable(&pattern.object)?,
    ]
    .into_iter()
    .flatten()
    {
        if !names.iter().any(|name| name == variable.as_str()) {
            names.push(variable.as_str().to_string());
        }
    }
    Some(names)
}

fn term_variable(term: &TermPattern) -> Option<Option<&Variable>> {
    match term {
        TermPattern::Variable(variable) => Some(Some(variable)),
        TermPattern::NamedNode(_) | TermPattern::Literal(_) => Some(None),
        _ => None,
    }
}

fn side_query(pattern: &TriplePattern, names: &[String]) -> String {
    let names: Vec<String> = names.iter().map(|name| format!("?{name}")).collect();
    let names = names.join(" ");
    format!(
        "SELECT DISTINCT {names} WHERE {{ {pattern} }} LIMIT {}",
        JOIN_SIDE_ROWS + 1
    )
}

/// Joins both sides on the shared variables, then projects, deduplicates and limits the rows
/// as one store holding every shard would. Oversized inputs or results refuse the query.
pub(super) fn join_rows(
    plan: &JoinPlan,
    left: Vec<Row>,
    right: Vec<Row>,
    cancel: &spareval::CancellationToken,
) -> Result<Vec<Row>, MetadataApiError> {
    if left.len() > JOIN_SIDE_ROWS || right.len() > JOIN_SIDE_ROWS {
        return Err(MetadataApiError::InvalidQuery(format!(
            "a pattern of the join matches more than {JOIN_SIDE_ROWS} rows; narrow it"
        )));
    }
    let limit = plan.limit.unwrap_or(QUERY_MAX_ROWS).min(QUERY_MAX_ROWS);
    let key = |row: &Row| -> Vec<Option<String>> {
        plan.shared
            .iter()
            .map(|name| row.get(name).cloned())
            .collect()
    };
    let mut index: HashMap<Vec<Option<String>>, Vec<&Row>> = HashMap::new();
    for row in &right {
        index.entry(key(row)).or_default().push(row);
    }
    let (mut seen, mut joined, mut pairs, mut bytes) = (HashSet::new(), Vec::new(), 0, 32usize);
    for row in &left {
        if cancel.is_cancelled() {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        for other in index.get(&key(row)).into_iter().flatten() {
            if joined.len() >= limit {
                return Ok(joined);
            }
            pairs += 1;
            if pairs > JOIN_MAX_PAIRS {
                return Err(MetadataApiError::InvalidQuery(format!(
                    "the join combines more than {JOIN_MAX_PAIRS} row pairs; narrow it"
                )));
            }
            let merged: Row = (plan.projected.iter())
                .filter_map(|name| {
                    let value = row.get(name).or_else(|| other.get(name))?;
                    Some((name.clone(), value.clone()))
                })
                .collect();
            if seen.contains(&merged) {
                continue;
            }
            let size = serde_json::to_vec(&merged)
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .len();
            bytes = bytes.saturating_add(size + 1);
            if bytes > MAX_RESULT_BYTES {
                return Err(MetadataApiError::BadRequest);
            }
            seen.insert(merged.clone());
            joined.push(merged);
        }
    }
    Ok(joined)
}

/// Both sides' fanout as one: a node either side could not ask counts as failed.
pub(super) fn merge_stats(
    left: MetadataFanoutStats,
    right: MetadataFanoutStats,
) -> MetadataFanoutStats {
    let mut failed_partitions = left.failed_partitions;
    for node_id in right.failed_partitions {
        if !failed_partitions.contains(&node_id) {
            failed_partitions.push(node_id);
        }
    }
    let mut omitted = left.omitted;
    for node_id in right.omitted {
        if !omitted.contains(&node_id) {
            omitted.push(node_id);
        }
    }
    MetadataFanoutStats {
        nodes_queried: left.nodes_queried.max(right.nodes_queried),
        nodes_failed: (left.nodes_failed.max(right.nodes_failed)).max(failed_partitions.len()),
        failed_partitions,
        discovery_failed: left.discovery_failed || right.discovery_failed,
        omitted,
    }
}
