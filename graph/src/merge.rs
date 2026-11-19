//! Merges query results off the async workers: orders partition rows as one store would,
//! counts ties at a page limit, and joins the two sides of a bounded cross-shard join.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aruna_core::metadata::{
    MAX_RESULT_BYTES, MergeError, MergeJob, MergeOutput, MetadataQueryResults, QUERY_MAX_ROWS,
};
use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout_at};

type Row = BTreeMap<String, String>;

/// Merges that may sort at once, so they cannot fill the shared blocking pool.
static MERGE_PERMITS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    Arc::new(Semaphore::new(cpus))
});

/// The `ORDER BY` keys of an ordered partition query, as its planner wrote them.
fn order_of(partition: &str) -> Result<Vec<spargebra::algebra::OrderExpression>, MergeError> {
    let refused = || MergeError::Failed("the partition query names no order".to_string());
    let parsed = spargebra::SparqlParser::new()
        .parse_query(partition)
        .map_err(|error| MergeError::Failed(error.to_string()))?;
    let spargebra::Query::Select { mut pattern, .. } = parsed else {
        return Err(refused());
    };
    loop {
        pattern = match pattern {
            GraphPattern::Slice { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Project { inner, .. } => *inner,
            GraphPattern::OrderBy { expression, .. } => return Ok(expression),
            _ => return Err(refused()),
        };
    }
}

/// Runs one merge on the blocking pool under a merge permit, within `budget`.
pub(crate) async fn run_merge(job: MergeJob, budget: Duration) -> Result<MergeOutput, MergeError> {
    run_permitted(job, budget, MERGE_PERMITS.clone()).await
}

async fn run_permitted(
    job: MergeJob,
    budget: Duration,
    permits: Arc<Semaphore>,
) -> Result<MergeOutput, MergeError> {
    let deadline = Instant::now() + budget;
    let Ok(Ok(permit)) = timeout_at(deadline, permits.acquire_owned()).await else {
        return Err(MergeError::Cancelled);
    };
    let cancel = spareval::CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        // A job that starts late drops its rows without work.
        if task_cancel.is_cancelled() || Instant::now() >= deadline {
            return Err(MergeError::Cancelled);
        }
        merge(job, &task_cancel)
    });
    match timeout_at(deadline, task).await {
        Ok(Ok(merged)) => merged,
        Ok(Err(error)) => Err(MergeError::Failed(error.to_string())),
        Err(_) => {
            cancel.cancel();
            Err(MergeError::Cancelled)
        }
    }
}

/// Runs one merge here, stopping once `cancel` is set.
pub fn merge(
    job: MergeJob,
    cancel: &spareval::CancellationToken,
) -> Result<MergeOutput, MergeError> {
    match job {
        MergeJob::Order { partition, results } => {
            let order = order_of(&partition)?;
            order_solutions(results, &order, cancel).map(MergeOutput::Rows)
        }
        MergeJob::Ties { partition, rows } => {
            let order = order_of(&partition)?;
            tied_rows(&rows, &order, cancel).map(MergeOutput::Tied)
        }
        MergeJob::Join { query, left, right } => {
            let plan = join_plan(&query)
                .ok_or_else(|| MergeError::Failed("the query is no bounded join".to_string()))?;
            let rows = join_rows(&plan, left, right, cancel)?;
            Ok(MergeOutput::Rows(MetadataQueryResults::Solutions(rows)))
        }
    }
}

/// Rows one side of a join may hold; a side with more refuses the query.
pub const JOIN_SIDE_ROWS: usize = 5_000;
/// Row pairs one join may combine before it refuses the query.
const JOIN_MAX_PAIRS: usize = 1_000_000;

/// A `SELECT DISTINCT` over two triple patterns that share a variable, with an optional
/// `LIMIT` and no other modifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinPlan {
    /// The union-safe query of each pattern, bounded to one row past [`JOIN_SIDE_ROWS`].
    pub sides: [String; 2],
    shared: Vec<String>,
    projected: Vec<String>,
    limit: Option<usize>,
}

/// The join plan of `query`, or `None` for every other form.
pub fn join_plan(query: &str) -> Option<JoinPlan> {
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
pub fn join_rows(
    plan: &JoinPlan,
    left: Vec<Row>,
    right: Vec<Row>,
    cancel: &spareval::CancellationToken,
) -> Result<Vec<Row>, MergeError> {
    if left.len() > JOIN_SIDE_ROWS || right.len() > JOIN_SIDE_ROWS {
        return Err(MergeError::Refused(format!(
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
            return Err(MergeError::Cancelled);
        }
        for other in index.get(&key(row)).into_iter().flatten() {
            if joined.len() >= limit {
                return Ok(joined);
            }
            pairs += 1;
            if pairs > JOIN_MAX_PAIRS {
                return Err(MergeError::Refused(format!(
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
                .map_err(|error| MergeError::Failed(error.to_string()))?
                .len();
            bytes = bytes.saturating_add(size + 1);
            if bytes > MAX_RESULT_BYTES {
                return Err(MergeError::TooLarge);
            }
            seen.insert(merged.clone());
            joined.push(merged);
        }
    }
    Ok(joined)
}

/// Sorts the union of partition rows with the engine's SPARQL ordering (spareval), so terms
/// order as in one store; rows with equal keys keep a fixed order by content.
pub fn order_solutions(
    results: Vec<MetadataQueryResults>,
    order: &[spargebra::algebra::OrderExpression],
    cancel: &spareval::CancellationToken,
) -> Result<MetadataQueryResults, MergeError> {
    // Partitions return whole tie groups past `LIMIT`, so only the byte budget bounds the union.
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    for result in results {
        let MetadataQueryResults::Solutions(part) = result else {
            continue;
        };
        for row in part {
            bytes = row.iter().fold(bytes, |bytes, (name, value)| {
                bytes.saturating_add(name.len()).saturating_add(value.len())
            });
            if bytes > MAX_RESULT_BYTES {
                return Err(MergeError::TooLarge);
            }
            rows.push(row);
        }
    }
    rows.sort_unstable();
    rows.dedup();
    let positions = sorted_positions(&rows, order, false, cancel)?;
    let mut slots: Vec<_> = rows.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(slots.len());
    for position in positions {
        let row = slots
            .get_mut(position)
            .and_then(Option::take)
            .ok_or_else(|| MergeError::Failed("ordering lost a row".to_string()))?;
        ordered.push(row);
    }
    Ok(MetadataQueryResults::Solutions(ordered))
}

/// Counts the leading `rows` that order equal to the first one, for rows already in key order.
pub fn tied_rows(
    rows: &[BTreeMap<String, String>],
    order: &[spargebra::algebra::OrderExpression],
    cancel: &spareval::CancellationToken,
) -> Result<usize, MergeError> {
    // Ties sort by descending position, so the first row out is the last row tied with the first.
    let last = sorted_positions(rows, order, true, cancel)?
        .first()
        .copied();
    Ok(last.map_or(0, |last| last + 1))
}

/// Orders row positions by `order`, then by position, with spareval's SPARQL ordering.
fn sorted_positions(
    rows: &[BTreeMap<String, String>],
    order: &[spargebra::algebra::OrderExpression],
    descending: bool,
    cancel: &spareval::CancellationToken,
) -> Result<Vec<usize>, MergeError> {
    use oxrdf::{BlankNode, Dataset, GraphName, Literal, NamedNode, Quad, Term, Variable};
    use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
    use spargebra::term::TriplePattern;

    // Each row becomes a subject with its position and one column triple per bound variable.
    let row = Variable::new_unchecked("order-row");
    let index = Variable::new_unchecked("order-index");
    let index_iri = NamedNode::new_unchecked("urn:aruna:order:index");
    let column = |name: &str| NamedNode::new_unchecked(format!("urn:aruna:order:column:{name}"));
    let mut dataset = Dataset::new();
    for (position, values) in rows.iter().enumerate() {
        if cancel.is_cancelled() {
            return Err(MergeError::Cancelled);
        }
        let subject = BlankNode::default();
        let position = Literal::from(position as u64);
        dataset.insert(&Quad::new(
            subject.clone(),
            index_iri.clone(),
            position,
            GraphName::DefaultGraph,
        ));
        for (name, value) in values {
            let term: Term = value
                .parse()
                .map_err(|error: oxrdf::TermParseError| MergeError::Failed(error.to_string()))?;
            dataset.insert(&Quad::new(
                subject.clone(),
                column(name),
                term,
                GraphName::DefaultGraph,
            ));
        }
    }
    let names: std::collections::BTreeSet<&String> = rows
        .iter()
        .flat_map(std::collections::BTreeMap::keys)
        .collect();
    let mut pattern = GraphPattern::Bgp {
        patterns: vec![TriplePattern {
            subject: row.clone().into(),
            predicate: index_iri.into(),
            object: index.clone().into(),
        }],
    };
    for name in names {
        let column = TriplePattern {
            subject: row.clone().into(),
            predicate: column(name).into(),
            object: Variable::new_unchecked(name).into(),
        };
        pattern = GraphPattern::LeftJoin {
            left: Box::new(pattern),
            right: Box::new(GraphPattern::Bgp {
                patterns: vec![column],
            }),
            expression: None,
        };
    }
    let mut expression = order.to_vec();
    let position = Expression::Variable(index.clone());
    expression.push(match descending {
        true => OrderExpression::Desc(position),
        false => OrderExpression::Asc(position),
    });
    let query = spargebra::Query::Select {
        dataset: None,
        base_iri: None,
        pattern: GraphPattern::Project {
            inner: Box::new(GraphPattern::OrderBy {
                inner: Box::new(pattern),
                expression,
            }),
            variables: vec![index.clone()],
        },
    };
    let evaluation_error = |error: spareval::QueryEvaluationError| match error {
        spareval::QueryEvaluationError::Cancelled => MergeError::Cancelled,
        error => MergeError::Failed(error.to_string()),
    };
    let evaluator = spareval::QueryEvaluator::new().with_cancellation_token(cancel.clone());
    let spareval::QueryResults::Solutions(solutions) = evaluator
        .prepare(&query)
        .execute(&dataset)
        .map_err(evaluation_error)?
    else {
        return Err(MergeError::Failed("ordering returned no rows".to_string()));
    };
    let mut positions = Vec::with_capacity(rows.len());
    for solution in solutions {
        let solution = solution.map_err(evaluation_error)?;
        let position = match solution.get(&index) {
            Some(Term::Literal(literal)) => literal.value().parse::<usize>().ok(),
            _ => None,
        };
        positions
            .push(position.ok_or_else(|| MergeError::Failed("ordering lost a row".to_string()))?);
    }
    Ok(positions)
}

#[cfg(test)]
#[path = "merge_tests.rs"]
mod tests;
