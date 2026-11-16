//! Holds the realm fan-out call types and the bearer and query checks they use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ApiQueryMode, Arc, AuthToken, BoxFuture, MetadataApiError, MetadataReadError, NodeId};
use futures_util::future::FutureExt;

pub(super) type MetadataNodeCall<T> =
    Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataReadError>> + Send + Sync>;

#[derive(Clone, Copy)]
pub(super) enum MetadataFanoutOperation {
    Query,
    Search,
    BucketSearch,
    ObjectSearch,
    ReferencePreflight,
    IdentifierLookup,
}

impl MetadataFanoutOperation {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Search => "search",
            Self::BucketSearch => "bucket_search",
            Self::ObjectSearch => "object_search",
            Self::ReferencePreflight => "reference_preflight",
            Self::IdentifierLookup => "identifier_lookup",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetadataFanoutStats {
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<NodeId>,
    pub discovery_failed: bool,
    /// Nodes left out by the fanout cap, for a cursor to ask on a later page.
    pub omitted: Vec<NodeId>,
}

#[derive(Debug, Clone)]
pub(crate) struct RealmNodeDiscovery {
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) failed: bool,
}

#[derive(Debug, Clone)]
pub(super) struct MetadataFanoutScope {
    pub(super) mode: Option<ApiQueryMode>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) allow_partial: bool,
    pub(super) discovery_failed: bool,
    pub(super) subject: Option<[u8; 32]>,
    pub(super) deadline: Option<tokio::time::Instant>,
    /// Discovered nodes not to ask, such as those a cursor says are done.
    pub(super) skip: Vec<NodeId>,
}

impl MetadataFanoutScope {
    pub(super) fn new(
        mode: Option<ApiQueryMode>,
        target_nodes: Option<Vec<NodeId>>,
        allow_partial: bool,
    ) -> Self {
        Self {
            mode,
            target_nodes,
            allow_partial,
            discovery_failed: false,
            subject: None,
            deadline: None,
            skip: Vec::new(),
        }
    }

    pub(super) fn with_skip(mut self, skip: Vec<NodeId>) -> Self {
        self.skip = skip;
        self
    }

    pub(super) fn with_discovery_failed(mut self, discovery_failed: bool) -> Self {
        self.discovery_failed = discovery_failed;
        self
    }

    pub(super) fn with_subject(mut self, subject: [u8; 32]) -> Self {
        self.subject = Some(subject);
        self
    }

    pub(super) fn with_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}

pub(crate) fn pattern_contains_service(pattern: &spargebra::algebra::GraphPattern) -> bool {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::Service { .. } => true,
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            pattern_contains_service(left) || pattern_contains_service(right)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            pattern_contains_service(left)
                || pattern_contains_service(right)
                || expression.as_ref().is_some_and(expression_contains_service)
        }
        GraphPattern::Filter { expr, inner } => {
            expression_contains_service(expr) || pattern_contains_service(inner)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => pattern_contains_service(inner),
        GraphPattern::Extend {
            inner, expression, ..
        } => expression_contains_service(expression) || pattern_contains_service(inner),
        GraphPattern::OrderBy { inner, expression } => {
            pattern_contains_service(inner)
                || expression.iter().any(|expression| match expression {
                    spargebra::algebra::OrderExpression::Asc(expression)
                    | spargebra::algebra::OrderExpression::Desc(expression) => {
                        expression_contains_service(expression)
                    }
                })
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            pattern_contains_service(inner)
                || aggregates.iter().any(|(_, aggregate)| match aggregate {
                    spargebra::algebra::AggregateExpression::CountSolutions { .. } => false,
                    spargebra::algebra::AggregateExpression::FunctionCall { expr, .. } => {
                        expression_contains_service(expr)
                    }
                })
        }
    }
}

fn expression_contains_service(expression: &spargebra::algebra::Expression) -> bool {
    use spargebra::algebra::Expression;

    match expression {
        Expression::Exists(pattern) => pattern_contains_service(pattern),
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => false,
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            expression_contains_service(inner)
        }
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            expression_contains_service(left) || expression_contains_service(right)
        }
        Expression::In(left, right) => {
            expression_contains_service(left) || right.iter().any(expression_contains_service)
        }
        Expression::If(condition, left, right) => {
            expression_contains_service(condition)
                || expression_contains_service(left)
                || expression_contains_service(right)
        }
        Expression::Coalesce(expressions) | Expression::FunctionCall(_, expressions) => {
            expressions.iter().any(expression_contains_service)
        }
    }
}

/// DEFERRED (#259): this local guard bounds distributed union queries to a safe
/// subset. The spec-correct single-evaluation captured-generation union awaits
/// one-holder-per-bucket selection from feat/routing-placement; not built here.
pub(super) fn query_union_safe(query: &str) -> bool {
    let Ok(parsed) = spargebra::SparqlParser::new().parse_query(query) else {
        return false;
    };
    match parsed {
        spargebra::Query::Select { pattern, .. } => {
            let pattern = match pattern {
                spargebra::algebra::GraphPattern::Slice {
                    inner, start: 0, ..
                } => *inner,
                spargebra::algebra::GraphPattern::Slice { .. } => return false,
                pattern => pattern,
            };
            let spargebra::algebra::GraphPattern::Distinct { inner } = pattern else {
                return false;
            };
            let spargebra::algebra::GraphPattern::Project { inner, variables } = *inner else {
                return false;
            };
            let inner = match *inner {
                spargebra::algebra::GraphPattern::OrderBy { inner, expression } => {
                    if !expression
                        .iter()
                        .all(|order| order_projected(order, &variables))
                    {
                        return false;
                    }
                    *inner
                }
                pattern => pattern,
            };
            union_pattern_safe(&inner)
        }
        spargebra::Query::Ask { pattern, .. } => {
            let spargebra::algebra::GraphPattern::Project { inner, .. } = pattern else {
                return false;
            };
            union_pattern_safe(&inner)
        }
        _ => false,
    }
}

fn order_projected(
    order: &spargebra::algebra::OrderExpression,
    variables: &[spargebra::term::Variable],
) -> bool {
    let (spargebra::algebra::OrderExpression::Asc(expression)
    | spargebra::algebra::OrderExpression::Desc(expression)) = order;
    matches!(expression, spargebra::algebra::Expression::Variable(variable) if variables.contains(variable))
}

fn union_pattern_safe(pattern: &spargebra::algebra::GraphPattern) -> bool {
    match pattern {
        spargebra::algebra::GraphPattern::Bgp { patterns } => patterns.len() <= 1,
        spargebra::algebra::GraphPattern::Union { left, right } => {
            union_pattern_safe(left) && union_pattern_safe(right)
        }
        spargebra::algebra::GraphPattern::Graph { inner, .. } => union_pattern_safe(inner),
        _ => false,
    }
}

pub fn forwarded_bearer(token: Option<&str>) -> Result<Option<AuthToken>, MetadataApiError> {
    token
        .map(AuthToken::bearer)
        .transpose()
        .map_err(|_| MetadataApiError::BadRequest)
}

pub(super) fn fanout_bearer(token: Option<&str>) -> Option<AuthToken> {
    token.and_then(|token| AuthToken::bearer(token).ok())
}

pub(super) fn metadata_node_call<C, T, F, Fut>(context: C, call: F) -> MetadataNodeCall<T>
where
    C: Clone + Send + Sync + 'static,
    T: Send + 'static,
    F: Fn(C, NodeId) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, MetadataReadError>> + Send + 'static,
{
    Arc::new(move |node_id| {
        let context = context.clone();
        call(context, node_id).boxed()
    })
}
