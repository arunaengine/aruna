use super::*;

pub(super) fn ensure_supported_query_mode(mode: &Option<MetadataApiQueryMode>) {
    match mode {
        None | Some(MetadataApiQueryMode::Local) | Some(MetadataApiQueryMode::Distributed) => {}
    }
}

pub(super) fn ensure_supported_query_form(query: &str) -> Result<(), MetadataApiError> {
    if query.len() > METADATA_QUERY_MAX_BYTES {
        return Err(MetadataApiError::BadRequest);
    }
    let parsed = spargebra::SparqlParser::new()
        .parse_query(query)
        .map_err(|_| MetadataApiError::BadRequest)?;
    let pattern = match &parsed {
        spargebra::Query::Select { pattern, .. } | spargebra::Query::Ask { pattern, .. } => pattern,
        _ => return Err(MetadataApiError::BadRequest),
    };
    if graph_pattern_contains_service(pattern) {
        return Err(MetadataApiError::BadRequest);
    }
    if matches!(
        pattern,
        spargebra::algebra::GraphPattern::Slice {
            length: Some(length),
            ..
        } if *length > METADATA_QUERY_MAX_ROWS
    ) {
        return Err(MetadataApiError::BadRequest);
    }
    Ok(())
}

pub(crate) fn graph_pattern_contains_service(pattern: &spargebra::algebra::GraphPattern) -> bool {
    use spargebra::algebra::GraphPattern;

    match pattern {
        GraphPattern::Service { .. } => true,
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            graph_pattern_contains_service(left) || graph_pattern_contains_service(right)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            graph_pattern_contains_service(left)
                || graph_pattern_contains_service(right)
                || expression.as_ref().is_some_and(expression_contains_service)
        }
        GraphPattern::Filter { expr, inner } => {
            expression_contains_service(expr) || graph_pattern_contains_service(inner)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => graph_pattern_contains_service(inner),
        GraphPattern::Extend {
            inner, expression, ..
        } => expression_contains_service(expression) || graph_pattern_contains_service(inner),
        GraphPattern::OrderBy { inner, expression } => {
            graph_pattern_contains_service(inner)
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
            graph_pattern_contains_service(inner)
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
        Expression::Exists(pattern) => graph_pattern_contains_service(pattern),
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
pub(super) fn distributed_query_is_union_safe(query: &str) -> bool {
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
            let spargebra::algebra::GraphPattern::Project { inner, .. } = *inner else {
                return false;
            };
            distributed_union_pattern_is_safe(&inner)
        }
        spargebra::Query::Ask { pattern, .. } => {
            let spargebra::algebra::GraphPattern::Project { inner, .. } = pattern else {
                return false;
            };
            distributed_union_pattern_is_safe(&inner)
        }
        _ => false,
    }
}

fn distributed_union_pattern_is_safe(pattern: &spargebra::algebra::GraphPattern) -> bool {
    match pattern {
        spargebra::algebra::GraphPattern::Bgp { patterns } => patterns.len() <= 1,
        spargebra::algebra::GraphPattern::Union { left, right } => {
            distributed_union_pattern_is_safe(left) && distributed_union_pattern_is_safe(right)
        }
        spargebra::algebra::GraphPattern::Graph { inner, .. } => {
            distributed_union_pattern_is_safe(inner)
        }
        _ => false,
    }
}
