//! Runs metadata queries and searches across realm nodes and merges the results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    ApiQueryMode, Arc, AuthContext, BucketPage, DISTRIBUTED_QUERY_DEADLINE, DriverContext, GroupId,
    HashMap, HashSet, Instant, MAX_PAGINATION_DEPTH, MAX_RESULT_BYTES, MetadataApiError,
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, MetadataNodeCall,
    MetadataQueryResults, MetadataReadError, MetadataSearchHit, NodeId, NodeSearchResult,
    ObjectKeyMatch, ObjectQueryMode, QUERY_MAX_ROWS, RealmId, ReferenceNodeExecution,
    SearchNodePage, SearchPageCursor, SearchWatermark, Span, fanout_bearer, field, map_read_error,
    merge_search_hits, metadata_node_call, paginate, query_union_safe, record_elapsed_ms,
    resume_fetch_limit, run_metadata_fanout,
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

#[tracing::instrument(
    name = "metadata.operation.query_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        cache = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_query_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
) -> Result<(MetadataQueryResults, MetadataFanoutStats), MetadataApiError> {
    let span = Span::current();
    let total_started = Instant::now();
    let mode = scope.mode.unwrap_or(ApiQueryMode::Distributed);
    let single_dataset_result = mode == ApiQueryMode::Local || graph_iris.is_some();
    if mode == ApiQueryMode::Distributed && graph_iris.is_none() && !query_union_safe(&query) {
        return Err(MetadataApiError::BadRequest);
    }
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let query_form = query_form(&query).ok_or(MetadataApiError::BadRequest)?;
    let select_limit = match query_form {
        MetadataQueryForm::Select => query_select_limit(&query),
        MetadataQueryForm::Ask => None,
    };
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());

    // Remote partitions authorize on the forwarded credential, so entries are partitioned
    // by credential digest.
    let cache_stamp = handle.query_cache().stamp(handle.visibility_generation());
    let cache_key =
        crate::metadata::query_cache::credential_digest(auth.as_ref(), bearer_token.as_deref())
            .map(|credential| {
                crate::metadata::query_cache::remote_key(
                    &crate::metadata::query_cache::RemoteKeyInput {
                        distributed: mode == ApiQueryMode::Distributed,
                        realm_id,
                        credential: &credential,
                        graph_iris: graph_iris.as_deref(),
                        sparql: &query,
                        allow_partial: scope.allow_partial,
                        target_nodes: scope.target_nodes.as_deref(),
                    },
                )
            })
            .filter(|_| !scope.discovery_failed);
    if let Some(key) = cache_key
        && let Some(cached) = handle.query_cache().get(&key, cache_stamp, Instant::now())
    {
        span.record("cache", "hit");
        span.record("result", cached.results.kind());
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        return Ok((
            (*cached.results).clone(),
            crate::metadata::query_cache::cached_stats(&cached),
        ));
    }
    span.record("cache", "miss");

    let deadline = scope
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE);
    let ordered = (!single_dataset_result)
        .then(|| ordered_query(&query))
        .flatten();
    let partition_query = ordered
        .as_ref()
        .map_or_else(|| query.clone(), |(text, _)| text.clone());
    let local_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            partition_query.clone(),
        ),
        |(handle, auth, graph_iris, query), _| async move {
            handle
                .query_authorized_local(auth, graph_iris, query)
                .await
                .map_err(crate::metadata::handle::metadata_read_error)
        },
    );
    let remote_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            partition_query.clone(),
        ),
        |(handle, auth_token, graph_iris, query), node_id| async move {
            handle
                .query_remote_graphs(node_id, auth_token, graph_iris, query)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Query,
        local_call,
        remote_call,
        record_query_result,
        map_read_error,
    )
    .await?;

    let parts: Vec<_> = parts.into_iter().map(|(_, result)| result).collect();
    let result = if single_dataset_result {
        match parts.into_iter().next() {
            Some(result) => Ok(result),
            None => aggregate_query_results(Vec::new(), query_form, select_limit),
        }
    } else if let Some((_, order)) = ordered {
        let row_limit = select_limit.unwrap_or(QUERY_MAX_ROWS).min(QUERY_MAX_ROWS);
        merge_ordered(parts, order, row_limit, deadline)
            .await
            .and_then(|rows| aggregate_query_results(vec![rows], query_form, select_limit))
    } else {
        aggregate_query_results(parts, query_form, select_limit)
    };
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", results.kind());
            if let Some(key) = cache_key
                && crate::metadata::query_cache::store_complete(
                    handle.query_cache(),
                    key,
                    results,
                    &fanout_stats,
                    cache_stamp,
                    handle.visibility_generation(),
                    Instant::now(),
                )
            {
                span.record("cache", "stored");
            }
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout_stats))
}

#[tracing::instrument(
    name = "metadata.operation.search_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, resume, watermark, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        page_size = page_size as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_search_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    conforms_to: Option<String>,
    group_id: Option<GroupId>,
    resume: HashMap<NodeId, u32>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    scope: MetadataFanoutScope,
) -> Result<
    (
        Vec<MetadataSearchHit>,
        Option<SearchPageCursor>,
        bool,
        MetadataFanoutStats,
    ),
    MetadataApiError,
> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());
    let resume = Arc::new(resume);

    let local_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to.clone(),
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth, graph_iris, query, conforms_to, group_id, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile::validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .search_local_filtered(
                                    auth.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    crate::metadata::iri_index::DCTERMS_CONFORMS_IRI.to_string(),
                                    object_iri,
                                    group_id,
                                )
                                .await
                                .map_err(crate::metadata::handle::metadata_read_error)?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => handle
                    .search_authorized_local(auth, graph_iris, query, limit, group_id)
                    .await
                    .map_err(crate::metadata::handle::metadata_read_error)?,
            };
            Ok((hits, limit))
        },
    );
    let remote_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to,
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, graph_iris, query, conforms_to, group_id, resume, page_size),
         node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile::validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .search_remote_filtered(
                                    node_id,
                                    auth_token.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    crate::metadata::iri_index::DCTERMS_CONFORMS_IRI.to_string(),
                                    object_iri,
                                    group_id,
                                )
                                .await?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => {
                    handle
                        .search_remote_graphs(
                            node_id, auth_token, graph_iris, query, limit, group_id,
                        )
                        .await?
                }
            };
            Ok((hits, limit))
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node,
        map_read_error,
    )
    .await?;

    let node_results = node_parts
        .into_iter()
        .map(|(node_id, (hits, requested))| NodeSearchResult {
            node_id,
            saturated: hits.len() >= requested,
            hits,
        })
        .collect();
    let page = paginate(node_results, watermark, page_size, MAX_PAGINATION_DEPTH);
    span.record("hit_count", page.hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((page.hits, page.next, page.truncated, fanout_stats))
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

/// Orders partition rows off the async workers and gives up when `deadline` passes.
pub(super) async fn merge_ordered(
    results: Vec<MetadataQueryResults>,
    order: Vec<spargebra::algebra::OrderExpression>,
    row_limit: usize,
    deadline: tokio::time::Instant,
) -> Result<MetadataQueryResults, MetadataApiError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let cancel = spareval::CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::task::spawn_blocking(move || {
        order_solutions(results, &order, row_limit, &task_cancel)
    });
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(merged)) => merged,
        Ok(Err(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(_) => {
            cancel.cancel();
            Err(MetadataApiError::ServiceUnavailable)
        }
    }
}

/// Returns the query each partition runs and the merge keys: the `ORDER BY` keys, every other
/// projected variable, then each variable's term parts, so each `LIMIT` cuts a total order.
pub(super) fn ordered_query(
    query: &str,
) -> Option<(String, Vec<spargebra::algebra::OrderExpression>)> {
    use spargebra::algebra::{Expression, Function, GraphPattern, OrderExpression};

    let mut parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = &mut parsed else {
        return None;
    };
    let mut pattern = pattern;
    let mut projected = Vec::new();
    let keys = loop {
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
                break expression.clone();
            }
            _ => return None,
        };
    };
    Some((parsed.to_string(), keys))
}

/// Sorts the union of partition rows with the engine's SPARQL ordering (spareval), so terms
/// order as in one store; rows with equal keys keep a fixed order by content.
pub(super) fn order_solutions(
    results: Vec<MetadataQueryResults>,
    order: &[spargebra::algebra::OrderExpression],
    row_limit: usize,
    cancel: &spareval::CancellationToken,
) -> Result<MetadataQueryResults, MetadataApiError> {
    use oxrdf::{BlankNode, Dataset, GraphName, Literal, NamedNode, Quad, Term, Variable};
    use spargebra::algebra::{Expression, GraphPattern, OrderExpression};
    use spargebra::term::TriplePattern;

    // A partition's rows past `row_limit` cannot reach the merged top rows.
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    for result in results {
        let MetadataQueryResults::Solutions(part) = result else {
            continue;
        };
        for row in part.into_iter().take(row_limit) {
            bytes = row.iter().fold(bytes, |bytes, (name, value)| {
                bytes.saturating_add(name.len()).saturating_add(value.len())
            });
            if bytes > MAX_RESULT_BYTES {
                return Err(MetadataApiError::BadRequest);
            }
            rows.push(row);
        }
    }
    rows.sort_unstable();
    rows.dedup();
    // Each row becomes a subject with its position and one column triple per bound variable.
    let row = Variable::new_unchecked("order-row");
    let index = Variable::new_unchecked("order-index");
    let index_iri = NamedNode::new_unchecked("urn:aruna:order:index");
    let column = |name: &str| NamedNode::new_unchecked(format!("urn:aruna:order:column:{name}"));
    let mut dataset = Dataset::new();
    for (position, values) in rows.iter().enumerate() {
        if cancel.is_cancelled() {
            return Err(MetadataApiError::ServiceUnavailable);
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
            let term: Term = value.parse().map_err(|error: oxrdf::TermParseError| {
                MetadataApiError::Internal(error.to_string())
            })?;
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
    expression.push(OrderExpression::Asc(Expression::Variable(index.clone())));
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
        spareval::QueryEvaluationError::Cancelled => MetadataApiError::ServiceUnavailable,
        error => MetadataApiError::Internal(error.to_string()),
    };
    let evaluator = spareval::QueryEvaluator::new().with_cancellation_token(cancel.clone());
    let spareval::QueryResults::Solutions(solutions) = evaluator
        .prepare(&query)
        .execute(&dataset)
        .map_err(evaluation_error)?
    else {
        return Err(MetadataApiError::Internal(
            "ordering returned no rows".to_string(),
        ));
    };
    let mut slots: Vec<_> = rows.into_iter().map(Some).collect();
    let mut ordered = Vec::with_capacity(slots.len());
    for solution in solutions {
        let solution = solution.map_err(evaluation_error)?;
        let position = match solution.get(&index) {
            Some(Term::Literal(literal)) => literal.value().parse::<usize>().ok(),
            _ => None,
        };
        let row = position
            .and_then(|position| slots.get_mut(position)?.take())
            .ok_or_else(|| MetadataApiError::Internal("ordering lost a row".to_string()))?;
        ordered.push(row);
    }
    Ok(MetadataQueryResults::Solutions(ordered))
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
