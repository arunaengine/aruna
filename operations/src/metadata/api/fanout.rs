use super::distributed::record_bucket_result;
use super::*;

pub(super) fn ensure_query_mode(mode: &Option<MetadataApiQueryMode>) {
    match mode {
        None | Some(MetadataApiQueryMode::Local) | Some(MetadataApiQueryMode::Distributed) => {}
    }
}

pub(super) fn ensure_query_form(query: &str) -> Result<(), MetadataApiError> {
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
    if pattern_contains_service(pattern) {
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
            let spargebra::algebra::GraphPattern::Project { inner, .. } = *inner else {
                return false;
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

pub fn forwarded_bearer(
    token: Option<&str>,
) -> Result<Option<MetadataAuthToken>, MetadataApiError> {
    token
        .map(MetadataAuthToken::bearer)
        .transpose()
        .map_err(|_| MetadataApiError::BadRequest)
}

pub(super) fn fanout_bearer(token: Option<&str>) -> Option<MetadataAuthToken> {
    token.and_then(|token| MetadataAuthToken::bearer(token).ok())
}

pub(super) type MetadataNodeCall<T> =
    Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<T, MetadataReadError>> + Send + Sync>;

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

#[derive(Clone, Copy)]
pub(super) enum MetadataFanoutOperation {
    Query,
    Search,
    BucketSearch,
    ObjectSearch,
    ReferencePreflight,
}

impl MetadataFanoutOperation {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Search => "search",
            Self::BucketSearch => "bucket_search",
            Self::ObjectSearch => "object_search",
            Self::ReferencePreflight => "reference_preflight",
        }
    }
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
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_fanout_node<T>(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
    deadline: tokio::time::Instant,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataReadError>),
    record_stage_detail: bool,
) -> Result<T, MetadataReadError> {
    let node_span = fanout_node_span(operation, node_id, local);
    let node_started = Instant::now();
    let result = if local {
        match tokio::time::timeout_at(deadline, local_call(node_id).instrument(node_span.clone()))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    } else {
        match tokio::time::timeout_at(deadline, remote_call(node_id).instrument(node_span.clone()))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    };
    let elapsed = record_elapsed_ms(&node_span, "elapsed_ms", node_started);
    if record_stage_detail {
        aruna_core::telemetry::record_stage_detail(
            "fanout_node",
            || short_display_id(node_id),
            elapsed,
        );
    }
    record_result(&node_span, &result);
    result
}

pub(super) async fn metadata_fanout_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    span: &Span,
    target_nodes: Option<Vec<NodeId>>,
    deadline: tokio::time::Instant,
) -> MetadataRealmNodeDiscovery {
    match target_nodes {
        Some(nodes) => {
            span.record("discovery_ms", 0u64);
            MetadataRealmNodeDiscovery {
                nodes: deduplicate_fanout_nodes(nodes),
                failed: false,
            }
        }
        None => {
            let discovery_started = Instant::now();
            let discovery = tokio::time::timeout_at(
                deadline,
                aruna_core::telemetry::time_stage(
                    "discovery",
                    discover_realm_nodes(context, realm_id, local_node_id),
                ),
            )
            .await
            .unwrap_or(MetadataRealmNodeDiscovery {
                nodes: vec![local_node_id],
                failed: true,
            });
            record_elapsed_ms(span, "discovery_ms", discovery_started);
            discovery
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_metadata_fanout<T>(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataReadError>),
    map_local_error: fn(MetadataReadError) -> MetadataApiError,
) -> Result<(Vec<(NodeId, T)>, MetadataFanoutStats), MetadataApiError>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope {
        mode,
        target_nodes,
        allow_partial,
        discovery_failed: scope_discovery_failed,
        subject: request_subject,
        deadline: scope_deadline,
    } = scope;
    let deadline = scope_deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE);
    ensure_query_mode(&mode);
    match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
        MetadataApiQueryMode::Local => {
            let result = run_fanout_node(
                operation,
                local_node_id,
                true,
                deadline,
                local_call,
                remote_call,
                record_result,
                false,
            )
            .await;
            let fanout_stats = MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
                failed_partitions: Vec::new(),
                discovery_failed: false,
            };
            match result {
                Ok(result) => Ok((vec![(local_node_id, result)], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        MetadataApiQueryMode::Distributed => {
            let discovery = metadata_fanout_nodes(
                context,
                realm_id,
                local_node_id,
                &span,
                target_nodes,
                deadline,
            )
            .await;
            let discovery_failed = scope_discovery_failed || discovery.failed;
            let mut nodes = discovery.nodes;
            if discovery_failed && !allow_partial {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            let failed_partitions = Vec::new();
            let mut omitted_nodes = 0usize;
            if nodes.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES {
                let mut subject = Vec::with_capacity(32 + operation.label().len() + 32 + 1);
                subject.extend_from_slice(realm_id.as_bytes());
                subject.extend_from_slice(operation.label().as_bytes());
                if let Some(request_subject) = request_subject {
                    subject.extend_from_slice(&request_subject);
                }
                subject.extend_from_slice(local_node_id.as_bytes());
                let selected = select_fanout_nodes(&nodes, local_node_id, &subject);
                omitted_nodes = nodes.len().saturating_sub(selected.len());
                nodes = selected;
            }
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: failed_partitions.len()
                    + omitted_nodes
                    + usize::from(discovery_failed),
                failed_partitions,
                discovery_failed,
            };
            let fanout_started = Instant::now();
            let mut node_parts = Vec::new();
            let mut auth_error = None;
            let mut not_found = false;
            let node_order = nodes.clone();
            let mut outstanding = nodes.iter().copied().collect::<HashSet<_>>();
            // Offline partitions become failures while partial-tolerant callers still use
            // reachable results within the shared deadline.
            let pending =
                stream::iter(nodes.into_iter().enumerate().map(|(node_index, node_id)| {
                    let local_call = local_call.clone();
                    let remote_call = remote_call.clone();
                    async move {
                        let result = run_fanout_node(
                            operation,
                            node_id,
                            node_id == local_node_id,
                            deadline,
                            local_call,
                            remote_call,
                            record_result,
                            true,
                        )
                        .await;
                        (node_index, node_id, result)
                    }
                }))
                .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
            futures_util::pin_mut!(pending);

            loop {
                let next = match tokio::time::timeout_at(deadline, pending.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        fanout_stats
                            .failed_partitions
                            .extend(outstanding.iter().copied());
                        fanout_stats.nodes_failed = fanout_stats.failed_partitions.len()
                            + omitted_nodes
                            + usize::from(fanout_stats.discovery_failed);
                        if !allow_partial && fanout_stats.nodes_failed > 0 {
                            return Err(MetadataApiError::ServiceUnavailable);
                        }
                        break;
                    }
                };
                let Some((node_index, node_id, result)) = next else {
                    break;
                };
                outstanding.remove(&node_id);
                match result {
                    Ok(result) => node_parts.push((node_index, node_id, result)),
                    // A rejected forwarded credential is a failed partition;
                    // local authorization already vouched for the caller.
                    Err(MetadataReadError::Unauthorized) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            "distributed metadata skipped unauthorized node result"
                        );
                    }
                    // An authenticated denial anywhere fails the whole query.
                    Err(error @ MetadataReadError::Forbidden) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        auth_error.get_or_insert(map_read_error(error));
                    }
                    Err(MetadataReadError::NotFound) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        not_found = true;
                    }
                    Err(MetadataReadError::Unavailable) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            error = "unavailable",
                            "distributed metadata skipped failed node result"
                        );
                    }
                }
            }

            if let Some(error) = auth_error {
                return Err(error);
            }
            if not_found {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            // Nodes omitted by the fanout cap truncate the answer, so a caller
            // that asked for a complete result must not be told it is complete.
            if !allow_partial && fanout_stats.nodes_failed > 0 {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            node_parts.sort_by_key(|(node_index, _, _)| *node_index);
            fanout_stats.failed_partitions.sort_by_key(|node_id| {
                node_order
                    .iter()
                    .position(|candidate| candidate == node_id)
                    .unwrap_or(usize::MAX)
            });
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts
                    .into_iter()
                    .map(|(_, node_id, result)| (node_id, result))
                    .collect(),
                fanout_stats,
            ))
        }
    }
}

pub async fn search_buckets_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: BucketSearchRequest,
) -> Result<BucketSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let limit = request.limit.clamp(1, 50);
    let subject = query_fingerprint(
        &request.query,
        None,
        Some(MetadataApiQueryMode::Distributed),
        None,
        None,
    );
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(request.bearer_token.as_deref());
    let local_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (
            context.clone(),
            request.auth,
            realm_id,
            request.query.clone(),
            limit,
        ),
        |(context, auth, realm_id, query, limit), node_id| async move {
            search_local_buckets(
                &context,
                SearchBucketsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    limit,
                    start_after: None,
                },
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (handle, remote_auth_token, request.query, limit),
        |(handle, auth_token, query, limit), node_id| async move {
            handle
                .request_bucket_search(node_id, auth_token, query, limit)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(
            Some(MetadataApiQueryMode::Distributed),
            request.target_nodes,
            true,
        )
        .with_subject(subject)
        .with_deadline(deadline),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        record_bucket_result,
        map_read_error,
    )
    .await?;
    let mut hits = parts
        .into_iter()
        .flat_map(|(_, hits)| hits)
        .collect::<Vec<_>>();
    hits.truncate(limit);
    Ok(BucketSearchExecution { hits, fanout_stats })
}
