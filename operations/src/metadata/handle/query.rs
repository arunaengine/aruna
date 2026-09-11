use super::*;
#[tracing::instrument(
    name = "metadata.query.local",
    level = "debug",
    skip(inner, auth_context, sparql),
    fields(
        query_len = sparql.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_query_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
        cache = field::Empty,
    )
)]
pub(super) async fn query_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    let query = parse_metadata_query(&sparql)?;
    if graph_iris
        .as_ref()
        .is_some_and(|graphs| graphs.len() > METADATA_REGISTRY_CANDIDATE_LIMIT)
    {
        return Err(MetadataError::Backend(
            "metadata candidate limit exceeded".to_string(),
        ));
    }
    // Stamped before any read so a mutation racing this query invalidates the
    // entry it stores.
    let cache_stamp = inner
        .query_cache
        .stamp(inner.visibility_cache.current_generation());

    let records = list_registry_records_for_local_read(inner.clone(), &span).await?;

    let authorization_started = Instant::now();
    // Document-scoped queries keep the eager per-record selection; the
    // all-metadata path defers per-graph visibility to query evaluation.
    let scope = match graph_iris {
        Some(graph_iris) => {
            let requested_graphs = graph_iris.iter().collect::<HashSet<_>>().len();
            let allowed =
                select_authorized_graphs(inner.clone(), auth_context, records, Some(graph_iris))
                    .await?;
            if allowed.len() != requested_graphs {
                return Err(MetadataError::GraphNotFound);
            }
            LocalReadScope::Eager(allowed)
        }
        None => LocalReadScope::Lazy(
            resolve_graph_visibility_scope(&inner, auth_context, records).await?,
        ),
    };
    record_elapsed_ms(&span, "authorization_ms", authorization_started);
    let lazy = match &scope {
        LocalReadScope::Eager(allowed) => {
            span.record("authorized_graphs", allowed.len() as u64);
            false
        }
        LocalReadScope::Lazy(scope) => {
            span.record("readable_groups", scope.permissions.group_count() as u64);
            true
        }
    };

    let cache_key = match &scope {
        LocalReadScope::Eager(allowed) => {
            local_key(LocalScopeKind::Eager, &graphs_digest(allowed), &sparql)
        }
        LocalReadScope::Lazy(scope) => local_key(
            LocalScopeKind::Lazy,
            &scope.visible_digest(&inner.visibility_cache),
            &sparql,
        ),
    };
    if let Some(cached) = inner
        .query_cache
        .get(&cache_key, cache_stamp, Instant::now())
    {
        span.record("cache", "hit");
        span.record("result", cached.results.kind());
        record_metadata_query_result_counts(&span, &cached.results);
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        return Ok((*cached.results).clone());
    }
    span.record("cache", "miss");

    let query_span = debug_span!(
        "metadata.backend.craqle.query_graphs",
        lazy,
        graph_count = field::Empty,
        query_len = sparql.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    );
    if let LocalReadScope::Eager(allowed) = &scope {
        query_span.record("graph_count", allowed.len() as u64);
    }
    let blocking_span = query_span.clone();
    let cache_inner = inner.clone();
    let query_started = Instant::now();
    // Queries are reads: take from the read pool so they never queue behind
    // long-running materializations holding the mutation permits.
    let query_deadline = tokio::time::Instant::now() + METADATA_QUERY_DEADLINE;
    let permit = tokio::time::timeout_at(
        query_deadline,
        inner.craqle_read_permits.clone().acquire_owned(),
    )
    .await
    .map_err(|_| {
        MetadataError::InvalidInput(format!(
            "metadata query timed out after {} seconds",
            METADATA_QUERY_DEADLINE.as_secs()
        ))
    })?
    .ok();
    let cancellation = CancellationToken::new();
    let _cancel_on_drop = MetadataQueryCancellationGuard(cancellation.clone());
    let blocking_cancellation = cancellation.clone();
    let mut blocking = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        blocking_span.in_scope(|| {
            evaluate_metadata_query_snapshot(&inner, scope, &query, &blocking_cancellation)
        })
    });
    let result = match tokio::time::timeout_at(query_deadline, &mut blocking).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(MetadataError::TaskJoin(error.to_string())),
        Err(_) => {
            cancellation.cancel();
            Err(MetadataError::InvalidInput(format!(
                "metadata query timed out after {} seconds",
                METADATA_QUERY_DEADLINE.as_secs()
            )))
        }
    };
    let query_elapsed = query_started.elapsed();
    record_duration_ms(&query_span, "elapsed_ms", query_elapsed);
    record_duration_ms(&span, "craqle_query_ms", query_elapsed);
    match &result {
        Ok(results) => {
            query_span.record("result", results.kind());
            span.record("result", results.kind());
            record_metadata_query_result_counts(&query_span, results);
            record_metadata_query_result_counts(&span, results);
            let stored = cache_inner.query_cache.insert(
                cache_key,
                CachedQuery {
                    results: Arc::new(results.clone()),
                    nodes_queried: 0,
                },
                cache_stamp,
                cache_inner.visibility_cache.current_generation(),
                Instant::now(),
            );
            if stored {
                span.record("cache", "stored");
            }
        }
        Err(error) => {
            record_error(&query_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    warn_if_slow_metadata_backend("query_graphs", None, query_elapsed);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    result
}
