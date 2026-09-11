use super::*;

#[tracing::instrument(
    name = "metadata.search.local",
    level = "debug",
    skip(inner, auth_context, query, iri_filter),
    fields(
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        readable_groups = field::Empty,
        lazy = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_search_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn search_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
    iri_filter: Option<(String, String)>,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    if graph_iris
        .as_ref()
        .is_some_and(|graphs| graphs.len() > METADATA_REGISTRY_CANDIDATE_LIMIT)
    {
        return Err(MetadataError::Backend(
            "metadata candidate limit exceeded".to_string(),
        ));
    }

    let records = list_registry_records_for_local_read(inner.clone(), &span).await?;
    // Without a candidate filter the request spans the realm, so craqle
    // authorizes the index hits it returns instead of every visible graph.
    let lazy = iri_filter.is_none() && graph_iris.is_none() && group_id.is_none();
    span.record("lazy", lazy);

    let result = if lazy {
        search_realm_scope(inner, auth_context, records, query, limit, &span).await
    } else {
        search_candidate_graphs(
            inner,
            auth_context,
            records,
            graph_iris,
            query,
            limit,
            group_id,
            iri_filter,
            &span,
        )
        .await
    };

    match &result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(error) => record_error(&span, &error.to_string()),
    }
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    result
}

pub(super) fn record_backend_search(
    span: &Span,
    search_span: &Span,
    started: Instant,
    result: &Result<Vec<MetadataSearchHit>, MetadataError>,
) {
    let elapsed = started.elapsed();
    record_duration_ms(search_span, "elapsed_ms", elapsed);
    record_duration_ms(span, "craqle_search_ms", elapsed);
    match result {
        Ok(hits) => {
            search_span.record("result", "ok");
            search_span.record("hit_count", hits.len() as u64);
        }
        Err(error) => record_error(search_span, &error.to_string()),
    }
    warn_if_slow_metadata_backend("search", None, elapsed);
}

async fn search_realm_scope(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    query: String,
    limit: usize,
    span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let authorization_started = Instant::now();
    let scope = resolve_graph_visibility_scope(&inner, auth_context, records)
        .boxed()
        .await?;
    record_elapsed_ms(span, "authorization_ms", authorization_started);
    span.record("readable_groups", scope.permissions.group_count() as u64);
    if limit == 0 || scope.records.is_empty() {
        return Ok(Vec::new());
    }

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = true,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let search_started = Instant::now();
    let result = search_visible_scope(&inner, Arc::new(scope), query, limit, &search_span).await;
    record_backend_search(span, &search_span, search_started, &result);
    result
}
