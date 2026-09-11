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

// Craqle post-filters the index hits it returns through the request's already
// resolved scope, so the work scales with the page instead of the realm.
async fn search_visible_scope(
    inner: &Arc<MetadataInner>,
    scope: Arc<GraphVisibilityScope>,
    query: String,
    limit: usize,
    search_span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let describe = scope_hit_describe(inner, &scope);
    let mut hits = {
        let task_inner = inner.clone();
        let task_scope = scope.clone();
        let task_query = query.clone();
        let blocking_span = search_span.clone();
        let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            blocking_span.in_scope(|| {
                let authorizer = ScopeAuthorizer {
                    scope: &task_scope,
                    visibility_cache: &task_inner.visibility_cache,
                };
                task_inner
                    .node
                    .search(
                        &authorizer,
                        SearchRequest {
                            query: &task_query,
                            limit,
                        },
                    )
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
        })
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))??
    };
    hits.retain(|hit| scope.record_for_graph(&hit.graph_id).is_some());
    let targets = hits
        .iter()
        .map(|hit| (hit.graph_id.clone(), hit.subject_iri.clone()))
        .collect::<Vec<_>>();
    let properties =
        describe_hits_parallel(&inner.craqle_read_permits, targets, describe, search_span).await;
    let mut visible = hits
        .into_iter()
        .zip(properties)
        .filter_map(|(hit, properties)| {
            let record = scope.record_for_graph(&hit.graph_id)?;
            Some(metadata_search_hit_from_craqle(
                hit,
                record,
                &properties,
                &query,
            ))
        })
        .collect::<Vec<_>>();
    // The unfiltered entry point returns raw index order; the search cursor's
    // watermark needs the merged score-descending order instead.
    visible.sort_by(compare_hits);
    Ok(visible)
}

// Requests narrowed by graph, group, or IRI filter keep a genuine candidate
// set, so pre-filtering it stays cheaper than post-filtering the whole index.
#[allow(clippy::too_many_arguments)]
async fn search_candidate_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
    group_id: Option<GroupId>,
    iri_filter: Option<(String, String)>,
    span: &Span,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let iri_matches = match iri_filter.as_ref() {
        Some((predicate_iri, object_iri)) => Some(
            super::super::iri_index::lookup_metadata_iri_references(
                &inner.storage_handle,
                records.as_ref(),
                predicate_iri,
                object_iri,
            )
            .await?,
        ),
        None => None,
    };
    let records = match iri_matches.as_ref() {
        Some(matches) => Arc::new(
            records
                .iter()
                .filter(|record| matches.contains_key(&record.document_id))
                .cloned()
                .collect(),
        ),
        None => records,
    };

    let authorization_started = Instant::now();
    let allowed_records =
        select_authorized_records(inner.clone(), auth_context, records, graph_iris, group_id)
            .await?;
    record_elapsed_ms(span, "authorization_ms", authorization_started);
    span.record("authorized_graphs", allowed_records.len() as u64);

    if limit == 0 || allowed_records.is_empty() {
        return Ok(Vec::new());
    }

    if query.trim().is_empty()
        && let Some(matches) = iri_matches
    {
        let allowed_graphs = allowed_records
            .iter()
            .map(|record| record.graph_iri.clone())
            .collect::<HashSet<_>>();
        let describe = allowed_hit_describe(
            &inner,
            Arc::new(AllowedGraphAuthorizer {
                graph_iris: allowed_graphs,
            }),
        );
        // The page is ordered by document id alone, so it is cut before the
        // describes instead of enriching every candidate.
        let mut candidates = allowed_records
            .into_iter()
            .filter_map(|record| {
                let subject_iri = matches.get(&record.document_id)?.first()?.clone();
                Some((record, subject_iri))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(record, _)| record.document_id);
        candidates.truncate(limit);
        let targets = candidates
            .iter()
            .map(|(record, subject_iri)| (record.graph_iri.clone(), subject_iri.clone()))
            .collect::<Vec<_>>();
        let properties =
            describe_hits_parallel(&inner.craqle_read_permits, targets, describe, span).await;
        return Ok(candidates
            .into_iter()
            .zip(properties)
            .map(|((record, subject_iri), properties)| MetadataSearchHit {
                document_id: record.document_id.to_string(),
                group_id: record.group_id.to_string(),
                title: hit_title(&properties, &record.document_path, &subject_iri),
                subject_types: hit_types(&properties),
                document_path: record.document_path,
                graph_iri: record.graph_iri,
                subject_iri,
                score: 1.0,
                snippet: None,
            })
            .collect());
    }

    let by_graph: HashMap<_, _> = allowed_records
        .into_iter()
        .map(|record| (record.graph_iri.clone(), record))
        .collect();
    let allowed_graphs = by_graph.keys().cloned().collect::<HashSet<_>>();
    let mut search_graphs = allowed_graphs.iter().cloned().collect::<Vec<_>>();
    search_graphs.sort_unstable();
    let graph_ids = graph_ids(&search_graphs);

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = false,
        graph_count = graph_ids.len() as u64,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let search_started = Instant::now();
    let authorizer = Arc::new(AllowedGraphAuthorizer {
        graph_iris: allowed_graphs,
    });
    let result = search_allowed_graphs(
        &inner,
        authorizer,
        by_graph,
        graph_ids,
        &query,
        limit,
        &search_span,
    )
    .await;
    record_backend_search(span, &search_span, search_started, &result);
    result
}
