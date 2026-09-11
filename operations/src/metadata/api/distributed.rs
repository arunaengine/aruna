use super::*;

pub(super) fn object_search_fingerprint(
    realm_id: RealmId,
    query: &str,
    key_match: ObjectKeyMatch,
    bucket: Option<&str>,
    mode: ObjectSearchQueryMode,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna.object.search.v1\0");
    hasher.update(realm_id.as_bytes());
    hasher.update(query.as_bytes());
    hasher.update(&[0]);
    hasher.update(&[match key_match {
        ObjectKeyMatch::Substring => 1,
        ObjectKeyMatch::Prefix => 2,
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
        ObjectSearchQueryMode::Local => 1,
        ObjectSearchQueryMode::DistributedBestEffort => 2,
        ObjectSearchQueryMode::DistributedStrict => 3,
    }]);
    *hasher.finalize().as_bytes()
}

pub(super) fn record_object_result(
    span: &Span,
    result: &Result<ObjectSearchNodePage, MetadataReadError>,
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

pub(super) fn record_bucket_result(
    span: &Span,
    result: &Result<Vec<BucketSearchHit>, MetadataReadError>,
) {
    match result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
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

pub(super) fn record_search_node_result(
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

pub(super) fn record_preflight_node_result(
    span: &Span,
    result: &Result<MetadataReferencePreflightNodeExecution, MetadataReadError>,
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
    let mode = scope.mode.unwrap_or(MetadataApiQueryMode::Distributed);
    let single_dataset_result = mode == MetadataApiQueryMode::Local || graph_iris.is_some();
    if mode == MetadataApiQueryMode::Distributed
        && graph_iris.is_none()
        && !distributed_query_is_union_safe(&query)
    {
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

    // Remote partitions authorize on the forwarded credential, so entries are
    // partitioned by credential digest. The local invalidation signals only
    // cover the local partition; the TTL bounds remote staleness.
    let cache_stamp = handle.query_cache().stamp(handle.visibility_generation());
    let cache_key =
        crate::metadata::query_cache::credential_digest(auth.as_ref(), bearer_token.as_deref())
            .map(|credential| {
                crate::metadata::query_cache::remote_key(
                    &crate::metadata::query_cache::RemoteKeyInput {
                        distributed: mode == MetadataApiQueryMode::Distributed,
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

    let local_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
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
            query.clone(),
        ),
        |(handle, auth_token, graph_iris, query), node_id| async move {
            handle
                .request_remote_query_graphs(node_id, auth_token, graph_iris, query)
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
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile_validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .search_authorized_local_filtered(
                                    auth.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    crate::metadata::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
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
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile_validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .request_remote_filtered_search_graphs(
                                    node_id,
                                    auth_token.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    crate::metadata::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
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
                        .request_remote_search_graphs(
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
        record_search_node_result,
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
    let page = paginate(
        node_results,
        watermark,
        page_size,
        METADATA_SEARCH_MAX_PAGINATION_DEPTH,
    );
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
            let row_limit = select_limit
                .unwrap_or(METADATA_QUERY_MAX_ROWS)
                .min(METADATA_QUERY_MAX_ROWS);
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
                        if merged_bytes > METADATA_QUERY_MAX_RESULT_BYTES {
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

pub fn query_form(query: &str) -> Option<MetadataQueryForm> {
    match spargebra::SparqlParser::new().parse_query(query).ok()? {
        spargebra::Query::Select { .. } => Some(MetadataQueryForm::Select),
        spargebra::Query::Ask { .. } => Some(MetadataQueryForm::Ask),
        _ => None,
    }
}
