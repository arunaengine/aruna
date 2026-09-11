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
