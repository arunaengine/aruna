use super::*;

pub async fn query_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataDocumentQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_supported_query_form(&request.query)?;
    let record = load_record_by_document(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record, None).await?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    if request.mode == Some(MetadataApiQueryMode::Local) {
        ensure_record_materialized_for_graph_read(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_metadata_query_error)?;
        return Ok(MetadataQueryExecution {
            results,
            fanout_stats: MetadataFanoutStats {
                nodes_queried: 1,
                ..MetadataFanoutStats::default()
            },
        });
    }

    let Some(config) = load_realm_config(context, realm_id).await else {
        if !request.allow_partial {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        ensure_record_materialized_for_graph_read(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_metadata_query_error)?;
        return Ok(MetadataQueryExecution {
            results,
            fanout_stats: MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 1,
                discovery_failed: true,
                ..MetadataFanoutStats::default()
            },
        });
    };
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let mut holders = document_replica_query_nodes(Some(&config), &record, local_node_id);
    if let Some(index) = holders.iter().position(|holder| *holder == local_node_id) {
        holders.swap(0, index);
    }
    let remote_auth = match request.bearer_token.as_deref() {
        Some(token) => {
            Some(MetadataAuthToken::bearer(token).map_err(|_| MetadataApiError::BadRequest)?)
        }
        None => request.auth.clone().map(MetadataAuthToken::internal),
    };
    let mut fanout_stats = MetadataFanoutStats::default();
    let mut auth_error = None;
    for holder in holders {
        fanout_stats.nodes_queried += 1;
        let result: Result<MetadataQueryResults, MetadataReadError> = if holder == local_node_id {
            match ensure_record_materialized_for_graph_read(context, &record).await {
                Ok(()) => metadata
                    .query_authorized_local(
                        request.auth.clone(),
                        Some(vec![record.graph_iri.clone()]),
                        request.query.clone(),
                    )
                    .await
                    .map_err(|_| MetadataReadError::Unavailable),
                Err(_) => Err(MetadataReadError::Unavailable),
            }
        } else {
            metadata
                .request_document_query(
                    holder,
                    remote_auth.clone(),
                    config_digest,
                    request.document_id,
                    request.query.clone(),
                )
                .await
        };
        match result {
            Ok(results) => {
                // First replica success answers the query; a lagging replica's
                // NotFound must not override an in-hand result.
                fanout_stats.nodes_failed = 0;
                fanout_stats.failed_partitions.clear();
                return Ok(MetadataQueryExecution {
                    results,
                    fanout_stats,
                });
            }
            Err(MetadataReadError::Unauthorized) => {
                auth_error.get_or_insert(MetadataApiError::Unauthorized);
            }
            Err(MetadataReadError::Forbidden) => {
                auth_error.get_or_insert(MetadataApiError::Forbidden);
            }
            Err(MetadataReadError::NotFound) => {}
            Err(MetadataReadError::Unavailable) => {
                fanout_stats.nodes_failed += 1;
                fanout_stats.failed_partitions.push(holder);
                warn!(%holder, "Document query holder unavailable; trying the next replica");
            }
        }
    }
    if let Some(error) = auth_error {
        return Err(error);
    }
    Err(MetadataApiError::ServiceUnavailable)
}

pub async fn query_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    ensure_supported_query_form(&request.query)?;
    let subject = query_fingerprint(
        &request.query,
        request.graph_iris.as_deref(),
        request.mode,
        None,
        None,
    );
    let (results, fanout_stats) = run_query_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        MetadataFanoutScope::new(request.mode, request.target_nodes, request.allow_partial)
            .with_subject(subject)
            .with_deadline(deadline),
    )
    .await?;
    Ok(MetadataQueryExecution {
        results,
        fanout_stats,
    })
}

pub async fn search_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    mut request: MetadataSearchRequest,
) -> Result<MetadataSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    if request.query.trim().is_empty() && request.conforms_to.is_none() {
        return Err(MetadataApiError::BadRequest);
    }
    if request
        .conforms_to
        .as_deref()
        .is_some_and(|iri| oxrdf::NamedNode::new(iri).is_err())
    {
        return Err(MetadataApiError::BadRequest);
    }
    if let Some(iri) = request.conforms_to.take() {
        request.conforms_to = crate::metadata::profile_validation::equivalent_profile_iris(&iri)
            .into_iter()
            .next();
    }
    let page_size = request
        .limit
        .unwrap_or(METADATA_SEARCH_DEFAULT_PAGE_SIZE)
        .clamp(1, METADATA_SEARCH_MAX_PAGE_SIZE);

    let fingerprint = query_fingerprint(
        &request.query,
        request.graph_iris.as_deref(),
        request.mode,
        request.conforms_to.as_deref(),
        request.group_id,
    );
    let mut cursor_discovery = None;
    let (watermark, resume) = match request.cursor.as_deref() {
        Some(raw) => {
            // Cursor signers are authorized against the full realm node set:
            // the serving node's capped fan-out selection may exclude the node
            // that legitimately signed the previous page.
            let signer_nodes = match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => match request.target_nodes.as_ref() {
                    Some(nodes) => {
                        let mut signers = nodes.clone();
                        signers.push(local_node_id);
                        signers
                    }
                    None => {
                        let discovery = tokio::time::timeout_at(
                            deadline,
                            discover_realm_nodes(context, realm_id, local_node_id),
                        )
                        .await
                        .unwrap_or(MetadataRealmNodeDiscovery {
                            nodes: vec![local_node_id],
                            failed: true,
                        });
                        let mut signers = discovery.nodes.clone();
                        signers.push(local_node_id);
                        let nodes =
                            select_fanout_nodes(&discovery.nodes, local_node_id, &fingerprint);
                        let mut discovery = discovery;
                        discovery.nodes = nodes;
                        cursor_discovery = Some(discovery);
                        signers
                    }
                },
            };
            let cursor = SearchCursor::decode(raw, &signer_nodes)
                .map_err(|error| MetadataApiError::InvalidCursor(error.to_string()))?;
            if cursor.fingerprint != fingerprint {
                return Err(MetadataApiError::InvalidCursor(
                    SearchCursorError::QueryMismatch.to_string(),
                ));
            }
            (
                Some(cursor.payload.watermark.clone()),
                cursor.resume_positions(),
            )
        }
        None => (None, HashMap::new()),
    };

    // Keep resumed nodes in the bounded selection so remaining hits are not
    // silently discarded when discovery changes.
    let (target_nodes, discovery_failed) = if request.cursor.is_some() {
        let mut nodes = match request.target_nodes.as_ref() {
            Some(nodes) => select_fanout_nodes(nodes, local_node_id, &fingerprint),
            None => match request.mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => cursor_discovery
                    .as_ref()
                    .map(|discovery| discovery.nodes.clone())
                    .unwrap_or_else(|| vec![local_node_id]),
            },
        };
        for node_id in resume.keys() {
            if !nodes.contains(node_id) {
                nodes.push(*node_id);
            }
        }
        (
            Some(deduplicate_fanout_nodes(nodes)),
            cursor_discovery
                .as_ref()
                .is_some_and(|discovery| discovery.failed),
        )
    } else {
        (request.target_nodes.take(), false)
    };

    let (hits, next, truncated, fanout_stats) = run_search_distributed(
        context,
        realm_id,
        local_node_id,
        request.auth,
        request.bearer_token,
        request.graph_iris,
        request.query,
        request.conforms_to,
        request.group_id,
        resume,
        watermark,
        page_size,
        MetadataFanoutScope::new(request.mode, target_nodes, true)
            .with_subject(fingerprint)
            .with_discovery_failed(discovery_failed)
            .with_deadline(deadline),
    )
    .await?;
    let next_cursor = match next {
        Some(cursor) => {
            let net = context.net_handle.as_ref().ok_or_else(|| {
                MetadataApiError::Internal(
                    "net handle unavailable for search cursor signing".to_string(),
                )
            })?;
            Some(
                SearchCursor::new_signed(
                    fingerprint,
                    cursor.watermark,
                    cursor.resume,
                    net.node_id(),
                    |bytes| net.sign(bytes),
                )
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .encode()
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
            )
        }
        None => None,
    };
    Ok(MetadataSearchExecution {
        hits,
        next_cursor,
        truncated,
        fanout_stats,
    })
}

/// Backlink lookup: scans the local IRI reference index for documents naming
/// `iri` as an object, joins and filters by read access. Empty scans for known
/// graph IRIs or `resolve` return one predicate-less summary. Local-node-only in v1.
