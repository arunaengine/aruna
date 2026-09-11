use super::*;
use crate::metadata::search_cursor::{
    METADATA_SEARCH_DEFAULT_PAGE_SIZE, METADATA_SEARCH_MAX_PAGE_SIZE,
};

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
pub async fn references_metadata(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataReferencesRequest,
) -> Result<MetadataReferencesExecution, MetadataApiError> {
    if request.iri.trim().is_empty() || oxrdf::NamedNode::new(&request.iri).is_err() {
        return Err(MetadataApiError::BadRequest);
    }
    if request
        .predicate
        .as_deref()
        .is_some_and(|iri| oxrdf::NamedNode::new(iri).is_err())
    {
        return Err(MetadataApiError::BadRequest);
    }
    let limit = request
        .limit
        .unwrap_or(METADATA_REFERENCES_DEFAULT_LIMIT)
        .clamp(1, METADATA_REFERENCES_MAX_LIMIT);

    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let registry = handle
        .list_cached_registry_records()
        .await
        .map_err(map_metadata_internal_error)?;
    let registry = filter_live_records(&context.storage_handle, registry.as_ref()).await?;

    if request.resolve {
        let entry = resolve_graph_reference(context, realm_id, &request, registry.as_ref()).await?;
        return Ok(MetadataReferencesExecution {
            references: entry.into_iter().collect(),
        });
    }

    let backlinks = crate::metadata::iri_index::lookup_iri_backlinks(
        &context.storage_handle,
        registry.as_ref(),
        &request.iri,
        request.predicate.as_deref(),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;

    let registry_by_id: HashMap<Ulid, &MetadataRegistryRecord> = registry
        .iter()
        .map(|record| (record.document_id, record))
        .collect();

    let mut references = Vec::new();
    let mut authorized: HashMap<Ulid, bool> = HashMap::new();
    let mut titles: HashMap<Ulid, Option<String>> = HashMap::new();
    for backlink in backlinks {
        let Some(record) = registry_by_id.get(&backlink.document_id) else {
            continue;
        };
        let allowed = match authorized.get(&backlink.document_id) {
            Some(allowed) => *allowed,
            None => {
                let allowed =
                    can_read_record(context, realm_id, request.auth.as_ref(), record).await?;
                authorized.insert(backlink.document_id, allowed);
                allowed
            }
        };
        if !allowed {
            continue;
        }
        let title = match titles.get(&backlink.document_id) {
            Some(title) => title.clone(),
            None => {
                let title = reference_document_title(context, record).await;
                titles.insert(backlink.document_id, title.clone());
                title
            }
        };
        references.push(MetadataReferenceEntry {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            predicate: Some(backlink.predicate_iri),
            subject_iris: backlink.subject_iris,
            title,
        });
        if references.len() >= limit {
            break;
        }
    }

    if references.is_empty()
        && let Some(entry) =
            resolve_graph_reference(context, realm_id, &request, registry.as_ref()).await?
    {
        references.push(entry);
    }

    Ok(MetadataReferencesExecution { references })
}

pub(super) fn effective_list_limit(requested: Option<usize>, anonymous: bool) -> usize {
    let maximum = if anonymous {
        ANONYMOUS_LIST_METADATA_LIMIT
    } else {
        MAX_LIST_METADATA_LIMIT
    };
    requested
        .unwrap_or(DEFAULT_LIST_METADATA_LIMIT)
        .clamp(1, maximum)
}

pub(super) fn check_policy_limit(
    group_ids: Vec<GroupId>,
) -> Result<Vec<GroupId>, MetadataApiError> {
    if group_ids.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(group_ids)
}

pub(super) async fn load_group_records(
    context: &DriverContext,
    group_id: GroupId,
    limit: usize,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    let records = if let Some(metadata_handle) = context.metadata_handle.as_ref() {
        // Listing remains eventually consistent: the handle-owned visibility
        // cache serves stale snapshots while one refill updates the read path.
        metadata_handle
            .list_group_records(group_id, limit)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?
            .as_ref()
            .clone()
    } else {
        let mut records = Vec::new();
        let mut start_after = None;
        loop {
            let event = context
                .storage_handle
                .send_effect(iter_registry_effect(group_id, start_after, None))
                .await;
            let (page, next_start_after) =
                parse_registry_iter(event).map_err(|_| MetadataApiError::ServiceUnavailable)?;
            if records.len().saturating_add(page.len()) > limit {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            records.extend(page);
            match next_start_after {
                Some(cursor) => start_after = Some(cursor),
                None => break,
            }
        }
        records
    };
    filter_live_records(&context.storage_handle, &records).await
}

pub(crate) async fn filter_live_records(
    storage: &StorageHandle,
    records: &[MetadataRegistryRecord],
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    if records.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut reads = Vec::with_capacity(records.len().saturating_mul(2));
    for record in records {
        reads.push((
            METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            metadata_graph_lifecycle_key(&record.graph_iri),
        ));
        reads.push((
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            metadata_document_lifecycle_key(record.document_id),
        ));
    }
    let values = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };
    if values.len() != records.len().saturating_mul(2) {
        return Err(MetadataApiError::Internal(format!(
            "metadata lifecycle batch returned {} values for {} records",
            values.len(),
            records.len()
        )));
    }

    let mut live = Vec::with_capacity(records.len());
    for (record, pair) in records.iter().zip(values.as_chunks::<2>().0) {
        let (graph_key, graph_value) = &pair[0];
        if graph_key != &metadata_graph_lifecycle_key(&record.graph_iri) {
            return Err(MetadataApiError::Internal(
                "metadata graph lifecycle batch key mismatch".to_string(),
            ));
        }
        let graph_deleted = graph_value
            .as_ref()
            .map(|value| graph_lifecycle_deleted(record, value))
            .transpose()?
            .unwrap_or(false);

        let (document_key, document_value) = &pair[1];
        if document_key != &metadata_document_lifecycle_key(record.document_id) {
            return Err(MetadataApiError::Internal(
                "metadata document lifecycle batch key mismatch".to_string(),
            ));
        }
        let document_deleted = document_value
            .as_ref()
            .map(|value| document_lifecycle_deleted(record, value))
            .transpose()?
            .unwrap_or(false);
        if !graph_deleted && !document_deleted {
            live.push(record.clone());
        }
    }
    Ok(live)
}
