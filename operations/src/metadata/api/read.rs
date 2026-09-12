use super::distributed::{run_query_distributed, run_search_distributed};
use super::fanout::ensure_query_form;
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
    ensure_query_form(&request.query)?;
    let record = load_document_record(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record, None).await?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    if request.mode == Some(MetadataApiQueryMode::Local) {
        ensure_record_materialized(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_query_error)?;
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
        ensure_record_materialized(context, &record).await?;
        let results = metadata
            .query_authorized_local(request.auth, Some(vec![record.graph_iri]), request.query)
            .await
            .map_err(map_query_error)?;
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
    let mut holders = replica_query_nodes(Some(&config), &record, local_node_id);
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
            match ensure_record_materialized(context, &record).await {
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
    ensure_query_form(&request.query)?;
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
            // Check the full realm because capped fan-out may omit the cursor's signer.
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
        .list_cached_records()
        .await
        .map_err(map_internal_error)?;
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
            graph_lifecycle_key(&record.graph_iri),
        ));
        reads.push((
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            document_lifecycle_key(record.document_id),
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
        if graph_key != &graph_lifecycle_key(&record.graph_iri) {
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
        if document_key != &document_lifecycle_key(record.document_id) {
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

pub(super) fn graph_lifecycle_deleted(
    record: &MetadataRegistryRecord,
    value: &[u8],
) -> Result<bool, MetadataApiError> {
    let lifecycle: MetadataGraphLifecycleRecord = postcard::from_bytes(value)
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    if lifecycle.graph_iri != record.graph_iri
        || lifecycle.realm_id != record.realm_id
        || lifecycle.group_id != record.group_id
        || lifecycle.document_id != record.document_id
    {
        return Err(MetadataApiError::Internal(
            "metadata graph lifecycle record mismatch".to_string(),
        ));
    }
    Ok(lifecycle.is_deleted())
}

pub(super) fn document_lifecycle_deleted(
    record: &MetadataRegistryRecord,
    value: &[u8],
) -> Result<bool, MetadataApiError> {
    let lifecycle: MetadataDocumentLifecycleRecord = postcard::from_bytes(value)
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let matches = match &lifecycle {
        MetadataDocumentLifecycleRecord::Upsert { event } => {
            event.record.document_id == record.document_id
                && event.record.graph_iri == record.graph_iri
                && event.record.realm_id == record.realm_id
                && event.record.group_id == record.group_id
        }
        MetadataDocumentLifecycleRecord::Delete { event } => {
            event.tombstone.document_id == record.document_id
                && event.tombstone.graph_iri == record.graph_iri
                && event.tombstone.realm_id == record.realm_id
                && event.tombstone.group_id == record.group_id
        }
    };
    if !matches {
        return Err(MetadataApiError::Internal(
            "metadata document lifecycle record mismatch".to_string(),
        ));
    }
    Ok(matches!(
        lifecycle,
        MetadataDocumentLifecycleRecord::Delete { .. }
    ))
}

pub(super) async fn load_claim_records(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    let group_ids = check_policy_limit(match group_id {
        Some(group_id) => vec![group_id],
        None => drive(
            ListGroupOperation::with_pagination(METADATA_REGISTRY_CANDIDATE_LIMIT + 1, 0),
            context,
        )
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .into_iter()
        .map(|group| group.group_id)
        .collect(),
    })?;
    let mut pending =
        load_pending_records(context, group_id, METADATA_REGISTRY_CANDIDATE_LIMIT).await?;
    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
        let mut group_records = load_group_records(context, group_id, remaining).await?;
        if let Some(pending_records) = pending.remove(&group_id) {
            merge_pending_records(&mut group_records, pending_records);
            if group_records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
        }
        group_records.sort_by_key(|record| record.document_id);
        records.extend(group_records);
    }
    for pending_records in pending.into_values() {
        if records.len().saturating_add(pending_records.len()) > METADATA_REGISTRY_CANDIDATE_LIMIT {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        records.extend(pending_records);
    }
    records.retain(|record| record.realm_id == realm_id);
    Ok(records)
}

pub(super) async fn load_pending_records(
    context: &DriverContext,
    group_filter: Option<GroupId>,
    limit: usize,
) -> Result<HashMap<GroupId, Vec<MetadataRegistryRecord>>, MetadataApiError> {
    let limit = limit.min(METADATA_REGISTRY_CANDIDATE_LIMIT);
    let mut targets = Vec::with_capacity(limit);
    let mut start_after = None;
    let mut scanned = 0usize;

    loop {
        let page = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: LIST_METADATA_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match page {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataApiError::Internal(error.to_string()));
            }
            other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
        };
        scanned = scanned.saturating_add(values.len());
        if scanned > limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }

        targets.extend(
            values
                .into_iter()
                .filter_map(|(key, _)| pending_projection_target(key.as_ref())),
        );

        if next_start_after.is_none() {
            break;
        }
        start_after = next_start_after;
    }

    if targets.is_empty() {
        return Ok(HashMap::new());
    }

    let event_reads = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                METADATA_EVENT_LOG_KEYSPACE.to_string(),
                event_log_key(*document_id, *event_id),
            )
        })
        .collect::<Vec<_>>();
    let event_values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: event_reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values })
            if values.len() == targets.len() =>
        {
            values
        }
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            return Err(MetadataApiError::Internal(format!(
                "metadata pending event batch returned {} values for {} targets",
                values.len(),
                targets.len()
            )));
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(MetadataApiError::Internal(error.to_string()));
        }
        other => return Err(MetadataApiError::Internal(format!("{other:?}"))),
    };

    let mut pending = Vec::with_capacity(event_values.len());
    for ((document_id, event_id), (key, value)) in targets.into_iter().zip(event_values) {
        if key != event_log_key(document_id, event_id) {
            return Err(MetadataApiError::Internal(
                "metadata pending event batch key mismatch".to_string(),
            ));
        }
        let Some(value) = value else {
            continue;
        };
        let event: MetadataCreateEventRecord = postcard::from_bytes(&value)
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        if event.record.document_id != document_id || event.event_id != event_id {
            return Err(MetadataApiError::Internal(format!(
                "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
                event.record.document_id, event.event_id
            )));
        }
        if group_filter.is_none_or(|group_id| event.record.group_id == group_id) {
            pending.push(event.record);
        }
    }

    if pending.is_empty() {
        return Ok(HashMap::new());
    }

    let pending = filter_live_records(&context.storage_handle, &pending).await?;
    let mut records: HashMap<GroupId, Vec<MetadataRegistryRecord>> = HashMap::new();
    for (count, record) in pending.into_iter().enumerate() {
        if count >= limit {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        records.entry(record.group_id).or_default().push(record);
    }
    Ok(records)
}

pub(super) async fn is_deleted(
    context: &DriverContext,
    graph_iri: &str,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            if record.graph_iri != graph_iri {
                return Err(MetadataApiError::Internal(
                    "metadata graph lifecycle record mismatch".to_string(),
                ));
            }
            Ok(record.is_deleted())
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

pub(super) fn merge_pending_records(
    records: &mut Vec<MetadataRegistryRecord>,
    pending_records: Vec<MetadataRegistryRecord>,
) {
    let mut positions = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.document_id, index))
        .collect::<HashMap<_, _>>();

    for pending_record in pending_records {
        if let Some(&index) = positions.get(&pending_record.document_id) {
            let existing_record = &records[index];
            if (pending_record.updated_at_ms, pending_record.last_event_id)
                > (existing_record.updated_at_ms, existing_record.last_event_id)
            {
                records[index] = pending_record;
            }
        } else {
            positions.insert(pending_record.document_id, records.len());
            records.push(pending_record);
        }
    }
}

pub(crate) async fn load_document_record(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    match load_document_record(context, document_id).await {
        Ok(Some(record)) => {
            filter_live_records(&context.storage_handle, std::slice::from_ref(&record))
                .await?
                .into_iter()
                .next()
                .ok_or(MetadataApiError::NotFound)
        }
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

pub(super) async fn load_record_txn(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let event = context
        .storage_handle
        .send_effect(read_document_registry(document_id, Some(txn_id)))
        .await;
    match parse_registry_read(event) {
        Ok(Some(record)) => {
            if record_deleted_txn(context, &record, txn_id).await? {
                Err(MetadataApiError::NotFound)
            } else {
                Ok(record)
            }
        }
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

async fn record_deleted_txn(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<bool, MetadataApiError> {
    Ok(graph_deleted_txn(context, record, txn_id).await?
        || document_deleted_txn(context, record, txn_id).await?)
}

async fn graph_deleted_txn(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: graph_lifecycle_key(&record.graph_iri),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => graph_lifecycle_deleted(record, &value),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

async fn document_deleted_txn(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<bool, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            key: document_lifecycle_key(record.document_id),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => document_lifecycle_deleted(record, &value),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
        other => Err(MetadataApiError::Internal(format!("{other:?}"))),
    }
}

pub(super) async fn ensure_record_materialized(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(), MetadataApiError> {
    match record_materialized_read(context, record).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(MetadataApiError::ServiceUnavailable),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
    }
}

/// The canonical `metadata.read` policy request for one record path and caller,
/// shared by the single-record and bulk visibility seams.
pub(crate) fn metadata_read_request(
    permission_path: &str,
    auth: Option<&AuthContext>,
) -> aruna_core::request_policy::PolicyRequest {
    crate::auth::request_policy::policy_request_with(
        permission_path,
        &Permission::READ,
        auth,
        crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
    )
}

pub(crate) async fn ensure_record_readable(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
    txn_id: Option<TxnId>,
) -> Result<(), MetadataApiError> {
    if record.public {
        // A policy denial on a found public record must read as NotFound, matching
        // the private-denied path, so read-by-id is not an existence oracle.
        let request = crate::auth::request_policy::policy_request_with(
            &record.permission_path,
            &Permission::READ,
            auth,
            crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
        );
        let result = match txn_id {
            Some(txn_id) => crate::auth::request_policy::PolicyEvaluator::load_with_txn(
                context,
                realm_id,
                record.group_id,
                txn_id,
            )
            .await
            .and_then(|evaluator| evaluator.evaluate(&request)),
            None => {
                crate::auth::request_policy::enforce_policies(context, realm_id, &request).await
            }
        };
        return result.map_err(|_| MetadataApiError::NotFound);
    }
    // Unreadable and absent records both return NotFound to prevent existence probing.
    let Some(auth) = auth.cloned() else {
        return Err(MetadataApiError::NotFound);
    };
    match ensure_permission(
        context,
        realm_id,
        auth,
        record.group_id,
        record.permission_path.clone(),
        Permission::READ,
        txn_id,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(MetadataApiError::Forbidden | MetadataApiError::Unauthorized) => {
            Err(MetadataApiError::NotFound)
        }
        Err(other) => Err(other),
    }
}

pub(crate) async fn can_read_record(
    context: &DriverContext,
    realm_id: RealmId,
    auth: Option<&AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<bool, MetadataApiError> {
    if record.public {
        let allowed = crate::auth::request_policy::enforce_policies(
            context,
            realm_id,
            &crate::auth::request_policy::policy_request_with(
                &record.permission_path,
                &Permission::READ,
                auth,
                crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
            ),
        )
        .await
        .is_ok();
        return Ok(allowed);
    }
    let Some(auth) = auth.cloned() else {
        return Ok(false);
    };
    if auth.realm_id != realm_id {
        return Ok(false);
    }

    match aruna_core::telemetry::time_stage(
        "permission",
        crate::auth::request_authorization::authorize(
            context,
            realm_id,
            &auth,
            &record.permission_path,
            &Permission::READ,
            crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
        ),
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

pub(super) async fn ensure_permission(
    context: &DriverContext,
    realm_id: RealmId,
    auth: AuthContext,
    group_id: GroupId,
    path: String,
    required_permission: Permission,
    txn_id: Option<TxnId>,
) -> Result<(), MetadataApiError> {
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let config = CheckPermissionsConfig {
        auth_context: auth.clone(),
        path: path.clone(),
        required_permission: required_permission.clone(),
    };
    let operation = match txn_id {
        Some(txn_id) => CheckPermissionsOperation::new_with_txn(config, txn_id),
        None => CheckPermissionsOperation::new(config),
    };
    let allowed = aruna_core::telemetry::time_stage("permission", drive(operation, context))
        .await
        .map_err(|err| match err {
            AuthorizationError::InvalidRealmId
            | AuthorizationError::InvalidGroupId
            | AuthorizationError::GroupNotFound
            | AuthorizationError::AuthDocNotFound => MetadataApiError::Forbidden,
            _ => MetadataApiError::Internal(err.to_string()),
        })?;
    if !allowed {
        return Err(MetadataApiError::Forbidden);
    }
    // Policies must see the permission the RBAC check enforced; a fixed read
    // would let a write-deny policy pass unevaluated.
    let request = crate::auth::request_policy::policy_request_with(
        &path,
        &required_permission,
        Some(&auth),
        crate::auth::request_policy::PolicyRequestExtras::operation("metadata.read"),
    );
    match txn_id {
        Some(txn_id) => crate::auth::request_policy::PolicyEvaluator::load_with_txn(
            context, realm_id, group_id, txn_id,
        )
        .await
        .and_then(|evaluator| evaluator.evaluate(&request))
        .map_err(|_| MetadataApiError::Forbidden)?,
        None => crate::auth::request_policy::enforce_policies(context, realm_id, &request)
            .await
            .map_err(|_| MetadataApiError::Forbidden)?,
    }
    Ok(())
}

pub(super) fn record_matches_filters(
    record: &MetadataRegistryRecord,
    path_prefix: Option<&str>,
) -> bool {
    path_prefix
        .map(|path_prefix| path_matches_prefix(&record.document_path, path_prefix))
        .unwrap_or(true)
}

fn path_matches_prefix(document_path: &str, path_prefix: &str) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(document_path);
    crate::placement::resolver::path_prefix_match(&normalized_path, path_prefix).is_some()
}
