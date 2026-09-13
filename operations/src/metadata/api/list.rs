use super::*;

pub(super) const DEFAULT_LIST_METADATA_LIMIT: usize = 50;

pub(super) const MAX_LIST_METADATA_LIMIT: usize = 1_000;

/// Bounds the response payload and the number of RO-Crate summary exports an
/// unauthenticated caller can force per request. The realm-wide registry scan
/// is removed by the cached list path, not by this clamp.
pub(super) const ANONYMOUS_LIST_METADATA_LIMIT: usize = 100;

/// Splits a targeted lookup from a browse page: the portal pages at 48, a
/// run-crate or preview lookup at 1, and only a browse page pays the estimate.
pub(super) const METADATA_ESTIMATE_MIN_LIMIT: usize = 24;

// Bounded so a single summary page cannot saturate the craqle read permits.
pub(super) const METADATA_SUMMARY_FANOUT_LIMIT: usize = 8;

/// Order the visible metadata listing is paginated in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataListOrder {
    /// Ascending document id, which is creation order for ULID ids.
    #[default]
    Created,
    /// Descending `updated_at_ms`, tie-broken by descending document id.
    Recent,
}

#[derive(Debug, Clone)]
pub struct ListVisibleMetadataDocumentsRequest {
    pub group_id: Option<GroupId>,
    pub path_prefix: Option<String>,
    pub include_summary: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: MetadataListOrder,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedMetadataDocument {
    pub record: MetadataRegistryRecord,
    pub rocrate_summary_jsonld: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListVisibleMetadataDocumentsResult {
    pub documents: Vec<ListedMetadataDocument>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
    /// Approximate number of matching documents across all pages; group-granular,
    /// so it may over- or under-count glob read rules. `None` when not computed
    /// for a request too small to be a browse page.
    pub total_estimate: Option<usize>,
}

pub async fn list_visible_documents(
    context: &DriverContext,
    realm_id: RealmId,
    request: ListVisibleMetadataDocumentsRequest,
) -> Result<ListVisibleMetadataDocumentsResult, MetadataApiError> {
    let limit = effective_list_limit(request.limit, request.auth.is_none());
    let offset = request.offset.unwrap_or(0);

    let group_ids = check_policy_limit(match request.group_id {
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
    // Summary listings and recency listings must show documents whose projection has not landed
    // yet; the pending keyspace is scanned once per request, never once per group.
    let recent = request.order == MetadataListOrder::Recent;
    let mut pending = if request.include_summary || recent {
        load_pending_records(context, request.group_id, METADATA_REGISTRY_CANDIDATE_LIMIT).await?
    } else {
        HashMap::new()
    };

    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
        let mut group_records = load_group_records(context, group_id, remaining).await?;
        if let Some(pending_records) = pending.remove(&group_id) {
            merge_pending_records(&mut group_records, pending_records);
            if group_records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            group_records.sort_by_key(|record| record.document_id);
        }
        records.extend(group_records);
    }
    // Ordering precedes both the estimate scan and the offset window so that
    // pagination and the early exit page the same sequence.
    if recent {
        records.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| right.document_id.cmp(&left.document_id))
        });
    }

    // One rule collection per group keeps every later visibility check in memory.
    let auth = request
        .auth
        .as_ref()
        .filter(|auth| auth.realm_id == realm_id);
    let permissions = GroupPermissionRules::collect(
        context,
        auth,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| record.group_id),
    )
    .await;
    // RBAC/public visibility is additionally constrained by the metadata.read
    // request policies, loaded once per distinct group (fail-closed on error).
    let evaluators = crate::auth::request_policy::PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let policy_auth = request.auth.as_ref();
    let record_visible = |record: &MetadataRegistryRecord| {
        permissions.record_visible(record)
            && evaluators
                .get(&(record.realm_id, record.group_id))
                .is_some_and(|evaluator| {
                    evaluator
                        .evaluate(&metadata_read_request(&record.permission_path, policy_auth))
                        .is_ok()
                })
    };

    let mut total_estimate = None;
    if limit >= METADATA_ESTIMATE_MIN_LIMIT {
        let matching = records
            .iter()
            .filter(|record| record_matches_filters(record, request.path_prefix.as_deref()))
            .filter(|record| record_visible(record))
            .count();
        total_estimate = Some(matching);
    }

    let needed = offset.saturating_add(limit);
    let mut selected = Vec::with_capacity(limit.min(records.len()));
    let mut visible_count = 0usize;
    for record in records {
        if !record_matches_filters(&record, request.path_prefix.as_deref()) {
            continue;
        }
        if !record_visible(&record) {
            continue;
        }
        visible_count += 1;
        if visible_count > offset {
            selected.push(record);
            if visible_count >= needed {
                break;
            }
        }
    }

    let mut documents = Vec::with_capacity(selected.len());
    if request.include_summary {
        let exports = selected
            .iter()
            .map(|record| async move {
                // A pending graph cannot export content from before its accepted event.
                ensure_record_materialized(context, record).await?;
                export_summary_jsonld(context, &record.graph_iri, record.last_event_id).await
            })
            .collect::<Vec<_>>();
        let summaries = stream::iter(exports)
            .buffered(METADATA_SUMMARY_FANOUT_LIMIT)
            .collect::<Vec<_>>()
            .await;
        for (record, summary) in selected.into_iter().zip(summaries) {
            let rocrate_summary_jsonld = match summary {
                Ok(summary) => Some(summary),
                Err(MetadataApiError::ServiceUnavailable) => None,
                Err(error) => return Err(error),
            };
            documents.push(ListedMetadataDocument {
                record,
                rocrate_summary_jsonld,
            });
        }
    } else {
        documents.extend(selected.into_iter().map(|record| ListedMetadataDocument {
            record,
            rocrate_summary_jsonld: None,
        }));
    }

    let total_returned = documents.len();
    Ok(ListVisibleMetadataDocumentsResult {
        documents,
        limit,
        offset,
        total_returned,
        // Never report fewer than the page already discloses.
        total_estimate: total_estimate.map(|estimate| estimate.max(total_returned)),
    })
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
