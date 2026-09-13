use super::{
    AuthContext, AuthorizationError, CheckPermissionsConfig, CheckPermissionsOperation,
    DriverContext, Event, GroupId, HashMap, ListGroupOperation,
    METADATA_DISTRIBUTED_QUERY_DEADLINE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_REFERENCES_DEFAULT_LIMIT,
    METADATA_REFERENCES_MAX_LIMIT, METADATA_REGISTRY_CANDIDATE_LIMIT, MetadataApiError,
    MetadataAuthToken, MetadataDocumentLifecycleRecord, MetadataFanoutScope, MetadataFanoutStats,
    MetadataGraphLifecycleRecord, MetadataQueryResults, MetadataReadError, MetadataRegistryRecord,
    NodeId, Permission, RealmId, StorageEffect, StorageEvent, StorageHandle, StorageReadError,
    TxnId, Ulid, check_policy_limit, document_lifecycle_key, drive, graph_lifecycle_key,
    load_document_record, load_group_records, load_pending_records, load_realm_config,
    map_internal_error, map_query_error, merge_pending_records, parse_registry_read,
    query_fingerprint, read_document_registry, record_materialized_read, reference_document_title,
    replica_query_nodes, resolve_graph_reference, warn,
};

use super::distributed::run_query_distributed;
use super::fanout::ensure_query_form;
use aruna_core::handle::Handle;

pub async fn query_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataDocumentQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_query_form(&request.query)?;
    let record = load_live_record(context, request.document_id).await?;
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

pub(crate) async fn load_live_record(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataApiQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone)]
pub struct MetadataDocumentQueryRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryExecution {
    pub results: MetadataQueryResults,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesRequest {
    pub auth: Option<AuthContext>,
    pub iri: String,
    pub predicate: Option<String>,
    pub limit: Option<usize>,
    pub resolve: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataReferenceEntry {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub predicate: Option<String>,
    pub subject_iris: Vec<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesExecution {
    pub references: Vec<MetadataReferenceEntry>,
}
