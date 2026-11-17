//! Reads metadata documents and references, checking that each record is live and readable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::read_types::{
    ApiQueryMode, DocumentQueryRequest, MetadataQueryExecution, MetadataQueryRequest,
    MetadataReferenceEntry, MetadataReferencesExecution, MetadataReferencesRequest,
};
use super::{
    AuthContext, AuthToken, AuthorizationError, CheckPermissionsConfig, CheckPermissionsOperation,
    DISTRIBUTED_QUERY_DEADLINE, DriverContext, Event, GRAPH_LIFECYCLE_KEYSPACE,
    GraphLifecycleRecord, GroupId, ListGroupOperation, MetadataApiError, MetadataFanoutScope,
    MetadataFanoutStats, MetadataQueryResults, MetadataReadError, MetadataRegistryRecord, NodeId,
    Permission, REFERENCES_LIMIT, REFERENCES_MAX_LIMIT, REGISTRY_CANDIDATE_LIMIT, RealmId,
    StorageEffect, StorageEvent, StorageReadError, TxnId, Ulid, check_policy_limit, drive,
    graph_lifecycle_key, load_document_record, load_group_records, load_pending_records,
    load_realm_config, map_query_error, merge_pending_records, parse_registry_read,
    query_fingerprint, read_document_registry, record_materialized_read, reference_document_title,
    replica_query_nodes, resolve_graph_reference, warn,
};

use super::distributed::run_query_distributed;
use super::fanout::ensure_query_form;
use crate::metadata::data_links::LinkBudget;
use crate::metadata::live_records::{LiveRecordsError, LiveRecordsOperation};
use aruna_core::handle::Handle;

pub async fn query_metadata_document(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: DocumentQueryRequest,
) -> Result<MetadataQueryExecution, MetadataApiError> {
    ensure_query_form(&request.query)?;
    let record = load_live_record(context, request.document_id).await?;
    ensure_record_readable(context, realm_id, request.auth.as_ref(), &record, None).await?;
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    if request.mode == Some(ApiQueryMode::Local) {
        ensure_record_materialized(context, &record).await?;
        let results = crate::metadata::query_authorized_local(
            context,
            request.auth,
            Some(vec![record.graph_iri]),
            request.query,
        )
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
        let results = crate::metadata::query_authorized_local(
            context,
            request.auth,
            Some(vec![record.graph_iri]),
            request.query,
        )
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
        Some(token) => Some(AuthToken::bearer(token).map_err(|_| MetadataApiError::BadRequest)?),
        None => request.auth.clone().map(AuthToken::internal),
    };
    let mut fanout_stats = MetadataFanoutStats::default();
    let mut auth_error = None;
    for holder in holders {
        fanout_stats.nodes_queried += 1;
        let result: Result<MetadataQueryResults, MetadataReadError> = if holder == local_node_id {
            match ensure_record_materialized(context, &record).await {
                Ok(()) => crate::metadata::query_authorized_local(
                    context,
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
            Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
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
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
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

/// Postings one references page may read, so a page never scans the index.
const REFERENCE_BUDGET: LinkBudget = LinkBudget {
    seeks: 1100,
    rows: 4096,
};

/// Reads one local page of the documents naming `iri` as an object, by document id after
/// `after`; each must be live at the posting's cursor and readable. An empty first page for a
/// known graph IRI, or `resolve`, returns one predicate-less summary.
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
        .unwrap_or(REFERENCES_LIMIT)
        .clamp(1, REFERENCES_MAX_LIMIT);

    if request.resolve {
        let registry = graph_record(context, &request.iri).await?;
        let entry =
            resolve_graph_reference(context, realm_id, &request, registry.as_slice()).await?;
        return Ok(MetadataReferencesExecution {
            references: entry.into_iter().collect(),
            next_after: None,
        });
    }

    let mut budget = REFERENCE_BUDGET;
    let page = crate::metadata::iri_index::read_backlinks(
        &context.storage_handle,
        &request.iri,
        request.predicate.as_deref(),
        request.after,
        limit,
        &mut budget,
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;

    let mut references = Vec::new();
    for (document_id, postings) in page.documents {
        let record = match load_live_record(context, document_id).await {
            Ok(record) => record,
            Err(MetadataApiError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        let current: Vec<_> = postings
            .into_iter()
            .filter(|posting| posting.document_cursor == record.last_event_id)
            .collect();
        if current.is_empty()
            || !can_read_record(context, realm_id, request.auth.as_ref(), &record).await?
        {
            continue;
        }
        let title = reference_document_title(context, &record).await;
        for posting in current {
            references.push(MetadataReferenceEntry {
                document_id: record.document_id.to_string(),
                group_id: record.group_id.to_string(),
                document_path: record.document_path.clone(),
                graph_iri: record.graph_iri.clone(),
                predicate: Some(posting.predicate_iri),
                subject_iris: posting.subject_iris,
                title: title.clone(),
            });
        }
    }

    if references.is_empty() && request.after.is_none() && page.next.is_none() {
        let registry = graph_record(context, &request.iri).await?;
        if let Some(entry) =
            resolve_graph_reference(context, realm_id, &request, registry.as_slice()).await?
        {
            references.push(entry);
        }
    }

    Ok(MetadataReferencesExecution {
        references,
        next_after: page.next,
    })
}

/// The live record whose graph IRI is `iri`, by one point read of the document it names; never
/// a walk over the registry.
async fn graph_record(
    context: &DriverContext,
    iri: &str,
) -> Result<Option<MetadataRegistryRecord>, MetadataApiError> {
    let Some(document_id) = MetadataRegistryRecord::graph_document(iri) else {
        return Ok(None);
    };
    match load_live_record(context, document_id).await {
        Ok(record) => Ok((record.graph_iri == iri).then_some(record)),
        Err(MetadataApiError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) async fn load_claim_records(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: Option<GroupId>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataApiError> {
    let group_ids = check_policy_limit(match group_id {
        Some(group_id) => vec![group_id],
        None => drive(
            ListGroupOperation::with_pagination(REGISTRY_CANDIDATE_LIMIT + 1, 0),
            context,
        )
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .into_iter()
        .map(|group| group.group_id)
        .collect(),
    })?;
    let mut pending = load_pending_records(context, group_id, REGISTRY_CANDIDATE_LIMIT).await?;
    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
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
        if records.len().saturating_add(pending_records.len()) > REGISTRY_CANDIDATE_LIMIT {
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
            key_space: GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: GraphLifecycleRecord = postcard::from_bytes(&value)
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
        other => Err(MetadataApiError::Internal(other.label().to_string())),
    }
}

impl From<LiveRecordsError> for MetadataApiError {
    fn from(error: LiveRecordsError) -> Self {
        match error {
            LiveRecordsError::TooMany => Self::ServiceUnavailable,
            other => Self::Internal(other.to_string()),
        }
    }
}

pub(crate) async fn load_live_record(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<MetadataRegistryRecord, MetadataApiError> {
    match load_document_record(context, document_id).await {
        Ok(Some(record)) => drive(LiveRecordsOperation::new(vec![record], None), context)
            .await?
            .into_iter()
            .next()
            .ok_or(MetadataApiError::NotFound),
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
        Ok(Some(record)) => drive(
            LiveRecordsOperation::new(vec![record], Some(txn_id)),
            context,
        )
        .await?
        .into_iter()
        .next()
        .ok_or(MetadataApiError::NotFound),
        Ok(None) => Err(MetadataApiError::NotFound),
        Err(StorageReadError::Storage(error)) => Err(MetadataApiError::Internal(error.to_string())),
        Err(StorageReadError::Conversion(error)) => {
            Err(MetadataApiError::Internal(error.to_string()))
        }
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
            | AuthorizationError::DocNotFound => MetadataApiError::Forbidden,
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
