//! Runs reference preflight: resolves target versions, fans out to nodes and merges locations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    ARUNA_DATA_PREFIX, ApiQueryMode, Arc, AuthContext, AuthToken, BLOB_HEAD_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BTreeMap, BTreeSet, BlobHeadKey, BlobVersion, BlobVersionState,
    ConversionError, CurrentVersionPointer, DISTRIBUTED_QUERY_DEADLINE, Deserialize, DriverContext,
    Event, GetBucketError, GetBucketOperation, GetConfigOperation, GetNodesOperation, HashMap,
    HashSet, IterStart, Key, MAX_PAGINATION_DEPTH, MetadataApiError, MetadataFanoutOperation,
    MetadataFanoutScope, MetadataFanoutStats, MetadataNodeCall, MetadataReferenceEntry,
    MetadataReferencesRequest, MetadataRegistryRecord, MetadataSearchHit, NodeId, NodeSearchResult,
    Permission, REALM_DISCOVERY_TIMEOUT, REFERENCES_LIMIT, REFERENCES_MAX_LIMIT,
    REGISTRY_CANDIDATE_LIMIT, RealmConfigDocument, RealmId, RealmNodeDiscovery,
    ResolvePathsOperation, SearchCursor, SearchCursorError, SearchWatermark, Serialize,
    StorageEffect, StorageEvent, Ulid, Value, VersionKey, W3idIdentifier, bucket_permission_path,
    can_read_record, deduplicate_fanout_nodes, drive, forwarded_bearer, load_pending_records,
    map_read_error, metadata_node_call, object_permission_path, paginate, record_preflight_node,
    resume_fetch_limit, run_metadata_fanout, select_fanout_nodes, warn,
};

use super::read::{ensure_permission, load_live_record};
use crate::metadata::data_links::LinkBudget;
use crate::metadata::search_cursor::tie_order;
use aruna_core::structs::storage::group_backend::MAX_KEY_LEN;

pub(super) async fn resolve_preflight_targets(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: &AuthContext,
    target: ReferenceTarget,
    s3_endpoint: Option<&str>,
) -> Result<ResolvedPreflightTargets, MetadataApiError> {
    match target {
        ReferenceTarget::ContentW3ids {
            content_w3ids,
            remove_resolvable_locations,
        } => {
            if content_w3ids.is_empty() || content_w3ids.len() > MAX_TARGET_VERSIONS {
                return Err(MetadataApiError::BadRequest);
            }
            let mut targets = BTreeMap::new();
            for content_w3id in content_w3ids {
                let W3idIdentifier::ContentHash(content_hash) =
                    W3idIdentifier::parse(&content_w3id)
                        .map_err(|_| MetadataApiError::BadRequest)?
                else {
                    return Err(MetadataApiError::BadRequest);
                };
                targets
                    .entry(content_hash)
                    .or_insert_with(|| MetadataResolvedTarget {
                        content_w3id: format!("{ARUNA_DATA_PREFIX}{}", hex::encode(content_hash)),
                        content_hash,
                        queried_iris: vec![content_w3id],
                        targeted_versions: Vec::new(),
                        removed_locations: Vec::new(),
                        remove_resolvable_locations,
                    });
            }
            Ok(ResolvedPreflightTargets {
                targets: targets.into_values().collect(),
                complete: true,
            })
        }
        ReferenceTarget::BucketPrefix {
            bucket,
            prefix,
            operation,
        } => {
            if bucket.trim().is_empty() {
                return Err(MetadataApiError::BadRequest);
            }
            let bucket_info = match drive(GetBucketOperation::new(bucket.clone()), context).await {
                Ok(info) => info,
                Err(GetBucketError::NotFound) => {
                    return Err(MetadataApiError::NotFound);
                }
                Err(_) => {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
            };
            let prefix = prefix.filter(|prefix| !prefix.is_empty());
            let permission_path = match prefix.as_deref() {
                Some(prefix) => object_permission_path(
                    realm_id,
                    bucket_info.group_id,
                    local_node_id,
                    &bucket,
                    prefix,
                ),
                None => {
                    bucket_permission_path(realm_id, bucket_info.group_id, local_node_id, &bucket)
                }
            };
            ensure_permission(
                context,
                realm_id,
                auth.clone(),
                bucket_info.group_id,
                permission_path,
                Permission::WRITE,
                None,
            )
            .await?;

            let (versions, mut complete) = match operation {
                MetadataStorageOperation::LatestVersionTombstone => {
                    resolve_preflight_versions(context, &bucket, prefix.as_deref()).await?
                }
                MetadataStorageOperation::AllVersionsPurge => {
                    resolve_all_preflight(context, &bucket, prefix.as_deref()).await?
                }
            };
            let mut authorized_keys = BTreeSet::new();
            for (version_key, _) in &versions {
                if authorized_keys.insert(version_key.key.clone()) {
                    ensure_permission(
                        context,
                        realm_id,
                        auth.clone(),
                        bucket_info.group_id,
                        object_permission_path(
                            realm_id,
                            bucket_info.group_id,
                            local_node_id,
                            &bucket,
                            &version_key.key,
                        ),
                        Permission::WRITE,
                        None,
                    )
                    .await?;
                }
            }
            let mut targets = BTreeMap::<[u8; 32], MetadataResolvedTarget>::new();
            for (version_key, version) in versions {
                let content_hash = match version.state {
                    BlobVersionState::Materialized { blob_hash, .. } => blob_hash,
                    BlobVersionState::Reference { .. }
                    | BlobVersionState::PendingContent { .. } => {
                        complete = false;
                        continue;
                    }
                    BlobVersionState::Deleted => continue,
                };
                let location = MetadataPreflightLocation {
                    node_id: local_node_id,
                    bucket: version_key.bucket.clone(),
                    key: version_key.key.clone(),
                    version_id: version_key.version_id,
                };
                let target = targets.entry(content_hash).or_insert_with(|| {
                    let content_w3id = format!("{ARUNA_DATA_PREFIX}{}", hex::encode(content_hash));
                    MetadataResolvedTarget {
                        queried_iris: vec![content_w3id.clone()],
                        content_w3id,
                        content_hash,
                        targeted_versions: Vec::new(),
                        removed_locations: Vec::new(),
                        remove_resolvable_locations: false,
                    }
                });
                add_location_iris(
                    &mut target.queried_iris,
                    s3_endpoint,
                    &location.bucket,
                    &location.key,
                );
                target.targeted_versions.push(location.clone());
                if operation == MetadataStorageOperation::AllVersionsPurge {
                    target.removed_locations.push(location);
                }
            }
            for target in targets.values_mut() {
                target.queried_iris.sort();
                target.queried_iris.dedup();
                target.targeted_versions.sort_by_key(|location| {
                    (
                        location.bucket.clone(),
                        location.key.clone(),
                        location.version_id,
                    )
                });
                target.removed_locations.sort_by_key(|location| {
                    (
                        location.bucket.clone(),
                        location.key.clone(),
                        location.version_id,
                    )
                });
            }
            Ok(ResolvedPreflightTargets {
                targets: targets.into_values().collect(),
                complete,
            })
        }
    }
}

/// Resolves the current versions of at most `MAX_TARGET_VERSIONS` keys; false marks a cut scope.
pub(super) async fn resolve_preflight_versions(
    context: &DriverContext,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<(Vec<(VersionKey, BlobVersion)>, bool), MetadataApiError> {
    let prefix_key = match prefix {
        Some(prefix) => BlobHeadKey::object_prefix(bucket, prefix),
        None => BlobHeadKey::bucket_prefix(bucket),
    }
    .map_err(|_| MetadataApiError::BadRequest)?;
    let (heads, more) = scan_preflight_rows(
        context,
        BLOB_HEAD_KEYSPACE,
        (prefix_key.into(), None),
        MAX_TARGET_VERSIONS,
    )
    .await?;
    let mut versions = Vec::with_capacity(heads.len());
    for (key, value) in heads {
        let head = BlobHeadKey::from_bytes(key.as_ref())
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        let version_key = VersionKey::new(head.bucket, head.key, pointer.version_id);
        let Some(value) = read_preflight_row(
            context,
            BLOB_VERSIONS_KEYSPACE,
            version_key
                .to_bytes()
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .into(),
        )
        .await?
        else {
            return Err(MetadataApiError::ServiceUnavailable);
        };
        let version = BlobVersion::from_bytes(value.as_ref())
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        versions.push((version_key, version));
    }
    Ok((versions, !more))
}

/// Resolves at most `MAX_TARGET_VERSIONS` versions under the prefix, also those without a head.
/// Version rows sort by key length first, so each key length below `MAX_KEY_LEN` is one range of rows.
pub(super) async fn resolve_all_preflight(
    context: &DriverContext,
    bucket: &str,
    key_prefix: Option<&str>,
) -> Result<(Vec<(VersionKey, BlobVersion)>, bool), MetadataApiError> {
    let bucket_rows =
        VersionKey::bucket_prefix(bucket).map_err(|_| MetadataApiError::BadRequest)?;
    let ranges = match key_prefix {
        None => vec![bucket_rows.clone()],
        Some(prefix) => (prefix.len()..MAX_KEY_LEN)
            .map(|length| {
                let mut range = bucket_rows.clone();
                range.extend(postcard::to_allocvec(&length)?);
                range.extend(prefix.as_bytes());
                Ok(range)
            })
            .collect::<Result<Vec<_>, postcard::Error>>()
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
    };
    let mut versions = Vec::new();
    for range in ranges {
        let remaining = MAX_TARGET_VERSIONS - versions.len();
        let range = (range.into(), None);
        let (rows, more) =
            scan_preflight_rows(context, BLOB_VERSIONS_KEYSPACE, range, remaining).await?;
        for (key, value) in rows {
            versions.push(decode_version(&key, &value)?);
        }
        if more {
            return Ok((versions, false));
        }
    }
    let Some(prefix) = key_prefix else {
        return Ok((versions, true));
    };
    // Keys of `MAX_KEY_LEN` bytes and more have a length varint whose second byte is 8 or more.
    let mut budget = LONG_KEY_ROWS;
    for lead in 0x80u8..=0xFF {
        let mut range = bucket_rows.clone();
        range.push(lead);
        let from = [range.as_slice(), &[8]].concat();
        let range = (range.into(), Some(from.into()));
        let (rows, more) =
            scan_preflight_rows(context, BLOB_VERSIONS_KEYSPACE, range, budget).await?;
        budget -= rows.len();
        for (key, value) in rows {
            let found = decode_version(&key, &value)?;
            if !found.0.key.starts_with(prefix) {
                continue;
            }
            if versions.len() == MAX_TARGET_VERSIONS {
                return Ok((versions, false));
            }
            versions.push(found);
        }
        if more {
            return Ok((versions, false));
        }
    }
    Ok((versions, true))
}

fn decode_version(key: &Key, value: &Value) -> Result<(VersionKey, BlobVersion), MetadataApiError> {
    let version_key = VersionKey::from_bytes(key.as_ref())
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    let version = BlobVersion::from_bytes(value.as_ref())
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
    Ok((version_key, version))
}

/// Reads at most `limit` rows under the prefix from `from` on; true reports that more rows exist.
pub(super) async fn scan_preflight_rows(
    context: &DriverContext,
    key_space: &str,
    (prefix, from): (Key, Option<Key>),
    limit: usize,
) -> Result<(Vec<(Key, Value)>, bool), MetadataApiError> {
    let mut start = from.map(IterStart::At);
    let mut rows = Vec::new();
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: Some(prefix.clone()),
                start: start.take(),
                limit: PREFLIGHT_PAGE_SIZE.min(limit + 1 - rows.len()),
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                rows.extend(values);
                if rows.len() > limit {
                    rows.truncate(limit);
                    return Ok((rows, true));
                }
                match next_start_after {
                    Some(next) => start = Some(IterStart::After(next)),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { .. }) => {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            _ => return Err(MetadataApiError::ServiceUnavailable),
        }
    }
    Ok((rows, false))
}

pub(super) async fn read_preflight_row(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Value>, MetadataApiError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { .. }) => Err(MetadataApiError::ServiceUnavailable),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

pub(super) fn add_location_iris(
    iris: &mut Vec<String>,
    s3_endpoint: Option<&str>,
    bucket: &str,
    key: &str,
) {
    iris.push(format!("s3://{bucket}/{key}"));
    if let Some(endpoint) = s3_endpoint.filter(|endpoint| !endpoint.is_empty()) {
        iris.push(format!("{}/{bucket}/{key}", endpoint.trim_end_matches('/')));
    }
}

pub(crate) async fn references_preflight_local(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    mut request: ReferenceNodeRequest,
    s3_endpoint: Option<String>,
) -> Result<ReferenceNodeExecution, MetadataApiError> {
    if request.targets.len() > MAX_TARGET_VERSIONS
        || request.limit == 0
        || request.limit > MAX_PAGINATION_DEPTH
    {
        return Err(MetadataApiError::BadRequest);
    }
    let auth = auth.ok_or(MetadataApiError::Unauthorized)?;
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }

    let mut iri_targets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut target_locations = BTreeMap::<String, (bool, bool)>::new();
    let mut aliases_seen = false;
    for target in request.targets.iter_mut() {
        let mut local_aliases = drive(ResolvePathsOperation::new(target.content_hash), context)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        local_aliases.retain(|alias| alias.realm_id == realm_id && alias.node_id == local_node_id);
        aliases_seen |= !local_aliases.is_empty();
        let mut found = false;
        let mut remaining = false;
        for alias in local_aliases {
            found = true;
            let location = MetadataPreflightLocation {
                node_id: alias.node_id,
                bucket: alias.bucket.clone(),
                key: alias.key.clone(),
                version_id: alias.version_id,
            };
            let removed =
                target.remove_resolvable_locations || target.removed_locations.contains(&location);
            remaining |= !removed;
            add_location_iris(
                &mut target.queried_iris,
                s3_endpoint.as_deref(),
                &alias.bucket,
                &alias.key,
            );
        }
        target.queried_iris.sort();
        target.queried_iris.dedup();
        for iri in &target.queried_iris {
            iri_targets
                .entry(iri.clone())
                .or_default()
                .insert(target.content_w3id.clone());
        }
        target_locations.insert(target.content_w3id.clone(), (found, remaining));
    }
    // Each object yields its first `limit + 1` visible documents or ends, which covers every
    // reference of the page prefix; a lookup over the budget fails instead of scanning.
    let mut budget = PREFLIGHT_BUDGET;
    let mut records = HashMap::<Ulid, Option<MetadataRegistryRecord>>::new();
    let mut readable = HashMap::<Ulid, bool>::new();
    let mut hidden = BTreeSet::<String>::new();
    let mut visible = BTreeSet::<(String, Ulid)>::new();
    for (iri, content_w3ids) in &iri_targets {
        let mut after = None;
        let mut found = 0usize;
        loop {
            let page = crate::metadata::iri_index::read_backlinks(
                &context.storage_handle,
                iri,
                None,
                after,
                request.limit.saturating_add(1),
                &mut budget,
            )
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
            for (document_id, postings) in page.documents {
                let record = match records.get(&document_id) {
                    Some(record) => record.clone(),
                    None => {
                        let record = match load_live_record(context, document_id).await {
                            Ok(record) => Some(record),
                            Err(MetadataApiError::NotFound) => None,
                            Err(error) => return Err(error),
                        };
                        records.insert(document_id, record.clone());
                        record
                    }
                };
                let Some(record) = record else {
                    continue;
                };
                if !postings
                    .iter()
                    .any(|posting| posting.document_cursor == record.last_event_id)
                {
                    continue;
                }
                let allowed = match readable.get(&document_id) {
                    Some(allowed) => *allowed,
                    None => {
                        let allowed =
                            can_read_record(context, realm_id, Some(&auth), &record).await?;
                        readable.insert(document_id, allowed);
                        allowed
                    }
                };
                if !allowed {
                    hidden.extend(content_w3ids.iter().cloned());
                    continue;
                }
                found = found.saturating_add(1);
                for content_w3id in content_w3ids {
                    visible.insert((content_w3id.clone(), document_id));
                }
            }
            match page.next {
                Some(next) if found <= request.limit => after = Some(next),
                _ => break,
            }
        }
    }
    let returned: Vec<_> = records.values().flatten().cloned().collect();
    let freshness =
        crate::metadata::iri_index::iri_index_freshness(&context.storage_handle, &returned)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let pending_for_realm = load_pending_records(context, None, REGISTRY_CANDIDATE_LIMIT)
        .await?
        .into_values()
        .flatten()
        .any(|record| record.realm_id == realm_id);
    // A queued or failed materialization has no postings yet, so its references are missing.
    let (queued, dead) =
        crate::metadata::data_links::unsettled_documents(&context.storage_handle, 1)
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let index_state = index_state(freshness.state, pending_for_realm || queued > 0, dead > 0);
    let (selected, saturated) = visible_prefix(visible, request.limit);
    let mut titles = HashMap::<Ulid, String>::new();
    let mut visible_references = Vec::with_capacity(selected.len());
    for (content_w3id, document_id) in selected {
        let Some(Some(record)) = records.get(&document_id) else {
            continue;
        };
        let title = match titles.get(&document_id) {
            Some(title) => title.clone(),
            None => {
                let title = reference_document_title(context, record)
                    .await
                    .unwrap_or_else(|| record.document_path.clone());
                titles.insert(document_id, title.clone());
                title
            }
        };
        visible_references.push(MetadataVisibleReference {
            content_w3id,
            document_id: document_id.to_string(),
            title,
        });
    }
    let targets = request
        .targets
        .into_iter()
        .map(|target| {
            let (resolvable_location_found, location_after_operation) = target_locations
                .remove(&target.content_w3id)
                .unwrap_or((false, false));
            ReferenceNodeTarget {
                hidden_references_exist: hidden.contains(&target.content_w3id),
                content_w3id: target.content_w3id,
                resolvable_location_found,
                location_after_operation,
            }
        })
        .collect();
    Ok(ReferenceNodeExecution {
        visible_references,
        targets,
        freshness: MetadataNodeFreshness {
            node_id: local_node_id,
            index_state,
            oldest_status_updated: freshness.oldest_status_updated,
        },
        path_style_available: s3_endpoint.is_some() || !aliases_seen,
        saturated,
    })
}

/// The index state of the returned documents, with documents still to be materialized
/// (`pending`) and documents whose materialization failed (`failed`) on this node.
pub(super) fn index_state(
    returned: crate::metadata::iri_index::IriFreshnessState,
    pending: bool,
    failed: bool,
) -> MetadataIndexState {
    use crate::metadata::iri_index::IriFreshnessState;
    let pending = pending || returned == IriFreshnessState::Pending;
    let failed = failed || returned == IriFreshnessState::Failed;
    match (returned == IriFreshnessState::Mixed, pending, failed) {
        (true, _, _) | (false, true, true) => MetadataIndexState::Mixed,
        (false, false, true) => MetadataIndexState::Failed,
        (false, true, false) => MetadataIndexState::Pending,
        (false, false, false) => MetadataIndexState::Current,
    }
}

/// Keeps the first `limit` references in watermark order and reports whether more exist.
/// The coordinator pages node prefixes by that order, so a cut in any other order skips hits.
pub(super) fn visible_prefix(
    visible: BTreeSet<(String, Ulid)>,
    limit: usize,
) -> (Vec<(String, Ulid)>, bool) {
    let mut keyed = visible
        .into_iter()
        .map(|(content_w3id, document_id)| (content_w3id, document_id.to_string(), document_id))
        .collect::<Vec<_>>();
    keyed.sort_by(|left, right| tie_order((&left.0, &left.1), (&right.0, &right.1)));
    let saturated = keyed.len() > limit;
    keyed.truncate(limit);
    let selected = keyed
        .into_iter()
        .map(|(content_w3id, _, document_id)| (content_w3id, document_id))
        .collect();
    (selected, saturated)
}

pub(super) fn preflight_fingerprint(
    targets: &[MetadataResolvedTarget],
    mode: Option<ApiQueryMode>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"metadata-reference-preflight\0");
    hasher.update(&[match mode {
        None => 0,
        Some(ApiQueryMode::Local) => 1,
        Some(ApiQueryMode::Distributed) => 2,
    }]);
    hasher.update(
        &postcard::to_allocvec(targets).expect("preflight cursor fingerprint payload serializes"),
    );
    *hasher.finalize().as_bytes()
}

pub(super) async fn resolve_graph_reference(
    context: &DriverContext,
    realm_id: RealmId,
    request: &MetadataReferencesRequest,
    registry: &[MetadataRegistryRecord],
) -> Result<Option<MetadataReferenceEntry>, MetadataApiError> {
    let Some(record) = registry
        .iter()
        .find(|record| record.graph_iri == request.iri)
    else {
        return Ok(None);
    };
    if !can_read_record(context, realm_id, request.auth.as_ref(), record).await? {
        return Ok(None);
    }
    let title = reference_document_title(context, record).await;
    Ok(Some(MetadataReferenceEntry {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: record.graph_iri.clone(),
        predicate: None,
        subject_iris: Vec::new(),
        title,
    }))
}

pub(super) async fn reference_document_title(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Option<String> {
    let handle = context.metadata_handle.clone()?;
    let properties = handle
        .describe_root_properties(record.graph_iri.clone())
        .await;
    // Root subject "./" makes the fallback the document path, not the id tail.
    let title =
        crate::metadata::search_enrichment::hit_title(&properties, &record.document_path, "./");
    (!title.is_empty()).then_some(title)
}

pub async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(error = %error, "realm config unavailable; querying the local replica only");
            None
        }
    }
}

pub async fn load_realm_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    discover_realm_nodes(context, realm_id, local_node_id)
        .await
        .nodes
}

pub(crate) async fn discover_realm_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
) -> RealmNodeDiscovery {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return RealmNodeDiscovery {
            nodes: vec![local_node_id],
            failed: true,
        };
    };
    // Race discovery so fanout queries degrade to reachable/local partitions
    // instead of stalling behind offline peers.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(GetNodesOperation::new(realm_id), context),
    )
    .await;
    let nodes = match discovery {
        // Bounded-stale candidates are allowed here; an unreachable one is
        // reported through the existing partial-result fields.
        Ok(Ok(presence)) => match authorized_realm_nodes(&config, presence.into_nodes()) {
            Ok(nodes) => (nodes, false),
            Err(error) => {
                warn!(error = %error, "realm config contains invalid node ids; using local-only metadata results");
                return RealmNodeDiscovery {
                    nodes: vec![local_node_id],
                    failed: true,
                };
            }
        },
        Ok(Err(error)) => {
            warn!(
                error = %error,
                "realm node discovery failed, using best-effort local-only metadata results"
            );
            (HashSet::new(), true)
        }
        Err(_) => {
            warn!("realm node discovery timed out, using best-effort local-only metadata results");
            (HashSet::new(), true)
        }
    };
    let (nodes, failed) = nodes;
    let mut nodes = nodes.into_iter().collect::<Vec<_>>();
    if !nodes.contains(&local_node_id) {
        nodes.push(local_node_id);
    }
    nodes.sort_by_key(|node_id| node_id.to_string());
    RealmNodeDiscovery { nodes, failed }
}

const MAX_TARGET_VERSIONS: usize = 128;
/// Postings one node's preflight may read across all of its target IRIs.
const PREFLIGHT_BUDGET: LinkBudget = LinkBudget {
    seeks: 16_384,
    rows: 262_144,
};

const PREFLIGHT_PAGE_SIZE: usize = 128;

/// Version rows of keys of `MAX_KEY_LEN` bytes and more one purge preflight reads at most.
const LONG_KEY_ROWS: usize = 1024;

#[derive(Debug, Clone)]
pub enum ReferenceTarget {
    ContentW3ids {
        content_w3ids: Vec<String>,
        remove_resolvable_locations: bool,
    },
    BucketPrefix {
        bucket: String,
        prefix: Option<String>,
        operation: MetadataStorageOperation,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataStorageOperation {
    #[default]
    LatestVersionTombstone,
    AllVersionsPurge,
}

#[derive(Debug, Clone)]
pub struct ReferenceRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub target: ReferenceTarget,
    pub s3_endpoint: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<ApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightLocation {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataResolvedTarget {
    pub content_w3id: String,
    pub content_hash: [u8; 32],
    pub queried_iris: Vec<String>,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub removed_locations: Vec<MetadataPreflightLocation>,
    #[serde(rename = "remove_all_resolvable_locations")]
    pub remove_resolvable_locations: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeRequest {
    pub targets: Vec<MetadataResolvedTarget>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataVisibleReference {
    pub content_w3id: String,
    pub document_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataIndexState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataNodeFreshness {
    pub node_id: NodeId,
    pub index_state: MetadataIndexState,
    #[serde(rename = "oldest_status_updated_at_ms")]
    pub oldest_status_updated: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeTarget {
    pub content_w3id: String,
    pub hidden_references_exist: bool,
    pub resolvable_location_found: bool,
    #[serde(rename = "resolvable_location_after_operation")]
    pub location_after_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeExecution {
    pub visible_references: Vec<MetadataVisibleReference>,
    pub targets: Vec<ReferenceNodeTarget>,
    pub freshness: MetadataNodeFreshness,
    #[serde(rename = "path_style_endpoint_available")]
    pub path_style_available: bool,
    pub saturated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceTargetExecution {
    pub content_w3id: String,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub visible_references: Vec<MetadataVisibleReference>,
    pub hidden_references_exist: bool,
    pub would_remove_location: bool,
    pub location_impact_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataExcludedForm {
    pub form: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceCoverage {
    pub queried_scope: &'static str,
    pub queried_forms: Vec<&'static str>,
    pub excluded_forms: Vec<MetadataExcludedForm>,
    pub node_freshness: Vec<MetadataNodeFreshness>,
    pub target_resolution_complete: bool,
    pub path_style_complete: bool,
    pub realm_coverage_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceExecution {
    pub targets: Vec<ReferenceTargetExecution>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub complete: bool,
    pub failed_partitions: Vec<NodeId>,
    pub coverage: ReferenceCoverage,
}

pub(super) struct ResolvedPreflightTargets {
    targets: Vec<MetadataResolvedTarget>,
    complete: bool,
}

pub(super) struct PreflightPlan {
    pub(super) auth: AuthContext,
    pub(super) bearer_token: Option<String>,
    pub(super) s3_endpoint: Option<String>,
    pub(super) cursor: Option<String>,
    pub(super) page_size: usize,
    pub(super) mode: Option<ApiQueryMode>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) allow_partial: bool,
}

pub(super) struct PreflightCursorPlan {
    pub(super) fingerprint: [u8; 32],
    pub(super) watermark: Option<SearchWatermark>,
    pub(super) resume: HashMap<NodeId, u32>,
    pub(super) target_nodes: Option<Vec<NodeId>>,
    pub(super) discovery_failed: bool,
}

pub async fn references_preflight(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: ReferenceRequest,
) -> Result<ReferenceExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
    let (plan, target) = plan_preflight_request(realm_id, request)?;
    let resolved = resolve_preflight_targets(
        context,
        realm_id,
        local_node_id,
        &plan.auth,
        target,
        plan.s3_endpoint.as_deref(),
    )
    .await?;
    let cursor =
        verify_preflight_cursor(context, realm_id, local_node_id, &plan, &resolved, deadline)
            .await?;
    let (node_parts, fanout_stats) = run_preflight_fanout(
        context,
        realm_id,
        local_node_id,
        &plan,
        &resolved,
        &cursor,
        deadline,
    )
    .await?;
    assemble_preflight_execution(context, resolved, &plan, cursor, node_parts, fanout_stats)
}

pub(super) fn plan_preflight_request(
    realm_id: RealmId,
    request: ReferenceRequest,
) -> Result<(PreflightPlan, ReferenceTarget), MetadataApiError> {
    let ReferenceRequest {
        auth,
        bearer_token,
        target,
        s3_endpoint,
        limit,
        cursor,
        mode,
        target_nodes,
        allow_partial,
    } = request;
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let page_size = limit
        .unwrap_or(REFERENCES_LIMIT)
        .clamp(1, REFERENCES_MAX_LIMIT);
    Ok((
        PreflightPlan {
            auth,
            bearer_token,
            s3_endpoint,
            cursor,
            page_size,
            mode,
            target_nodes,
            allow_partial,
        },
        target,
    ))
}

pub(super) async fn verify_preflight_cursor(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    plan: &PreflightPlan,
    resolved: &ResolvedPreflightTargets,
    deadline: tokio::time::Instant,
) -> Result<PreflightCursorPlan, MetadataApiError> {
    let fingerprint = preflight_fingerprint(&resolved.targets, plan.mode);
    let mut cursor_discovery = None;
    let (watermark, resume) = match plan.cursor.as_deref() {
        Some(raw) => {
            let signer_nodes = match plan.mode.unwrap_or(ApiQueryMode::Distributed) {
                ApiQueryMode::Local => vec![local_node_id],
                ApiQueryMode::Distributed => match plan.target_nodes.as_ref() {
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
                        .unwrap_or(RealmNodeDiscovery {
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
    let (target_nodes, discovery_failed) = if plan.cursor.is_some() {
        let mut nodes = match plan.target_nodes.as_ref() {
            Some(nodes) => select_fanout_nodes(nodes, local_node_id, &fingerprint),
            None => match plan.mode.unwrap_or(ApiQueryMode::Distributed) {
                ApiQueryMode::Local => vec![local_node_id],
                ApiQueryMode::Distributed => cursor_discovery
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
        (plan.target_nodes.clone(), false)
    };
    Ok(PreflightCursorPlan {
        fingerprint,
        watermark,
        resume,
        target_nodes,
        discovery_failed,
    })
}

pub(super) async fn run_preflight_fanout(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    plan: &PreflightPlan,
    resolved: &ResolvedPreflightTargets,
    cursor: &PreflightCursorPlan,
    deadline: tokio::time::Instant,
) -> Result<(Vec<(NodeId, ReferenceNodeExecution)>, MetadataFanoutStats), MetadataApiError> {
    let resume = Arc::new(cursor.resume.clone());
    let remote_auth = forwarded_bearer(plan.bearer_token.as_deref())?
        .or_else(|| Some(AuthToken::internal(plan.auth.clone())));
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let local_call: MetadataNodeCall<ReferenceNodeExecution> = metadata_node_call(
        (
            context.clone(),
            realm_id,
            plan.auth.clone(),
            resolved.targets.clone(),
            plan.s3_endpoint.clone(),
            resume.clone(),
            plan.page_size,
        ),
        |(context, realm_id, auth, targets, endpoint, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            references_preflight_local(
                &context,
                realm_id,
                node_id,
                Some(auth),
                ReferenceNodeRequest { targets, limit },
                endpoint,
            )
            .await
            .map_err(crate::forward::transport::read_error)
        },
    );
    let remote_call: MetadataNodeCall<ReferenceNodeExecution> = metadata_node_call(
        (
            handle,
            remote_auth,
            resolved.targets.clone(),
            resume.clone(),
            plan.page_size,
        ),
        |(handle, auth_token, targets, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            handle
                .request_remote_preflight(
                    node_id,
                    auth_token,
                    ReferenceNodeRequest { targets, limit },
                )
                .await
        },
    );
    run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(plan.mode, cursor.target_nodes.clone(), plan.allow_partial)
            .with_subject(cursor.fingerprint)
            .with_discovery_failed(cursor.discovery_failed)
            .with_deadline(deadline),
        MetadataFanoutOperation::ReferencePreflight,
        local_call,
        remote_call,
        record_preflight_node,
        map_read_error,
    )
    .await
}

pub(super) fn assemble_preflight_execution(
    context: &DriverContext,
    resolved: ResolvedPreflightTargets,
    plan: &PreflightPlan,
    cursor: PreflightCursorPlan,
    node_parts: Vec<(NodeId, ReferenceNodeExecution)>,
    fanout_stats: MetadataFanoutStats,
) -> Result<ReferenceExecution, MetadataApiError> {
    let mut node_results = Vec::new();
    let mut node_freshness = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut locations = BTreeMap::<String, (bool, bool)>::new();
    let mut path_style_complete = true;
    for (node_id, part) in node_parts {
        node_freshness.push(part.freshness.clone());
        path_style_complete &= part.path_style_available;
        for target in part.targets {
            if target.hidden_references_exist {
                hidden.insert(target.content_w3id.clone());
            }
            let entry = locations.entry(target.content_w3id).or_default();
            entry.0 |= target.resolvable_location_found;
            entry.1 |= target.location_after_operation;
        }
        let hits = part
            .visible_references
            .into_iter()
            .map(|reference| MetadataSearchHit {
                document_id: reference.document_id.clone(),
                group_id: String::new(),
                document_path: String::new(),
                graph_iri: reference.content_w3id,
                subject_iri: reference.document_id,
                score: 0.0,
                title: reference.title,
                snippet: None,
                subject_types: Vec::new(),
            })
            .collect();
        node_results.push(NodeSearchResult {
            node_id,
            hits,
            saturated: part.saturated,
        });
    }
    node_freshness.sort_by_key(|freshness| freshness.node_id.to_string());
    let page = paginate(
        node_results,
        cursor.watermark,
        plan.page_size,
        MAX_PAGINATION_DEPTH,
    );
    let mut visible_by_target = BTreeMap::<String, Vec<MetadataVisibleReference>>::new();
    for hit in page.hits {
        visible_by_target
            .entry(hit.graph_iri.clone())
            .or_default()
            .push(MetadataVisibleReference {
                content_w3id: hit.graph_iri,
                document_id: hit.document_id,
                title: hit.title,
            });
    }
    let index_current = node_freshness
        .iter()
        .all(|freshness| freshness.index_state == MetadataIndexState::Current);
    let complete = fanout_stats.nodes_failed == 0
        && resolved.complete
        && index_current
        && path_style_complete
        && !page.truncated;
    if !plan.allow_partial && !complete {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let targets = resolved
        .targets
        .into_iter()
        .map(|target| {
            let (found, remaining) = locations
                .remove(&target.content_w3id)
                .unwrap_or((false, false));
            let removes_location =
                target.remove_resolvable_locations || !target.removed_locations.is_empty();
            ReferenceTargetExecution {
                visible_references: visible_by_target
                    .remove(&target.content_w3id)
                    .unwrap_or_default(),
                hidden_references_exist: hidden.contains(&target.content_w3id),
                would_remove_location: complete && removes_location && found && !remaining,
                location_impact_complete: complete,
                content_w3id: target.content_w3id,
                targeted_versions: target.targeted_versions,
            }
        })
        .collect();
    let next_cursor = match page.next {
        Some(next) => {
            let net = context.net_handle.as_ref().ok_or_else(|| {
                MetadataApiError::Internal(
                    "net handle unavailable for preflight cursor signing".to_string(),
                )
            })?;
            Some(
                SearchCursor::new_signed(
                    cursor.fingerprint,
                    next.watermark,
                    next.resume,
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
    let distributed = plan.mode.unwrap_or(ApiQueryMode::Distributed) == ApiQueryMode::Distributed;
    Ok(ReferenceExecution {
        targets,
        next_cursor,
        truncated: page.truncated,
        nodes_queried: fanout_stats.nodes_queried,
        nodes_failed: fanout_stats.nodes_failed,
        complete,
        failed_partitions: fanout_stats.failed_partitions,
        coverage: ReferenceCoverage {
            queried_scope: if distributed { "realm" } else { "local_node" },
            queried_forms: vec![
                "canonical_content_w3id",
                "legacy_s3_iri",
                "legacy_path_style_http_iri",
            ],
            excluded_forms: vec![
                MetadataExcludedForm {
                    form: "literal_content_url",
                    reason: "literal objects are not materialized in the NamedNode IRI index",
                },
                MetadataExcludedForm {
                    form: "imported_relative_identity",
                    reason: "relative imported identities are outside exact absolute-IRI matching",
                },
                MetadataExcludedForm {
                    form: "imported_external_identity",
                    reason: "external identities without an Aruna content mapping are outside coverage",
                },
            ],
            node_freshness,
            target_resolution_complete: resolved.complete,
            path_style_complete,
            realm_coverage_complete: distributed && complete,
        },
    })
}

pub(super) fn authorized_realm_nodes(
    config: &RealmConfigDocument,
    nodes: HashSet<NodeId>,
) -> Result<HashSet<NodeId>, ConversionError> {
    let authorized = config
        .sync_eligible_nodes()?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(nodes
        .into_iter()
        .filter(|node_id| authorized.contains(node_id))
        .collect())
}
