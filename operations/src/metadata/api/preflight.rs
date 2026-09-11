use super::*;

pub(super) async fn resolve_preflight_targets(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: &AuthContext,
    target: MetadataReferencePreflightTarget,
    s3_endpoint: Option<&str>,
) -> Result<ResolvedPreflightTargets, MetadataApiError> {
    match target {
        MetadataReferencePreflightTarget::ContentW3ids {
            content_w3ids,
            remove_all_resolvable_locations,
        } => {
            if content_w3ids.is_empty()
                || content_w3ids.len() > METADATA_PREFLIGHT_MAX_TARGET_VERSIONS
            {
                return Err(MetadataApiError::BadRequest);
            }
            let mut targets = BTreeMap::new();
            for content_w3id in content_w3ids {
                let W3idDataIdentifier::ContentHash(content_hash) =
                    W3idDataIdentifier::parse(&content_w3id)
                        .map_err(|_| MetadataApiError::BadRequest)?
                else {
                    return Err(MetadataApiError::BadRequest);
                };
                targets
                    .entry(content_hash)
                    .or_insert_with(|| MetadataPreflightResolvedTarget {
                        content_w3id: format!("{ARUNA_DATA_PREFIX}{}", hex::encode(content_hash)),
                        content_hash,
                        queried_iris: vec![content_w3id],
                        targeted_versions: Vec::new(),
                        removed_locations: Vec::new(),
                        remove_all_resolvable_locations,
                    });
            }
            Ok(ResolvedPreflightTargets {
                targets: targets.into_values().collect(),
                complete: true,
            })
        }
        MetadataReferencePreflightTarget::BucketPrefix {
            bucket,
            prefix,
            operation,
        } => {
            if bucket.trim().is_empty() {
                return Err(MetadataApiError::BadRequest);
            }
            let bucket_info =
                match drive(GetBucketInfoOperation::new(bucket.clone()), context).await {
                    Ok(Some(Ok(info))) => info,
                    Ok(Some(Err(GetBucketInfoError::NotFound))) => {
                        return Err(MetadataApiError::NotFound);
                    }
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                        return Err(MetadataApiError::ServiceUnavailable);
                    }
                };
            let prefix = prefix.filter(|prefix| !prefix.is_empty());
            let permission_path = match prefix.as_deref() {
                Some(prefix) => blob_object_permission_path(
                    realm_id,
                    bucket_info.group_id,
                    local_node_id,
                    &bucket,
                    prefix,
                ),
                None => blob_bucket_permission_path(
                    realm_id,
                    bucket_info.group_id,
                    local_node_id,
                    &bucket,
                ),
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

            let versions = match operation {
                MetadataPreflightStorageOperation::LatestVersionTombstone => {
                    resolve_current_preflight_versions(context, &bucket, prefix.as_deref()).await?
                }
                MetadataPreflightStorageOperation::AllVersionsPurge => {
                    resolve_all_preflight_versions(context, &bucket, prefix.as_deref()).await?
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
                        blob_object_permission_path(
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
            let mut complete = true;
            let mut targets = BTreeMap::<[u8; 32], MetadataPreflightResolvedTarget>::new();
            for (version_key, version) in versions {
                let content_hash = match version.state {
                    BlobVersionState::Materialized { blob_hash, .. } => blob_hash,
                    BlobVersionState::Reference { .. } => {
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
                    MetadataPreflightResolvedTarget {
                        queried_iris: vec![content_w3id.clone()],
                        content_w3id,
                        content_hash,
                        targeted_versions: Vec::new(),
                        removed_locations: Vec::new(),
                        remove_all_resolvable_locations: false,
                    }
                });
                add_location_iris(
                    &mut target.queried_iris,
                    s3_endpoint,
                    &location.bucket,
                    &location.key,
                );
                target.targeted_versions.push(location.clone());
                if operation == MetadataPreflightStorageOperation::AllVersionsPurge {
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

pub(super) async fn resolve_current_preflight_versions(
    context: &DriverContext,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<Vec<(VersionKey, BlobVersion)>, MetadataApiError> {
    let prefix_key = match prefix {
        Some(prefix) => BlobHeadKey::object_prefix(bucket, prefix),
        None => BlobHeadKey::bucket_prefix(bucket),
    }
    .map_err(|_| MetadataApiError::BadRequest)?;
    let heads = scan_preflight_rows(context, BLOB_HEAD_KEYSPACE, prefix_key.into()).await?;
    if heads.len() > METADATA_PREFLIGHT_MAX_TARGET_VERSIONS {
        return Err(MetadataApiError::BadRequest);
    }
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
    Ok(versions)
}

pub(super) async fn resolve_all_preflight_versions(
    context: &DriverContext,
    bucket: &str,
    key_prefix: Option<&str>,
) -> Result<Vec<(VersionKey, BlobVersion)>, MetadataApiError> {
    let prefix = VersionKey::bucket_prefix(bucket)
        .map_err(|_| MetadataApiError::BadRequest)?
        .into();
    let rows = scan_preflight_rows(context, BLOB_VERSIONS_KEYSPACE, prefix).await?;
    let mut versions = Vec::new();
    for (key, value) in rows {
        let version_key = VersionKey::from_bytes(key.as_ref())
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        if key_prefix.is_some_and(|prefix| !version_key.key.starts_with(prefix)) {
            continue;
        }
        if versions.len() >= METADATA_PREFLIGHT_MAX_TARGET_VERSIONS {
            return Err(MetadataApiError::BadRequest);
        }
        let version = BlobVersion::from_bytes(value.as_ref())
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
        versions.push((version_key, version));
    }
    Ok(versions)
}

pub(super) async fn scan_preflight_rows(
    context: &DriverContext,
    key_space: &str,
    prefix: Key,
) -> Result<Vec<(Key, Value)>, MetadataApiError> {
    let mut start_after = None;
    let mut rows = Vec::new();
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.take().map(IterStart::After),
                limit: METADATA_PREFLIGHT_SCAN_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if rows.len().saturating_add(values.len()) > METADATA_REGISTRY_CANDIDATE_LIMIT {
                    return Err(MetadataApiError::BadRequest);
                }
                rows.extend(values);
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { .. }) => {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            _ => return Err(MetadataApiError::ServiceUnavailable),
        }
    }
    Ok(rows)
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
    mut request: MetadataReferencePreflightNodeRequest,
    s3_endpoint: Option<String>,
) -> Result<MetadataReferencePreflightNodeExecution, MetadataApiError> {
    if request.targets.len() > METADATA_PREFLIGHT_MAX_TARGET_VERSIONS
        || request.limit == 0
        || request.limit > METADATA_SEARCH_MAX_PAGINATION_DEPTH
    {
        return Err(MetadataApiError::BadRequest);
    }
    let auth = auth.ok_or(MetadataApiError::Unauthorized)?;
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let registry = handle
        .list_cached_registry_records()
        .await
        .map_err(map_metadata_internal_error)?;
    let registry = filter_live_records(&context.storage_handle, registry.as_ref()).await?;
    let freshness =
        crate::metadata::iri_index::iri_index_freshness(&context.storage_handle, registry.as_ref())
            .await
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let pending_for_realm = load_pending_records(context, None, METADATA_REGISTRY_CANDIDATE_LIMIT)
        .await?
        .into_values()
        .flatten()
        .any(|record| record.realm_id == realm_id);
    let index_state = match (freshness.state, pending_for_realm) {
        (crate::metadata::iri_index::IriIndexFreshnessState::Current, false) => {
            MetadataPreflightIndexState::Current
        }
        (crate::metadata::iri_index::IriIndexFreshnessState::Current, true)
        | (crate::metadata::iri_index::IriIndexFreshnessState::Pending, _) => {
            MetadataPreflightIndexState::Pending
        }
        (crate::metadata::iri_index::IriIndexFreshnessState::Failed, false) => {
            MetadataPreflightIndexState::Failed
        }
        (crate::metadata::iri_index::IriIndexFreshnessState::Failed, true)
        | (crate::metadata::iri_index::IriIndexFreshnessState::Mixed, _) => {
            MetadataPreflightIndexState::Mixed
        }
    };

    let mut iri_targets = BTreeMap::<String, BTreeSet<String>>::new();
    let mut target_locations = BTreeMap::<String, (bool, bool)>::new();
    let mut aliases_seen = false;
    for target in request.targets.iter_mut() {
        let mut local_aliases = drive(
            ResolveBlobPermissionPathsOperation::new(target.content_hash),
            context,
        )
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
            let removed = target.remove_all_resolvable_locations
                || target.removed_locations.contains(&location);
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
    let object_iris = iri_targets.keys().cloned().collect::<BTreeSet<_>>();
    let backlinks = crate::metadata::iri_index::lookup_iri_backlinks_for_objects(
        &context.storage_handle,
        registry.as_ref(),
        &object_iris,
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let registry_by_id = registry
        .iter()
        .map(|record| (record.document_id, record))
        .collect::<HashMap<_, _>>();
    let mut readable = HashMap::<Ulid, bool>::new();
    let mut titles = HashMap::<Ulid, String>::new();
    let mut hidden = BTreeSet::<String>::new();
    let mut visible = BTreeMap::<(String, Ulid), MetadataPreflightVisibleReference>::new();
    for (iri, iri_backlinks) in backlinks {
        let Some(content_w3ids) = iri_targets.get(&iri) else {
            continue;
        };
        for backlink in iri_backlinks {
            let Some(record) = registry_by_id.get(&backlink.document_id) else {
                continue;
            };
            let allowed = match readable.get(&record.document_id) {
                Some(allowed) => *allowed,
                None => {
                    let allowed = can_read_record(context, realm_id, Some(&auth), record).await?;
                    readable.insert(record.document_id, allowed);
                    allowed
                }
            };
            for content_w3id in content_w3ids {
                if !allowed {
                    hidden.insert(content_w3id.clone());
                    continue;
                }
                let visible_key = (content_w3id.clone(), record.document_id);
                if visible.contains_key(&visible_key) || visible.len() > request.limit {
                    continue;
                }
                let title = match titles.get(&record.document_id) {
                    Some(title) => title.clone(),
                    None => {
                        let title = reference_document_title(context, record)
                            .await
                            .unwrap_or_else(|| record.document_path.clone());
                        titles.insert(record.document_id, title.clone());
                        title
                    }
                };
                visible
                    .entry(visible_key)
                    .or_insert(MetadataPreflightVisibleReference {
                        content_w3id: content_w3id.clone(),
                        document_id: record.document_id.to_string(),
                        title,
                    });
            }
        }
    }
    let saturated = visible.len() > request.limit;
    let visible_references = visible.into_values().take(request.limit).collect();
    let targets = request
        .targets
        .into_iter()
        .map(|target| {
            let (resolvable_location_found, resolvable_location_after_operation) = target_locations
                .remove(&target.content_w3id)
                .unwrap_or((false, false));
            MetadataReferencePreflightNodeTarget {
                hidden_references_exist: hidden.contains(&target.content_w3id),
                content_w3id: target.content_w3id,
                resolvable_location_found,
                resolvable_location_after_operation,
            }
        })
        .collect();
    Ok(MetadataReferencePreflightNodeExecution {
        visible_references,
        targets,
        freshness: MetadataPreflightNodeFreshness {
            node_id: local_node_id,
            index_state,
            oldest_status_updated_at_ms: freshness.oldest_status_updated_at_ms,
        },
        path_style_endpoint_available: s3_endpoint.is_some() || !aliases_seen,
        saturated,
    })
}

pub(super) fn preflight_fingerprint(
    targets: &[MetadataPreflightResolvedTarget],
    mode: Option<MetadataApiQueryMode>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"metadata-reference-preflight\0");
    hasher.update(&[match mode {
        None => 0,
        Some(MetadataApiQueryMode::Local) => 1,
        Some(MetadataApiQueryMode::Distributed) => 2,
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
    let title = crate::metadata::search_enrichment::hit_title(&properties, &record.document_path, "./");
    (!title.is_empty()).then_some(title)
}

pub async fn load_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Option<RealmConfigDocument> {
    match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(error = %error, "realm config unavailable; querying the local replica only");
            None
        }
    }
}

pub async fn load_metadata_realm_nodes(
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
) -> MetadataRealmNodeDiscovery {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return MetadataRealmNodeDiscovery {
            nodes: vec![local_node_id],
            failed: true,
        };
    };
    // Race discovery so fanout queries degrade to reachable/local partitions
    // instead of stalling behind offline peers.
    let discovery = tokio::time::timeout(
        REALM_DISCOVERY_TIMEOUT,
        drive(GetRealmNodesOperation::new(realm_id), context),
    )
    .await;
    let nodes = match discovery {
        // Bounded-stale candidates are allowed here; an unreachable one is
        // reported through the existing partial-result fields.
        Ok(Ok(presence)) => match authorized_realm_nodes(&config, presence.into_nodes()) {
            Ok(nodes) => (nodes, false),
            Err(error) => {
                warn!(error = %error, "realm config contains invalid node ids; using local-only metadata results");
                return MetadataRealmNodeDiscovery {
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
    MetadataRealmNodeDiscovery { nodes, failed }
}
