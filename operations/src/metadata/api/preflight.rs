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
