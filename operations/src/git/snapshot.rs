//! Materializes each document's accepted metadata as an ARC on its fixed Git owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{GitError, document, records};
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::git::{
    GitEffect, GitEvent, GitRepository, GitSnapshot, GitStatus, REPOSITORIES, STATUS,
};
use aruna_core::keyspaces::{EVENT_LOG_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEventRecord, MetadataRawRevision,
};
use aruna_core::storage_entries::{event_log_key, materialization_status_key};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use std::time::{Duration, UNIX_EPOCH};
use ulid::Ulid;

pub async fn capture(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    revision: Option<&MetadataRawRevision>,
) -> Result<(), GitError> {
    let Some(store) = context
        .metadata_handle
        .as_ref()
        .and_then(|handle| handle.git())
    else {
        return Ok(());
    };
    let origin: MetadataEventRecord = records::load(
        context,
        EVENT_LOG_KEYSPACE,
        event_log_key(record.document_id, record.establishing_event_id).to_vec(),
    )
    .await?
    .ok_or(GitError::Unavailable)?;
    if origin.record.document_id != record.document_id
        || origin.event_id != record.establishing_event_id
    {
        return Err(GitError::Unavailable);
    }
    let owner = if origin.record.holder_node_ids.contains(&origin.node_id) {
        origin.node_id
    } else {
        *origin
            .record
            .holder_node_ids
            .first()
            .ok_or(GitError::Unavailable)?
    };
    if context.net_handle.as_ref().map(|net| net.node_id()) != Some(owner) {
        return Ok(());
    }
    let (event_id, jsonld) = match revision {
        Some(revision) => (revision.winning_event_id, revision.jsonld.clone()),
        None => {
            let handle = context
                .metadata_handle
                .as_ref()
                .ok_or(GitError::Unavailable)?;
            let jsonld = handle
                .export_rocrate_jsonld(record.graph_iri.clone())
                .await
                .map_err(|_| GitError::Unavailable)?;
            let projected: MaterializationStatusRecord = records::load(
                context,
                MATERIALIZATION_STATUS_KEYSPACE,
                materialization_status_key(record.document_id).to_vec(),
            )
            .await?
            .ok_or(GitError::Unavailable)?;
            if projected.event_id != record.last_event_id {
                return Ok(());
            }
            (record.last_event_id, jsonld)
        }
    };
    let id = record.document_id;
    let existing: Option<GitRepository> =
        records::load(context, REPOSITORIES, id.to_bytes().to_vec()).await?;
    let repository = existing.unwrap_or_else(|| GitRepository {
        document_id: id,
        group_id: record.group_id,
        bucket: format!("arc-{}", record.group_id.to_string().to_lowercase()),
        arc: true,
    });
    if repository.group_id != record.group_id || !repository.arc {
        return Err(GitError::Conflict);
    }
    match drive(GetBucketOperation::new(repository.bucket.clone()), context).await {
        Ok(bucket) if bucket.group_id == record.group_id => {}
        Ok(_) => return Err(GitError::Conflict),
        Err(GetBucketError::NotFound) => {
            let bucket = BucketInfo {
                group_id: record.group_id,
                created_at: UNIX_EPOCH + Duration::from_millis(record.created_at_ms),
                created_by: origin.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            };
            match drive(
                CreateBucketOperation::new(repository.bucket.clone(), bucket),
                context,
            )
            .await
            {
                Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => {}
                Err(_) => return Err(GitError::Unavailable),
            }
            let bucket = drive(GetBucketOperation::new(repository.bucket.clone()), context)
                .await
                .map_err(|_| GitError::Unavailable)?;
            if bucket.group_id != record.group_id {
                return Err(GitError::Conflict);
            }
        }
        Err(_) => return Err(GitError::Unavailable),
    }
    store
        .execute(GitEffect::Initialize(id), origin.user_id)
        .await
        .map_err(|_| GitError::Unavailable)?;
    let bound: GitRepository =
        records::insert(context, REPOSITORIES, id.to_bytes().to_vec(), &repository).await?;
    if bound != repository {
        return Err(GitError::Conflict);
    }
    let GitEvent::Snapshot(status) = store
        .execute(
            GitEffect::Snapshot(GitSnapshot {
                document_id: id,
                event_id,
                occurred_at_ms: record.updated_at_ms,
                jsonld,
            }),
            origin.user_id,
        )
        .await
        .map_err(|_| GitError::Unavailable)?
    else {
        return Err(GitError::Unavailable);
    };
    records::save(context, STATUS, id.to_bytes().to_vec(), &status).await
}

pub async fn ensure(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> Result<GitRepository, GitError> {
    let record = document(context, auth, id, permission).await?;
    let current: Option<GitRepository> =
        records::load(context, REPOSITORIES, id.to_bytes().to_vec()).await?;
    let status: Option<GitStatus> = records::load(context, STATUS, id.to_bytes().to_vec()).await?;
    if let Some(repository) = current
        && status.is_some_and(|status| status.error.is_none())
    {
        if repository.document_id != id || repository.group_id != record.group_id {
            return Err(GitError::Unavailable);
        }
        return Ok(repository);
    }
    let raw = crate::metadata::raw_revision::load_raw_view(context, id, None)
        .await
        .map_err(|_| GitError::Unavailable)?;
    if raw.is_none() {
        let projected: MaterializationStatusRecord = records::load(
            context,
            MATERIALIZATION_STATUS_KEYSPACE,
            materialization_status_key(id).to_vec(),
        )
        .await?
        .ok_or(GitError::Unavailable)?;
        if projected.state != MaterializationState::Materialized
            || projected.event_id != record.last_event_id
        {
            return Err(GitError::Unavailable);
        }
    }
    capture(context, &record, raw.as_ref().map(|value| &value.revision)).await?;
    let repository: GitRepository = records::load(context, REPOSITORIES, id.to_bytes().to_vec())
        .await?
        .ok_or(GitError::NotFound)?;
    if repository.document_id != id || repository.group_id != record.group_id {
        return Err(GitError::Unavailable);
    }
    Ok(repository)
}

pub async fn status(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
) -> Result<Option<GitStatus>, GitError> {
    document(context, auth, id, Permission::READ).await?;
    records::load(context, STATUS, id.to_bytes().to_vec()).await
}

pub async fn export(
    context: &DriverContext,
    store: &aruna_blob::git::GitStore,
    auth: &AuthContext,
    id: Ulid,
    revision: String,
) -> Result<bytes::Bytes, GitError> {
    ensure(context, auth, id, Permission::READ).await?;
    let GitEvent::Exported(bytes) = store
        .execute(
            GitEffect::Export {
                document_id: id,
                revision,
            },
            auth.user_id,
        )
        .await
        .map_err(|_| GitError::Unavailable)?
    else {
        return Err(GitError::Unavailable);
    };
    let result: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| GitError::Unavailable)?;
    if result.get("error").is_some() {
        return Err(GitError::Invalid);
    }
    Ok(bytes)
}
