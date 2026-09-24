//! Authorizes native Git repositories and executes their storage and transport effects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod hook;
pub mod lfs;
pub mod publish;
mod records;
pub mod snapshot;
pub mod state;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::metadata::get_document::load_document_record;
use crate::metadata::repository::{parse_lifecycle_read, read_lifecycle_effect};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_blob::git::GitStore;
use aruna_core::NodeId;
use aruna_core::git::{GitEffect, GitEvent, GitRepository, GitRequest, REPOSITORIES};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("repository not found")]
    NotFound,
    #[error("invalid Git or LFS request")]
    Invalid,
    #[error("repository binding already exists")]
    Conflict,
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
    #[error("Git storage unavailable")]
    Unavailable,
    #[error("this node does not hold the document")]
    NotHolder,
    #[error("the document's Git history needs a checkpoint before more changes")]
    Full,
}

pub async fn document(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> Result<MetadataRegistryRecord, GitError> {
    let record = load_document_record(context, id)
        .await
        .map_err(|_| GitError::Unavailable)?
        .ok_or(GitError::NotFound)?;
    authorize(
        context,
        record.realm_id,
        auth,
        &record.permission_path,
        &permission,
        PolicyRequestExtras::rest(),
    )
    .await?;
    let lifecycle = parse_lifecycle_read(
        context
            .storage_handle
            .send_effect(read_lifecycle_effect(&record.graph_iri, None))
            .await,
    )
    .map_err(|_| GitError::Unavailable)?;
    if lifecycle.is_some_and(|state| state.is_deleted()) {
        return Err(GitError::NotFound);
    }
    Ok(record)
}

pub async fn repository(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> Result<GitRepository, GitError> {
    let document = document(context, auth, id, permission).await?;
    let record: GitRepository = records::load(context, REPOSITORIES, id.to_bytes().to_vec())
        .await?
        .ok_or(GitError::NotFound)?;
    if record.document_id != id || record.group_id != document.group_id {
        return Err(GitError::Unavailable);
    }
    Ok(record)
}

pub async fn create(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    node: NodeId,
    id: Ulid,
    bucket: String,
    arc: bool,
) -> Result<GitRepository, GitError> {
    let document = document(context, auth, id, Permission::WRITE).await?;
    if !arc {
        return Err(GitError::Invalid);
    }
    authorize(
        context,
        auth.realm_id,
        auth,
        &bucket_permission_path(auth.realm_id, document.group_id, node, &bucket),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await?;
    let info = drive(GetBucketOperation::new(bucket.clone()), context)
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => GitError::NotFound,
            _ => GitError::Unavailable,
        })?;
    if info.group_id != document.group_id {
        return Err(GitError::NotFound);
    }
    let repository = GitRepository {
        document_id: id,
        group_id: info.group_id,
        bucket,
        arc,
    };
    store
        .execute(GitEffect::Initialize(id), auth.user_id)
        .await
        .map_err(|_| GitError::Unavailable)?;
    let existing =
        records::insert(context, REPOSITORIES, id.to_bytes().to_vec(), &repository).await?;
    if existing != repository {
        return Err(GitError::Conflict);
    }
    Ok(existing)
}

pub async fn transport(
    context: &DriverContext,
    store: &GitStore,
    auth: &AuthContext,
    request: GitRequest,
) -> Result<GitEvent, GitError> {
    let permission =
        if request.action == "git-receive-pack" || request.query == "service=git-receive-pack" {
            Permission::WRITE
        } else {
            Permission::READ
        };
    let record = repository(context, auth, request.repository.document_id, permission).await?;
    if record != request.repository {
        return Err(GitError::Conflict);
    }
    store
        .execute(GitEffect::Http(Box::new(request)), auth.user_id)
        .await
        .map_err(|_| GitError::Unavailable)
}
