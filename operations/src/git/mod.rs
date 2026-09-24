//! Authorizes native Git repositories and serves them from records on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod hook;
pub mod lfs;
pub mod locks;
pub mod objects;
pub mod project;
pub mod publish;
pub mod push;
mod records;
pub mod snapshot;
pub mod state;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::metadata::get_document::load_document_record;
use crate::metadata::repository::{parse_lifecycle_read, read_lifecycle_effect};
use aruna_blob::git::GitStore;
use aruna_core::git::{GitEffect, GitEvent, GitRepository, GitRequest};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("repository not found")]
    NotFound,
    #[error("invalid Git or LFS request")]
    Invalid,
    #[error("repository state conflicts with the request")]
    Conflict,
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
    #[error("Git storage unavailable")]
    Unavailable,
    #[error("this node does not hold the document")]
    NotHolder,
    #[error("the document's Git history needs a checkpoint before more changes")]
    Full,
    #[error("{0} is locked by another user")]
    Locked(String),
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

/// The document's repository on this holder; its LFS content lives in the node's ARC bucket.
pub async fn repository(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    permission: Permission,
) -> Result<(MetadataRegistryRecord, GitRepository), GitError> {
    let document = document(context, auth, id, permission).await?;
    publish::holders(context, &document).await?;
    let repository = GitRepository {
        document_id: id,
        group_id: document.group_id,
        bucket: format!("arc-{}", document.group_id.to_string().to_lowercase()),
        arc: true,
    };
    Ok((document, repository))
}

/// Serves Git over HTTP after bringing the local cache up to date from the records.
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
    let (document, repository) =
        repository(context, auth, request.repository.document_id, permission).await?;
    if repository != request.repository {
        return Err(GitError::Conflict);
    }
    let _guard = project::lock(document.document_id).await;
    snapshot::refresh(context, store, &document).await?;
    store
        .execute(GitEffect::Http(Box::new(request)), auth.user_id)
        .await
        .map_err(|_| GitError::Unavailable)
}
