//! Authorizes native Git repositories and serves them from records on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod changes;
pub mod hook;
pub mod lfs;
pub mod locks;
pub mod merge;
pub mod objects;
pub mod project;
pub mod publish;
pub mod push;
mod records;
pub mod snapshot;
pub mod state;
pub mod versions;

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
use std::collections::HashMap;
use std::sync::LazyLock;
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
    #[error("the branch moved since the version the request named")]
    Stale,
    #[error("the name already exists")]
    Exists,
    #[error("{0}")]
    Refused(String),
    #[error("the merge has conflicts")]
    MergeConflict(Box<MergeConflict>),
    #[error("only a push's own receive hook may record it")]
    NotHook,
}

/// Why a merge could not be completed; nothing was changed.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeConflict {
    /// Files both sides changed that no metadata merge resolves.
    pub files: Vec<String>,
    pub properties: Vec<PropertyConflict>,
}

/// A metadata property both sides changed from their common version to different values.
#[derive(Clone, Debug, PartialEq)]
pub struct PropertyConflict {
    pub entity: String,
    pub property: String,
    pub source: Vec<serde_json::Value>,
    pub target: Vec<serde_json::Value>,
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
    let write = permission == Permission::WRITE;
    let (document, repository) =
        repository(context, auth, request.repository.document_id, permission).await?;
    if repository != request.repository {
        return Err(GitError::Conflict);
    }
    let mut request = request;
    let _guard = project::lock(document.document_id).await;
    snapshot::refresh(context, store, &document).await?;
    let _key = write.then(|| {
        let key = PushKey::open(document.document_id);
        request.push_key = key.value.clone();
        key
    });
    store
        .execute(GitEffect::Http(Box::new(request)), auth.user_id)
        .await
        .map_err(|_| GitError::Unavailable)
}

/// Keys of receive-pack requests running on this node, one per document under its lock.
static PUSH_KEYS: LazyLock<std::sync::Mutex<HashMap<Ulid, String>>> =
    LazyLock::new(Default::default);

/// A push key that is valid while the receive-pack request that created it runs.
struct PushKey {
    document_id: Ulid,
    value: String,
}

impl PushKey {
    fn open(document_id: Ulid) -> Self {
        let value = hex::encode(rand::random::<[u8; 32]>());
        if let Ok(mut keys) = PUSH_KEYS.lock() {
            keys.insert(document_id, value.clone());
        }
        Self { document_id, value }
    }
}

impl Drop for PushKey {
    fn drop(&mut self) {
        if let Ok(mut keys) = PUSH_KEYS.lock() {
            keys.remove(&self.document_id);
        }
    }
}

/// Whether `key` belongs to the push now running for the document.
pub fn push_key_valid(document_id: Ulid, key: &str) -> bool {
    PUSH_KEYS.lock().is_ok_and(|keys| {
        keys.get(&document_id).is_some_and(|expected| {
            expected.len() == key.len()
                && expected
                    .bytes()
                    .zip(key.bytes())
                    .fold(0u8, |difference, (a, b)| difference | (a ^ b))
                    == 0
        })
    })
}
