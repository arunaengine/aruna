//! Transfers LFS content through Aruna object operations on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::objects::{self, Blob};
use super::state::{Ancestry, reduce};
use super::{GitError, publish, records, repository};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use aruna_core::git::{LfsObject, StoredObject};
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use bytes::Bytes;
use ulid::Ulid;

fn key(id: Ulid, oid: &str) -> String {
    format!("git-lfs/{id}/{oid}")
}

/// This node's copy, or the location a replicated record names.
async fn locate(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    oid: &str,
) -> Result<Option<StoredObject>, GitError> {
    if let Some(copy) = objects::copy(context, document, oid).await? {
        return Ok(Some(copy));
    }
    let records = records::scan(context, document.document_id).await?;
    Ok(reduce(&records, &Ancestry::new()).0.lfs.remove(oid))
}

async fn permit(
    context: &DriverContext,
    auth: &AuthContext,
    object: &StoredObject,
    permission: Permission,
) -> Result<(), GitError> {
    let path = object_permission_path(
        auth.realm_id,
        object.group_id,
        object.node_id,
        &object.bucket,
        &object.key,
    );
    authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &permission,
        PolicyRequestExtras::rest(),
    )
    .await?;
    Ok(())
}

pub async fn inspect(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    object: &LfsObject,
    permission: Permission,
) -> Result<Option<StoredObject>, GitError> {
    if !object.valid() {
        return Err(GitError::Invalid);
    }
    let (document, _) = repository(context, auth, id, permission).await?;
    let location = locate(context, &document, &object.oid).await?;
    if location
        .as_ref()
        .is_some_and(|location| location.size != object.size)
    {
        return Err(GitError::Invalid);
    }
    Ok(location)
}

pub async fn upload(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    object: LfsObject,
    body: BackendStream<Result<Bytes, StreamError>>,
) -> Result<(), GitError> {
    if !object.valid() {
        return Err(GitError::Invalid);
    }
    let (document, repository) = repository(context, auth, id, Permission::WRITE).await?;
    if objects::copy(context, &document, &object.oid)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let target = StoredObject {
        node_id: node,
        group_id: document.group_id,
        bucket: repository.bucket,
        key: key(id, &object.oid),
        version_id: Ulid::nil(),
        size: object.size,
        sha256: object.oid.clone(),
        blake3: [0; 32],
    };
    permit(context, auth, &target, Permission::WRITE).await?;
    let content = (target.key, object.size, object.oid.as_str());
    objects::store(context, auth, &document, content, body)
        .await
        .map(|_| ())
}

/// Streams LFS content. Content held elsewhere is first copied to this node, whose copy
/// then serves this and later requests even if the original node leaves.
pub async fn download(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    oid: &str,
) -> Result<(Blob, u64), GitError> {
    let (document, _) = repository(context, auth, id, Permission::READ).await?;
    let mut object = locate(context, &document, oid)
        .await?
        .ok_or(GitError::NotFound)?;
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    if object.node_id != node {
        let holders = publish::holders(context, &document).await?;
        let blob = objects::open(context, auth, &document, &object, &holders).await?;
        let content = (format!("git-copies/{id}/{oid}"), object.size, oid);
        object = objects::store(context, auth, &document, content, blob).await?;
    }
    permit(context, auth, &object, Permission::READ).await?;
    let blob = objects::open(context, auth, &document, &object, &[]).await?;
    Ok((blob, object.size))
}
