//! Transfers LFS content through Aruna object operations on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::objects::{self, Blob};
use super::state::{Ancestry, reduce};
use super::{GitError, publish, records, repository};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::replication::bao_read::{BaoReadError, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use aruna_core::git::{LfsObject, StoredObject};
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use bytes::Bytes;
use ulid::Ulid;

fn key(id: Ulid, oid: &str) -> String {
    format!("git-lfs/{id}/{oid}")
}

/// Where content was originally stored: a replicated record, or an upload not pushed yet.
async fn locate(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    oid: &str,
) -> Result<Option<StoredObject>, GitError> {
    let records = records::scan(context, document.document_id).await?;
    if let Some(object) = reduce(&records, &Ancestry::new()).0.lfs.remove(oid) {
        return Ok(Some(object));
    }
    objects::copy(context, document, oid).await
}

/// Access is always decided for the original object, never for a node's copy of it. With a
/// known bucket group the path policy decides here; otherwise the source node decides.
async fn permit(
    context: &DriverContext,
    auth: &AuthContext,
    object: &StoredObject,
    permission: Permission,
) -> Result<(), GitError> {
    let Some(group_id) = object.group_id else {
        if permission != Permission::READ {
            return Err(GitError::Invalid);
        }
        let request = BaoReadRequest {
            auth_context: auth.clone(),
            realm_id: auth.realm_id,
            target: BaoReadTarget::ExactVersion(VersionedObjectArn {
                realm_id: auth.realm_id,
                node_id: object.node_id,
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version: object.version_id,
            }),
            expected_blake3: Some(object.blake3),
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
        };
        return match managed_read(context, object.node_id, request).await {
            Ok(_) => Ok(()),
            Err(BaoReadError::Refused(_)) => Err(GitError::NotFound),
            Err(_) => Err(GitError::Unavailable),
        };
    };
    let path = object_permission_path(
        auth.realm_id,
        group_id,
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
        group_id: Some(document.group_id),
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

/// Streams LFS content after authorizing the original object. Content held elsewhere is
/// first copied to this node, whose copy serves later requests even if the source leaves.
pub async fn download(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    oid: &str,
) -> Result<(Blob, u64), GitError> {
    let (document, _) = repository(context, auth, id, Permission::READ).await?;
    let original = locate(context, &document, oid)
        .await?
        .ok_or(GitError::NotFound)?;
    permit(context, auth, &original, Permission::READ).await?;
    let node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)?;
    let local = match objects::copy(context, &document, oid).await? {
        Some(copy) => copy,
        None if original.node_id == node => original,
        None => {
            let holders = publish::holders(context, &document).await?;
            let blob = objects::open(context, auth, &document, &original, &holders).await?;
            let content = (format!("git-copies/{id}/{oid}"), original.size, oid);
            objects::store(context, auth, &document, content, blob).await?
        }
    };
    let blob = objects::open(context, auth, &document, &local, &[]).await?;
    Ok((blob, local.size))
}
