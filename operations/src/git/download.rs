//! Reads stored Git packs and LFS content from this node or from any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::git::{GitPack, GitPackRecord, GitRecord, StoredObject, git_pack_key};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::time::Instant;
use ulid::Ulid;

use super::lfs::{PermitObjectOperation, locate};
use super::objects::{self, Blob};
use super::{GitError, publish, records, repository};
use crate::distribution::resolve::{RESOLVE_BUDGET, resolve_version, version_found};
use crate::driver::{DriverContext, drive, drive_until};
use crate::replication::bao_read::BaoReadOutput;
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::GetObjectInput;
use crate::s3::object::read::get_object_routed;

/// The bytes of a pack `owner` names first. A pack this node still keeps only as an object
/// from before packs moved into Fjall is moved there once and replicated.
pub(super) async fn pack_bytes(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    pack: &GitPack,
    owner: &GitRecord,
) -> Result<Bytes, GitError> {
    let digest = pack.digest().ok_or(GitError::Invalid)?;
    let key = git_pack_key(document.document_id, &digest).to_vec();
    if let Some(stored) = drive(
        records::load::<GitPackRecord>(GIT_PACK_KEYSPACE, key),
        context,
    )
    .await?
    {
        return Ok(stored.bytes);
    }
    let Some(object) = drive(objects::copy(document, &pack.sha256), context).await? else {
        return Err(GitError::Unavailable);
    };
    let auth = super::project::author(owner.user_id);
    let mut blob = open(context, &auth, document, &object, &[]).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk.map_err(|_| GitError::Unavailable)?);
        if bytes.len() as u64 > pack.size {
            return Err(GitError::Invalid);
        }
    }
    let bytes = Bytes::from(bytes);
    if objects::describe_pack(&bytes).ok().as_ref() != Some(pack) {
        return Err(GitError::Invalid);
    }
    let record = super::publish::pack_record(owner, bytes.clone());
    drive(
        super::publish::publish_pack(context.node_id(), document, record),
        context,
    )
    .await?;
    Ok(bytes)
}

/// Opens exact bytes: this node's copy, the recording node's version, or any holder's copy.
pub(super) async fn open(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
    holders: &[NodeId],
) -> Result<Blob, GitError> {
    let node = context.node_id().ok_or(GitError::Unavailable)?;
    let own = drive(objects::copy(document, &object.sha256), context).await?;
    if let Some(own) = own.as_ref().or((object.node_id == node).then_some(object)) {
        let input = GetObjectInput {
            bucket: own.bucket.clone(),
            key: own.key.clone(),
            version_id: Some(own.version_id),
            range: None,
            group_id: own.group_id.ok_or(GitError::Unavailable)?,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: node,
        };
        return get_object_routed(context, input, auth.path_restrictions.clone())
            .await
            .map(|result| result.blob)
            .map_err(|_| GitError::Unavailable);
    }
    let exact = BaoReadTarget::ExactVersion(VersionedObjectArn {
        realm_id: auth.realm_id,
        node_id: object.node_id,
        bucket: object.bucket.clone(),
        key: object.key.clone(),
        version: object.version_id,
    });
    let sources = std::iter::once((object.node_id, exact)).chain(
        holders
            .iter()
            .filter(|holder| **holder != node && **holder != object.node_id)
            .map(|holder| (*holder, BaoReadTarget::Blake3(object.blake3))),
    );
    for (source, target) in sources {
        let request = BaoReadRequest {
            auth_context: auth.clone(),
            realm_id: auth.realm_id,
            target,
            expected_blake3: Some(object.blake3),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        if let Ok(BaoReadOutput::Stream { blob, size, .. }) =
            drive(ManagedReadOperation::new(source, request), context).await
            && size == object.size
        {
            return Ok(blob);
        }
    }
    Err(GitError::Unavailable)
}

/// The members that keep a pinned distributed version, found through the resolver.
async fn set_holders(context: &DriverContext, object: &StoredObject) -> Vec<NodeId> {
    match &object.global {
        Some(GlobalId::Version { version_id, .. }) => {
            let deadline = Instant::now() + RESOLVE_BUDGET;
            let resolve = resolve_version(context.node_and_realm(), *version_id, deadline);
            version_found(drive_until(resolve, context, deadline).await)
                .map(|resolved| resolved.holders)
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// Streams LFS content after authorizing the original object. Content held elsewhere is
/// relayed from its node and not stored here.
pub async fn download(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    oid: &str,
) -> Result<(Blob, u64), GitError> {
    let (document, _) = drive(
        repository(context.node_id(), auth, id, Permission::READ),
        context,
    )
    .await?;
    let original = drive(locate(&document, oid), context)
        .await?
        .ok_or(GitError::NotFound)?;
    let permit = PermitObjectOperation::new(auth, &original, Permission::READ);
    drive(permit, context).await?;
    let node = context.node_id().ok_or(GitError::Unavailable)?;
    let local = match drive(objects::copy(&document, oid), context).await? {
        Some(copy) => copy,
        None if original.node_id == node => original,
        None => {
            let mut holders =
                drive(publish::holders(context.node_id(), &document), context).await?;
            holders.extend(set_holders(context, &original).await);
            let blob = open(context, auth, &document, &original, &holders).await?;
            return Ok((blob, original.size));
        }
    };
    let blob = open(context, auth, &document, &local, &[]).await?;
    Ok((blob, local.size))
}
