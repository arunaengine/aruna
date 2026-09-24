//! Stores Git packs and LFS content as Aruna objects and reads them from any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{GitError, records};
use crate::driver::{DriverContext, bucket_snapshot, drive, gate_context, now_ms};
use crate::realm::get_config::GetConfigOperation;
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{GetObjectInput, get_object_info, get_object_routed};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use aruna_core::NodeId;
use aruna_core::git::{LOCAL_OBJECTS, StoredObject};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use bytes::Bytes;
use futures_util::StreamExt;
use std::time::{Duration, UNIX_EPOCH};

pub type Blob = BackendStream<Result<Bytes, StreamError>>;

fn local(context: &DriverContext) -> Result<NodeId, GitError> {
    context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(GitError::Unavailable)
}

fn index_key(document: &MetadataRegistryRecord, sha256: &str) -> Vec<u8> {
    [
        document.document_id.to_bytes().as_slice(),
        sha256.as_bytes(),
    ]
    .concat()
}

/// This node's group-local ARC bucket, created through ordinary bucket operations.
pub async fn bucket(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    auth: &AuthContext,
) -> Result<BucketInfo, GitError> {
    let name = format!("arc-{}", document.group_id.to_string().to_lowercase());
    match drive(GetBucketOperation::new(name.clone()), context).await {
        Ok(bucket) if bucket.group_id == document.group_id => return Ok(bucket),
        Ok(_) => return Err(GitError::Conflict),
        Err(GetBucketError::NotFound) => {}
        Err(_) => return Err(GitError::Unavailable),
    }
    let info = BucketInfo {
        group_id: document.group_id,
        created_at: UNIX_EPOCH + Duration::from_millis(document.created_at_ms),
        created_by: auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    match drive(CreateBucketOperation::new(name.clone(), info), context).await {
        Ok(_) | Err(CreateBucketError::BucketAlreadyExists) => {}
        Err(_) => return Err(GitError::Unavailable),
    }
    let bucket = drive(GetBucketOperation::new(name), context)
        .await
        .map_err(|_| GitError::Unavailable)?;
    if bucket.group_id != document.group_id {
        return Err(GitError::Conflict);
    }
    Ok(bucket)
}

/// This node's own copy of content with `sha256`, if it has stored one.
pub async fn copy(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    sha256: &str,
) -> Result<Option<StoredObject>, GitError> {
    records::load(context, LOCAL_OBJECTS, index_key(document, sha256)).await
}

/// Stores content under `key` in this node's ARC bucket after checking its SHA-256.
pub async fn store(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    object: (String, u64, &str),
    body: Blob,
) -> Result<StoredObject, GitError> {
    let (key, size, sha256) = object;
    if let Some(existing) = copy(context, document, sha256).await? {
        return Ok(existing);
    }
    let node = local(context)?;
    let bucket = bucket(context, document, auth).await?;
    let routing = bucket_snapshot(context, &bucket)
        .await
        .map_err(|_| GitError::Unavailable)?;
    let gate = gate_context(context, auth.realm_id, now_ms())
        .await
        .map_err(|_| GitError::Unavailable)?;
    let quota = drive(GetConfigOperation::new(auth.realm_id), context)
        .await
        .map_err(|_| GitError::Unavailable)?
        .quota
        .effective_group_ceiling(&document.group_id);
    let name = format!("arc-{}", document.group_id.to_string().to_lowercase());
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id: document.group_id,
        realm_id: auth.realm_id,
        node_id: node,
        request: PutObjectInput {
            bucket: name.clone(),
            key: key.clone(),
            content_length: Some(size),
            body: Some(body),
        },
        expected_checksums: vec![ExpectedChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            digest: hex::decode(sha256).map_err(|_| GitError::Invalid)?,
        }],
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: quota,
        routing,
    })
    .with_bucket_guard(bucket)
    .with_restrictions(auth.path_restrictions.clone());
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let result = drive(operation, context)
        .await
        .map_err(|error| match error {
            PutObjectError::IncompleteBody
            | PutObjectError::MissingBody
            | PutObjectError::ChecksumMismatch(_) => GitError::Invalid,
            _ => GitError::Unavailable,
        })?;
    let input = GetObjectInput {
        bucket: name.clone(),
        key: key.clone(),
        version_id: Some(result.version_id),
        range: None,
        group_id: document.group_id,
        user_identity: auth.user_id,
        node_id: node,
    };
    let info = get_object_info(context, input, auth.path_restrictions.clone())
        .await
        .map_err(|_| GitError::Unavailable)?;
    let blake3 = info
        .hashes
        .get("blake3")
        .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok())
        .ok_or(GitError::Unavailable)?;
    let stored = StoredObject {
        node_id: node,
        bucket: name,
        key,
        version_id: result.version_id,
        size,
        sha256: sha256.to_string(),
        blake3,
    };
    records::insert(context, LOCAL_OBJECTS, index_key(document, sha256), &stored).await
}

/// Opens exact bytes: this node's copy, the recording node's version, or any holder's copy.
pub async fn open(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
    holders: &[NodeId],
) -> Result<Blob, GitError> {
    let node = local(context)?;
    let own = copy(context, document, &object.sha256).await?;
    if let Some(own) = own.as_ref().or((object.node_id == node).then_some(object)) {
        let input = GetObjectInput {
            bucket: own.bucket.clone(),
            key: own.key.clone(),
            version_id: Some(own.version_id),
            range: None,
            group_id: document.group_id,
            user_identity: auth.user_id,
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
        };
        if let Ok(BaoReadOutput::Stream { blob, size, .. }) =
            managed_read(context, source, request).await
            && size == object.size
        {
            return Ok(blob);
        }
    }
    Err(GitError::Unavailable)
}

/// Reads a bounded object fully, checks its SHA-256 and keeps a local copy of remote content.
pub async fn fetch(
    context: &DriverContext,
    auth: &AuthContext,
    document: &MetadataRegistryRecord,
    object: &StoredObject,
    holders: &[NodeId],
) -> Result<Bytes, GitError> {
    if usize::try_from(object.size).map_or(true, |size| size > aruna_core::git::MAX_GIT_BYTES) {
        return Err(GitError::Invalid);
    }
    let mut blob = open(context, auth, document, object, holders).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk.map_err(|_| GitError::Unavailable)?);
        if bytes.len() as u64 > object.size {
            return Err(GitError::Invalid);
        }
    }
    let hashes = aruna_blob::hash::Hasher::new_with_bytes(&bytes).to_map();
    if hashes.get("sha256").map(hex::encode).as_deref() != Some(object.sha256.as_str()) {
        return Err(GitError::Invalid);
    }
    let bytes = Bytes::from(bytes);
    if copy(context, document, &object.sha256).await?.is_none() && object.node_id != local(context)?
    {
        let key = format!("git-copies/{}/{}", document.document_id, object.sha256);
        let body = BackendStream::new(futures_util::stream::iter([Ok::<_, StreamError>(
            bytes.clone(),
        )]));
        store(
            context,
            auth,
            document,
            (key, object.size, &object.sha256),
            body,
        )
        .await?;
    }
    Ok(bytes)
}
