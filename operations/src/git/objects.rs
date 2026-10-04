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
use crate::s3::object::get::{GetObjectInput, get_object_routed};
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use aruna_core::NodeId;
use aruna_core::git::{
    GitPack, GitPackRecord, GitRecord, LOCAL_OBJECTS, MAX_PACK_BYTES, StoredObject, git_pack_key,
};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::dataset_location::default_bucket;
use aruna_core::structs::storage::format::Compression;
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

/// This node's group-local dataset bucket, created through ordinary bucket operations.
pub async fn bucket(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    auth: &AuthContext,
) -> Result<BucketInfo, GitError> {
    let name = default_bucket(document.group_id);
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
        compression: Compression::Off,
        encryption: Default::default(),
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

/// Stores content under `key` in this node's dataset bucket after checking its SHA-256.
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
    let bucket = bucket(context, document, auth).await?;
    let name = default_bucket(document.group_id);
    let stored = put(context, auth, (&name, bucket), (key, size, sha256), body).await?;
    records::insert(context, LOCAL_OBJECTS, index_key(document, sha256), &stored).await
}

/// Writes content to `key` in a bucket of this node through the ordinary put with its
/// quota, gate and routing guards, after checking its SHA-256.
pub async fn put(
    context: &DriverContext,
    auth: &AuthContext,
    (name, bucket): (&str, BucketInfo),
    (key, size, sha256): (String, u64, &str),
    body: Blob,
) -> Result<StoredObject, GitError> {
    let node = local(context)?;
    let group_id = bucket.group_id;
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
        .effective_group_ceiling(&group_id);
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id,
        realm_id: auth.realm_id,
        node_id: node,
        request: PutObjectInput {
            bucket: name.to_string(),
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
    let stored = described(context, (name, group_id), &key, Some(result.version_id))
        .await?
        .ok_or(GitError::Unavailable)?;
    Ok(stored)
}

/// The size and digests of an object version of this node, or of its current version with
/// `None`; `None` when there is no such object. Callers authorize the read.
pub async fn described(
    context: &DriverContext,
    (name, group_id): (&str, aruna_core::types::GroupId),
    key: &str,
    version: Option<ulid::Ulid>,
) -> Result<Option<StoredObject>, GitError> {
    let node = local(context)?;
    let input = HeadObjectInput {
        bucket: name.to_string(),
        key: key.to_string(),
        version_id: version,
    };
    let head = match drive(HeadObjectOperation::new(input), context).await {
        Ok(head) => head,
        Err(_) if version.is_none() => return Ok(None),
        Err(_) => return Err(GitError::Unavailable),
    };
    let (Some(location), Some(version_id)) = (head.location, head.version_id) else {
        return Ok(None);
    };
    let digest = |name: &str| location.hashes.get(name).cloned();
    let blake3 = digest("blake3").and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
    let (Some(blake3), Some(sha256)) = (blake3, digest("sha256")) else {
        return Ok(None);
    };
    Ok(Some(StoredObject {
        node_id: node,
        group_id: Some(group_id),
        bucket: name.to_string(),
        key: key.to_string(),
        version_id,
        size: location.blob_size,
        sha256: hex::encode(sha256),
        blake3,
    }))
}

/// Describes a pack for a record. Larger packs are refused: large files belong in Git LFS.
pub fn describe_pack(pack: &Bytes) -> Result<GitPack, GitError> {
    if pack.len() > MAX_PACK_BYTES {
        return Err(GitError::Refused(format!(
            "the pushed Git objects exceed {} MiB; track large files with Git LFS",
            MAX_PACK_BYTES >> 20
        )));
    }
    let hashes = aruna_blob::hash::Hasher::new_with_bytes(pack).to_map();
    let sha256 = hashes
        .get("sha256")
        .map(hex::encode)
        .ok_or(GitError::Unavailable)?;
    Ok(GitPack {
        sha256,
        size: pack.len() as u64,
    })
}

/// The bytes of a pack `owner` names first. A pack this node still keeps only as an object
/// from before packs moved into Fjall is moved there once and replicated.
pub async fn pack_bytes(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    pack: &GitPack,
    owner: &GitRecord,
) -> Result<Bytes, GitError> {
    let digest = pack.digest().ok_or(GitError::Invalid)?;
    let key = git_pack_key(document.document_id, &digest).to_vec();
    if let Some(stored) = records::load::<GitPackRecord>(context, GIT_PACK_KEYSPACE, key).await? {
        return Ok(stored.bytes);
    }
    let Some(object) = copy(context, document, &pack.sha256).await? else {
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
    if describe_pack(&bytes).ok().as_ref() != Some(pack) {
        return Err(GitError::Invalid);
    }
    super::publish::publish_pack(
        context,
        document,
        super::publish::pack_record(owner, bytes.clone()),
    )
    .await?;
    Ok(bytes)
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
            group_id: own.group_id.ok_or(GitError::Unavailable)?,
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

/// Notes a published pack that was made from this node's cache as imported there, so the next
/// projection does not index objects the cache already holds. A failure only costs that import.
pub async fn mark_imported(
    context: &DriverContext,
    (document_id, actor): (ulid::Ulid, aruna_core::UserId),
    pack: &GitPack,
) {
    let Some(store) = context
        .metadata_handle
        .as_ref()
        .and_then(|handle| handle.git())
    else {
        return;
    };
    let effect = aruna_core::git::GitEffect::MarkImported {
        document_id,
        digest: pack.sha256.clone(),
    };
    if let Err(error) = store.execute(effect, actor).await {
        tracing::debug!(%document_id, %error, "Published pack not marked as imported");
    }
}
