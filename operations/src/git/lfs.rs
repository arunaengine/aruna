//! Transfers LFS content through Aruna's authorized, quota-governed object operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{GitError, records, repository};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, bucket_snapshot, drive, gate_context, now_ms};
use crate::realm::get_config::GetConfigOperation;
use crate::s3::bucket::get::GetBucketOperation;
use crate::s3::object::get::{GetObjectInput, GetObjectResult, get_object_info, get_object_routed};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use aruna_core::NodeId;
use aruna_core::git::{GitRepository, LFS_OBJECTS, LfsObject, LfsVersion};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use bytes::Bytes;
use ulid::Ulid;

fn key(id: Ulid, oid: &str) -> String {
    format!("git-lfs/{id}/{oid}")
}

fn record_key(id: Ulid, oid: &str) -> Vec<u8> {
    [id.to_bytes().as_slice(), oid.as_bytes()].concat()
}

async fn access(
    context: &DriverContext,
    auth: &AuthContext,
    node: NodeId,
    id: Ulid,
    oid: &str,
    permission: Permission,
) -> Result<GitRepository, GitError> {
    if !(LfsObject {
        oid: oid.to_string(),
        size: 0,
    })
    .valid()
    {
        return Err(GitError::Invalid);
    }
    let repository = repository(context, auth, id, permission.clone()).await?;
    authorize(
        context,
        auth.realm_id,
        auth,
        &object_permission_path(
            auth.realm_id,
            repository.group_id,
            node,
            &repository.bucket,
            &key(id, oid),
        ),
        &permission,
        PolicyRequestExtras::rest(),
    )
    .await?;
    Ok(repository)
}

fn input(
    repository: &GitRepository,
    auth: &AuthContext,
    node: NodeId,
    version: &LfsVersion,
) -> GetObjectInput {
    GetObjectInput {
        bucket: repository.bucket.clone(),
        key: key(repository.document_id, &version.object.oid),
        version_id: Some(version.version_id),
        range: None,
        group_id: repository.group_id,
        user_identity: auth.user_id,
        node_id: node,
    }
}

pub async fn inspect(
    context: &DriverContext,
    auth: &AuthContext,
    node: NodeId,
    id: Ulid,
    object: &LfsObject,
    permission: Permission,
) -> Result<Option<LfsVersion>, GitError> {
    let repository = access(context, auth, node, id, &object.oid, permission).await?;
    let record: Option<LfsVersion> =
        records::load(context, LFS_OBJECTS, record_key(id, &object.oid)).await?;
    if let Some(version) = &record {
        if version.object != *object {
            return Err(GitError::Invalid);
        }
        let info = get_object_info(
            context,
            input(&repository, auth, node, version),
            auth.path_restrictions.clone(),
        )
        .await
        .map_err(|_| GitError::Unavailable)?;
        if info.size != object.size
            || info.hashes.get("sha256").map(hex::encode).as_deref() != Some(&object.oid)
        {
            return Err(GitError::Unavailable);
        }
    }
    Ok(record)
}

pub async fn upload(
    context: &DriverContext,
    auth: &AuthContext,
    node: NodeId,
    id: Ulid,
    object: LfsObject,
    body: BackendStream<Result<Bytes, StreamError>>,
) -> Result<(), GitError> {
    let repository = access(context, auth, node, id, &object.oid, Permission::WRITE).await?;
    if inspect(context, auth, node, id, &object, Permission::WRITE)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let bucket = drive(GetBucketOperation::new(repository.bucket.clone()), context)
        .await
        .map_err(|_| GitError::Unavailable)?;
    if bucket.group_id != repository.group_id {
        return Err(GitError::Conflict);
    }
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
        .effective_group_ceiling(&repository.group_id);
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id: repository.group_id,
        realm_id: auth.realm_id,
        node_id: node,
        request: PutObjectInput {
            bucket: repository.bucket.clone(),
            key: key(id, &object.oid),
            content_length: Some(object.size),
            body: Some(body),
        },
        expected_checksums: vec![ExpectedChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            digest: hex::decode(&object.oid).map_err(|_| GitError::Invalid)?,
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
    let version = LfsVersion {
        object: object.clone(),
        version_id: result.version_id,
    };
    let _: LfsVersion =
        records::insert(context, LFS_OBJECTS, record_key(id, &object.oid), &version).await?;
    Ok(())
}

pub async fn download(
    context: &DriverContext,
    auth: &AuthContext,
    node: NodeId,
    id: Ulid,
    oid: &str,
) -> Result<GetObjectResult, GitError> {
    let repository = access(context, auth, node, id, oid, Permission::READ).await?;
    let version: LfsVersion = records::load(context, LFS_OBJECTS, record_key(id, oid))
        .await?
        .ok_or(GitError::NotFound)?;
    if version.object.oid != oid {
        return Err(GitError::Unavailable);
    }
    get_object_routed(
        context,
        input(&repository, auth, node, &version),
        auth.path_restrictions.clone(),
    )
    .await
    .map_err(|_| GitError::Unavailable)
}
