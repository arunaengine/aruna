//! Follows links for reads: authorizes the target for the caller, like an S3 read of it, and
//! reads its exact version here or, through a delegation, at its node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{CONTENT_TYPE_KEY, UserAccess, object_permission_path};
use aruna_core::structs::storage::distribution::delegation::Payload;
use aruna_core::structs::storage::link::LinkTarget;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::distribution::delegate::DelegationInput;
use crate::driver::{DriverContext, drive};
use crate::jobs::source_read::{HEAD_OPERATION, READ_OPERATION, SourceReadError, authorize_read};
use crate::jobs::workflow::input_view::ViewStream;
use crate::s3::object::get::{GetObjectInput, GetObjectOperation, ObjectRangeRequest};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use crate::staging::native_source::{body_stream, delegated_read};
use crate::staging::reference::{ReferenceWrite, write_reference_version};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LinkError {
    #[error("the link target is denied")]
    Denied,
    #[error("the link target is not found")]
    NotFound,
    #[error("the link target is unavailable: {0}")]
    Unavailable(String),
    #[error("the link request is invalid: {0}")]
    Invalid(String),
}

/// Who reads through a link, as the node that verified the request's credential saw them.
#[derive(Clone, Debug)]
pub struct LinkCaller {
    pub auth: AuthContext,
    /// The verified credential's id, group and expiry in Unix seconds, for delegations.
    pub credential: (String, GroupId, u64),
}

impl LinkCaller {
    /// The caller of an S3 request signed with `access`.
    pub fn of_access(auth: AuthContext, access: &UserAccess) -> Self {
        let expiry = access
            .expiry
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        Self {
            auth,
            credential: (access.access_key.clone(), access.group_id, expiry),
        }
    }
}

/// Whether a listing may show a link: the caller may read its target.
pub async fn target_visible(
    context: &DriverContext,
    caller: &LinkCaller,
    target: &LinkTarget,
    local: NodeId,
) -> Result<bool, LinkError> {
    match head_target(context, caller, target, local).await {
        Ok(()) => Ok(true),
        Err(LinkError::Denied | LinkError::NotFound) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether the caller may HEAD the target: checked here and, for a target on another node,
/// by that node in one delegated HEAD.
pub async fn head_target(
    context: &DriverContext,
    caller: &LinkCaller,
    target: &LinkTarget,
    local: NodeId,
) -> Result<(), LinkError> {
    authorize_target(context, &caller.auth, target, HEAD_OPERATION).await?;
    let arn = &target.arn;
    if arn.node_id == local {
        return Ok(());
    }
    delegated_read(context, delegation(caller, arn.node_id), arn, None, true)
        .await
        .map(|_| ())
        .map_err(staging_error)
}

fn staging_error(error: aruna_core::errors::StagingSourceError) -> LinkError {
    match error {
        aruna_core::errors::StagingSourceError::AccessDenied => LinkError::Denied,
        aruna_core::errors::StagingSourceError::NotFound => LinkError::NotFound,
        other => LinkError::Unavailable(other.to_string()),
    }
}

/// Whether the caller may read the target, evaluated here from the target's recorded group
/// and the replicated roles and policies; the target's node checks again on every byte read.
pub async fn authorize_target(
    context: &DriverContext,
    auth: &AuthContext,
    target: &LinkTarget,
    operation: &str,
) -> Result<(), LinkError> {
    let arn = &target.arn;
    let path = object_permission_path(
        auth.realm_id,
        target.group_id,
        arn.node_id,
        &arn.bucket,
        &arn.key,
    );
    let extras = PolicyRequestExtras::operation(operation);
    match Box::pin(authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &Permission::READ,
        extras,
    ))
    .await
    {
        Ok(()) => Ok(()),
        Err(
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
        ) => Err(LinkError::Denied),
        Err(error) => Err(LinkError::Unavailable(error.to_string())),
    }
}

/// The target's bytes, or `range` of them, at its exact version. A target on another node is
/// read through one delegation that carries the caller this node verified.
pub async fn read_target(
    context: &DriverContext,
    caller: &LinkCaller,
    target: &LinkTarget,
    (local, range): (NodeId, Option<Range<u64>>),
) -> Result<ViewStream, LinkError> {
    authorize_target(context, &caller.auth, target, READ_OPERATION).await?;
    let arn = &target.arn;
    if arn.node_id == local {
        let get = Box::pin(drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: arn.bucket.clone(),
                key: arn.key.clone(),
                version_id: Some(arn.version),
                range: range.map(|range| ObjectRangeRequest::StartEnd {
                    start: range.start,
                    end: range.end.saturating_sub(1),
                }),
                group_id: target.group_id,
                user_identity: caller.auth.user_id,
                realm_id: caller.auth.realm_id,
                node_id: local,
            }),
            context,
        ))
        .await
        .map_err(|error| LinkError::Unavailable(error.to_string()))?;
        return Ok(get.blob);
    }
    let (_, _, stream) =
        delegated_read(context, delegation(caller, arn.node_id), arn, range, false)
            .await
            .map_err(staging_error)?;
    Ok(body_stream(stream))
}

/// The verified caller a delegation carries; the read fills in what it binds.
fn delegation(caller: &LinkCaller, target: NodeId) -> DelegationInput {
    let (credential_id, group_id, expiry) = caller.credential.clone();
    DelegationInput {
        set_id: Ulid::nil(),
        range_id: Ulid::nil(),
        target,
        user_id: caller.auth.user_id,
        group_id,
        credential_id,
        restrictions: caller.auth.path_restrictions.clone(),
        credential_expiry: expiry,
        operation: String::new(),
        bucket: String::new(),
        key: String::new(),
        copy_source: None,
        request_digest: [0; 32],
        payload: Payload::Empty,
    }
}

/// Where a link should point: a node, a bucket and key there, and maybe an exact version.
pub struct LinkSource {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version: Option<Ulid>,
}

/// Writes a link at `dest` (bucket, key, its group) to the source's exact version, for a
/// caller who may read that version now. A source on another node must name its version.
pub async fn create_link(
    context: &DriverContext,
    caller: &LinkCaller,
    (bucket, key, group_id): (&str, &str, GroupId),
    (local, source): (NodeId, LinkSource),
) -> Result<Ulid, LinkError> {
    let (target, metadata) = match source.node_id == local {
        true => local_target(context, caller, &source).await?,
        false => remote_target(context, caller, &source).await?,
    };
    let write = ReferenceWrite {
        preassigned_version_id: None,
        group_id,
        user_id: caller.auth.user_id,
        realm_id: caller.auth.realm_id,
        node_id: local,
        bucket: bucket.to_string(),
        key: key.to_string(),
        expected_bucket: None,
        version_source: target.binding(),
        metadata,
        inherited_policies: Vec::new(),
        connector_guard: None,
        restrictions: caller.auth.path_restrictions.clone(),
        mirror_guards: Vec::new(),
    };
    let (version, _) = Box::pin(write_reference_version(context, write))
        .await
        .map_err(|error| LinkError::Unavailable(error.to_string()))?;
    Ok(version)
}

/// A source on this node: its bucket group, the caller's right to it, and its exact version.
async fn local_target(
    context: &DriverContext,
    caller: &LinkCaller,
    source: &LinkSource,
) -> Result<(LinkTarget, SourceMetadata), LinkError> {
    let object = (source.bucket.as_str(), source.key.as_str());
    let group_id = authorize_read(
        context,
        &caller.auth,
        source.node_id,
        object,
        HEAD_OPERATION,
    )
    .await
    .map_err(|error| match error {
        SourceReadError::NotFound => LinkError::NotFound,
        SourceReadError::Denied => LinkError::Denied,
        SourceReadError::Unavailable(error) => LinkError::Unavailable(error),
    })?;
    let head = Box::pin(drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            version_id: source.version,
        }),
        context,
    ))
    .await
    .map_err(|error| match error {
        HeadObjectError::NoSuchKey
        | HeadObjectError::NoSuchVersion
        | HeadObjectError::DeleteMarker => LinkError::NotFound,
        other => LinkError::Unavailable(other.to_string()),
    })?;
    let version = head
        .resolved_version_id
        .or(head.version_id)
        .ok_or(LinkError::NotFound)?;
    let metadata = SourceMetadata {
        content_length: head
            .location
            .as_ref()
            .map(|location| location.blob_size)
            .or_else(|| {
                head.source_metadata
                    .as_ref()
                    .map(|source| source.content_length)
            })
            .unwrap_or_default(),
        content_type: head.metadata.get(CONTENT_TYPE_KEY).cloned().or_else(|| {
            head.source_metadata
                .as_ref()
                .and_then(|source| source.content_type.clone())
        }),
        etag: Some(version.to_string()),
        last_modified: head.version_created_at,
        source_version: None,
        digests: Default::default(),
    };
    let target = LinkTarget {
        arn: VersionedObjectArn {
            realm_id: caller.auth.realm_id,
            node_id: source.node_id,
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            version,
        },
        group_id,
    };
    Ok((target, metadata))
}

/// A source on another node, described by that node for the caller in one delegated HEAD.
async fn remote_target(
    context: &DriverContext,
    caller: &LinkCaller,
    source: &LinkSource,
) -> Result<(LinkTarget, SourceMetadata), LinkError> {
    let version = source.version.ok_or_else(|| {
        LinkError::Invalid("a link to another node names the exact version".to_string())
    })?;
    let arn = VersionedObjectArn {
        realm_id: caller.auth.realm_id,
        node_id: source.node_id,
        bucket: source.bucket.clone(),
        key: source.key.clone(),
        version,
    };
    let (metadata, group_id, _) =
        delegated_read(context, delegation(caller, arn.node_id), &arn, None, true)
            .await
            .map_err(staging_error)?;
    let group_id = group_id.ok_or_else(|| {
        LinkError::Unavailable("the source node did not name its bucket group".to_string())
    })?;
    let metadata = SourceMetadata {
        etag: Some(version.to_string()),
        ..metadata
    };
    Ok((LinkTarget { arn, group_id }, metadata))
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::identity::realm::RealmId;

    use super::*;
    use crate::tests::access::seed_access;
    use crate::tests::s3::{test_context, test_storage};

    #[tokio::test]
    async fn head_asks_target() {
        // Local roles allow both targets, but one on another node shows only once that node
        // answers a delegated HEAD, which it cannot without a network here.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (realm_id, group_id) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(2, 2));
        let local = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let remote = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let auth = seed_access(&context, (realm_id, group_id, local), "/denied").await;
        let caller = LinkCaller {
            auth,
            credential: ("key".to_string(), group_id, u64::MAX),
        };
        let target = |node_id| LinkTarget {
            arn: VersionedObjectArn {
                realm_id,
                node_id,
                bucket: "db".to_string(),
                key: "bakta.db".to_string(),
                version: Ulid::from_parts(3, 3),
            },
            group_id,
        };
        let here = target_visible(&context, &caller, &target(local), local).await;
        assert_eq!(here, Ok(true));
        let there = target_visible(&context, &caller, &target(remote), local).await;
        assert!(matches!(there, Err(LinkError::Unavailable(_))), "{there:?}");
    }

    #[tokio::test]
    async fn remote_needs_version() {
        // A link to another node without a version is a bad request, not an unavailable target.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (realm_id, group_id) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(2, 2));
        let local = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let remote = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let auth = seed_access(&context, (realm_id, group_id, local), "/denied").await;
        let caller = LinkCaller {
            auth,
            credential: ("key".to_string(), group_id, u64::MAX),
        };
        let source = LinkSource {
            node_id: remote,
            bucket: "work3".to_string(),
            key: "n3.txt".to_string(),
            version: None,
        };
        let created = create_link(
            &context,
            &caller,
            ("work", "n3.txt", group_id),
            (local, source),
        );
        let error = created.await.unwrap_err();
        assert!(matches!(&error, LinkError::Invalid(_)), "{error:?}");
        assert!(error.to_string().contains("names the exact version"));
    }
}
