//! Requests and serves object bytes and metadata between nodes over the native source ALPN.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::native::{NATIVE_IO_TIMEOUT, read_frame, send_request, write_frame};
use aruna_core::NodeId;
use aruna_core::errors::StagingSourceError;
use aruna_core::native_reference::{
    NativeAuthority, NativeRange, NativeReferenceReject, NativeReferenceRequest,
    NativeReferenceResponse,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::distribution::delegation::SignedDelegation;
use aruna_core::structs::storage::read_grant::{
    self, GrantScope, GrantedRequest, HEAD_OPERATION, MAX_REQUEST_SECS, READ_OPERATION,
    SignedGrant, SignedRequest,
};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, SendStream};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tracing::warn;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::nonce::SpendNonceOperation;
use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::realm::load_config::config_unavailable;
use crate::realm::peer_trust::is_sync_eligible;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::sync::mirror_repair::{KickRepairOperation, StoreStatusOperation};
use crate::sync::sync_relationship::{
    GetRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};

struct PreparedReference {
    metadata: SourceMetadata,
    group_id: Option<aruna_core::types::GroupId>,
    body: Option<BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>>,
}

/// Seconds a granted request this node signs stays valid.
const GRANTED_SECS: u64 = 15;

/// One exact version read from `holder` for the caller `grant` names. The holder checks the
/// grant it issued, this node's signature and the caller's permissions, then serves the range.
pub(crate) async fn granted_read(
    context: &DriverContext,
    holder: NodeId,
    grant: &SignedGrant,
    range: Option<std::ops::Range<u64>>,
    head: bool,
) -> Result<(SourceMetadata, BiStream), StagingSourceError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or(StagingSourceError::HandleMissing)?;
    let GrantScope::Version(arn) = &grant.grant.scope else {
        return Err(StagingSourceError::AccessDenied);
    };
    let range = NativeRange::checked(range)?;
    let operation = if head { HEAD_OPERATION } else { READ_OPERATION };
    let scope = &grant.grant.scope;
    let digest = read_grant::request_digest(
        operation,
        scope,
        range.map(|range| (range.start, range.end)),
    )
    .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let request = GrantedRequest {
        realm_id: *net.realm_id(),
        issuer: net.node_id(),
        target: holder,
        operation: operation.to_string(),
        request_digest: digest,
        nonce: rand::random(),
        issued_at: aruna_core::time::unix_timestamp_millis() / 1000,
        valid_secs: GRANTED_SECS,
        grant: grant.clone(),
    };
    let bytes = request
        .signing_bytes()
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let signed = SignedRequest {
        signature: net.sign(&bytes),
        request,
    };
    let request = NativeReferenceRequest {
        authority: NativeAuthority::Granted(Box::new(signed)),
        bucket: arn.bucket.clone(),
        key: arn.key.clone(),
        version_id: arn.version,
        head,
        range,
    };
    let (response, stream) = send_request(net, holder, request).await?;
    match response {
        NativeReferenceResponse::Content(metadata)
        | NativeReferenceResponse::Described { metadata, .. } => Ok((metadata, stream)),
        NativeReferenceResponse::Rejected(reject) => Err(reject.into()),
    }
}

/// One exact version read from `holder` for a caller this node verified itself, bound by a
/// D33 delegation to this operation, version and range.
pub(crate) async fn delegated_read(
    context: &DriverContext,
    caller: crate::distribution::delegate::DelegationInput,
    arn: &VersionedObjectArn,
    range: Option<std::ops::Range<u64>>,
    head: bool,
) -> Result<(SourceMetadata, Option<aruna_core::types::GroupId>, BiStream), StagingSourceError> {
    let range = NativeRange::checked(range)?;
    let operation = if head { HEAD_OPERATION } else { READ_OPERATION };
    let scope = GrantScope::Version(arn.clone());
    let digest = read_grant::request_digest(
        operation,
        &scope,
        range.map(|range| (range.start, range.end)),
    )
    .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let input = crate::distribution::delegate::DelegationInput {
        target: arn.node_id,
        operation: operation.to_string(),
        bucket: arn.bucket.clone(),
        key: arn.key.clone(),
        request_digest: digest,
        ..caller
    };
    let now = aruna_core::time::unix_timestamp_millis() / 1000;
    let signed = crate::distribution::delegate::issue_delegation(context, input, now)
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let request = NativeReferenceRequest {
        authority: NativeAuthority::Delegated(Box::new(signed)),
        bucket: arn.bucket.clone(),
        key: arn.key.clone(),
        version_id: arn.version,
        head,
        range,
    };
    let net = context
        .net_handle
        .as_ref()
        .ok_or(StagingSourceError::HandleMissing)?;
    let (response, stream) = send_request(net, arn.node_id, request).await?;
    match response {
        NativeReferenceResponse::Content(metadata) => Ok((metadata, None, stream)),
        NativeReferenceResponse::Described { metadata, group_id } => {
            Ok((metadata, Some(group_id), stream))
        }
        NativeReferenceResponse::Rejected(reject) => Err(reject.into()),
    }
}

pub(crate) async fn handle_native_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let request = match timeout(
        NATIVE_IO_TIMEOUT,
        read_frame::<NativeReferenceRequest>(&mut stream.1),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            warn!(%peer, %error, "Failed to read native reference request");
            return;
        }
        Err(_) => {
            warn!(%peer, "Timed out reading native reference request");
            return;
        }
    };

    match prepare_reference(context, peer, &request).await {
        Ok(mut prepared) => {
            // Only a caller read names the group; a sync relationship knows its own bucket.
            let response = match (&request.authority, prepared.group_id) {
                (NativeAuthority::Relationship(_), _) | (_, None) => {
                    NativeReferenceResponse::Content(prepared.metadata)
                }
                (_, Some(group_id)) => NativeReferenceResponse::Described {
                    metadata: prepared.metadata,
                    group_id,
                },
            };
            if write_response(&mut stream.0, &response).await.is_err() {
                return;
            }
            if let Some(body) = prepared.body.as_mut() {
                while let Some(chunk) = body.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            warn!(%peer, %error, "Failed to read native reference source");
                            stream.0.reset(1u32.into()).ok();
                            return;
                        }
                    };
                    if !matches!(
                        timeout(NATIVE_IO_TIMEOUT, stream.0.write_all(&chunk)).await,
                        Ok(Ok(()))
                    ) {
                        return;
                    }
                }
            }
        }
        Err(reject) => {
            if write_response(&mut stream.0, &NativeReferenceResponse::Rejected(reject))
                .await
                .is_err()
            {
                return;
            }
        }
    }
    stream.0.finish().ok();
}

async fn write_response(
    send: &mut SendStream,
    response: &NativeReferenceResponse,
) -> Result<(), String> {
    timeout(NATIVE_IO_TIMEOUT, write_frame(send, response))
        .await
        .map_err(|_| "native response timed out".to_string())?
}

async fn prepare_reference(
    context: &DriverContext,
    peer: NodeId,
    request: &NativeReferenceRequest,
) -> Result<PreparedReference, NativeReferenceReject> {
    let net_handle = context.net_handle.as_ref().ok_or_else(|| {
        NativeReferenceReject::Unavailable("network handle unavailable".to_string())
    })?;
    let relationship_id = match &request.authority {
        NativeAuthority::Relationship(relationship_id) => *relationship_id,
        NativeAuthority::Granted(signed) => {
            return Box::pin(prepare_granted(context, peer, request, signed)).await;
        }
        NativeAuthority::Delegated(signed) => {
            return Box::pin(prepare_delegated(context, peer, request, signed)).await;
        }
    };
    let relationship = drive(
        GetRelationshipOperation::new(relationship_id, SyncRelationshipDirection::Outgoing),
        context,
    )
    .await
    .map_err(map_relationship_error)?;
    validate_relationship(net_handle, peer, &relationship, request)?;
    let auth_context = AuthContext {
        user_id: relationship.created_by,
        realm_id: relationship.source.realm_id,
        path_restrictions: None,
        session: None,
    };
    let read = read_exact(context, &auth_context, relationship.source.node_id, request).await;
    if matches!(read, Err(NativeReferenceReject::AccessDenied)) {
        mark_access_denied(context, relationship).await;
    }
    read
}

/// A read a grant of this node covers: the reading node signed it, is realm infrastructure,
/// sent it once, and asks exactly the version and range the grant and request name.
async fn prepare_granted(
    context: &DriverContext,
    peer: NodeId,
    request: &NativeReferenceRequest,
    signed: &SignedRequest,
) -> Result<PreparedReference, NativeReferenceReject> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        NativeReferenceReject::Unavailable("network handle unavailable".to_string())
    })?;
    let now = aruna_core::time::unix_timestamp_millis() / 1000;
    let granted = signed
        .verify(*net.realm_id(), net.node_id(), now)
        .map_err(|_| NativeReferenceReject::AccessDenied)?;
    let config = drive(GetConfigOperation::new(*net.realm_id()), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or_else(|| {
            NativeReferenceReject::Unavailable("realm config unavailable".to_string())
        })?;
    if granted.issuer != peer || !is_sync_eligible(&config, peer) {
        return Err(NativeReferenceReject::AccessDenied);
    }
    let grant = &granted.grant.grant;
    let scope = GrantScope::Version(VersionedObjectArn {
        realm_id: *net.realm_id(),
        node_id: net.node_id(),
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        version: request.version_id,
    });
    let operation = if request.head {
        HEAD_OPERATION
    } else {
        READ_OPERATION
    };
    let range = request.range.map(|range| (range.start, range.end));
    let digest = read_grant::request_digest(operation, &scope, range)
        .map_err(|_| NativeReferenceReject::AccessDenied)?;
    if grant.scope != scope || granted.operation != operation || granted.request_digest != digest {
        return Err(NativeReferenceReject::AccessDenied);
    }
    let expires = granted.issued_at.saturating_add(2 * MAX_REQUEST_SECS);
    drive(
        SpendNonceOperation::new(granted.nonce, expires, now),
        context,
    )
    .await
    .map_err(|_| NativeReferenceReject::AccessDenied)?;
    let auth_context = AuthContext {
        user_id: grant.user_id,
        realm_id: grant.realm_id,
        path_restrictions: grant.restrictions.clone(),
        session: None,
    };
    refuse_governed(context, request).await?;
    read_exact(context, &auth_context, net.node_id(), request).await
}

/// A read the reading node vouches for in one D33 delegation: signed by a realm
/// infrastructure node, unused, and naming this exact version, operation and range.
async fn prepare_delegated(
    context: &DriverContext,
    peer: NodeId,
    request: &NativeReferenceRequest,
    signed: &SignedDelegation,
) -> Result<PreparedReference, NativeReferenceReject> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        NativeReferenceReject::Unavailable("network handle unavailable".to_string())
    })?;
    let now = aruna_core::time::unix_timestamp_millis() / 1000;
    let delegation = signed
        .verify(*net.realm_id(), net.node_id(), now)
        .map_err(|_| NativeReferenceReject::AccessDenied)?;
    let config = drive(GetConfigOperation::new(*net.realm_id()), context)
        .await
        .inspect_err(config_unavailable)
        .ok()
        .ok_or_else(|| {
            NativeReferenceReject::Unavailable("realm config unavailable".to_string())
        })?;
    if delegation.issuer != peer || !is_sync_eligible(&config, peer) {
        return Err(NativeReferenceReject::AccessDenied);
    }
    let scope = GrantScope::Version(VersionedObjectArn {
        realm_id: *net.realm_id(),
        node_id: net.node_id(),
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        version: request.version_id,
    });
    let operation = if request.head {
        HEAD_OPERATION
    } else {
        READ_OPERATION
    };
    let range = request.range.map(|range| (range.start, range.end));
    let digest = read_grant::request_digest(operation, &scope, range)
        .map_err(|_| NativeReferenceReject::AccessDenied)?;
    if delegation.operation != operation
        || delegation.bucket != request.bucket
        || delegation.key != request.key
        || delegation.request_digest != digest
    {
        return Err(NativeReferenceReject::AccessDenied);
    }
    let expires = delegation.issued_at.saturating_add(2 * MAX_REQUEST_SECS);
    drive(
        SpendNonceOperation::new(delegation.nonce, expires, now),
        context,
    )
    .await
    .map_err(|_| NativeReferenceReject::AccessDenied)?;
    let auth_context = AuthContext {
        user_id: delegation.user_id,
        realm_id: delegation.realm_id,
        path_restrictions: delegation.restrictions.clone(),
        session: None,
    };
    refuse_governed(context, request).await?;
    read_exact(context, &auth_context, net.node_id(), request).await
}

/// Governed content answers only the managed read's destination challenge.
async fn refuse_governed(
    context: &DriverContext,
    request: &NativeReferenceRequest,
) -> Result<(), NativeReferenceReject> {
    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            version_id: Some(request.version_id),
        }),
        context,
    )
    .await
    .map_err(map_head_error)?;
    match head.source_policies.is_empty() {
        true => Ok(()),
        false => Err(NativeReferenceReject::AccessDenied),
    }
}

/// Authorizes `auth_context` like an S3 read of the exact version, then reads it.
async fn read_exact(
    context: &DriverContext,
    auth_context: &AuthContext,
    node_id: NodeId,
    request: &NativeReferenceRequest,
) -> Result<PreparedReference, NativeReferenceReject> {
    let bucket_info = match drive(GetBucketOperation::new(request.bucket.clone()), context).await {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => {
            return Err(NativeReferenceReject::NotFound);
        }
        Err(error) => {
            return Err(NativeReferenceReject::Unavailable(error.to_string()));
        }
    };
    let path = object_permission_path(
        auth_context.realm_id,
        bucket_info.group_id,
        node_id,
        &request.bucket,
        &request.key,
    );
    if let Err(error) = drive(
        AuthorizeOperation::new(
            auth_context.realm_id,
            auth_context,
            &path,
            &Permission::READ,
            PolicyRequestExtras::operation(if request.head {
                HEAD_OPERATION
            } else {
                READ_OPERATION
            }),
        ),
        context,
    )
    .await
    {
        match error {
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => {
                return Err(NativeReferenceReject::AccessDenied);
            }
            AuthorizeError::Policy(PolicyEnforcementError::Unavailable(error)) => {
                return Err(NativeReferenceReject::Unavailable(error));
            }
            AuthorizeError::CheckFailed(error) => {
                return Err(NativeReferenceReject::Unavailable(error));
            }
            AuthorizeError::Storage(error) => {
                return Err(NativeReferenceReject::Unavailable(error.to_string()));
            }
        }
    }

    if request.head {
        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                version_id: Some(request.version_id),
            }),
            context,
        )
        .await
        .map_err(map_head_error)?;
        let metadata = head_metadata(&result, request.version_id)?;
        return Ok(PreparedReference {
            metadata,
            group_id: Some(bucket_info.group_id),
            body: None,
        });
    }

    let range = request.range.map(|range| ObjectRangeRequest::StartEnd {
        start: range.start,
        end: range.end.saturating_sub(1),
    });
    let result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            version_id: Some(request.version_id),
            range,
            group_id: bucket_info.group_id,
            user_identity: auth_context.user_id,
            realm_id: auth_context.realm_id,
            node_id,
        }),
        context,
    )
    .await
    .map_err(map_get_error)?;
    let metadata = result_metadata(&result, request.version_id)?;
    Ok(PreparedReference {
        metadata,
        group_id: Some(bucket_info.group_id),
        body: Some(result.blob),
    })
}

fn validate_relationship(
    net_handle: &NetHandle,
    peer: NodeId,
    relationship: &SyncRelationship,
    request: &NativeReferenceRequest,
) -> Result<(), NativeReferenceReject> {
    let valid_prefix = relationship
        .source
        .key_prefix()
        .is_none_or(|prefix| request.key.starts_with(prefix));
    // Detached stubs keep serving retained references after relationship deletion.
    // Other disabled states refuse access.
    if !relationship.serves_references()
        || !matches!(relationship.state, SyncState::Enabled | SyncState::Detached)
        || relationship.source.realm_id != *net_handle.realm_id()
        || relationship.source.node_id != net_handle.node_id()
        || relationship.target.node_id != peer
        || relationship.source.bucket() != Some(request.bucket.as_str())
        || !valid_prefix
    {
        return Err(NativeReferenceReject::AccessDenied);
    }
    Ok(())
}

fn map_relationship_error(error: SyncRelationshipError) -> NativeReferenceReject {
    match error {
        SyncRelationshipError::NotFound => NativeReferenceReject::AccessDenied,
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn map_get_error(error: GetObjectError) -> NativeReferenceReject {
    match error {
        GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker
        // The pinned observation no longer has bytes to relay.
        // Drift and exhausted bindings remain retryable through rebinding.
        | GetObjectError::HistoricalReferenceUnavailable
        | GetObjectError::ReferenceChecksumMismatch => NativeReferenceReject::NotFound,
        GetObjectError::ReferenceSourceChanged | GetObjectError::ReferenceAdvanceExhausted => {
            NativeReferenceReject::Unavailable(error.to_string())
        }
        GetObjectError::StagingSourceError(StagingSourceError::NotFound) => {
            NativeReferenceReject::NotFound
        }
        GetObjectError::StagingSourceError(StagingSourceError::AccessDenied) => {
            NativeReferenceReject::AccessDenied
        }
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn map_head_error(error: HeadObjectError) -> NativeReferenceReject {
    match error {
        HeadObjectError::NoSuchKey
        | HeadObjectError::NoSuchVersion
        | HeadObjectError::DeleteMarker => NativeReferenceReject::NotFound,
        HeadObjectError::StagingSourceError(StagingSourceError::NotFound) => {
            NativeReferenceReject::NotFound
        }
        HeadObjectError::StagingSourceError(StagingSourceError::AccessDenied) => {
            NativeReferenceReject::AccessDenied
        }
        error => NativeReferenceReject::Unavailable(error.to_string()),
    }
}

fn result_metadata(
    result: &GetObjectResult,
    version_id: Ulid,
) -> Result<SourceMetadata, NativeReferenceReject> {
    let mut metadata = if let Some(metadata) = result.source_metadata.clone() {
        metadata
    } else if let Some(location) = result.location.as_ref() {
        SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: result.info.version_created_at,
            source_version: None,
            digests: Default::default(),
        }
    } else {
        return Err(NativeReferenceReject::Unavailable(
            "source metadata unavailable".to_string(),
        ));
    };
    metadata.source_version = Some(version_id.to_string());
    Ok(metadata)
}

fn head_metadata(
    result: &HeadObjectResult,
    version_id: Ulid,
) -> Result<SourceMetadata, NativeReferenceReject> {
    let mut metadata = if let Some(metadata) = result.source_metadata.clone() {
        metadata
    } else if let Some(location) = result.location.as_ref() {
        SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: result.version_created_at,
            source_version: None,
            digests: Default::default(),
        }
    } else {
        return Err(NativeReferenceReject::Unavailable(
            "source metadata unavailable".to_string(),
        ));
    };
    metadata.source_version = Some(version_id.to_string());
    Ok(metadata)
}

async fn mark_access_denied(context: &DriverContext, mut relationship: SyncRelationship) {
    if relationship.state == SyncState::Detached {
        // Detached stubs have no visible status entry. Marking them failed would stop retained
        // reads, so only the request reports denial.
        return;
    }
    relationship.state = SyncState::Failed {
        reason: "access_denied".to_string(),
    };
    relationship.status.last_error = Some("access_denied".to_string());
    relationship.status.counters.failures = relationship.status.counters.failures.saturating_add(1);
    relationship.status.counters.consecutive_failures = relationship
        .status
        .counters
        .consecutive_failures
        .saturating_add(1);
    match drive(StoreStatusOperation::new(relationship.clone()), context).await {
        Ok(true) => _ = drive(KickRepairOperation::new(), context).await,
        Ok(false) => {}
        Err(error) => warn!(%error, "Failed to persist native reference denial"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{ReferenceHandling, SyncMode, SyncStatusSnapshot};
    use aruna_net::NetConfig;
    use aruna_storage::storage::FjallStorage;

    // The relaying peer must learn that a pinned observation is gone (NotFound)
    // rather than treating it as a transient origin failure.
    #[test]
    fn maps_reference_rejects() {
        assert_eq!(
            map_get_error(GetObjectError::HistoricalReferenceUnavailable),
            NativeReferenceReject::NotFound
        );
        for error in [
            GetObjectError::ReferenceSourceChanged,
            GetObjectError::ReferenceAdvanceExhausted,
        ] {
            assert!(matches!(
                map_get_error(error),
                NativeReferenceReject::Unavailable(_)
            ));
        }
    }

    #[tokio::test]
    async fn denial_journals_failure() {
        // A native read whose creator lost READ fails the relationship and journals its
        // `sync_failed` with that status write, with no replication job queued.
        use crate::sync::sync_relationship::{
            StoreRelationshipOperation, SyncRelationshipDirection,
        };
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::format::Compression;
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        let source = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let target = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let creator = aruna_core::UserId::local(Ulid::from_parts(5, 5), realm_id);
        let relationship = SyncRelationship {
            id: Ulid::from_parts(2, 2),
            source: ArunaArn::s3_bucket(realm_id, source, "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, target, "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: ReferenceHandling::Preserve,
            reference_serving: true,
            replicate_deletes: true,
            created_by: creator,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        let direction = SyncRelationshipDirection::Outgoing;
        let store = StoreRelationshipOperation::new(relationship.clone(), direction);
        drive(store, &context).await.unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_parts(6, 6),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let write = aruna_core::effects::StorageEffect::Write {
            key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
            key: b"source".to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;

        mark_access_denied(&context, relationship.clone()).await;

        let pending = aruna_core::plugin_runs::PENDING_ROW;
        let page = crate::plugins::journal::journal_page(pending, None, 10);
        let (rows, _) = crate::driver::drive(page, &context).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.resource, relationship.id.to_string());
        let transition = format!("sync_failed/{source}/{}/", relationship.id);
        assert!(rows[0].1.source.starts_with(&transition));
    }

    #[tokio::test]
    async fn preserve_allows_native() {
        let tempdir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(NetConfig::default(), storage).await.unwrap();
        let peer = iroh::SecretKey::generate().public();
        let realm_id = *net.realm_id();
        let mut relationship = SyncRelationship {
            id: Ulid::generate(),
            source: ArunaArn::s3_bucket(realm_id, net.node_id(), "source").unwrap(),
            target: ArunaArn::s3_bucket(realm_id, peer, "target").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: ReferenceHandling::Materialize,
            reference_serving: false,
            replicate_deletes: true,
            created_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        let request = NativeReferenceRequest {
            authority: NativeAuthority::Relationship(relationship.id),
            bucket: "source".to_string(),
            key: "data.txt".to_string(),
            version_id: Ulid::generate(),
            head: true,
            range: None,
        };

        assert_eq!(
            validate_relationship(&net, peer, &relationship, &request),
            Err(NativeReferenceReject::AccessDenied)
        );
        relationship.set_reference_handling(ReferenceHandling::Preserve);
        assert!(validate_relationship(&net, peer, &relationship, &request).is_ok());
        relationship.set_reference_handling(ReferenceHandling::Materialize);
        assert!(validate_relationship(&net, peer, &relationship, &request).is_ok());
    }
}
