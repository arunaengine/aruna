//! Realm side of a device's synced folders.
//!
//! A device never pushes. It asks its realm node to pull one exact local
//! version; this node re-checks the owner binding, reads those bytes back from
//! the device and commits its own copy through the ordinary object write, so
//! head advance, usage, obligations and watch events all stay unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::effects::IterStart;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::structs::{
    AuthContext, BlobVersion, BlobVersionState, BucketInfo, Permission, RemoteHead,
    SYNC_SOURCE_VERSION_TAG, SyncListCursor, SyncPageLimit, SyncPullAck, SyncRefusal,
    SyncVersionPage, VersionKey, VersionedObjectArn, blob_object_permission_path,
};
use aruna_core::types::{GroupId, NodeId};
use tracing::debug;
use ulid::Ulid;

use crate::driver::{DriverContext, drive, routing_snapshot};
use crate::metadata::forward::peer_acts_for;
use crate::metadata::handle::MetadataWritePeerError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::process_placements::load_realm_config;
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::s3::delete_object::{DeleteObjectInput, DeleteObjectOperation};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};

/// Versions one idempotency scan reads at a time.
const VERSION_SCAN_PAGE: usize = 256;

/// Serves one forwarded sync pull.
pub async fn serve_sync_pull(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardSyncPull {
        auth_token,
        source,
        blake3,
        size,
        target_bucket,
        target_key,
        deleted,
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected metadata control message".to_string());
    };
    let result = apply_pull(
        context,
        peer,
        PullRequest {
            auth_token,
            source: *source,
            blake3,
            size,
            target_bucket,
            target_key,
            deleted,
        },
    )
    .await;
    MetadataTransportMessage::ForwardedSyncPull { result }
}

struct PullRequest {
    auth_token: aruna_core::metadata::MetadataAuthToken,
    source: VersionedObjectArn,
    blake3: Option<[u8; 32]>,
    size: u64,
    target_bucket: String,
    target_key: String,
    deleted: bool,
}

async fn apply_pull(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: PullRequest,
) -> Result<SyncPullAck, SyncRefusal> {
    let auth = authorize_peer(context, peer, request.auth_token.clone()).await?;
    // The device may only ask for a version it serves itself: a peer must not
    // make this node read another node's object under its own authority.
    if request.source.node_id != peer {
        return Err(SyncRefusal::Invalid(
            "a sync pull must name the requesting device".to_string(),
        ));
    }
    let node_id = local_node(context)?;
    let bucket = read_bucket(context, &request.target_bucket).await?;
    ensure_write(context, &auth, node_id, &bucket, &request).await?;

    // A replay of either kind answers with the version it already produced.
    if let Some(version_id) = applied_version(context, &request).await {
        return Ok(SyncPullAck {
            version_id,
            already_applied: true,
        });
    }
    match request.deleted {
        true => delete_target(context, &auth, node_id, &bucket, &request).await,
        false => commit_pull(context, &auth, node_id, &bucket, request).await,
    }
}

/// The owner the device forwards for. A User peer is owner-bound, so the
/// binding is re-checked here rather than trusted from the request.
///
/// Deliberate deviation: this node holds no record of which folder a device
/// bound, so a pull carries no binding proof. The owner's WRITE permission on
/// the target object is the whole authority, exactly as it is when the same
/// owner writes through S3. A device can therefore publish into any path its
/// owner may already write, and nothing more.
async fn authorize_peer(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: aruna_core::metadata::MetadataAuthToken,
) -> Result<AuthContext, SyncRefusal> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token))
        .await
        .map_err(|error| match error {
            MetadataWritePeerError::Unauthorized => SyncRefusal::Unauthorized,
            MetadataWritePeerError::Unavailable(_) => SyncRefusal::Unavailable,
        })?;
    let config = load_realm_config(context, auth.realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    match peer_acts_for(&config, peer, auth.user_id) {
        true => Ok(auth),
        false => Err(SyncRefusal::Unauthorized),
    }
}

/// Reads the target bucket. Only a bucket that is really absent is `NotFound`:
/// the drain parks that permanently, so a storage or decoding failure has to
/// stay retryable instead.
async fn read_bucket(
    context: &Arc<DriverContext>,
    bucket: &str,
) -> Result<BucketInfo, SyncRefusal> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), context).await {
        Ok(Some(Ok(info))) => Ok(info),
        Ok(None) => Err(SyncRefusal::NotFound),
        Ok(Some(Err(error))) | Err(error) => {
            debug!(error = %error, bucket = %bucket, "A sync pull could not read its target bucket");
            Err(bucket_refusal(error))
        }
    }
}

/// A bucket that is not there is permanent; everything else is this node's
/// problem and must not park the device's upload.
fn bucket_refusal(error: GetBucketInfoError) -> SyncRefusal {
    match error {
        GetBucketInfoError::NotFound => SyncRefusal::NotFound,
        _ => SyncRefusal::Unavailable,
    }
}

/// This node's own id. A pull cannot be attributed without it.
fn local_node(context: &Arc<DriverContext>) -> Result<NodeId, SyncRefusal> {
    context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(SyncRefusal::Unavailable)
}

async fn ensure_write(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    node_id: NodeId,
    bucket: &BucketInfo,
    request: &PullRequest,
) -> Result<(), SyncRefusal> {
    let operation = match request.deleted {
        true => "s3.DeleteObject",
        false => "s3.PutObject",
    };
    authorize_pull(
        context,
        auth,
        blob_object_permission_path(
            auth.realm_id,
            bucket.group_id,
            node_id,
            &request.target_bucket,
            &request.target_key,
        ),
        Permission::WRITE,
        operation,
    )
    .await
}

/// The same boundary the equivalent S3 request passes: RBAC first, then the
/// realm and group request policies. A pull is an ordinary write by the owner,
/// so a policy that denies their put or delete denies this too.
async fn authorize_pull(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    path: String,
    permission: Permission,
    operation: &'static str,
) -> Result<(), SyncRefusal> {
    authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &permission,
        PolicyRequestExtras::operation(operation),
    )
    .await
    .map_err(pull_refusal)
}

/// Only a real denial refuses the caller; an unreadable policy state or a
/// failed check is this node's problem and stays retryable.
fn pull_refusal(error: AuthorizeError) -> SyncRefusal {
    match error {
        AuthorizeError::PermissionDenied
        | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => SyncRefusal::Forbidden,
        AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_))
        | AuthorizeError::Storage(_)
        | AuthorizeError::CheckFailed(_) => SyncRefusal::Unavailable,
    }
}

async fn delete_target(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    node_id: NodeId,
    bucket: &BucketInfo,
    request: &PullRequest,
) -> Result<SyncPullAck, SyncRefusal> {
    let result = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: request.target_bucket.clone(),
            key: request.target_key.clone(),
            version_id: None,
            group_id: bucket.group_id,
            realm_id: auth.realm_id,
            node_id,
            deleted_by: auth.user_id,
        })
        // The marker carries the device version it came from, so a retried
        // delete is recognized as the same one instead of stacking markers.
        .with_metadata(HashMap::from([(
            SYNC_SOURCE_VERSION_TAG.to_string(),
            request.source.version.to_string(),
        )])),
        context,
    )
    .await
    .map_err(|error| {
        debug!(error = %error, "A forwarded sync delete failed");
        SyncRefusal::Unavailable
    })?
    .ok_or(SyncRefusal::Unavailable)?
    .map_err(|error| {
        debug!(error = %error, "A forwarded sync delete failed");
        SyncRefusal::Unavailable
    })?;
    Ok(SyncPullAck {
        version_id: result.version_id,
        already_applied: false,
    })
}

/// Reads the device's exact version and commits it as the owner's own write.
async fn commit_pull(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    node_id: NodeId,
    bucket: &BucketInfo,
    request: PullRequest,
) -> Result<SyncPullAck, SyncRefusal> {
    let read = BaoReadRequest {
        auth_context: auth.clone(),
        realm_id: auth.realm_id,
        target: BaoReadTarget::ExactVersion(request.source.clone()),
        expected_blake3: request.blake3,
        metadata_only: false,
        destination: None,
        known_refs: Vec::new(),
    };
    let blob = match managed_read(context, request.source.node_id, read).await {
        Ok(BaoReadOutput::Stream { blob, .. }) => blob,
        // A metadata-only answer to a full read is a broken exchange, not an
        // absent object: parking the upload on it would lose the file.
        Ok(BaoReadOutput::Metadata { .. }) => return Err(SyncRefusal::Unavailable),
        Err(error) => {
            debug!(error = %error, "Could not read a device version for a sync pull");
            return Err(read_refusal(&error));
        }
    };
    let routing = routing_snapshot(context, bucket.group_id, &request.target_bucket)
        .await
        .map_err(|_| SyncRefusal::Unavailable)?;
    let quota = load_realm_config(context, auth.realm_id)
        .await
        .and_then(|config| config.quota.effective_group_ceiling(&bucket.group_id));
    let operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id: bucket.group_id,
        realm_id: auth.realm_id,
        node_id,
        request: PutObjectInput {
            bucket: request.target_bucket.clone(),
            key: request.target_key.clone(),
            content_length: Some(request.size),
            body: Some(blob),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: quota,
        routing,
    })
    .with_bucket_guard(bucket.clone())
    .with_metadata(HashMap::from([(
        SYNC_SOURCE_VERSION_TAG.to_string(),
        request.source.version.to_string(),
    )]));
    let result = drive(operation, context)
        .await
        .map_err(|_| SyncRefusal::Unavailable)?
        .ok_or(SyncRefusal::Unavailable)?
        .map_err(|error| {
            debug!(error = %error, "A forwarded sync pull could not be written");
            SyncRefusal::Unavailable
        })?;
    Ok(SyncPullAck {
        version_id: result.version_id,
        already_applied: false,
    })
}

/// Why a device read failed. Bytes that do not answer for the hash the upload
/// named are the request's own fault: retrying re-reads the same mismatch, so
/// the row is parked for the owner instead. Everything else is infrastructure.
fn read_refusal(error: &BaoReadError) -> SyncRefusal {
    match error {
        BaoReadError::Refused(BaoReadRefusal::HashMismatch) => SyncRefusal::Invalid(
            "the device version no longer carries the bytes this pull named".to_string(),
        ),
        _ => SyncRefusal::Unavailable,
    }
}

/// The version this device's exact source already produced here, if any.
///
/// Every version of the key is examined, not only the head: a later unrelated
/// write must not make a replayed pull look new and mint a second copy.
async fn applied_version(context: &Arc<DriverContext>, request: &PullRequest) -> Option<Ulid> {
    let source = request.source.version.to_string();
    let prefix = VersionKey::object_prefix(&request.target_bucket, &request.target_key).ok()?;
    let mut start: Option<aruna_core::types::Key> = None;
    loop {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                prefix: Some(prefix.clone().into()),
                start: start.map(IterStart::After),
                limit: VERSION_SCAN_PAGE,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return None;
        };
        for (key, value) in &values {
            let Ok(version) = BlobVersion::from_bytes(value.as_ref()) else {
                continue;
            };
            // A device version is published once and deleted once, both under
            // its tag: only the kind this request asks for is its replay.
            if version
                .metadata
                .get(SYNC_SOURCE_VERSION_TAG)
                .is_some_and(|tag| *tag == source)
                && matches!(version.state, BlobVersionState::Deleted) == request.deleted
                && let Ok(parsed) = VersionKey::from_bytes(key.as_ref())
            {
                return Some(parsed.version_id);
            }
        }
        match next_start_after {
            Some(next) => start = Some(next),
            None => return None,
        }
    }
}

/// Serves one bounded listing of the current heads under a bucket prefix.
pub async fn serve_list_versions(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardListVersions {
        auth_token,
        bucket,
        prefix,
        cursor,
        limit,
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected metadata control message".to_string());
    };
    let result = list_heads(context, peer, auth_token, bucket, prefix, cursor, limit).await;
    MetadataTransportMessage::ForwardedVersions { result }
}

async fn list_heads(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: aruna_core::metadata::MetadataAuthToken,
    bucket: String,
    prefix: String,
    cursor: Option<SyncListCursor>,
    limit: SyncPageLimit,
) -> Result<SyncVersionPage, SyncRefusal> {
    let auth = authorize_peer(context, peer, auth_token).await?;
    let node_id = local_node(context)?;
    let info = read_bucket(context, &bucket).await?;
    ensure_read(context, &auth, node_id, info.group_id, &bucket).await?;
    let (key_marker, version_id_marker) = match cursor {
        Some(cursor) => (Some(cursor.key), cursor.version_id),
        None => (None, None),
    };
    let result = drive(
        ListObjectVersionsOperation::new(ListObjectVersionsInput {
            bucket,
            prefix: (!prefix.is_empty()).then(|| listing_prefix(&prefix)),
            delimiter: None,
            key_marker,
            version_id_marker,
            max_keys: Some(limit.get()),
        }),
        context,
    )
    .await
    .map_err(|_| SyncRefusal::Unavailable)?
    .ok_or(SyncRefusal::Unavailable)?
    .map_err(|_| SyncRefusal::Unavailable)?;
    let heads = result
        .items
        .iter()
        .filter_map(|item| head_of(item, &prefix))
        .collect();
    let next = result.is_truncated.then(|| SyncListCursor {
        key: result.next_key_marker.unwrap_or_default(),
        version_id: result.next_version_id_marker,
    });
    SyncVersionPage::new(heads, next).map_err(|error| SyncRefusal::Invalid(error.to_string()))
}

/// The listing prefix a bound folder maps to, always ending in a separator so
/// a sibling directory with the same leading characters is not listed too.
fn listing_prefix(prefix: &str) -> String {
    format!("{}/", prefix.trim_end_matches('/'))
}

/// One current head, named relative to the requested prefix. Only the latest
/// version of a key is a head; older versions are not the folder's state.
fn head_of(item: &ListObjectVersionsItem, prefix: &str) -> Option<RemoteHead> {
    let (key, version_id, deleted, location, metadata, created_at) = match item {
        ListObjectVersionsItem::Version {
            key,
            version_id,
            is_latest: true,
            location,
            source_metadata,
            created_at,
        } => (
            key,
            version_id,
            false,
            location.as_ref(),
            source_metadata,
            created_at,
        ),
        ListObjectVersionsItem::DeleteMarker {
            key,
            version_id,
            is_latest: true,
            created_at,
        } => (key, version_id, true, None, &None, created_at),
        _ => return None,
    };
    Some(RemoteHead {
        relative: relative_key(key, prefix)?,
        version_id: *version_id,
        size: location
            .map(|location| location.blob_size)
            .or_else(|| metadata.as_ref().map(|metadata| metadata.content_length))
            .unwrap_or_default(),
        blake3: location
            .and_then(|location| location.get_blake3())
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok()),
        modified_at_ms: created_at
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|since| u64::try_from(since.as_millis()).ok()),
        deleted,
    })
}

fn relative_key(key: &str, prefix: &str) -> Option<String> {
    match prefix.is_empty() {
        true => Some(key.to_string()),
        false => key
            .strip_prefix(&listing_prefix(prefix))
            .map(ToOwned::to_owned),
    }
}

async fn ensure_read(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    bucket: &str,
) -> Result<(), SyncRefusal> {
    authorize_pull(
        context,
        auth,
        aruna_core::structs::blob_bucket_permission_path(auth.realm_id, group_id, node_id, bucket),
        Permission::READ,
        "s3.ListObjectVersions",
    )
    .await
}

#[cfg(test)]
mod tests;
