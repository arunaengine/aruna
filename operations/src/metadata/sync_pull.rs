//! Realm side of a device's synced folders.
//!
//! A device never pushes. It asks its realm node to pull one exact local
//! version; this node re-checks the owner binding, reads those bytes back from
//! the device and commits its own copy through the ordinary object write, so
//! head advance, usage, obligations and watch events all stay unchanged.

use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::{
    AuthContext, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, Permission,
    RemoteHead, SYNC_SOURCE_VERSION_TAG, SyncListCursor, SyncPageLimit, SyncPullAck, SyncRefusal,
    SyncVersionPage, VersionKey, VersionedObjectArn, blob_object_permission_path,
};
use aruna_core::types::{GroupId, NodeId};
use tracing::debug;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive, routing_snapshot};
use crate::metadata::forward::peer_acts_for;
use crate::metadata::handle::MetadataWritePeerError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::process_placements::load_realm_config;
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::delete_object::{DeleteObjectInput, DeleteObjectOperation};
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};

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
    let auth = authorize(context, peer, request.auth_token.clone()).await?;
    // The device may only ask for a version it serves itself: a peer must not
    // make this node read another node's object under its own authority.
    if request.source.node_id != peer {
        return Err(SyncRefusal::Invalid(
            "a sync pull must name the requesting device".to_string(),
        ));
    }
    let bucket = read_bucket(context, &request.target_bucket).await?;
    ensure_write(context, &auth, &bucket, &request).await?;

    match request.deleted {
        true => delete_target(context, &auth, &bucket, &request).await,
        false => commit_pull(context, &auth, &bucket, request).await,
    }
}

/// The owner the device forwards for. A User peer is owner-bound, so the
/// binding is re-checked here rather than trusted from the request.
async fn authorize(
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

async fn read_bucket(
    context: &Arc<DriverContext>,
    bucket: &str,
) -> Result<BucketInfo, SyncRefusal> {
    drive(GetBucketInfoOperation::new(bucket.to_string()), context)
        .await
        .ok()
        .and_then(|result| result.and_then(Result::ok))
        .ok_or(SyncRefusal::NotFound)
}

async fn ensure_write(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    bucket: &BucketInfo,
    request: &PullRequest,
) -> Result<(), SyncRefusal> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?
        .node_id();
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: blob_object_permission_path(
                auth.realm_id,
                bucket.group_id,
                node_id,
                &request.target_bucket,
                &request.target_key,
            ),
            required_permission: Permission::WRITE,
        }),
        context,
    )
    .await
    .map_err(|_| SyncRefusal::Unavailable)?;
    match allowed {
        true => Ok(()),
        false => Err(SyncRefusal::Forbidden),
    }
}

async fn delete_target(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    bucket: &BucketInfo,
    request: &PullRequest,
) -> Result<SyncPullAck, SyncRefusal> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?
        .node_id();
    let result = drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: request.target_bucket.clone(),
            key: request.target_key.clone(),
            version_id: None,
            group_id: bucket.group_id,
            realm_id: auth.realm_id,
            node_id,
            deleted_by: auth.user_id,
        }),
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
    bucket: &BucketInfo,
    request: PullRequest,
) -> Result<SyncPullAck, SyncRefusal> {
    if let Some(version_id) = applied_version(context, &request).await {
        return Ok(SyncPullAck {
            version_id,
            already_applied: true,
        });
    }
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?
        .node_id();
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
        Ok(BaoReadOutput::Metadata { .. }) => return Err(SyncRefusal::NotFound),
        Err(error) => {
            debug!(error = %error, "Could not read a device version for a sync pull");
            return Err(SyncRefusal::Unavailable);
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

/// The version this device's exact source already produced here, if any. It
/// makes a replayed pull a no-op instead of a second version.
async fn applied_version(context: &Arc<DriverContext>, request: &PullRequest) -> Option<Ulid> {
    let pointer = read_row(
        context,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(&request.target_bucket, &request.target_key)
            .to_bytes()
            .ok()?,
    )
    .await
    .and_then(|bytes| CurrentVersionPointer::from_bytes(&bytes).ok())?;
    let version = read_row(
        context,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(
            &request.target_bucket,
            &request.target_key,
            pointer.version_id,
        )
        .to_bytes()
        .ok()?,
    )
    .await
    .and_then(|bytes| BlobVersion::from_bytes(&bytes).ok())?;
    version
        .metadata
        .get(SYNC_SOURCE_VERSION_TAG)
        .is_some_and(|tag| *tag == request.source.version.to_string())
        .then_some(pointer.version_id)
}

async fn read_row(
    context: &Arc<DriverContext>,
    key_space: &str,
    key: Vec<u8>,
) -> Option<byteview::ByteView> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        _ => None,
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
    let auth = authorize(context, peer, auth_token).await?;
    let info = read_bucket(context, &bucket).await?;
    ensure_read(context, &auth, info.group_id, &bucket).await?;
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
    group_id: GroupId,
    bucket: &str,
) -> Result<(), SyncRefusal> {
    let node_id = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?
        .node_id();
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: aruna_core::structs::blob_bucket_permission_path(
                auth.realm_id,
                group_id,
                node_id,
                bucket,
            ),
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    .map_err(|_| SyncRefusal::Unavailable)?;
    match allowed {
        true => Ok(()),
        false => Err(SyncRefusal::Forbidden),
    }
}

#[cfg(test)]
mod tests {
    use super::{head_of, listing_prefix, relative_key};
    use crate::s3::list_object_versions::ListObjectVersionsItem;
    use aruna_core::structs::SourceMetadata;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn version(key: &str, latest: bool) -> ListObjectVersionsItem {
        ListObjectVersionsItem::Version {
            key: key.to_string(),
            version_id: Ulid::from_bytes([1u8; 16]),
            is_latest: latest,
            location: None,
            source_metadata: Some(SourceMetadata {
                content_length: 7,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            }),
            created_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn names_heads_relatively() {
        // The device binds a prefix, so the answer is relative to that prefix.
        let head = head_of(&version("notes/a/b.txt", true), "notes").expect("a head");
        assert_eq!(head.relative, "a/b.txt");
        assert_eq!(head.size, 7);
        assert!(!head.deleted);
        assert!(head_of(&version("notes/a/b.txt", false), "notes").is_none());
        assert!(head_of(&version("other/b.txt", true), "notes").is_none());
    }

    #[test]
    fn bounds_listing_prefix() {
        // `notes` must not list `notesbook`.
        assert_eq!(listing_prefix("notes"), "notes/");
        assert_eq!(listing_prefix("notes/"), "notes/");
        assert_eq!(relative_key("notesbook/x", "notes"), None);
        assert_eq!(relative_key("x", "").as_deref(), Some("x"));
    }
}
