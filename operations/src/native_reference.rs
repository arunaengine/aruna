use std::str::FromStr;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::StagingSourceEffect;
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, Permission, ResolvedSourceAccess, SourceConnectorKind, SourceMetadata,
    SyncRelationship, SyncState, blob_object_permission_path,
};
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, RecvStream, SendStream};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::connectors::resolver::{ARUNA_NATIVE_ORIGIN_NODE_ID, ARUNA_NATIVE_RELATIONSHIP_ID};
use crate::driver::{DriverContext, drive};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::get_object::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use crate::s3::head_object::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::sync_mirror_repair::{kick_mirror_repair, store_sync_status};
use crate::sync_relationship::{
    GetSyncRelationshipOperation, SyncRelationshipDirection, SyncRelationshipError,
};

const NATIVE_IO_TIMEOUT: Duration = Duration::from_secs(30);
const NATIVE_MAX_HEADER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct NativeReferenceRequest {
    relationship_id: Ulid,
    bucket: String,
    key: String,
    version_id: Ulid,
    head: bool,
    range: Option<NativeRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct NativeRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum NativeReferenceResponse {
    Content(SourceMetadata),
    Rejected(NativeReferenceReject),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum NativeReferenceReject {
    AccessDenied,
    NotFound,
    Unavailable(String),
}

struct PreparedReference {
    metadata: SourceMetadata,
    body: Option<BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>>>,
}

pub(crate) fn is_native_effect(effect: &StagingSourceEffect) -> bool {
    let access = match effect {
        StagingSourceEffect::Check { access }
        | StagingSourceEffect::Head { access }
        | StagingSourceEffect::List { access, .. }
        | StagingSourceEffect::Read { access, .. } => access,
    };
    matches!(
        access,
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::ArunaNative,
            ..
        }
    )
}

pub(crate) async fn send_native_effect(
    effect: StagingSourceEffect,
    context: &DriverContext,
) -> Event {
    let result = match effect {
        StagingSourceEffect::Head { access } => native_head(context, access).await,
        StagingSourceEffect::Read { access, range } => native_read(context, access, range).await,
        StagingSourceEffect::Check { .. } | StagingSourceEffect::List { .. } => Err(
            StagingSourceError::UnsupportedKind(SourceConnectorKind::ArunaNative.to_string()),
        ),
    };
    match result {
        Ok(event) => Event::StagingSource(event),
        Err(error) => Event::StagingSource(StagingSourceEvent::Error { error }),
    }
}

async fn native_head(
    context: &DriverContext,
    access: ResolvedSourceAccess,
) -> Result<StagingSourceEvent, StagingSourceError> {
    let (origin, request) = native_request(&access, None, true)?;
    let (response, mut stream) = send_request(context, origin, request).await?;
    close_stream(&mut stream);
    match response {
        NativeReferenceResponse::Content(metadata) => {
            Ok(StagingSourceEvent::HeadResult { metadata })
        }
        NativeReferenceResponse::Rejected(reject) => Err(map_reject(reject)),
    }
}

async fn native_read(
    context: &DriverContext,
    access: ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<StagingSourceEvent, StagingSourceError> {
    let (origin, request) = native_request(&access, range, false)?;
    let (response, stream) = send_request(context, origin, request).await?;
    match response {
        NativeReferenceResponse::Content(metadata) => {
            let reader = ReaderStream::new(stream.into_recv());
            Ok(StagingSourceEvent::ReadResult {
                metadata,
                stream: BackendStream::new(reader),
            })
        }
        NativeReferenceResponse::Rejected(reject) => Err(map_reject(reject)),
    }
}

fn native_request(
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
    head: bool,
) -> Result<(NodeId, NativeReferenceRequest), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::ArunaNative,
        config,
        path,
        version: Some(version),
    } = access
    else {
        return Err(StagingSourceError::UnsupportedKind(
            "invalid ArunaNative access".to_string(),
        ));
    };
    let origin_node_id = config
        .get(ARUNA_NATIVE_ORIGIN_NODE_ID)
        .and_then(|value| NodeId::from_str(value).ok())
        .ok_or_else(|| StagingSourceError::ReadError("missing origin node".to_string()))?;
    let relationship_id = config
        .get(ARUNA_NATIVE_RELATIONSHIP_ID)
        .and_then(|value| Ulid::from_string(value).ok())
        .ok_or_else(|| StagingSourceError::ReadError("missing relationship id".to_string()))?;
    let version_id = Ulid::from_string(version)
        .map_err(|_| StagingSourceError::ReadError("invalid source version".to_string()))?;
    let (bucket, key) = path
        .split_once('/')
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or_else(|| StagingSourceError::ReadError("invalid source path".to_string()))?;
    let range = range
        .map(|range| {
            (range.start < range.end)
                .then_some(NativeRange {
                    start: range.start,
                    end: range.end,
                })
                .ok_or_else(|| StagingSourceError::ReadError("invalid source range".to_string()))
        })
        .transpose()?;
    Ok((
        origin_node_id,
        NativeReferenceRequest {
            relationship_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            head,
            range,
        },
    ))
}

async fn send_request(
    context: &DriverContext,
    origin: NodeId,
    request: NativeReferenceRequest,
) -> Result<(NativeReferenceResponse, BiStream), StagingSourceError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(StagingSourceError::HandleMissing)?;
    let mut stream = net_handle
        .open_stream(origin, Alpn::NativeReference)
        .await
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    timeout(NATIVE_IO_TIMEOUT, write_frame(&mut stream.0, &request))
        .await
        .map_err(|_| StagingSourceError::ReadError("native request timed out".to_string()))?
        .map_err(StagingSourceError::ReadError)?;
    stream
        .0
        .finish()
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let response = timeout(
        NATIVE_IO_TIMEOUT,
        read_frame::<NativeReferenceResponse>(&mut stream.1),
    )
    .await
    .map_err(|_| StagingSourceError::ReadError("native response timed out".to_string()))?
    .map_err(StagingSourceError::ReadError)?;
    Ok((response, stream))
}

fn map_reject(reject: NativeReferenceReject) -> StagingSourceError {
    match reject {
        NativeReferenceReject::AccessDenied => StagingSourceError::AccessDenied,
        NativeReferenceReject::NotFound => StagingSourceError::NotFound,
        NativeReferenceReject::Unavailable(message) => StagingSourceError::ReadError(message),
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
            if write_response(
                &mut stream.0,
                &NativeReferenceResponse::Content(prepared.metadata),
            )
            .await
            .is_err()
            {
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
    let relationship = drive(
        GetSyncRelationshipOperation::new(
            request.relationship_id,
            SyncRelationshipDirection::Outgoing,
        ),
        context,
    )
    .await
    .map_err(map_relationship_error)?;
    validate_relationship(net_handle, peer, &relationship, request)?;
    let bucket_info =
        match drive(GetBucketInfoOperation::new(request.bucket.clone()), context).await {
            Ok(Some(Ok(info))) => info,
            Ok(Some(Err(GetBucketInfoError::NotFound))) => {
                return Err(NativeReferenceReject::NotFound);
            }
            Ok(Some(Err(error))) | Err(error) => {
                return Err(NativeReferenceReject::Unavailable(error.to_string()));
            }
            Ok(None) => {
                return Err(NativeReferenceReject::Unavailable(
                    "source bucket lookup did not finish".to_string(),
                ));
            }
        };
    let permitted = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: relationship.created_by,
                realm_id: relationship.source.realm_id,
                path_restrictions: None,
            },
            path: blob_object_permission_path(
                relationship.source.realm_id,
                bucket_info.group_id,
                relationship.source.node_id,
                &request.bucket,
                &request.key,
            ),
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    .map_err(|error| NativeReferenceReject::Unavailable(error.to_string()))?;
    if !permitted {
        mark_access_denied(context, relationship).await;
        return Err(NativeReferenceReject::AccessDenied);
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
        .map_err(map_head_error)?
        .ok_or_else(|| {
            NativeReferenceReject::Unavailable("source head did not finish".to_string())
        })?
        .map_err(map_head_error)?;
        let metadata = head_metadata(&result, request.version_id)?;
        return Ok(PreparedReference {
            metadata,
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
            user_identity: relationship.created_by,
        }),
        context,
    )
    .await
    .map_err(map_get_error)?
    .ok_or_else(|| NativeReferenceReject::Unavailable("source read did not finish".to_string()))?
    .map_err(map_get_error)?;
    let metadata = result_metadata(&result, request.version_id)?;
    Ok(PreparedReference {
        metadata,
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
    // Detached stubs are deleted relationships that must keep serving the
    // reference records the target retained; every other non-enabled state
    // still refuses access.
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
        | GetObjectError::DeleteMarker => NativeReferenceReject::NotFound,
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
            last_modified: result.version_created_at,
            source_version: None,
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
        // A detached stub has no owner-visible entry left to surface the
        // failure in, and flipping it to Failed would stop serving retained
        // data permanently; the per-request denial is the only surface.
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
    match store_sync_status(context, &relationship).await {
        Ok(true) => kick_mirror_repair(context).await,
        Ok(false) => {}
        Err(error) => warn!(%error, "Failed to persist native reference denial"),
    }
}

async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    if bytes.len() > NATIVE_MAX_HEADER_SIZE {
        return Err("native reference header is too large".to_string());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    send.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())
}

async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, String> {
    let mut length = [0u8; 4];
    recv.read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > NATIVE_MAX_HEADER_SIZE {
        return Err("native reference header is too large".to_string());
    }
    let mut bytes = vec![0u8; length];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}

fn close_stream(stream: &mut BiStream) {
    stream.0.finish().ok();
    stream.1.stop(0u32.into()).ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{ArunaArn, ReferenceHandling, SyncMode, SyncStatusSnapshot};
    use aruna_net::NetConfig;
    use aruna_storage::storage::FjallStorage;

    #[test]
    fn request_roundtrips() {
        let request = NativeReferenceRequest {
            relationship_id: Ulid::from_bytes([1u8; 16]),
            bucket: "source".to_string(),
            key: "nested/data.txt".to_string(),
            version_id: Ulid::from_bytes([2u8; 16]),
            head: false,
            range: Some(NativeRange { start: 3, end: 9 }),
        };
        let bytes = postcard::to_allocvec(&request).unwrap();
        assert_eq!(
            postcard::from_bytes::<NativeReferenceRequest>(&bytes).unwrap(),
            request
        );
    }

    #[test]
    fn response_roundtrips() {
        let response = NativeReferenceResponse::Content(SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag".to_string()),
            last_modified: Some(std::time::SystemTime::UNIX_EPOCH),
            source_version: None,
        });
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert_eq!(
            postcard::from_bytes::<NativeReferenceResponse>(&bytes).unwrap(),
            response
        );
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
            relationship_id: relationship.id,
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
