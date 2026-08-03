use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{AuthContext, JobId, JobPayload, RealmId};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::streams::{BiStream, RecvStream, SendStream};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tracing::warn;

use super::runtime::JobsRuntime;
use super::service::{
    ArtifactLookup, CancelJobOutcome, JobReportLookup, OwnedArtifact, cancel_owned_job,
    read_artifact_range, read_job_run_crate_status, read_owned_artifact, read_owned_job,
    read_owned_report, resolve_job_owner,
};
use super::staging::read_staging_checkpoint;
use crate::driver::DriverContext;
use crate::metadata::MetadataWritePeerError;

pub(crate) use aruna_core::jobs::{
    JobRequest, JobResponse, JobStatusView, WireArtifact, WireRange,
};

const JOB_IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_JOB_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JobRouteError {
    #[error("job request is unauthorized")]
    Unauthorized,
    #[error("job request is forbidden")]
    Forbidden,
    #[error("job not found")]
    NotFound,
    #[error("job holder unavailable: {0}")]
    Unavailable(String),
    #[error("job operation failed: {0}")]
    Internal(String),
}

impl From<&OwnedArtifact> for WireArtifact {
    fn from(owned: &OwnedArtifact) -> Self {
        Self {
            job_id: owned.job_id,
            created_by: owned.created_by,
            blake3: owned.blake3,
            size: owned.size,
            filename: owned.filename.clone(),
        }
    }
}

impl From<WireArtifact> for OwnedArtifact {
    fn from(owned: WireArtifact) -> Self {
        Self {
            job_id: owned.job_id,
            created_by: owned.created_by,
            blake3: owned.blake3,
            size: owned.size,
            filename: owned.filename,
            artifact: None,
        }
    }
}

pub(crate) struct RemoteJobReply {
    pub response: JobResponse,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

pub(crate) async fn send_job_request(
    context: &DriverContext,
    holder: NodeId,
    request: JobRequest,
) -> Result<RemoteJobReply, JobRouteError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("network handle unavailable".to_string()))?;
    let expects_body = matches!(request, JobRequest::Artifact { range: Some(_), .. });
    let mut stream = net_handle
        .open_stream(holder, Alpn::JobControl)
        .await
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    timeout(JOB_IO_TIMEOUT, write_frame(&mut stream.0, &request))
        .await
        .map_err(|_| JobRouteError::Unavailable("job request timed out".to_string()))?
        .map_err(JobRouteError::Unavailable)?;
    stream
        .0
        .finish()
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    let response = timeout(JOB_IO_TIMEOUT, read_frame(&mut stream.1))
        .await
        .map_err(|_| JobRouteError::Unavailable("job response timed out".to_string()))?
        .map_err(JobRouteError::Unavailable)?;
    let body = if expects_body && matches!(response, JobResponse::ArtifactReady { .. }) {
        Some(BackendStream::new(ReaderStream::new(stream.into_recv())))
    } else {
        stream.1.stop(0u32.into()).ok();
        None
    };
    Ok(RemoteJobReply { response, body })
}

pub(crate) async fn handle_job_stream(
    context: &DriverContext,
    runtime: &Arc<JobsRuntime>,
    mut stream: BiStream,
    peer: NodeId,
) {
    let request = match timeout(JOB_IO_TIMEOUT, read_frame::<JobRequest>(&mut stream.1)).await {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            warn!(%peer, %error, "Failed to read job-control request");
            return;
        }
        Err(_) => {
            warn!(%peer, "Timed out reading job-control request");
            return;
        }
    };
    let mut prepared = prepare_response(context, runtime, peer, request).await;
    if !matches!(
        timeout(
            JOB_IO_TIMEOUT,
            write_frame(&mut stream.0, &prepared.response)
        )
        .await,
        Ok(Ok(()))
    ) {
        return;
    }
    if let Some(body) = prepared.body.as_mut() {
        loop {
            // Bound the source poll so a stalled artifact reader releases its
            // blob-read and inbound-handler permits instead of pinning them.
            let next = match timeout(JOB_IO_TIMEOUT, body.next()).await {
                Ok(next) => next,
                Err(_) => {
                    warn!(%peer, "Timed out reading job artifact source");
                    stream.0.reset(1u32.into()).ok();
                    return;
                }
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    warn!(%peer, %error, "Failed to read job artifact");
                    stream.0.reset(1u32.into()).ok();
                    return;
                }
            };
            if !matches!(
                timeout(JOB_IO_TIMEOUT, stream.0.write_all(&chunk)).await,
                Ok(Ok(()))
            ) {
                return;
            }
        }
    }
    stream.0.finish().ok();
}

struct PreparedResponse {
    response: JobResponse,
    body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

impl PreparedResponse {
    fn new(response: JobResponse) -> Self {
        Self {
            response,
            body: None,
        }
    }
}

async fn prepare_response(
    context: &DriverContext,
    runtime: &Arc<JobsRuntime>,
    peer: NodeId,
    request: JobRequest,
) -> PreparedResponse {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "metadata auth handle unavailable".to_string(),
        ));
    };
    let auth = match metadata_handle
        .authorize_write_peer(peer, Some(request.auth_token()))
        .await
    {
        Ok(auth) => auth,
        Err(MetadataWritePeerError::Unauthorized) => {
            return PreparedResponse::new(JobResponse::Unauthorized);
        }
        Err(MetadataWritePeerError::Unavailable(error)) => {
            return PreparedResponse::new(JobResponse::Unavailable(error.to_string()));
        }
    };
    let Some(local_realm_id) = context.net_handle.as_ref().map(|net| *net.realm_id()) else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job-control network handle unavailable".to_string(),
        ));
    };
    if !auth_realm_matches(&auth, local_realm_id) {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    match request {
        // Owner-directed operations: only the immutable owner may answer them.
        JobRequest::Status { job_id, .. } => {
            if let Some(rejected) = owner_gate(context, job_id, local_node).await {
                return rejected;
            }
            prepare_status(context, auth.user_id, job_id).await
        }
        JobRequest::Report {
            job_id,
            expected_digest,
            last_key,
            limit,
            ..
        } => {
            if let Some(rejected) = owner_gate(context, job_id, local_node).await {
                return rejected;
            }
            prepare_report(
                context,
                auth.user_id,
                job_id,
                expected_digest,
                last_key,
                usize::from(limit),
            )
            .await
        }
        JobRequest::Artifact { job_id, range, .. } => {
            if let Some(rejected) = owner_gate(context, job_id, local_node).await {
                return rejected;
            }
            prepare_artifact(
                context,
                auth.user_id,
                job_id,
                unix_timestamp_millis(),
                range,
            )
            .await
        }
        JobRequest::Cancel { job_id, .. } => {
            if let Some(rejected) = owner_gate(context, job_id, local_node).await {
                return rejected;
            }
            prepare_cancel(context, runtime, auth.user_id, job_id).await
        }
        JobRequest::Record { job_id, .. } => {
            if let Some(rejected) = owner_gate(context, job_id, local_node).await {
                return rejected;
            }
            prepare_record(context, auth.user_id, job_id).await
        }
    }
}

async fn prepare_record(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> PreparedResponse {
    let record = match read_owned_job(context, user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return PreparedResponse::new(JobResponse::NotFound),
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    let checkpoint = if matches!(&record.payload, JobPayload::Staging(_)) {
        match read_staging_checkpoint(context, job_id).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
        }
    } else {
        None
    };
    PreparedResponse::new(JobResponse::Record {
        record: Box::new(record),
        checkpoint,
    })
}

/// Owner-directed requests are answered only by the derived owner, the sole
/// absence authority: a non-owner or unresolved owner answers `Unavailable`,
/// and only a provably invalid id is `NotFound`.
async fn owner_gate(
    context: &DriverContext,
    job_id: JobId,
    local_node: Option<NodeId>,
) -> Option<PreparedResponse> {
    match resolve_job_owner(context, job_id).await {
        Ok(owner) if local_node == Some(owner) => None,
        Ok(_) => Some(PreparedResponse::new(JobResponse::Unavailable(
            "receiving node does not own this job".to_string(),
        ))),
        Err(JobRouteError::NotFound) => Some(PreparedResponse::new(JobResponse::NotFound)),
        Err(error) => Some(PreparedResponse::new(JobResponse::Unavailable(
            error.to_string(),
        ))),
    }
}

fn auth_realm_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && auth.user_id.realm_id == realm_id
}

async fn prepare_status(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> PreparedResponse {
    let record = match read_owned_job(context, user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return PreparedResponse::new(JobResponse::NotFound),
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    let run_crate = match read_job_run_crate_status(context, job_id).await {
        Ok(status) => status.map(|status| status.to_public_json().to_string()),
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    PreparedResponse::new(JobResponse::Status {
        job: JobStatusView::from(&record),
        run_crate,
    })
}

async fn prepare_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> PreparedResponse {
    let response =
        match read_owned_report(context, user_id, job_id, expected_digest, last_key, limit).await {
            Ok(JobReportLookup::NotFound) => JobResponse::NotFound,
            Ok(JobReportLookup::Pending(state)) => JobResponse::ReportPending(state),
            Ok(JobReportLookup::CursorConflict) => JobResponse::ReportConflict,
            Ok(JobReportLookup::Ready {
                job,
                rows,
                next_key,
            }) => JobResponse::ReportReady {
                job,
                rows: rows
                    .into_iter()
                    .map(|(key, value)| (key, value.as_ref().to_vec()))
                    .collect(),
                next_key,
            },
            Err(error) => JobResponse::Unavailable(error),
        };
    PreparedResponse::new(response)
}

async fn prepare_artifact(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<WireRange>,
) -> PreparedResponse {
    let owned = match read_owned_artifact(context, user_id, job_id, now_ms).await {
        Ok(ArtifactLookup::NotFound) => {
            return PreparedResponse::new(JobResponse::NotFound);
        }
        Ok(ArtifactLookup::Pending(state)) => {
            return PreparedResponse::new(JobResponse::ArtifactPending(state));
        }
        Ok(ArtifactLookup::Gone) => {
            return PreparedResponse::new(JobResponse::ArtifactGone);
        }
        Ok(ArtifactLookup::Ready(owned)) => owned,
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    let Some(range) = range else {
        return PreparedResponse::new(JobResponse::ArtifactReady {
            owned: WireArtifact::from(&owned),
            stream_size: 0,
        });
    };
    if range.start >= range.end || range.end > owned.size {
        return PreparedResponse::new(JobResponse::Unavailable(
            "invalid job artifact range".to_string(),
        ));
    }
    let Some(artifact) = owned.source() else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "local artifact source is unavailable".to_string(),
        ));
    };
    let read = match read_artifact_range(context, artifact, range.start..range.end).await {
        Ok(read) => read,
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    if read.stream_size != range.end - range.start {
        return PreparedResponse::new(JobResponse::Unavailable(
            "artifact reader returned an unexpected range size".to_string(),
        ));
    }
    PreparedResponse {
        response: JobResponse::ArtifactReady {
            owned: WireArtifact::from(&owned),
            stream_size: read.stream_size,
        },
        body: Some(read.blob),
    }
}

async fn prepare_cancel(
    context: &DriverContext,
    runtime: &Arc<JobsRuntime>,
    user_id: UserId,
    job_id: JobId,
) -> PreparedResponse {
    let response = match cancel_owned_job(context, runtime, user_id, job_id).await {
        Ok(CancelJobOutcome::NotFound) => JobResponse::NotFound,
        Ok(CancelJobOutcome::AlreadyTerminal(record)) => JobResponse::Cancelled {
            job: JobStatusView::from(&record),
            terminal: true,
        },
        Ok(CancelJobOutcome::Requested(record)) => JobResponse::Cancelled {
            job: JobStatusView::from(&record),
            terminal: false,
        },
        Err(error) => JobResponse::Unavailable(error),
    };
    PreparedResponse::new(response)
}

async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    if bytes.len() > MAX_JOB_FRAME_SIZE {
        return Err("job-control frame exceeds maximum size".to_string());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    send.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())?;
    send.flush().await.map_err(|error| error.to_string())
}

async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, String> {
    let mut length = [0u8; 4];
    recv.read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_JOB_FRAME_SIZE {
        return Err("job-control frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; length];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{JobProgress, JobState, WorkspaceMode};
    use ulid::Ulid;

    use crate::jobs::service::JobKind;

    fn user(realm_id: RealmId) -> UserId {
        UserId::new(Ulid::from_bytes([2u8; 16]), realm_id)
    }

    #[test]
    fn rejects_foreign_realm() {
        let local = RealmId([1u8; 32]);
        let foreign = RealmId([2u8; 32]);
        let mut auth = AuthContext {
            user_id: user(local),
            realm_id: local,
            path_restrictions: None,
        };
        assert!(auth_realm_matches(&auth, local));
        auth.realm_id = foreign;
        assert!(!auth_realm_matches(&auth, local));
        auth.realm_id = local;
        auth.user_id = user(foreign);
        assert!(!auth_realm_matches(&auth, local));
    }

    #[test]
    fn status_wire_roundtrips() {
        let realm_id = RealmId([1u8; 32]);
        let response = JobResponse::Status {
            job: JobStatusView {
                job_id: JobId::from_bytes([3u8; 16]),
                created_by: user(realm_id),
                kind: JobKind::Execution,
                state: JobState::Succeeded,
                attempts: 1,
                cancel_requested: false,
                created_at_ms: 1,
                updated_at_ms: 2,
                finished_at_ms: Some(2),
                progress: JobProgress::new("items"),
                last_error: None,
                result: Some(serde_json::json!({ "exit_code": 0 })),
                workspace_bucket: Some("workspace".to_string()),
                workspace_mode: WorkspaceMode::Kept,
            },
            run_crate: Some(r#"{"status":"pending"}"#.to_string()),
        };
        let encoded = postcard::to_allocvec(&response).unwrap();
        let decoded: JobResponse = postcard::from_bytes(&encoded).unwrap();
        let JobResponse::Status { job, run_crate } = decoded else {
            panic!("status response roundtrip changed variants");
        };
        assert_eq!(job.result, Some(serde_json::json!({ "exit_code": 0 })));
        assert_eq!(run_crate.as_deref(), Some(r#"{"status":"pending"}"#));
    }
}
