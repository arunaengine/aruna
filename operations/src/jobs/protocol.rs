use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    AuthContext, DocumentClass, JobId, JobRecord, JobState, PlacementRef, PlacementScope, RealmId,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::streams::{BiStream, RecvStream, SendStream};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tracing::warn;

use super::runtime::JobsRuntime;
use super::service::{
    ArtifactLookup, CancelJobOutcome, JobReportLookup, JobReportView, JobStatusView, OwnedArtifact,
    RoutedCancelOutcome, cancel_job_routed, cancel_owned_job, lookup_local_dedup,
    read_artifact_range, read_artifact_routed, read_job_routed, read_job_run_crate_status,
    read_owned_artifact, read_owned_job, read_owned_report, read_report_routed,
    submit_authority_job,
};
use super::store::{JobSyncError, apply_terminal, list_jobs_for_user, write_passive_record};
use super::submit::{SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec};
use crate::driver::{DriverContext, drive};
use crate::metadata::api::load_realm_config;
use crate::metadata::{MetadataAuthToken, MetadataWritePeerError};
use crate::placement::resolve_shard_holders;

const JOB_IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_JOB_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum JobRequest {
    List {
        auth_token: MetadataAuthToken,
        user_id: UserId,
        cursor: Option<Vec<u8>>,
        limit: u16,
        config_digest: [u8; 32],
    },
    Submit {
        auth_token: MetadataAuthToken,
        spec: SubmitJobSpec,
        config_digest: [u8; 32],
    },
    Replicate {
        auth_token: MetadataAuthToken,
        record: JobRecord,
        config_digest: [u8; 32],
    },
    Status {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        config_digest: [u8; 32],
    },
    Report {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        expected_digest: Option<[u8; 32]>,
        last_key: Option<Vec<u8>>,
        limit: u16,
        config_digest: [u8; 32],
    },
    Artifact {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        range: Option<WireRange>,
        config_digest: [u8; 32],
    },
    Cancel {
        auth_token: MetadataAuthToken,
        job_id: JobId,
        config_digest: [u8; 32],
    },
    Deliver {
        auth_token: MetadataAuthToken,
        record: JobRecord,
    },
    Dedup {
        auth_token: MetadataAuthToken,
        user_id: UserId,
        dedup_key: Vec<u8>,
        plan_digest: [u8; 32],
        config_digest: [u8; 32],
    },
    Sync {
        auth_token: MetadataAuthToken,
        record: JobRecord,
    },
}

impl JobRequest {
    fn auth_token(&self) -> MetadataAuthToken {
        match self {
            Self::List { auth_token, .. }
            | Self::Submit { auth_token, .. }
            | Self::Replicate { auth_token, .. }
            | Self::Status { auth_token, .. }
            | Self::Report { auth_token, .. }
            | Self::Artifact { auth_token, .. }
            | Self::Cancel { auth_token, .. }
            | Self::Deliver { auth_token, .. }
            | Self::Dedup { auth_token, .. }
            | Self::Sync { auth_token, .. } => auth_token.clone(),
        }
    }

    fn job_id(&self) -> Option<JobId> {
        match self {
            Self::List { .. } | Self::Submit { .. } | Self::Dedup { .. } => None,
            Self::Replicate { record, .. } => Some(record.job_id),
            Self::Deliver { record, .. } => Some(record.job_id),
            Self::Sync { record, .. } => Some(record.job_id),
            Self::Status { job_id, .. }
            | Self::Report { job_id, .. }
            | Self::Artifact { job_id, .. }
            | Self::Cancel { job_id, .. } => Some(*job_id),
        }
    }

    fn user_id(&self) -> Option<UserId> {
        match self {
            Self::List { user_id, .. } => Some(*user_id),
            Self::Submit { spec, .. } => Some(spec.created_by),
            Self::Dedup { user_id, .. } => Some(*user_id),
            _ => None,
        }
    }

    fn config_digest(&self) -> Option<[u8; 32]> {
        match self {
            Self::List { config_digest, .. }
            | Self::Submit { config_digest, .. }
            | Self::Replicate { config_digest, .. }
            | Self::Status { config_digest, .. }
            | Self::Report { config_digest, .. }
            | Self::Artifact { config_digest, .. }
            | Self::Cancel { config_digest, .. }
            | Self::Dedup { config_digest, .. } => Some(*config_digest),
            Self::Deliver { .. } | Self::Sync { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct WireRange {
    pub start: u64,
    pub end: u64,
}

impl From<Range<u64>> for WireRange {
    fn from(range: Range<u64>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WireArtifact {
    pub job_id: JobId,
    pub created_by: UserId,
    pub blake3: [u8; 32],
    pub size: u64,
    pub filename: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum JobResponse {
    Unauthorized,
    Forbidden,
    NotFound,
    Unavailable(String),
    Listed {
        records: Vec<JobRecord>,
        next_cursor: Option<Vec<u8>>,
    },
    Submitted(SubmitJobResult),
    SubmitConflict(JobId),
    SubmitCap(u32),
    Replicated,
    Status {
        job: JobStatusView,
        run_crate: Option<String>,
    },
    ReportPending(JobState),
    ReportConflict,
    ReportReady {
        job: JobReportView,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        next_key: Option<Vec<u8>>,
    },
    ArtifactPending(JobState),
    ArtifactGone,
    ArtifactReady {
        owned: WireArtifact,
        stream_size: u64,
    },
    Cancelled {
        job: JobStatusView,
        terminal: bool,
    },
    Delivered(SubmitJobResult),
    DeliveryConflict,
    Dedup(Option<SubmitJobResult>),
    Synced(JobId),
    SyncConflict,
}

pub(crate) struct RemoteJobReply {
    pub response: JobResponse,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

pub(crate) struct JobRoute {
    pub holders: Vec<NodeId>,
    pub config_digest: [u8; 32],
}

pub(crate) async fn resolve_job_holders(
    context: &DriverContext,
    job_id: JobId,
) -> Result<JobRoute, JobRouteError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("network handle unavailable".to_string()))?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or_else(|| JobRouteError::Unavailable("realm config unavailable".to_string()))?;
    let routable = job_id
        .as_routable()
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    let resolved = config
        .binding_directory()
        .resolve_id(&routable, |strategy_id| {
            config
                .strategy(&strategy_id)
                .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
        })
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    if resolved.document_class != DocumentClass::JobControl
        || resolved.scope != PlacementScope::Realm(realm_id)
    {
        return Err(JobRouteError::Unavailable(
            "job id does not name this realm's job-control placement".to_string(),
        ));
    }
    let placement = PlacementRef {
        strategy_id: resolved.strategy_id,
        epoch: 0,
        shard: u32::from(resolved.bucket.get()),
    };
    let holders = resolve_shard_holders(&config, &placement);
    if holders.is_empty() {
        return Err(JobRouteError::Unavailable(
            "job-control bucket has no holders".to_string(),
        ));
    }
    let config_digest = config
        .digest()
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    Ok(JobRoute {
        holders,
        config_digest,
    })
}

pub(crate) async fn resolve_job_authority(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<JobRoute, JobRouteError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("network handle unavailable".to_string()))?;
    if *net_handle.realm_id() != realm_id {
        return Err(JobRouteError::Unavailable(
            "job authority does not belong to the serving realm".to_string(),
        ));
    }
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or_else(|| JobRouteError::Unavailable("realm config unavailable".to_string()))?;
    let authority = config.handle_allocator_node().ok_or_else(|| {
        JobRouteError::Unavailable("realm job authority is missing or invalid".to_string())
    })?;
    let config_digest = config
        .digest()
        .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    Ok(JobRoute {
        holders: vec![authority],
        config_digest,
    })
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
    let request = match request {
        JobRequest::Deliver { record, .. } => {
            return prepare_delivery(context, peer, auth.user_id, record).await;
        }
        JobRequest::Sync { record, .. } => {
            return prepare_sync(context, peer, auth.user_id, record).await;
        }
        request => request,
    };
    let Some(config_digest) = request.config_digest() else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job request is missing its placement digest".to_string(),
        ));
    };
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    if let Some(user_id) = request.user_id() {
        let route = match resolve_job_authority(context, user_id.realm_id).await {
            Ok(route) => route,
            Err(error) => {
                return PreparedResponse::new(JobResponse::Unavailable(error.to_string()));
            }
        };
        if route.config_digest != config_digest {
            return PreparedResponse::new(JobResponse::Unavailable(
                "job authority config does not match the requester".to_string(),
            ));
        }
        if local_node != route.holders.first().copied() {
            return PreparedResponse::new(JobResponse::Unavailable(
                "receiving node is not the realm job authority".to_string(),
            ));
        }
        return match request {
            JobRequest::List {
                user_id,
                cursor,
                limit,
                ..
            } => prepare_list(context, auth.user_id, user_id, cursor, limit).await,
            JobRequest::Submit { spec, .. } => prepare_submit(context, auth.user_id, spec).await,
            JobRequest::Dedup {
                user_id,
                dedup_key,
                plan_digest,
                ..
            } => prepare_dedup(context, auth.user_id, user_id, dedup_key, plan_digest).await,
            _ => PreparedResponse::new(JobResponse::Unavailable(
                "invalid authority-routed job request".to_string(),
            )),
        };
    }
    if matches!(
        request,
        JobRequest::Status { .. }
            | JobRequest::Report { .. }
            | JobRequest::Artifact { .. }
            | JobRequest::Cancel { .. }
    ) {
        return prepare_external(context, runtime, peer, auth.user_id, config_digest, request)
            .await;
    }
    let Some(job_id) = request.job_id() else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job-routed request is missing a job id".to_string(),
        ));
    };
    let route = match resolve_job_holders(context, job_id).await {
        Ok(route) => route,
        Err(error) => {
            return PreparedResponse::new(JobResponse::Unavailable(error.to_string()));
        }
    };
    if route.config_digest != config_digest {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job-control realm config does not match the requester".to_string(),
        ));
    }
    if local_node.is_none_or(|node| !route.holders.contains(&node)) {
        return PreparedResponse::new(JobResponse::Unavailable(
            "receiving node does not hold this job-control bucket".to_string(),
        ));
    }
    match request {
        JobRequest::List { .. } | JobRequest::Submit { .. } | JobRequest::Dedup { .. } => {
            PreparedResponse::new(JobResponse::Unavailable(
                "authority-routed request reached the job dispatcher".to_string(),
            ))
        }
        JobRequest::Replicate { record, .. } => {
            prepare_replicate(context, auth.user_id, record).await
        }
        JobRequest::Status { .. }
        | JobRequest::Report { .. }
        | JobRequest::Artifact { .. }
        | JobRequest::Cancel { .. } => PreparedResponse::new(JobResponse::Unavailable(
            "owner-routed request reached the replica dispatcher".to_string(),
        )),
        JobRequest::Deliver { .. } | JobRequest::Sync { .. } => PreparedResponse::new(
            JobResponse::Unavailable("direct job obligation reached the dispatcher".to_string()),
        ),
    }
}

async fn prepare_delivery(
    context: &DriverContext,
    peer: NodeId,
    user_id: UserId,
    record: JobRecord,
) -> PreparedResponse {
    let Some(local_node) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job-control network handle unavailable".to_string(),
        ));
    };
    let Some(config) = load_realm_config(context, user_id.realm_id).await else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "realm config unavailable".to_string(),
        ));
    };
    if record.created_by != user_id
        || record.payload.is_internal()
        || record.owner_node_id != local_node
        || config.handle_allocator_node() != Some(peer)
    {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match drive(SubmitJobOperation::delivered(record), context).await {
        Ok(result) => PreparedResponse::new(JobResponse::Delivered(result)),
        Err(SubmitJobError::JobDeliveryConflict { .. }) => {
            PreparedResponse::new(JobResponse::DeliveryConflict)
        }
        Err(error) => PreparedResponse::new(JobResponse::Unavailable(error.to_string())),
    }
}

async fn prepare_sync(
    context: &DriverContext,
    peer: NodeId,
    user_id: UserId,
    record: JobRecord,
) -> PreparedResponse {
    let Some(local_node) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "job-control network handle unavailable".to_string(),
        ));
    };
    let Some(config) = load_realm_config(context, user_id.realm_id).await else {
        return PreparedResponse::new(JobResponse::Unavailable(
            "realm config unavailable".to_string(),
        ));
    };
    if record.created_by != user_id
        || record.payload.is_internal()
        || config.handle_allocator_node() != Some(local_node)
    {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match apply_terminal(&context.storage_handle, peer, &record).await {
        Ok(record) => PreparedResponse::new(JobResponse::Synced(record.job_id)),
        Err(JobSyncError::NotFound) => PreparedResponse::new(JobResponse::NotFound),
        Err(JobSyncError::Forbidden) => PreparedResponse::new(JobResponse::Forbidden),
        Err(JobSyncError::Conflict) => PreparedResponse::new(JobResponse::SyncConflict),
        Err(JobSyncError::Storage(error)) => PreparedResponse::new(JobResponse::Unavailable(error)),
    }
}

async fn prepare_external(
    context: &DriverContext,
    runtime: &Arc<JobsRuntime>,
    peer: NodeId,
    user_id: UserId,
    config_digest: [u8; 32],
    request: JobRequest,
) -> PreparedResponse {
    let route = match resolve_job_authority(context, user_id.realm_id).await {
        Ok(route) => route,
        Err(error) => return PreparedResponse::new(route_error_response(error)),
    };
    let local_node = context.net_handle.as_ref().map(|net| net.node_id());
    let authority = route.holders[0];
    if local_node == Some(authority) {
        if route.config_digest != config_digest {
            return PreparedResponse::new(JobResponse::Unavailable(
                "job authority config does not match the requester".to_string(),
            ));
        }
        return match request {
            JobRequest::Status {
                auth_token, job_id, ..
            } => prepare_routed_status(context, user_id, job_id, auth_token).await,
            JobRequest::Report {
                auth_token,
                job_id,
                expected_digest,
                last_key,
                limit,
                ..
            } => {
                prepare_routed_report(
                    context,
                    user_id,
                    job_id,
                    expected_digest,
                    last_key,
                    usize::from(limit),
                    auth_token,
                )
                .await
            }
            JobRequest::Artifact {
                auth_token,
                job_id,
                range,
                ..
            } => prepare_routed_artifact(context, user_id, job_id, range, auth_token).await,
            JobRequest::Cancel {
                auth_token, job_id, ..
            } => prepare_routed_cancel(context, runtime, user_id, job_id, auth_token).await,
            _ => PreparedResponse::new(JobResponse::Unavailable(
                "invalid owner-routed job request".to_string(),
            )),
        };
    }
    if peer != authority {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    let Some(job_id) = request.job_id() else {
        return PreparedResponse::new(JobResponse::Forbidden);
    };
    let record = match read_owned_job(context, user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return PreparedResponse::new(JobResponse::NotFound),
        Err(error) => return PreparedResponse::new(JobResponse::Unavailable(error)),
    };
    if local_node != Some(record.owner_node_id) {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match request {
        JobRequest::Status { .. } => prepare_status(context, user_id, job_id).await,
        JobRequest::Report {
            expected_digest,
            last_key,
            limit,
            ..
        } => {
            prepare_report(
                context,
                user_id,
                job_id,
                expected_digest,
                last_key,
                usize::from(limit),
            )
            .await
        }
        JobRequest::Artifact { range, .. } => {
            prepare_artifact(context, user_id, job_id, unix_timestamp_millis(), range).await
        }
        JobRequest::Cancel { .. } => prepare_cancel(context, runtime, user_id, job_id).await,
        _ => PreparedResponse::new(JobResponse::Forbidden),
    }
}

async fn prepare_list(
    context: &DriverContext,
    auth_user: UserId,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: u16,
) -> PreparedResponse {
    if auth_user != user_id {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match list_jobs_for_user(
        &context.storage_handle,
        user_id,
        cursor,
        usize::from(limit),
        |_| true,
    )
    .await
    {
        Ok((records, next_cursor)) => PreparedResponse::new(JobResponse::Listed {
            records,
            next_cursor,
        }),
        Err(error) => PreparedResponse::new(JobResponse::Unavailable(error)),
    }
}

async fn prepare_submit(
    context: &DriverContext,
    user_id: UserId,
    spec: SubmitJobSpec,
) -> PreparedResponse {
    if spec.created_by != user_id {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    let response = match submit_authority_job(context, spec).await {
        Ok(result) => JobResponse::Submitted(result),
        Err(SubmitJobError::JobPlanConflict { existing_job_id }) => {
            JobResponse::SubmitConflict(existing_job_id)
        }
        Err(SubmitJobError::ActiveJobLimit { limit }) => JobResponse::SubmitCap(limit),
        Err(error) => JobResponse::Unavailable(error.to_string()),
    };
    PreparedResponse::new(response)
}

async fn prepare_dedup(
    context: &DriverContext,
    auth_user: UserId,
    user_id: UserId,
    dedup_key: Vec<u8>,
    plan_digest: [u8; 32],
) -> PreparedResponse {
    if auth_user != user_id {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match lookup_local_dedup(context, user_id, &dedup_key, plan_digest).await {
        Ok(result) => PreparedResponse::new(JobResponse::Dedup(result)),
        Err(SubmitJobError::JobPlanConflict { existing_job_id }) => {
            PreparedResponse::new(JobResponse::SubmitConflict(existing_job_id))
        }
        Err(error) => PreparedResponse::new(JobResponse::Unavailable(error.to_string())),
    }
}

async fn prepare_replicate(
    context: &DriverContext,
    user_id: UserId,
    record: JobRecord,
) -> PreparedResponse {
    if record.created_by != user_id || !record.payload.is_internal() {
        return PreparedResponse::new(JobResponse::Forbidden);
    }
    match write_passive_record(&context.storage_handle, &record).await {
        Ok(()) => PreparedResponse::new(JobResponse::Replicated),
        Err(error) => PreparedResponse::new(JobResponse::Unavailable(error)),
    }
}

fn auth_realm_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && auth.user_id.realm_id == realm_id
}

fn route_error_response(error: JobRouteError) -> JobResponse {
    match error {
        JobRouteError::Unauthorized => JobResponse::Unauthorized,
        JobRouteError::Forbidden => JobResponse::Forbidden,
        JobRouteError::NotFound => JobResponse::NotFound,
        JobRouteError::Unavailable(error) | JobRouteError::Internal(error) => {
            JobResponse::Unavailable(error)
        }
    }
}

async fn prepare_routed_status(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: MetadataAuthToken,
) -> PreparedResponse {
    match read_job_routed(context, user_id, job_id, Some(auth_token)).await {
        Ok(status) => PreparedResponse::new(JobResponse::Status {
            job: status.job,
            run_crate: status.run_crate.map(|status| status.to_string()),
        }),
        Err(error) => PreparedResponse::new(route_error_response(error)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_routed_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
    auth_token: MetadataAuthToken,
) -> PreparedResponse {
    let response = match read_report_routed(
        context,
        user_id,
        job_id,
        expected_digest,
        last_key,
        limit,
        Some(auth_token),
    )
    .await
    {
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
        Err(error) => route_error_response(error),
    };
    PreparedResponse::new(response)
}

async fn prepare_routed_artifact(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    range: Option<WireRange>,
    auth_token: MetadataAuthToken,
) -> PreparedResponse {
    let range = range.map(|range| range.start..range.end);
    let (lookup, read) = match read_artifact_routed(
        context,
        user_id,
        job_id,
        unix_timestamp_millis(),
        range,
        Some(auth_token),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => return PreparedResponse::new(route_error_response(error)),
    };
    match lookup {
        ArtifactLookup::NotFound => PreparedResponse::new(JobResponse::NotFound),
        ArtifactLookup::Pending(state) => {
            PreparedResponse::new(JobResponse::ArtifactPending(state))
        }
        ArtifactLookup::Gone => PreparedResponse::new(JobResponse::ArtifactGone),
        ArtifactLookup::Ready(owned) => {
            let stream_size = read.as_ref().map_or(0, |read| read.stream_size);
            PreparedResponse {
                response: JobResponse::ArtifactReady {
                    owned: WireArtifact::from(&owned),
                    stream_size,
                },
                body: read.map(|read| read.blob),
            }
        }
    }
}

async fn prepare_routed_cancel(
    context: &DriverContext,
    runtime: &Arc<JobsRuntime>,
    user_id: UserId,
    job_id: JobId,
    auth_token: MetadataAuthToken,
) -> PreparedResponse {
    let response =
        match cancel_job_routed(context, runtime, user_id, job_id, Some(auth_token)).await {
            Ok(RoutedCancelOutcome::NotFound) => JobResponse::NotFound,
            Ok(RoutedCancelOutcome::AlreadyTerminal(job)) => JobResponse::Cancelled {
                job,
                terminal: true,
            },
            Ok(RoutedCancelOutcome::Requested(job)) => JobResponse::Cancelled {
                job,
                terminal: false,
            },
            Err(error) => route_error_response(error),
        };
    PreparedResponse::new(response)
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
    use aruna_core::structs::{JobProgress, WorkspaceMode};
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
