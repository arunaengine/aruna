//! Ingress of one external submission.
//!
//! The node that takes the request normalizes it, authorizes it, derives its
//! replicated identity and family placement, and then either commits it here
//! because its own unconflicted view selects it as a holder, or forwards the
//! complete request one hop to a holder it observes. A non-holder never accepts
//! a job it could not deliver: it writes nothing and returns an availability
//! error instead.

use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{
    AuthContext, ExecutionSpec, JobAdmissionRecord, JobFamilyRecord, JobId, JobRecordEnvelope,
    JobRetryPolicy, LogicalJobSpec, Permission, RealmConfigDocument, SubmissionClaim, SubmissionId,
    WorkspaceMode, blob_group_permission_path,
};
use aruna_core::types::{NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};
use ulid::Ulid;

use super::admit::{AdmissionCandidate, AdmitSubmissionConfig, AdmitSubmissionOperation};
use super::ids::{
    RequestIdentity, SubmissionRequest, SubmissionScope, effective_resources, seal_workspace,
};
use super::witness::arm_family;
use super::{LifecycleError, ids};
use crate::driver::{DriverContext, drive};
use crate::jobs::quota::quota_gate;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::service::{mint_local_job, validate_execution};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::{MetadataAuthToken, MetadataWritePeerError};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::PolicyRequestExtras;

/// Launches one witness may spend on a request over its whole lifetime. It is
/// sealed into the immutable spec, so a later config change cannot widen it.
pub const MAX_LAUNCHES_PER_WITNESS: u32 = 3;

/// What the caller learns about an accepted submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedSubmission {
    /// The canonical alias of this request family at the accepting holder.
    pub job_id: JobId,
    /// False when a matching claim already existed.
    pub created: bool,
    pub submission_id: SubmissionId,
}

/// The holder's answer to a forwarded submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionAck {
    pub job_id: JobId,
    pub created: bool,
}

/// Why a holder did not accept a forwarded submission. Only a definitive
/// refusal ends the ingress; anything else lets it try the next holder.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionRefusal {
    Unauthorized,
    /// This node does not hold the family in its own unconflicted view.
    NotHolder,
    /// The recomputed identity or request digest differs from the forwarded one.
    IdentityMismatch,
    Conflict {
        existing_job_id: JobId,
    },
    Invalid,
    Unavailable,
}

/// Normalizes, authorizes, and admits one external execution request.
#[allow(clippy::too_many_arguments)]
pub async fn submit_external_job(
    context: &DriverContext,
    mut spec: ExecutionSpec,
    created_by: UserId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    validate_execution(&mut spec, workspace_mode, workspace_bucket.as_deref())?;
    seal_workspace(&mut spec, workspace_mode, workspace_bucket)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    ids::required_labels(&spec)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    let scope = match idempotency_key {
        Some(key) => SubmissionScope::Keyed(key),
        None => SubmissionScope::Unkeyed(Ulid::generate()),
    };
    let request = SubmissionRequest {
        created_by,
        spec,
        scope,
        retention_ms,
    };
    let identity = request
        .identity()
        .map_err(|error| SubmitJobError::Conversion(error))?;
    quota_gate(request.spec.group_id, &effective_resources(&request.spec))
        .map_err(|denied| SubmitJobError::QuotaDenied(denied.to_string()))?;
    let (config, local) = local_view(context).await?;
    let view = family_view(&config, local, &identity)?;
    if view.holds(local) {
        let admitted = admit_here(context, &request, &identity, &config, local).await?;
        return Ok(AcceptedSubmission {
            job_id: admitted.0,
            created: admitted.1,
            submission_id: identity.submission_id,
        });
    }
    forward_once(context, &request, &identity, &view, local, auth_token).await
}

/// The realm config this node synchronized plus its own identity.
async fn local_view(
    context: &DriverContext,
) -> Result<(RealmConfigDocument, NodeId), SubmitJobError> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("network handle unavailable".to_string())
    })?;
    let realm_id = *net.realm_id();
    let config = load_realm_config(context, realm_id).await.ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
    })?;
    Ok((config, net.node_id()))
}

/// The unconflicted family view. A view this node cannot resolve is an
/// availability failure, never an accepted job.
fn family_view(
    config: &RealmConfigDocument,
    local: NodeId,
    identity: &RequestIdentity,
) -> Result<FamilyView, SubmitJobError> {
    let _ = local;
    FamilyView::resolve(config, config.realm_id, identity.family()).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job family placement is unavailable".to_string())
    })
}

/// Mints the alias, signs the immutable spec and its claim, and commits them.
async fn admit_here(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    config: &RealmConfigDocument,
    local: NodeId,
) -> Result<(JobId, bool), SubmitJobError> {
    let now_ms = unix_timestamp_millis();
    let placement = config
        .family_placement(identity.submission_id)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let job_id = mint_local_job(
        context,
        request.created_by.realm_id,
        local,
        Some(&identity.submission_id.0),
    )
    .await?;
    let spec = LogicalJobSpec {
        submission_id: identity.submission_id,
        job_id,
        origin_node_id: local,
        realm_id: config.realm_id,
        group_id: request.spec.group_id,
        created_by: request.created_by,
        created_at_ms: now_ms,
        payload: request.spec.clone(),
        request_digest: identity.request_digest,
        spec_digest: [0u8; 32],
        resources: effective_resources(&request.spec),
        retry: JobRetryPolicy {
            max_launches_per_witness: MAX_LAUNCHES_PER_WITNESS,
        },
        admission: JobAdmissionRecord {
            submission_id: identity.submission_id,
            request_digest: identity.request_digest,
            job_id,
            group_id: request.spec.group_id,
            admitting_node_id: local,
            // The realm config publishes no membership generation yet; the
            // departure round fills it. Zero is unstamped, not an epoch claim.
            membership_generation: 0,
            resources: effective_resources(&request.spec),
            admitted_at_ms: now_ms,
        },
        placement,
    }
    .seal()
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let claim = SubmissionClaim {
        submission_id: identity.submission_id,
        job_id,
        request_digest: identity.request_digest,
        spec_digest: spec.spec_digest,
        committing_node_id: local,
        accepted_at_ms: now_ms,
    };
    let spec_frame = sign_frame(
        context,
        config.realm_id,
        JobFamilyRecord::Spec(Box::new(spec)),
    )?;
    let claim_frame = sign_frame(context, config.realm_id, JobFamilyRecord::Claim(claim))?;
    let admitted = drive(
        AdmitSubmissionOperation::new(AdmitSubmissionConfig {
            realm_id: config.realm_id,
            local_node_id: local,
            submission_id: identity.submission_id,
            request_digest: identity.request_digest,
            candidate: Box::new(AdmissionCandidate {
                job_id,
                spec: spec_frame,
                claim: claim_frame,
            }),
            now_ms,
        }),
        context,
    )
    .await
    .map_err(admission_error)?;
    // Scheduling is armed only after the claim is durable, so a failed commit
    // never leaves a witness round pointing at a family that does not exist.
    arm_family(context, identity.family(), now_ms).await;
    Ok((admitted.job_id, admitted.created))
}

fn sign_frame(
    context: &DriverContext,
    realm_id: aruna_core::structs::RealmId,
    record: JobFamilyRecord,
) -> Result<JobRecordFrame, SubmitJobError> {
    let net = context.net_handle.as_ref().ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("network handle unavailable".to_string())
    })?;
    let envelope = JobRecordEnvelope::signed_with(realm_id, record, net.node_id(), |message| {
        net.sign(message)
    })
    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    JobRecordFrame::new(envelope)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))
}

fn admission_error(error: LifecycleError) -> SubmitJobError {
    match error {
        LifecycleError::IdempotencyConflict { existing_job_id } => {
            SubmitJobError::JobPlanConflict { existing_job_id }
        }
        error => SubmitJobError::PlacementUnavailable(error.to_string()),
    }
}

/// Forwards the complete request and its preassigned identity one hop. The
/// holder revalidates the caller and recomputes the identity itself, so this
/// node never becomes the authority for a job it does not hold.
async fn forward_once(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    view: &FamilyView,
    local: NodeId,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    let (Some(metadata), Some(auth_token)) = (context.metadata_handle.as_ref(), auth_token) else {
        return Err(SubmitJobError::PlacementUnavailable(
            "forwarding a submission needs the caller's token".to_string(),
        ));
    };
    for holder in view.holders().iter().filter(|holder| **holder != local) {
        let message = MetadataTransportMessage::ForwardJobSubmission {
            auth_token: auth_token.clone(),
            submission_id: identity.submission_id,
            request: Box::new(request.clone()),
        };
        let reply = match metadata.request_forwarded_write(*holder, message).await {
            Ok(reply) => reply,
            Err(error) => {
                debug!(peer = %holder, error = %error, "Submission forwarding failed");
                continue;
            }
        };
        match reply {
            MetadataTransportMessage::ForwardedJobSubmission { result: Ok(ack) } => {
                return Ok(AcceptedSubmission {
                    job_id: ack.job_id,
                    created: ack.created,
                    submission_id: identity.submission_id,
                });
            }
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(SubmissionRefusal::Conflict { existing_job_id }),
            } => return Err(SubmitJobError::JobPlanConflict { existing_job_id }),
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(SubmissionRefusal::Unauthorized),
            } => return Err(SubmitJobError::AuthorityDenied),
            MetadataTransportMessage::ForwardedJobSubmission {
                result: Err(reason),
            } => {
                debug!(peer = %holder, reason = ?reason, "Family holder refused the submission");
            }
            MetadataTransportMessage::ForwardedWriteUnavailable => continue,
            other => warn!(
                peer = %holder,
                reply = crate::metadata::transport_message_kind(&other),
                "Unexpected submission forwarding reply"
            ),
        }
    }
    Err(SubmitJobError::PlacementUnavailable(
        "no job family holder accepted the submission".to_string(),
    ))
}

/// Serves one forwarded submission. The caller's token, the identity, and this
/// node's own holder standing are all revalidated before anything is committed,
/// and the request is never forwarded a second hop.
pub async fn serve_submission(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: MetadataAuthToken,
    submission_id: SubmissionId,
    request: SubmissionRequest,
) -> MetadataTransportMessage {
    let result = admit_forwarded(context, peer, auth_token, submission_id, request).await;
    MetadataTransportMessage::ForwardedJobSubmission { result }
}

async fn admit_forwarded(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: MetadataAuthToken,
    submission_id: SubmissionId,
    request: SubmissionRequest,
) -> Result<SubmissionAck, SubmissionRefusal> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(SubmissionRefusal::Unavailable);
    };
    let auth = metadata
        .authorize_write_peer(peer, Some(auth_token))
        .await
        .map_err(|error| match error {
            MetadataWritePeerError::Unauthorized => SubmissionRefusal::Unauthorized,
            MetadataWritePeerError::Unavailable(_) => SubmissionRefusal::Unavailable,
        })?;
    // The forwarded request keeps its own submitter: a relay may not re-attribute
    // a plan to another caller, and the identity is recomputed from that caller.
    if auth.user_id != request.created_by || auth.realm_id != request.created_by.realm_id {
        return Err(SubmissionRefusal::Unauthorized);
    }
    let identity = request.identity().map_err(|_| SubmissionRefusal::Invalid)?;
    if identity.submission_id != submission_id {
        return Err(SubmissionRefusal::IdentityMismatch);
    }
    let (config, local) = local_view(context)
        .await
        .map_err(|_| SubmissionRefusal::Unavailable)?;
    authorize_group(context, &auth, &request, local)
        .await
        .map_err(|_| SubmissionRefusal::Unauthorized)?;
    let view = FamilyView::resolve(&config, config.realm_id, identity.family())
        .ok_or(SubmissionRefusal::Unavailable)?;
    if !view.holds(local) {
        return Err(SubmissionRefusal::NotHolder);
    }
    match admit_here(context, &request, &identity, &config, local).await {
        Ok((job_id, created)) => Ok(SubmissionAck { job_id, created }),
        Err(SubmitJobError::JobPlanConflict { existing_job_id }) => {
            Err(SubmissionRefusal::Conflict { existing_job_id })
        }
        Err(error) => {
            warn!(error = %error, "Forwarded submission could not be admitted");
            Err(SubmissionRefusal::Unavailable)
        }
    }
}

async fn authorize_group(
    context: &DriverContext,
    auth: &AuthContext,
    request: &SubmissionRequest,
    local: NodeId,
) -> Result<(), AuthorizeError> {
    authorize(
        context,
        auth.realm_id,
        auth,
        &blob_group_permission_path(auth.realm_id, request.spec.group_id, local),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
}
