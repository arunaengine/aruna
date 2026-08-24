//! Ingress of one external submission.
//!
//! The node that takes the request normalizes it, authorizes it, derives its
//! replicated identity and family placement, and then either commits it here
//! because its own unconflicted view selects it as a holder, or forwards the
//! complete request one hop to a holder it observes. A non-holder never accepts
//! a job it could not deliver: it writes nothing and returns an availability
//! error instead.
//!
//! A user device is never an authority: it resolves nothing node-local and
//! always forwards, and the admitting holder pins the outputs to itself and
//! resolves the inputs the device only referenced.

use aruna_core::effects::JobRecordFrame;
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{JOB_FAMILY_PROJECTION_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::{
    AuthContext, ExecutionSpec, JobAdmissionRecord, JobFamilyId, JobFamilyRecord, JobId,
    JobInputFact, JobRecordEnvelope, JobRecordKind, JobRetryPolicy, LogicalJobSpec,
    LogicalJobState, OutputDestination, Permission, RealmConfigDocument, SubmissionClaim,
    SubmissionId, WorkspaceMode, blob_group_permission_path,
};
use aruna_core::types::{NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, warn};
use ulid::Ulid;

use super::admit::{
    AdmissionCandidate, AdmitSubmissionConfig, AdmitSubmissionOperation, AdmittedSubmission,
};
use super::ids::{
    RequestIdentity, SubmissionRequest, SubmissionScope, effective_resources, seal_workspace,
};
use super::witness::arm_family;
use super::{LifecycleError, ids};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::keys::{family_prefix, kind_prefix};
use crate::jobs::records::rows::ProjectionCache;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::jobs::service::{mint_local_job, validate_execution};
use crate::jobs::submit::SubmitJobError;
use crate::metadata::api::load_realm_config;
use crate::metadata::forward::{is_sync_eligible, peer_acts_for};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::{MetadataAuthToken, MetadataWritePeerError};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::PolicyRequestExtras;
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::s3::head_object::{HeadObjectInput, HeadObjectOperation};

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
    /// The family's state at this accept: `Queued` for a fresh admission, and
    /// what this holder currently reduces for a replay of a request that may
    /// already be running or finished.
    pub state: LogicalJobState,
}

/// The holder's answer to a forwarded submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionAck {
    pub job_id: JobId,
    pub created: bool,
    pub state: LogicalJobState,
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
    seal_workspace(&mut spec, workspace_mode, workspace_bucket.clone())
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    ids::required_labels(&spec)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    let (config, local) = local_view(context).await?;
    let scope = match idempotency_key {
        Some(key) => SubmissionScope::Keyed(key),
        None => SubmissionScope::Unkeyed(Ulid::generate()),
    };
    if is_device(&config, local) {
        let request = SubmissionRequest {
            created_by,
            spec,
            scope,
            retention_ms,
            ingress_node_id: local,
            input_facts: Vec::new(),
            output_policies: Vec::new(),
        };
        return forward_device(context, request, &config, local, auth_token).await;
    }
    pin_outputs(&mut spec, local);
    let (input_facts, output_policies) = resolve_facts(context, &spec, local).await?;
    let request = SubmissionRequest {
        created_by,
        spec,
        scope,
        retention_ms,
        ingress_node_id: local,
        input_facts,
        output_policies,
    };
    let identity = request.identity().map_err(SubmitJobError::Conversion)?;
    let view = family_view(&config, &identity)?;
    if view.holds(local) {
        return admit_here(context, &request, &identity, &config, local).await;
    }
    forward_once(context, &request, &identity, &view, local, auth_token).await
}

/// Whether `node_id` is a user device: it carries no shared realm
/// responsibility, holds nothing, and therefore admits nothing.
fn is_device(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    !is_sync_eligible(config, node_id)
}

/// Pins every declared output to the node that will admit the request and write
/// its results.
fn pin_outputs(spec: &mut ExecutionSpec, node_id: NodeId) {
    for output in &mut spec.file_outputs {
        output.destination_node_id = Some(node_id);
    }
    let (mode, bucket) = ids::workspace_of(spec);
    if mode == WorkspaceMode::Existing
        && let Some(bucket) = bucket
    {
        spec.resolve_outputs(&bucket, node_id);
    }
}

/// A device forwards without resolving anything: it holds none of the objects it
/// names, so only the reference shape is its to check and the holder resolves
/// the rest. Holders follow the submission id alone, so the device selects the
/// same holder set as the node that recomputes the digest after normalizing.
async fn forward_device(
    context: &DriverContext,
    request: SubmissionRequest,
    config: &RealmConfigDocument,
    local: NodeId,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AcceptedSubmission, SubmitJobError> {
    reference_shape(&request.spec)?;
    // A device may not assert an auth context for the realm, so the caller's own
    // bearer token is the only credential a holder accepts from it.
    if !matches!(auth_token, Some(MetadataAuthToken::Bearer(_))) {
        return Err(SubmitJobError::AuthorityDenied);
    }
    let identity = request.identity().map_err(SubmitJobError::Conversion)?;
    let view = family_view(config, &identity)?;
    forward_once(context, &request, &identity, &view, local, auth_token).await
}

/// Validates the shape of every input reference without requiring the object to
/// be present here. Local presence is the admitting holder's check.
fn reference_shape(spec: &ExecutionSpec) -> Result<(), SubmitJobError> {
    for input in &spec.inputs {
        let aruna_core::structs::InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        if bucket.trim().is_empty() || key.trim().is_empty() {
            return Err(SubmitJobError::InvalidWorkspace(
                "input reference needs a bucket and a key".to_string(),
            ));
        }
        if let Some(version) = version_id {
            Ulid::from_string(version)
                .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        }
    }
    Ok(())
}

/// Resolve node-local names once at ingress. The resulting facts are sealed in
/// the family spec and remain valid when admission or planning is forwarded.
async fn resolve_facts(
    context: &DriverContext,
    spec: &ExecutionSpec,
    local: NodeId,
) -> Result<
    (
        Vec<JobInputFact>,
        Vec<aruna_core::structs::PlacementPolicyRef>,
    ),
    SubmitJobError,
> {
    let mut input_facts = Vec::with_capacity(spec.inputs.len());
    for input in &spec.inputs {
        let aruna_core::structs::InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &input.source;
        let requested = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        let head = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: requested,
            }),
            context,
        )
        .await
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?
        .transpose()
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?
        .ok_or_else(|| SubmitJobError::InvalidWorkspace("input object not found".to_string()))?;
        let version = head
            .resolved_version_id
            .or(head.version_id)
            .ok_or_else(|| SubmitJobError::InvalidWorkspace("input has no version".to_string()))?;
        let blake3 = head
            .location
            .as_ref()
            .and_then(|location| location.hashes.get(HASH_BLAKE3))
            .and_then(|hash| <[u8; 32]>::try_from(hash.as_slice()).ok());
        let blake3 = match blake3 {
            Some(hash) => hash,
            None => crate::jobs::lifecycle::plan::version_hash(context, bucket, key, version)
                .await
                .ok_or_else(|| {
                    SubmitJobError::InvalidWorkspace("input is not materialized".to_string())
                })?,
        };
        let bytes = head
            .location
            .as_ref()
            .map(|location| location.blob_size)
            .or_else(|| {
                head.source_metadata
                    .as_ref()
                    .map(|metadata| metadata.content_length)
            })
            .unwrap_or_default();
        let policies =
            aruna_core::structs::PlacementPolicyRef::canonical_set(&head.source_policies)
                .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        input_facts.push(JobInputFact {
            destination_key: input.dest_key.clone(),
            source_node_id: local,
            version_id: version,
            blake3,
            bytes,
            policies,
        });
    }
    let mut buckets = std::collections::BTreeSet::new();
    for output in &spec.file_outputs {
        if output.destination_node_id != Some(local) {
            return Err(SubmitJobError::InvalidWorkspace(
                "output destination endpoint is invalid".to_string(),
            ));
        }
        let OutputDestination::S3 { bucket, .. } = &output.destination;
        buckets.insert(bucket.clone());
    }
    let (mode, workspace) = ids::workspace_of(spec);
    if mode == WorkspaceMode::Existing
        && let Some(bucket) = workspace
    {
        buckets.insert(bucket);
    }
    let mut output_policies = Vec::new();
    for bucket in buckets {
        let info = drive(GetBucketInfoOperation::new(bucket), context)
            .await
            .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?
            .transpose()
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?
            .ok_or_else(|| {
                SubmitJobError::InvalidWorkspace("output bucket not found".to_string())
            })?;
        if info.group_id != spec.group_id {
            return Err(SubmitJobError::InvalidWorkspace(
                "output bucket is outside the execution group".to_string(),
            ));
        }
        output_policies.extend(info.placement_policies);
    }
    output_policies = aruna_core::structs::PlacementPolicyRef::canonical_set(&output_policies)
        .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
    Ok((input_facts, output_policies))
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
    identity: &RequestIdentity,
) -> Result<FamilyView, SubmitJobError> {
    FamilyView::resolve(config, config.realm_id, identity.family()).ok_or_else(|| {
        SubmitJobError::PlacementUnavailable("job family placement is unavailable".to_string())
    })
}

/// Mints the alias, signs the immutable spec and its claim, and commits them.
/// The alias it answers with is the canonical one at this accept: a fresh
/// admission holds the only claim, and a replay settles on the claim the family
/// already reduces as canonical.
async fn admit_here(
    context: &DriverContext,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    config: &RealmConfigDocument,
    local: NodeId,
) -> Result<AcceptedSubmission, SubmitJobError> {
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
        ingress_node_id: request.ingress_node_id,
        realm_id: config.realm_id,
        group_id: request.spec.group_id,
        created_by: request.created_by,
        created_at_ms: now_ms,
        payload: request.spec.clone(),
        request_digest: identity.request_digest,
        spec_digest: [0u8; 32],
        resources: effective_resources(&request.spec),
        retention_ms: request.retention_ms,
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
        input_facts: request.input_facts.clone(),
        output_policies: request.output_policies.clone(),
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
    let candidate = AdmissionCandidate {
        job_id,
        spec: spec_frame,
        claim: claim_frame,
    };
    let admitted =
        admit_with_quota(context, config, local, request, identity, candidate, now_ms).await?;
    let state = match admitted.created {
        true => LogicalJobState::Queued,
        false => observed_state(context, admitted.family).await,
    };
    // Scheduling and replication are armed only after the claim is durable, so
    // a failed commit never leaves a round pointing at a family that is absent.
    arm_family(context, identity.family(), now_ms).await;
    super::outbox::kick(context).await;
    Ok(AcceptedSubmission {
        job_id: admitted.job_id,
        created: admitted.created,
        submission_id: identity.submission_id,
        state,
    })
}

/// The state this holder currently reduces for `family`. The cached projection
/// answers when it is current; otherwise the family is reduced once from its own
/// records. A family this node cannot fully reduce, including one too large to
/// project at once, is reported as indeterminate rather than as queued work.
async fn observed_state(context: &DriverContext, family: JobFamilyId) -> LogicalJobState {
    let cached = match cached_projection(context, &family).await {
        Ok(cached) => cached,
        Err(error) => {
            warn!(error = %error, "Replayed submission could not read its projection cache");
            None
        }
    };
    let projection = match cached {
        Some(cache) if !cache.stale => cache.projection,
        _ => match drive(
            ProjectFamilyOperation::new(ProjectFamilyConfig {
                family: FamilyRef::Family(family),
                now_ms: unix_timestamp_millis(),
                rebuild: false,
            }),
            context,
        )
        .await
        {
            Ok(projected) if projected.truncated => {
                warn!("Replayed submission reduced only part of its family");
                None
            }
            Ok(projected) => projected.projection,
            Err(error) => {
                warn!(error = %error, "Replayed submission could not reduce its family");
                None
            }
        },
    };
    projection.map_or(LogicalJobState::Indeterminate, |projection| {
        projection.state
    })
}

async fn cached_projection(
    context: &DriverContext,
    family: &JobFamilyId,
) -> Result<Option<ProjectionCache>, String> {
    let (page, _) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_PROJECTION_KEYSPACE,
        Some(family_prefix(family)),
        None,
        1,
        None,
    )
    .await?;
    Ok(page
        .first()
        .and_then(|(_, value)| ProjectionCache::decode(value)))
}

/// Admission transactions one submission runs before it reports the group as
/// unavailable. Only a conflict with a concurrent admission is retried.
const ADMISSION_ATTEMPTS: usize = 3;

/// Decides the standing quota and commits the admission. A replay is settled
/// from records this node already holds, so it never reads the quota view, and
/// a transaction a concurrent submission of the same group won is retried
/// instead of surfacing as an availability failure.
async fn admit_with_quota(
    context: &DriverContext,
    config: &RealmConfigDocument,
    local: NodeId,
    request: &SubmissionRequest,
    identity: &RequestIdentity,
    candidate: AdmissionCandidate,
    now_ms: u64,
) -> Result<AdmittedSubmission, SubmitJobError> {
    for attempt in 0..ADMISSION_ATTEMPTS {
        let (quota_refusal, quota_revision) = match has_claim(context, &identity.family()).await? {
            true => (None, None),
            false => crate::jobs::quota::quota_refusal(
                context,
                config,
                local,
                request.spec.group_id,
                &effective_resources(&request.spec),
            )
            .await
            .map_err(SubmitJobError::PlacementUnavailable)?,
        };
        let admitted = drive(
            AdmitSubmissionOperation::new(AdmitSubmissionConfig {
                realm_id: config.realm_id,
                local_node_id: local,
                submission_id: identity.submission_id,
                request_digest: identity.request_digest,
                candidate: Box::new(candidate.clone()),
                now_ms,
                quota_refusal,
                quota_revision,
            }),
            context,
        )
        .await;
        match admitted {
            Ok(admitted) => return Ok(admitted),
            Err(LifecycleError::Storage(StorageError::TransactionConflict))
                if attempt + 1 < ADMISSION_ATTEMPTS =>
            {
                debug!(group_id = %request.spec.group_id, "Retrying an admission a concurrent one won");
            }
            Err(error) => return Err(admission_error(error)),
        }
    }
    Err(SubmitJobError::PlacementUnavailable(
        "admission exhausted conflict retries".to_string(),
    ))
}

/// Whether this node already holds a claim for `family`, which makes the
/// admission a replay that settles inside its own transaction.
async fn has_claim(context: &DriverContext, family: &JobFamilyId) -> Result<bool, SubmitJobError> {
    let (page, _) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        JOB_FAMILY_RECORD_KEYSPACE,
        Some(kind_prefix(family, JobRecordKind::Claim)),
        None,
        1,
        None,
    )
    .await
    .map_err(SubmitJobError::PlacementUnavailable)?;
    Ok(!page.is_empty())
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
        LifecycleError::QuotaDenied(reason) => SubmitJobError::QuotaDenied(reason),
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
                    state: ack.state,
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
    mut request: SubmissionRequest,
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
    let (config, local) = local_view(context)
        .await
        .map_err(|_| SubmissionRefusal::Unavailable)?;
    // A user device is owner-bound: whatever token it presents, the submission it
    // forwards must be its owner's own.
    if !peer_acts_for(&config, peer, auth.user_id) {
        return Err(SubmissionRefusal::Unauthorized);
    }
    authorize_group(context, &auth, &request, local)
        .await
        .map_err(|_| SubmissionRefusal::Unauthorized)?;
    // The holder owns what it writes and reads, so a device submission is
    // normalized here rather than trusted from a node that holds none of it.
    let ingress = match is_device(&config, peer) {
        true => {
            pin_device_request(context, &mut request, local).await?;
            local
        }
        false => peer,
    };
    if request.input_facts.len() != request.spec.inputs.len()
        || request.input_facts.iter().any(|fact| {
            !request
                .spec
                .inputs
                .iter()
                .any(|input| input.dest_key == fact.destination_key)
        })
        || request.ingress_node_id != ingress
        || request
            .input_facts
            .iter()
            .any(|fact| fact.source_node_id != request.ingress_node_id)
        || request
            .spec
            .file_outputs
            .iter()
            .any(|output| output.destination_node_id != Some(request.ingress_node_id))
        || (ids::workspace_of(&request.spec).0 == WorkspaceMode::Existing
            && !request.spec.workspace_outputs.is_empty())
    {
        return Err(SubmissionRefusal::IdentityMismatch);
    }
    let identity = request.identity().map_err(|_| SubmissionRefusal::Invalid)?;
    if identity.submission_id != submission_id {
        return Err(SubmissionRefusal::IdentityMismatch);
    }
    let view = FamilyView::resolve(&config, config.realm_id, identity.family())
        .ok_or(SubmissionRefusal::Unavailable)?;
    if !view.holds(local) {
        return Err(SubmissionRefusal::NotHolder);
    }
    match admit_here(context, &request, &identity, &config, local).await {
        Ok(accepted) => Ok(SubmissionAck {
            job_id: accepted.job_id,
            created: accepted.created,
            state: accepted.state,
        }),
        Err(SubmitJobError::JobPlanConflict { existing_job_id }) => {
            Err(SubmissionRefusal::Conflict { existing_job_id })
        }
        Err(error) => {
            warn!(error = %error, "Forwarded submission could not be admitted");
            Err(SubmissionRefusal::Unavailable)
        }
    }
}

/// Normalizes a device submission at the holder: the outputs are pinned here and
/// the referenced inputs are resolved against this node's own objects, so the
/// recomputed identity covers names only this node assigned.
async fn pin_device_request(
    context: &Arc<DriverContext>,
    request: &mut SubmissionRequest,
    local: NodeId,
) -> Result<(), SubmissionRefusal> {
    pin_outputs(&mut request.spec, local);
    let (input_facts, output_policies) = resolve_facts(context, &request.spec, local)
        .await
        .map_err(refusal_of)?;
    request.input_facts = input_facts;
    request.output_policies = output_policies;
    request.ingress_node_id = local;
    Ok(())
}

/// A request this holder could not read is unavailable, so the device tries the
/// next holder; one it read and must refuse is definitively invalid.
fn refusal_of(error: SubmitJobError) -> SubmissionRefusal {
    match error {
        SubmitJobError::PlacementUnavailable(_) => SubmissionRefusal::Unavailable,
        _ => SubmissionRefusal::Invalid,
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::{
        InputMode, InputSelection, InputSource, OutputSelection, RealmId, WorkspaceOutput,
    };
    use aruna_core::types::Key;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    use crate::jobs::records::tests::fixture::{node, payload};

    fn family(seed: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([seed; 32]),
            request_digest: [seed; 32],
        }
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn write_claim(context: &DriverContext, realm_id: RealmId, family: JobFamilyId) {
        let job_id = JobId::from_bytes([7u8; 16]);
        let claim = SubmissionClaim {
            submission_id: family.submission_id,
            job_id,
            request_digest: family.request_digest,
            spec_digest: [0u8; 32],
            committing_node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            accepted_at_ms: 1,
        };
        let envelope = JobRecordEnvelope::sign(
            realm_id,
            JobFamilyRecord::Claim(claim),
            &iroh::SecretKey::from_bytes(&[1u8; 32]),
        )
        .expect("record signs");
        let mut key = kind_prefix(&family, JobRecordKind::Claim).to_vec();
        key.extend_from_slice(&[0u8; 40]);
        write_row(
            context,
            JOB_FAMILY_RECORD_KEYSPACE,
            Key::from(key.as_slice()),
            postcard::to_allocvec(&envelope).unwrap(),
        )
        .await;
    }

    async fn write_projection(
        context: &DriverContext,
        family: JobFamilyId,
        state: LogicalJobState,
    ) {
        let cache = ProjectionCache {
            version: crate::jobs::records::rows::PROJECTION_CACHE_VERSION,
            revision: 1,
            stale: false,
            projection: Some(aruna_core::structs::JobProjection {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                canonical_job_id: JobId::from_bytes([7u8; 16]),
                aliases: Vec::new(),
                state,
                canonical_execution_id: None,
                executions: Vec::new(),
                outputs: aruna_core::structs::OutputSet::new(Vec::new()).expect("empty outputs"),
                cancel_requested: false,
            }),
        };
        write_row(
            context,
            JOB_FAMILY_PROJECTION_KEYSPACE,
            family_prefix(&family),
            postcard::to_allocvec(&cache).unwrap(),
        )
        .await;
    }

    async fn write_row(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn replay_skips_quota() {
        // A family this node already claimed is a replay: the admission settles
        // from its own records, so no quota view is read for it.
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3u8; 32]);

        assert!(!has_claim(&context, &family(1)).await.unwrap());

        write_claim(&context, realm_id, family(1)).await;

        assert!(has_claim(&context, &family(1)).await.unwrap());
        assert!(!has_claim(&context, &family(2)).await.unwrap());
    }

    #[tokio::test]
    async fn replay_reports_state() {
        // A replay answers with the family's observed state, so a request that
        // is already running or finished is never reported as freshly queued.
        let dir = tempdir().unwrap();
        let context = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        write_claim(&context, realm_id, family(1)).await;

        write_projection(&context, family(1), LogicalJobState::Running).await;
        assert_eq!(
            observed_state(&context, family(1)).await,
            LogicalJobState::Running
        );

        write_projection(&context, family(1), LogicalJobState::Succeeded).await;
        assert_eq!(
            observed_state(&context, family(1)).await,
            LogicalJobState::Succeeded
        );

        // A family this node cannot reduce is indeterminate, never queued.
        assert_eq!(
            observed_state(&context, family(2)).await,
            LogicalJobState::Indeterminate
        );
    }

    fn s3_input(bucket: &str, key: &str, version: Option<&str>) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version.map(str::to_string),
            },
            source_node_id: None,
            dest_key: "in.txt".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }
    }

    #[test]
    fn pins_holder_outputs() {
        // The admitting holder owns every destination: a device-supplied node id
        // is overwritten and the workspace outputs resolve to the holder.
        let mut spec = payload();
        spec.file_outputs.push(OutputSelection {
            container_path: "/out/result.txt".to_string(),
            path_prefix: None,
            destination_node_id: Some(node(2)),
            destination: OutputDestination::S3 {
                bucket: "results".to_string(),
                key: "result.txt".to_string(),
            },
            name: None,
            description: None,
        });
        spec.workspace_outputs.push(WorkspaceOutput {
            container_path: "/out/extra.txt".to_string(),
            dest_key: "extra.txt".to_string(),
        });
        seal_workspace(
            &mut spec,
            WorkspaceMode::Existing,
            Some("results".to_string()),
        )
        .expect("workspace seals");

        pin_outputs(&mut spec, node(1));

        assert!(spec.workspace_outputs.is_empty());
        assert_eq!(spec.file_outputs.len(), 2);
        for output in &spec.file_outputs {
            assert_eq!(output.destination_node_id, Some(node(1)));
        }
    }

    #[test]
    fn keeps_reference_shape() {
        // A device holds none of the objects it names, so a well-formed absent
        // reference passes while a malformed one is still refused here.
        let mut spec = payload();
        spec.inputs.push(s3_input("bucket", "key", None));
        assert!(reference_shape(&spec).is_ok());

        let mut versioned = payload();
        versioned.inputs.push(s3_input(
            "bucket",
            "key",
            Some(&Ulid::generate().to_string()),
        ));
        assert!(reference_shape(&versioned).is_ok());

        let mut empty_key = payload();
        empty_key.inputs.push(s3_input("bucket", "  ", None));
        assert!(matches!(
            reference_shape(&empty_key),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));

        let mut bad_version = payload();
        bad_version
            .inputs
            .push(s3_input("bucket", "key", Some("not-a-ulid")));
        assert!(matches!(
            reference_shape(&bad_version),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    #[test]
    fn maps_holder_refusal() {
        // A holder that could not read is retried at the next holder; one that
        // read the request and refused it ends the ingress definitively.
        assert_eq!(
            refusal_of(SubmitJobError::PlacementUnavailable("offline".to_string())),
            SubmissionRefusal::Unavailable
        );
        assert_eq!(
            refusal_of(SubmitJobError::InvalidWorkspace("absent".to_string())),
            SubmissionRefusal::Invalid
        );
    }
}
