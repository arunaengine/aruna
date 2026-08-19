//! Append-only cancellation of one request family.
//!
//! Cancelling is a replicated intent, not a global stop: a holder that checked
//! the caller's permission against the sealed spec signs a token-free record,
//! every holder that observes it stops launching, and known active executions
//! are asked to stop. A partitioned execution may still finish, and its late
//! success is projected with `cancel_requested` set.

use aruna_core::effects::JobRecordFrame;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::structs::{
    AuthContext, CancelAuthority, JobCancelRecord, JobFamilyRecord, JobId, JobRecordEnvelope,
    LogicalJobSpec, Permission, blob_group_permission_path,
};
use aruna_core::util::unix_timestamp_millis;
use tracing::{debug, warn};
use ulid::Ulid;

use super::routing::{family_of_alias, family_projection};
use crate::driver::{DriverContext, drive};
use crate::jobs::JobRouteError;
use crate::jobs::protocol::send_job_request;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::metadata::MetadataAuthToken;
use crate::metadata::api::load_realm_config;
use crate::request_authorization::authorize;
use crate::request_policy::PolicyRequestExtras;

/// Cancels one external job through its family. `None` means the alias names no
/// family here, so the caller keeps its ordinary local cancellation.
pub async fn cancel_family(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    auth_token: Option<MetadataAuthToken>,
) -> Option<Result<(), JobRouteError>> {
    match family_of_alias(context, job_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    }
    let (projected, spec) = match family_projection(context, job_id).await {
        Ok(Some(projected)) => projected,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    let Some(projection) = projected.projection else {
        return Some(Err(JobRouteError::Unavailable(
            "job family has no projection".to_string(),
        )));
    };
    let authority = match cancel_authority(context, auth, &spec).await {
        Some(authority) => authority,
        None => return Some(Err(JobRouteError::Forbidden)),
    };
    if let Err(error) = publish_cancel(context, &spec, auth, authority).await {
        return Some(Err(error));
    }
    // Every known active execution is asked to stop; a partitioned one may
    // still finish and stays visible as a late completion.
    if let Some(auth_token) = auth_token {
        for execution in projection
            .executions
            .iter()
            .filter(|execution| !execution.state.is_terminal())
        {
            stop_execution(context, execution.executor_node_id, job_id, &auth_token).await;
        }
    }
    Some(Ok(()))
}

/// How this node may state the caller's permission against the sealed spec. The
/// submitter is checked by every holder again; a group admin's permission is
/// checked here and the signature is that statement.
async fn cancel_authority(
    context: &DriverContext,
    auth: &AuthContext,
    spec: &LogicalJobSpec,
) -> Option<CancelAuthority> {
    if auth.user_id == spec.created_by {
        return Some(CancelAuthority::Submitter);
    }
    let local = context.net_handle.as_ref()?.node_id();
    authorize(
        context,
        spec.realm_id,
        auth,
        &blob_group_permission_path(spec.realm_id, spec.group_id, local),
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    .ok()
    .map(|()| CancelAuthority::GroupAdmin)
}

/// Signs and appends the cancellation. Only a current family holder may author
/// it, so a node that does not hold the family reports availability instead.
async fn publish_cancel(
    context: &DriverContext,
    spec: &LogicalJobSpec,
    auth: &AuthContext,
    authority: CancelAuthority,
) -> Result<(), JobRouteError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| JobRouteError::Unavailable("network handle unavailable".to_string()))?;
    let realm_id = *net.realm_id();
    let local = net.node_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or_else(|| JobRouteError::Unavailable("realm config unavailable".to_string()))?;
    let family = aruna_core::structs::JobFamilyId {
        submission_id: spec.submission_id,
        request_digest: spec.request_digest,
    };
    let view = FamilyView::resolve(&config, realm_id, family).ok_or_else(|| {
        JobRouteError::Unavailable("job family holder view unavailable".to_string())
    })?;
    if !view.holds(local) {
        let holder = view
            .holders()
            .iter()
            .copied()
            .find(|holder| view.holds(*holder))
            .ok_or_else(|| {
                JobRouteError::Unavailable("job family has no current holder".to_string())
            })?;
        return match send_job_request(
            context,
            holder,
            JobRequest::Cancel {
                auth_token: MetadataAuthToken::internal(auth.clone()),
                job_id: spec.job_id,
            },
        )
        .await?
        .response
        {
            JobResponse::Cancelled { .. } => Ok(()),
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            JobResponse::Unavailable(error) => Err(JobRouteError::Unavailable(error)),
            response => Err(JobRouteError::Unavailable(format!(
                "unexpected family cancel response: {response:?}"
            ))),
        };
    }
    let record = JobCancelRecord {
        cancel_id: Ulid::generate(),
        submission_id: spec.submission_id,
        request_digest: spec.request_digest,
        job_id: spec.job_id,
        spec_digest: spec.spec_digest,
        requested_by: auth.user_id,
        authority,
        requested_at_ms: unix_timestamp_millis(),
    };
    let envelope = JobRecordEnvelope::signed_with(
        realm_id,
        JobFamilyRecord::Cancel(record),
        local,
        |message| net.sign(message),
    )
    .map_err(|error| JobRouteError::Internal(error.to_string()))?;
    let frame = JobRecordFrame::new(envelope)
        .map_err(|error| JobRouteError::Internal(error.to_string()))?;
    let outcome = drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id: local,
            record: frame,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: unix_timestamp_millis(),
        }),
        context,
    )
    .await
    .map_err(|error| JobRouteError::Unavailable(error.to_string()))?;
    if !matches!(
        outcome.admission,
        Admission::Authentic | Admission::Duplicate
    ) {
        return Err(JobRouteError::Unavailable(
            "job cancellation is awaiting authentic admission".to_string(),
        ));
    }
    debug!(job_id = %spec.job_id, "Job cancellation published");
    Ok(())
}

/// Asks one executor to stop its physical execution. Delivery is best effort:
/// an unreachable executor keeps running and converges through the record.
async fn stop_execution(
    context: &DriverContext,
    executor: aruna_core::types::NodeId,
    job_id: JobId,
    auth_token: &MetadataAuthToken,
) {
    if context
        .net_handle
        .as_ref()
        .is_some_and(|net| net.node_id() == executor)
    {
        return;
    }
    let request = JobRequest::Cancel {
        auth_token: auth_token.clone(),
        job_id,
    };
    if let Err(error) = send_job_request(context, executor, request).await {
        warn!(peer = %executor, error = %error, "Cancel delivery to an executor failed");
    }
}
