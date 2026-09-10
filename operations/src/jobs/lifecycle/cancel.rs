//! Append-only cancellation of one request family: a permission-checked holder
//! signs a token-free record; observers stop launching and active executions are
//! asked to stop. A partitioned execution may finish with `cancel_requested` set.

use aruna_core::effects::JobRecordFrame;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::structs::{
    AuthContext, CancelAuthority, JobCancelRecord, JobFamilyId, JobFamilyRecord, JobId,
    JobRecordEnvelope, JobRecordKind, LogicalJobSpec, Permission, blob_group_permission_path,
};
use aruna_core::types::NodeId;
use aruna_core::util::unix_timestamp_millis;
use tracing::{debug, warn};
use ulid::Ulid;

use super::routing::{family_of_alias, family_projection};
use super::updates::{SETTLE_RETRY_AFTER, publish_terminal, schedule_terminal_settle};
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::jobs::JobRouteError;
use crate::jobs::protocol::send_job_request;
use crate::jobs::records::verify::FamilyView;
use crate::jobs::records::{
    Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_kind_complete,
};
use crate::jobs::service::kick_drain;
use crate::jobs::store::{CancelRequestOutcome, JobMutationError, set_cancel_requested};
use crate::metadata::MetadataAuthToken;
use crate::metadata::api::load_realm_config;

/// Cancels one external job through its family. `None` means the alias names no
/// family here, so the caller keeps its ordinary local cancellation.
pub async fn cancel_family(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    auth_token: Option<MetadataAuthToken>,
) -> Option<Result<(), JobRouteError>> {
    let family = match family_of_alias(context, job_id).await {
        Ok(Some(family)) => family,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    let (projected, spec) = match family_projection(context, job_id).await {
        Ok(Some(projected)) => projected,
        Ok(None) => return None,
        Err(error) => return Some(Err(error)),
    };
    // Cancel authority is a decision: a truncated projection proves nothing.
    if let Err(error) = super::routing::decidable(&projected) {
        return Some(Err(error));
    }
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
    cancel_local_runs(context, family).await;
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

/// How this node may state the caller's permission against the stored spec. The
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
        for holder in view
            .holders()
            .iter()
            .copied()
            .filter(|holder| view.holds(*holder))
        {
            let response = match send_job_request(
                context,
                holder,
                JobRequest::Cancel {
                    auth_token: MetadataAuthToken::internal(auth.clone()),
                    job_id: spec.job_id,
                },
            )
            .await
            {
                Ok(response) => response.response,
                Err(error) => {
                    debug!(peer = %holder, error = %error, "Family cancellation forwarding failed");
                    continue;
                }
            };
            match response {
                JobResponse::Cancelled { .. } => return Ok(()),
                JobResponse::Unauthorized => return Err(JobRouteError::Unauthorized),
                JobResponse::Forbidden => return Err(JobRouteError::Forbidden),
                JobResponse::NotFound => return Err(JobRouteError::NotFound),
                JobResponse::Unavailable(error) => {
                    debug!(peer = %holder, error, "Family cancellation holder unavailable");
                }
                response => {
                    return Err(JobRouteError::Unavailable(format!(
                        "unexpected family cancel response: {response:?}"
                    )));
                }
            }
        }
        return Err(JobRouteError::Unavailable(
            "job family has no reachable holder".to_string(),
        ));
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
    // A local execution has no network cancel: `cancel_local_runs` flagged it.
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

/// Stops every physical execution of one family this node runs. The replicated
/// record reaches no local attempt by itself, so each local row is flagged here
/// and the drain is woken once.
pub(crate) async fn cancel_local_runs(context: &DriverContext, family: JobFamilyId) {
    let Some(local) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return;
    };
    let receipts = match load_kind_complete(context, family, JobRecordKind::Receipt).await {
        Ok(receipts) => receipts,
        Err(error) => {
            warn!(error = %error, "Family receipts unreadable for a local cancel");
            return;
        }
    };
    let mut flagged = false;
    for physical in local_physical_jobs(&receipts, local) {
        flagged |= flag_local_run(context, physical).await;
    }
    if flagged {
        kick_drain(context).await;
    }
}

/// Physical rows of the executions this node signed a receipt for.
fn local_physical_jobs(receipts: &[JobRecordEnvelope], local: NodeId) -> Vec<JobId> {
    receipts
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Receipt(receipt) if receipt.executor_node_id == local => {
                Some(receipt.physical_job_id)
            }
            _ => None,
        })
        .collect()
}

/// Cancels one local row whose launch committed while the family was already
/// cancelled, waking the drain so the row never starts.
pub(crate) async fn cancel_local_run(context: &DriverContext, physical: JobId) {
    if flag_local_run(context, physical).await {
        kick_drain(context).await;
    }
}

/// Requests cancellation of one local physical row. A settled or pruned row is
/// a no-op, and a repeated request leaves the stored record untouched.
async fn flag_local_run(context: &DriverContext, physical: JobId) -> bool {
    match set_cancel_requested(&context.storage_handle, physical, unix_timestamp_millis()).await {
        Ok(CancelRequestOutcome::Cancelled(record)) => {
            if !publish_terminal(context, &record).await
                && let Some(task) = context.task_handle.as_ref()
            {
                use aruna_core::handle::Handle;
                let _ = task
                    .send_effect(schedule_terminal_settle(SETTLE_RETRY_AFTER))
                    .await;
            }
            true
        }
        Ok(CancelRequestOutcome::Flagged(_)) => {
            debug!(job_id = %physical, "Local execution cancelled by its family");
            true
        }
        Ok(CancelRequestOutcome::AlreadyTerminal(_)) | Err(JobMutationError::NotFound) => false,
        Err(error) => {
            warn!(job_id = %physical, error = %error, "Local execution cancel flag failed");
            false
        }
    }
}
