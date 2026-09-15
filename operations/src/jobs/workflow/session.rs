use std::str::FromStr;
use std::sync::Arc;

use aruna_compute::ExecutorBackend;
use aruna_compute::session::{EndReason, Session, SessionConfig};
use aruna_core::compute::FenceContext;
use aruna_core::structs::placement::compute_config::IDLE_AFTER_MS;
use aruna_core::structs::execution::job::{
    ExecutionSpec, JobId, JobRecord, JobResultPayload, SessionReportDetail, SessionReportRow,
};
use tracing::warn;

use super::super::store::{put_job_entry, read_job_record};
use super::workspace::mint_workspace_credential;
use crate::driver::DriverContext;
use crate::jobs::lifecycle::ids::session_of;
use crate::jobs::lifecycle::reservation::job_reservation;

/// Registers the interactive session of a session job. `None` is an ordinary
/// run, or a node with no compute plane.
pub(super) async fn start_session(
    context: &Arc<DriverContext>,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    job_id: JobId,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Option<Arc<Session>> {
    let requested = session_of(spec)?;
    let public_job_id = job_reservation(context, job_id)
        .await
        .ok()?
        .map_or(job_id, |reservation| reservation.logical_job_id);
    let registry = context.compute_handle.as_ref()?.sessions().clone();
    let net = context.net_handle.as_ref()?;
    let node_id = net.node_id();
    // The credential was minted while the task was prepared; this reads the
    // same one back, so the client sees when it really expires.
    let credential_expires_ms =
        match read_job_record(&context.storage_handle, job_id, None).await {
            Ok(Some(record)) => Box::pin(mint_workspace_credential(
                context, spec, &record, node_id, bucket,
            ))
            .await
            .map(|credential| credential.expires_at_ms)
            .unwrap_or_default(),
            _ => 0,
        };
    let realm_idle = realm_session_idle(context).await;
    let idle_after_ms = requested
        .idle_after_ms
        .map_or(realm_idle, |asked| asked.min(realm_idle));
    Some(registry.open(
        SessionConfig {
            job_id: job_id.to_string(),
            public_job_id: public_job_id.to_string(),
            runtime: requested.runtime,
            workspace_bucket: bucket.to_string(),
            executor_node_id: node_id.to_string(),
            idle_after_ms,
            credential_expires_ms,
        },
        backend.clone(),
        fence.clone(),
    ))
}

/// The realm's idle wait, which the request may only shorten.
async fn realm_session_idle(context: &DriverContext) -> u64 {
    let Some(net) = context.net_handle.as_ref() else {
        return IDLE_AFTER_MS;
    };
    crate::metadata::api::load_realm_config(context, *net.realm_id())
        .await
        .map(|config| config.compute.session_idle_ms)
        .filter(|value| *value > 0)
        .unwrap_or(IDLE_AFTER_MS)
}

/// The stored record of a queued copy, when its id parses and it is readable.
async fn finished_copy(storage: &aruna_storage::StorageHandle, job_id: &str) -> Option<JobRecord> {
    let job_id = JobId::from_str(job_id).ok()?;
    read_job_record(storage, job_id, None).await.ok().flatten()
}

/// Lists what the session brought into its workspace bucket and why it stopped.
/// Cell traffic is never recorded: the family log is capped per family.
pub(super) async fn write_session_report(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    session: Option<&Arc<Session>>,
    reason: EndReason,
) {
    let mut rows = Vec::new();
    if let Some(session) = session {
        let inventory = session.inventory();
        let mut next_input = inventory.len();
        for (index, input) in inventory.into_iter().enumerate() {
            rows.push(SessionReportRow {
                entry_key: format!("input/{index:04}"),
                detail: SessionReportDetail::Input {
                    dest_key: input.dest_key,
                    bytes: input.bytes,
                    blake3: input.blake3,
                    source_node_id: input.source_node_id,
                    version_id: input.version_id,
                },
            });
        }
        // A queued copy that finished counts like an inline one. One still
        // running never reached the kernel, so it is left out.
        for pending in session.pending() {
            let Some(record) = finished_copy(storage, &pending.job_id).await else {
                continue;
            };
            let Some(JobResultPayload::CopyObject {
                version_id,
                bytes,
                blake3,
            }) = record.result
            else {
                continue;
            };
            rows.push(SessionReportRow {
                entry_key: format!("input/{next_input:04}"),
                detail: SessionReportDetail::Input {
                    dest_key: pending.dest_key,
                    bytes,
                    blake3,
                    source_node_id: pending.source_node_id,
                    version_id,
                },
            });
            next_input += 1;
        }
        for (index, object) in session.touched().into_iter().enumerate() {
            rows.push(SessionReportRow {
                entry_key: format!("touched/{index:04}"),
                detail: SessionReportDetail::Touched {
                    bucket: object.bucket,
                    key: object.key,
                    operation: object.operation,
                },
            });
        }
    }
    rows.push(SessionReportRow {
        entry_key: "end".to_string(),
        detail: SessionReportDetail::End {
            reason: reason.as_str().to_string(),
        },
    });
    for row in rows {
        if let Err(error) =
            put_job_entry(storage, job_id, token, row.entry_key.as_bytes(), &row).await
        {
            warn!(job_id = %job_id, error = %error, "Session report write failed");
            return;
        }
    }
}
