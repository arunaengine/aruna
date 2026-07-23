use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::handle::Handle;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ArtifactRef, ExecutionSpec, ExportRoCrateSpec, ImportRoCrateSpec, JobId, JobPayload, JobRecord,
    JobResultPayload, JobState, RunCrateStatus, StagingJobSpec, WorkspaceMode, user_dedup_key,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{NodeId, UserId, Value};
use aruna_core::util::unix_timestamp_millis;
use bytes::Bytes;
use std::ops::Range;
use std::path::Path;
use tracing::warn;

use super::runtime::JobsRuntime;
use super::store::{
    CancelRequestOutcome, JobMutationError, find_dedup_plan, list_job_entries, list_jobs_for_user,
    read_artifact_tombstone, read_job_record, read_run_crate_status, set_cancel_requested,
};
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, schedule_job_drain_effect,
};
use super::workflow::finalize_followups;
use crate::driver::{DriverContext, drive};
use crate::metadata::repository::StorageReadError;

/// Submit a container execution job on behalf of `created_by`. The drain claims it
/// and drives the fenced external attempt lifecycle. The idempotency key is
/// namespaced per user, disjoint from internal obligation keys.
#[allow(clippy::too_many_arguments)]
pub async fn submit_execution_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    match workspace_mode {
        WorkspaceMode::None if workspace_bucket.is_some() => {
            return Err(SubmitJobError::InvalidWorkspace(
                "none mode does not accept a bucket".to_string(),
            ));
        }
        WorkspaceMode::Existing
            if workspace_bucket
                .as_deref()
                .is_none_or(|bucket| bucket.trim().is_empty()) =>
        {
            return Err(SubmitJobError::InvalidWorkspace(
                "existing mode requires a bucket".to_string(),
            ));
        }
        WorkspaceMode::Temporary | WorkspaceMode::Kept if workspace_bucket.is_some() => {
            return Err(SubmitJobError::InvalidWorkspace(
                "bucket is only valid for existing mode".to_string(),
            ));
        }
        _ => {}
    }
    if workspace_mode == WorkspaceMode::None
        && (spec
            .inputs
            .iter()
            .any(|input| input.mode != aruna_core::structs::InputMode::Mount)
            || !spec.workspace_outputs.is_empty()
            || !spec.output_prefixes.is_empty())
    {
        return Err(SubmitJobError::InvalidWorkspace(
            "none mode requires mounted inputs and explicit output destinations".to_string(),
        ));
    }
    if workspace_mode != WorkspaceMode::None
        && spec
            .inputs
            .iter()
            .any(|input| input.mode == aruna_core::structs::InputMode::Mount)
    {
        return Err(SubmitJobError::InvalidWorkspace(
            "mounted inputs require none workspace mode".to_string(),
        ));
    }
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    drive(
        SubmitJobOperation::new(SubmitJobSpec {
            payload: JobPayload::Execution(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode,
            workspace_bucket,
        }),
        context,
    )
    .await
}

pub async fn submit_staging_job(
    context: &DriverContext,
    spec: StagingJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    drive(
        SubmitJobOperation::new(SubmitJobSpec {
            payload: JobPayload::Staging(spec),
            created_by,
            owner_node_id,
            dedup_key: None,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
        }),
        context,
    )
    .await
}

pub async fn submit_rocrate_import(
    context: &DriverContext,
    spec: ImportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    let retention_ms = spec.limits.artifact_retention_ms;
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    drive(
        SubmitJobOperation::new(SubmitJobSpec {
            payload: JobPayload::ImportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
        }),
        context,
    )
    .await
}

pub async fn lookup_job_dedup(
    context: &DriverContext,
    created_by: UserId,
    idempotency_key: &str,
    plan_digest: [u8; 32],
) -> Result<Option<SubmitJobResult>, SubmitJobError> {
    let dedup_key = user_dedup_key(created_by, idempotency_key);
    let Some((job_id, existing_digest)) =
        find_dedup_plan(&context.storage_handle, created_by, &dedup_key, None)
            .await
            .map_err(SubmitJobError::UnexpectedEvent)?
    else {
        return Ok(None);
    };
    if read_job_record(&context.storage_handle, job_id, None)
        .await
        .map_err(SubmitJobError::UnexpectedEvent)?
        .is_none()
    {
        return Ok(None);
    }
    if existing_digest != plan_digest {
        return Err(SubmitJobError::JobPlanConflict {
            existing_job_id: job_id,
        });
    }
    Ok(Some(SubmitJobResult {
        job_id,
        created: false,
    }))
}

pub async fn submit_export_job(
    context: &DriverContext,
    spec: ExportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let created_by = spec.auth_context.user_id;
    let retention_ms = spec.limits.artifact_retention_ms;
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    drive(
        SubmitJobOperation::new(SubmitJobSpec {
            payload: JobPayload::ExportRoCrate(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            retention_ms,
        }),
        context,
    )
    .await
}

/// Read the run-crate obligation status surfaced alongside an execution job.
pub async fn read_job_run_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<RunCrateStatus>, String> {
    read_run_crate_status(&context.storage_handle, job_id).await
}

/// API-facing helpers so REST handlers never orchestrate storage/task effects directly.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    list_jobs_for_user(&context.storage_handle, user_id, cursor, limit, filter).await
}

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    Ok(
        match read_job_record(&context.storage_handle, job_id, None).await? {
            Some(record) if record.created_by == user_id && !record.payload.is_internal() => {
                Some(record)
            }
            _ => None,
        },
    )
}

pub enum JobReportLookup {
    NotFound,
    Pending(JobState),
    CursorConflict,
    Ready {
        record: JobRecord,
        rows: Vec<(Vec<u8>, Value)>,
        next_key: Option<Vec<u8>>,
    },
}

pub async fn read_owned_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<JobReportLookup, String> {
    let Some(record) = read_owned_job(context, user_id, job_id).await? else {
        return Ok(JobReportLookup::NotFound);
    };
    if !record.payload.is_rocrate() {
        return Ok(JobReportLookup::NotFound);
    }
    if !record.state.is_terminal() {
        return Ok(JobReportLookup::Pending(record.state));
    }
    let finished_at_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
    if finished_at_ms.saturating_add(record.retention_ms) <= unix_timestamp_millis() {
        return Ok(JobReportLookup::NotFound);
    }
    let report_digest = record
        .report_digest
        .ok_or_else(|| "terminal RO-Crate job is missing its report digest".to_string())?;
    if expected_digest.is_some_and(|expected| expected != report_digest) {
        return Ok(JobReportLookup::CursorConflict);
    }
    let (rows, next_key) =
        list_job_entries(&context.storage_handle, job_id, last_key, limit).await?;
    Ok(JobReportLookup::Ready {
        record,
        rows,
        next_key,
    })
}

pub struct OwnedArtifact {
    pub artifact: ArtifactRef,
    pub filename: String,
}

pub enum ArtifactLookup {
    NotFound,
    Pending(JobState),
    Gone,
    Ready(OwnedArtifact),
}

fn artifact_filename(document_path: Option<&str>, document_id: ulid::Ulid) -> String {
    let stem = document_path
        .and_then(|path| Path::new(path.trim_end_matches('/')).file_stem())
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| document_id.to_string());
    format!("{stem}.zip")
}

pub async fn read_owned_artifact(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
) -> Result<ArtifactLookup, String> {
    let Some(record) = read_owned_job(context, user_id, job_id).await? else {
        return Ok(
            match read_artifact_tombstone(&context.storage_handle, job_id).await? {
                Some(owner) if owner == user_id => ArtifactLookup::Gone,
                _ => ArtifactLookup::NotFound,
            },
        );
    };
    let JobPayload::ExportRoCrate(spec) = &record.payload else {
        return Ok(ArtifactLookup::NotFound);
    };
    if !record.state.is_terminal() {
        return Ok(ArtifactLookup::Pending(record.state));
    }
    let Some(JobResultPayload::ExportRoCrate(result)) = &record.result else {
        return Ok(ArtifactLookup::NotFound);
    };
    let Some(artifact) = result.artifact.clone() else {
        return Ok(ArtifactLookup::NotFound);
    };
    if artifact.expires_at_ms <= now_ms {
        return Ok(ArtifactLookup::Gone);
    }
    let document_path =
        crate::get_metadata_document::load_metadata_record_by_document(context, spec.document_id)
            .await
            .map_err(|error| match error {
                StorageReadError::Storage(error) => error.to_string(),
                StorageReadError::Conversion(error) => error.to_string(),
            })?
            .map(|record| record.document_path);
    Ok(ArtifactLookup::Ready(OwnedArtifact {
        artifact,
        filename: artifact_filename(document_path.as_deref(), spec.document_id),
    }))
}

pub struct ArtifactRead {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub stream_size: u64,
}

pub async fn read_artifact_range(
    context: &DriverContext,
    artifact: &ArtifactRef,
    range: Range<u64>,
) -> Result<ArtifactRead, String> {
    let blob_handle = context
        .blob_handle
        .as_ref()
        .ok_or_else(|| "blob handle unavailable".to_string())?;
    match blob_handle
        .send_blob_effect(BlobEffect::ReadHiddenRange {
            location: artifact.location.clone(),
            range,
        })
        .await
    {
        Event::Blob(BlobEvent::HiddenRead { blob, stream_size }) => {
            Ok(ArtifactRead { blob, stream_size })
        }
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        event => Err(format!("unexpected hidden artifact read event: {event:?}")),
    }
}

pub enum CancelJobOutcome {
    NotFound,
    AlreadyTerminal(JobRecord),
    Requested(JobRecord),
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    if read_owned_job(context, user_id, job_id).await?.is_none() {
        return Ok(CancelJobOutcome::NotFound);
    }
    // The job may be pruned between the ownership read and here; treat that as a 404
    // rather than a 500.
    let outcome = match set_cancel_requested(
        &context.storage_handle,
        job_id,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(JobMutationError::NotFound) => return Ok(CancelJobOutcome::NotFound),
        Err(error) => return Err(error.to_string()),
    };
    Ok(match outcome {
        CancelRequestOutcome::AlreadyTerminal(record) => CancelJobOutcome::AlreadyTerminal(record),
        // Already terminalized in the store transaction: wake the durable run-crate child.
        CancelRequestOutcome::Cancelled(record) => {
            if matches!(&record.payload, JobPayload::Execution(_)) {
                finalize_followups(context, job_id).await;
            }
            CancelJobOutcome::Requested(record)
        }
        CancelRequestOutcome::Flagged(record) => {
            runtime.request_cancel(job_id);
            kick_drain(context).await;
            CancelJobOutcome::Requested(record)
        }
    })
}

async fn kick_drain(context: &DriverContext) {
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_job_drain_effect()).await
    {
        warn!(message = %message, "Failed to kick job drain after cancel");
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::{insert_job, preserve_artifact_tombstone, read_job_record};
    use super::*;
    use aruna_core::structs::{
        AuthContext, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec,
        ImportRoCrateTarget, JobState, RealmId, RoCrateLimits,
    };
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    #[tokio::test]
    async fn dedup_lookup_matches() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let mut record = JobRecord::new(
            JobId::from_bytes([3u8; 16]),
            payload.clone(),
            owner,
            node_id(),
            1,
            1,
            Some(user_dedup_key(owner, "key")),
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(1);
        insert_job(&storage, &record).await.unwrap();

        let found = lookup_job_dedup(&context, owner, "key", payload.plan_digest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.job_id, record.job_id);
        assert!(!found.created);
        assert!(matches!(
            lookup_job_dedup(&context, owner, "key", [9u8; 32]).await,
            Err(SubmitJobError::JobPlanConflict { existing_job_id })
                if existing_job_id == record.job_id
        ));
    }

    #[test]
    fn artifact_uses_stem() {
        assert_eq!(
            artifact_filename(
                Some("datasets/experiment.crate"),
                Ulid::from_bytes([1u8; 16])
            ),
            "experiment.zip"
        );
    }

    #[tokio::test]
    async fn tombstone_preserves_gone() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let job_id = JobId::from_bytes([3u8; 16]);
        preserve_artifact_tombstone(&storage, job_id, owner)
            .await
            .unwrap();

        assert!(matches!(
            read_owned_artifact(&context, owner, job_id, u64::MAX).await,
            Ok(ArtifactLookup::Gone)
        ));
        assert!(matches!(
            read_owned_artifact(
                &context,
                UserId::new(Ulid::from_bytes([4u8; 16]), RealmId([1u8; 32])),
                job_id,
                u64::MAX,
            )
            .await,
            Ok(ArtifactLookup::NotFound)
        ));
    }

    #[tokio::test]
    async fn report_expiry_hides() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId([1u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
        let job_id = JobId::from_bytes([3u8; 16]);
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: owner,
                    realm_id,
                    path_restrictions: None,
                },
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([4u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: String::new(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([5u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([6u8; 16]),
            }),
            owner,
            node_id(),
            1,
            1,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(1);
        record.retention_ms = 1;
        record.report_digest = Some([7u8; 32]);
        insert_job(&storage, &record).await.unwrap();

        assert!(matches!(
            read_owned_report(&context, owner, job_id, None, None, 1).await,
            Ok(JobReportLookup::NotFound)
        ));
    }

    #[tokio::test]
    async fn internal_access_hidden() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let runtime = JobsRuntime::new();
        for (job_id, payload) in [
            (
                JobId::from_bytes([0xC3; 16]),
                JobPayload::WriteRunCrate {
                    for_job: JobId::from_bytes([0xC4; 16]),
                },
            ),
            (
                JobId::from_bytes([0xC5; 16]),
                JobPayload::TerminalCleanup {
                    for_job: JobId::from_bytes([0xC6; 16]),
                    attempt: None,
                    access_key: "access".to_string(),
                },
            ),
        ] {
            let record = JobRecord::new(job_id, payload, owner, node_id(), 1_000, 1_000, None);
            insert_job(&storage, &record).await.unwrap();

            assert!(
                read_owned_job(&context, owner, job_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                cancel_owned_job(&context, &runtime, owner, job_id)
                    .await
                    .unwrap(),
                CancelJobOutcome::NotFound
            ));
            let stored = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert!(!stored.cancel_requested);
            assert_eq!(stored.state, JobState::Queued);
        }
    }
}
