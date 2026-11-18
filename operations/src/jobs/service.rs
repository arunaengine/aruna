//! Provides the job service calls that submit, list, read, and delete jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::execution::harvest::HarvestJobSpec;
use aruna_core::structs::execution::job::{
    ArtifactRef, CopyJobSpec, ExecutionSpec, ExportRoCrateSpec, ImportRoCrateSpec, JobId,
    JobRecord, StagingJobCheckpoint, StagingJobSpec, WorkspaceMode, pid_dedup_key, user_dedup_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use aruna_core::structs::storage::storage_purge::StoragePurgeSpec;
use aruna_core::time::unix_timestamp_millis;
use std::ops::Range;
use std::sync::Arc;

pub use super::job_artifacts::{ArtifactLookup, ArtifactRead, OwnedArtifact, RoutedArtifact};
use super::job_artifacts::{ArtifactRangeOperation, ArtifactRouteOperation};
pub use super::job_cancel::{CancelJobOutcome, RoutedCancelOutcome};
use super::job_cancel::{CancelOwnedOperation, RoutedCancelOperation};
use super::job_delete::DeleteOwnedOperation;
use super::job_reads::{JobOwnerOperation, OwnedJobOperation, RecordData};
pub use super::job_reports::JobReportLookup;
use super::job_reports::{
    JobReportOperation, ReportQuery, ReportRouteOperation, SessionReasonOperation,
};
use super::job_routes::{RecordRouteOperation, StatusRouteOperation};
use super::route::{JobRouteError, RoutedJobStatus};
use super::runtime::JobsRuntime;
use super::store::query::{find_dedup_plan, list_user_jobs};
use super::store::state::read_job_record;
use super::store::{JobMutationError, RunDelete};
use super::submit::{SubmitJobError, SubmitJobResult, SubmitJobSpec};
use super::submit_execution::{ExecutionSubmission, SubmitExecutionOperation};
use super::submit_local::{
    HandedJobOperation, MintJobOperation, QueueJobOperation, SubmitLocalOperation,
};
use crate::driver::{DriverContext, drive};

/// Mints a JobId whose handle is the serving node's JobControl handle, so the
/// owner is encoded in the id itself. The bucket is a local queue shard only;
/// it never selects a remote owner.
pub(crate) async fn mint_local_job(
    context: &DriverContext,
    realm_id: RealmId,
    owner_node_id: NodeId,
    dedup_key: Option<&[u8]>,
) -> Result<JobId, SubmitJobError> {
    let serving = context.node_and_realm();
    let mint = MintJobOperation::new(realm_id, owner_node_id, dedup_key, serving);
    drive(mint, context).await
}

/// The accepting node is the immutable owner: creation, listing, dedup,
/// active-cap, and schedule rows commit in one local transaction, so a failed
/// submit leaves no ghost rows and nothing is reserved on any other node.
pub(crate) async fn submit_local_job(
    context: &DriverContext,
    spec: SubmitJobSpec,
    job_id: JobId,
) -> Result<SubmitJobResult, SubmitJobError> {
    let queue = QueueJobOperation::new(spec, job_id, context.node_and_realm());
    drive(queue, context).await
}

/// Submit a container execution job on behalf of `created_by`; the drain claims it
/// and drives the fenced external attempt lifecycle. The idempotency key is
/// user-namespaced, and `active_cap` is enforced inside the admitting transaction.
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
    active_cap: Option<u32>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submission = ExecutionSubmission {
        spec,
        created_by,
        owner_node_id,
        idempotency_key,
        workspace_mode,
        workspace_bucket,
        retention_ms,
        active_cap,
        serving: context.node_and_realm(),
        now_ms: unix_timestamp_millis(),
    };
    drive(SubmitExecutionOperation::new(submission), context).await
}

pub async fn submit_staging_job(
    context: &DriverContext,
    spec: StagingJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::staging(
        spec,
        owner_node_id,
        retention_ms,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues a staging job a range's former coordinator handed over, starting from `resume`.
pub(crate) async fn submit_handed_job(
    context: &DriverContext,
    spec: StagingJobSpec,
    resume: Option<&StagingJobCheckpoint>,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, String> {
    let handed = HandedJobOperation::new(
        spec,
        resume.cloned(),
        owner_node_id,
        retention_ms,
        (context.node_and_realm(), unix_timestamp_millis()),
    );
    drive(handed, context)
        .await
        .map_err(|error| error.to_string())
}

/// Queues one background object copy on this node for the caller in `spec`.
pub async fn submit_copy_job(
    context: &DriverContext,
    spec: CopyJobSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::copy(
        spec,
        owner_node_id,
        retention_ms,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues one run of a backend import on this node. A scheduled run passes its due
/// time in `dedup`, so a repeated timer never starts the same run twice.
pub async fn submit_import_job(
    context: &DriverContext,
    spec: BackendImportSpec,
    owner_node_id: NodeId,
    dedup: Option<String>,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::backend_import(
        spec,
        owner_node_id,
        (dedup, retention_ms),
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues one run of a harvest source for `created_by`. Provenance keeps a rerun idempotent.
pub async fn submit_harvest_job(
    context: &DriverContext,
    spec: HarvestJobSpec,
    created_by: UserId,
    realm_id: RealmId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::harvest(
        spec,
        (created_by, realm_id),
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues a purge; with `files` it also deletes the linked-storage files of the versions.
pub async fn submit_purge_job(
    context: &DriverContext,
    (spec, files): (StoragePurgeSpec, bool),
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::purge(
        (spec, files),
        owner_node_id,
        (idempotency_key, retention_ms),
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Register a w3id PID for a document as a fenced job on the document's PID
/// authority. The dedup key names the document without the submitting user, so a
/// concurrent re-mint by another user joins the same job across ingress nodes.
pub async fn submit_mint_pid(
    context: &Arc<DriverContext>,
    spec: MintPersistentSpec,
    local_node_id: NodeId,
    retention_ms: u64,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let (job_id, created) = crate::metadata::persistent_id::forward::submit_pid_routed(
        context,
        spec.document_id,
        spec.minted_by,
        local_node_id,
        retention_ms,
        auth_token,
    )
    .await
    .map_err(pid_submit_error)?;
    Ok(SubmitJobResult { job_id, created })
}

fn pid_submit_error(error: crate::metadata::api::MetadataApiError) -> SubmitJobError {
    use crate::metadata::api::MetadataApiError;
    match error {
        MetadataApiError::NotFound => SubmitJobError::DocumentMissing,
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            SubmitJobError::AuthorityDenied
        }
        error => SubmitJobError::PlacementUnavailable(error.to_string()),
    }
}

/// The authority's own submission: mints the job id against this node's
/// job-control binding, so the dedup row and the execution share one owner.
pub(crate) async fn submit_mint_local(
    context: &DriverContext,
    spec: MintPersistentSpec,
    owner_node_id: NodeId,
    retention_ms: u64,
) -> Result<SubmitJobResult, SubmitJobError> {
    let realm_id = context.serving_realm(spec.minted_by.realm_id);
    let submit = SubmitLocalOperation::mint_pid(
        spec,
        realm_id,
        owner_node_id,
        retention_ms,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Queues identifier registration on this node. The key names the calling job, so a replayed
/// step joins the job it already queued.
pub(crate) async fn submit_identifiers(
    context: &DriverContext,
    spec: aruna_core::structs::secondary_id::RegisterIdentifiersSpec,
    owner_node_id: NodeId,
    key: String,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::identifiers(
        spec,
        owner_node_id,
        key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

pub async fn submit_rocrate_import(
    context: &DriverContext,
    spec: ImportRoCrateSpec,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submit = SubmitLocalOperation::rocrate_import(
        spec,
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// The job and plan digest a dedup key names, if that job's record still exists.
pub(crate) async fn find_dedup_job(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
) -> Result<Option<(JobId, [u8; 32])>, SubmitJobError> {
    let Some((job_id, digest)) = find_dedup_plan(&context, created_by, dedup_key, None)
        .await
        .map_err(SubmitJobError::UnexpectedEvent)?
    else {
        return Ok(None);
    };
    let record = read_job_record(&context, job_id, None)
        .await
        .map_err(SubmitJobError::UnexpectedEvent)?;
    Ok(record.map(|_| (job_id, digest)))
}

pub async fn lookup_job_dedup(
    context: &DriverContext,
    created_by: UserId,
    idempotency_key: &str,
    plan_digest: [u8; 32],
) -> Result<Option<SubmitJobResult>, SubmitJobError> {
    let dedup_key = user_dedup_key(created_by, idempotency_key);
    let Some((job_id, existing_digest)) = find_dedup_job(context, created_by, &dedup_key).await?
    else {
        return Ok(None);
    };
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
    let submit = SubmitLocalOperation::rocrate_export(
        spec,
        owner_node_id,
        idempotency_key,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(submit, context).await
}

/// Node-local listing: returns only jobs owned by the serving node (every job
/// the user submitted through it). There is no realm-wide aggregation, so jobs
/// owned by other nodes are omitted; listings are per-origin by contract.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool + Send,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    list_user_jobs(context, user_id, cursor, limit, filter).await
}

/// Removes the caller's finished run from this node's lists.
pub async fn delete_owned_run(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Result<RunDelete, JobMutationError> {
    let serving = context.node_and_realm();
    let delete = DeleteOwnedOperation::new(auth, job_id, serving, unix_timestamp_millis());
    drive(delete, context).await
}

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    drive(OwnedJobOperation::new(user_id, job_id), context)
        .await
        .map_err(|error| error.0)
}

/// Full owner record for API projections that the status view cannot rebuild off-owner.
pub async fn read_record_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<Option<JobRecord>, JobRouteError> {
    Ok(read_staging_routed(context, user_id, job_id, auth_token)
        .await?
        .map(|(record, _)| record))
}

/// Returns a staging record and its checkpoint from the immutable owner.
pub async fn read_staging_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<Option<RecordData>, JobRouteError> {
    let route = RecordRouteOperation::new(
        user_id,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

/// Derives the immutable owner from the JobId alone using replicated placement state.
pub async fn resolve_job_owner(
    context: &DriverContext,
    job_id: JobId,
) -> Result<NodeId, JobRouteError> {
    drive(
        JobOwnerOperation::new(job_id, context.node_and_realm()),
        context,
    )
    .await
}

pub async fn read_job_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<RoutedJobStatus, JobRouteError> {
    let route = StatusRouteOperation::new(
        auth,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

pub async fn read_owned_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<JobReportLookup, String> {
    let query = ReportQuery {
        expected_digest,
        last_key,
        limit,
    };
    let read = JobReportOperation::owned(user_id, job_id, query, unix_timestamp_millis());
    drive(read, context).await.map_err(|error| error.0)
}

/// Why a finished session stopped; a failed read reads as no reason.
pub async fn read_session_reason(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Option<String> {
    drive(SessionReasonOperation::new(user_id, job_id), context)
        .await
        .ok()
        .flatten()
}

pub async fn read_report_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
    last_key: Option<Vec<u8>>,
    limit: usize,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<JobReportLookup, JobRouteError> {
    let query = ReportQuery {
        expected_digest,
        last_key,
        limit,
    };
    let route = ReportRouteOperation::new(
        auth,
        job_id,
        query,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    drive(route, context).await
}

pub async fn read_artifact_routed(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<Range<u64>>,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<RoutedArtifact, JobRouteError> {
    let route = ArtifactRouteOperation::new(
        user_id,
        job_id,
        now_ms,
        range,
        auth_token,
        context.node_and_realm(),
    );
    drive(route, context).await
}

pub async fn read_artifact_range(
    context: &DriverContext,
    artifact: &ArtifactRef,
    range: Range<u64>,
) -> Result<ArtifactRead, String> {
    drive(ArtifactRangeOperation::new(artifact, range), context)
        .await
        .map_err(|error| error.0)
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    let cancel = CancelOwnedOperation::new(user_id, job_id, unix_timestamp_millis());
    let report = drive(cancel, context).await.map_err(|error| error.0)?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

/// Cancels a job on the node that answers for it and signals the attempts running here.
pub async fn cancel_job_routed(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
    auth_token: Option<aruna_core::metadata::AuthToken>,
) -> Result<RoutedCancelOutcome, JobRouteError> {
    let cancel = RoutedCancelOperation::new(
        user_id,
        job_id,
        auth_token,
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    let report = drive(cancel, context).await?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

#[cfg(test)]
mod tests {
    use super::super::store::state::{preserve_artifact_tombstone, read_job_record};
    use super::*;
    use crate::jobs::job_artifacts::OwnedArtifactOperation;
    use crate::jobs::store::query::insert_job;
    use crate::jobs::submit_execution::validate_execution;
    use aruna_core::calls::{IoCall, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::execution::job::{
        ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
        JobPayload, JobState, RoCrateLimits,
    };
    use aruna_core::structs::execution::job::{MAX_EXECUTION_OUTPUTS, OutputDestination};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    fn submission(serving: Option<(NodeId, RealmId)>) -> ExecutionSubmission {
        let realm_id = RealmId([1u8; 32]);
        ExecutionSubmission {
            spec: ExecutionSpec {
                group_id: Ulid::from_bytes([3u8; 16]),
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: Vec::new(),
                workdir: None,
                env: Default::default(),
                resources: Default::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            },
            created_by: UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
            owner_node_id: node_id(),
            idempotency_key: None,
            workspace_mode: WorkspaceMode::None,
            workspace_bucket: None,
            retention_ms: 1,
            active_cap: None,
            serving,
            now_ms: 1,
        }
    }

    // An unplaced node submits at once; a placed node reads its realm config first and refuses
    // to own a job for another node.
    #[test]
    fn mints_from_placement() {
        let mut unplaced = SubmitExecutionOperation::new(submission(None));
        assert!(matches!(unplaced.first(), Ok(Next::Call(IoCall::Child(_)))));
        let realm_id = RealmId([1u8; 32]);
        let mut placed = SubmitExecutionOperation::new(submission(Some((node_id(), realm_id))));
        assert!(matches!(placed.first(), Ok(Next::Call(IoCall::Child(_)))));
        let other = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let mut foreign = SubmitExecutionOperation::new(submission(Some((other, realm_id))));
        assert!(matches!(
            foreign.first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    #[tokio::test]
    async fn caps_declared_outputs() {
        // The declaration bound is the immutable record's bound, so a valid
        // local success can never be impossible to publish.
        let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId([1u8; 32]);
        let mut spec = ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: (0..=MAX_EXECUTION_OUTPUTS)
                .map(
                    |index| aruna_core::structs::execution::job::WorkspaceOutput {
                        container_path: format!("/out/{index}"),
                        dest_key: format!("out/{index}"),
                    },
                )
                .collect(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };

        let error = submit_execution_job(
            &context,
            spec.clone(),
            UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
            node_id(),
            None,
            WorkspaceMode::Existing,
            Some("shared".to_string()),
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            SubmitJobError::TooManyOutputs {
                limit: MAX_EXECUTION_OUTPUTS
            }
        ));

        spec.workspace_outputs.pop();
        spec.output_prefixes.push("out/".to_string());
        assert!(matches!(
            validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
        spec.output_prefixes.clear();
        assert!(!matches!(
            submit_execution_job(
                &context,
                spec,
                UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
                node_id(),
                None,
                WorkspaceMode::Existing,
                Some("shared".to_string()),
                1,
                None,
            )
            .await,
            Err(SubmitJobError::TooManyOutputs { .. })
        ));
    }

    #[test]
    fn refuses_pathless_input() {
        // Nothing copies an input into a bucket, so it must land in the container.
        let mut spec = ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: vec![aruna_core::structs::execution::job::InputSelection {
                source: aruna_core::structs::execution::job::InputSource::S3 {
                    bucket: "data".to_string(),
                    key: "in.txt".to_string(),
                    version_id: None,
                },
                source_node_id: None,
                dest_key: "in.txt".to_string(),
                mode: aruna_core::structs::execution::job::InputMode::Snapshot,
                container_path: None,
                name: None,
                description: None,
            }],
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };

        let error = validate_execution(&mut spec, WorkspaceMode::None, None).unwrap_err();

        assert!(
            matches!(error, SubmitJobError::InvalidWorkspace(message) if message.contains("in.txt"))
        );
        spec.inputs[0].container_path = Some("/in.txt".to_string());
        assert!(validate_execution(&mut spec, WorkspaceMode::None, None).is_ok());
    }

    #[test]
    fn refuses_shared_destination() {
        // An explicit output inside the workspace bucket and a workspace output
        // naming the same key would reserve one version twice.
        let mut spec = ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: vec![aruna_core::structs::execution::job::OutputSelection {
                container_path: "/out/result.txt".to_string(),
                path_prefix: None,
                destination: OutputDestination::S3 {
                    bucket: "shared".to_string(),
                    key: "result.txt".to_string(),
                },
                destination_node_id: None,
                name: None,
                description: None,
            }],
            workspace_outputs: vec![aruna_core::structs::execution::job::WorkspaceOutput {
                container_path: "/result.txt".to_string(),
                dest_key: "result.txt".to_string(),
            }],
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };

        let error =
            validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).unwrap_err();

        assert!(
            matches!(error, SubmitJobError::InvalidWorkspace(message) if message.contains("shared/result.txt"))
        );
        spec.workspace_outputs[0].dest_key = "other.txt".to_string();
        assert!(validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).is_ok());

        // Nextflow sends every glob with the task directory as its url.
        let mut glob = spec.file_outputs[0].clone();
        glob.container_path = "/out/*.html".to_string();
        glob.path_prefix = Some("/out".to_string());
        spec.file_outputs = vec![glob.clone(), glob];
        spec.file_outputs[1].container_path = "/out/*.zip".to_string();
        assert!(validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).is_ok());
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
        insert_job(&context, &record).await.unwrap();

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

    #[tokio::test]
    async fn tombstone_preserves_gone() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let job_id = JobId::from_bytes([3u8; 16]);
        preserve_artifact_tombstone(&context, job_id, owner, 10)
            .await
            .unwrap();

        assert!(matches!(
            drive(OwnedArtifactOperation::new(owner, job_id, 9), &context).await,
            Ok(ArtifactLookup::Gone)
        ));
        assert!(matches!(
            drive(
                OwnedArtifactOperation::new(
                    UserId::new(Ulid::from_bytes([4u8; 16]), RealmId([1u8; 32])),
                    job_id,
                    9,
                ),
                &context,
            )
            .await,
            Ok(ArtifactLookup::NotFound)
        ));
        assert!(matches!(
            drive(OwnedArtifactOperation::new(owner, job_id, 10), &context).await,
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
                    session: None,
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
        insert_job(&context, &record).await.unwrap();

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
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
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
            insert_job(&context, &record).await.unwrap();

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
            let stored = read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert!(!stored.cancel_requested);
            assert_eq!(stored.state, JobState::Queued);
        }
    }
}
