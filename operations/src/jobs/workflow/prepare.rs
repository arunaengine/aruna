use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_core::compute::runtimes::SESSION_SOCKET_PATH;
use aruna_core::compute::{
    AttemptRef, NetworkAccess, ResourceRequest, S3Mount, Secret, SecurityContext, StagingMode,
    TaskInput, TaskSpec, UserSpec, WorkspaceBinding,
};
use aruna_core::id::NodeId;
use aruna_core::structs::{ExecutionSpec, InputMode, JobError, JobRecord, WorkspaceMode};

use super::DEFAULT_WALLTIME;
use crate::driver::DriverContext;
use crate::jobs::lifecycle::ids::session_of;
use crate::jobs::workflow::workspace::{
    check_workspace_bucket, ensure_group_write, load_direct_inputs, mint_input_credential,
    mint_workspace_credential, pinned_inputs, prepare_mounts,
};

pub(super) const NETWORK_TAG_KEY: &str = "aruna-engine.org/network";

/// Authorize the bucket an `Existing`-mode run works inside before anything is
/// staged. A `None`-mode run touches no bucket, so it has nothing to authorize.
pub(super) async fn prepare_workspace(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<(), JobError> {
    if record.workspace_mode != WorkspaceMode::Existing {
        return Ok(());
    }
    Box::pin(ensure_group_write(context, spec, record, node_id)).await?;
    Box::pin(check_workspace_bucket(
        context, spec, record, node_id, bucket,
    ))
    .await
}

pub(super) struct PreparedTask {
    pub(super) inputs: Vec<TaskInput>,
    pub(super) mounts: Vec<S3Mount>,
    pub(super) secrets: BTreeMap<String, Secret>,
    pub(super) staging: StagingMode,
    /// Set for a session only: the bucket and endpoint its credential reaches.
    pub(super) workspace: Option<WorkspaceBinding>,
}

/// The bucket a run works inside, empty when it owns none.
pub(super) fn job_bucket(record: &JobRecord) -> String {
    record.workspace_bucket.clone().unwrap_or_default()
}

/// True when every input is an unpinned mount, the only shape an S3 mount can
/// deliver: a mount serves the current head of the object it names.
fn mountable(spec: &ExecutionSpec) -> bool {
    !spec.inputs.is_empty()
        && spec
            .inputs
            .iter()
            .all(|input| input.mode == InputMode::Mount)
        && !pinned_inputs(spec)
}

/// Build the attempt's inputs. Every input is read straight from its source,
/// except an unpinned mounted spec, which the container reaches over S3.
/// Nothing is copied for an attempt, so an adopted one restages the same way.
pub(super) async fn prepare_inputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
) -> Result<PreparedTask, JobError> {
    if !mountable(spec) {
        return Ok(PreparedTask {
            inputs: Box::pin(load_direct_inputs(context, spec, record, node_id)).await?,
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: StagingMode::Files,
            workspace: None,
        });
    }
    let mounts = Box::pin(prepare_mounts(context, spec, record, node_id)).await?;
    let mut secrets = BTreeMap::new();
    if !mounts.is_empty() {
        let buckets = mounts
            .iter()
            .map(|mount| mount.bucket.clone())
            .collect::<BTreeSet<_>>();
        let credential = Box::pin(mint_input_credential(
            context, spec, record, node_id, &buckets,
        ))
        .await?;
        secrets.insert(
            "access_key_id".to_string(),
            Secret::new(credential.access_key),
        );
        secrets.insert(
            "secret_access_key".to_string(),
            Secret::new(credential.secret),
        );
    }
    Ok(PreparedTask {
        inputs: Vec::new(),
        mounts,
        secrets,
        staging: StagingMode::S3Mount,
        workspace: None,
    })
}

pub(super) async fn prepare_task(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<PreparedTask, JobError> {
    Box::pin(prepare_workspace(context, spec, record, node_id, bucket)).await?;
    let mut prepared = Box::pin(prepare_inputs(context, spec, record, node_id)).await?;
    if session_of(spec).is_some() {
        Box::pin(add_session_credential(
            context,
            spec,
            record,
            node_id,
            bucket,
            &mut prepared,
        ))
        .await?;
    }
    Ok(prepared)
}

/// A session reads and writes its workspace bucket over S3 and nothing else.
/// The credential is revoked with the job's terminal cleanup.
async fn add_session_credential(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
    prepared: &mut PreparedTask,
) -> Result<(), JobError> {
    if bucket.is_empty() {
        return Err(JobError::permanent("a session needs a workspace bucket"));
    }
    let endpoint = context
        .compute_handle
        .as_ref()
        .and_then(|registry| {
            let workspace = registry.workspace_endpoint();
            workspace
                .session_endpoint
                .clone()
                .or_else(|| workspace.endpoint.clone())
        })
        .ok_or_else(|| JobError::permanent("a session needs a container-reachable S3 endpoint"))?;
    let region = context
        .compute_handle
        .as_ref()
        .map(|registry| registry.workspace_endpoint().region.clone())
        .unwrap_or_default();
    let credential = Box::pin(mint_workspace_credential(
        context, spec, record, node_id, bucket,
    ))
    .await?;
    prepared.secrets.insert(
        "AWS_ACCESS_KEY_ID".to_string(),
        Secret::new(credential.access_key),
    );
    prepared.secrets.insert(
        "AWS_SECRET_ACCESS_KEY".to_string(),
        Secret::new(credential.secret),
    );
    prepared.workspace = Some(WorkspaceBinding {
        s3_endpoint: endpoint,
        bucket_name: bucket.to_string(),
        region,
    });
    Ok(())
}

pub(super) fn build_task_spec(
    spec: &ExecutionSpec,
    attempt: &AttemptRef,
    pinned_image: &str,
    prepared: PreparedTask,
    run_as: UserSpec,
) -> TaskSpec {
    let PreparedTask {
        inputs,
        mounts,
        secrets,
        staging,
        workspace,
    } = prepared;
    let resources = ResourceRequest {
        cpu_cores: spec.resources.cpu_cores,
        ram_bytes: spec.resources.ram_bytes,
        disk_bytes: spec.resources.disk_bytes,
        max_walltime: spec
            .resources
            .max_walltime_ms
            .map(Duration::from_millis)
            .or(Some(DEFAULT_WALLTIME)),
        preemptible: spec.resources.preemptible,
        backend_extensions: std::collections::BTreeMap::new(),
    };
    let session = session_of(spec);
    let mut env = spec.env.clone();
    if let (Some(_), Some(workdir)) = (&session, spec.workdir.as_deref()) {
        env.insert(
            "ARUNA_SESSION_SOCKET".to_string(),
            format!("{}/{SESSION_SOCKET_PATH}", workdir.trim_end_matches('/')),
        );
    }
    TaskSpec {
        attempt: attempt.clone(),
        image: pinned_image.to_string(),
        entrypoint: spec.entrypoint.clone(),
        command: spec.command.clone(),
        workdir: spec.workdir.clone(),
        env,
        secret_env: secrets,
        resources,
        workspace,
        security: SecurityContext {
            run_as,
            network: if spec
                .tags
                .get(NETWORK_TAG_KEY)
                .is_some_and(|value| value == "open")
            {
                NetworkAccess::Open
            } else {
                NetworkAccess::Isolated
            },
            ..Default::default()
        },
        log_limits: Default::default(),
        session: session.is_some(),
        session_mount: session.and_then(|session| session.mount),
        inputs,
        s3_mounts: mounts,
        staging_mode: staging,
        output_paths: spec
            .file_outputs
            .iter()
            .map(|output| output.container_path.clone())
            .collect(),
    }
}
