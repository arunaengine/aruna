//! Prepares one execution run: authorizes the bucket, mounts inputs, builds the task spec.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_compute::executor::BackendCaps;
use aruna_core::compute::runtimes::SESSION_SOCKET_PATH;
use aruna_core::compute::{
    AttemptRef, ExecutorKind, NetworkAccess, ResourceRequest, S3Mount, Secret, SecurityContext,
    StagingMode, TaskInput, TaskSpec, UserSpec, WorkspaceBinding,
};
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{ExecutionSpec, JobError, JobRecord, WorkspaceMode};

use super::DEFAULT_WALLTIME;
use crate::driver::DriverContext;
use crate::jobs::lifecycle::ids::session_of;
use crate::jobs::workflow::input_view::{cached_keys, view_bucket, view_entries};
use crate::jobs::workflow::workspace::{
    check_workspace_bucket, ensure_group_write, load_direct_inputs, mint_input_credential,
    mint_workspace_credential,
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
    /// Set for a session or a batch run inside its bucket: the bucket and endpoint its
    /// credential reaches.
    pub(super) workspace: Option<WorkspaceBinding>,
}

/// The bucket a run works inside, empty when it owns none.
pub(super) fn job_bucket(record: &JobRecord) -> String {
    record.workspace_bucket.clone().unwrap_or_default()
}

/// The capabilities of the backend this run is placed on.
fn backend_caps(context: &DriverContext, spec: &ExecutionSpec) -> Option<BackendCaps> {
    let kind = spec
        .executor_constraint
        .as_deref()
        .map(ExecutorKind::from_wire);
    context
        .compute_handle
        .as_ref()
        .zip(kind)
        .and_then(|(registry, kind)| registry.get(&kind).map(|backend| backend.capabilities()))
}

/// Whether the backend this run is placed on can mount inputs.
fn backend_mounts(context: &DriverContext, spec: &ExecutionSpec) -> bool {
    backend_caps(context, spec).is_some_and(|caps| caps.s3_mount)
}

/// Whether a batch run inside its bucket gets a session's S3 access: its backend gives
/// sessions S3 and this node names an endpoint for it.
fn workspace_access(context: &DriverContext, spec: &ExecutionSpec, record: &JobRecord) -> bool {
    let endpoint = context.compute_handle.as_ref().is_some_and(|registry| {
        let workspace = registry.workspace_endpoint();
        workspace.session_endpoint.is_some() || workspace.endpoint.is_some()
    });
    record.workspace_mode == WorkspaceMode::Existing
        && endpoint
        && backend_caps(context, spec).is_some_and(|caps| caps.session)
}

/// The input view a run mounts, when its backend mounts and any input is mounted.
pub(super) fn mounted_view(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
) -> Option<String> {
    let mounts = backend_mounts(context, spec)
        && view_entries(record).is_ok_and(|entries| !entries.is_empty());
    mounts.then(|| view_bucket(record.job_id))
}

/// Build the attempt's inputs. Mounted inputs are read through the run's input view at their
/// exact versions; every other input is read straight from its source into the workspace.
/// Nothing is copied for an attempt, so an adopted one restages the same way.
async fn prepare_inputs(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    mint: bool,
) -> Result<PreparedTask, JobError> {
    let entries = match backend_mounts(context, spec) {
        true => view_entries(record).unwrap_or_default(),
        false => Vec::new(),
    };
    let mounted: BTreeSet<String> = entries
        .iter()
        .map(|entry| entry.input.dest_key.clone())
        .collect();
    let inputs = Box::pin(load_direct_inputs(context, spec, record, node_id, &mounted)).await?;
    if entries.is_empty() {
        return Ok(PreparedTask {
            inputs,
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: StagingMode::Files,
            workspace: None,
        });
    }
    let view = view_bucket(record.job_id);
    let cached = Box::pin(cached_keys(context, &entries, node_id))
        .await
        .map_err(|error| JobError::retryable(error.to_string()))?;
    let mounts = entries
        .iter()
        .map(|entry| S3Mount {
            bucket: view.clone(),
            key: entry.key.clone(),
            path: entry.input.container_path.clone().unwrap_or_default(),
            cached: cached.contains(&entry.key),
        })
        .collect();
    let mut secrets = BTreeMap::new();
    // A session's one credential carries the view as well, so it is minted there instead.
    if mint {
        let buckets = BTreeSet::from([view]);
        let credential = Box::pin(mint_input_credential(
            context, spec, record, node_id, &buckets,
        ))
        .await?;
        insert_mount_secrets(&mut secrets, credential.access_key, credential.secret);
    }
    Ok(PreparedTask {
        inputs,
        mounts,
        secrets,
        staging: StagingMode::S3Mount,
        workspace: None,
    })
}

/// The names the mount driver reads the run credential under.
fn insert_mount_secrets(
    secrets: &mut BTreeMap<String, Secret>,
    access_key: String,
    secret: String,
) {
    secrets.insert("access_key_id".to_string(), Secret::new(access_key));
    secrets.insert("secret_access_key".to_string(), Secret::new(secret));
}

pub(super) async fn prepare_task(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<PreparedTask, JobError> {
    Box::pin(prepare_workspace(context, spec, record, node_id, bucket)).await?;
    Box::pin(prepare_attempt(context, spec, record, node_id, bucket)).await
}

/// The inputs and credentials of one attempt, for its first submit and for every recovery,
/// so a resubmit carries the very credential the first submit minted.
pub(super) async fn prepare_attempt(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    bucket: &str,
) -> Result<PreparedTask, JobError> {
    let session = session_of(spec).is_some() || workspace_access(context, spec, record);
    let mut prepared = Box::pin(prepare_inputs(context, spec, record, node_id, !session)).await?;
    if session {
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

/// A session, or a batch run inside its bucket, reads and writes that bucket over S3 and
/// nothing else. The credential is revoked with the job's terminal cleanup.
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
    let view = mounted_view(context, spec, record).filter(|_| !prepared.mounts.is_empty());
    let credential = Box::pin(mint_workspace_credential(
        context,
        spec,
        record,
        node_id,
        (bucket, view.as_deref()),
    ))
    .await?;
    if view.is_some() {
        let (access_key, secret) = (credential.access_key.clone(), credential.secret.clone());
        insert_mount_secrets(&mut prepared.secrets, access_key, secret);
    }
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
