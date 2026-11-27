//! Validates one execution request, mints its job id and submits it as a job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::valid_vault_env;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::family::MAX_EXECUTION_OUTPUTS;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::{
    ExecutionSpec, InputSource, JobPayload, MAX_INLINE_BYTES, OutputDestination, WORKFLOW_RUN_TAG,
    WORKFLOW_SECRETS_TAG, parse_workflow_secrets, plugin_tag,
};
use aruna_core::structs::execution::job::local::{LocalAdmission, WorkspaceMode, user_dedup_key};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::{
    DEFAULT_SHARD_COUNT, FIRST_GRANTABLE_HANDLE, shard_for_subject,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::user::id::UserId;
use tracing::warn;

use super::{SubmitJobOperation, SubmitJobSpec};
use aruna_core::job_request::{SubmitJobError, SubmitJobResult, mint_configured_job, mint_job_id};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};

/// Normalizes one execution request and enforces every bound that holds
/// regardless of where the job runs: composition, the shared output bound, and
/// the workspace rules. The single gate both submission paths pass through.
pub(crate) fn validate_execution(
    spec: &mut ExecutionSpec,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<&str>,
) -> Result<(), SubmitJobError> {
    spec.inputs = aruna_core::structs::execution::job::input::plan_composition(
        spec.inputs.clone(),
        spec.collision_policy,
    )?;
    // Nothing copies an input into a bucket any more, so one that names no
    // container path would reach nobody.
    if let Some(input) = spec
        .inputs
        .iter()
        .find(|input| input.container_path.is_none())
    {
        return Err(SubmitJobError::InvalidWorkspace(format!(
            "input {} needs a container path",
            input.dest_key
        )));
    }
    // Only the plugin host and workflow runs write bytes into a spec, and only a few.
    let inline = (spec.inputs.iter()).filter_map(|input| match &input.source {
        InputSource::Inline { bytes } => Some(bytes.len()),
        InputSource::S3 { .. } => None,
    });
    let (count, bytes) = inline.fold((0, 0usize), |(count, sum), len| (count + 1, sum + len));
    let tagged = plugin_tag(&spec.tags).is_some() || spec.tags.contains_key(WORKFLOW_RUN_TAG);
    if (count > 0 && !tagged) || bytes > MAX_INLINE_BYTES {
        return Err(SubmitJobError::InvalidWorkspace(
            "inline inputs are for plugin and workflow tasks only, at most 64 KiB together"
                .to_string(),
        ));
    }
    // Only a workflow run's tasks write into a group's run area.
    let run_area = (spec.file_outputs.iter())
        .any(|output| matches!(output.destination, OutputDestination::RunArea { .. }));
    if run_area && !spec.tags.contains_key(WORKFLOW_RUN_TAG) {
        return Err(SubmitJobError::InvalidWorkspace(
            "run area outputs are for workflow tasks only".to_string(),
        ));
    }
    if spec.tags.contains_key(WORKFLOW_SECRETS_TAG) {
        if !spec.tags.contains_key(WORKFLOW_RUN_TAG) {
            return Err(SubmitJobError::InvalidWorkspace(
                "workflow secret bindings are for workflow tasks only".to_string(),
            ));
        }
        let bindings = parse_workflow_secrets(&spec.tags).map_err(|_| {
            SubmitJobError::InvalidWorkspace("workflow secret bindings are malformed".to_string())
        })?;
        for name in bindings.keys() {
            if !valid_vault_env(name) || spec.env.contains_key(name) {
                return Err(SubmitJobError::InvalidWorkspace(format!(
                    "`{name}` cannot carry a released secret"
                )));
            }
        }
    }
    // One bound governs declaration, expansion, the local result, and the
    // immutable output record, so a valid success is always publishable.
    if spec.file_outputs.len() + spec.workspace_outputs.len() > MAX_EXECUTION_OUTPUTS {
        return Err(SubmitJobError::TooManyOutputs {
            limit: MAX_EXECUTION_OUTPUTS,
        });
    }
    if spec.file_outputs.is_empty() && !spec.output_prefixes.is_empty() {
        return Err(SubmitJobError::InvalidWorkspace(
            "output prefixes require declared file outputs".to_string(),
        ));
    }
    match workspace_mode {
        WorkspaceMode::None if workspace_bucket.is_some() => {
            return Err(SubmitJobError::InvalidWorkspace(
                "none mode does not accept a bucket".to_string(),
            ));
        }
        WorkspaceMode::Existing
            if workspace_bucket.is_none_or(|bucket| bucket.trim().is_empty()) =>
        {
            return Err(SubmitJobError::InvalidWorkspace(
                "existing mode requires a bucket".to_string(),
            ));
        }
        _ => {}
    }
    // Without a bucket of its own a run has nowhere to resolve a relative
    // output key against, so every output must name its own destination.
    if workspace_mode == WorkspaceMode::None
        && (!spec.workspace_outputs.is_empty() || !spec.output_prefixes.is_empty())
    {
        return Err(SubmitJobError::InvalidWorkspace(
            "none mode requires explicit output destinations".to_string(),
        ));
    }
    // An explicit destination and a workspace output resolve into one bucket
    // and key namespace, so a key both name would share one reserved version.
    let mut destinations = HashSet::new();
    let workspace = spec
        .workspace_outputs
        .iter()
        .filter_map(|output| workspace_bucket.map(|bucket| (bucket, output.dest_key.as_str())));
    // A glob's url is the directory its matches are written below, so globs may share it.
    let explicit = spec
        .file_outputs
        .iter()
        .filter(|output| !aruna_core::compute::has_wildcard(&output.container_path))
        .map(|output| match &output.destination {
            OutputDestination::S3 { bucket, key } => (bucket.as_str(), key.as_str()),
            OutputDestination::RunArea { prefix } => ("run area", prefix.as_str()),
        });
    for (bucket, key) in explicit.chain(workspace) {
        if !destinations.insert((bucket, key)) {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "output {bucket}/{key} is declared twice"
            )));
        }
    }
    Ok(())
}

/// A job id for a node without a net transport, in the first grantable handle.
pub(crate) fn mint_unplaced(
    subject: &[u8],
    ids: &mut IdMint,
    now_ms: u64,
) -> Result<JobId, SubmitJobError> {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    let shard = shard_for_subject(subject, DEFAULT_SHARD_COUNT);
    let bucket = BucketId::new(shard as u16)
        .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
    Ok(mint_job_id(handle, bucket, ids, now_ms)?)
}

/// One execution job request, with the node that serves it.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionSubmission {
    pub spec: ExecutionSpec,
    pub created_by: UserId,
    pub owner_node_id: NodeId,
    pub idempotency_key: Option<String>,
    pub workspace_mode: WorkspaceMode,
    pub workspace_bucket: Option<String>,
    pub retention_ms: u64,
    pub active_cap: Option<u32>,
    /// This node and the realm it serves; `None` on a node without a net transport.
    pub serving: Option<(NodeId, RealmId)>,
    pub now_ms: u64,
    /// The backend values a device admitted the run under; `None` on any other node.
    pub local_admission: Option<LocalAdmission>,
}

/// Validates an execution request, mints its job id from the serving node's placement, and
/// submits it as a child [`SubmitJobOperation`].
#[derive(Debug, PartialEq)]
pub struct SubmitExecutionOperation {
    submission: ExecutionSubmission,
    dedup_key: Option<Vec<u8>>,
    /// The placement subject of a job without a dedup key.
    entropy: [u8; 16],
    ids: IdMint,
    minted: bool,
}

impl SubmitExecutionOperation {
    pub fn new(submission: ExecutionSubmission) -> Self {
        let dedup_key = submission
            .idempotency_key
            .as_ref()
            .map(|key| user_dedup_key(submission.created_by, key));
        Self {
            submission,
            dedup_key,
            entropy: [0; 16],
            ids: IdMint::default(),
            minted: false,
        }
    }

    fn subject(&self) -> &[u8] {
        self.dedup_key.as_deref().unwrap_or(&self.entropy)
    }

    fn submit(&mut self, job_id: JobId) -> Result<Next<SubmitJobResult>, SubmitJobError> {
        self.minted = true;
        let submission = &self.submission;
        let spec = SubmitJobSpec {
            payload: JobPayload::Execution(submission.spec.clone()),
            created_by: submission.created_by,
            owner_node_id: submission.owner_node_id,
            dedup_key: self.dedup_key.clone(),
            now_ms: submission.now_ms,
            retention_ms: submission.retention_ms,
            workspace_mode: submission.workspace_mode,
            workspace_bucket: submission.workspace_bucket.clone(),
            active_cap: submission.active_cap,
        };
        let admission = submission.local_admission.clone();
        let build =
            move || SubmitJobOperation::new(spec.clone(), job_id).with_admission(admission.clone());
        Ok(Next::Call(IoCall::retry_with(build, &job_id.to_bytes())))
    }
}

impl Operation for SubmitExecutionOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn seed(&mut self, entropy: u128) {
        self.entropy = entropy.to_be_bytes();
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> Result<Next<SubmitJobResult>, SubmitJobError> {
        let submission = &mut self.submission;
        validate_execution(
            &mut submission.spec,
            submission.workspace_mode,
            submission.workspace_bucket.as_deref(),
        )?;
        let (serving, owner) = (submission.serving, submission.owner_node_id);
        let Some((node_id, realm_id)) = serving else {
            let subject = self.subject().to_vec();
            let job_id = mint_unplaced(&subject, &mut self.ids, self.submission.now_ms)?;
            return self.submit(job_id);
        };
        if node_id != owner {
            return Err(SubmitJobError::PlacementUnavailable(
                "job owner does not match the serving node".to_string(),
            ));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SubmitJobResult>, SubmitJobError> {
        if self.minted {
            return Ok(Next::Done(
                result.child::<SubmitJobResult, SubmitJobError>()??,
            ));
        }
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|error| {
                warn!(error = %error, "realm config unavailable; querying the local replica only");
                SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
            })?;
        let (subject, owner) = (self.subject().to_vec(), self.submission.owner_node_id);
        let now_ms = self.submission.now_ms;
        let job_id = mint_configured_job(&config, owner, &subject, &mut self.ids, now_ms)?;
        self.submit(job_id)
    }
}
