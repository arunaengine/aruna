//! Decides owner-initiated local runs on a user device and reads their state as calls.
//! Only the owner's work runs here; staging reads the realm-held inputs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::ResourceEnvelope;
use aruna_core::id::NodeId;
use aruna_core::node_state::{DrainState, DrainStateError, DrainStateOperation};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ExecutionSpec, InputMode, InputSource, JobPayload, JobRecord, JobState, WorkspaceMode,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::store::list_jobs::{ListJobsError, ListJobsOperation};
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::jobs::submit_execution::{ExecutionSubmission, SubmitExecutionOperation};
use crate::realm::mutate_placement::node_kind;
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};

/// Unfinished runs one status scan counts before it stops. A device queues its
/// owner's work, not a realm's, so the count is bounded instead of paged.
const MAX_RUN_SCAN: usize = 64;

/// One owner-initiated run of the local executor.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalExecutionConfig {
    pub spec: ExecutionSpec,
    /// The user this device is enrolled for; every local run is theirs.
    pub owner: UserId,
    /// This device, which owns the job it is about to accept.
    pub node_id: NodeId,
    pub idempotency_key: Option<String>,
    pub workspace_mode: WorkspaceMode,
    pub retention_ms: u64,
}

/// Why a device refused a local run.
#[derive(Debug, Error, PartialEq)]
pub enum LocalExecutionError {
    #[error("this node is not a user device")]
    NotADevice,
    #[error("this device runs jobs for its owner only")]
    NotOwner,
    #[error("this device's compute plane is paused")]
    Paused,
    #[error("this device has no compute backend for the request")]
    NoExecutor,
    #[error("this device does not run the request: {0}")]
    Unsupported(&'static str),
    #[error("input {bucket}/{key} is not on this device")]
    InputNotLocal { bucket: String, key: String },
    /// The request itself asks for a copy this device may never make.
    #[error("input {bucket}/{key} cannot be copied onto this device: {reason}")]
    InputRefused {
        bucket: String,
        key: String,
        reason: String,
    },
    #[error(transparent)]
    Submit(#[from] SubmitJobError),
    #[error("device state unavailable: {0}")]
    Unavailable(String),
}

impl From<IoError> for LocalExecutionError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

fn unavailable(error: impl std::fmt::Display) -> LocalExecutionError {
    LocalExecutionError::Unavailable(error.to_string())
}

#[derive(Debug, PartialEq)]
enum RunStep {
    Config,
    Drain,
    /// The head of the input at `index`, requested at `version` when the run named one.
    Input {
        index: usize,
        version: Option<Ulid>,
    },
    Submit,
}

/// What an accepted local run answers.
pub(super) type RunOutput = SubmitJobResult;

type RunTurn = Result<Next<SubmitJobResult>, LocalExecutionError>;

/// Accepts one local run. Refusals are decided before the job exists; the admitting transaction
/// bounds concurrent submissions. `limits` are the ceilings of the backend the request would run
/// on: a device without one refuses the run instead of queueing work nothing will claim.
#[derive(Debug, PartialEq)]
pub(super) struct LocalRunOperation {
    config: LocalExecutionConfig,
    limits: Option<ResourceEnvelope>,
    serving: Option<(NodeId, RealmId)>,
    now_ms: u64,
    step: RunStep,
}

impl LocalRunOperation {
    pub(super) fn new(
        config: LocalExecutionConfig,
        limits: Option<ResourceEnvelope>,
        serving: Option<(NodeId, RealmId)>,
    ) -> Self {
        Self {
            config,
            limits,
            serving,
            now_ms: 0,
            step: RunStep::Config,
        }
    }

    fn check_owner(&mut self, realm: &RealmConfigDocument) -> RunTurn {
        let owner = node_kind(realm, self.config.node_id)
            .and_then(|kind| kind.owner())
            .ok_or(LocalExecutionError::NotADevice)?;
        if owner != self.config.owner {
            return Err(LocalExecutionError::NotOwner);
        }
        self.step = RunStep::Drain;
        Ok(Next::Call(IoCall::child(DrainStateOperation::new())))
    }

    /// Refuses at submit time what staging could only discover mid-run: other-node inputs are
    /// fetched by exact version, local ones must be readable and open inputs are pinned to the
    /// version staging will read.
    fn next_input(&mut self, start: usize) -> RunTurn {
        let node_id = self.config.node_id;
        for index in start..self.config.spec.inputs.len() {
            let input = &self.config.spec.inputs[index];
            let remote = input.source_node_id.filter(|source| *source != node_id);
            let InputSource::S3 {
                bucket,
                key,
                version_id,
            } = &input.source;
            if remote.is_some() {
                remote_version(bucket, key, version_id.as_deref())?;
                continue;
            }
            let version = (version_id.as_deref().map(Ulid::from_string))
                .transpose()
                .map_err(|_| missing_input(bucket, key))?;
            let head = HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version,
            });
            self.step = RunStep::Input { index, version };
            return Ok(Next::Call(IoCall::child(head)));
        }
        self.submit()
    }

    /// Pins the input to the exact local version the run will read, from the head when the
    /// request named none.
    fn pin_input(&mut self, index: usize, version: Option<Ulid>, result: IoResult) -> RunTurn {
        let head = result.child::<HeadObjectResult, HeadObjectError>()?;
        let input = (self.config.spec.inputs.get_mut(index)).ok_or(IoError::Unfinished)?;
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = &mut input.source;
        let pinned = match head {
            Ok(head) => head.resolved_version_id.or(head.version_id).or(version),
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => None,
            Err(error) => return Err(unavailable(error)),
        };
        let pinned = pinned.ok_or_else(|| missing_input(bucket, key))?;
        *version_id = Some(pinned.to_string());
        self.next_input(index + 1)
    }

    fn submit(&mut self) -> RunTurn {
        let limits = self
            .limits
            .as_ref()
            .ok_or(LocalExecutionError::NoExecutor)?;
        let config = &self.config;
        let mut spec = config.spec.clone();
        pin_outputs(&mut spec, config.node_id);
        let submit = SubmitExecutionOperation::new(ExecutionSubmission {
            spec,
            created_by: config.owner,
            owner_node_id: config.node_id,
            idempotency_key: config.idempotency_key.clone(),
            workspace_mode: config.workspace_mode,
            workspace_bucket: None,
            retention_ms: config.retention_ms,
            active_cap: limits.max_concurrent,
            serving: self.serving,
            now_ms: self.now_ms,
        });
        self.step = RunStep::Submit;
        Ok(Next::Call(IoCall::child(submit)))
    }
}

impl Operation for LocalRunOperation {
    type Output = SubmitJobResult;
    type Error = LocalExecutionError;

    fn first(&mut self) -> RunTurn {
        let realm_id = self.config.owner.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> RunTurn {
        match self.step {
            RunStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(realm) => self.check_owner(&realm),
                Err(error) => {
                    warn!(%error, "realm config unavailable for a local run");
                    Err(unavailable("realm configuration"))
                }
            },
            RunStep::Drain => {
                let drain = result.child::<DrainState, DrainStateError>()?;
                if drain.map_err(unavailable)?.operator_draining {
                    return Err(LocalExecutionError::Paused);
                }
                local_staging(&self.config.spec, self.config.workspace_mode)?;
                if self.limits.is_none() {
                    return Err(LocalExecutionError::NoExecutor);
                }
                self.next_input(0)
            }
            RunStep::Input { index, version } => self.pin_input(index, version, result),
            RunStep::Submit => {
                let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
                Ok(Next::Done(submitted))
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// Pins every declared output to this device, the only node that will ever
/// write them.
fn pin_outputs(spec: &mut ExecutionSpec, node_id: NodeId) {
    for output in &mut spec.file_outputs {
        output.destination_node_id = Some(node_id);
    }
}

/// This device's compute plane as the owner's tools see it.
#[derive(Debug, Clone, PartialEq)]
pub struct ComputeStatus {
    pub enabled: bool,
    /// Wire kind of the backend a local run would select.
    pub backend: Option<String>,
    pub healthy: bool,
    /// A paused plane accepts no local run at all.
    pub paused: bool,
    pub limits: ResourceEnvelope,
    pub running: u32,
    pub queued: u32,
    /// Why a local run would be refused right now, if it would be.
    pub message: Option<String>,
}

/// What the selected backend reports about itself.
pub(super) struct BackendState {
    pub kind: Option<String>,
    pub limits: ResourceEnvelope,
    pub failure: Option<String>,
}

pub(super) fn status_of(plane: BackendState, runs: LocalRuns) -> ComputeStatus {
    let enabled = plane.kind.is_some();
    ComputeStatus {
        enabled,
        backend: plane.kind,
        healthy: enabled && plane.failure.is_none(),
        paused: runs.paused,
        limits: plane.limits,
        running: runs.running,
        queued: runs.queued,
        message: match runs.paused {
            true => Some(LocalExecutionError::Paused.to_string()),
            false => plane.failure,
        },
    }
}

/// The owner's runs this device has not finished, and whether its plane is paused.
#[derive(Debug, PartialEq)]
pub(super) struct LocalRuns {
    running: u32,
    queued: u32,
    paused: bool,
}

fn unfinished_run(record: &JobRecord) -> bool {
    matches!(record.payload, JobPayload::Execution(_)) && !record.state.is_terminal()
}

/// Counts the owner's unfinished runs from durable job records, stopping at [`MAX_RUN_SCAN`],
/// then reads the operator drain.
#[derive(Debug, PartialEq)]
pub(super) struct LocalRunsOperation {
    owner: UserId,
    counted: Option<(u32, u32)>,
}

impl LocalRunsOperation {
    pub(super) fn new(owner: UserId) -> Self {
        Self {
            owner,
            counted: None,
        }
    }
}

impl Operation for LocalRunsOperation {
    type Output = LocalRuns;
    type Error = LocalExecutionError;

    fn first(&mut self) -> Result<Next<LocalRuns>, LocalExecutionError> {
        let filter: fn(&JobRecord) -> bool = unfinished_run;
        let list = ListJobsOperation::new(self.owner, None, MAX_RUN_SCAN, filter);
        Ok(Next::Call(IoCall::child(list)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LocalRuns>, LocalExecutionError> {
        let Some((running, queued)) = self.counted else {
            let page = result.child::<(Vec<JobRecord>, Option<Vec<u8>>), ListJobsError>()?;
            let (records, _) = page.map_err(unavailable)?;
            let queued = (records.iter())
                .filter(|record| record.state == JobState::Queued)
                .count();
            self.counted = Some(((records.len() - queued) as u32, queued as u32));
            return Ok(Next::Call(IoCall::child(DrainStateOperation::new())));
        };
        let drain = result.child::<DrainState, DrainStateError>()?;
        Ok(Next::Done(LocalRuns {
            running,
            queued,
            paused: drain.map_err(unavailable)?.operator_draining,
        }))
    }
}

/// A device stages every input as a file and forwards no bucket name: a mount
/// needs an S3 endpoint it does not expose, and it names no workspace bucket.
fn local_staging(spec: &ExecutionSpec, mode: WorkspaceMode) -> Result<(), LocalExecutionError> {
    if mode == WorkspaceMode::Existing {
        return Err(LocalExecutionError::Unsupported(
            "a local run does not accept a workspace bucket",
        ));
    }
    if spec
        .inputs
        .iter()
        .any(|input| input.mode == InputMode::Mount)
    {
        return Err(LocalExecutionError::Unsupported(
            "a mounted input needs an S3 endpoint a device does not expose",
        ));
    }
    Ok(())
}

/// A realm input is fetched by exact version or not at all: a device plans
/// nothing, so the request is the only thing that can name the version.
fn remote_version(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(), LocalExecutionError> {
    let refuse = |reason: &str| LocalExecutionError::InputRefused {
        bucket: bucket.to_string(),
        key: key.to_string(),
        reason: reason.to_string(),
    };
    match version_id.map(Ulid::from_string) {
        Some(Ok(_)) => Ok(()),
        Some(Err(_)) => Err(refuse("the version is not a version id")),
        None => Err(refuse("a realm input needs an exact version")),
    }
}

fn missing_input(bucket: &str, key: &str) -> LocalExecutionError {
    LocalExecutionError::InputNotLocal {
        bucket: bucket.to_string(),
        key: key.to_string(),
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::tests::records::payload;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::InputSelection;
    use aruna_core::structs::identity::realm::RealmNodeKind;

    pub(in crate::device) const REALM: RealmId = RealmId([3u8; 32]);

    pub(in crate::device) fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    pub(in crate::device) fn owner(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), REALM)
    }

    pub(in crate::device) fn realm_config(
        local: NodeId,
        kind: RealmNodeKind,
    ) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
        config.seed_default_placement();
        config.ensure_node(local, kind);
        config
    }

    pub(in crate::device) fn run_config(local: NodeId, caller: UserId) -> LocalExecutionConfig {
        LocalExecutionConfig {
            spec: payload(),
            owner: caller,
            node_id: local,
            idempotency_key: None,
            workspace_mode: WorkspaceMode::None,
            retention_ms: 60_000,
        }
    }

    pub(in crate::device) fn input(
        bucket: &str,
        source: Option<NodeId>,
        version: Option<Ulid>,
    ) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: bucket.to_string(),
                key: "reads.fastq".to_string(),
                version_id: version.map(|version| version.to_string()),
            },
            source_node_id: source,
            dest_key: "reads.fastq".to_string(),
            mode: InputMode::Snapshot,
            container_path: Some("/inputs/reads.fastq".to_string()),
            name: None,
            description: None,
        }
    }

    #[test]
    fn refuses_foreign_staging() {
        // A device has no S3 endpoint to mount through and names no bucket.
        let mut spec = payload();
        assert!(matches!(
            local_staging(&spec, WorkspaceMode::Existing),
            Err(LocalExecutionError::Unsupported(_))
        ));

        spec.inputs.push(input("project", None, None));
        spec.inputs[0].mode = InputMode::Mount;
        assert!(matches!(
            local_staging(&spec, WorkspaceMode::None),
            Err(LocalExecutionError::Unsupported(_))
        ));

        spec.inputs[0].mode = InputMode::Snapshot;
        assert!(local_staging(&spec, WorkspaceMode::None).is_ok());
    }

    fn local_run(config: LocalExecutionConfig) -> LocalRunOperation {
        LocalRunOperation::new(config, Some(ResourceEnvelope::default()), None)
    }

    fn outcome<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn drain(operator_draining: bool) -> IoResult {
        outcome::<_, DrainStateError>(Ok(DrainState {
            operator_draining,
            departure: None,
        }))
    }

    fn head(version: Option<Ulid>) -> HeadObjectInput {
        HeadObjectInput {
            bucket: "project".to_string(),
            key: "reads.fastq".to_string(),
            version_id: version,
        }
    }

    #[test]
    fn run_checks_device() {
        let (local, caller) = (node(1), owner(2));
        let device = realm_config(local, RealmNodeKind::User { owner: caller });
        let config = |result| outcome::<RealmConfigDocument, GetConfigError>(result);
        let mut run = local_run(run_config(local, caller));
        let read = IoCall::child(GetConfigOperation::new(REALM));
        assert_eq!(run.first(), Ok(Next::Call(read)));
        let drain_read = IoCall::child(DrainStateOperation::new());
        let answer = run.next(config(Ok(device.clone())));
        assert_eq!(answer, Ok(Next::Call(drain_read)));
        // A server has no owner, another caller is not the owner, no config is no answer.
        let server = realm_config(local, RealmNodeKind::Server);
        let answer = local_run(run_config(local, caller)).next(config(Ok(server)));
        assert_eq!(answer, Err(LocalExecutionError::NotADevice));
        let answer = local_run(run_config(local, owner(3))).next(config(Ok(device)));
        assert_eq!(answer, Err(LocalExecutionError::NotOwner));
        let answer = (local_run(run_config(local, caller)))
            .next(config(Err(GetConfigError::DocumentNotFound)));
        let unavailable = LocalExecutionError::Unavailable("realm configuration".to_string());
        assert_eq!(answer, Err(unavailable));
    }

    #[test]
    fn run_stops_early() {
        let drained = |limits| {
            let config = run_config(node(1), owner(2));
            let mut run = LocalRunOperation::new(config, limits, None);
            run.step = RunStep::Drain;
            run
        };
        let limits = Some(ResourceEnvelope::default());
        let answer = drained(limits).next(drain(true));
        assert_eq!(answer, Err(LocalExecutionError::Paused));
        let answer = drained(None).next(drain(false));
        assert_eq!(answer, Err(LocalExecutionError::NoExecutor));
    }

    #[test]
    fn run_pins_inputs() {
        let version = Ulid::from_bytes([21; 16]);
        let mut config = run_config(node(1), owner(2));
        config.spec.inputs.push(input("project", None, None));
        let mut run = local_run(config);
        run.stamp(7);
        run.step = RunStep::Drain;
        let call = IoCall::child(HeadObjectOperation::new(head(None)));
        assert_eq!(run.next(drain(false)), Ok(Next::Call(call)));
        let found = HeadObjectResult {
            resolved_version_id: Some(version),
            ..head_result()
        };
        let answer = run.next(outcome::<_, HeadObjectError>(Ok(found)));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(run.step, RunStep::Submit);
        let InputSource::S3 { version_id, .. } = &run.config.spec.inputs[0].source;
        assert_eq!(version_id.as_deref(), Some(version.to_string().as_str()));
        let job = SubmitJobResult {
            job_id: aruna_core::structs::execution::job::JobId::from_bytes([1; 16]),
            created: true,
        };
        let answer = run.next(outcome::<_, SubmitJobError>(Ok(job.clone())));
        assert_eq!(answer, Ok(Next::Done(job)));
    }

    fn head_result() -> HeadObjectResult {
        HeadObjectResult {
            location: None,
            metadata: Default::default(),
            source_metadata: None,
            last_refresh: None,
            version_created_at: None,
            version_id: None,
            resolved_version_id: None,
            checksum_type:
                aruna_core::structs::storage::multipart::MultipartChecksumType::FullObject,
            composite_hashes: Default::default(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: None,
        }
    }

    #[test]
    fn refuses_missing_input() {
        let mut config = run_config(node(1), owner(2));
        config.spec.inputs.push(input("project", None, None));
        let mut run = local_run(config);
        run.next_input(0).unwrap();
        let answer = run.next(outcome::<HeadObjectResult, _>(Err(
            HeadObjectError::NoSuchKey,
        )));
        assert!(matches!(
            answer,
            Err(LocalExecutionError::InputNotLocal { bucket, .. }) if bucket == "project"
        ));
    }

    #[test]
    fn refuses_floating_copy() {
        // A realm input is copied by exact version or not at all.
        let mut config = run_config(node(1), owner(2));
        config
            .spec
            .inputs
            .push(input("project", Some(node(4)), None));
        let answer = local_run(config).next_input(0);
        assert!(matches!(
            answer,
            Err(LocalExecutionError::InputRefused { reason, .. }) if reason.contains("exact version")
        ));
    }

    #[test]
    fn counts_local_runs() {
        let mut runs = LocalRunsOperation::new(owner(2));
        assert!(matches!(runs.first(), Ok(Next::Call(IoCall::Child(_)))));
        let page =
            outcome::<(Vec<JobRecord>, Option<Vec<u8>>), ListJobsError>(Ok((Vec::new(), None)));
        let read = IoCall::child(DrainStateOperation::new());
        assert_eq!(runs.next(page), Ok(Next::Call(read)));
        let expected = LocalRuns {
            running: 0,
            queued: 0,
            paused: true,
        };
        assert_eq!(runs.next(drain(true)), Ok(Next::Done(expected)));
    }

    #[test]
    fn pause_explains_status() {
        let runs = |paused| LocalRuns {
            running: 1,
            queued: 2,
            paused,
        };
        let healthy = || BackendState {
            kind: Some("local".to_string()),
            limits: ResourceEnvelope::default(),
            failure: None,
        };
        let status = status_of(healthy(), runs(true));
        assert!(status.enabled && status.healthy && status.paused);
        assert_eq!(
            status.message,
            Some(LocalExecutionError::Paused.to_string())
        );
        let failing = BackendState {
            failure: Some("down".to_string()),
            ..healthy()
        };
        let status = status_of(failing, runs(false));
        assert!(!status.healthy);
        assert_eq!(status.message.as_deref(), Some("down"));
    }
}
