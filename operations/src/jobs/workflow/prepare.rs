//! Prepares one execution attempt: authorizes the bucket, mounts inputs, builds the task spec.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::effect::{BackendFacts, ComputeAnswer, ComputeEffect, WorkspaceAddress};
use aruna_core::compute::runtimes::SESSION_SOCKET_PATH;
use aruna_core::compute::{
    AttemptRef, ExecutorKind, NetworkAccess, ResourceRequest, S3Mount, Secret, SecurityContext,
    StagingMode, TaskInput, TaskSpec, UserSpec, WorkspaceBinding,
};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{ExecutionSpec, JobError, JobRecord, WorkspaceMode};

use super::DEFAULT_WALLTIME;
use super::compute::{compute_answer, compute_call};
use super::credential::{
    MintCredentialOperation, RunIdentity, WorkspaceAccessOperation, WorkspaceCredential,
};
use super::input_view::{CachedKeysOperation, ViewEntry, ViewError, view_bucket, view_entries};
use super::inputs::LoadInputsOperation;
use crate::driver::DriverContext;
use crate::jobs::lifecycle::ids::session_of;

pub(super) const NETWORK_TAG_KEY: &str = "aruna-engine.org/network";

/// What one attempt needs of its preparation.
#[derive(Debug, PartialEq)]
pub struct PreparedTask {
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

/// The operation that mints the run's credential for its workspace bucket.
pub fn workspace_mint(
    context: &DriverContext,
    spec: &ExecutionSpec,
    record: &JobRecord,
    node_id: NodeId,
    scope: (&str, Option<&str>),
) -> MintCredentialOperation {
    let realm_id = context.serving_realm(record.created_by.realm_id);
    let run = RunIdentity::new(spec, record, realm_id, node_id);
    let key = (context.net_handle.as_ref()).map(|net| net.credential_encryption_key());
    MintCredentialOperation::workspace((spec, record), run, scope, (key, SystemTime::now()))
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

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Access,
    Facts,
    Endpoint,
    Inputs,
    Cached,
    InputCredential,
    SessionCredential { view: bool },
}

type PrepareNext = Result<Next<PreparedTask>, JobError>;

/// Builds the inputs and credentials of one attempt, for its first submit and for every
/// recovery, so a resubmit carries the very credential the first submit minted. Mounted inputs
/// are read through the run's input view at their exact versions; every other input is read
/// straight from its source. Nothing is copied, so an adopted attempt restages the same way.
#[derive(Debug, PartialEq)]
pub struct PrepareAttemptOperation {
    run: RunIdentity,
    spec: Arc<ExecutionSpec>,
    record: Arc<JobRecord>,
    bucket: String,
    encryption_key: Option<CredentialEncryptionKey>,
    /// A first preparation also authorizes the bucket of an `Existing`-mode run.
    check_workspace: bool,
    facts: Option<BackendFacts>,
    address: WorkspaceAddress,
    entries: Vec<ViewEntry>,
    prepared: Option<PreparedTask>,
    now_ms: u64,
    step: PrepareStep,
}

impl PrepareAttemptOperation {
    pub fn new(
        run: RunIdentity,
        (spec, record): (Arc<ExecutionSpec>, Arc<JobRecord>),
        bucket: &str,
        encryption_key: Option<CredentialEncryptionKey>,
        check_workspace: bool,
    ) -> Self {
        Self {
            run,
            spec,
            record,
            bucket: bucket.to_string(),
            encryption_key,
            check_workspace,
            facts: None,
            address: WorkspaceAddress::default(),
            entries: Vec::new(),
            prepared: None,
            now_ms: 0,
            step: PrepareStep::Access,
        }
    }

    fn call(&mut self, step: PrepareStep, call: IoCall) -> PrepareNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn secret_inputs(&self) -> (Option<CredentialEncryptionKey>, SystemTime) {
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
        (self.encryption_key.clone(), now)
    }

    fn mounts(&self) -> bool {
        self.facts.as_ref().is_some_and(|facts| facts.s3_mount)
    }

    /// Whether the run gets a session's S3 access: a session always, a batch run inside its
    /// bucket when its backend gives sessions S3 and this node names an endpoint for it.
    fn wants_session(&self) -> bool {
        let endpoint = self.address.session_endpoint.is_some() || self.address.endpoint.is_some();
        let workspace = self.record.workspace_mode == WorkspaceMode::Existing
            && endpoint
            && self.facts.as_ref().is_some_and(|facts| facts.session);
        session_of(&self.spec).is_some() || workspace
    }

    /// The capabilities of the backend this run is placed on.
    fn read_facts(&mut self) -> PrepareNext {
        let Some(kind) = self.spec.executor_constraint.as_deref() else {
            return self.read_endpoint();
        };
        let kind = ExecutorKind::from_wire(kind);
        let call = compute_call(ComputeEffect::Facts { kind });
        self.call(PrepareStep::Facts, call)
    }

    fn read_endpoint(&mut self) -> PrepareNext {
        self.call(PrepareStep::Endpoint, compute_call(ComputeEffect::Endpoint))
    }

    fn load_inputs(&mut self) -> PrepareNext {
        self.entries = match self.mounts() {
            true => view_entries(&self.record).unwrap_or_default(),
            false => Vec::new(),
        };
        let mounted: BTreeSet<String> = (self.entries.iter())
            .map(|entry| entry.input.dest_key.clone())
            .collect();
        let load = LoadInputsOperation::new(self.run, &self.spec, self.record.clone(), &mounted);
        self.call(PrepareStep::Inputs, IoCall::child(load))
    }

    fn inputs_loaded(&mut self, inputs: Vec<TaskInput>) -> PrepareNext {
        let empty = self.entries.is_empty();
        self.prepared = Some(PreparedTask {
            inputs,
            mounts: Vec::new(),
            secrets: BTreeMap::new(),
            staging: match empty {
                true => StagingMode::Files,
                false => StagingMode::S3Mount,
            },
            workspace: None,
        });
        if empty {
            return self.after_mounts();
        }
        let cached = CachedKeysOperation::new(self.entries.clone(), self.run.node_id);
        self.call(PrepareStep::Cached, IoCall::child(cached))
    }

    fn mounts_known(&mut self, cached: BTreeSet<String>) -> PrepareNext {
        let view = view_bucket(self.record.job_id);
        let mounts = (self.entries.iter())
            .map(|entry| S3Mount {
                bucket: view.clone(),
                key: entry.key.clone(),
                path: entry.input.container_path.clone().unwrap_or_default(),
                cached: cached.contains(&entry.key),
            })
            .collect();
        if let Some(prepared) = self.prepared.as_mut() {
            prepared.mounts = mounts;
        }
        // A session's one credential carries the view as well, so it is minted there instead.
        if self.wants_session() {
            return self.after_mounts();
        }
        let buckets = BTreeSet::from([view]);
        let job = (self.spec.as_ref(), self.record.as_ref());
        let mint = MintCredentialOperation::inputs(job, self.run, &buckets, self.secret_inputs());
        self.call(PrepareStep::InputCredential, IoCall::child(mint))
    }

    fn after_mounts(&mut self) -> PrepareNext {
        if !self.wants_session() {
            return self.finish();
        }
        self.session_credential()
    }

    /// A session, or a batch run inside its bucket, reads and writes that bucket over S3 and
    /// nothing else. The credential is revoked with the job's terminal cleanup.
    fn session_credential(&mut self) -> PrepareNext {
        if self.bucket.is_empty() {
            return Err(JobError::permanent("a session needs a workspace bucket"));
        }
        if self.endpoint().is_none() {
            return Err(JobError::permanent(
                "a session needs a container-reachable S3 endpoint",
            ));
        }
        let has_mounts = self
            .prepared
            .as_ref()
            .is_some_and(|prepared| !prepared.mounts.is_empty());
        let view = has_mounts.then(|| view_bucket(self.record.job_id));
        let job = (self.spec.as_ref(), self.record.as_ref());
        let scope = (self.bucket.as_str(), view.as_deref());
        let mint = MintCredentialOperation::workspace(job, self.run, scope, self.secret_inputs());
        let step = PrepareStep::SessionCredential { view: has_mounts };
        self.call(step, IoCall::child(mint))
    }

    fn endpoint(&self) -> Option<String> {
        (self.address.session_endpoint.clone()).or_else(|| self.address.endpoint.clone())
    }

    fn session_minted(&mut self, credential: WorkspaceCredential, view: bool) -> PrepareNext {
        let endpoint = self.endpoint().unwrap_or_default();
        let region = self.address.region.clone();
        let bucket_name = self.bucket.clone();
        let prepared = self.prepared.as_mut().ok_or(JobError::retryable(
            "the attempt preparation lost its inputs",
        ))?;
        if view {
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
            bucket_name,
            region,
        });
        self.finish()
    }

    fn finish(&mut self) -> PrepareNext {
        self.prepared
            .take()
            .map(Next::Done)
            .ok_or_else(|| JobError::retryable("the attempt preparation lost its inputs"))
    }
}

impl Operation for PrepareAttemptOperation {
    type Output = PreparedTask;
    type Error = JobError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PrepareNext {
        // A `None`-mode run touches no bucket, so it has nothing to authorize.
        if self.check_workspace && self.record.workspace_mode == WorkspaceMode::Existing {
            let access = WorkspaceAccessOperation::existing(self.run, &self.bucket);
            return self.call(PrepareStep::Access, IoCall::child(access));
        }
        self.read_facts()
    }

    fn next(&mut self, result: IoResult) -> PrepareNext {
        match self.step {
            PrepareStep::Access => {
                result.child::<(), JobError>()??;
                self.read_facts()
            }
            PrepareStep::Facts => {
                if let ComputeAnswer::Facts(facts) = compute_answer(result)? {
                    self.facts = Some(facts);
                }
                self.read_endpoint()
            }
            PrepareStep::Endpoint => {
                if let ComputeAnswer::Endpoint(address) = compute_answer(result)? {
                    self.address = address;
                }
                self.load_inputs()
            }
            PrepareStep::Inputs => {
                let inputs = result.child::<Vec<TaskInput>, JobError>()??;
                self.inputs_loaded(inputs)
            }
            PrepareStep::Cached => {
                let cached = result
                    .child::<BTreeSet<String>, ViewError>()?
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                self.mounts_known(cached)
            }
            PrepareStep::InputCredential => {
                let credential = result.child::<WorkspaceCredential, JobError>()??;
                if let Some(prepared) = self.prepared.as_mut() {
                    let (access_key, secret) = (credential.access_key, credential.secret);
                    insert_mount_secrets(&mut prepared.secrets, access_key, secret);
                }
                self.finish()
            }
            PrepareStep::SessionCredential { view } => {
                let credential = result.child::<WorkspaceCredential, JobError>()??;
                self.session_minted(credential, view)
            }
        }
    }
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
