//! Prepares one execution attempt: authorizes the bucket, mounts inputs, builds the task spec.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::effect::{BackendFacts, ComputeAnswer, ComputeEffect, WorkspaceAddress};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::compute::runtimes::SESSION_SOCKET_PATH;
use aruna_core::compute::{
    AttemptRef, ExecutorKind, NetworkAccess, NfsBindings, NfsShareConfig, ResourceRequest, S3Mount,
    Secret, SecurityContext, StagingMode, TaskInput, TaskSpec, UserSpec, WorkspaceBinding,
};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_core::structs::execution::job::input::{
    ExecutionSpec, PLUGIN_REPORT_TAG, WORKFLOW_RUN_TAG, WORKFLOW_SECRETS_TAG,
    parse_workflow_secrets, tagged,
};
use aruna_core::structs::execution::job::local::{JobError, JobRecord, WorkspaceMode};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::vfs::{BucketScope, NfsMountBinding, NfsMountTarget};
use aruna_core::vfs_effects::VfsProtocol;

use super::DEFAULT_WALLTIME;
use super::compute::{compute_answer, compute_call};
use super::credential::{
    MintCredentialOperation, RunIdentity, WorkspaceAccessOperation, WorkspaceCredential,
};
use super::input_view::{CachedKeysOperation, ViewEntry, ViewError, view_bucket, view_entries};
use super::inputs::LoadInputsOperation;
use super::vault_secrets::ReleaseVaultSecrets;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::alias::AliasFamilyOperation;
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::lifecycle::screen::{AdmitTask, ScreenError, ScreenTask};
use crate::jobs::route::JobRouteError;
use aruna_core::job_request::session_of;
use aruna_ops_data::vfs::admin::MountInput;
use aruna_ops_data::vfs::{BucketScopeOperation, CreateMountOperation, VfsAccessContext, VfsError};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};

pub const NETWORK_TAG_KEY: &str = "aruna-engine.org/network";
/// Set to `files` so a run never mounts its inputs, and so never gets an input credential.
pub const STAGING_TAG_KEY: &str = "aruna-engine.org/staging";

/// What one attempt needs of its preparation.
#[derive(Debug, PartialEq)]
pub struct PreparedTask {
    pub inputs: Vec<TaskInput>,
    pub mounts: Vec<S3Mount>,
    pub secrets: BTreeMap<String, Secret>,
    pub vault_env: BTreeMap<String, Secret>,
    pub staging: StagingMode,
    /// Set for a session or a batch run inside its bucket: the bucket and endpoint its
    /// credential reaches.
    pub workspace: Option<WorkspaceBinding>,
    /// The attempt's own bindings on its backend's NFS worker-pool host.
    pub nfs_mounts: NfsBindings,
}

/// The bucket a run works inside, empty when it owns none.
pub(super) fn job_bucket(record: &JobRecord) -> String {
    record.workspace_bucket.clone().unwrap_or_default()
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

/// A binding the host or storage may still take later retries; a refused one fails the job.
fn share_error(error: VfsError) -> JobError {
    let message = format!("the attempt's NFS binding failed: {error}");
    match error {
        VfsError::Conflict | VfsError::Unavailable(_) | VfsError::Storage(_) | VfsError::Vfs(_) => {
            JobError::retryable(message)
        }
        _ => JobError::permanent(message),
    }
}

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Access,
    TaskConfig,
    TaskReservation(Box<RealmConfigDocument>),
    TaskFamily(Box<RealmConfigDocument>),
    TaskAdmit,
    Facts,
    Endpoint,
    Inputs,
    Cached,
    InputCredential,
    SessionCredential { view: bool },
    VaultSecrets,
    InputShare,
    SessionScope,
    SessionShare,
}

type PrepareNext = Result<Next<PreparedTask>, JobError>;

/// Builds the inputs and credentials of one attempt, for its first submit and every recovery,
/// so a resubmit carries the credential the first one minted. Mounted inputs are read through
/// the run's input view; nothing is copied, so an adopted attempt restages the same way.
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
    /// The NFS host of the selected backend and the user its workers run as.
    nfs: Option<(NfsShareConfig, UserSpec)>,
    /// The minted key the attempt's NFS bindings are admitted under.
    share_key: String,
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
            nfs: None,
            share_key: String::new(),
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
        let files = (self.spec.tags.get(STAGING_TAG_KEY)).is_some_and(|value| value == "files");
        !files && self.facts.as_ref().is_some_and(|facts| facts.s3_mount)
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

    /// A plugin task checks its install, account, grants and work row again at every attempt
    /// start: a lost authority fails the job, anything not decidable now retries it.
    fn start(&mut self) -> PrepareNext {
        if tagged(&self.spec).is_none() {
            return self.read_facts();
        }
        let read = GetConfigOperation::new(self.run.realm_id);
        self.call(PrepareStep::TaskConfig, IoCall::child(read))
    }

    /// The reservation names the family's job, whose family the receiver must have adopted.
    fn task_family(
        &mut self,
        config: RealmConfigDocument,
        held: Vec<JobReservationRecord>,
    ) -> PrepareNext {
        let Some(reserved) = held.into_iter().next() else {
            return Err(JobError::permanent(
                "a plugin task runs only with its reservation",
            ));
        };
        let alias = AliasFamilyOperation::new(reserved.logical_job_id);
        self.call(
            PrepareStep::TaskFamily(Box::new(config)),
            IoCall::child(alias),
        )
    }

    fn task_admit(&mut self, config: RealmConfigDocument, family: JobFamilyId) -> PrepareNext {
        let node = self.run.node_id;
        let task = (self.spec.as_ref(), self.record.created_by, node);
        let screen = ScreenTask::new(Arc::new(config), task);
        let admit = AdmitTask::new(screen, (node, family));
        self.call(PrepareStep::TaskAdmit, IoCall::child(admit))
    }

    /// The backend this run is placed on: by its stored name, else by kind, else the first.
    fn read_facts(&mut self) -> PrepareNext {
        let backend = self.spec.backend.clone();
        let constraint = (self.spec.executor_constraint.as_deref()).map(ExecutorKind::from_wire);
        let call = compute_call(ComputeEffect::Select {
            backend,
            constraint,
        });
        self.call(PrepareStep::Facts, call)
    }

    /// The S3 endpoints the containers of `backend` reach.
    fn read_endpoint(&mut self, backend: Option<String>) -> PrepareNext {
        let Some(backend) = backend else {
            return self.load_inputs();
        };
        let call = compute_call(ComputeEffect::Endpoint { backend });
        self.call(PrepareStep::Endpoint, call)
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
            vault_env: BTreeMap::new(),
            staging: match empty {
                true => StagingMode::Files,
                false => StagingMode::S3Mount,
            },
            workspace: None,
            nfs_mounts: NfsBindings::default(),
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
        self.share_key = credential.access_key.clone();
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
        self.bind_shares()
    }

    /// Binds the attempt's own exports on its backend's NFS worker-pool host, the input view
    /// first. Each binding names the attempt's key, so its terminal cleanup removes it.
    fn bind_shares(&mut self) -> PrepareNext {
        let (Some((share, _)), Some(prepared)) = (self.nfs, self.prepared.as_ref()) else {
            return self.finish();
        };
        let bound = &prepared.nfs_mounts;
        if share.inputs && !prepared.mounts.is_empty() && bound.input.is_none() {
            let job_id = self.run.job_id;
            let create = self.share_mount(NfsMountTarget::JobInputView { job_id })?;
            return self.call(PrepareStep::InputShare, IoCall::child(create));
        }
        let mounted = session_of(&self.spec).is_some_and(|session| session.mount.is_some());
        if share.sessions && mounted && bound.session.is_none() {
            let scope = BucketScopeOperation::new(self.bucket.clone());
            return self.call(PrepareStep::SessionScope, IoCall::child(scope));
        }
        self.finish()
    }

    /// A worker-pool binding of `target` under the attempt's key, shown as the run's user: the
    /// job input view read-only, the session's folder writable by that user alone.
    fn share_mount(&self, target: NfsMountTarget) -> Result<CreateMountOperation, JobError> {
        let (share, run_as) = self.nfs.ok_or(JobError::retryable(
            "the attempt preparation lost its NFS host",
        ))?;
        let job_id = match &target {
            NfsMountTarget::JobInputView { job_id } => Some(*job_id),
            NfsMountTarget::Bucket { .. } | NfsMountTarget::Folder { .. } => None,
        };
        let read_only = job_id.is_some();
        let (bucket, prefix) = match read_only {
            true => (String::new(), None),
            false => (self.bucket.clone(), self.session_prefix()),
        };
        let context = VfsAccessContext {
            realm_id: self.run.realm_id,
            node_id: self.run.node_id,
            access_key: self.share_key.clone(),
            protocol: VfsProtocol::Nfs,
            target,
            read_only,
            now_ms: self.now_ms,
        };
        let input = MountInput {
            context,
            caller: self.run.user_id,
            host_id: share.host_id,
            group_id: self.run.group_id,
            bucket,
            prefix,
            job_id,
            read_only,
            allowed_networks: Vec::new(),
            display_uid: run_as.uid,
            display_gid: run_as.gid,
            mode: if read_only { 0o555 } else { 0o700 },
        };
        Ok(CreateMountOperation::new(input).for_worker_pool())
    }

    /// The session's folder in its bucket, `None` when it mounts the whole bucket.
    fn session_prefix(&self) -> Option<String> {
        let mount = session_of(&self.spec).and_then(|session| session.mount)?;
        (!mount.prefix.trim_matches('/').is_empty()).then_some(mount.prefix)
    }

    fn share_bound(&mut self, binding: NfsMountBinding, session: bool) -> PrepareNext {
        let prepared = self.prepared.as_mut().ok_or(JobError::retryable(
            "the attempt preparation lost its inputs",
        ))?;
        let mount_id = Some(binding.mount_id.to_string());
        match session {
            true => prepared.nfs_mounts.session = mount_id,
            false => prepared.nfs_mounts.input = mount_id,
        }
        self.bind_shares()
    }

    fn finish(&mut self) -> PrepareNext {
        if self.spec.tags.contains_key(WORKFLOW_SECRETS_TAG)
            && self
                .prepared
                .as_ref()
                .is_some_and(|prepared| prepared.vault_env.is_empty())
        {
            let bindings = parse_workflow_secrets(&self.spec.tags)
                .map_err(|_| JobError::permanent("workflow secret bindings are malformed"))?;
            if !bindings.is_empty() {
                let run_id = self
                    .spec
                    .tags
                    .get(WORKFLOW_RUN_TAG)
                    .and_then(|id| ulid::Ulid::from_string(id).ok())
                    .ok_or_else(|| {
                        JobError::permanent("workflow secret bindings require a valid workflow run")
                    })?;
                let release = ReleaseVaultSecrets::new(self.run, run_id, bindings);
                return self.call(PrepareStep::VaultSecrets, IoCall::child(release));
            }
        }
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
        self.start()
    }

    fn next(&mut self, result: IoResult) -> PrepareNext {
        match self.step {
            PrepareStep::Access => {
                result.child::<(), JobError>()??;
                self.start()
            }
            PrepareStep::TaskConfig => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| JobError::retryable("realm config unavailable"))?;
                let held = HeldReservations::of_job(self.record.job_id);
                let step = PrepareStep::TaskReservation(Box::new(config));
                self.call(step, IoCall::child(held))
            }
            PrepareStep::TaskReservation(ref config) => {
                let config = (**config).clone();
                let held = result.child::<Vec<JobReservationRecord>, LifecycleError>()?;
                let held = held.map_err(|error| JobError::retryable(error.to_string()))?;
                self.task_family(config, held)
            }
            PrepareStep::TaskFamily(ref config) => {
                let config = (**config).clone();
                let family = result.child::<Option<JobFamilyId>, JobRouteError>()?;
                let family = family.map_err(|error| JobError::retryable(error.to_string()))?;
                let Some(family) = family else {
                    return Err(JobError::retryable(
                        "the plugin task's family is not here yet",
                    ));
                };
                self.task_admit(config, family)
            }
            PrepareStep::TaskAdmit => match result.child::<PluginInstall, ScreenError>()? {
                Ok(_) => self.read_facts(),
                Err(error @ (ScreenError::Pending | ScreenError::Unavailable(_))) => {
                    Err(JobError::retryable(error.to_string()))
                }
                Err(error) => Err(JobError::permanent(error.to_string())),
            },
            PrepareStep::Facts => {
                let ComputeAnswer::Selected(Some(facts)) = compute_answer(result)? else {
                    return self.read_endpoint(None);
                };
                let name = facts.name.clone();
                // The selected backend runs the attempt, so its NFS host serves the bindings.
                self.nfs = facts.nfs_share.map(|share| (share, facts.run_as));
                // Mounts and sessions follow a backend the run is pinned to.
                if self.spec.backend.is_some() || self.spec.executor_constraint.is_some() {
                    self.facts = Some(facts);
                }
                self.read_endpoint(Some(name))
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
                self.share_key = credential.access_key.clone();
                if let Some(prepared) = self.prepared.as_mut() {
                    let (access_key, secret) = (credential.access_key, credential.secret);
                    insert_mount_secrets(&mut prepared.secrets, access_key, secret);
                }
                self.bind_shares()
            }
            PrepareStep::SessionCredential { view } => {
                let credential = result.child::<WorkspaceCredential, JobError>()??;
                self.session_minted(credential, view)
            }
            PrepareStep::VaultSecrets => {
                let released = result.child::<BTreeMap<String, Secret>, JobError>()??;
                let mut prepared = self.prepared.take().ok_or_else(|| {
                    JobError::retryable("the attempt preparation lost its inputs")
                })?;
                prepared.vault_env = released;
                Ok(Next::Done(prepared))
            }
            PrepareStep::InputShare => {
                let binding = result.child::<NfsMountBinding, VfsError>()?;
                self.share_bound(binding.map_err(share_error)?, false)
            }
            PrepareStep::SessionScope => {
                let scope = result
                    .child::<BucketScope, VfsError>()?
                    .map_err(share_error)?;
                let target = NfsMountTarget::for_bucket(&scope.bucket, scope.incarnation)
                    .map_err(|error| JobError::permanent(error.to_string()))?;
                let create = self.share_mount(target)?;
                self.call(PrepareStep::SessionShare, IoCall::child(create))
            }
            PrepareStep::SessionShare => {
                let binding = result.child::<NfsMountBinding, VfsError>()?;
                self.share_bound(binding.map_err(share_error)?, true)
            }
        }
    }
}

pub fn build_task_spec(
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
        vault_env,
        staging,
        workspace,
        nfs_mounts,
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
        vault_env,
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
        nfs_mounts,
        inputs,
        s3_mounts: mounts,
        staging_mode: staging,
        // A plugin task's report is declared so the backend lets it be read; no export takes it.
        output_paths: (spec.file_outputs.iter())
            .map(|output| output.container_path.clone())
            .chain(spec.tags.get(PLUGIN_REPORT_TAG).cloned())
            .collect(),
    }
}

#[cfg(test)]
#[path = "prepare_tests.rs"]
mod tests;
