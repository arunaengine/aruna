//! Starts a workflow run: checks the caller's rights on the group, the version, the pinned inputs
//! and the output, answers a repeated idempotency key with its run, then stores and queues it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::valid_vault_env;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::job_request::{SubmitJobError, SubmitJobResult};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, RUN_IDEMPOTENCY_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::input::JobPayload;
use aruna_core::structs::execution::job::local::{WorkspaceMode, user_dedup_key};
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunKeyRecord, RunOutput, RunState, WorkflowRunRecord, WorkflowRunSpec,
    WorkflowSecretBinding, run_key, run_key_row,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{GroupId, Key};
use byteview::ByteView;
use serde_json::Value;
use ulid::Ulid;

use super::data::ObjectAccessOperation;
use super::{RunChange, change_call, changed, events};
use crate::jobs::submit::SubmitJobSpec;
use crate::jobs::submit::local::SubmitLocalOperation;
use crate::workflows::versions::{GetVersionOperation, VersionView};
use crate::workflows::{CatalogError, GroupAccessOperation, access_answer};

/// One pinned input object of a request, by the input name it fills.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinnedInput {
    pub name: String,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub source_node_id: Option<NodeId>,
}

/// A run as the caller asked for it, with its transport input already checked.
#[derive(Clone, Debug, PartialEq)]
pub struct RunRequest {
    pub group_id: GroupId,
    pub workflow_id: Ulid,
    pub version: u32,
    pub values: Value,
    pub inputs: Vec<PinnedInput>,
    /// The inputs as the request named them, shown again in the snapshot.
    pub inputs_json: Option<String>,
    pub output: Option<RunOutput>,
    pub provenance_crate: bool,
    pub idempotency_key: Option<String>,
    /// The site every task of the run must run at; `None` lets each task go where it fits.
    pub placement_site: Option<String>,
    /// Vault secret bindings of the run, by workflow secret name; carries only references.
    pub secrets: BTreeMap<String, WorkflowSecretBinding>,
}

impl RunRequest {
    /// What a repeated key must ask for again: the workflow, its version, values, inputs,
    /// output and secret bindings.
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"aruna-workflow-run-request-v1\0");
        hasher.update(&self.group_id.to_bytes());
        hasher.update(&self.workflow_id.to_bytes());
        hasher.update(&self.version.to_be_bytes());
        let secrets = serde_json::to_string(&self.secrets).unwrap_or_default();
        for text in [
            self.values.to_string(),
            self.inputs_json.clone().unwrap_or_default(),
            (self.output.as_ref())
                .map(|output| format!("{}\0{}", output.bucket, output.prefix))
                .unwrap_or_default(),
            secrets,
        ] {
            hasher.update(&(text.len() as u64).to_be_bytes());
            hasher.update(text.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}

/// Where a new run stands in a lineage: the run it resumes or repeats, and its cache scope.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Lineage {
    /// The first run of the lineage; `None` starts a new one with the run itself.
    pub first: Option<Ulid>,
    pub resumes: Option<Ulid>,
    pub repeats: Option<Ulid>,
    pub cache_scope: CacheScope,
    /// The earlier run was protected, so this one is too.
    pub protected: bool,
}

/// A run the caller started, and whether this request created it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedRun {
    pub record: WorkflowRunRecord,
    pub created: bool,
}

#[derive(Debug, PartialEq)]
enum AdmitStep {
    Group,
    Version,
    Key(Box<VersionView>),
    Existing(Box<VersionView>, [u8; 32]),
    Inputs(Box<VersionView>),
    Output(Box<VersionView>),
    Encryption(Box<VersionView>),
    Record,
    Submit(Box<WorkflowRunRecord>),
    Job(Box<WorkflowRunRecord>),
    Failed(String),
}

/// Admits one run on this node; see the module comment for the checks, in that order.
#[derive(Debug, PartialEq)]
pub struct StartRunOperation {
    auth: AuthContext,
    node: (NodeId, RealmId),
    request: RunRequest,
    lineage: Lineage,
    pending: Vec<PinnedInput>,
    ids: IdMint,
    now_ms: u64,
    step: AdmitStep,
}

type AdmitNext = Result<Next<AdmittedRun>, CatalogError>;

impl StartRunOperation {
    pub fn new(auth: &AuthContext, node: (NodeId, RealmId), request: RunRequest) -> Self {
        Self {
            auth: auth.clone(),
            node,
            pending: request.inputs.clone(),
            request,
            lineage: Lineage::default(),
            ids: IdMint::default(),
            now_ms: 0,
            step: AdmitStep::Group,
        }
    }

    /// Starts the run in an existing lineage, as a resume or a repeat does.
    pub fn in_lineage(mut self, lineage: Lineage) -> Self {
        self.lineage = lineage;
        self
    }

    fn key_row(&self, key: &str) -> Key {
        run_key_row(self.request.group_id, self.auth.user_id, key)
    }

    fn after_version(&mut self, view: VersionView) -> AdmitNext {
        let Some(key) = self.request.idempotency_key.clone() else {
            return self.next_input(view);
        };
        self.step = AdmitStep::Key(Box::new(view));
        let row = self.key_row(&key);
        Ok(Next::Call(IoCall::Read(vec![(
            RUN_IDEMPOTENCY_KEYSPACE.to_string(),
            row,
        )])))
    }

    /// Checks READ on the next pinned input this node holds; inputs of other nodes are read
    /// there when a task stages them, as the unrestricted creator, so a restricted caller may
    /// name only inputs of this node.
    fn next_input(&mut self, view: VersionView) -> AdmitNext {
        let local = self.node.0;
        let restricted = self.auth.path_restrictions.is_some();
        while let Some(input) = self.pending.pop() {
            if input.source_node_id.is_some_and(|node| node != local) {
                if restricted {
                    return Err(CatalogError::Forbidden);
                }
                continue;
            }
            let access = ObjectAccessOperation::new(
                &self.auth,
                local,
                (&input.bucket, &input.key),
                Permission::READ,
            );
            self.step = AdmitStep::Inputs(Box::new(view));
            return Ok(Next::Call(IoCall::child(access)));
        }
        let Some(output) = self.request.output.clone() else {
            return self.encryption(view);
        };
        let access = ObjectAccessOperation::new(
            &self.auth,
            local,
            (&output.bucket, output.prefix.trim_start_matches('/')),
            Permission::WRITE,
        );
        self.step = AdmitStep::Output(Box::new(view));
        Ok(Next::Call(IoCall::child(access)))
    }

    /// Reads whether the pinned inputs this node holds and the output bucket encrypt. Inputs of
    /// other nodes are not readable here, so they never make a run protected.
    fn encryption(&mut self, view: VersionView) -> AdmitNext {
        let local = self.node.0;
        let mut buckets: Vec<&str> = (self.request.inputs.iter())
            .filter(|input| input.source_node_id.is_none_or(|node| node == local))
            .map(|input| input.bucket.as_str())
            .collect();
        buckets.sort_unstable();
        buckets.dedup();
        let output = self
            .request
            .output
            .as_ref()
            .map(|output| output.bucket.as_str());
        let rows = (buckets.into_iter().chain(output))
            .map(|bucket| {
                let key = Key::from(bucket.as_bytes().to_vec());
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key)
            })
            .collect();
        self.step = AdmitStep::Encryption(Box::new(view));
        Ok(Next::Call(IoCall::Read(rows)))
    }

    /// Protects a run with an encrypting input or output; an encrypting input needs an
    /// encrypting output bucket, so its results never land in plain storage.
    fn protection(&self, result: IoResult) -> Result<bool, CatalogError> {
        let mut encrypted = Vec::new();
        for row in result.values()? {
            let settings = BucketEncryption::from_row(row.as_deref())
                .map_err(|error| CatalogError::Storage(error.to_string()))?;
            encrypted.push(settings.is_encrypted());
        }
        let output = match self.request.output.is_some() {
            true => encrypted.pop().unwrap_or(false),
            false => false,
        };
        if (encrypted.contains(&true) || self.lineage.protected) && !output {
            return Err(CatalogError::Invalid(
                "a protected run requires an encrypted output bucket".into(),
            ));
        }
        Ok(self.lineage.protected || output || encrypted.contains(&true))
    }

    fn record(&mut self, view: VersionView, protected: bool) -> AdmitNext {
        let run_id = self.ids.mint(self.now_ms);
        let request = &self.request;
        let record = WorkflowRunRecord {
            run_id,
            realm_id: self.node.1,
            group_id: request.group_id,
            workflow_id: request.workflow_id,
            workflow_name: view.workflow.name.clone(),
            version: request.version,
            lineage: self.lineage.first.unwrap_or(run_id),
            state: RunState::Queued,
            node_id: self.node.0,
            created_by: self.auth.user_id,
            created_at_ms: self.now_ms,
            finished_at_ms: None,
            error: None,
            values_json: request.values.to_string(),
            inputs_json: request.inputs_json.clone(),
            output: request.output.clone(),
            outputs: Vec::new(),
            provenance_crate: request.provenance_crate,
            provenance_document_id: None,
            provenance_status: None,
            last_event_seq: 0,
            local_only: false,
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 0,
            steps: Vec::new(),
            resumes_run_id: self.lineage.resumes,
            repeats_run_id: self.lineage.repeats,
            cache_scope: match protected {
                true => CacheScope::Run,
                false => self.lineage.cache_scope,
            },
            placement_site: request.placement_site.clone(),
            protected,
            started_at_ms: None,
            secrets: request.secrets.clone(),
        };
        let key = request.idempotency_key.as_ref().map(|key| {
            let row = self.key_row(key);
            (row, request.digest())
        });
        self.step = AdmitStep::Record;
        let store = RecordRunOperation::new(record, key);
        Ok(Next::Call(IoCall::retry(store, &run_id.to_bytes())))
    }

    fn submit(&mut self, record: WorkflowRunRecord) -> AdmitNext {
        let spec = WorkflowRunSpec {
            run_id: record.run_id,
            group_id: record.group_id,
            created_by: record.created_by,
            realm_id: record.realm_id,
        };
        let dedup = format!("workflow-run/{}", record.run_id);
        let spec = SubmitJobSpec {
            payload: JobPayload::WorkflowRun(Box::new(spec)),
            created_by: record.created_by,
            owner_node_id: self.node.0,
            dedup_key: Some(user_dedup_key(record.created_by, &dedup)),
            now_ms: self.now_ms,
            retention_ms: RETENTION_MS,
            workspace_mode: WorkspaceMode::None,
            workspace_bucket: None,
            active_cap: None,
        };
        let serving = Some(self.node);
        let submit = SubmitLocalOperation::new(spec, self.node.1, serving);
        self.step = AdmitStep::Submit(Box::new(record));
        Ok(Next::Call(IoCall::child(submit)))
    }
}

impl Operation for StartRunOperation {
    type Output = AdmittedRun;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> AdmitNext {
        if !self.request.values.is_object() {
            return Err(CatalogError::Invalid("values must be a JSON object".into()));
        }
        if self.request.output.is_none() {
            return Err(CatalogError::Invalid(
                "a run needs an output bucket: its task files are kept below the output prefix"
                    .into(),
            ));
        }
        if !self.request.secrets.is_empty() {
            if self.auth.path_restrictions.is_some() {
                return Err(CatalogError::Forbidden);
            }
            for (name, binding) in &self.request.secrets {
                if !valid_vault_env(name) {
                    return Err(CatalogError::Invalid(format!(
                        "secret name `{name}` is not an allowed environment variable name"
                    )));
                }
                if let Some(field) = &binding.field {
                    let valid = !field.is_empty()
                        && field.len() <= 32
                        && field
                            .bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
                    if !valid {
                        return Err(CatalogError::Invalid(format!(
                            "secret `{name}` names an invalid field `{field}`"
                        )));
                    }
                }
            }
        }
        self.pending.reverse();
        self.step = AdmitStep::Group;
        let check = GroupAccessOperation::new(&self.auth, [self.request.group_id]);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> AdmitNext {
        match std::mem::replace(&mut self.step, AdmitStep::Record) {
            AdmitStep::Group => {
                let access = access_answer(result)?;
                let access = access
                    .get(&self.request.group_id)
                    .copied()
                    .unwrap_or_default();
                match (access.read, access.write) {
                    (_, true) => {}
                    (true, false) => return Err(CatalogError::Forbidden),
                    (false, false) => return Err(CatalogError::NotFound),
                }
                self.step = AdmitStep::Version;
                let read = GetVersionOperation::new(
                    &self.auth,
                    self.request.workflow_id,
                    self.request.version,
                );
                Ok(Next::Call(IoCall::child(read)))
            }
            AdmitStep::Version => {
                let view = result.child::<VersionView, CatalogError>()??;
                self.after_version(view)
            }
            AdmitStep::Key(view) => {
                let Some(value) = result.value()? else {
                    return self.next_input(*view);
                };
                let known = RunKeyRecord::from_bytes(&value)?;
                self.step = AdmitStep::Existing(view, known.request_digest);
                Ok(Next::Call(IoCall::Read(vec![(
                    WORKFLOW_RUN_KEYSPACE.to_string(),
                    run_key(known.run_id),
                )])))
            }
            AdmitStep::Existing(view, digest) => match super::run_answer(result)? {
                Some(record) if digest == self.request.digest() => Ok(Next::Done(AdmittedRun {
                    record,
                    created: false,
                })),
                Some(_) => Err(CatalogError::Conflict(
                    "the idempotency key started another run with other values".into(),
                )),
                None => self.next_input(*view),
            },
            AdmitStep::Inputs(view) => {
                super::data::bucket_answer(result).map_err(|error| match error {
                    CatalogError::NotFound => {
                        CatalogError::Invalid("a pinned input's bucket does not exist".into())
                    }
                    other => other,
                })?;
                self.next_input(*view)
            }
            AdmitStep::Output(view) => {
                super::data::bucket_answer(result).map_err(|error| match error {
                    CatalogError::NotFound => {
                        CatalogError::Invalid("the output bucket does not exist".into())
                    }
                    other => other,
                })?;
                self.encryption(*view)
            }
            AdmitStep::Encryption(view) => {
                let protected = self.protection(result)?;
                self.record(*view, protected)
            }
            AdmitStep::Record => {
                let admitted = result.child::<AdmittedRun, CatalogError>()??;
                match admitted.created {
                    true => self.submit(admitted.record),
                    false => Ok(Next::Done(admitted)),
                }
            }
            AdmitStep::Submit(record) => match result.child::<SubmitJobResult, SubmitJobError>()? {
                Ok(submitted) => {
                    let run_id = record.run_id;
                    self.step = AdmitStep::Job(record);
                    let change = RunChange::Job(submitted.job_id);
                    Ok(Next::Call(change_call(run_id, vec![change])))
                }
                Err(error) => {
                    let message = format!("the run could not be queued: {error}");
                    let change = RunChange::State {
                        state: RunState::Failed,
                        error: Some(message.clone()),
                    };
                    self.step = AdmitStep::Failed(message);
                    Ok(Next::Call(change_call(record.run_id, vec![change])))
                }
            },
            AdmitStep::Job(_) => {
                let record = changed(result)?;
                Ok(Next::Done(AdmittedRun {
                    record,
                    created: true,
                }))
            }
            AdmitStep::Failed(message) => {
                changed(result)?;
                Err(CatalogError::Storage(message))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::NotFound | CatalogError::Forbidden | CatalogError::Invalid(_)
        )
    }
}

/// Stores a new run with its first event and its idempotency row in one transaction; a key that
/// another request stored meanwhile answers that run instead.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordRunOperation {
    record: WorkflowRunRecord,
    key: Option<(Key, [u8; 32])>,
    stage: u8,
}

impl RecordRunOperation {
    pub fn new(record: WorkflowRunRecord, key: Option<(Key, [u8; 32])>) -> Self {
        Self {
            record,
            key,
            stage: 0,
        }
    }

    fn write(&mut self) -> Result<Next<AdmittedRun>, CatalogError> {
        let mut writes = Vec::new();
        let payload = events::run_json(&self.record);
        events::push(&mut self.record, "run", payload, &mut writes)?;
        writes.push(super::run_row(&self.record)?);
        if let Some((row, digest)) = &self.key {
            let known = RunKeyRecord {
                run_id: self.record.run_id,
                request_digest: *digest,
            };
            writes.push((
                RUN_IDEMPOTENCY_KEYSPACE.to_string(),
                row.clone(),
                ByteView::from(known.to_bytes()?),
            ));
        }
        self.stage = 3;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for RecordRunOperation {
    type Output = AdmittedRun;
    type Error = CatalogError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<AdmittedRun>, CatalogError> {
        let Some((row, _)) = &self.key else {
            return self.write();
        };
        self.stage = 1;
        Ok(Next::Call(IoCall::Read(vec![(
            RUN_IDEMPOTENCY_KEYSPACE.to_string(),
            row.clone(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdmittedRun>, CatalogError> {
        match self.stage {
            1 => {
                let Some(value) = result.value()? else {
                    return self.write();
                };
                let known = RunKeyRecord::from_bytes(&value)?;
                let digest = self.key.as_ref().map(|(_, digest)| *digest);
                if Some(known.request_digest) != digest {
                    return Err(CatalogError::Conflict(
                        "the idempotency key started another run with other values".into(),
                    ));
                }
                self.stage = 2;
                Ok(Next::Call(super::read_run(known.run_id)))
            }
            2 => {
                let record = super::run_answer(result)?.ok_or(CatalogError::NotFound)?;
                Ok(Next::Done(AdmittedRun {
                    record,
                    created: false,
                }))
            }
            _ => {
                result.done()?;
                Ok(Next::Done(AdmittedRun {
                    record: self.record.clone(),
                    created: true,
                }))
            }
        }
    }
}
