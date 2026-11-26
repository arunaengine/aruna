//! Starts a workflow run: checks the caller's rights on the group, the version, the pinned inputs
//! and the output, answers a repeated idempotency key with its run, then stores and queues it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::valid_vault_env;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::job_request::{self, SubmissionScope, SubmitJobError, SubmitJobResult};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, RUN_IDEMPOTENCY_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::input::{
    CapturedInput, InputMode, InputSelection, InputSource, JobPayload,
};
use aruna_core::structs::execution::job::local::{WorkspaceMode, user_dedup_key};
use aruna_core::structs::execution::workflow_run::{
    CacheScope, DirectoryPin, DirectoryRoot, PrecapturedInput, RunKeyRecord, RunOutput, RunState,
    WorkflowRunRecord, WorkflowRunSpec, WorkflowSecretBinding, WorkflowSecretScope, run_key,
    run_key_row,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::types::{GroupId, Key};
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_ops_data::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};
use aruna_ops_identity::auth::error::{AuthorizeError, PolicyEnforcementError};
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::realm::mutate_placement::node_kind;
use byteview::ByteView;
use serde_json::Value;
use ulid::Ulid;

use super::data::ObjectAccessOperation;
use super::{RunChange, change_call, changed, events};
use crate::jobs::lifecycle::remote_inputs::{RemoteInputsOperation, ResolveCaller};
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
    /// What a repeated key must ask for again: every field that affects execution or publication.
    pub fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"aruna-workflow-run-request-v2\0");
        hasher.update(&self.group_id.to_bytes());
        hasher.update(&self.workflow_id.to_bytes());
        hasher.update(&self.version.to_be_bytes());
        hasher.update(&[u8::from(self.provenance_crate)]);
        let secrets = serde_json::to_string(&self.secrets).unwrap_or_default();
        let output = self
            .output
            .as_ref()
            .map(|output| {
                format!(
                    "{}\0{}\0{}\0{}",
                    output.bucket,
                    output.prefix,
                    output.node_id.map(|n| n.to_string()).unwrap_or_default(),
                    output.dataset_json.as_deref().unwrap_or_default(),
                )
            })
            .unwrap_or_default();
        let mut pins: Vec<String> = (self.inputs.iter())
            .map(|pin| {
                format!(
                    "{}\0{}\0{}\0{}\0{}",
                    pin.name,
                    pin.bucket,
                    pin.key,
                    pin.version_id,
                    pin.source_node_id
                        .map(|n| n.to_string())
                        .unwrap_or_default(),
                )
            })
            .collect();
        pins.sort();
        for text in [
            self.values.to_string(),
            self.inputs_json.clone().unwrap_or_default(),
            pins.join("\n"),
            output,
            self.placement_site.clone().unwrap_or_default(),
            secrets,
        ] {
            hasher.update(&(text.len() as u64).to_be_bytes());
            hasher.update(text.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }
}

/// Extracts `aruna://` and `s3://` addresses from workflow values so access, encryption, and
/// remote capture checks cover file-valued parameters under every scheme the coordinator serves.
fn value_refs(
    value: &Value,
    out: &mut Vec<(String, String, Option<Ulid>, Option<NodeId>)>,
) -> Result<(), String> {
    match value {
        Value::String(text) if text.starts_with("aruna://") || text.starts_with("s3://") => {
            let parsed = super::data::parse_address(text).map_err(|error| error.to_string())?;
            out.push(parsed);
        }
        Value::Array(items) => {
            for item in items {
                value_refs(item, out)?;
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                value_refs(item, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Rewrites local unversioned file addresses in `value` with their resolved head versions.
fn pin_versions(value: &mut Value, local: NodeId, versions: &BTreeMap<(String, String), Ulid>) {
    match value {
        Value::String(text) => {
            let scheme = if text.starts_with("aruna://") {
                Some("aruna://")
            } else if text.starts_with("s3://") {
                Some("s3://")
            } else {
                None
            };
            if let Some(scheme) = scheme
                && let Ok((bucket, key, None, node)) = super::data::parse_address(text)
                && node.is_none_or(|n| n == local)
                && let Some(&version) = versions.get(&(bucket.clone(), key.clone()))
            {
                *text = super::data::format_address(scheme, &bucket, &key, Some(version), node);
            }
        }
        Value::Array(items) => {
            for item in items {
                pin_versions(item, local, versions);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                pin_versions(item, local, versions);
            }
        }
        _ => {}
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
    /// Resolved `(tool_id, version)` pairs inherited by a resumed composed run.
    pub used_tools: Vec<(String, String)>,
}

/// A run the caller started, and whether this request created it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedRun {
    pub record: WorkflowRunRecord,
    pub created: bool,
}

/// Upper bound on directory-entry pins captured from directory-valued parameters per run.
const MAX_DIR_PINS: usize = 4096;
/// Objects requested per listing page while capturing local directory parameters.
const DIR_LIST_PAGE: usize = 256;

#[derive(Debug, PartialEq)]
struct DirScan {
    bucket: String,
    group_id: GroupId,
    prefix: String,
    pending: VecDeque<(String, Ulid)>,
    checking: Option<(String, Ulid)>,
    next_token: Option<ListContinuationToken>,
}

#[derive(Debug, PartialEq)]
enum AdmitStep {
    Group,
    Version,
    Key(Box<VersionView>),
    Existing(Box<VersionView>, [u8; 32]),
    Inputs(Box<VersionView>),
    Head(Box<VersionView>, String, String),
    DirVersion(Box<VersionView>, String, String, Ulid),
    DirBucket(Box<VersionView>, String, String),
    DirList(Box<(VersionView, DirScan)>),
    DirAuth(Box<(VersionView, DirScan)>),
    Output(Box<VersionView>),
    Encryption(Box<VersionView>),
    Config(Box<VersionView>, bool),
    Remote(Box<VersionView>, bool, bool),
    Record,
    Submit(Box<WorkflowRunRecord>),
    Job(Box<WorkflowRunRecord>),
    Failed(String),
}

/// Admits one run on this node; see the module comment for the checks, in that order.
#[derive(Debug, PartialEq)]
pub struct StartRunOperation {
    auth: AuthContext,
    token: Option<AuthToken>,
    node: (NodeId, RealmId),
    request: RunRequest,
    lineage: Lineage,
    bad_address: Option<String>,
    pending: Vec<PinnedInput>,
    value_heads: Vec<(String, String)>,
    value_dirs: Vec<(String, String)>,
    value_versions: BTreeMap<(String, String), Ulid>,
    retained_pins: Vec<(String, String, Ulid)>,
    directory_pins: Vec<DirectoryPin>,
    directory_roots: Vec<DirectoryRoot>,
    dir_limit: usize,
    resolved_values: Value,
    pub(super) remote: BTreeMap<NodeId, Vec<(PinnedInput, InputSelection)>>,
    asked: Vec<PinnedInput>,
    pub(super) captured: Vec<PrecapturedInput>,
    ids: IdMint,
    now_ms: u64,
    step: AdmitStep,
}

type AdmitNext = Result<Next<AdmittedRun>, CatalogError>;

impl StartRunOperation {
    pub fn new(auth: &AuthContext, node: (NodeId, RealmId), request: RunRequest) -> Self {
        let mut pending = request.inputs.clone();
        let mut refs = Vec::new();
        let bad_address = value_refs(&request.values, &mut refs).err();
        let mut value_heads = Vec::new();
        let mut value_dirs = Vec::new();
        for (bucket, key, version, source_node_id) in refs {
            if version.is_none() && source_node_id.is_none_or(|n| n == node.0) {
                if key.is_empty() || key.ends_with('/') {
                    if !value_dirs.iter().any(|(b, k)| b == &bucket && k == &key) {
                        value_dirs.push((bucket.clone(), key.clone()));
                    }
                } else if !value_heads.iter().any(|(b, k)| b == &bucket && k == &key) {
                    value_heads.push((bucket.clone(), key.clone()));
                }
            }
            pending.push(PinnedInput {
                name: String::new(),
                bucket,
                key,
                version_id: version.unwrap_or_else(Ulid::nil),
                source_node_id,
            });
        }
        let resolved_values = request.values.clone();
        Self {
            auth: auth.clone(),
            token: None,
            node,
            pending,
            value_heads,
            value_dirs,
            value_versions: BTreeMap::new(),
            retained_pins: Vec::new(),
            directory_pins: Vec::new(),
            directory_roots: Vec::new(),
            dir_limit: MAX_DIR_PINS,
            resolved_values,
            request,
            lineage: Lineage::default(),
            bad_address,
            remote: BTreeMap::new(),
            asked: Vec::new(),
            captured: Vec::new(),
            ids: IdMint::default(),
            now_ms: 0,
            step: AdmitStep::Group,
        }
    }

    /// Supplies the caller's bearer token for resolving remote pinned inputs at admission.
    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }

    /// Carries versioned directory entries and admitted directory roots from an earlier run in
    /// the lineage, rechecking access and exact-version existence for any entries held here.
    pub fn with_directories(mut self, pins: Vec<DirectoryPin>, roots: Vec<DirectoryRoot>) -> Self {
        for pin in &pins {
            if pin.source_node_id.is_none() {
                self.pending.push(PinnedInput {
                    name: String::new(),
                    bucket: pin.bucket.clone(),
                    key: pin.key.clone(),
                    version_id: pin.version_id,
                    source_node_id: None,
                });
                self.retained_pins
                    .push((pin.bucket.clone(), pin.key.clone(), pin.version_id));
            }
        }
        for root in &roots {
            if root.source_node_id.is_none() {
                self.pending.push(PinnedInput {
                    name: String::new(),
                    bucket: root.bucket.clone(),
                    key: root.prefix.trim_end_matches('/').to_owned(),
                    version_id: root.version_id.unwrap_or_else(Ulid::nil),
                    source_node_id: None,
                });
                if let Some(version_id) = root.version_id {
                    self.retained_pins
                        .push((root.bucket.clone(), root.prefix.clone(), version_id));
                }
            }
        }
        self.directory_pins = pins;
        self.directory_roots = roots;
        self
    }

    #[cfg(test)]
    pub(super) fn with_dir_limit(mut self, limit: usize) -> Self {
        self.dir_limit = limit;
        self
    }

    /// Reuses remote inputs already captured and granted for an earlier run in the lineage.
    pub fn with_captured(mut self, captured: Vec<PrecapturedInput>) -> Self {
        self.captured = captured;
        self
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
        self.next_head(view)
    }

    /// Resolves head versions of local unversioned file parameters so cross-node resumes keep
    /// exact object identities.
    fn next_head(&mut self, view: VersionView) -> AdmitNext {
        if let Some((bucket, key)) = self.value_heads.pop() {
            let head = HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: None,
            });
            self.step = AdmitStep::Head(Box::new(view), bucket, key);
            return Ok(Next::Call(IoCall::child(head)));
        }
        if !self.value_versions.is_empty() {
            pin_versions(&mut self.resolved_values, self.node.0, &self.value_versions);
        }
        self.next_retained(view)
    }

    /// Verifies each retained local directory pin still exists at its exact version.
    fn next_retained(&mut self, view: VersionView) -> AdmitNext {
        if let Some((bucket, key, version_id)) = self.retained_pins.pop() {
            let head = HeadObjectOperation::new(HeadObjectInput {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: Some(version_id),
            });
            self.step = AdmitStep::DirVersion(Box::new(view), bucket, key, version_id);
            return Ok(Next::Call(IoCall::child(head)));
        }
        self.next_dir(view)
    }

    /// Captures authorized versioned file entries below each local directory-valued parameter.
    fn next_dir(&mut self, view: VersionView) -> AdmitNext {
        let keep = self.lineage.resumes.is_some() || self.lineage.repeats.is_some();
        while let Some((bucket, key)) = self.value_dirs.pop() {
            let prefix = match key.is_empty() || key.ends_with('/') {
                true => key.clone(),
                false => format!("{key}/"),
            };
            let retained = keep
                && (self.directory_roots.iter().any(|root| {
                    root.source_node_id.is_none() && root.bucket == bucket && root.prefix == prefix
                }) || self.directory_pins.iter().any(|pin| {
                    pin.source_node_id.is_none()
                        && pin.bucket == bucket
                        && pin.key.starts_with(&prefix)
                }));
            if retained {
                continue;
            }
            let access = ObjectAccessOperation::new(
                &self.auth,
                self.node.0,
                (&bucket, key.trim_end_matches('/')),
                Permission::READ,
            );
            self.step = AdmitStep::DirBucket(Box::new(view), bucket, key);
            return Ok(Next::Call(IoCall::child(access)));
        }
        let local = self.node.0;
        let Some(output) = self.request.output.clone() else {
            return self.encryption(view);
        };
        let access = ObjectAccessOperation::new(
            &self.auth,
            local,
            (&output.bucket, output.prefix.trim_end_matches('/')),
            Permission::WRITE,
        );
        self.step = AdmitStep::Output(Box::new(view));
        Ok(Next::Call(IoCall::child(access)))
    }

    fn list_dir(&mut self, view: VersionView, mut scan: DirScan) -> AdmitNext {
        let token = scan.next_token.take();
        let list = ListBucketOperation::new(ListBucketInput {
            bucket: scan.bucket.clone(),
            group_id: scan.group_id,
            continuation_token: token,
            max_keys: Some(DIR_LIST_PAGE),
            prefix: (!scan.prefix.is_empty()).then(|| scan.prefix.clone()),
            delimiter: None,
            start_after: None,
        });
        self.step = AdmitStep::DirList(Box::new((view, scan)));
        Ok(Next::Call(IoCall::child(list)))
    }

    fn next_dir_auth(&mut self, view: VersionView, mut scan: DirScan) -> AdmitNext {
        if let Some((key, version_id)) = scan.pending.pop_front() {
            let path = object_permission_path(
                self.auth.realm_id,
                scan.group_id,
                self.node.0,
                &scan.bucket,
                &key,
            );
            let check = AuthorizeOperation::new(
                self.auth.realm_id,
                &self.auth,
                &path,
                &Permission::READ,
                PolicyRequestExtras::rest(),
            );
            scan.checking = Some((key, version_id));
            self.step = AdmitStep::DirAuth(Box::new((view, scan)));
            return Ok(Next::Call(IoCall::child(check)));
        }
        if scan.next_token.is_some() {
            return self.list_dir(view, scan);
        }
        self.next_dir(view)
    }

    /// Reads whether local input buckets (from pins and `values`) and the output bucket encrypt.
    fn encryption(&mut self, view: VersionView) -> AdmitNext {
        let local = self.node.0;
        let mut refs = Vec::new();
        value_refs(&self.request.values, &mut refs).map_err(CatalogError::Invalid)?;
        let mut buckets: Vec<String> = (self.request.inputs.iter())
            .filter(|input| input.source_node_id.is_none_or(|node| node == local))
            .map(|input| input.bucket.clone())
            .chain(
                refs.into_iter()
                    .filter(|(_, _, _, node)| node.is_none_or(|n| n == local))
                    .map(|(bucket, _, _, _)| bucket),
            )
            .collect();
        buckets.sort_unstable();
        buckets.dedup();
        let output = self
            .request
            .output
            .as_ref()
            .map(|output| output.bucket.clone());
        let rows = (buckets.into_iter().chain(output))
            .map(|bucket| {
                let key = Key::from(bucket.into_bytes());
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

    fn record(&mut self, view: VersionView, protected: bool, local_only: bool) -> AdmitNext {
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
            values_json: self.resolved_values.to_string(),
            inputs_json: request.inputs_json.clone(),
            output: request.output.clone(),
            outputs: Vec::new(),
            provenance_crate: request.provenance_crate,
            provenance_document_id: None,
            provenance_status: None,
            last_event_seq: 0,
            local_only,
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 0,
            steps: Vec::new(),
            resumes_run_id: self.lineage.resumes,
            repeats_run_id: self.lineage.repeats,
            cache_scope: match protected || self.auth.path_restrictions.is_some() {
                true => CacheScope::Run,
                false => self.lineage.cache_scope,
            },
            placement_site: request.placement_site.clone(),
            protected,
            started_at_ms: None,
            secrets: request.secrets.clone(),
            path_restrictions: self.auth.path_restrictions.clone(),
            captured_remote: std::mem::take(&mut self.captured),
            directory_pins: std::mem::take(&mut self.directory_pins),
            directory_roots: std::mem::take(&mut self.directory_roots),
            used_tools: self.lineage.used_tools.clone(),
        };
        let key = request.idempotency_key.as_ref().map(|key| {
            let row = self.key_row(key);
            (row, request.digest())
        });
        self.step = AdmitStep::Record;
        let store = RecordRunOperation::new(record, key);
        Ok(Next::Call(IoCall::retry(store, &run_id.to_bytes())))
    }

    fn next_remote(&mut self, view: VersionView, protected: bool, local_only: bool) -> AdmitNext {
        let Some((node, pairs)) = self.remote.pop_first() else {
            return self.record(view, protected, local_only);
        };
        let (pins, inputs): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
        self.asked = pins;
        let scope = SubmissionScope::Unkeyed(Ulid::nil());
        let caller = ResolveCaller {
            created_by: self.auth.user_id,
            submission_id: job_request::submission_of(self.auth.user_id, &scope),
            expires_at: self.now_ms.saturating_add(RETENTION_MS) / 1000,
            disallow_encrypted: !protected,
        };
        let resolve = RemoteInputsOperation::new(node, inputs, caller, self.token.clone());
        self.step = AdmitStep::Remote(Box::new(view), protected, local_only);
        Ok(Next::Call(IoCall::child(resolve)))
    }

    pub(super) fn queue_remote(
        &mut self,
        cfg: &RealmConfigDocument,
        local_only: bool,
    ) -> Result<(), CatalogError> {
        if local_only {
            return Ok(());
        }
        let local = self.node.0;
        let now_secs = self.now_ms / 1000;
        let mut pins = self.request.inputs.clone();
        let mut refs = Vec::new();
        value_refs(&self.request.values, &mut refs).map_err(CatalogError::Invalid)?;
        for (bucket, key, version, source_node_id) in refs {
            if let Some(version_id) = version {
                pins.push(PinnedInput {
                    name: String::new(),
                    bucket,
                    key,
                    version_id,
                    source_node_id,
                });
            }
        }
        for pin in &self.directory_pins {
            pins.push(PinnedInput {
                name: String::new(),
                bucket: pin.bucket.clone(),
                key: pin.key.clone(),
                version_id: pin.version_id,
                source_node_id: pin.source_node_id,
            });
        }
        if self.token.is_some() || self.lineage.resumes.is_none() {
            self.captured.retain(|entry| {
                entry
                    .captured
                    .grant
                    .as_ref()
                    .is_none_or(|g| g.grant.expires_at > now_secs)
            });
        }
        for root in &self.directory_roots {
            if let Some(node) = root.source_node_id.filter(|n| *n != local) {
                if !cfg
                    .node_kind(&node)
                    .is_some_and(|kind| kind.is_sync_eligible())
                {
                    return Err(CatalogError::Invalid(format!(
                        "input node {node} is not an eligible realm server"
                    )));
                }
                if let Some(version_id) = root.version_id {
                    pins.push(PinnedInput {
                        name: String::new(),
                        bucket: root.bucket.clone(),
                        key: root.prefix.clone(),
                        version_id,
                        source_node_id: Some(node),
                    });
                }
            }
        }
        for pin in pins {
            let Some(node) = pin.source_node_id.filter(|n| *n != local) else {
                continue;
            };
            if !cfg
                .node_kind(&node)
                .is_some_and(|kind| kind.is_sync_eligible())
            {
                return Err(CatalogError::Invalid(format!(
                    "input node {node} is not an eligible realm server"
                )));
            }
            let already = self.captured.iter().any(|entry| {
                entry.pin_node_id == node
                    && entry.pin_bucket == pin.bucket
                    && entry.pin_key == pin.key
                    && entry.pin_version_id == pin.version_id
                    && entry
                        .captured
                        .grant
                        .as_ref()
                        .is_none_or(|g| g.grant.expires_at > now_secs)
            });
            if already {
                continue;
            }
            if self.token.is_none() {
                if self.lineage.resumes.is_some() {
                    continue;
                }
                return Err(CatalogError::Invalid(
                    "an input on another node requires a bearer token".into(),
                ));
            }
            let dest_key = format!("{}/{}", pin.bucket, pin.key);
            let version_text = pin.version_id.to_string();
            let selection = InputSelection {
                source: InputSource::S3 {
                    bucket: pin.bucket.clone(),
                    key: pin.key.clone(),
                    version_id: Some(version_text.clone()),
                },
                source_node_id: Some(node),
                dest_key: dest_key.clone(),
                mode: InputMode::Snapshot,
                container_path: Some(format!("/{dest_key}")),
                name: None,
                description: None,
            };
            let entry = self.remote.entry(node).or_default();
            let duplicate = entry.iter().any(|(existing, _)| {
                existing.bucket == pin.bucket
                    && existing.key == pin.key
                    && existing.version_id == pin.version_id
            });
            if !duplicate {
                entry.push((pin, selection));
            }
        }
        Ok(())
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
        let serving = (!record.local_only).then_some(self.node);
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
        if let Some(error) = self.bad_address.take() {
            return Err(CatalogError::Invalid(error));
        }
        let Some(output) = &self.request.output else {
            return Err(CatalogError::Invalid(
                "a run needs an output bucket: its task files are kept below the output prefix"
                    .into(),
            ));
        };
        if output.node_id.is_some_and(|node| node != self.node.0) {
            return Err(CatalogError::Invalid(
                "output node must be the local node".into(),
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
            AdmitStep::Head(view, bucket, key) => {
                match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => {
                        if let Some(version) = head.resolved_version_id.or(head.version_id) {
                            self.value_versions.insert((bucket, key), version);
                        }
                    }
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => {
                        if !self
                            .value_dirs
                            .iter()
                            .any(|(b, k)| b == &bucket && k == &key)
                        {
                            self.value_dirs.push((bucket, key));
                        }
                    }
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                }
                self.next_head(*view)
            }
            AdmitStep::DirVersion(view, bucket, key, version_id) => {
                match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(_) => self.next_retained(*view),
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => Err(CatalogError::Invalid(format!(
                        "pinned directory object {bucket}/{key} version {version_id} is unavailable"
                    ))),
                    Err(error) => Err(CatalogError::Storage(error.to_string())),
                }
            }
            AdmitStep::DirBucket(view, bucket, key) => match super::data::bucket_answer(result) {
                Ok(info) => {
                    let prefix = match key.is_empty() || key.ends_with('/') {
                        true => key,
                        false => format!("{key}/"),
                    };
                    if !self.directory_roots.iter().any(|root| {
                        root.source_node_id.is_none()
                            && root.bucket == bucket
                            && root.prefix == prefix
                    }) {
                        self.directory_roots.push(DirectoryRoot {
                            bucket: bucket.clone(),
                            prefix: prefix.clone(),
                            version_id: None,
                            source_node_id: None,
                        });
                    }
                    let scan = DirScan {
                        bucket,
                        group_id: info.group_id,
                        prefix,
                        pending: VecDeque::new(),
                        checking: None,
                        next_token: None,
                    };
                    self.list_dir(*view, scan)
                }
                Err(CatalogError::NotFound | CatalogError::Forbidden) => self.next_dir(*view),
                Err(error) => Err(error),
            },
            AdmitStep::DirList(state) => {
                let (view, mut scan) = *state;
                let listed = result
                    .child::<ListBucketResult, ListBucketError>()?
                    .map_err(|error| CatalogError::Storage(error.to_string()))?;
                for object in listed.objects {
                    scan.pending.push_back((object.head.key, object.version_id));
                }
                scan.next_token = listed.continuation_token;
                self.next_dir_auth(view, scan)
            }
            AdmitStep::DirAuth(state) => {
                let (view, mut scan) = *state;
                let checking = scan.checking.take();
                match result.child::<(), AuthorizeError>()? {
                    Ok(()) => {
                        if let Some((key, version_id)) = checking {
                            if key.ends_with('/') {
                                if let Some(existing) =
                                    self.directory_roots.iter_mut().find(|root| {
                                        root.source_node_id.is_none()
                                            && root.bucket == scan.bucket
                                            && root.prefix == key
                                    })
                                {
                                    existing.version_id = Some(version_id);
                                } else {
                                    if self.directory_roots.len() >= self.dir_limit {
                                        return Err(CatalogError::Invalid(format!(
                                            "directory inputs exceed the maximum of {} captured directories",
                                            self.dir_limit
                                        )));
                                    }
                                    self.directory_roots.push(DirectoryRoot {
                                        bucket: scan.bucket.clone(),
                                        prefix: key,
                                        version_id: Some(version_id),
                                        source_node_id: None,
                                    });
                                }
                            } else if !self.directory_pins.iter().any(|pin| {
                                pin.source_node_id.is_none()
                                    && pin.bucket == scan.bucket
                                    && pin.key == key
                            }) {
                                if self.directory_pins.len() >= self.dir_limit {
                                    return Err(CatalogError::Invalid(format!(
                                        "directory inputs exceed the maximum of {} captured files",
                                        self.dir_limit
                                    )));
                                }
                                self.directory_pins.push(DirectoryPin {
                                    bucket: scan.bucket.clone(),
                                    key,
                                    version_id,
                                    source_node_id: None,
                                });
                            }
                        }
                    }
                    Err(
                        AuthorizeError::PermissionDenied
                        | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
                    ) => {}
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                }
                self.next_dir_auth(view, scan)
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
                self.step = AdmitStep::Config(view, protected);
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.node.1,
                ))))
            }
            AdmitStep::Config(view, protected) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let cfg = config.map_err(|error| CatalogError::Storage(error.to_string()))?;
                let kind = node_kind(&cfg, self.node.0).ok_or_else(|| {
                    CatalogError::Storage("this node is not in the realm configuration".into())
                })?;
                let local_only = match kind.owner() {
                    Some(owner) if owner != self.auth.user_id => {
                        return Err(CatalogError::Forbidden);
                    }
                    Some(_) => true,
                    None => false,
                };
                if local_only
                    && self
                        .request
                        .secrets
                        .values()
                        .any(|binding| binding.scope != WorkflowSecretScope::Node)
                {
                    return Err(CatalogError::Unsupported(
                        "user, group, and realm vault secrets require a realm server".into(),
                    ));
                }
                self.queue_remote(&cfg, local_only)?;
                self.next_remote(*view, protected, local_only)
            }
            AdmitStep::Remote(view, protected, local_only) => {
                type Answers = Vec<(InputSelection, CapturedInput)>;
                let answers = result.child::<Answers, SubmitJobError>()?;
                let asked = std::mem::take(&mut self.asked);
                match answers {
                    Ok(answers) => {
                        for (pin, (resolved, captured)) in asked.into_iter().zip(answers) {
                            let Some(node) = pin.source_node_id else {
                                continue;
                            };
                            self.captured.retain(|entry| {
                                !(entry.pin_node_id == node
                                    && entry.pin_bucket == pin.bucket
                                    && entry.pin_key == pin.key
                                    && entry.pin_version_id == pin.version_id)
                            });
                            self.captured.push(PrecapturedInput {
                                pin_node_id: node,
                                pin_bucket: pin.bucket,
                                pin_key: pin.key,
                                pin_version_id: pin.version_id,
                                resolved,
                                captured,
                            });
                        }
                    }
                    Err(SubmitJobError::AuthorityDenied | SubmitJobError::InputDenied(_)) => {
                        return Err(CatalogError::Forbidden);
                    }
                    Err(SubmitJobError::InvalidWorkspace(reason)) => {
                        return Err(CatalogError::Invalid(reason));
                    }
                    Err(_) if self.lineage.resumes.is_some() => {}
                    Err(other) => return Err(CatalogError::Storage(other.to_string())),
                }
                self.next_remote(*view, protected, local_only)
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
    found_digest: Option<[u8; 32]>,
    stage: u8,
}

impl RecordRunOperation {
    pub fn new(record: WorkflowRunRecord, key: Option<(Key, [u8; 32])>) -> Self {
        Self {
            record,
            key,
            found_digest: None,
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
                self.found_digest = Some(known.request_digest);
                self.stage = 2;
                Ok(Next::Call(super::read_run(known.run_id)))
            }
            2 => match super::run_answer(result)? {
                Some(record) if self.found_digest == self.key.as_ref().map(|(_, d)| *d) => {
                    Ok(Next::Done(AdmittedRun {
                        record,
                        created: false,
                    }))
                }
                Some(_) => Err(CatalogError::Conflict(
                    "the idempotency key started another run with other values".into(),
                )),
                None => self.write(),
            },
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
