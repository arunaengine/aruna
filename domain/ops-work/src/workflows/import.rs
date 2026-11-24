//! Imports workflow versions from a composed text, uploads, wir, bucket objects or Git, checks
//! them with a preflight coordinator, pins their images and stores them in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::keyspaces::{
    WORKFLOW_KEYSPACE, WORKFLOW_SOURCE_KEYSPACE, WORKFLOW_VERSION_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::execution::workflow::{
    StoredFile, StoredWorkflowLanguage, ToolRecord, WorkflowFiles, WorkflowInspection,
    WorkflowRecord, WorkflowVersionRecord, WorkflowVisibility, version_key, workflow_key,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::types::GroupId;
use aruna_core::workflow::source_digest;
use aruna_ops_data::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult,
};
use aruna_ops_data::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_workflow_contract::preflight::{
    ComposedLowering, Severity, SourceFile, WorkflowSource, check_path,
};
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde_json::Value;
use ulid::Ulid;

use super::{
    Access, CatalogError, CheckReport, GraphView, GroupAccessOperation, MAX_SOURCE_BYTES, Need,
    ScanOperation, WorkflowAccessOperation, access_answer, diagnostic, preflight_answer,
    preflight_call, refused, scan_answer, to_text,
};

/// The entry file of a composed workflow.
pub const COMPOSED_ENTRY: &str = "workflow.uwe.yaml";
/// The entry file of a wir document, and of the wir a composed workflow lowers to.
pub const WIR_ENTRY: &str = "workflow.wir.json";

/// Where the files of a new version come from.
#[derive(Clone, Debug, PartialEq)]
pub enum SourceInput {
    /// A composed `.uwe.yaml` text whose steps name tools of the group's catalog.
    UweYaml(String),
    Upload {
        files: Vec<SourceFile>,
        entrypoint: String,
    },
    Wir(Value),
    Objects(ObjectsInput),
    Git(GitInput),
}

/// Objects of a bucket on this node, named by key or by a prefix.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectsInput {
    pub bucket: String,
    pub prefix: Option<String>,
    pub keys: Vec<String>,
    pub node_id: Option<NodeId>,
    pub entrypoint: Option<String>,
}

/// A Git repository at a revision.
#[derive(Clone, Debug, PartialEq)]
pub struct GitInput {
    pub url: String,
    pub revision: Option<String>,
    pub path: Option<String>,
    pub entrypoint: Option<String>,
}

/// A new workflow with its first version.
#[derive(Clone, Debug, PartialEq)]
pub struct NewWorkflow {
    pub group_id: GroupId,
    pub name: String,
    pub description: Option<String>,
    pub language: Option<StoredWorkflowLanguage>,
    pub visibility: WorkflowVisibility,
    pub source: SourceInput,
}

/// What an import stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Created {
    pub workflow_id: Ulid,
    pub version: u32,
    pub commit: Option<String>,
    pub digest: String,
}

/// A checked version, ready to store.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Prepared {
    pub language: Option<StoredWorkflowLanguage>,
    pub entrypoint: String,
    pub files: Vec<StoredFile>,
    pub commit: Option<String>,
    pub uwe_yaml: Option<String>,
    pub images: BTreeMap<String, String>,
    pub inspection: WorkflowInspection,
}

/// Refuses a Git URL that is no plain HTTPS address and a revision or path that is no name.
pub fn check_git(git: &GitInput) -> Result<(), CatalogError> {
    let url = url::Url::parse(&git.url)
        .map_err(|error| CatalogError::Invalid(format!("the Git URL: {error}")))?;
    if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty() {
        return Err(CatalogError::Invalid(
            "the Git URL must be an https address without credentials".into(),
        ));
    }
    let named = |text: &Option<String>| {
        (text.as_deref()).is_none_or(|text| {
            !text.is_empty() && !text.starts_with('-') && !text.chars().any(char::is_whitespace)
        })
    };
    if !named(&git.revision) {
        return Err(CatalogError::Invalid("the Git revision is no name".into()));
    }
    if let Some(path) = git.path.as_deref().or(git.entrypoint.as_deref()) {
        check_path(path).map_err(|error| CatalogError::Invalid(error.to_string()))?;
    }
    Err(CatalogError::Unsupported(
        "Git imports need a staging job that fetches the repository; this node does not run \
         one yet, so import the files from a bucket or upload them"
            .into(),
    ))
}

/// Checks paths, the entry file and the size of an uploaded source.
pub(crate) fn checked_files(
    files: Vec<SourceFile>,
    entrypoint: &str,
) -> Result<Vec<SourceFile>, CatalogError> {
    let source = WorkflowSource { files };
    source
        .validate()
        .map_err(|error| CatalogError::Invalid(error.to_string()))?;
    if source.file(entrypoint).is_none() {
        return Err(CatalogError::Invalid(format!(
            "the entry file {entrypoint} is not among the files"
        )));
    }
    let size: u64 = (source.files.iter())
        .map(|file| file.content.len() as u64)
        .sum();
    if size > MAX_SOURCE_BYTES {
        return Err(CatalogError::Invalid(format!(
            "the files hold {size} bytes, above the limit of {MAX_SOURCE_BYTES}"
        )));
    }
    Ok(source.files)
}

fn text_file(path: &str, text: String) -> SourceFile {
    SourceFile {
        path: path.into(),
        content: text.into_bytes(),
        executable: false,
    }
}

/// The report of uwe's support check, with every refused part and problem as a diagnostic.
pub fn support_report(runnable: bool, report: &Value) -> CheckReport {
    let mut diagnostics = Vec::new();
    for problem in report["problems"].as_array().into_iter().flatten() {
        let severity = match problem["kind"].as_str() {
            Some("warning") => Severity::Warning,
            _ => Severity::Error,
        };
        let message = problem["message"].as_str().unwrap_or("a problem");
        diagnostics.push(diagnostic(severity, message));
    }
    for part in report["parts"].as_array().into_iter().flatten() {
        if part["support"] != "unsupported" {
            continue;
        }
        let name = part["name"].as_str().unwrap_or_default();
        let reason = part["reason"].as_str().unwrap_or("not supported");
        let mut refused = diagnostic(Severity::Error, format!("{name}: {reason}"));
        refused.pointer.node = Some(name.to_owned());
        diagnostics.push(refused);
    }
    for note in report["notes"].as_array().into_iter().flatten() {
        diagnostics.push(diagnostic(
            Severity::Info,
            note.as_str().unwrap_or_default(),
        ));
    }
    CheckReport {
        ok: runnable,
        diagnostics,
    }
}

/// The check report of a composed lowering: it passes when it lowered without errors.
pub fn lowering_report(lowering: &ComposedLowering) -> CheckReport {
    let errors = (lowering.diagnostics.iter()).any(|found| found.severity == Severity::Error);
    CheckReport {
        ok: lowering.wir_json.is_some() && !errors,
        diagnostics: lowering.diagnostics.clone(),
    }
}

/// The tools of a group's catalog as the coordinator takes them: definitions, and the files
/// their sources name, below each tool's own folder.
pub fn catalog_tools(tools: &[ToolRecord]) -> Result<(Vec<Value>, WorkflowSource), CatalogError> {
    let mut definitions = Vec::with_capacity(tools.len());
    let mut files = Vec::new();
    for tool in tools {
        definitions.push(super::from_text(&tool.definition)?);
        files.extend(tool.files.iter().map(StoredFile::contract));
    }
    Ok((definitions, WorkflowSource { files }))
}

#[derive(Debug, PartialEq)]
enum PrepareStage {
    Tools,
    Objects,
    Lower,
    Check,
    Inspect,
    Select,
    Pin { backend: String },
}

/// Resolves a source to files and checks them: a composed text is lowered with the group's
/// tools, any other workflow is checked and inspected. Images are pinned on this node's backend.
#[derive(Debug, PartialEq)]
pub struct PrepareVersionOperation {
    auth: AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    source: Option<SourceInput>,
    prepared: Prepared,
    report: CheckReport,
    graph: Option<GraphView>,
    schema: Option<Value>,
    unpinned: Vec<String>,
    stage: PrepareStage,
}

impl PrepareVersionOperation {
    /// `language` is the workflow's for a new version, or the one a new workflow states.
    pub fn new(
        (auth, node_id): (&AuthContext, NodeId),
        group_id: GroupId,
        language: Option<StoredWorkflowLanguage>,
        source: SourceInput,
    ) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            group_id,
            source: Some(source),
            prepared: Prepared {
                language,
                ..Prepared::default()
            },
            report: CheckReport::default(),
            graph: None,
            schema: None,
            unpinned: Vec::new(),
            stage: PrepareStage::Check,
        }
    }

    fn source(&self) -> WorkflowSource {
        WorkflowSource {
            files: self
                .prepared
                .files
                .iter()
                .map(StoredFile::contract)
                .collect(),
        }
    }

    fn check(&mut self, files: Vec<SourceFile>, entrypoint: String) -> Next<Prepared> {
        self.prepared.files = files.into_iter().map(StoredFile::from).collect();
        self.prepared.entrypoint = entrypoint;
        self.stage = PrepareStage::Check;
        Next::Call(preflight_call(PreflightKind::Check {
            source: self.source(),
            language: self
                .prepared
                .language
                .and_then(StoredWorkflowLanguage::contract),
            entrypoint: self.prepared.entrypoint.clone(),
        }))
    }

    fn inspect(
        &mut self,
        language: Option<StoredWorkflowLanguage>,
        entrypoint: &str,
    ) -> Next<Prepared> {
        self.stage = PrepareStage::Inspect;
        Next::Call(preflight_call(PreflightKind::Inspect {
            source: self.source(),
            language: language.and_then(StoredWorkflowLanguage::contract),
            entrypoint: entrypoint.to_owned(),
        }))
    }

    /// Asks this node's backend for the images, or finishes when there are none.
    fn select(&mut self) -> Result<Next<Prepared>, CatalogError> {
        if self.unpinned.is_empty() {
            return self.finish();
        }
        self.stage = PrepareStage::Select;
        let select = ComputeEffect::Select {
            backend: None,
            constraint: None,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Compute(
            Box::new(select),
        )))))
    }

    fn pin_next(&mut self, backend: String) -> Result<Next<Prepared>, CatalogError> {
        let Some(image) = self.unpinned.last().cloned() else {
            return self.finish();
        };
        let resolve = ComputeEffect::ResolveImage {
            backend: backend.clone(),
            image,
        };
        self.stage = PrepareStage::Pin { backend };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Compute(
            Box::new(resolve),
        )))))
    }

    fn finish(&mut self) -> Result<Next<Prepared>, CatalogError> {
        for image in std::mem::take(&mut self.unpinned) {
            let message =
                format!("the image {image} stays unpinned: no backend of this node pins it");
            self.report
                .diagnostics
                .push(diagnostic(Severity::Info, message));
        }
        let inspection = &mut self.prepared.inspection;
        inspection.check = Some(to_text(&self.report)?);
        inspection.graph = self.graph.as_ref().map(to_text).transpose()?;
        inspection.schema = self.schema.as_ref().map(to_text).transpose()?;
        Ok(Next::Done(std::mem::take(&mut self.prepared)))
    }

    fn start(&mut self, source: SourceInput) -> Result<Next<Prepared>, CatalogError> {
        let language = self.prepared.language;
        match source {
            SourceInput::UweYaml(text) => {
                if language.is_some_and(|language| language != StoredWorkflowLanguage::Uwe) {
                    return Err(language_mismatch());
                }
                self.prepared.language = Some(StoredWorkflowLanguage::Uwe);
                self.prepared.entrypoint = COMPOSED_ENTRY.into();
                self.prepared.files = vec![text_file(COMPOSED_ENTRY, text.clone()).into()];
                self.prepared.uwe_yaml = Some(text);
                self.stage = PrepareStage::Tools;
                let groups = Some([self.group_id].into());
                Ok(Next::Call(IoCall::child(
                    ScanOperation::<ToolRecord>::of_groups(groups),
                )))
            }
            SourceInput::Upload { files, entrypoint } => {
                let files = checked_files(files, &entrypoint)?;
                self.check_known(files, entrypoint)
            }
            SourceInput::Wir(document) => {
                if language.is_some_and(|language| language != StoredWorkflowLanguage::Wir) {
                    return Err(language_mismatch());
                }
                self.prepared.language = Some(StoredWorkflowLanguage::Wir);
                let text = super::to_text(&document)?;
                self.check_known(vec![text_file(WIR_ENTRY, text)], WIR_ENTRY.into())
            }
            SourceInput::Objects(objects) => {
                if objects.node_id.is_some_and(|node| node != self.node_id) {
                    return Err(CatalogError::Invalid(
                        "the bucket is read on the node that holds it; send the import there"
                            .into(),
                    ));
                }
                self.stage = PrepareStage::Objects;
                let read = ObjectFilesOperation::new(&self.auth, self.node_id, objects);
                Ok(Next::Call(IoCall::child(read)))
            }
            SourceInput::Git(git) => Err(check_git(&git).err().unwrap_or_else(|| {
                CatalogError::Unsupported("Git sources are not read here".into())
            })),
        }
    }

    /// Checks files of any language but a composed one, which only a `.uwe.yaml` text gives.
    fn check_known(
        &mut self,
        files: Vec<SourceFile>,
        entrypoint: String,
    ) -> Result<Next<Prepared>, CatalogError> {
        if self.prepared.language == Some(StoredWorkflowLanguage::Uwe) {
            return Err(language_mismatch());
        }
        Ok(self.check(files, entrypoint))
    }
}

fn language_mismatch() -> CatalogError {
    CatalogError::Invalid(
        "a composed workflow takes only .uwe.yaml versions, and a .uwe.yaml text only a \
         composed workflow"
            .into(),
    )
}

impl Operation for PrepareVersionOperation {
    type Output = Prepared;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Prepared>, CatalogError> {
        let source = self.source.take().ok_or(IoError::Unfinished)?;
        self.start(source)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Prepared>, CatalogError> {
        match std::mem::replace(&mut self.stage, PrepareStage::Check) {
            PrepareStage::Tools => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                let (catalog_tools, tool_files) = catalog_tools(&tools)?;
                self.stage = PrepareStage::Lower;
                Ok(Next::Call(preflight_call(PreflightKind::LowerComposed {
                    uwe_yaml: self.prepared.uwe_yaml.clone().unwrap_or_default(),
                    catalog_tools,
                    tool_files,
                })))
            }
            PrepareStage::Objects => {
                let (files, entrypoint) =
                    result.child::<(Vec<SourceFile>, String), CatalogError>()??;
                let files = checked_files(files, &entrypoint)?;
                self.check_known(files, entrypoint)
            }
            PrepareStage::Lower => {
                let lowering = match preflight_answer(result)?.map_err(refused)? {
                    PreflightResult::LowerComposed(lowering) => lowering,
                    _ => return Err(super::unexpected("PreflightResult::LowerComposed")),
                };
                self.report = lowering_report(&lowering);
                self.graph = Some(GraphView {
                    graph: lowering.graph.clone(),
                    complete: true,
                });
                let Some(wir) = lowering.wir_json else {
                    return self.finish();
                };
                // The lowered wir sits beside the text, so the version can be inspected and run.
                self.prepared
                    .files
                    .push(text_file(WIR_ENTRY, to_text(&wir)?).into());
                Ok(self.inspect(Some(StoredWorkflowLanguage::Wir), WIR_ENTRY))
            }
            PrepareStage::Check => {
                let (runnable, report_json, images) =
                    match preflight_answer(result)?.map_err(refused)? {
                        PreflightResult::Check {
                            runnable,
                            report_json,
                            images,
                        } => (runnable, report_json, images),
                        _ => return Err(super::unexpected("PreflightResult::Check")),
                    };
                self.report = support_report(runnable, &report_json);
                let detected =
                    (report_json["language"].as_str()).and_then(StoredWorkflowLanguage::parse);
                self.prepared.language = self.prepared.language.or(detected);
                self.unpinned = images;
                let entrypoint = self.prepared.entrypoint.clone();
                Ok(self.inspect(self.prepared.language, &entrypoint))
            }
            PrepareStage::Inspect => {
                match preflight_answer(result)? {
                    Ok(PreflightResult::Inspect { form_json, graph }) => {
                        self.schema = Some(form_json["schema"].clone());
                        if self.graph.is_none() {
                            self.graph = Some(GraphView {
                                graph,
                                complete: false,
                            });
                        }
                    }
                    Ok(_) => return Err(super::unexpected("PreflightResult::Inspect")),
                    Err(error @ aruna_core::workflow::WorkflowError::Refused(_)) => {
                        let message = format!("the inputs could not be read: {error}");
                        self.report
                            .diagnostics
                            .push(diagnostic(Severity::Warning, message));
                    }
                    Err(error) => return Err(CatalogError::Coordinator(error)),
                }
                self.select()
            }
            PrepareStage::Select => match compute_answer(result)? {
                ComputeAnswer::Selected(Some(facts)) => self.pin_next(facts.name),
                _ => self.finish(),
            },
            PrepareStage::Pin { backend } => {
                let image = self.unpinned.pop().ok_or(IoError::Unfinished)?;
                match compute_answer(result)? {
                    ComputeAnswer::Image(Ok(pinned)) => {
                        self.prepared.images.insert(image, pinned);
                    }
                    ComputeAnswer::Image(Err(error)) => {
                        let message = format!("the image {image} stays unpinned: {error}");
                        self.report
                            .diagnostics
                            .push(diagnostic(Severity::Warning, message));
                    }
                    _ => return Err(super::unexpected("ComputeAnswer::Image")),
                }
                self.pin_next(backend)
            }
        }
    }
}

fn compute_answer(result: IoResult) -> Result<ComputeAnswer, CatalogError> {
    match result.event()? {
        Event::Compute(answer) => Ok(*answer),
        _ => Err(super::unexpected("Event::Compute")),
    }
}

#[derive(Debug, PartialEq)]
enum ObjectStage {
    Bucket,
    List,
    Allowed,
    Get,
    Collect,
}

/// Reads objects of a bucket on this node into workflow files, each under the caller's READ, as
/// an S3 GET would be decided. Paths are the keys below the prefix.
#[derive(Debug, PartialEq)]
pub struct ObjectFilesOperation {
    auth: AuthContext,
    node_id: NodeId,
    input: ObjectsInput,
    group_id: Option<GroupId>,
    keys: Vec<String>,
    files: Vec<SourceFile>,
    read_bytes: u64,
    stage: ObjectStage,
}

impl ObjectFilesOperation {
    pub fn new(auth: &AuthContext, node_id: NodeId, input: ObjectsInput) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            input,
            group_id: None,
            keys: Vec::new(),
            files: Vec::new(),
            read_bytes: 0,
            stage: ObjectStage::Bucket,
        }
    }

    fn list(
        &mut self,
        token: Option<ListContinuationToken>,
    ) -> Result<Next<ObjectFiles>, CatalogError> {
        self.stage = ObjectStage::List;
        let group_id = self.group_id.ok_or(IoError::Unfinished)?;
        let list = ListBucketOperation::new(ListBucketInput {
            bucket: self.input.bucket.clone(),
            group_id,
            continuation_token: token,
            max_keys: Some(ListBucketOperation::DEFAULT_MAX_KEYS),
            prefix: self.input.prefix.clone(),
            delimiter: None,
            start_after: None,
        });
        Ok(Next::Call(IoCall::child(list)))
    }

    /// Checks READ on the next key, or ends with the files read.
    fn next_key(&mut self) -> Result<Next<ObjectFiles>, CatalogError> {
        let Some(key) = self.keys.last() else {
            let entrypoint = match (&self.input.entrypoint, self.files.as_slice()) {
                (Some(entry), _) => entry.clone(),
                (None, [only]) => only.path.clone(),
                (None, _) => {
                    return Err(CatalogError::Invalid(
                        "name the entry file of a workflow with several files".into(),
                    ));
                }
            };
            return Ok(Next::Done((std::mem::take(&mut self.files), entrypoint)));
        };
        let group_id = self.group_id.ok_or(IoError::Unfinished)?;
        let path = object_permission_path(
            self.auth.realm_id,
            group_id,
            self.node_id,
            &self.input.bucket,
            key,
        );
        self.stage = ObjectStage::Allowed;
        let check = AuthorizeOperation::new(
            self.auth.realm_id,
            &self.auth,
            &path,
            &Permission::READ,
            PolicyRequestExtras::rest(),
        );
        Ok(Next::Call(IoCall::child(check)))
    }

    /// The file path of a key: the key below the prefix.
    fn path_of(&self, key: &str) -> String {
        let prefix = self.input.prefix.as_deref().unwrap_or_default();
        key.strip_prefix(prefix)
            .unwrap_or(key)
            .trim_start_matches('/')
            .to_owned()
    }
}

type ObjectFiles = (Vec<SourceFile>, String);

impl Operation for ObjectFilesOperation {
    type Output = ObjectFiles;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ObjectFiles>, CatalogError> {
        let bucket = GetBucketOperation::new(self.input.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectFiles>, CatalogError> {
        match self.stage {
            ObjectStage::Bucket => {
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => info,
                    Err(GetBucketError::NotFound) => return Err(CatalogError::NotFound),
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                };
                self.group_id = Some(info.group_id);
                if self.input.keys.is_empty() {
                    return self.list(None);
                }
                self.keys = self.input.keys.iter().rev().cloned().collect();
                self.next_key()
            }
            ObjectStage::List => {
                let listed = result.child::<ListBucketResult, ListBucketError>()?;
                let listed = listed.map_err(|error| CatalogError::Storage(error.to_string()))?;
                self.keys
                    .extend(listed.objects.into_iter().map(|object| object.head.key));
                if let Some(token) = listed.continuation_token {
                    return self.list(Some(token));
                }
                if self.keys.is_empty() {
                    return Err(CatalogError::Invalid(
                        "no object holds a workflow file".into(),
                    ));
                }
                self.keys.reverse();
                self.next_key()
            }
            ObjectStage::Allowed => {
                match result.child::<(), AuthorizeError>()? {
                    Ok(()) => {}
                    Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
                        return Err(CatalogError::Forbidden);
                    }
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                }
                let key = self.keys.last().cloned().ok_or(IoError::Unfinished)?;
                self.stage = ObjectStage::Get;
                let get = GetObjectOperation::new(GetObjectInput {
                    bucket: self.input.bucket.clone(),
                    key,
                    version_id: None,
                    range: None,
                    group_id: self.group_id.ok_or(IoError::Unfinished)?,
                    user_identity: self.auth.user_id,
                    realm_id: self.auth.realm_id,
                    node_id: self.node_id,
                })
                .with_restrictions(self.auth.path_restrictions.clone());
                Ok(Next::Call(IoCall::child(get)))
            }
            ObjectStage::Get => {
                let object = match result.child::<GetObjectResult, GetObjectError>()? {
                    Ok(object) => object,
                    Err(error) => {
                        let key = self.keys.last().cloned().unwrap_or_default();
                        return Err(CatalogError::Invalid(format!("the object {key}: {error}")));
                    }
                };
                self.stage = ObjectStage::Collect;
                let collect = JobEffect::CollectUpTo {
                    stream: object.blob,
                    limit: MAX_SOURCE_BYTES.saturating_sub(self.read_bytes),
                };
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(collect)))))
            }
            ObjectStage::Collect => {
                let key = self.keys.pop().ok_or(IoError::Unfinished)?;
                let collected = match result.event()? {
                    Event::Job(JobEvent::CollectedUpTo(collected)) => collected,
                    _ => return Err(super::unexpected("JobEvent::CollectedUpTo")),
                };
                let content = collected
                    .map_err(|error| CatalogError::Invalid(format!("the object {key}: {error}")))?
                    .ok_or_else(|| {
                        CatalogError::Invalid(format!(
                            "the workflow files exceed the limit of {MAX_SOURCE_BYTES} bytes"
                        ))
                    })?;
                self.read_bytes += content.len() as u64;
                let path = self.path_of(&key);
                self.files.push(SourceFile {
                    path,
                    content,
                    executable: false,
                });
                self.next_key()
            }
        }
    }
}

/// Stores a prepared version: of a new workflow, or as the next version of an existing one.
#[derive(Debug, PartialEq)]
pub struct StoreVersionOperation {
    workflow_id: Ulid,
    created: Option<WorkflowRecord>,
    prepared: Prepared,
    created_by: aruna_core::user::id::UserId,
    message: Option<String>,
    now_ms: u64,
    stored: Option<Created>,
}

impl StoreVersionOperation {
    fn write(&mut self, mut record: WorkflowRecord) -> Result<Next<Created>, CatalogError> {
        let version = record.version_count + 1;
        record.version_count = version;
        record.updated_at_ms = self.now_ms;
        let prepared = std::mem::take(&mut self.prepared);
        let files = WorkflowFiles {
            files: prepared.files,
        };
        let digest = source_digest(&files.source());
        let mut paths: Vec<String> = files.files.iter().map(|file| file.path.clone()).collect();
        paths.sort();
        let row = WorkflowVersionRecord {
            workflow_id: self.workflow_id,
            version,
            language: record.language,
            entrypoint: prepared.entrypoint,
            files: paths,
            commit: prepared.commit.clone(),
            digest: digest.clone(),
            created_by: self.created_by,
            created_at_ms: self.now_ms,
            message: self.message.take(),
            uwe_yaml: prepared.uwe_yaml,
            images: prepared.images,
            inspection: prepared.inspection,
        };
        let key = version_key(self.workflow_id, version);
        self.stored = Some(Created {
            workflow_id: self.workflow_id,
            version,
            commit: prepared.commit,
            digest,
        });
        Ok(Next::Call(IoCall::Write(vec![
            (
                WORKFLOW_KEYSPACE.into(),
                workflow_key(self.workflow_id),
                record.to_bytes()?.into(),
            ),
            (
                WORKFLOW_VERSION_KEYSPACE.into(),
                key.clone(),
                row.to_bytes()?.into(),
            ),
            (
                WORKFLOW_SOURCE_KEYSPACE.into(),
                key,
                files.to_bytes()?.into(),
            ),
        ])))
    }
}

impl Operation for StoreVersionOperation {
    type Output = Created;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Created>, CatalogError> {
        let key = workflow_key(self.workflow_id);
        Ok(Next::Call(aruna_core::rows::read_row(
            WORKFLOW_KEYSPACE,
            key,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Created>, CatalogError> {
        if let Some(created) = self.stored.take() {
            result.done()?;
            return Ok(Next::Done(created));
        }
        let stored = result.value()?;
        let record = match (stored, self.created.take()) {
            (None, Some(record)) => record,
            (Some(value), None) => WorkflowRecord::from_bytes(&value)?,
            (None, None) => return Err(CatalogError::NotFound),
            (Some(_), Some(_)) => return Err(CatalogError::Conflict("the workflow exists".into())),
        };
        if self
            .prepared
            .language
            .is_some_and(|language| language != record.language)
        {
            return Err(CatalogError::Invalid(format!(
                "the files are {}, the workflow is {}",
                self.prepared
                    .language
                    .map_or("unknown", StoredWorkflowLanguage::as_str),
                record.language.as_str()
            )));
        }
        self.write(record)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[derive(Debug, PartialEq)]
enum CreateStage {
    Access,
    Prepare,
    Store,
}

/// Creates a workflow in a group the caller may write, with its first version.
#[derive(Debug, PartialEq)]
pub struct CreateWorkflowOperation {
    auth: AuthContext,
    node_id: NodeId,
    input: Option<NewWorkflow>,
    record: Option<WorkflowRecord>,
    ids: IdMint,
    now_ms: u64,
    stage: CreateStage,
}

impl CreateWorkflowOperation {
    pub fn new(auth: &AuthContext, node_id: NodeId, input: NewWorkflow) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            input: Some(input),
            record: None,
            ids: IdMint::default(),
            now_ms: 0,
            stage: CreateStage::Access,
        }
    }
}

impl Operation for CreateWorkflowOperation {
    type Output = Created;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Created>, CatalogError> {
        let input = self.input.as_ref().ok_or(IoError::Unfinished)?;
        if input.name.trim().is_empty() {
            return Err(CatalogError::Invalid("a workflow needs a name".into()));
        }
        let check = GroupAccessOperation::new(&self.auth, [input.group_id]);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Created>, CatalogError> {
        match self.stage {
            CreateStage::Access => {
                let input = self.input.take().ok_or(IoError::Unfinished)?;
                let access: Access =
                    (access_answer(result)?.get(&input.group_id).copied()).unwrap_or_default();
                if !access.write {
                    return Err(CatalogError::Forbidden);
                }
                let prepare = PrepareVersionOperation::new(
                    (&self.auth, self.node_id),
                    input.group_id,
                    input.language,
                    input.source,
                );
                self.record = Some(WorkflowRecord {
                    workflow_id: self.ids.mint(self.now_ms),
                    group_id: input.group_id,
                    name: input.name.trim().to_owned(),
                    description: input.description,
                    language: input.language.unwrap_or(StoredWorkflowLanguage::Wir),
                    visibility: input.visibility,
                    version_count: 0,
                    edam_formats: Vec::new(),
                    created_by: self.auth.user_id,
                    created_at_ms: self.now_ms,
                    updated_at_ms: self.now_ms,
                });
                self.stage = CreateStage::Prepare;
                Ok(Next::Call(IoCall::child(prepare)))
            }
            CreateStage::Prepare => {
                let prepared = result.child::<Prepared, CatalogError>()??;
                let mut record = self.record.take().ok_or(IoError::Unfinished)?;
                record.language = prepared.language.ok_or_else(|| {
                    CatalogError::Invalid("the language of the workflow is not known".into())
                })?;
                self.stage = CreateStage::Store;
                Ok(Next::Call(IoCall::child(StoreVersionOperation {
                    workflow_id: record.workflow_id,
                    created: Some(record),
                    prepared,
                    created_by: self.auth.user_id,
                    message: None,
                    now_ms: self.now_ms,
                    stored: None,
                })))
            }
            CreateStage::Store => Ok(Next::Done(result.child::<Created, CatalogError>()??)),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn expected_error(error: &CatalogError) -> bool {
        !matches!(
            error,
            CatalogError::Storage(_) | CatalogError::Coordinator(_)
        )
    }
}

/// Adds the next version to a workflow the caller may write.
#[derive(Debug, PartialEq)]
pub struct AddVersionOperation {
    auth: AuthContext,
    node_id: NodeId,
    workflow_id: Ulid,
    source: Option<SourceInput>,
    message: Option<String>,
    now_ms: u64,
    stage: CreateStage,
}

impl AddVersionOperation {
    pub fn new(
        (auth, node_id): (&AuthContext, NodeId),
        workflow_id: Ulid,
        source: SourceInput,
        message: Option<String>,
    ) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            workflow_id,
            source: Some(source),
            message,
            now_ms: 0,
            stage: CreateStage::Access,
        }
    }
}

impl Operation for AddVersionOperation {
    type Output = Created;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Created>, CatalogError> {
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Created>, CatalogError> {
        match self.stage {
            CreateStage::Access => {
                let (record, _) = result.child::<(WorkflowRecord, Access), CatalogError>()??;
                let source = self.source.take().ok_or(IoError::Unfinished)?;
                self.stage = CreateStage::Prepare;
                let prepare = PrepareVersionOperation::new(
                    (&self.auth, self.node_id),
                    record.group_id,
                    Some(record.language),
                    source,
                );
                Ok(Next::Call(IoCall::child(prepare)))
            }
            CreateStage::Prepare => {
                let prepared = result.child::<Prepared, CatalogError>()??;
                self.stage = CreateStage::Store;
                Ok(Next::Call(IoCall::child(StoreVersionOperation {
                    workflow_id: self.workflow_id,
                    created: None,
                    prepared,
                    created_by: self.auth.user_id,
                    message: self.message.take(),
                    now_ms: self.now_ms,
                    stored: None,
                })))
            }
            CreateStage::Store => Ok(Next::Done(result.child::<Created, CatalogError>()??)),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn expected_error(error: &CatalogError) -> bool {
        !matches!(
            error,
            CatalogError::Storage(_) | CatalogError::Coordinator(_)
        )
    }
}
