//! The RO-Crate 1.2 of a workflow run with Aruna as its single writer: uwe's Workflow Run Crate
//! merged with Aruna's objects, nodes and cache reuse, stored per run and published once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::WORKFLOW_CRATE_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::api::CreateDocumentPayload;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    RunFile, RunState, WorkflowRunRecord, WorkflowTaskRecord, run_key,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::data::identity::{ObjectLocation, content_id, parse_hash};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structured_id::{MetaResourceId, StructuredId};
use aruna_ops_data::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentError, CreateDocumentOperation, CreateDocumentResult,
};
use aruna_ops_data::metadata::create_entry::DocumentMintOperation;
use aruna_ops_data::metadata::forward::route_create::RouteCreateOperation;
use aruna_ops_data::metadata::forward::write::MetadataWriteError;
use aruna_ops_identity::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use aruna_workflow_contract::PreflightKind;
use aruna_workflow_contract::PreflightResult;
use aruna_workflow_contract::preflight::WorkflowSource;
use byteview::ByteView;
use serde_json::{Value, json};
use tracing::warn;
use ulid::Ulid;

use super::control::pins_of;
use super::data::area_of;
use super::events::time_text;
use super::log::{LoadLogOperation, pages_answer};
use super::reports::{ReportAnswer, ReportCallOperation};
use super::{
    RunAccessOperation, RunChange, RunTasksOperation, access_of, change_call, changed,
    creator_auth, tasks_answer,
};
use crate::workflows::versions::{GetVersionOperation, VersionView};
use crate::workflows::{CatalogError, Need};

pub const PROCESS_PROFILE: &str = "https://w3id.org/ro/wfrun/process/0.5";
pub const WORKFLOW_PROFILE: &str = "https://w3id.org/ro/wfrun/workflow/0.5";
pub const PROVENANCE_PROFILE: &str = "https://w3id.org/ro/wfrun/provenance/0.5";
const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const WORKFLOW_RUN_CONTEXT: &str = "https://w3id.org/ro/terms/workflow-run/context";
const CRATE_SPEC: &str = "https://w3id.org/ro/crate/1.2";
const OLD_SPEC: &str = "https://w3id.org/ro/crate/1.1";
const TERMS: &str = "https://w3id.org/aruna/terms/";
const DESCRIPTOR: &str = "ro-crate-metadata.json";
const ROOT: &str = "./";

/// The `@id` of a run's workflow action, which the portal links to the run's page.
pub fn action_id(run_id: Ulid) -> String {
    format!("#workflow-run-{run_id}")
}

/// The API path of a run, which the workflow action names as its `url`.
pub fn run_path(run_id: Ulid) -> String {
    format!("/compute/workflow-runs/{run_id}")
}

/// The crate of a run: uwe's crate of the run log when `built` holds one, or else one from the
/// run's records alone, in both cases with Aruna's facts merged in.
pub fn crate_json(
    record: &WorkflowRunRecord,
    tasks: &[WorkflowTaskRecord],
    built: Option<Value>,
) -> Value {
    let mut graph = match built.map(|mut document| document["@graph"].take()) {
        Some(Value::Array(graph)) => graph,
        _ => Vec::new(),
    };
    let root = (entity(&mut graph, DESCRIPTOR))
        .and_then(|descriptor| descriptor["about"]["@id"].as_str().map(str::to_owned))
        .unwrap_or_else(|| ROOT.to_owned());
    if root != ROOT {
        rename(&mut graph, &root, ROOT);
    }
    let action = action_id(record.run_id);
    if let Some(old) = workflow_action(&graph, record.run_id).filter(|old| *old != action) {
        rename(&mut graph, &old, &action);
    }
    describe_descriptor(&mut graph);
    describe_root(&mut graph, record, &action);
    describe_action(&mut graph, record, &action);
    for (profile, name) in [
        (PROCESS_PROFILE, "Process Run Crate"),
        (WORKFLOW_PROFILE, "Workflow Run Crate"),
        (PROVENANCE_PROFILE, "Provenance Run Crate"),
    ] {
        let entity = upsert(&mut graph, profile, "CreativeWork");
        set_missing(entity, "name", json!(name));
        set_missing(entity, "version", json!("0.5"));
    }
    for task in tasks {
        describe_task(&mut graph, record, task);
    }
    json!({
        "@context": [CRATE_CONTEXT, WORKFLOW_RUN_CONTEXT],
        "@graph": graph,
    })
}

/// The id of uwe's action of the whole run: `#run/<run id>`, or the action of the root's main
/// entity.
fn workflow_action(graph: &[Value], run_id: Ulid) -> Option<String> {
    let named = format!("#run/{run_id}");
    if graph.iter().any(|entity| entity["@id"] == named.as_str()) {
        return Some(named);
    }
    let root = graph.iter().find(|entity| entity["@id"] == ROOT)?;
    let main = root["mainEntity"]["@id"].as_str()?;
    (graph.iter())
        .filter(|entity| has_type(entity, "CreateAction"))
        .find(|entity| entity["instrument"]["@id"] == main)
        .and_then(|entity| entity["@id"].as_str().map(str::to_owned))
}

fn describe_descriptor(graph: &mut Vec<Value>) {
    let descriptor = upsert(graph, DESCRIPTOR, "CreativeWork");
    descriptor["about"] = json!({ "@id": ROOT });
    let mut profiles: Vec<Value> = refs(&descriptor["conformsTo"])
        .into_iter()
        .filter(|id| id != OLD_SPEC && id != CRATE_SPEC)
        .map(|id| json!({ "@id": id }))
        .collect();
    profiles.insert(0, json!({ "@id": CRATE_SPEC }));
    descriptor["conformsTo"] = one_or_many(profiles);
}

fn describe_root(graph: &mut Vec<Value>, record: &WorkflowRunRecord, action: &str) {
    let root = upsert(graph, ROOT, "Dataset");
    let mut profiles = refs(&root["conformsTo"]);
    for profile in [PROCESS_PROFILE, WORKFLOW_PROFILE, PROVENANCE_PROFILE] {
        if !profiles.iter().any(|known| known == profile) {
            profiles.push(profile.to_owned());
        }
    }
    root["conformsTo"] = Value::Array(
        profiles
            .into_iter()
            .map(|id| json!({ "@id": id }))
            .collect(),
    );
    push_ref(root, "mentions", action);
    set_missing(
        root,
        "name",
        json!(format!("Run {} of {}", record.run_id, record.workflow_name)),
    );
    if let Some(published) = record.finished_at_ms.and_then(time_text) {
        set_missing(root, "datePublished", json!(published));
    }
    let dataset = (record.output.as_ref())
        .and_then(|output| output.dataset_json.as_deref())
        .and_then(|text| serde_json::from_str::<Value>(text).ok());
    if let Some(dataset) = dataset {
        for field in ["name", "description"] {
            if let Some(text) = dataset[field].as_str() {
                root[field] = json!(text);
            }
        }
        if let Some(license) = dataset["license"].as_str() {
            root["license"] = match license.starts_with("http") {
                true => json!({ "@id": license }),
                false => json!(license),
            };
        }
        if let Some(public) = dataset["public"].as_bool() {
            root["isAccessibleForFree"] = json!(public);
        }
    }
}

fn status_iri(state: RunState) -> &'static str {
    match state {
        RunState::Succeeded => "http://schema.org/CompletedActionStatus",
        RunState::Failed | RunState::Cancelled => "http://schema.org/FailedActionStatus",
        _ => "http://schema.org/ActiveActionStatus",
    }
}

fn describe_action(graph: &mut Vec<Value>, record: &WorkflowRunRecord, action: &str) {
    let inputs = run_inputs(record);
    let mut objects = Vec::new();
    for file in &inputs {
        objects.extend(upsert_file(graph, record, file));
    }
    let mut results = Vec::new();
    for file in &record.outputs {
        results.extend(upsert_file(graph, record, file));
    }
    let agent = format!("#agent-{}", record.created_by);
    let person = upsert(graph, &agent, "Person");
    set_missing(person, "identifier", json!(record.created_by.to_string()));
    let mut properties = vec![
        ("node-id", "Coordinator node", record.node_id.to_string()),
        ("lineage", "Lineage", record.lineage.to_string()),
        (
            "cache-scope",
            "Cache scope",
            record.cache_scope.as_str().to_owned(),
        ),
        (
            "attempts",
            "Coordinator attempts",
            record.attempts.to_string(),
        ),
    ];
    if let Some(resumed) = record.resumes_run_id {
        properties.push(("resumes", "Resumes run", resumed.to_string()));
    }
    if let Some(repeated) = record.repeats_run_id {
        properties.push(("repeats", "Repeats run", repeated.to_string()));
    }
    let property_ids = add_properties(graph, action, &properties);
    let entity = upsert(graph, action, "CreateAction");
    set_missing(
        entity,
        "name",
        json!(format!("Run {} of {}", record.run_id, record.workflow_name)),
    );
    entity["url"] = json!(run_path(record.run_id));
    entity["identifier"] = json!(record.run_id.to_string());
    entity["agent"] = json!({ "@id": agent });
    entity["actionStatus"] = json!(status_iri(record.state));
    if let Some(started) = time_text(record.created_at_ms) {
        set_missing(entity, "startTime", json!(started));
    }
    if let Some(ended) = record.finished_at_ms.and_then(time_text) {
        entity["endTime"] = json!(ended);
    }
    if let Some(error) = &record.error {
        entity["error"] = json!(error);
    }
    for id in objects {
        push_ref(entity, "object", &id);
    }
    for id in results {
        push_ref(entity, "result", &id);
    }
    for id in property_ids {
        push_ref(entity, "additionalProperty", &id);
    }
}

/// Adds Aruna's facts of one task to uwe's action of it: where it ran and whether its result was
/// reused. A reused result names no producer. A task uwe did not describe gets an action of its own.
fn describe_task(graph: &mut Vec<Value>, record: &WorkflowRunRecord, task: &WorkflowTaskRecord) {
    let id = format!("#task/{}", task.task_id);
    let known = graph.iter().any(|entity| entity["@id"] == id.as_str());
    let mut properties = vec![
        (
            "cache-reuse",
            "Cache reuse",
            (if task.cached { "reused" } else { "ran" }).to_owned(),
        ),
        ("attempt", "Attempt", task.attempt.to_string()),
    ];
    if let Some(node) = task.node_id {
        properties.push(("node-id", "Execution node", node.to_string()));
    }
    if let Some(mode) = &task.mode {
        properties.push(("placement-mode", "Placement mode", mode.clone()));
    }
    if let Some(key) = &task.cache_key {
        properties.push(("cache-key", "Cache key", key.clone()));
    }
    if let Some(deterministic) = task.deterministic {
        properties.push(("deterministic", "Deterministic", deterministic.to_string()));
    }
    let property_ids = add_properties(graph, &id, &properties);
    let mut objects = Vec::new();
    let mut results = Vec::new();
    if !known {
        for file in &task.inputs {
            objects.extend(upsert_file(graph, record, file));
        }
        for file in &task.outputs {
            results.extend(upsert_file(graph, record, file));
        }
    }
    let entity = upsert(graph, &id, "CreateAction");
    if !known {
        entity["name"] = json!(task.name);
        entity["description"] = json!(format!("step {}", task.step));
        entity["actionStatus"] = json!(status_iri(task.state));
        if let Some(started) = task.started_at_ms.and_then(time_text) {
            entity["startTime"] = json!(started);
        }
        if let Some(ended) = task.finished_at_ms.and_then(time_text) {
            entity["endTime"] = json!(ended);
        }
        if let Some(error) = &task.error {
            entity["error"] = json!(error);
        }
    }
    for id in objects {
        push_ref(entity, "object", &id);
    }
    for id in results {
        push_ref(entity, "result", &id);
    }
    for id in property_ids {
        push_ref(entity, "additionalProperty", &id);
    }
}

/// The pinned inputs of the run as files on the nodes that hold them.
fn run_inputs(record: &WorkflowRunRecord) -> Vec<RunFile> {
    let parsed = (record.inputs_json.as_deref())
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .and_then(|inputs| pins_of(&inputs).ok())
        .unwrap_or_default();
    (parsed.into_iter())
        .map(|pin| RunFile {
            name: pin.name,
            bucket: Some(pin.bucket),
            key: Some(pin.key),
            version_id: Some(pin.version_id.to_string()),
            node_id: pin.source_node_id.or(Some(record.node_id)),
            ..RunFile::default()
        })
        .collect()
}

/// Adds or completes the entity of one object version: its exact versioned ARN as `@id` where
/// it has a version, its S3 URL, size and digests. Answers the entity's id.
fn upsert_file(
    graph: &mut Vec<Value>,
    record: &WorkflowRunRecord,
    file: &RunFile,
) -> Option<String> {
    let (bucket, key) = (file.bucket.as_deref()?, file.key.as_deref()?);
    let location = ObjectLocation {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    let version = (file.version_id.as_deref()).and_then(|version| version.parse::<Ulid>().ok());
    let node = file.node_id.unwrap_or(record.node_id);
    let arn = version
        .and_then(|version| {
            VersionedObjectArn::new(record.realm_id, node, bucket, key, version).ok()
        })
        .map(|arn| arn.to_w3id());
    let id = arn.clone().unwrap_or_else(|| location.to_url());
    let mut urls = vec![json!(location.to_url())];
    urls.extend(arn.map(Value::String));
    let entity = upsert(graph, &id, "File");
    set_missing(entity, "name", json!(file.name));
    entity["contentUrl"] = one_or_many(urls);
    if let Some(version) = version {
        entity["version"] = json!(version.to_string());
    }
    if let Some(size) = file.size {
        entity["contentSize"] = json!(size.to_string());
    }
    if let Some(digest) = file.digest.as_deref() {
        match digest.strip_prefix("sha256:") {
            Some(sha256) => entity["sha256"] = json!(sha256),
            None => {
                if let Some(hash) = parse_hash(digest) {
                    entity["identifier"] = json!(content_id(hash));
                }
            }
        }
    }
    Some(id)
}

/// Adds Aruna's facts about `owner` as property values; answers their ids.
fn add_properties(
    graph: &mut Vec<Value>,
    owner: &str,
    properties: &[(&str, &str, String)],
) -> Vec<String> {
    let mut ids = Vec::new();
    for (term, name, value) in properties {
        let id = format!("{owner}/aruna/{term}");
        let entity = upsert(graph, &id, "PropertyValue");
        entity["name"] = json!(name);
        entity["propertyID"] = json!(format!("{TERMS}{term}"));
        entity["value"] = json!(value);
        ids.push(id);
    }
    ids
}

fn has_type(entity: &Value, expected: &str) -> bool {
    match &entity["@type"] {
        Value::String(kind) => kind == expected,
        Value::Array(kinds) => kinds.iter().any(|kind| kind == expected),
        _ => false,
    }
}

fn entity<'a>(graph: &'a mut [Value], id: &str) -> Option<&'a mut Value> {
    graph.iter_mut().find(|entity| entity["@id"] == id)
}

/// The entity `id`, added with type `kind` when the graph has none.
fn upsert<'a>(graph: &'a mut Vec<Value>, id: &str, kind: &str) -> &'a mut Value {
    let position = match graph.iter().position(|entity| entity["@id"] == id) {
        Some(position) => position,
        None => {
            graph.push(json!({ "@id": id, "@type": kind }));
            graph.len() - 1
        }
    };
    &mut graph[position]
}

fn set_missing(entity: &mut Value, key: &str, value: Value) {
    if entity.get(key).is_none_or(Value::is_null) {
        entity[key] = value;
    }
}

/// The ids a property refers to, whether it holds one reference or a list.
fn refs(value: &Value) -> Vec<String> {
    let items = match value {
        Value::Array(items) => items.iter().collect(),
        Value::Null => Vec::new(),
        other => vec![other],
    };
    (items.into_iter())
        .filter_map(|item| match item {
            Value::String(text) => Some(text.clone()),
            other => other["@id"].as_str().map(str::to_owned),
        })
        .collect()
}

/// Adds a reference to `id` under `key`, once.
fn push_ref(entity: &mut Value, key: &str, id: &str) {
    let mut ids = refs(&entity[key]);
    if ids.iter().any(|known| known == id) {
        return;
    }
    ids.push(id.to_owned());
    entity[key] = one_or_many(ids.into_iter().map(|id| json!({ "@id": id })).collect());
}

fn one_or_many(mut values: Vec<Value>) -> Value {
    match values.len() {
        1 => values.remove(0),
        _ => Value::Array(values),
    }
}

/// Renames the entity `old` and every reference to it.
fn rename(graph: &mut [Value], old: &str, new: &str) {
    for entity in graph {
        rename_value(entity, old, new);
    }
}

fn rename_value(value: &mut Value, old: &str, new: &str) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                match (key.as_str(), child.as_str()) {
                    ("@id", Some(id)) if id == old => *child = json!(new),
                    _ => rename_value(child, old, new),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                rename_value(item, old, new);
            }
        }
        _ => {}
    }
}

/// The files of a run's workflow version and its entry file, when the creator still reads them.
type VersionFiles = Option<(WorkflowSource, String)>;

#[derive(Debug, PartialEq)]
enum BuildStep {
    Tasks,
    Version(Vec<WorkflowTaskRecord>),
    Log(Box<(Vec<WorkflowTaskRecord>, VersionFiles)>),
    Report(Vec<WorkflowTaskRecord>),
}

/// Builds the crate of a run: uwe's crate from the run log in a report coordinator where it can,
/// else one from the records. It always answers a crate; a coordinator failure only warns.
#[derive(Debug, PartialEq)]
pub struct BuildCrateOperation {
    record: WorkflowRunRecord,
    step: BuildStep,
}

impl BuildCrateOperation {
    pub fn new(record: WorkflowRunRecord) -> Self {
        Self {
            record,
            step: BuildStep::Tasks,
        }
    }

    fn done(
        &self,
        tasks: &[WorkflowTaskRecord],
        built: Option<Value>,
    ) -> Result<Next<Value>, CatalogError> {
        Ok(Next::Done(crate_json(&self.record, tasks, built)))
    }
}

impl Operation for BuildCrateOperation {
    type Output = Value;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        Ok(Next::Call(IoCall::child(RunTasksOperation::new(
            self.record.run_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        let record = &self.record;
        match std::mem::replace(&mut self.step, BuildStep::Tasks) {
            BuildStep::Tasks => {
                let tasks = tasks_answer(result)?;
                let read = GetVersionOperation::new(
                    &creator_auth(record),
                    record.workflow_id,
                    record.version,
                )
                .with_files();
                self.step = BuildStep::Version(tasks);
                Ok(Next::Call(IoCall::child(read)))
            }
            BuildStep::Version(tasks) => {
                let source = match result.child::<VersionView, CatalogError>()? {
                    Ok(view) => (view.files.as_ref())
                        .map(|files| (files.source(), view.version.entrypoint.clone())),
                    Err(error) => {
                        warn!(%error, run = %record.run_id, "the crate names no workflow files");
                        None
                    }
                };
                let log = LoadLogOperation::new(record.run_id);
                self.step = BuildStep::Log(Box::new((tasks, source)));
                Ok(Next::Call(IoCall::child(log)))
            }
            BuildStep::Log(parts) => {
                let (tasks, source) = *parts;
                let log_pages = pages_answer(result)?;
                let Some((source, entrypoint)) = source.filter(|_| !log_pages.is_empty()) else {
                    return self.done(&tasks, None);
                };
                let base_uri = area_of(record)
                    .map(|(bucket, prefix)| format!("s3://{bucket}/{prefix}"))
                    .unwrap_or_default();
                let kind = PreflightKind::BuildRunCrate {
                    run_id: record.run_id.to_string(),
                    log_pages,
                    base_uri,
                    source,
                    entrypoint,
                };
                self.step = BuildStep::Report(tasks);
                Ok(Next::Call(IoCall::child(ReportCallOperation::new(
                    record.run_id,
                    kind,
                ))))
            }
            BuildStep::Report(tasks) => match result.child::<ReportAnswer, CatalogError>()?? {
                Ok(PreflightResult::BuildRunCrate { metadata_json, .. }) => {
                    self.done(&tasks, Some(metadata_json))
                }
                other => {
                    warn!(run = %record.run_id, ?other, "uwe built no crate; the records describe the run");
                    self.done(&tasks, None)
                }
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadStep {
    Access,
    Stored(Box<WorkflowRunRecord>),
    Built,
}

/// The crate of a run for a caller who reads it: the stored crate, or one built now for a run
/// that ended. A run that has not ended has no crate yet.
#[derive(Debug, PartialEq)]
pub struct RunCrateOperation {
    auth: AuthContext,
    run_id: Ulid,
    step: ReadStep,
}

impl RunCrateOperation {
    pub fn new(auth: &AuthContext, run_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            run_id,
            step: ReadStep::Access,
        }
    }
}

impl Operation for RunCrateOperation {
    type Output = Value;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Value>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.run_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Value>, CatalogError> {
        match std::mem::replace(&mut self.step, ReadStep::Built) {
            ReadStep::Access => {
                let (record, _) = access_of(result)?;
                self.step = ReadStep::Stored(Box::new(record));
                Ok(Next::Call(IoCall::Read(vec![(
                    WORKFLOW_CRATE_KEYSPACE.to_string(),
                    run_key(self.run_id),
                )])))
            }
            ReadStep::Stored(record) => {
                if let Some(value) = result.value()? {
                    let stored = serde_json::from_slice(&value)
                        .map_err(|error| CatalogError::Storage(error.to_string()))?;
                    return Ok(Next::Done(stored));
                }
                if !record.state.is_terminal() {
                    return Err(CatalogError::Conflict(format!(
                        "the run is still {}; its crate is written when it ends",
                        record.state.as_str()
                    )));
                }
                Ok(Next::Call(IoCall::child(BuildCrateOperation::new(*record))))
            }
            ReadStep::Built => Ok(Next::Done(result.child::<Value, CatalogError>()??)),
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Conflict(_))
    }
}

/// How the publication of a run's crate ended, as the run's `provenance_status` names it.
pub mod status {
    /// Published as a metadata document of the run's group.
    pub const WRITTEN: &str = "written";
    /// Kept on this node only, for a run that has not reached the realm.
    pub const LOCAL: &str = "local";
    /// Kept beside the protected run only, readable with the run, never as a group document.
    pub const PROTECTED: &str = "protected";
    pub const FAILED: &str = "failed";
    /// The run's creator may not write metadata documents of the group.
    pub const DENIED: &str = "denied";
}

#[derive(Debug, PartialEq)]
enum WriteStep {
    Run,
    Build(Box<WorkflowRunRecord>),
    Store(Box<WorkflowRunRecord>, Box<Value>),
    Mint(Box<WorkflowRunRecord>, Box<Value>),
    Allowed(Box<WorkflowRunRecord>, Box<Value>, Ulid),
    Create,
    Record,
}

/// Writes the crate of a run that ended, once: it builds and stores the crate, publishes it as a
/// metadata document of the group as the run's creator and records how that went. A failure of
/// the publication is recorded on the run and never fails the run itself.
#[derive(Debug, PartialEq)]
pub struct WriteProvenanceOperation {
    run_id: Ulid,
    node: (NodeId, RealmId),
    step: WriteStep,
}

type WriteNext = Result<Next<WorkflowRunRecord>, CatalogError>;

impl WriteProvenanceOperation {
    pub fn new(run_id: Ulid, node: (NodeId, RealmId)) -> Self {
        Self {
            run_id,
            node,
            step: WriteStep::Run,
        }
    }

    fn document_path(&self) -> String {
        format!("runs/{}", self.run_id)
    }

    fn actor(&self, record: &WorkflowRunRecord) -> Actor {
        Actor {
            node_id: self.node.0,
            user_id: record.created_by,
            realm_id: self.node.1,
        }
    }

    fn internal(&self, record: &WorkflowRunRecord) -> AuthContext {
        AuthContext {
            user_id: record.created_by,
            realm_id: self.node.1,
            path_restrictions: None,
            session: None,
        }
    }

    fn record(&mut self, status: &str, document_id: Option<Ulid>) -> WriteNext {
        let change = RunChange::Provenance {
            status: status.to_owned(),
            document_id,
        };
        self.step = WriteStep::Record;
        Ok(Next::Call(change_call(self.run_id, vec![change])))
    }

    fn create(
        &mut self,
        record: &WorkflowRunRecord,
        jsonld: &Value,
        document_id: Ulid,
    ) -> WriteNext {
        let mut published = jsonld.clone();
        if let Some(Value::Array(graph)) = published.get_mut("@graph") {
            rename(
                graph,
                ROOT,
                &MetadataRegistryRecord::graph_iri_for(document_id),
            );
        }
        let public = (record.output.as_ref())
            .and_then(|output| output.dataset_json.as_deref())
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .and_then(|dataset| dataset["public"].as_bool())
            .unwrap_or(false);
        let create = RouteCreateOperation::new(
            CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                actor: self.actor(record),
                group_id: record.group_id,
                document_id,
                document_path: self.document_path(),
                public,
                payload: CreateDocumentPayload::RoCrate {
                    jsonld: published.to_string(),
                },
            }),
            Some(self.node.0),
            Some(AuthToken::internal(self.internal(record))),
        );
        self.step = WriteStep::Create;
        Ok(Next::Call(IoCall::child(create)))
    }
}

impl Operation for WriteProvenanceOperation {
    type Output = WorkflowRunRecord;
    type Error = CatalogError;

    fn first(&mut self) -> WriteNext {
        Ok(Next::Call(super::read_run(self.run_id)))
    }

    fn next(&mut self, result: IoResult) -> WriteNext {
        match std::mem::replace(&mut self.step, WriteStep::Record) {
            WriteStep::Run => {
                let record = super::run_answer(result)?.ok_or(CatalogError::NotFound)?;
                let wanted = record.provenance_crate
                    && record.state.is_terminal()
                    && record.provenance_status.is_none();
                if !wanted {
                    return Ok(Next::Done(record));
                }
                self.step = WriteStep::Build(Box::new(record.clone()));
                Ok(Next::Call(IoCall::child(BuildCrateOperation::new(record))))
            }
            WriteStep::Build(record) => {
                let jsonld = match result.child::<Value, CatalogError>()? {
                    Ok(jsonld) => jsonld,
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "the run crate was not built");
                        return self.record(status::FAILED, None);
                    }
                };
                let row = (
                    WORKFLOW_CRATE_KEYSPACE.to_string(),
                    run_key(self.run_id),
                    ByteView::from(jsonld.to_string().into_bytes()),
                );
                self.step = WriteStep::Store(record, Box::new(jsonld));
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            WriteStep::Store(record, jsonld) => {
                result.done()?;
                if record.local_only {
                    return self.record(status::LOCAL, None);
                }
                if record.protected {
                    return self.record(status::PROTECTED, None);
                }
                let mint = DocumentMintOperation::job(
                    self.actor(&record),
                    record.group_id,
                    self.document_path(),
                );
                self.step = WriteStep::Mint(record, jsonld);
                Ok(Next::Call(IoCall::child(mint)))
            }
            WriteStep::Mint(record, jsonld) => {
                let document_id = match result.child::<MetaResourceId, CreateDocumentError>()? {
                    Ok(id) => id.as_ulid(),
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "the run crate got no document id");
                        return self.record(status::FAILED, None);
                    }
                };
                let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: self.internal(&record),
                    path: MetadataRegistryRecord::permission_path_for(
                        &self.node.1,
                        record.group_id,
                        &self.document_path(),
                        document_id,
                    ),
                    required_permission: Permission::WRITE,
                });
                self.step = WriteStep::Allowed(record, jsonld, document_id);
                Ok(Next::Call(IoCall::child(check)))
            }
            WriteStep::Allowed(record, jsonld, document_id) => {
                match result.child::<bool, AuthorizationError>()? {
                    Ok(true) => self.create(&record, &jsonld, document_id),
                    Ok(false) => self.record(status::DENIED, None),
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "the run crate was not authorized");
                        self.record(status::DENIED, None)
                    }
                }
            }
            WriteStep::Create => {
                match result.child::<CreateDocumentResult, MetadataWriteError>()? {
                    Ok(created) => self.record(status::WRITTEN, Some(created.record.document_id)),
                    Err(error) => {
                        warn!(%error, run = %self.run_id, "the run crate was not published");
                        self.record(status::FAILED, None)
                    }
                }
            }
            WriteStep::Record => Ok(Next::Done(changed(result)?)),
        }
    }
}
