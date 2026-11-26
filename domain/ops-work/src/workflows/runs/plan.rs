//! Previews where the tasks of a run would go without side effects, simulating pinned tasks like
//! launch while deferring tasks that depend on earlier outputs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::scheduling::ExecutionPlan;
use aruna_core::structs::execution::job::input::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, TaskSecretBinding,
    WORKFLOW_SECRETS_TAG, encode_workflow_secrets,
};
use aruna_core::structs::execution::workflow::{StoredWorkflowLanguage, ToolRecord};
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunState, WorkflowRunRecord, WorkflowSecretScope,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::workflow::WorkflowError;
use aruna_workflow_contract::preflight::{
    Diagnostic, Severity, SourceFile, WorkflowLanguage, WorkflowSource,
};
use aruna_workflow_contract::settings::ArunaStepSettings;
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde_json::{Map, Value, json};
use ulid::Ulid;

use super::admit::{PinnedInput, RunRequest};
use super::cache::{CacheHit, LookupCacheOperation};
use super::data::ObjectAccessOperation;
use super::run::run_inputs;
use super::tasks::{site_tags, task_key_of};
use crate::jobs::lifecycle::preview::{PreviewError, PreviewPlanOperation};
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::workflow::prepare::NETWORK_TAG_KEY;
use crate::workflows::import::{COMPOSED_ENTRY, WIR_ENTRY, catalog_tools};
use crate::workflows::inspect::values_report;
use crate::workflows::versions::{GetVersionOperation, VersionView};
use crate::workflows::{
    CatalogError, GroupAccessOperation, ScanOperation, access_answer, preflight_answer,
    preflight_call, refused, scan_answer, to_text,
};

/// The task placed where it reads all its inputs.
pub const COMPUTE_TO_DATA: &str = "compute_to_data";
/// The task placed elsewhere, with some inputs staged to it.
pub const DATA_TO_COMPUTE: &str = "data_to_compute";
/// The task reads outputs of other tasks, so it is placed when the run creates it.
pub const PLACED_AT_RUNTIME: &str = "placed_at_runtime";

/// The plan of one step as the preview estimates it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedStep {
    pub step: String,
    pub node_id: Option<NodeId>,
    pub mode: &'static str,
    /// Bytes staged from other nodes; `None` when the preview cannot know them.
    pub bytes: Option<u64>,
    pub transfer_ms: Option<u64>,
    pub cached: bool,
    /// Tasks of the step; `None` when the run decides how many.
    pub task_count: Option<u64>,
}

/// What a run would do before it starts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunPlan {
    /// False when the run decides some placements or task counts only once it runs.
    pub complete: bool,
    pub tasks: Vec<PlannedStep>,
    /// The value checks and simulation messages, as the portal reads value diagnostics.
    pub diagnostics: Vec<Value>,
    /// Nodes no task could run on, and why, each pair once.
    pub rejected: Vec<(NodeId, String)>,
}

/// What vault secret placement requirement a simulated task carries.
#[derive(Clone, Debug, PartialEq)]
enum VaultNeed {
    None,
    Known(BTreeMap<String, TaskSecretBinding>),
    Unknown,
}

/// One task of the simulated run, as the preview reads it from the simulated graph.
#[derive(Clone, Debug, PartialEq)]
struct SimTask {
    id: u64,
    step: String,
    image: Option<String>,
    /// The pinned objects the task reads, by the paths it sees them at.
    inputs: Vec<(String, PinnedInput)>,
    upstream: Vec<u64>,
    resources: ComputeResources,
    seed: Option<String>,
    vault: VaultNeed,
}

/// Where a planned task goes.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Placement {
    Planned {
        node_id: NodeId,
        bytes: u64,
        transfer_ms: u64,
    },
    Runtime(Option<NodeId>),
}

/// Distinguishes preview rounds by inputs, compute ceilings, network mode, vault scope, and tags.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PreviewKey {
    inputs: Vec<(String, String, Option<NodeId>)>,
    cpu_cores: Option<u32>,
    ram_bytes: Option<u64>,
    disk_bytes: Option<u64>,
    max_walltime_ms: Option<u64>,
    preemptible: bool,
    gpus: Option<u32>,
    network_open: bool,
    vault: (bool, bool),
    tags: Vec<(String, String)>,
}

#[derive(Debug, PartialEq)]
enum PlanStep {
    Group,
    Version,
    Tools(Box<VersionView>),
    Lower,
    Inputs,
    Validate,
    Simulate,
    Preview(PreviewKey),
    Cache(u64),
}

/// Plans a run request; it reads, checks and simulates, but writes and admits nothing.
#[derive(Debug, PartialEq)]
pub struct PlanRunOperation {
    auth: AuthContext,
    token: Option<AuthToken>,
    node: IngressNode,
    request: RunRequest,
    source: Option<(WorkflowSource, String, Option<WorkflowLanguage>)>,
    settings: BTreeMap<String, ArunaStepSettings>,
    pending_inputs: Vec<PinnedInput>,
    plan: RunPlan,
    tasks: Vec<SimTask>,
    markers: BTreeSet<u64>,
    previews: VecDeque<(PreviewKey, ExecutionSpec)>,
    planned: BTreeMap<PreviewKey, Option<ExecutionPlan>>,
    seeds: VecDeque<(u64, String)>,
    cached: BTreeSet<u64>,
    step: PlanStep,
}

type PlanNext = Result<Next<RunPlan>, CatalogError>;

impl PlanRunOperation {
    pub fn new(auth: &AuthContext, node: IngressNode, request: RunRequest) -> Self {
        let mut pending_inputs = request.inputs.clone();
        pending_inputs.reverse();
        Self {
            auth: auth.clone(),
            token: None,
            node,
            request,
            source: None,
            settings: BTreeMap::new(),
            pending_inputs,
            plan: RunPlan::default(),
            tasks: Vec::new(),
            markers: BTreeSet::new(),
            previews: VecDeque::new(),
            planned: BTreeMap::new(),
            seeds: VecDeque::new(),
            cached: BTreeSet::new(),
            step: PlanStep::Group,
        }
    }

    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }

    /// The run as it would be admitted, which the value check, the simulation and the cache
    /// lookups read; its id names no run.
    fn draft(&self) -> WorkflowRunRecord {
        let request = &self.request;
        WorkflowRunRecord {
            run_id: Ulid::nil(),
            realm_id: self.node.realm_id,
            group_id: request.group_id,
            workflow_id: request.workflow_id,
            workflow_name: String::new(),
            version: request.version,
            lineage: Ulid::nil(),
            state: RunState::Queued,
            node_id: self.node.local,
            created_by: self.auth.user_id,
            created_at_ms: self.node.now_ms,
            finished_at_ms: None,
            error: None,
            values_json: request.values.to_string(),
            inputs_json: request.inputs_json.clone(),
            output: request.output.clone(),
            outputs: Vec::new(),
            provenance_crate: false,
            provenance_document_id: None,
            provenance_status: None,
            last_event_seq: 0,
            local_only: false,
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 0,
            steps: Vec::new(),
            resumes_run_id: None,
            repeats_run_id: None,
            cache_scope: CacheScope::Group,
            placement_site: request.placement_site.clone(),
            protected: false,
            started_at_ms: None,
            secrets: request.secrets.clone(),
            path_restrictions: self.auth.path_restrictions.clone(),
            captured_remote: Vec::new(),
            directory_pins: Vec::new(),
            directory_roots: Vec::new(),
            used_tools: Vec::new(),
        }
    }

    fn versioned(&mut self, view: VersionView) -> PlanNext {
        let version = &view.version;
        if version.language == StoredWorkflowLanguage::Uwe {
            let groups = Some([view.workflow.group_id].into());
            self.step = PlanStep::Tools(Box::new(view));
            let scan = ScanOperation::<ToolRecord>::of_groups(groups);
            return Ok(Next::Call(IoCall::child(scan)));
        }
        let files = view.files.clone().unwrap_or_default();
        let language = version.language.contract();
        self.source = Some((files.source(), version.entrypoint.clone(), language));
        self.next_input()
    }

    /// Checks READ on the next pinned input this node holds, as a start does.
    fn next_input(&mut self) -> PlanNext {
        let local = self.node.local;
        while let Some(input) = self.pending_inputs.pop() {
            if input.source_node_id.is_some_and(|node| node != local) {
                continue;
            }
            let access = ObjectAccessOperation::new(
                &self.auth,
                local,
                (&input.bucket, &input.key),
                Permission::READ,
            );
            self.step = PlanStep::Inputs;
            return Ok(Next::Call(IoCall::child(access)));
        }
        let (source, entrypoint, language) = self.source.clone().ok_or_else(missing)?;
        let values = run_inputs(&self.draft())?;
        self.step = PlanStep::Validate;
        Ok(Next::Call(preflight_call(PreflightKind::ValidateValues {
            source,
            language,
            entrypoint,
            values,
        })))
    }

    fn simulate(&mut self) -> PlanNext {
        let (source, entrypoint, language) = self.source.clone().ok_or_else(missing)?;
        let inputs = run_inputs(&self.draft())?;
        self.step = PlanStep::Simulate;
        Ok(Next::Call(preflight_call(PreflightKind::DryRun {
            source,
            language,
            entrypoint,
            inputs,
            params: Map::new(),
        })))
    }

    /// Reads the simulated graph and queues one preview per distinct set of pinned inputs.
    fn simulated(&mut self, graph: &Value, diagnostics: &[Diagnostic]) -> PlanNext {
        let error = diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == Severity::Error);
        self.plan
            .diagnostics
            .extend(diagnostics.iter().map(diagnostic_json));
        let local = self.node.local;
        let pins: Vec<(String, PinnedInput)> = (self.request.inputs.iter())
            .map(|pin| (pin_address(local, pin), pin.clone()))
            .collect();
        let mut upstream: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for edge in graph["edges"].as_array().into_iter().flatten() {
            if let (Some(from), Some(to)) = (edge["from"].as_u64(), edge["to"].as_u64()) {
                upstream.entry(to).or_default().push(from);
            }
        }
        for node in graph["nodes"].as_array().into_iter().flatten() {
            let task = &node["task"];
            let Some(id) = task["id"].as_u64() else {
                continue;
            };
            if node["markers"]
                .as_array()
                .is_some_and(|markers| !markers.is_empty())
            {
                self.markers.insert(id);
            }
            let step = task["step"].as_str().unwrap_or_default().to_owned();
            let settings = self.settings.get(&step);
            let vault = self.vault_need(task);
            self.tasks.push(SimTask {
                id,
                inputs: pinned_reads(task, &pins),
                upstream: (task["after"].as_array().into_iter().flatten())
                    .filter_map(Value::as_u64)
                    .chain(upstream.remove(&id).unwrap_or_default())
                    .collect(),
                resources: resources_of(&task["resources"], settings),
                image: task["image"].as_str().map(str::to_owned),
                seed: task["task_key_seed"].as_str().map(str::to_owned),
                step,
                vault,
            });
        }
        if error {
            self.plan.complete = false;
            self.tasks.clear();
            return Ok(Next::Done(std::mem::take(&mut self.plan)));
        }
        for task in &self.tasks {
            if !task.upstream.is_empty() || matches!(task.vault, VaultNeed::Unknown) {
                continue;
            }
            let key = self.preview_key(task);
            if self.planned.contains_key(&key)
                || self.previews.iter().any(|(known, _)| *known == key)
            {
                continue;
            }
            let spec = self.preview_spec(task);
            self.previews.push_back((key, spec));
        }
        self.seeds = (self.tasks.iter())
            .filter_map(|task| Some((task.id, task.seed.clone()?)))
            .collect();
        self.next_preview()
    }

    fn vault_need(&self, task: &Value) -> VaultNeed {
        if self.request.secrets.is_empty() {
            return VaultNeed::None;
        }
        let Some(names) = task.get("secrets").and_then(Value::as_array) else {
            return VaultNeed::Unknown;
        };
        let mut bound = BTreeMap::new();
        for item in names {
            let Some(name) = item.as_str() else {
                return VaultNeed::Unknown;
            };
            let Some(binding) = self.request.secrets.get(name) else {
                return VaultNeed::Unknown;
            };
            bound.insert(
                name.to_owned(),
                TaskSecretBinding {
                    workflow_id: self.request.workflow_id,
                    scope: binding.scope,
                    name: binding.name.clone(),
                    field: binding.field.clone(),
                },
            );
        }
        VaultNeed::Known(bound)
    }

    fn preview_key(&self, task: &SimTask) -> PreviewKey {
        let local = self.node.local;
        let settings = self.settings.get(&task.step);
        let tags = site_tags(None, settings, self.request.placement_site.clone());
        let network_open = settings.is_some_and(|s| s.execution.network == Some(true));
        let vault = match &task.vault {
            VaultNeed::Known(bound) => (
                bound.values().any(|b| b.scope != WorkflowSecretScope::Node),
                bound.values().any(|b| b.scope == WorkflowSecretScope::Node),
            ),
            VaultNeed::None | VaultNeed::Unknown => (false, false),
        };
        let r = &task.resources;
        PreviewKey {
            inputs: task
                .inputs
                .iter()
                .map(|(path, pin)| (path.clone(), pin_address(local, pin), pin.source_node_id))
                .collect(),
            cpu_cores: r.cpu_cores,
            ram_bytes: r.ram_bytes,
            disk_bytes: r.disk_bytes,
            max_walltime_ms: r.max_walltime_ms,
            preemptible: r.preemptible,
            gpus: r.gpus,
            network_open,
            vault,
            tags,
        }
    }

    /// The execution spec a launch of `task` would plan from.
    fn preview_spec(&self, task: &SimTask) -> ExecutionSpec {
        let inputs = (task.inputs.iter())
            .map(|(path, pin)| InputSelection {
                source: InputSource::S3 {
                    bucket: pin.bucket.clone(),
                    key: pin.key.clone(),
                    version_id: Some(pin.version_id.to_string()),
                },
                source_node_id: pin.source_node_id,
                dest_key: path.clone(),
                mode: InputMode::Snapshot,
                container_path: Some(path.clone()),
                name: None,
                description: None,
            })
            .collect();
        let settings = self.settings.get(&task.step);
        let mut tags: BTreeMap<String, String> =
            site_tags(None, settings, self.request.placement_site.clone())
                .into_iter()
                .collect();
        if settings.is_some_and(|s| s.execution.network == Some(true)) {
            tags.insert(NETWORK_TAG_KEY.to_owned(), "open".to_owned());
        }
        if let VaultNeed::Known(bound) = &task.vault
            && !bound.is_empty()
            && let Ok(encoded) = encode_workflow_secrets(bound)
        {
            tags.insert(WORKFLOW_SECRETS_TAG.to_owned(), encoded);
        }
        ExecutionSpec {
            group_id: self.request.group_id,
            name: Some(task.step.clone()),
            description: None,
            tags,
            image: task.image.clone().unwrap_or_default(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: BTreeMap::new(),
            resources: task.resources.clone(),
            executor_constraint: None,
            backend: None,
            inputs,
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }
    }

    fn next_preview(&mut self) -> PlanNext {
        let Some((key, spec)) = self.previews.pop_front() else {
            return self.next_seed();
        };
        let preview = PreviewPlanOperation::new(self.node.clone(), spec, self.auth.user_id)
            .with_token(self.token.clone());
        self.step = PlanStep::Preview(key);
        Ok(Next::Call(IoCall::child(preview)))
    }

    fn previewed(&mut self, key: PreviewKey, result: IoResult) -> PlanNext {
        let plan = match result.child::<ExecutionPlan, PreviewError>()? {
            Ok(plan) => Some(plan),
            Err(error) => {
                let message = format!("the inputs of a task could not be planned: {error}");
                self.plan
                    .diagnostics
                    .push(json!({"message": message, "severity": "error"}));
                None
            }
        };
        if let Some(plan) = &plan {
            for rejected in &plan.rejected {
                let entry = (rejected.target.node_id, rejected.verdict.reason());
                if !self.plan.rejected.contains(&entry) {
                    self.plan.rejected.push(entry);
                }
            }
        }
        self.planned.insert(key, plan);
        self.next_preview()
    }

    fn next_seed(&mut self) -> PlanNext {
        let Some((id, seed)) = self.seeds.pop_front() else {
            return Ok(Next::Done(self.assemble()));
        };
        let lookup =
            LookupCacheOperation::new(&self.draft(), CacheScope::Group, &task_key_of(&seed));
        self.step = PlanStep::Cache(id);
        Ok(Next::Call(IoCall::child(lookup)))
    }

    /// The steps in the order the simulation first asked for them, each with its tasks.
    fn assemble(&mut self) -> RunPlan {
        let mut placements: BTreeMap<u64, Placement> = BTreeMap::new();
        let mut complete = self.markers.is_empty();
        for task in &self.tasks {
            let placement = if matches!(task.vault, VaultNeed::Unknown) {
                Placement::Runtime(None)
            } else if task.upstream.is_empty() {
                let key = self.preview_key(task);
                planned(self.planned.get(&key).and_then(Option::as_ref))
            } else {
                let hint = (task.upstream.iter())
                    .filter_map(|upstream| placements.get(upstream))
                    .find_map(Placement::node);
                Placement::Runtime(hint)
            };
            if matches!(placement, Placement::Runtime(_)) {
                complete = false;
            }
            placements.insert(task.id, placement);
        }
        let unknown = downstream_of(&self.tasks, &self.markers);
        let mut order: Vec<String> = Vec::new();
        for task in &self.tasks {
            if !order.contains(&task.step) {
                order.push(task.step.clone());
            }
        }
        let tasks = (order.into_iter())
            .map(|step| {
                let members: Vec<&SimTask> =
                    self.tasks.iter().filter(|task| task.step == step).collect();
                let places: Vec<&Placement> = members
                    .iter()
                    .filter_map(|task| placements.get(&task.id))
                    .collect();
                let counted = !members.iter().any(|task| unknown.contains(&task.id));
                let cached = members.iter().all(|task| self.cached.contains(&task.id));
                step_plan(
                    step,
                    &places,
                    cached,
                    counted.then_some(members.len() as u64),
                )
            })
            .collect();
        let mut plan = std::mem::take(&mut self.plan);
        plan.complete = complete;
        plan.tasks = tasks;
        plan
    }
}

impl Operation for PlanRunOperation {
    type Output = RunPlan;
    type Error = CatalogError;

    fn first(&mut self) -> PlanNext {
        if !self.request.values.is_object() {
            return Err(CatalogError::Invalid("values must be a JSON object".into()));
        }
        let check = GroupAccessOperation::new(&self.auth, [self.request.group_id]);
        self.step = PlanStep::Group;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> PlanNext {
        match std::mem::replace(&mut self.step, PlanStep::Lower) {
            PlanStep::Group => {
                let access = access_answer(result)?;
                let group = self.request.group_id;
                if !access.get(&group).is_some_and(|access| access.read) {
                    return Err(CatalogError::NotFound);
                }
                let read = GetVersionOperation::new(
                    &self.auth,
                    self.request.workflow_id,
                    self.request.version,
                )
                .with_files();
                self.step = PlanStep::Version;
                Ok(Next::Call(IoCall::child(read)))
            }
            PlanStep::Version => {
                let view = result.child::<VersionView, CatalogError>()??;
                self.versioned(view)
            }
            PlanStep::Tools(view) => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                let (catalog_tools, tool_files) = catalog_tools(&tools)?;
                let text = view.version.uwe_yaml.clone().or_else(|| {
                    (view.files.as_ref())
                        .and_then(|files| {
                            files.files.iter().find(|file| file.path == COMPOSED_ENTRY)
                        })
                        .map(|file| String::from_utf8_lossy(&file.content).into_owned())
                });
                let uwe_yaml = text.ok_or_else(|| {
                    CatalogError::Invalid("the composed workflow has no .uwe.yaml text".into())
                })?;
                self.step = PlanStep::Lower;
                Ok(Next::Call(preflight_call(PreflightKind::LowerComposed {
                    uwe_yaml,
                    catalog_tools,
                    tool_files,
                })))
            }
            PlanStep::Lower => {
                let lowering = match preflight_answer(result)?.map_err(refused)? {
                    PreflightResult::LowerComposed(lowering) => lowering,
                    _ => {
                        return Err(crate::workflows::unexpected(
                            "PreflightResult::LowerComposed",
                        ));
                    }
                };
                let Some(wir) = lowering.wir_json else {
                    self.plan.diagnostics =
                        lowering.diagnostics.iter().map(diagnostic_json).collect();
                    return Ok(Next::Done(std::mem::take(&mut self.plan)));
                };
                let file = SourceFile {
                    path: WIR_ENTRY.into(),
                    content: to_text(&wir)?.into_bytes(),
                    executable: false,
                };
                let source = WorkflowSource { files: vec![file] };
                self.source = Some((source, WIR_ENTRY.into(), Some(WorkflowLanguage::Wir)));
                self.settings = lowering.step_settings;
                self.next_input()
            }
            PlanStep::Inputs => {
                super::data::bucket_answer(result).map_err(|error| match error {
                    CatalogError::NotFound => {
                        CatalogError::Invalid("a pinned input's bucket does not exist".into())
                    }
                    other => other,
                })?;
                self.next_input()
            }
            PlanStep::Validate => {
                let (valid, response) = match preflight_answer(result)?.map_err(refused)? {
                    PreflightResult::ValidateValues {
                        valid,
                        response_json,
                    } => (valid, response_json),
                    _ => {
                        return Err(crate::workflows::unexpected(
                            "PreflightResult::ValidateValues",
                        ));
                    }
                };
                let report = values_report(valid, &response);
                if !report.ok {
                    self.plan.diagnostics = report.diagnostics;
                    return Ok(Next::Done(std::mem::take(&mut self.plan)));
                }
                self.simulate()
            }
            PlanStep::Simulate => match preflight_answer(result)? {
                Ok(PreflightResult::DryRun {
                    graph_json,
                    diagnostics,
                    ..
                }) => self.simulated(&graph_json, &diagnostics),
                Ok(_) => Err(crate::workflows::unexpected("PreflightResult::DryRun")),
                Err(WorkflowError::Refused(refusal)) => {
                    let message = refusal.to_string();
                    self.plan
                        .diagnostics
                        .push(json!({"message": message, "severity": "error"}));
                    Ok(Next::Done(std::mem::take(&mut self.plan)))
                }
                Err(error) => Err(refused(error)),
            },
            PlanStep::Preview(key) => self.previewed(key, result),
            PlanStep::Cache(id) => {
                if let Ok(Some(_)) = result.child::<Option<CacheHit>, CatalogError>()? {
                    self.cached.insert(id);
                }
                self.next_seed()
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

fn missing() -> CatalogError {
    CatalogError::Storage("the workflow source was not loaded".into())
}

impl Placement {
    fn node(&self) -> Option<NodeId> {
        match self {
            Placement::Planned { node_id, .. } => Some(*node_id),
            Placement::Runtime(hint) => *hint,
        }
    }
}

/// The placement a planning round chose: the target and what it stages from other nodes.
fn planned(plan: Option<&ExecutionPlan>) -> Placement {
    let Some(selection) = plan.and_then(|plan| plan.selected.as_ref()) else {
        return Placement::Runtime(None);
    };
    let staged = selection
        .inputs
        .iter()
        .filter(|input| input.source_node_id.is_some());
    let (bytes, transfer_ms) = staged.fold((0u64, 0u64), |(bytes, ms), input| {
        (
            bytes.saturating_add(input.bytes),
            ms.saturating_add(input.transfer_ms),
        )
    });
    Placement::Planned {
        node_id: selection.target.node_id,
        bytes,
        transfer_ms,
    }
}

/// One step from the placements of its tasks: placed at run time when any task is, else on
/// the first task's node with the staged bytes of all of them.
fn step_plan(
    step: String,
    places: &[&Placement],
    cached: bool,
    task_count: Option<u64>,
) -> PlannedStep {
    let runtime = places.iter().find_map(|place| match place {
        Placement::Runtime(hint) => Some(*hint),
        Placement::Planned { .. } => None,
    });
    if let Some(hint) = runtime {
        return PlannedStep {
            step,
            node_id: hint,
            mode: PLACED_AT_RUNTIME,
            bytes: None,
            transfer_ms: None,
            cached,
            task_count,
        };
    }
    let (mut bytes, mut transfer_ms) = (0u64, 0u64);
    for place in places {
        if let Placement::Planned {
            bytes: staged,
            transfer_ms: ms,
            ..
        } = place
        {
            bytes = bytes.saturating_add(*staged);
            transfer_ms = transfer_ms.max(*ms);
        }
    }
    PlannedStep {
        step,
        node_id: places.first().and_then(|place| place.node()),
        mode: match bytes {
            0 => COMPUTE_TO_DATA,
            _ => DATA_TO_COMPUTE,
        },
        bytes: Some(bytes),
        transfer_ms: Some(transfer_ms),
        cached,
        task_count,
    }
}

/// The tasks whose number the simulation cannot know: those after a task with markers.
fn downstream_of(tasks: &[SimTask], markers: &BTreeSet<u64>) -> BTreeSet<u64> {
    let mut unknown: BTreeSet<u64> = BTreeSet::new();
    for task in tasks {
        let after_marker = (task.upstream.iter())
            .any(|upstream| markers.contains(upstream) || unknown.contains(upstream));
        if after_marker {
            unknown.insert(task.id);
        }
    }
    unknown
}

/// The address a pinned input has in the run's values, matching `run_inputs`.
fn pin_address(local: NodeId, pin: &PinnedInput) -> String {
    match pin.source_node_id.filter(|node| *node != local) {
        Some(node) => super::data::address_on(&pin.bucket, &pin.key, Some(pin.version_id), node),
        None => super::data::address_of(&pin.bucket, &pin.key, Some(pin.version_id)),
    }
}

/// The pinned objects a simulated task reads, by the path it sees each at.
fn pinned_reads(task: &Value, pins: &[(String, PinnedInput)]) -> Vec<(String, PinnedInput)> {
    let mut reads = Vec::new();
    for input in task["inputs"].as_array().into_iter().flatten() {
        let source = input["source"].as_str().unwrap_or_default();
        let Some((_, pin)) = pins.iter().find(|(address, _)| address == source) else {
            continue;
        };
        let path = input["path"].as_str().unwrap_or(&pin.key).to_owned();
        reads.push((path, pin.clone()));
    }
    reads
}

/// The ceilings of a simulated task; the step's `aruna` settings win over the workflow's.
fn resources_of(resources: &Value, settings: Option<&ArunaStepSettings>) -> ComputeResources {
    let step = settings.map(|settings| &settings.resources);
    let cpus = (step.and_then(|step| step.cpus))
        .or_else(|| resources["cpus"].as_f64())
        .map(|cpus| cpus.ceil() as u32);
    let walltime = settings.and_then(ArunaStepSettings::walltime_seconds);
    ComputeResources {
        cpu_cores: cpus.filter(|cpus| *cpus > 0),
        ram_bytes: (settings.and_then(ArunaStepSettings::memory_bytes))
            .or_else(|| resources["memory_bytes"].as_u64()),
        disk_bytes: (settings.and_then(ArunaStepSettings::disk_bytes))
            .or_else(|| resources["disk_bytes"].as_u64()),
        max_walltime_ms: (walltime.or_else(|| resources["time_seconds"].as_u64()))
            .map(|seconds| seconds.saturating_mul(1000)),
        preemptible: false,
        gpus: (step.and_then(|step| step.gpus))
            .or_else(|| {
                resources["gpus"]
                    .as_u64()
                    .and_then(|gpus| u32::try_from(gpus).ok())
            })
            .filter(|gpus| *gpus > 0),
    }
}

/// A simulation message as a value diagnostic of the portal.
fn diagnostic_json(diagnostic: &Diagnostic) -> Value {
    let severity = match diagnostic.severity {
        Severity::Error => "error",
        Severity::Warning | Severity::Info => "warning",
    };
    let mut value = json!({"message": diagnostic.message, "severity": severity});
    if let Some(field) = &diagnostic.pointer.field {
        value["instance_path"] = Value::String(field.clone());
    }
    value
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
