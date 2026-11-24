//! Answers preflight and report requests with uwe: support reports, forms, value checks,
//! composed graphs, tool cards and suggestions, dry runs, and reports over replayed run logs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use aruna_workflow_contract::preflight::{
    Diagnostic, DiagnosticPointer, GraphEdge, GraphNode, NodeKind, RunLogPage, SuggestDirection,
    WorkflowGraph, WorkflowLanguage, WorkflowSource,
};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use executor::MemoryJournal;
use run_database::{MemoryStore, RunReader};
use serde_json::{Map, Value, json};
use tool_definition::{Definition, InputPort, Ranking, Shape, Supply};
use uwe::application::Policy;
use uwe::forms::{Form, ValidationResponse};
use uwe::run::{Run, RunRequest, SecretValues};
use uwe::tools::ToolLibrary;
use workflow_graph::{CancellationToken, ExecutionGraph, MORE_TASKS};

use crate::changes::{Relocation, replay_pages};
use crate::composed;
use crate::run_store::{ReportReader, memory_store};
use crate::source::{Workspace, io_error};

/// Whether a request reads recorded runs, which a coordinator in report mode answers.
pub fn is_report(kind: &PreflightKind) -> bool {
    matches!(
        kind,
        PreflightKind::ComputeWhy { .. }
            | PreflightKind::ComputeCompare { .. }
            | PreflightKind::ComputeTime { .. }
            | PreflightKind::ComputeChanges { .. }
            | PreflightKind::BuildRunCrate { .. }
            | PreflightKind::BuildReport { .. }
    )
}

/// Answers one request. Nothing it does starts a task or reaches the host.
pub async fn answer(
    kind: PreflightKind,
    cancel: CancellationToken,
) -> Result<PreflightResult, ContractError> {
    match kind {
        PreflightKind::Check {
            source,
            language,
            entrypoint,
        } => check(&source, language, &entrypoint, cancel).await,
        PreflightKind::CheckOrder { .. } => Err(ContractError::unsupported(
            "uwe gives embedders no choice of the order in which tasks finish, which the order \
             check needs; it stays a command of uwe itself",
        )),
        PreflightKind::Inspect {
            source,
            language,
            entrypoint,
        } => inspect(&source, language, &entrypoint, cancel).await,
        PreflightKind::ValidateValues {
            source,
            language,
            entrypoint,
            values,
        } => {
            let workspace = Workspace::create(&source, &entrypoint)?;
            let form = form(&workspace, language, cancel).await?;
            let response = form.validate_response(values);
            let valid =
                matches!(&response, ValidationResponse::Checked(checked) if checked.accepted);
            Ok(PreflightResult::ValidateValues {
                valid,
                response_json: to_json(&response)?,
            })
        }
        PreflightKind::LowerComposed {
            uwe_yaml,
            catalog_tools,
            tool_files,
        } => {
            blocking(move || {
                let folder = tool_folder(&tool_files)?;
                let lowering = composed::lower_composed(&uwe_yaml, &catalog_tools, folder.path());
                Ok(PreflightResult::LowerComposed(lowering))
            })
            .await
        }
        PreflightKind::ExtractTools {
            source,
            language: _,
            entrypoint,
        } => blocking(move || extract_tools(&source, &entrypoint)).await,
        PreflightKind::SuggestTools {
            catalog_tools,
            direction,
            kind,
            cardinality,
            formats,
            conditions,
            limit,
        } => {
            blocking(move || {
                let port = PortQuery {
                    kind,
                    cardinality,
                    formats,
                    conditions,
                };
                suggest_tools(&catalog_tools, direction, port, limit)
            })
            .await
        }
        PreflightKind::DryRun {
            source,
            language,
            entrypoint,
            inputs,
            params,
        } => {
            let workspace = Workspace::create(&source, &entrypoint)?;
            let run = simulate(&workspace, language, inputs, params, cancel).await?;
            dry_result(&run)
        }
        report => blocking(move || answer_report(report)).await,
    }
}

async fn check(
    source: &WorkflowSource,
    language: Option<WorkflowLanguage>,
    entrypoint: &str,
    cancel: CancellationToken,
) -> Result<PreflightResult, ContractError> {
    let workspace = Workspace::create(source, entrypoint)?;
    let mut settings = workspace.settings(language, &SecretValues::default())?;
    settings.cancel = cancel;
    let report = RunRequest::new(settings)
        .support()
        .await
        .map_err(|failure| ContractError::workflow(failure.to_string()))?;
    let images = report.images().into_iter().map(str::to_owned).collect();
    Ok(PreflightResult::Check {
        runnable: report.runnable(),
        report_json: to_json(&report)?,
        images,
    })
}

/// The form of a workflow and the step graph of a simulated run with its default values.
async fn inspect(
    source: &WorkflowSource,
    language: Option<WorkflowLanguage>,
    entrypoint: &str,
    cancel: CancellationToken,
) -> Result<PreflightResult, ContractError> {
    let workspace = Workspace::create(source, entrypoint)?;
    let form = form(&workspace, language, cancel.clone()).await?;
    let form_json = to_json(&form.response())?;
    if uwe::tools::composed::is_composed(&workspace.entry) {
        let entry = workspace.entry.clone();
        let graph = blocking(move || {
            let mut library = ToolLibrary::new();
            let workflow = uwe::tools::composed::load(&entry, &mut library)
                .map_err(ContractError::workflow)?;
            Ok(composed::graph_of(&workflow))
        })
        .await?;
        return Ok(PreflightResult::Inspect { form_json, graph });
    }
    // Without values a simulation may stop early; the form stays the answer then.
    let graph = match simulate(&workspace, language, Value::Null, Map::new(), cancel).await {
        Ok(run) => step_graph(&run.execution.graph),
        Err(error) => {
            tracing::debug!(%error, "no step graph without values");
            WorkflowGraph::default()
        }
    };
    Ok(PreflightResult::Inspect { form_json, graph })
}

async fn form(
    workspace: &Workspace,
    language: Option<WorkflowLanguage>,
    cancel: CancellationToken,
) -> Result<Form, ContractError> {
    let mut settings = workspace.settings(language, &SecretValues::default())?;
    settings.cancel = cancel;
    let description = RunRequest::new(settings)
        .describe()
        .await
        .map_err(|failure| ContractError::workflow(failure.to_string()))?;
    Form::new(description, Policy::Relaxed)
        .map_err(|error| ContractError::workflow(error.to_string()))
}

/// A run with the simulator: made-up outputs, an in-memory run database, no task started.
async fn simulate(
    workspace: &Workspace,
    language: Option<WorkflowLanguage>,
    inputs: Value,
    params: Map<String, Value>,
    cancel: CancellationToken,
) -> Result<Run, ContractError> {
    let mut settings = workspace.settings(language, &SecretValues::default())?;
    settings.executor = Some("simulator".into());
    settings.run_store = Some(Arc::new(memory_store()?));
    settings.journal_store = Some(Arc::new(MemoryJournal::new()));
    if !inputs.is_null() {
        settings.inputs = Some(inputs);
    }
    settings.params = params;
    settings.cancel = cancel;
    let request = RunRequest {
        settings,
        policy: Policy::Relaxed,
    };
    request
        .run()
        .await
        .map_err(|failure| ContractError::workflow(failure.to_string()))
}

fn dry_result(run: &Run) -> Result<PreflightResult, ContractError> {
    let graph = &run.execution.graph;
    let mut diagnostics = Vec::new();
    if let Some(error) = &run.execution.error {
        diagnostics.push(composed::error(
            error.to_string(),
            DiagnosticPointer::default(),
        ));
    }
    let mut tasks = Vec::new();
    for node in &graph.nodes {
        let task = &node.task;
        let more = !node.markers.is_empty();
        tasks.push(json!({
            "id": task.id.0,
            "name": task.name,
            "step": task.step,
            "image": task.image,
            "markers": node.markers,
            "more_tasks": more,
        }));
        for marker in &node.markers {
            let pointer = DiagnosticPointer {
                node: Some(task.step.clone()),
                ..DiagnosticPointer::default()
            };
            let message = format!("{}: {}; {MORE_TASKS}", task.name, marker.reason);
            diagnostics.push(Diagnostic {
                severity: aruna_workflow_contract::preflight::Severity::Info,
                message,
                pointer,
            });
        }
    }
    Ok(PreflightResult::DryRun {
        graph_json: to_json(graph)?,
        tasks,
        diagnostics,
    })
}

/// The steps of a run and the edges between them, as the portal draws them.
fn step_graph(graph: &ExecutionGraph) -> WorkflowGraph {
    let steps = graph.step_graph();
    WorkflowGraph {
        nodes: (steps.steps.iter())
            .map(|step| GraphNode {
                id: step.name.clone(),
                label: step.name.clone(),
                kind: NodeKind::Step,
                tool: None,
                version: None,
            })
            .collect(),
        edges: (steps.edges.iter())
            .map(|edge| GraphEdge {
                from: edge.from.clone(),
                to: edge.to.clone(),
                from_port: None,
                to_port: None,
            })
            .collect(),
    }
}

/// The tool cards of the entry file, with their source addresses relative to the workflow.
fn extract_tools(
    source: &WorkflowSource,
    entrypoint: &str,
) -> Result<PreflightResult, ContractError> {
    let workspace = Workspace::create(source, entrypoint)?;
    let base = workspace
        .entry
        .parent()
        .unwrap_or(workspace.path())
        .to_owned();
    let folder = workspace.path().join("workflow");
    let definitions = uwe::tools::extract(&workspace.entry).map_err(ContractError::workflow)?;
    let mut tools = Vec::new();
    for mut definition in definitions {
        if let Some(source) = definition.source.as_mut() {
            let address = Path::new(&source.address);
            let inside = address
                .strip_prefix(&folder)
                .or_else(|_| address.strip_prefix(&base));
            if let Ok(relative) = inside {
                source.address = relative.to_string_lossy().into_owned();
            }
        }
        tools.push(to_json(&definition)?);
    }
    Ok(PreflightResult::ExtractTools { tools })
}

/// The port a suggestion starts from.
struct PortQuery {
    kind: Value,
    cardinality: Value,
    formats: Vec<String>,
    conditions: Vec<Value>,
}

fn suggest_tools(
    catalog: &[Value],
    direction: SuggestDirection,
    port: PortQuery,
    limit: u32,
) -> Result<PreflightResult, ContractError> {
    let mut library = ToolLibrary::new();
    for tool in catalog {
        let definition: Definition = serde_json::from_value(tool.clone())
            .map_err(|error| ContractError::invalid(format!("a catalog tool: {error}")))?;
        library.add(definition, None);
    }
    let invalid = |what: &str, error: serde_json::Error| {
        ContractError::invalid(format!("the port {what}: {error}"))
    };
    let ranking = Ranking {
        operations: Vec::new(),
        uses: library.uses().clone(),
    };
    let suggestions = match direction {
        SuggestDirection::Followers => {
            let supply = Supply {
                kind: serde_json::from_value(port.kind).map_err(|error| invalid("kind", error))?,
                shape: Shape {
                    cardinality: serde_json::from_value(port.cardinality)
                        .map_err(|error| invalid("cardinality", error))?,
                    per_sample: false,
                },
                formats: port.formats,
                facts: serde_json::from_value(Value::Array(port.conditions))
                    .map_err(|error| invalid("conditions", error))?,
            };
            library.suggest_followers(&supply, &ranking)
        }
        SuggestDirection::Producers => {
            let input: InputPort = serde_json::from_value(json!({
                "name": "port",
                "kind": port.kind,
                "cardinality": port.cardinality,
                "formats": port.formats,
                "conditions": port.conditions,
            }))
            .map_err(|error| invalid("description", error))?;
            library.suggest_producers(&input, &ranking)
        }
    };
    let suggestions = (suggestions.iter())
        .take(limit as usize)
        .map(to_json)
        .collect::<Result<_, _>>()?;
    Ok(PreflightResult::SuggestTools { suggestions })
}

/// A folder with the tool files of a composed workflow, which stays while it lowers.
fn tool_folder(files: &WorkflowSource) -> Result<tempfile::TempDir, ContractError> {
    files.validate()?;
    let folder = tempfile::Builder::new()
        .prefix("aruna-tools-")
        .tempdir()
        .map_err(io_error)?;
    for file in &files.files {
        let path = folder.path().join(&file.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(io_error)?;
        }
        std::fs::write(&path, &file.content).map_err(io_error)?;
    }
    Ok(folder)
}

fn answer_report(kind: PreflightKind) -> Result<PreflightResult, ContractError> {
    match kind {
        PreflightKind::ComputeWhy {
            run_id,
            task_id,
            log_pages,
        } => {
            let store = replayed(&[&log_pages], Path::new(REPLAY_ROOT))?;
            known_run(&store, &run_id)?;
            let why = store.why(&run_id, task_id).map_err(read_error)?;
            Ok(PreflightResult::ComputeWhy {
                why_json: why.map(|why| to_json(&why)).transpose()?,
            })
        }
        PreflightKind::ComputeCompare {
            first_run_id,
            second_run_id,
            first_log_pages,
            second_log_pages,
        } => {
            let store = replayed(
                &[&first_log_pages, &second_log_pages],
                Path::new(REPLAY_ROOT),
            )?;
            known_run(&store, &first_run_id)?;
            known_run(&store, &second_run_id)?;
            let comparison = (store.compare(&first_run_id, &second_run_id)).map_err(read_error)?;
            Ok(PreflightResult::ComputeCompare {
                comparison_json: comparison.map(|found| to_json(&found)).transpose()?,
            })
        }
        PreflightKind::ComputeTime { run_id, log_pages } => {
            let store = replayed(&[&log_pages], Path::new(REPLAY_ROOT))?;
            known_run(&store, &run_id)?;
            let time = store.time_spent(&run_id).map_err(read_error)?;
            Ok(PreflightResult::ComputeTime {
                time_json: time.map(|time| to_json(&time)).transpose()?,
            })
        }
        PreflightKind::ComputeChanges { run_id, log_pages } => {
            let store = replayed(&[&log_pages], Path::new(REPLAY_ROOT))?;
            let run = known_run(&store, &run_id)?;
            // The node compares these with what its storage and registries hold now.
            let changes_json = json!({
                "run": to_json(&run)?,
                "sources": to_json(&store.sources(&run_id).map_err(read_error)?)?,
                "remote_files": to_json(&store.remote_files(&run_id).map_err(read_error)?)?,
                "images": to_json(&store.images(&run_id).map_err(read_error)?)?,
            });
            Ok(PreflightResult::ComputeChanges { changes_json })
        }
        PreflightKind::BuildRunCrate {
            run_id,
            log_pages,
            base_uri,
            source,
            entrypoint,
        } => build_crate(&run_id, &log_pages, &base_uri, &source, &entrypoint),
        PreflightKind::BuildReport { run_id, log_pages } => {
            let store = replayed(&[&log_pages], Path::new(REPLAY_ROOT))?;
            known_run(&store, &run_id)?;
            let html = run_page::report(&ReportReader { store }, &run_id)
                .map_err(read_error)?
                .ok_or_else(|| not_found(&run_id))?;
            Ok(PreflightResult::BuildReport { html })
        }
        other => Err(ContractError::invalid(format!(
            "{} is no report request",
            other.name()
        ))),
    }
}

/// The Workflow Run Crate of a recorded run, with its files described where they are and the
/// files of its store under `base_uri`.
fn build_crate(
    run_id: &str,
    log_pages: &[RunLogPage],
    base_uri: &str,
    source: &WorkflowSource,
    entrypoint: &str,
) -> Result<PreflightResult, ContractError> {
    let workspace = Workspace::create(source, entrypoint)?;
    let store = replayed(&[log_pages], workspace.path())?;
    let run = known_run(&store, run_id)?;
    let mut sources = HashMap::new();
    for file in &source.files {
        let digest = task_model::Digest::sha256(&file.content).to_string();
        sources.insert(digest, workspace.path().join("workflow").join(&file.path));
    }
    let mut uri_map = BTreeMap::new();
    if let Some(folder) = run.store.as_deref().filter(|_| !base_uri.is_empty()) {
        uri_map.insert(
            format!("{}/", folder.trim_end_matches('/')),
            base_uri.to_owned(),
        );
    }
    let options = run_crate::Options {
        workflow: workspace.entry.clone(),
        sources,
        describe_data: true,
        uri_map,
        ..run_crate::Options::default()
    };
    let target = workspace.path().join("crate");
    let database = store.lock();
    run_crate::export(&database, run_id, &target, &options)
        .map_err(|error| ContractError::workflow(error.to_string()))?;
    drop(database);
    let metadata = std::fs::read(target.join("ro-crate-metadata.json")).map_err(io_error)?;
    let metadata_json = serde_json::from_slice(&metadata)
        .map_err(|error| ContractError::internal(format!("the crate metadata: {error}")))?;
    let mut files = Vec::new();
    let mut pending = vec![target.clone()];
    while let Some(folder) = pending.pop() {
        for entry in std::fs::read_dir(&folder).map_err(io_error)? {
            let path = entry.map_err(io_error)?.path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(relative) = path.strip_prefix(&target) {
                files.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    files.sort();
    Ok(PreflightResult::BuildRunCrate {
        metadata_json,
        files,
    })
}

/// The root that replayed runs move to, so that runs of one workflow compare; no file is
/// read there.
const REPLAY_ROOT: &str = "/aruna-coordinator/replay";

/// An in-memory run database with the recorded runs of the page lists, moved to `root`.
fn replayed(lists: &[&[RunLogPage]], root: &Path) -> Result<MemoryStore, ContractError> {
    let store = memory_store()?;
    let relocation = Relocation {
        root: root.to_owned(),
    };
    for pages in lists {
        replay_pages(&store, pages, Some(&relocation))?;
    }
    Ok(store)
}

fn known_run(store: &MemoryStore, run_id: &str) -> Result<run_database::Run, ContractError> {
    store
        .run(run_id)
        .map_err(read_error)?
        .ok_or_else(|| not_found(run_id))
}

fn not_found(run_id: &str) -> ContractError {
    ContractError::NotFound {
        message: format!("the run log holds no run {run_id}"),
    }
}

fn read_error(error: run_database::DatabaseError) -> ContractError {
    ContractError::internal(format!("run database: {error}"))
}

fn to_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Value, ContractError> {
    serde_json::to_value(value)
        .map_err(|error| ContractError::internal(format!("a result does not write: {error}")))
}

/// Runs blocking uwe work off the async workers.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ContractError> + Send + 'static,
) -> Result<T, ContractError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| ContractError::internal(format!("a worker failed: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composed::warning;

    #[test]
    fn separates_report_requests() {
        let pages = Vec::new();
        assert!(is_report(&PreflightKind::BuildReport {
            run_id: "r".into(),
            log_pages: pages,
        }));
        assert!(!is_report(&PreflightKind::LowerComposed {
            uwe_yaml: String::new(),
            catalog_tools: Vec::new(),
            tool_files: WorkflowSource::default(),
        }));
    }

    #[test]
    fn warns_missing_tools() {
        let text = "uwe_version: \"1\"\nsteps:\n  count:\n    tool: count\n";
        let folder = tempfile::tempdir().expect("a folder");
        let lowering = composed::lower_composed(text, &[json!({"id": 1})], folder.path());
        let messages: Vec<_> = (lowering.diagnostics.iter())
            .map(|diagnostic| (diagnostic.severity, diagnostic.pointer.node.clone()))
            .collect();
        assert_eq!(
            messages,
            [
                (warning("", DiagnosticPointer::default()).severity, None),
                (
                    aruna_workflow_contract::preflight::Severity::Error,
                    Some("count".into())
                ),
            ]
        );
        assert_eq!(lowering.graph.nodes.len(), 1);
        assert!(lowering.wir_json.is_none());
    }
}
