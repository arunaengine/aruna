//! Preflight requests that run no task: checks, forms, composed graphs, tool cards, dry runs and
//! the reports computed from recorded run log pages, with their results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ContractError;
use crate::settings::ArunaStepSettings;

/// The files of a workflow version, by their paths relative to its folder.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSource {
    pub files: Vec<SourceFile>,
}

/// One file of a workflow, with its content.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFile {
    pub path: String,
    #[serde(with = "crate::encoding")]
    pub content: Vec<u8>,
    #[serde(default)]
    pub executable: bool,
}

/// The languages the coordinator runs; `None` in a request lets it detect the language.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowLanguage {
    Nextflow,
    Snakemake,
    Cwl,
    Argo,
    Wdl,
    Wir,
    Galaxy,
}

/// Which way a tool suggestion looks from the given port.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestDirection {
    /// Tool inputs that can take what an output gives.
    Followers,
    /// Tool outputs that can feed an input.
    Producers,
}

/// One page of the recorded run log of a run, in log order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunLogPage {
    pub entries: Vec<RunLogEntry>,
}

/// One record of a run log: the start of the run or one change, as the coordinator sent them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "entry", rename_all = "snake_case")]
pub enum RunLogEntry {
    Begin { run_start_json: Value },
    Change { seq: u64, change_json: Value },
}

/// A preflight request; each one is answered by one `PreflightResponse`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PreflightKind {
    /// Which parts of the workflow uwe compiles, runs natively or refuses, with their images.
    Check {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
    },
    /// Whether the outputs of the workflow depend on the order in which tasks finish.
    CheckOrder {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
    },
    /// The input form, the JSON Schemas and the step graph.
    Inspect {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
    },
    /// Checks input values against the form of the workflow.
    ValidateValues {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
        values: Value,
    },
    /// Reads a `.uwe.yaml` file of version 1 with its `aruna` step keys and lowers it to wir.
    /// `tool_files` holds the source files that the catalog tools name, from the yaml's folder.
    LowerComposed {
        uwe_yaml: String,
        catalog_tools: Vec<Value>,
        #[serde(default)]
        tool_files: WorkflowSource,
    },
    /// The tool cards of the steps of a workflow.
    ExtractTools {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
    },
    /// The catalog tools whose ports fit the described port, best first.
    SuggestTools {
        catalog_tools: Vec<Value>,
        direction: SuggestDirection,
        kind: Value,
        cardinality: Value,
        formats: Vec<String>,
        conditions: Vec<Value>,
        limit: u32,
    },
    /// Simulates a run: the tasks it would ask for, with made-up outputs.
    DryRun {
        source: WorkflowSource,
        language: Option<WorkflowLanguage>,
        entrypoint: String,
        inputs: Value,
        params: Map<String, Value>,
    },
    ComputeWhy {
        run_id: String,
        task_id: u64,
        log_pages: Vec<RunLogPage>,
    },
    ComputeCompare {
        first_run_id: String,
        second_run_id: String,
        first_log_pages: Vec<RunLogPage>,
        second_log_pages: Vec<RunLogPage>,
    },
    ComputeTime {
        run_id: String,
        log_pages: Vec<RunLogPage>,
    },
    /// What the run depends on: its sources, remote inputs and images, as recorded.
    ComputeChanges {
        run_id: String,
        log_pages: Vec<RunLogPage>,
    },
    /// The Workflow Run Crate of a recorded run. The source gives the workflow files the crate
    /// holds, as their recorded digests name them.
    BuildRunCrate {
        run_id: String,
        log_pages: Vec<RunLogPage>,
        base_uri: String,
        source: WorkflowSource,
        entrypoint: String,
    },
    /// The standalone HTML report of a recorded run.
    BuildReport {
        run_id: String,
        log_pages: Vec<RunLogPage>,
    },
}

/// Where a diagnostic points: a line of a text, a step, an edge or a value.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticPointer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<EdgePointer>,
    /// A dotted key below the node, like `aruna.resources.cpus`, or a form field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// An edge of a composed graph, by its ends as `<step>/<port>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgePointer {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: Severity,
    pub message: String,
    #[serde(default)]
    pub pointer: DiagnosticPointer,
}

/// The step graph of a workflow as the portal draws it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    /// The tool of a step and its version, for composed graphs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Input,
    Step,
    Output,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    /// The ports at both ends, where the graph knows them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_port: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_port: Option<String>,
}

/// What a `.uwe.yaml` file lowers to. Without errors, `wir_json` holds the wir document.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ComposedLowering {
    pub diagnostics: Vec<Diagnostic>,
    pub graph: WorkflowGraph,
    pub wir_json: Option<Value>,
    /// The `aruna` settings of each step that has them, by step id.
    pub step_settings: BTreeMap<String, ArunaStepSettings>,
    /// uwe's report of the graph check, per node and edge.
    pub check_json: Option<Value>,
}

/// The answer to a preflight request. uwe-typed parts travel as JSON.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum PreflightResult {
    Check {
        runnable: bool,
        report_json: Value,
        images: Vec<String>,
    },
    CheckOrder {
        order_dependent: bool,
        differences: Vec<String>,
    },
    Inspect {
        form_json: Value,
        graph: WorkflowGraph,
    },
    ValidateValues {
        valid: bool,
        response_json: Value,
    },
    LowerComposed(ComposedLowering),
    ExtractTools {
        tools: Vec<Value>,
    },
    SuggestTools {
        suggestions: Vec<Value>,
    },
    DryRun {
        graph_json: Value,
        tasks: Vec<Value>,
        diagnostics: Vec<Diagnostic>,
    },
    ComputeWhy {
        why_json: Option<Value>,
    },
    ComputeCompare {
        comparison_json: Option<Value>,
    },
    ComputeTime {
        time_json: Option<Value>,
    },
    ComputeChanges {
        changes_json: Value,
    },
    BuildRunCrate {
        metadata_json: Value,
        files: Vec<String>,
    },
    BuildReport {
        html: String,
    },
}

impl WorkflowSource {
    /// Refuses empty, absolute, parent and duplicate paths, which could leave the folder that
    /// the coordinator writes the files into.
    pub fn validate(&self) -> Result<(), ContractError> {
        let mut seen = std::collections::BTreeSet::new();
        for file in &self.files {
            check_path(&file.path)?;
            if !seen.insert(file.path.as_str()) {
                return Err(ContractError::invalid(format!(
                    "the workflow holds the file {} twice",
                    file.path
                )));
            }
        }
        Ok(())
    }

    pub fn file(&self, path: &str) -> Option<&SourceFile> {
        self.files.iter().find(|file| file.path == path)
    }
}

/// Checks that a path stays inside the folder it is relative to.
pub fn check_path(path: &str) -> Result<(), ContractError> {
    let bad = path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..");
    match bad {
        true => Err(ContractError::invalid(format!(
            "{path:?} is no relative path inside the workflow folder"
        ))),
        false => Ok(()),
    }
}

impl PreflightKind {
    /// The name of the request, as its JSON tag reads.
    pub fn name(&self) -> &'static str {
        match self {
            PreflightKind::Check { .. } => "check",
            PreflightKind::CheckOrder { .. } => "check_order",
            PreflightKind::Inspect { .. } => "inspect",
            PreflightKind::ValidateValues { .. } => "validate_values",
            PreflightKind::LowerComposed { .. } => "lower_composed",
            PreflightKind::ExtractTools { .. } => "extract_tools",
            PreflightKind::SuggestTools { .. } => "suggest_tools",
            PreflightKind::DryRun { .. } => "dry_run",
            PreflightKind::ComputeWhy { .. } => "compute_why",
            PreflightKind::ComputeCompare { .. } => "compute_compare",
            PreflightKind::ComputeTime { .. } => "compute_time",
            PreflightKind::ComputeChanges { .. } => "compute_changes",
            PreflightKind::BuildRunCrate { .. } => "build_run_crate",
            PreflightKind::BuildReport { .. } => "build_report",
        }
    }
}
