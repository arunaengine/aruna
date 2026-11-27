//! The request and response bodies of the workflow registry and the tool catalog, with the
//! field names the portal reads, and their mapping to and from the domain values.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, ToolCuration, WorkflowVersionRecord, WorkflowVisibility,
};
use aruna_ops_work::workflows::reads::WorkflowView;
use aruna_ops_work::workflows::tools::{ToolView, tool_language};
use aruna_ops_work::workflows::{CheckReport, GraphView};
use aruna_workflow_contract::preflight::{Diagnostic, NodeKind, Severity};
use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

use crate::error::ServerError;

/// The language of a workflow; `uwe` is a composed `.uwe.yaml` workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowLanguage {
    Uwe,
    Nextflow,
    Snakemake,
    Cwl,
    Wdl,
    Galaxy,
    Argo,
    Wir,
}

/// Who reads a workflow: its group, or every user of the realm.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    #[default]
    Group,
    Public,
}

/// How far the catalog trusts a tool definition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Curation {
    Curated,
    Imported,
    Unchecked,
}

/// One workflow as lists and reads show it.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WorkflowSummary {
    pub workflow_id: String,
    pub group_id: String,
    pub name: String,
    pub description: Option<String>,
    pub language: WorkflowLanguage,
    pub visibility: Visibility,
    pub latest_version: Option<String>,
    pub version_count: Option<u32>,
    pub edam_formats: Vec<String>,
    /// RFC 3339.
    pub updated_at: Option<String>,
    pub created_by: Option<String>,
    /// Whether the caller may edit, version and delete it.
    pub can_write: bool,
}

/// One page of workflows.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WorkflowPage {
    pub items: Vec<WorkflowSummary>,
    pub next_cursor: Option<String>,
    pub total: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WorkflowQuery {
    /// Only this group's workflows; without it, every group the caller reads and every public one.
    pub group_id: Option<String>,
    /// Only this language.
    pub language: Option<String>,
    /// Only workflows that name this EDAM format.
    pub edam_format: Option<String>,
    /// Words of the name or description, in any case.
    pub q: Option<String>,
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
    /// Items per page, 1 to 500; 50 when left out.
    pub limit: Option<usize>,
}

/// How a file's content is written: UTF-8 text, or base64 for other bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileEncoding {
    #[default]
    Utf8,
    Base64,
}

/// One file of a workflow, by its path inside the workflow folder.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SourceFileBody {
    pub path: String,
    pub content: String,
    #[serde(default)]
    pub encoding: Option<FileEncoding>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct GitSourceBody {
    pub url: String,
    pub revision: Option<String>,
    pub path: Option<String>,
    pub entrypoint: Option<String>,
}

/// Objects in a bucket, read on the node that holds it.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ObjectsSourceBody {
    pub bucket: String,
    pub prefix: Option<String>,
    #[serde(default)]
    pub keys: Vec<String>,
    pub node_id: Option<String>,
    pub entrypoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct UploadSourceBody {
    pub files: Vec<SourceFileBody>,
    pub entrypoint: String,
}

/// Exactly one source of the files of a version.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowSourceBody {
    Git(GitSourceBody),
    Objects(ObjectsSourceBody),
    Upload(UploadSourceBody),
    /// A wir document.
    Wir(Value),
    /// A composed `.uwe.yaml` text.
    UweYaml(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateWorkflowRequest {
    pub group_id: String,
    pub name: String,
    pub description: Option<String>,
    pub language: Option<WorkflowLanguage>,
    pub visibility: Option<Visibility>,
    #[serde(flatten)]
    pub source: WorkflowSourceBody,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateVersionRequest {
    #[serde(flatten)]
    pub source: WorkflowSourceBody,
    pub message: Option<String>,
}

/// What an import stored.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WorkflowCreated {
    pub workflow_id: String,
    pub version: String,
    /// The commit a Git import read.
    pub commit: Option<String>,
    pub digest: Option<String>,
}

/// The fields to change; a `description` of null clears it.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct WorkflowPatchBody {
    pub name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub description: Option<Option<String>>,
    pub visibility: Option<Visibility>,
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowDiagnostic {
    /// `error`, `warning` or `info`.
    pub severity: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
    /// The step or input the message is about.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The wire the message is about, `<step>/<output>-><step>/<input>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowCheck {
    pub ok: bool,
    pub diagnostics: Vec<WorkflowDiagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WorkflowVersionBody {
    pub workflow_id: String,
    pub version: String,
    pub language: WorkflowLanguage,
    pub entrypoint: Option<String>,
    pub files: Vec<String>,
    pub commit: Option<String>,
    pub digest: Option<String>,
    pub created_at: Option<String>,
    pub created_by: Option<String>,
    pub message: Option<String>,
    /// The `.uwe.yaml` text of a composed workflow.
    pub uwe_yaml: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct VersionPage {
    pub items: Vec<WorkflowVersionBody>,
    pub next_cursor: Option<String>,
    pub total: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageQuery {
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
    /// Items per page, 1 to 500; 50 when left out.
    pub limit: Option<usize>,
}

/// `input`, `step`, `subworkflow` or `output`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeKind {
    Input,
    Step,
    Subworkflow,
    Output,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowGraphNode {
    pub id: String,
    pub kind: GraphNodeKind,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// One end of an edge: a node and its port.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct GraphEnd {
    pub node: String,
    pub port: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowGraphEdge {
    pub from: GraphEnd,
    pub to: GraphEnd,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scatter: Option<bool>,
    /// `fits`, `unknown` or `does_not_fit`, where the check knows it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fit: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowGraphBody {
    pub nodes: Vec<WorkflowGraphNode>,
    pub edges: Vec<WorkflowGraphEdge>,
    /// False when dynamic branches or scatters are only known at run time.
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ValuesRequest {
    pub values: Value,
}

/// Whether input values fit the form, with uwe's diagnostics per value.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ValuesCheck {
    pub ok: bool,
    #[schema(value_type = Vec<Object>)]
    pub diagnostics: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateRequest {
    pub language: WorkflowLanguage,
    /// The group whose catalog tools a composed text names.
    pub group_id: Option<String>,
    pub uwe_yaml: Option<String>,
    #[serde(default)]
    pub files: Vec<SourceFileBody>,
    pub entrypoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ValidateResponse {
    pub check: WorkflowCheck,
    pub graph: Option<WorkflowGraphBody>,
    /// The workflow lowered to wir, when it checks.
    #[schema(value_type = Option<Object>)]
    pub wir: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ToolSummary {
    pub tool_id: String,
    pub version: String,
    pub title: String,
    pub description: Option<String>,
    /// A workflow language, `container` or `other`.
    pub language: Option<String>,
    pub group_id: Option<String>,
    pub curation: Curation,
    pub image: Option<String>,
    /// The tool is a whole workflow.
    pub workflow: bool,
    #[schema(value_type = Vec<Object>)]
    pub inputs: Vec<Value>,
    #[schema(value_type = Vec<Object>)]
    pub outputs: Vec<Value>,
    #[schema(value_type = Option<Object>)]
    pub parameters: Option<Value>,
    pub operations: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ToolDetail {
    #[serde(flatten)]
    pub summary: ToolSummary,
    /// The full tool definition, as uwe's tool-definition crate writes it.
    #[schema(value_type = Object)]
    pub definition: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ToolPage {
    pub items: Vec<ToolSummary>,
    pub next_cursor: Option<String>,
    pub total: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ToolList {
    pub tools: Vec<ToolSummary>,
}

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ToolQuery {
    /// Only this group's tools; without it, every group the caller reads.
    pub group_id: Option<String>,
    /// A workflow language, `container` or `other`.
    pub language: Option<String>,
    /// An EDAM format a port takes or gives.
    pub format: Option<String>,
    /// Only tools of this curation.
    pub curation: Option<Curation>,
    /// Words of the id, title or description, in any case.
    pub q: Option<String>,
    /// The `next_cursor` of the page before.
    pub cursor: Option<String>,
    /// Items per page, 1 to 500; 50 when left out.
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateToolRequest {
    pub group_id: String,
    #[schema(value_type = Object)]
    pub definition: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRef {
    pub workflow_id: String,
    pub version: String,
}

/// A stored version, uploaded files, or Git.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ExtractSourceBody {
    Workflow {
        workflow: WorkflowRef,
    },
    Git {
        git: GitSourceBody,
        language: Option<WorkflowLanguage>,
    },
    Files {
        files: Vec<SourceFileBody>,
        entrypoint: String,
        language: WorkflowLanguage,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractRequest {
    pub group_id: String,
    pub source: ExtractSourceBody,
    /// Adds the tools to the group's catalog; without it they are only shown.
    #[serde(default)]
    pub save: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractResponse {
    pub tools: Vec<ToolSummary>,
    pub diagnostics: Vec<WorkflowDiagnostic>,
}

/// Tools that read an output, or tools that feed an input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SuggestWay {
    Next,
    Previous,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SuggestRequest {
    pub group_id: Option<String>,
    pub direction: SuggestWay,
    /// `file`, `directory` or `{"value": <type>}`.
    pub kind: Value,
    pub cardinality: Option<Value>,
    #[serde(default)]
    pub formats: Vec<String>,
    #[serde(default)]
    #[schema(value_type = Vec<Object>)]
    pub conditions: Vec<Value>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct ToolSuggestion {
    pub tool: ToolSummary,
    pub port: String,
    pub fit: String,
    pub scatter: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SuggestResponse {
    pub suggestions: Vec<ToolSuggestion>,
}

impl From<WorkflowLanguage> for StoredWorkflowLanguage {
    fn from(language: WorkflowLanguage) -> Self {
        match language {
            WorkflowLanguage::Uwe => Self::Uwe,
            WorkflowLanguage::Nextflow => Self::Nextflow,
            WorkflowLanguage::Snakemake => Self::Snakemake,
            WorkflowLanguage::Cwl => Self::Cwl,
            WorkflowLanguage::Wdl => Self::Wdl,
            WorkflowLanguage::Galaxy => Self::Galaxy,
            WorkflowLanguage::Argo => Self::Argo,
            WorkflowLanguage::Wir => Self::Wir,
        }
    }
}

impl From<StoredWorkflowLanguage> for WorkflowLanguage {
    fn from(language: StoredWorkflowLanguage) -> Self {
        match language {
            StoredWorkflowLanguage::Uwe => Self::Uwe,
            StoredWorkflowLanguage::Nextflow => Self::Nextflow,
            StoredWorkflowLanguage::Snakemake => Self::Snakemake,
            StoredWorkflowLanguage::Cwl => Self::Cwl,
            StoredWorkflowLanguage::Wdl => Self::Wdl,
            StoredWorkflowLanguage::Galaxy => Self::Galaxy,
            StoredWorkflowLanguage::Argo => Self::Argo,
            StoredWorkflowLanguage::Wir => Self::Wir,
        }
    }
}

impl From<Visibility> for WorkflowVisibility {
    fn from(visibility: Visibility) -> Self {
        match visibility {
            Visibility::Group => Self::Group,
            Visibility::Public => Self::Public,
        }
    }
}

impl From<WorkflowVisibility> for Visibility {
    fn from(visibility: WorkflowVisibility) -> Self {
        match visibility {
            WorkflowVisibility::Group => Self::Group,
            WorkflowVisibility::Public => Self::Public,
        }
    }
}

impl From<Curation> for ToolCuration {
    fn from(curation: Curation) -> Self {
        match curation {
            Curation::Curated => Self::Curated,
            Curation::Imported => Self::Imported,
            Curation::Unchecked => Self::Unchecked,
        }
    }
}

impl From<ToolCuration> for Curation {
    fn from(curation: ToolCuration) -> Self {
        match curation {
            ToolCuration::Curated => Self::Curated,
            ToolCuration::Imported => Self::Imported,
            ToolCuration::Unchecked => Self::Unchecked,
        }
    }
}

/// Milliseconds since the Unix epoch in RFC 3339.
pub fn rfc3339(ms: u64) -> Option<String> {
    let ms = i64::try_from(ms).ok()?;
    chrono::DateTime::from_timestamp_millis(ms).map(|time| time.to_rfc3339())
}

impl From<WorkflowView> for WorkflowSummary {
    fn from(view: WorkflowView) -> Self {
        let record = view.record;
        let latest = (record.version_count > 0).then(|| record.version_count.to_string());
        Self {
            workflow_id: record.workflow_id.to_string(),
            group_id: record.group_id.to_string(),
            name: record.name,
            description: record.description,
            language: record.language.into(),
            visibility: record.visibility.into(),
            latest_version: latest,
            version_count: Some(record.version_count),
            edam_formats: record.edam_formats,
            updated_at: rfc3339(record.updated_at_ms),
            created_by: Some(record.created_by.to_string()),
            can_write: view.can_write,
        }
    }
}

impl From<WorkflowVersionRecord> for WorkflowVersionBody {
    fn from(version: WorkflowVersionRecord) -> Self {
        Self {
            workflow_id: version.workflow_id.to_string(),
            version: version.version.to_string(),
            language: version.language.into(),
            entrypoint: Some(version.entrypoint),
            files: version.files,
            commit: version.commit,
            digest: Some(version.digest),
            created_at: rfc3339(version.created_at_ms),
            created_by: Some(version.created_by.to_string()),
            message: version.message,
            uwe_yaml: version.uwe_yaml,
        }
    }
}

impl From<&Diagnostic> for WorkflowDiagnostic {
    fn from(diagnostic: &Diagnostic) -> Self {
        let pointer = &diagnostic.pointer;
        let severity = match diagnostic.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        };
        Self {
            severity: severity.into(),
            message: diagnostic.message.clone(),
            code: pointer.field.clone(),
            file: None,
            line: pointer.line,
            column: None,
            node: pointer.node.clone(),
            edge: (pointer.edge.as_ref()).map(|edge| format!("{}->{}", edge.from, edge.to)),
        }
    }
}

impl From<CheckReport> for WorkflowCheck {
    fn from(report: CheckReport) -> Self {
        Self {
            ok: report.ok,
            diagnostics: report
                .diagnostics
                .iter()
                .map(WorkflowDiagnostic::from)
                .collect(),
        }
    }
}

impl From<GraphView> for WorkflowGraphBody {
    fn from(view: GraphView) -> Self {
        let nodes = (view.graph.nodes.into_iter())
            .map(|node| WorkflowGraphNode {
                id: node.id,
                kind: match node.kind {
                    NodeKind::Input => GraphNodeKind::Input,
                    NodeKind::Step => GraphNodeKind::Step,
                    NodeKind::Output => GraphNodeKind::Output,
                },
                label: node.label,
                tool: node.tool,
                version: node.version,
            })
            .collect();
        let edges = (view.graph.edges.into_iter())
            .map(|edge| WorkflowGraphEdge {
                from: GraphEnd {
                    node: edge.from,
                    port: edge.from_port.unwrap_or_default(),
                },
                to: GraphEnd {
                    node: edge.to,
                    port: edge.to_port.unwrap_or_default(),
                },
                scatter: None,
                fit: None,
            })
            .collect();
        Self {
            nodes,
            edges,
            complete: view.complete,
        }
    }
}

impl ToolSummary {
    /// The summary of a definition as uwe writes it, outside any catalog.
    pub fn of_definition(definition: &Value) -> Self {
        let text = |field: &str| definition[field].as_str().map(str::to_owned);
        let list = |field: &str| definition[field].as_array().cloned().unwrap_or_default();
        let operations = (definition["operations"].as_array().into_iter().flatten())
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let curation = match definition["curation"].as_str() {
            Some("curated") => Curation::Curated,
            Some("imported") => Curation::Imported,
            _ => Curation::Unchecked,
        };
        let parameters = &definition["parameters"];
        Self {
            tool_id: text("id").unwrap_or_default(),
            version: text("version").unwrap_or_default(),
            title: text("title").unwrap_or_default(),
            description: text("description"),
            language: tool_language(definition).map(str::to_owned),
            group_id: None,
            curation,
            image: definition["image"]["name"].as_str().map(str::to_owned),
            workflow: definition["workflow"].as_bool().unwrap_or(false),
            inputs: list("inputs"),
            outputs: list("outputs"),
            parameters: (!parameters.is_null()).then(|| parameters.clone()),
            operations,
        }
    }
}

impl From<&ToolView> for ToolSummary {
    fn from(tool: &ToolView) -> Self {
        Self {
            tool_id: tool.record.tool_id.clone(),
            version: tool.record.version.clone(),
            title: tool.record.title.clone(),
            group_id: Some(tool.record.group_id.to_string()),
            curation: tool.record.curation.into(),
            ..Self::of_definition(&tool.definition)
        }
    }
}

impl From<&ToolView> for ToolDetail {
    fn from(tool: &ToolView) -> Self {
        Self {
            summary: ToolSummary::from(tool),
            definition: tool.definition.clone(),
        }
    }
}

impl SourceFileBody {
    /// The bytes of the file as its encoding writes them.
    pub fn decoded(self) -> Result<aruna_workflow_contract::preflight::SourceFile, ServerError> {
        let content = match self.encoding.unwrap_or_default() {
            FileEncoding::Utf8 => self.content.into_bytes(),
            FileEncoding::Base64 => base64::engine::general_purpose::STANDARD
                .decode(self.content.as_bytes())
                .map_err(|_| {
                    ServerError::BadRequestReason(format!("{} is no base64 text", self.path))
                })?,
        };
        Ok(aruna_workflow_contract::preflight::SourceFile {
            path: self.path,
            content,
            executable: false,
        })
    }
}
