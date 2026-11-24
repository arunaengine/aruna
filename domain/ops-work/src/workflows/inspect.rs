//! Reads the stored preflight results of a version, computing and storing a missing one on the
//! first read; checks input values; and checks an unsaved workflow without storing anything.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WORKFLOW_VERSION_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, ToolRecord, WorkflowInspection, WorkflowVersionRecord, version_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::GroupId;
use aruna_core::workflow::WorkflowError;
use aruna_workflow_contract::preflight::{Severity, SourceFile, WorkflowSource};
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use super::import::{WIR_ENTRY, catalog_tools, checked_files, lowering_report, support_report};
use super::versions::{GetVersionOperation, VersionView};
use super::{
    CatalogError, CheckReport, GraphView, GroupAccessOperation, ScanOperation, access_answer,
    diagnostic, from_text, preflight_answer, preflight_call, refused, scan_answer, to_text,
};

/// One stored preflight result of a version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Facet {
    Check,
    Schema,
    Graph,
    Tools,
}

/// The value of one facet.
#[derive(Clone, Debug, PartialEq)]
pub enum FacetValue {
    Check(CheckReport),
    Schema(Value),
    Graph(GraphView),
    /// Tool definitions as uwe's tool-definition crate writes them.
    Tools(Vec<Value>),
}

/// The file and language a version is checked and inspected by: a composed version by the wir
/// it lowered to.
fn checked_entry(version: &WorkflowVersionRecord) -> (Option<StoredWorkflowLanguage>, String) {
    let lowered = version.files.iter().any(|path| path == WIR_ENTRY);
    match version.language {
        StoredWorkflowLanguage::Uwe if lowered => {
            (Some(StoredWorkflowLanguage::Wir), WIR_ENTRY.into())
        }
        language => (Some(language), version.entrypoint.clone()),
    }
}

fn cached(
    inspection: &WorkflowInspection,
    facet: Facet,
) -> Result<Option<FacetValue>, CatalogError> {
    Ok(match facet {
        Facet::Check => (inspection.check.as_deref().map(from_text))
            .transpose()?
            .map(FacetValue::Check),
        Facet::Schema => (inspection.schema.as_deref().map(from_text))
            .transpose()?
            .map(FacetValue::Schema),
        Facet::Graph => (inspection.graph.as_deref().map(from_text))
            .transpose()?
            .map(FacetValue::Graph),
        Facet::Tools => (inspection.tools.as_deref().map(from_text))
            .transpose()?
            .map(FacetValue::Tools),
    })
}

#[derive(Debug, PartialEq)]
enum InspectStage {
    Version,
    Preflight(Box<VersionView>),
    Store(Box<FacetValue>),
}

/// Reads one facet of a version; a facet not stored yet is computed by a preflight coordinator
/// and stored for the next read.
#[derive(Debug, PartialEq)]
pub struct InspectOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    version: u32,
    facet: Facet,
    stage: InspectStage,
}

impl InspectOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, version: u32, facet: Facet) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            version,
            facet,
            stage: InspectStage::Version,
        }
    }

    fn request(&self, view: &VersionView) -> Result<PreflightKind, CatalogError> {
        let files = view.files.as_ref().ok_or(IoError::Unfinished)?;
        let source = files.source();
        let (language, entrypoint) = checked_entry(&view.version);
        let language = language.and_then(StoredWorkflowLanguage::contract);
        Ok(match self.facet {
            Facet::Check => PreflightKind::Check {
                source,
                language,
                entrypoint,
            },
            Facet::Schema | Facet::Graph => PreflightKind::Inspect {
                source,
                language,
                entrypoint,
            },
            Facet::Tools => PreflightKind::ExtractTools {
                source,
                language,
                entrypoint: view.version.entrypoint.clone(),
            },
        })
    }

    /// The facet from an answer, with the other values the same answer gives stored too.
    fn found(
        &self,
        view: &VersionView,
        result: PreflightResult,
        inspection: &mut WorkflowInspection,
    ) -> Result<FacetValue, CatalogError> {
        match result {
            PreflightResult::Check {
                runnable,
                report_json,
                ..
            } => {
                let report = support_report(runnable, &report_json);
                inspection.check = Some(to_text(&report)?);
                Ok(FacetValue::Check(report))
            }
            PreflightResult::Inspect { form_json, graph } => {
                let schema = form_json["schema"].clone();
                let complete = view.version.language == StoredWorkflowLanguage::Uwe;
                let graph = GraphView { graph, complete };
                inspection.schema = Some(to_text(&schema)?);
                if inspection.graph.is_none() {
                    inspection.graph = Some(to_text(&graph)?);
                }
                let stored = cached(inspection, self.facet)?;
                Ok(stored.unwrap_or(FacetValue::Schema(schema)))
            }
            PreflightResult::ExtractTools { tools } => {
                inspection.tools = Some(to_text(&tools)?);
                Ok(FacetValue::Tools(tools))
            }
            _ => Err(super::unexpected("a preflight result of the facet")),
        }
    }
}

impl Operation for InspectOperation {
    type Output = FacetValue;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<FacetValue>, CatalogError> {
        let read =
            GetVersionOperation::new(&self.auth, self.workflow_id, self.version).with_files();
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FacetValue>, CatalogError> {
        match std::mem::replace(&mut self.stage, InspectStage::Version) {
            InspectStage::Version => {
                let view = result.child::<VersionView, CatalogError>()??;
                if let Some(value) = cached(&view.version.inspection, self.facet)? {
                    return Ok(Next::Done(value));
                }
                let request = self.request(&view)?;
                self.stage = InspectStage::Preflight(Box::new(view));
                Ok(Next::Call(preflight_call(request)))
            }
            InspectStage::Preflight(view) => {
                let answer = preflight_answer(result)?.map_err(refused)?;
                let mut inspection = view.version.inspection.clone();
                let value = self.found(&view, answer, &mut inspection)?;
                self.stage = InspectStage::Store(Box::new(value));
                Ok(Next::Call(IoCall::child(CacheOperation {
                    workflow_id: self.workflow_id,
                    version: self.version,
                    inspection: Some(inspection),
                })))
            }
            InspectStage::Store(value) => {
                result.child::<(), CatalogError>()??;
                Ok(Next::Done(*value))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Invalid(_))
    }
}

/// Merges computed results into the stored version, keeping results stored meanwhile.
#[derive(Debug, PartialEq)]
struct CacheOperation {
    workflow_id: Ulid,
    version: u32,
    inspection: Option<WorkflowInspection>,
}

impl Operation for CacheOperation {
    type Output = ();
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        let key = version_key(self.workflow_id, self.version);
        Ok(Next::Call(aruna_core::rows::read_row(
            WORKFLOW_VERSION_KEYSPACE,
            key,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        let Some(computed) = self.inspection.take() else {
            result.done()?;
            return Ok(Next::Done(()));
        };
        // The version was deleted meanwhile; nothing is left to remember the results for.
        let Some(value) = result.value()? else {
            return Ok(Next::Done(()));
        };
        let mut version = WorkflowVersionRecord::from_bytes(&value)?;
        let stored = &mut version.inspection;
        stored.check = stored.check.take().or(computed.check);
        stored.schema = stored.schema.take().or(computed.schema);
        stored.graph = stored.graph.take().or(computed.graph);
        stored.tools = stored.tools.take().or(computed.tools);
        let key = version_key(self.workflow_id, self.version);
        let row = (
            WORKFLOW_VERSION_KEYSPACE.to_string(),
            key,
            version.to_bytes()?.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Whether input values fit the form of a version, with uwe's diagnostics per value.
#[derive(Clone, Debug, PartialEq)]
pub struct ValuesReport {
    pub ok: bool,
    pub diagnostics: Vec<Value>,
}

/// Checks input values against the form of a version the caller sees.
#[derive(Debug, PartialEq)]
pub struct ValidateValuesOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    version: u32,
    values: Option<Value>,
}

impl ValidateValuesOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, version: u32, values: Value) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            version,
            values: Some(values),
        }
    }
}

impl Operation for ValidateValuesOperation {
    type Output = ValuesReport;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ValuesReport>, CatalogError> {
        let read =
            GetVersionOperation::new(&self.auth, self.workflow_id, self.version).with_files();
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ValuesReport>, CatalogError> {
        let Some(values) = self.values.take() else {
            let (valid, response) = match preflight_answer(result)?.map_err(refused)? {
                PreflightResult::ValidateValues {
                    valid,
                    response_json,
                } => (valid, response_json),
                _ => return Err(super::unexpected("PreflightResult::ValidateValues")),
            };
            return Ok(Next::Done(values_report(valid, &response)));
        };
        let view = result.child::<VersionView, CatalogError>()??;
        let files = view.files.as_ref().ok_or(IoError::Unfinished)?;
        let (language, entrypoint) = checked_entry(&view.version);
        Ok(Next::Call(preflight_call(PreflightKind::ValidateValues {
            source: files.source(),
            language: language.and_then(StoredWorkflowLanguage::contract),
            entrypoint,
            values,
        })))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Invalid(_))
    }
}

/// uwe's value check as the portal reads it: its diagnostics, or the reason it could not check.
pub fn values_report(valid: bool, response: &Value) -> ValuesReport {
    let diagnostics = match response["diagnostics"].as_array() {
        Some(found) => found.clone(),
        None => {
            let message = (response["message"].as_str())
                .or(response["error"].as_str())
                .unwrap_or("the values could not be checked");
            vec![json!({"message": message, "severity": "error"})]
        }
    };
    ValuesReport {
        ok: valid,
        diagnostics,
    }
}

/// An unsaved workflow: a composed text, or files with their entry.
#[derive(Clone, Debug, PartialEq)]
pub struct Draft {
    pub language: StoredWorkflowLanguage,
    /// The group whose catalog tools a composed text names.
    pub group_id: Option<GroupId>,
    pub uwe_yaml: Option<String>,
    pub files: Vec<SourceFile>,
    pub entrypoint: Option<String>,
}

/// The check of a draft, its graph, and the wir it lowers to when it checks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DraftReport {
    pub check: CheckReport,
    pub graph: Option<GraphView>,
    pub wir: Option<Value>,
}

#[derive(Debug, PartialEq)]
enum DraftStage {
    Access,
    Tools,
    Lower,
    Check(WorkflowSource, String),
    Inspect,
}

/// Checks an unsaved workflow and previews its graph; it stores nothing.
#[derive(Debug, PartialEq)]
pub struct ValidateDraftOperation {
    auth: AuthContext,
    draft: Draft,
    report: DraftReport,
    stage: DraftStage,
}

impl ValidateDraftOperation {
    pub fn new(auth: &AuthContext, draft: Draft) -> Self {
        Self {
            auth: auth.clone(),
            draft,
            report: DraftReport::default(),
            stage: DraftStage::Access,
        }
    }

    fn lower(&mut self, tools: &[ToolRecord]) -> Result<Next<DraftReport>, CatalogError> {
        let (catalog_tools, tool_files) = catalog_tools(tools)?;
        let uwe_yaml = self.draft.uwe_yaml.clone().unwrap_or_default();
        self.stage = DraftStage::Lower;
        Ok(Next::Call(preflight_call(PreflightKind::LowerComposed {
            uwe_yaml,
            catalog_tools,
            tool_files,
        })))
    }

    fn check(&mut self) -> Result<Next<DraftReport>, CatalogError> {
        let entrypoint = (self.draft.entrypoint.clone())
            .ok_or_else(|| CatalogError::Invalid("name the entry file of the files".into()))?;
        let files = checked_files(std::mem::take(&mut self.draft.files), &entrypoint)?;
        let source = WorkflowSource { files };
        self.stage = DraftStage::Check(source.clone(), entrypoint.clone());
        Ok(Next::Call(preflight_call(PreflightKind::Check {
            source,
            language: self.draft.language.contract(),
            entrypoint,
        })))
    }
}

impl Operation for ValidateDraftOperation {
    type Output = DraftReport;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<DraftReport>, CatalogError> {
        let composed = self.draft.language == StoredWorkflowLanguage::Uwe;
        match (composed, self.draft.group_id) {
            (true, None) if self.draft.uwe_yaml.is_none() => Err(CatalogError::Invalid(
                "a composed workflow needs its uwe_yaml text".into(),
            )),
            (true, None) => self.lower(&[]),
            (true, Some(group_id)) => Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                &self.auth,
                [group_id],
            )))),
            (false, _) => self.check(),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DraftReport>, CatalogError> {
        match std::mem::replace(&mut self.stage, DraftStage::Inspect) {
            DraftStage::Access => {
                let group_id = self.draft.group_id.ok_or(IoError::Unfinished)?;
                let access = access_answer(result)?
                    .get(&group_id)
                    .copied()
                    .unwrap_or_default();
                if !access.read {
                    return Err(CatalogError::Forbidden);
                }
                if self.draft.uwe_yaml.is_none() {
                    return Err(CatalogError::Invalid(
                        "a composed workflow needs its uwe_yaml text".into(),
                    ));
                }
                self.stage = DraftStage::Tools;
                let scan = ScanOperation::<ToolRecord>::of_groups(Some([group_id].into()));
                Ok(Next::Call(IoCall::child(scan)))
            }
            DraftStage::Tools => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                self.lower(&tools)
            }
            DraftStage::Lower => {
                let lowering = match preflight_answer(result)?.map_err(refused)? {
                    PreflightResult::LowerComposed(lowering) => lowering,
                    _ => return Err(super::unexpected("PreflightResult::LowerComposed")),
                };
                Ok(Next::Done(DraftReport {
                    check: lowering_report(&lowering),
                    graph: Some(GraphView {
                        graph: lowering.graph,
                        complete: true,
                    }),
                    wir: lowering.wir_json,
                }))
            }
            DraftStage::Check(source, entrypoint) => {
                match preflight_answer(result)? {
                    Ok(PreflightResult::Check {
                        runnable,
                        report_json,
                        ..
                    }) => self.report.check = support_report(runnable, &report_json),
                    Ok(_) => return Err(super::unexpected("PreflightResult::Check")),
                    Err(WorkflowError::Refused(refusal)) => {
                        let found = diagnostic(Severity::Error, refusal.to_string());
                        return Ok(Next::Done(DraftReport {
                            check: CheckReport {
                                ok: false,
                                diagnostics: vec![found],
                            },
                            ..DraftReport::default()
                        }));
                    }
                    Err(error) => return Err(CatalogError::Coordinator(error)),
                }
                self.stage = DraftStage::Inspect;
                Ok(Next::Call(preflight_call(PreflightKind::Inspect {
                    source,
                    language: self.draft.language.contract(),
                    entrypoint,
                })))
            }
            DraftStage::Inspect => {
                match preflight_answer(result)? {
                    Ok(PreflightResult::Inspect { graph, .. }) => {
                        self.report.graph = Some(GraphView {
                            graph,
                            complete: false,
                        });
                    }
                    Ok(_) => return Err(super::unexpected("PreflightResult::Inspect")),
                    Err(error @ WorkflowError::Refused(_)) => {
                        let message = format!("the inputs could not be read: {error}");
                        self.report
                            .check
                            .diagnostics
                            .push(diagnostic(Severity::Warning, message));
                    }
                    Err(error) => return Err(CatalogError::Coordinator(error)),
                }
                Ok(Next::Done(std::mem::take(&mut self.report)))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::Forbidden | CatalogError::Invalid(_))
    }
}
