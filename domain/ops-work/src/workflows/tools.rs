//! The tool catalog of the groups: lists, adds, reads and removes tool definitions, extracts them
//! from workflows and suggests tools whose ports fit a given port.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WORKFLOW_TOOL_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::{
    StoredFile, StoredWorkflowLanguage, ToolCuration, ToolRecord, WorkflowFiles, tool_key,
    tool_version_prefix,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{GroupId, Key};
use aruna_core::workflow::WorkflowError;
use aruna_workflow_contract::preflight::{
    Diagnostic, Severity, SourceFile, SuggestDirection, WorkflowSource,
};
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde_json::Value;
use ulid::Ulid;

use super::import::{GitInput, check_git, checked_files};
use super::versions::{GetVersionOperation, VersionView};
use super::{
    Access, CatalogError, GroupAccessOperation, MAX_SOURCE_BYTES, ScanOperation, access_answer,
    diagnostic, from_text, page_of, preflight_answer, preflight_call, scan_answer, to_text,
};

/// A stored tool with its definition read.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolView {
    pub record: ToolRecord,
    pub definition: Value,
}

impl ToolView {
    fn of(record: ToolRecord) -> Result<Self, CatalogError> {
        let definition = from_text(&record.definition)?;
        Ok(Self { record, definition })
    }

    /// The language of the tool's source, as the portal names it.
    pub fn language(&self) -> Option<&'static str> {
        tool_language(&self.definition)
    }

    /// Whether a port of the tool takes or gives `format`.
    fn has_format(&self, format: &str) -> bool {
        let named = |port: &Value| {
            let terms = port["format"]["terms"].as_array().into_iter().flatten();
            let taken = port["formats"].as_array().into_iter().flatten();
            terms.chain(taken).any(|term| term == format)
        };
        let ports = ["inputs", "outputs"].map(|side| self.definition[side].as_array());
        ports.into_iter().flatten().flatten().any(named)
    }
}

/// The language of a definition's source: a workflow language, `container` or `other`.
pub fn tool_language(definition: &Value) -> Option<&'static str> {
    match definition["source"]["kind"].as_str()? {
        "nf_core" | "nextflow" => Some("nextflow"),
        "snakemake" => Some("snakemake"),
        "cwl" => Some("cwl"),
        "wdl" => Some("wdl"),
        "galaxy" => Some("galaxy"),
        "argo" => Some("argo"),
        "container" => Some("container"),
        _ => Some("other"),
    }
}

/// A definition's id, version and title, which every tool needs.
fn named(definition: &Value) -> Result<(String, String, String), CatalogError> {
    let text = |field: &str| {
        (definition[field].as_str())
            .filter(|text| !text.trim().is_empty())
            .map(str::to_owned)
            .ok_or_else(|| CatalogError::Invalid(format!("a tool definition needs its {field}")))
    };
    Ok((text("id")?, text("version")?, text("title")?))
}

fn curation_of(definition: &Value) -> ToolCuration {
    match definition["curation"].as_str() {
        Some("curated") => ToolCuration::Curated,
        Some("imported") => ToolCuration::Imported,
        _ => ToolCuration::Unchecked,
    }
}

/// A record of a definition in a group, with the files its source names.
fn record_of(
    group_id: GroupId,
    definition: &Value,
    files: Vec<StoredFile>,
    (created_by, now_ms): (aruna_core::user::id::UserId, u64),
) -> Result<ToolRecord, CatalogError> {
    let (tool_id, version, title) = named(definition)?;
    let size: u64 = files.iter().map(|file| file.content.len() as u64).sum();
    if size > MAX_SOURCE_BYTES {
        return Err(CatalogError::Invalid(
            "the files of the tool are too large".into(),
        ));
    }
    Ok(ToolRecord {
        tool_id,
        version,
        group_id,
        title,
        curation: curation_of(definition),
        definition: to_text(definition)?,
        files,
        created_by,
        created_at_ms: now_ms,
    })
}

/// What a tool list keeps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolFilter {
    pub group_id: Option<GroupId>,
    pub language: Option<String>,
    pub format: Option<String>,
    pub curation: Option<ToolCuration>,
    pub text: Option<String>,
}

impl ToolFilter {
    fn keeps(&self, tool: &ToolView) -> bool {
        let words = self.text.as_deref().map(str::to_lowercase);
        let described = |words: &str| {
            let title = tool.record.title.to_lowercase();
            let description = tool.definition["description"].as_str().unwrap_or_default();
            title.contains(words)
                || tool.record.tool_id.to_lowercase().contains(words)
                || description.to_lowercase().contains(words)
        };
        (self.language.as_deref()).is_none_or(|language| tool.language() == Some(language))
            && (self.format.as_deref()).is_none_or(|format| tool.has_format(format))
            && self
                .curation
                .is_none_or(|curation| curation == tool.record.curation)
            && words.as_deref().is_none_or(described)
    }
}

/// One page of tools, the key to continue after, and how many match in all.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolPage {
    pub items: Vec<ToolView>,
    pub next: Option<Key>,
    pub total: usize,
}

/// The tools of the groups the caller may read, filtered, in key order.
#[derive(Debug, PartialEq)]
pub struct ListToolsOperation {
    auth: AuthContext,
    filter: ToolFilter,
    after: Option<Key>,
    limit: usize,
    found: Option<Vec<(Key, ToolView)>>,
}

impl ListToolsOperation {
    pub fn new(auth: &AuthContext, filter: ToolFilter, after: Option<Key>, limit: usize) -> Self {
        Self {
            auth: auth.clone(),
            filter,
            after,
            limit,
            found: None,
        }
    }
}

impl Operation for ListToolsOperation {
    type Output = ToolPage;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ToolPage>, CatalogError> {
        let groups = self.filter.group_id.map(|group| [group].into());
        Ok(Next::Call(IoCall::child(
            ScanOperation::<ToolRecord>::of_groups(groups),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ToolPage>, CatalogError> {
        let Some(found) = self.found.take() else {
            let mut found = Vec::new();
            for (key, record) in scan_answer::<ToolRecord>(result)? {
                let tool = ToolView::of(record)?;
                if self.filter.keeps(&tool) {
                    found.push((key, tool));
                }
            }
            let groups: Vec<GroupId> = found.iter().map(|(_, tool)| tool.record.group_id).collect();
            self.found = Some(found);
            return Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                &self.auth, groups,
            ))));
        };
        let access = access_answer(result)?;
        let readable = (found.into_iter()).filter(|(_, tool)| {
            access
                .get(&tool.record.group_id)
                .is_some_and(|access| access.read)
        });
        let (items, next, total) = page_of(readable.collect(), self.after.as_ref(), self.limit);
        Ok(Next::Done(ToolPage { items, next, total }))
    }
}

/// Stores tool records, replacing a group's earlier copy of the same tool version when asked.
#[derive(Debug, PartialEq)]
struct StoreToolsOperation {
    records: Vec<ToolRecord>,
    replace: bool,
    written: bool,
}

impl Operation for StoreToolsOperation {
    type Output = ();
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        let keys = (self.records.iter())
            .map(|tool| {
                let key = tool_key(&tool.tool_id, &tool.version, tool.group_id);
                (WORKFLOW_TOOL_KEYSPACE.to_string(), key)
            })
            .collect();
        Ok(Next::Call(IoCall::Read(keys)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let stored = result.values()?;
        if !self.replace && stored.iter().any(Option::is_some) {
            return Err(CatalogError::Conflict(
                "the group's catalog holds this tool version already".into(),
            ));
        }
        let mut rows = Vec::with_capacity(self.records.len());
        for tool in &self.records {
            let key = tool_key(&tool.tool_id, &tool.version, tool.group_id);
            rows.push((
                WORKFLOW_TOOL_KEYSPACE.to_string(),
                key,
                tool.to_bytes()?.into(),
            ));
        }
        self.written = true;
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }
}

/// Adds a tool definition to the catalog of a group the caller may write.
#[derive(Debug, PartialEq)]
pub struct CreateToolOperation {
    auth: AuthContext,
    group_id: GroupId,
    definition: Value,
    record: Option<ToolRecord>,
    now_ms: u64,
}

impl CreateToolOperation {
    pub fn new(auth: &AuthContext, group_id: GroupId, definition: Value) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            definition,
            record: None,
            now_ms: 0,
        }
    }
}

impl Operation for CreateToolOperation {
    type Output = ToolView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ToolView>, CatalogError> {
        named(&self.definition)?;
        let check = GroupAccessOperation::new(&self.auth, [self.group_id]);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ToolView>, CatalogError> {
        if let Some(record) = self.record.take() {
            result.child::<(), CatalogError>()??;
            return Ok(Next::Done(ToolView::of(record)?));
        }
        let access: Access = access_answer(result)?
            .get(&self.group_id)
            .copied()
            .unwrap_or_default();
        if !access.write {
            return Err(CatalogError::Forbidden);
        }
        let stamp = (self.auth.user_id, self.now_ms);
        let record = record_of(self.group_id, &self.definition, Vec::new(), stamp)?;
        self.record = Some(record.clone());
        let store = StoreToolsOperation {
            records: vec![record],
            replace: false,
            written: false,
        };
        Ok(Next::Call(IoCall::child(store)))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::Forbidden | CatalogError::Conflict(_) | CatalogError::Invalid(_)
        )
    }
}

/// The groups' copies of one tool version and the caller's access to each group.
#[derive(Debug, PartialEq)]
struct ToolCopiesOperation {
    auth: AuthContext,
    tool_id: String,
    version: String,
    copies: Option<Vec<(Key, ToolRecord)>>,
}

impl Operation for ToolCopiesOperation {
    type Output = Vec<(Key, ToolRecord, Access)>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        let prefix = tool_version_prefix(&self.tool_id, &self.version);
        Ok(Next::Call(IoCall::child(
            ScanOperation::<ToolRecord>::below(prefix),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let Some(copies) = self.copies.take() else {
            let copies = scan_answer::<ToolRecord>(result)?;
            if copies.is_empty() {
                return Err(CatalogError::NotFound);
            }
            let groups: Vec<GroupId> = copies.iter().map(|(_, tool)| tool.group_id).collect();
            self.copies = Some(copies);
            return Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                &self.auth, groups,
            ))));
        };
        let access = access_answer(result)?;
        let seen = (copies.into_iter()).filter_map(|(key, tool)| {
            let granted = access.get(&tool.group_id).copied().unwrap_or_default();
            granted.read.then_some((key, tool, granted))
        });
        Ok(Next::Done(seen.collect()))
    }
}

/// Reads a tool version from the first group in id order whose catalog the caller may read.
#[derive(Debug, PartialEq)]
pub struct GetToolOperation {
    copies: Option<ToolCopiesOperation>,
}

impl GetToolOperation {
    pub fn new(auth: &AuthContext, tool_id: &str, version: &str) -> Self {
        Self {
            copies: Some(ToolCopiesOperation {
                auth: auth.clone(),
                tool_id: tool_id.to_owned(),
                version: version.to_owned(),
                copies: None,
            }),
        }
    }
}

impl Operation for GetToolOperation {
    type Output = ToolView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ToolView>, CatalogError> {
        let copies = self.copies.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(copies)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ToolView>, CatalogError> {
        let copies = result.child::<Vec<(Key, ToolRecord, Access)>, CatalogError>()??;
        let (_, record, _) = copies.into_iter().next().ok_or(CatalogError::NotFound)?;
        Ok(Next::Done(ToolView::of(record)?))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// Removes a tool version from every group catalog the caller may write; one it only reads is
/// forbidden, one it does not see is not found.
#[derive(Debug, PartialEq)]
pub struct DeleteToolOperation {
    copies: Option<ToolCopiesOperation>,
    deleting: bool,
}

impl DeleteToolOperation {
    pub fn new(auth: &AuthContext, tool_id: &str, version: &str) -> Self {
        Self {
            copies: Some(ToolCopiesOperation {
                auth: auth.clone(),
                tool_id: tool_id.to_owned(),
                version: version.to_owned(),
                copies: None,
            }),
            deleting: false,
        }
    }
}

impl Operation for DeleteToolOperation {
    type Output = ();
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        let copies = self.copies.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(copies)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let copies = result.child::<Vec<(Key, ToolRecord, Access)>, CatalogError>()??;
        if copies.is_empty() {
            return Err(CatalogError::NotFound);
        }
        let writable: Vec<_> = (copies.into_iter())
            .filter(|(_, _, access)| access.write)
            .map(|(key, _, _)| (WORKFLOW_TOOL_KEYSPACE.to_string(), key))
            .collect();
        if writable.is_empty() {
            return Err(CatalogError::Forbidden);
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(writable)))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

/// Where tool definitions are extracted from.
#[derive(Clone, Debug, PartialEq)]
pub enum ExtractSource {
    Workflow {
        workflow_id: Ulid,
        version: u32,
    },
    Files {
        files: Vec<SourceFile>,
        entrypoint: String,
        language: StoredWorkflowLanguage,
    },
    Git(GitInput),
}

/// The extracted tools and what the extraction found.
#[derive(Clone, Debug, PartialEq)]
pub struct ExtractReport {
    pub tools: Vec<ToolView>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, PartialEq)]
enum ExtractStage {
    Access,
    Version,
    Extract(WorkflowSource),
    Store(Vec<ToolRecord>),
}

/// Extracts the tool definitions of a workflow; with `save` they join the group's catalog, each
/// with the file its source names, below a folder of its own.
#[derive(Debug, PartialEq)]
pub struct ExtractToolsOperation {
    auth: AuthContext,
    group_id: GroupId,
    source: Option<ExtractSource>,
    save: bool,
    now_ms: u64,
    stage: ExtractStage,
}

impl ExtractToolsOperation {
    pub fn new(auth: &AuthContext, group_id: GroupId, source: ExtractSource, save: bool) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            source: Some(source),
            save,
            now_ms: 0,
            stage: ExtractStage::Access,
        }
    }

    fn extract(
        &mut self,
        source: WorkflowSource,
        entrypoint: String,
        language: StoredWorkflowLanguage,
    ) -> Next<ExtractReport> {
        self.stage = ExtractStage::Extract(source.clone());
        Next::Call(preflight_call(PreflightKind::ExtractTools {
            source,
            language: language.contract(),
            entrypoint,
        }))
    }

    /// The records of the definitions, each source address moved below `<id>/<version>/`.
    fn records(
        &self,
        source: &WorkflowSource,
        tools: &[Value],
    ) -> Result<Vec<ToolRecord>, CatalogError> {
        let mut records = Vec::with_capacity(tools.len());
        for tool in tools {
            let mut definition = tool.clone();
            let (id, version, _) = named(&definition)?;
            let mut files = Vec::new();
            let address = definition["source"]["address"].as_str().map(str::to_owned);
            if let Some(file) = address.as_deref().and_then(|address| source.file(address)) {
                let path = format!("{id}/{version}/{}", file.path);
                definition["source"]["address"] = Value::String(path.clone());
                files.push(StoredFile {
                    path,
                    content: file.content.clone(),
                    executable: file.executable,
                });
            }
            let stamp = (self.auth.user_id, self.now_ms);
            records.push(record_of(self.group_id, &definition, files, stamp)?);
        }
        Ok(records)
    }
}

impl Operation for ExtractToolsOperation {
    type Output = ExtractReport;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<ExtractReport>, CatalogError> {
        if let Some(ExtractSource::Git(git)) = &self.source {
            check_git(git)?;
        }
        let check = GroupAccessOperation::new(&self.auth, [self.group_id]);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ExtractReport>, CatalogError> {
        match std::mem::replace(&mut self.stage, ExtractStage::Version) {
            ExtractStage::Access => {
                let access = access_answer(result)?
                    .get(&self.group_id)
                    .copied()
                    .unwrap_or_default();
                if !access.read || (self.save && !access.write) {
                    return Err(CatalogError::Forbidden);
                }
                match self.source.take().ok_or(IoError::Unfinished)? {
                    ExtractSource::Workflow {
                        workflow_id,
                        version,
                    } => {
                        let read =
                            GetVersionOperation::new(&self.auth, workflow_id, version).with_files();
                        Ok(Next::Call(IoCall::child(read)))
                    }
                    ExtractSource::Files {
                        files,
                        entrypoint,
                        language,
                    } => {
                        let files = checked_files(files, &entrypoint)?;
                        Ok(self.extract(WorkflowSource { files }, entrypoint, language))
                    }
                    ExtractSource::Git(git) => Err(check_git(&git).err().unwrap_or_else(|| {
                        CatalogError::Unsupported("Git sources are not read here".into())
                    })),
                }
            }
            ExtractStage::Version => {
                let view = result.child::<VersionView, CatalogError>()??;
                let files: WorkflowFiles = view.files.ok_or(IoError::Unfinished)?;
                let version = view.version;
                Ok(self.extract(files.source(), version.entrypoint, version.language))
            }
            ExtractStage::Extract(source) => {
                let tools = match preflight_answer(result)? {
                    Ok(PreflightResult::ExtractTools { tools }) => tools,
                    Ok(_) => return Err(super::unexpected("PreflightResult::ExtractTools")),
                    Err(WorkflowError::Refused(refusal)) => {
                        return Ok(Next::Done(ExtractReport {
                            tools: Vec::new(),
                            diagnostics: vec![diagnostic(Severity::Error, refusal.to_string())],
                        }));
                    }
                    Err(error) => return Err(CatalogError::Coordinator(error)),
                };
                let records = self.records(&source, &tools)?;
                if !self.save {
                    let views = records
                        .into_iter()
                        .map(ToolView::of)
                        .collect::<Result<_, _>>()?;
                    return Ok(Next::Done(ExtractReport {
                        tools: views,
                        diagnostics: Vec::new(),
                    }));
                }
                self.stage = ExtractStage::Store(records.clone());
                let store = StoreToolsOperation {
                    records,
                    replace: true,
                    written: false,
                };
                Ok(Next::Call(IoCall::child(store)))
            }
            ExtractStage::Store(records) => {
                result.child::<(), CatalogError>()??;
                let views = records
                    .into_iter()
                    .map(ToolView::of)
                    .collect::<Result<_, _>>()?;
                Ok(Next::Done(ExtractReport {
                    tools: views,
                    diagnostics: Vec::new(),
                }))
            }
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

/// The port a suggestion starts from, in uwe's tool-definition terms.
#[derive(Clone, Debug, PartialEq)]
pub struct PortQuery {
    pub direction: SuggestDirection,
    pub kind: Value,
    pub cardinality: Value,
    pub formats: Vec<String>,
    pub conditions: Vec<Value>,
    pub limit: u32,
}

/// A tool port that fits the query, with how well.
#[derive(Clone, Debug, PartialEq)]
pub struct SuggestionView {
    pub tool: ToolView,
    pub port: String,
    pub fit: String,
    pub scatter: bool,
}

#[derive(Debug, PartialEq)]
enum SuggestStage {
    Scan,
    Access(Vec<ToolRecord>),
    Suggest(BTreeMap<(String, String), ToolRecord>),
}

/// Suggests tools of a group, or of every group the caller reads, that can follow an output or
/// feed an input.
#[derive(Debug, PartialEq)]
pub struct SuggestToolsOperation {
    auth: AuthContext,
    group_id: Option<GroupId>,
    query: PortQuery,
    stage: SuggestStage,
}

impl SuggestToolsOperation {
    pub fn new(auth: &AuthContext, group_id: Option<GroupId>, query: PortQuery) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            query,
            stage: SuggestStage::Scan,
        }
    }
}

impl Operation for SuggestToolsOperation {
    type Output = Vec<SuggestionView>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        let groups = self.group_id.map(|group| [group].into());
        Ok(Next::Call(IoCall::child(
            ScanOperation::<ToolRecord>::of_groups(groups),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        match std::mem::replace(&mut self.stage, SuggestStage::Scan) {
            SuggestStage::Scan => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                let mut groups: Vec<GroupId> = tools.iter().map(|tool| tool.group_id).collect();
                groups.extend(self.group_id);
                self.stage = SuggestStage::Access(tools);
                Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                    &self.auth, groups,
                ))))
            }
            SuggestStage::Access(tools) => {
                let access = access_answer(result)?;
                let reads = |group: &GroupId| access.get(group).is_some_and(|access| access.read);
                if self.group_id.as_ref().is_some_and(|group| !reads(group)) {
                    return Err(CatalogError::Forbidden);
                }
                let mut catalog = BTreeMap::new();
                for tool in tools.into_iter().filter(|tool| reads(&tool.group_id)) {
                    let key = (tool.tool_id.clone(), tool.version.clone());
                    catalog.entry(key).or_insert(tool);
                }
                if catalog.is_empty() {
                    return Ok(Next::Done(Vec::new()));
                }
                let catalog_tools = (catalog.values())
                    .map(|tool| from_text(&tool.definition))
                    .collect::<Result<Vec<Value>, _>>()?;
                let query = self.query.clone();
                self.stage = SuggestStage::Suggest(catalog);
                Ok(Next::Call(preflight_call(PreflightKind::SuggestTools {
                    catalog_tools,
                    direction: query.direction,
                    kind: query.kind,
                    cardinality: query.cardinality,
                    formats: query.formats,
                    conditions: query.conditions,
                    limit: query.limit,
                })))
            }
            SuggestStage::Suggest(catalog) => {
                let suggestions = match preflight_answer(result)?.map_err(super::refused)? {
                    PreflightResult::SuggestTools { suggestions } => suggestions,
                    _ => return Err(super::unexpected("PreflightResult::SuggestTools")),
                };
                let mut found = Vec::with_capacity(suggestions.len());
                for suggestion in suggestions {
                    let text =
                        |field: &str| suggestion[field].as_str().unwrap_or_default().to_owned();
                    let Some(record) = catalog.get(&(text("tool"), text("version"))) else {
                        continue;
                    };
                    let check = &suggestion["check"];
                    found.push(SuggestionView {
                        tool: ToolView::of(record.clone())?,
                        port: text("port"),
                        fit: check["fit"].as_str().unwrap_or("unknown").to_owned(),
                        scatter: check["scatter"].as_bool().unwrap_or(false),
                    });
                }
                Ok(Next::Done(found))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::Forbidden | CatalogError::Invalid(_))
    }
}
