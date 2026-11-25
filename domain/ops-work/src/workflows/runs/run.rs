//! Drives one attempt of a run on its coordinator: prepares and opens the session, routes each
//! frame, answers every host call, applies live events and records how the run ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::{StoredWorkflowLanguage, ToolRecord};
use aruna_core::structs::execution::workflow_run::{
    RunState, WorkflowRunRecord, WorkflowRunSpec, WorkflowTaskRecord,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError};
use aruna_workflow_contract::host::{HostRpcRequest, HostRpcResult};
use aruna_workflow_contract::message::{
    CoordinatorMode, CoordinatorToHost, HostToCoordinator, LiveWorkflowEvent, RunFinishedPayload,
    RunStartPayload, TaskChange, TaskState,
};
use aruna_workflow_contract::preflight::{
    ComposedLowering, RunLogEntry, RunLogPage, SourceFile, WorkflowLanguage, WorkflowSource,
};
use aruna_workflow_contract::settings::ArunaStepSettings;
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::{Map, Value};
use ulid::Ulid;

use super::data::{
    DataCall, ReadDataOperation, Target, WriteBody, WriteObjectOperation, delivery_target, resolve,
    upload_target,
};
use super::log::{AppendLogOperation, LineageLogOperation, LoadLogOperation, pages_answer};
use super::tasks::{RunTaskOperation, StopTasksOperation, StoppedTasks};
use super::{RunChange, change_call, changed, creator_auth};
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::workflows::import::{COMPOSED_ENTRY, WIR_ENTRY, catalog_tools};
use crate::workflows::versions::{GetVersionOperation, VersionView};
use crate::workflows::{
    CatalogError, ScanOperation, preflight_answer, preflight_call, refused, scan_answer, to_text,
};

/// Child submissions one run keeps in flight, below the bound of one demand snapshot.
pub const MAX_PENDING: u32 = 64;

/// The digest of a claim token, which ties a coordinator attempt to its claim without the token.
pub fn claim_digest(claim_token: Ulid) -> String {
    let hash = blake3::hash(&claim_token.to_bytes());
    format!("blake3:{}", hash.to_hex())
}

/// The session name of a run's coordinator attempt on this node.
pub fn session_name(run_id: Ulid) -> String {
    format!("run-{run_id}")
}

pub(crate) fn workflow_call(effect: WorkflowEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Workflow(Box::new(effect))))
}

pub(crate) fn workflow_answer(result: IoResult) -> Result<WorkflowAnswer, CatalogError> {
    match result.event()? {
        Event::Workflow(answer) => Ok(*answer),
        _ => Err(crate::workflows::unexpected("Event::Workflow")),
    }
}

/// How an attempt can go on after its preparation.
#[derive(Clone, Debug, PartialEq)]
pub enum Prepared {
    /// The session is open and the run started; frames follow.
    Ready(Box<WorkflowRunRecord>),
    /// The run is over already, or ended while it was prepared, in this state.
    Ended(RunState),
}

/// What a coordinator needs to run a version, as the run prepares it.
#[derive(Clone, Debug, PartialEq)]
struct Start {
    source: WorkflowSource,
    entrypoint: String,
    language: Option<WorkflowLanguage>,
    step_settings: BTreeMap<String, ArunaStepSettings>,
    images: BTreeMap<String, String>,
}

/// Largest encoded replay-page batch sent in one `RunReplayPages` frame, well inside 8 MiB.
const REPLAY_CHUNK_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Run,
    Begin,
    Version,
    Tools(Box<VersionView>),
    Lower(Box<VersionView>),
    Lineage(Box<Start>),
    Log(Box<(Start, Vec<RunLogPage>)>),
    Open(Box<(Start, Vec<RunLogPage>, Option<String>)>),
    Replay(
        Box<(
            Start,
            std::collections::VecDeque<Vec<RunLogPage>>,
            Option<String>,
        )>,
    ),
    Send,
    Running,
    Failed(String),
}

/// Prepares one attempt: marks the run planning, loads its version, lowers a composed text
/// with the group's tools again, loads the stored run log, opens the session and starts the run.
#[derive(Debug, PartialEq)]
pub struct PrepareRunOperation {
    spec: WorkflowRunSpec,
    node_id: NodeId,
    claim: String,
    record: Option<WorkflowRunRecord>,
    used_tools: Vec<(String, String)>,
    step: PrepareStep,
}

type PrepareNext = Result<Next<Prepared>, CatalogError>;

impl PrepareRunOperation {
    pub fn new(spec: &WorkflowRunSpec, node_id: NodeId, claim: String) -> Self {
        Self {
            spec: spec.clone(),
            node_id,
            claim,
            record: None,
            used_tools: Vec::new(),
            step: PrepareStep::Run,
        }
    }

    fn record(&self) -> Result<&WorkflowRunRecord, CatalogError> {
        self.record
            .as_ref()
            .ok_or_else(|| IoError::Unfinished.into())
    }

    /// Ends the run as failed for a reason of its own workflow, which no attempt changes.
    fn fail(&mut self, message: String) -> PrepareNext {
        let change = RunChange::State {
            state: RunState::Failed,
            error: Some(message.clone()),
        };
        self.step = PrepareStep::Failed(message);
        Ok(Next::Call(change_call(self.spec.run_id, vec![change])))
    }

    fn versioned(&mut self, view: VersionView) -> PrepareNext {
        let version = &view.version;
        if version.language == StoredWorkflowLanguage::Uwe {
            let groups = Some([view.workflow.group_id].into());
            self.step = PrepareStep::Tools(Box::new(view));
            let scan = ScanOperation::<ToolRecord>::of_groups(groups);
            return Ok(Next::Call(IoCall::child(scan)));
        }
        let files = view.files.clone().unwrap_or_default();
        let start = Start {
            source: files.source(),
            entrypoint: version.entrypoint.clone(),
            language: version.language.contract(),
            step_settings: BTreeMap::new(),
            images: version.images.clone(),
        };
        self.load_log(start)
    }

    fn lowered(&mut self, view: VersionView, lowering: ComposedLowering) -> PrepareNext {
        let Some(wir) = lowering.wir_json else {
            let problems: Vec<String> = (lowering.diagnostics.iter())
                .map(|diagnostic| diagnostic.message.clone())
                .collect();
            return self.fail(format!(
                "the composed workflow no longer lowers: {}",
                problems.join("; ")
            ));
        };
        self.used_tools = lowering
            .graph
            .nodes
            .iter()
            .filter_map(|node| {
                (node.kind == aruna_workflow_contract::preflight::NodeKind::Step)
                    .then(|| Some((node.tool.clone()?, node.version.clone()?)))
                    .flatten()
            })
            .collect();
        self.used_tools.sort();
        self.used_tools.dedup();
        let start = Start {
            source: WorkflowSource {
                files: vec![SourceFile {
                    path: WIR_ENTRY.into(),
                    content: to_text(&wir)?.into_bytes(),
                    executable: false,
                }],
            },
            entrypoint: WIR_ENTRY.into(),
            language: Some(WorkflowLanguage::Wir),
            step_settings: lowering.step_settings,
            images: view.version.images.clone(),
        };
        self.load_log(start)
    }

    /// Loads the run log of a resumed lineage first, then the run's own log.
    fn load_log(&mut self, start: Start) -> PrepareNext {
        if let Some(source) = self.record()?.resumes_run_id {
            self.step = PrepareStep::Lineage(Box::new(start));
            let lineage = LineageLogOperation::new(source);
            return Ok(Next::Call(IoCall::child(lineage)));
        }
        self.own_log(start, Vec::new())
    }

    fn own_log(&mut self, start: Start, lineage: Vec<RunLogPage>) -> PrepareNext {
        self.step = PrepareStep::Log(Box::new((start, lineage)));
        Ok(Next::Call(IoCall::child(LoadLogOperation::new(
            self.spec.run_id,
        ))))
    }

    /// The run a coordinator resumes: this run after an earlier attempt, or else the run it
    /// resumes; `None` without any log.
    fn resume_of(&self, lineage: &[RunLogPage], own: &[RunLogPage]) -> Option<String> {
        let record = self.record.as_ref()?;
        match (own.is_empty(), record.resumes_run_id) {
            (false, _) => Some(record.run_id.to_string()),
            (true, Some(source)) if !lineage.is_empty() => Some(source.to_string()),
            _ => None,
        }
    }

    fn payload(
        &self,
        start: Start,
        pages: Vec<RunLogPage>,
        resume: Option<String>,
    ) -> Result<RunStartPayload, CatalogError> {
        let record = self.record()?;
        let inputs = run_inputs(record)?;
        let namespace = super::cache::namespaces(record, record.cache_scope).pop();
        Ok(RunStartPayload {
            source: start.source,
            entrypoint: start.entrypoint,
            language: start.language,
            inputs,
            params: Map::new(),
            pinned_images: start.images,
            cache_namespace: namespace,
            resume_lineage: resume,
            secret_names: record.secrets.keys().cloned().collect(),
            network_isolated: true,
            max_pending: Some(MAX_PENDING),
            prior_log_pages: pages,
            step_settings: start.step_settings,
        })
    }
}

/// Restricts catalog tools to the versions resolved by the source run when resuming a composed
/// workflow so newly imported tool versions cannot invalidate cached step digests.
pub(super) fn filter_tools(
    record: &WorkflowRunRecord,
    mut tools: Vec<ToolRecord>,
) -> Vec<ToolRecord> {
    if record.resumes_run_id.is_some() && !record.used_tools.is_empty() {
        let pinned: std::collections::BTreeSet<&(String, String)> =
            record.used_tools.iter().collect();
        tools.retain(|tool| pinned.contains(&(tool.tool_id.clone(), tool.version.clone())));
    }
    tools
}

/// Splits prior log pages into bounded batches so long lineages never exceed one frame.
fn replay_chunks(pages: &[RunLogPage]) -> Vec<Vec<RunLogPage>> {
    let mut chunks: Vec<Vec<RunLogPage>> = Vec::new();
    let mut current: Vec<RunLogPage> = Vec::new();
    let mut bytes = 0usize;
    for page in pages {
        let size = serde_json::to_vec(page).map_or(0, |v| v.len());
        if !current.is_empty() && bytes + size > REPLAY_CHUNK_BYTES {
            chunks.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes += size;
        current.push(page.clone());
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// The values of a run with its pinned inputs as addresses the coordinator reads through the
/// node, a list where the request pinned several objects for one input.
pub(crate) fn run_inputs(record: &WorkflowRunRecord) -> Result<Value, CatalogError> {
    let mut values: Map<String, Value> = crate::workflows::from_text(&record.values_json)?;
    let Some(inputs) = record.inputs_json.as_deref() else {
        return Ok(Value::Object(values));
    };
    let inputs: Map<String, Value> = crate::workflows::from_text(inputs)?;
    // A pin another node holds names that node, so the run reads it there.
    let address = |pin: &Value| -> Option<Value> {
        let bucket = pin.get("bucket")?.as_str()?;
        let key = pin.get("key")?.as_str()?;
        let version = Ulid::from_string(pin.get("version_id")?.as_str()?).ok()?;
        let node = (pin.get("source_node_id").and_then(Value::as_str))
            .and_then(|node| node.parse::<NodeId>().ok())
            .filter(|node| *node != record.node_id);
        Some(Value::String(match node {
            Some(node) => super::data::address_on(bucket, key, Some(version), node),
            None => super::data::address_of(bucket, key, Some(version)),
        }))
    };
    for (name, pin) in inputs {
        let value = match &pin {
            Value::Array(pins) => Value::Array(pins.iter().filter_map(address).collect()),
            other => address(other).unwrap_or(Value::Null),
        };
        values.insert(name, value);
    }
    Ok(Value::Object(values))
}

impl Operation for PrepareRunOperation {
    type Output = Prepared;
    type Error = CatalogError;

    fn first(&mut self) -> PrepareNext {
        Ok(Next::Call(super::read_run(self.spec.run_id)))
    }

    fn next(&mut self, result: IoResult) -> PrepareNext {
        match std::mem::replace(&mut self.step, PrepareStep::Running) {
            PrepareStep::Run => {
                let Some(record) = super::run_answer(result)? else {
                    return Ok(Next::Done(Prepared::Ended(RunState::Unknown)));
                };
                if record.state.is_terminal() {
                    return Ok(Next::Done(Prepared::Ended(record.state)));
                }
                if record.state == RunState::Cancelling {
                    let change = RunChange::State {
                        state: RunState::Cancelled,
                        error: None,
                    };
                    self.step = PrepareStep::Failed(String::new());
                    return Ok(Next::Call(change_call(self.spec.run_id, vec![change])));
                }
                let changes = vec![
                    RunChange::Attempt {
                        node_id: self.node_id,
                        claim: self.claim.clone(),
                    },
                    RunChange::State {
                        state: RunState::Planning,
                        error: None,
                    },
                ];
                self.step = PrepareStep::Begin;
                Ok(Next::Call(change_call(self.spec.run_id, changes)))
            }
            PrepareStep::Begin => {
                let record = changed(result)?;
                let read = GetVersionOperation::new(
                    &creator_auth(&record),
                    record.workflow_id,
                    record.version,
                )
                .with_files();
                self.record = Some(record);
                self.step = PrepareStep::Version;
                Ok(Next::Call(IoCall::child(read)))
            }
            PrepareStep::Version => match result.child::<VersionView, CatalogError>()? {
                Ok(view) => self.versioned(view),
                Err(CatalogError::NotFound | CatalogError::Forbidden) => {
                    self.fail("the run's creator can no longer read its workflow version".into())
                }
                Err(error) => Err(error),
            },
            PrepareStep::Tools(view) => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                let tools = filter_tools(self.record()?, tools);
                let (catalog_tools, tool_files) = catalog_tools(&tools)?;
                let text = view.version.uwe_yaml.clone().or_else(|| {
                    (view.files.as_ref())
                        .and_then(|files| {
                            files.files.iter().find(|file| file.path == COMPOSED_ENTRY)
                        })
                        .map(|file| String::from_utf8_lossy(&file.content).into_owned())
                });
                let Some(uwe_yaml) = text else {
                    return self.fail("the composed workflow has no .uwe.yaml text".into());
                };
                self.step = PrepareStep::Lower(view);
                Ok(Next::Call(preflight_call(PreflightKind::LowerComposed {
                    uwe_yaml,
                    catalog_tools,
                    tool_files,
                })))
            }
            PrepareStep::Lower(view) => match preflight_answer(result)? {
                Ok(PreflightResult::LowerComposed(lowering)) => self.lowered(*view, lowering),
                Ok(_) => Err(crate::workflows::unexpected(
                    "PreflightResult::LowerComposed",
                )),
                Err(WorkflowError::Refused(refusal)) => self.fail(refusal.to_string()),
                Err(error) => Err(refused(error)),
            },
            PrepareStep::Lineage(start) => {
                let lineage = pages_answer(result)?;
                self.own_log(*start, lineage)
            }
            PrepareStep::Log(parts) => {
                let (start, mut pages) = *parts;
                let own = pages_answer(result)?;
                let resume = self.resume_of(&pages, &own);
                pages.extend(own);
                let session = session_name(self.spec.run_id);
                self.step = PrepareStep::Open(Box::new((start, pages, resume)));
                Ok(Next::Call(workflow_call(WorkflowEffect::Open {
                    session,
                    mode: CoordinatorMode::Run,
                    run_id: self.spec.run_id.to_string(),
                    claim_token_digest: self.claim.clone(),
                })))
            }
            PrepareStep::Open(parts) => {
                match workflow_answer(result)? {
                    WorkflowAnswer::Opened(Ok(())) => {}
                    WorkflowAnswer::Opened(Err(error)) => {
                        return Err(CatalogError::Coordinator(error));
                    }
                    _ => return Err(crate::workflows::unexpected("WorkflowAnswer::Opened")),
                }
                let (start, pages, resume) = *parts;
                let chunks = replay_chunks(&pages);
                if !chunks.is_empty() {
                    let mut queue: std::collections::VecDeque<_> = chunks.into();
                    let first = queue.pop_front().unwrap_or_default();
                    self.step = PrepareStep::Replay(Box::new((start, queue, resume)));
                    return Ok(Next::Call(workflow_call(WorkflowEffect::Send {
                        session: session_name(self.spec.run_id),
                        message: HostToCoordinator::RunReplayPages { pages: first },
                    })));
                }
                let payload = self.payload(start, Vec::new(), resume)?;
                self.step = PrepareStep::Send;
                Ok(Next::Call(workflow_call(WorkflowEffect::Send {
                    session: session_name(self.spec.run_id),
                    message: HostToCoordinator::RunStart(payload),
                })))
            }
            PrepareStep::Replay(parts) => {
                match workflow_answer(result)? {
                    WorkflowAnswer::Sent(Ok(())) => {}
                    WorkflowAnswer::Sent(Err(error)) => {
                        return Err(CatalogError::Coordinator(error));
                    }
                    _ => return Err(crate::workflows::unexpected("WorkflowAnswer::Sent")),
                }
                let (start, mut queue, resume) = *parts;
                if let Some(next_chunk) = queue.pop_front() {
                    self.step = PrepareStep::Replay(Box::new((start, queue, resume)));
                    return Ok(Next::Call(workflow_call(WorkflowEffect::Send {
                        session: session_name(self.spec.run_id),
                        message: HostToCoordinator::RunReplayPages { pages: next_chunk },
                    })));
                }
                let payload = self.payload(start, Vec::new(), resume)?;
                self.step = PrepareStep::Send;
                Ok(Next::Call(workflow_call(WorkflowEffect::Send {
                    session: session_name(self.spec.run_id),
                    message: HostToCoordinator::RunStart(payload),
                })))
            }
            PrepareStep::Send => {
                match workflow_answer(result)? {
                    WorkflowAnswer::Sent(Ok(())) => {}
                    WorkflowAnswer::Sent(Err(error)) => {
                        return Err(CatalogError::Coordinator(error));
                    }
                    _ => return Err(crate::workflows::unexpected("WorkflowAnswer::Sent")),
                }
                let mut changes = Vec::new();
                if !self.used_tools.is_empty() {
                    changes.push(RunChange::Tools(std::mem::take(&mut self.used_tools)));
                }
                changes.push(RunChange::State {
                    state: RunState::Running,
                    error: None,
                });
                self.step = PrepareStep::Running;
                Ok(Next::Call(change_call(self.spec.run_id, changes)))
            }
            PrepareStep::Running => {
                let record = changed(result)?;
                Ok(Next::Done(Prepared::Ready(Box::new(record))))
            }
            PrepareStep::Failed(_) => {
                let record = changed(result)?;
                Ok(Next::Done(Prepared::Ended(record.state)))
            }
        }
    }
}

/// One frame of the coordinator, sorted by what the host does with it.
#[derive(Clone, Debug, PartialEq)]
pub enum Inbound {
    /// A call answered by [`AnswerCallOperation`] beside the other calls.
    Call {
        call_id: u64,
        request: HostRpcRequest,
    },
    /// An upload opens; its chunks follow under the id the host gives.
    UploadStart {
        call_id: u64,
        address: String,
    },
    /// One chunk of an open upload.
    UploadChunk {
        call_id: u64,
        upload_id: String,
        seq: u64,
        bytes: Vec<u8>,
        eof: bool,
    },
    /// A live event, applied in the order it came.
    Event(LiveWorkflowEvent),
    Finished(RunFinishedPayload),
    /// A frame no run session takes, like a second greeting.
    Unexpected(&'static str),
}

/// Sorts one frame of a run session.
pub fn inbound(frame: CoordinatorToHost) -> Inbound {
    match frame {
        CoordinatorToHost::RpcRequest { call_id, request } => match request {
            HostRpcRequest::StorageUploadStart { address, .. } => {
                Inbound::UploadStart { call_id, address }
            }
            HostRpcRequest::StorageUploadChunk {
                upload_id,
                seq,
                bytes,
                eof,
            } => Inbound::UploadChunk {
                call_id,
                upload_id,
                seq,
                bytes,
                eof,
            },
            request => Inbound::Call { call_id, request },
        },
        CoordinatorToHost::LiveEvent { event, .. } => Inbound::Event(event),
        CoordinatorToHost::RunFinished(payload) => Inbound::Finished(payload),
        CoordinatorToHost::HelloAck { .. } => Inbound::Unexpected("a second greeting"),
        CoordinatorToHost::PreflightResponse { .. } => Inbound::Unexpected("a preflight answer"),
    }
}

/// What a call needs of this node besides the run: its routing and its view as an ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct CallNode {
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub routing: NodeRouting,
    pub ingress: IngressNode,
}

/// Outcome of answering one coordinator RPC call, plus any local job IDs to cancel in-process.
#[derive(Clone, Debug, PartialEq)]
pub struct CallOutcome {
    pub result: HostRpcResult,
    pub running: Vec<aruna_core::structs::execution::job::identity::JobId>,
}

#[derive(Debug, PartialEq)]
enum CallStep {
    Child,
    Stop,
    Uploaded(u64),
    Logged(u64),
    Select(String),
    Pin,
    Send(Box<HostRpcResult>),
}

/// Answers one host call of the coordinator and sends the answer: images, tasks, data, deliveries
/// and the run log, whose entry is stored before the acknowledgement leaves.
#[derive(Debug, PartialEq)]
pub struct AnswerCallOperation {
    run: WorkflowRunRecord,
    node: CallNode,
    call_id: u64,
    request: Option<HostRpcRequest>,
    upload: Option<(Target, Vec<u8>, u64)>,
    running: Vec<aruna_core::structs::execution::job::identity::JobId>,
    step: CallStep,
}

type CallNext = Result<Next<CallOutcome>, CatalogError>;

impl AnswerCallOperation {
    pub fn new(
        run: WorkflowRunRecord,
        node: CallNode,
        call_id: u64,
        request: HostRpcRequest,
    ) -> Self {
        Self {
            run,
            node,
            call_id,
            request: Some(request),
            upload: None,
            running: Vec::new(),
            step: CallStep::Child,
        }
    }

    /// Stores the bytes of a finished upload and acknowledges its last chunk.
    pub fn upload(
        run: WorkflowRunRecord,
        node: CallNode,
        call_id: u64,
        (target, bytes, seq): (Target, Vec<u8>, u64),
    ) -> Self {
        Self {
            run,
            node,
            call_id,
            request: None,
            upload: Some((target, bytes, seq)),
            running: Vec::new(),
            step: CallStep::Child,
        }
    }

    fn answer(&mut self, result: HostRpcResult) -> CallNext {
        let message = HostToCoordinator::RpcResponse {
            call_id: self.call_id,
            result: result.clone(),
        };
        self.step = CallStep::Send(Box::new(result));
        Ok(Next::Call(workflow_call(WorkflowEffect::Send {
            session: session_name(self.run.run_id),
            message,
        })))
    }

    fn child<O>(&mut self, operation: O) -> CallNext
    where
        O: Operation<Output = HostRpcResult, Error = CatalogError> + 'static,
    {
        self.step = CallStep::Child;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn start(&mut self, request: HostRpcRequest) -> CallNext {
        let run = self.run.clone();
        let serving = (self.node.node_id, self.node.realm_id);
        match request {
            HostRpcRequest::PinImage { image, .. } => {
                self.step = CallStep::Select(image);
                let select = ComputeEffect::Select {
                    backend: None,
                    constraint: None,
                };
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Compute(
                    Box::new(select),
                )))))
            }
            HostRpcRequest::StartTask(task) => {
                let routing = self.node.routing.clone();
                let task = RunTaskOperation::new(run, self.node.ingress.clone(), task);
                self.child(task.routing(routing))
            }
            HostRpcRequest::CancelTask { name } => {
                self.step = CallStep::Stop;
                let stop = StopTasksOperation::new(run, serving, Some(name));
                Ok(Next::Call(IoCall::child(stop)))
            }
            HostRpcRequest::StopOrphans { .. } => {
                self.step = CallStep::Stop;
                let stop = StopTasksOperation::new(run, serving, None);
                Ok(Next::Call(IoCall::child(stop)))
            }
            HostRpcRequest::StorageStat { address } => self.read(DataCall::Stat { address }),
            HostRpcRequest::StorageList { address } => self.read(DataCall::List { address }),
            HostRpcRequest::StorageDownload { address, offset } => {
                self.read(DataCall::Download { address, offset })
            }
            HostRpcRequest::DeliverOutput {
                source_ref,
                target_path,
            } => {
                let targets = resolve(&run, &source_ref)
                    .and_then(|source| Ok((source, delivery_target(&run, &target_path)?)));
                let (source, target) = match targets {
                    Ok(targets) => targets,
                    Err(error) => return self.answer(HostRpcResult::Failed(error)),
                };
                let write = WriteObjectOperation::new(
                    (run, self.node.node_id),
                    self.node.routing.clone(),
                    target,
                    WriteBody::Copy(source),
                )
                .delivery(target_path);
                self.child(write)
            }
            HostRpcRequest::RunLogBegin { run_start_json } => {
                self.log(RunLogEntry::Begin { run_start_json }, 0)
            }
            HostRpcRequest::RunLogAppend {
                seq, change_json, ..
            } => self.log(RunLogEntry::Change { seq, change_json }, seq),
            HostRpcRequest::StorageUploadStart { .. }
            | HostRpcRequest::StorageUploadChunk { .. } => {
                let refused = ContractError::invalid("uploads are answered on the upload path");
                self.answer(HostRpcResult::Failed(refused))
            }
        }
    }

    fn read(&mut self, call: DataCall) -> CallNext {
        let read = ReadDataOperation::new(self.run.clone(), self.node.node_id, call);
        self.child(read)
    }

    fn log(&mut self, entry: RunLogEntry, seq: u64) -> CallNext {
        self.step = CallStep::Logged(seq);
        let append = AppendLogOperation::new(self.run.run_id, entry);
        Ok(Next::Call(IoCall::retry(
            append,
            &self.run.run_id.to_bytes(),
        )))
    }
}

/// A failed call as the coordinator reads it; only a fault of the node may pass on a retry.
pub fn call_failure(error: CatalogError) -> ContractError {
    match error {
        CatalogError::NotFound => ContractError::NotFound {
            message: "not found".into(),
        },
        CatalogError::Forbidden => ContractError::Denied {
            message: "the run's creator may not do this".into(),
        },
        CatalogError::Invalid(message) | CatalogError::Conflict(message) => {
            ContractError::invalid(message)
        }
        CatalogError::Unsupported(message) => ContractError::unsupported(message),
        other => ContractError::internal(other.to_string()),
    }
}

fn compute_answer(result: IoResult) -> Result<ComputeAnswer, CatalogError> {
    match result.event()? {
        Event::Compute(answer) => Ok(*answer),
        _ => Err(crate::workflows::unexpected("Event::Compute")),
    }
}

impl Operation for AnswerCallOperation {
    type Output = CallOutcome;
    type Error = CatalogError;

    fn first(&mut self) -> CallNext {
        if let Some((target, bytes, seq)) = self.upload.take() {
            let write = WriteObjectOperation::new(
                (self.run.clone(), self.node.node_id),
                self.node.routing.clone(),
                target,
                WriteBody::Bytes(bytes),
            );
            self.step = CallStep::Uploaded(seq);
            return Ok(Next::Call(IoCall::child(write)));
        }
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        self.start(request)
    }

    fn next(&mut self, result: IoResult) -> CallNext {
        match std::mem::replace(&mut self.step, CallStep::Child) {
            CallStep::Child => {
                let answer = result.child::<HostRpcResult, CatalogError>()?;
                self.answer(
                    answer.unwrap_or_else(|error| HostRpcResult::Failed(call_failure(error))),
                )
            }
            CallStep::Stop => match result.child::<StoppedTasks, CatalogError>()? {
                Ok(stopped) => {
                    self.running = stopped.running;
                    self.answer(stopped.result)
                }
                Err(error) => self.answer(HostRpcResult::Failed(call_failure(error))),
            },
            CallStep::Uploaded(seq) => {
                let answer = match result.child::<HostRpcResult, CatalogError>()? {
                    Ok(HostRpcResult::UploadAck { receipt, .. }) => {
                        HostRpcResult::UploadAck { seq, receipt }
                    }
                    Ok(other) => other,
                    Err(error) => HostRpcResult::Failed(call_failure(error)),
                };
                self.answer(answer)
            }
            CallStep::Logged(seq) => {
                let answer = match result.child::<(), CatalogError>()? {
                    Ok(()) => HostRpcResult::RunLogAck { seq },
                    Err(error) => HostRpcResult::Failed(call_failure(error)),
                };
                self.answer(answer)
            }
            CallStep::Select(image) => match compute_answer(result)? {
                ComputeAnswer::Selected(Some(facts)) => {
                    self.step = CallStep::Pin;
                    let resolve = ComputeEffect::ResolveImage {
                        backend: facts.name,
                        image,
                    };
                    Ok(Next::Call(IoCall::Effect(Box::new(Effect::Compute(
                        Box::new(resolve),
                    )))))
                }
                _ => self.answer(HostRpcResult::PinImage { pinned: None }),
            },
            CallStep::Pin => {
                let pinned = match compute_answer(result)? {
                    ComputeAnswer::Image(Ok(pinned)) => Some(pinned),
                    _ => None,
                };
                self.answer(HostRpcResult::PinImage { pinned })
            }
            CallStep::Send(answer) => match workflow_answer(result)? {
                WorkflowAnswer::Sent(Ok(())) => Ok(Next::Done(CallOutcome {
                    result: *answer,
                    running: std::mem::take(&mut self.running),
                })),
                WorkflowAnswer::Sent(Err(error)) => Err(CatalogError::Coordinator(error)),
                _ => Err(crate::workflows::unexpected("WorkflowAnswer::Sent")),
            },
        }
    }
}

/// Checks where an upload goes before its chunks arrive; the answer names the upload.
#[derive(Debug, PartialEq)]
pub struct OpenUploadOperation {
    run: WorkflowRunRecord,
    call_id: u64,
    address: String,
    target: Option<Target>,
    sent: bool,
}

impl OpenUploadOperation {
    pub fn new(run: WorkflowRunRecord, call_id: u64, address: String) -> Self {
        Self {
            run,
            call_id,
            address,
            target: None,
            sent: false,
        }
    }
}

/// The upload id the host answers for the upload opened by call `call_id`.
pub fn upload_id(call_id: u64) -> String {
    format!("upload-{call_id}")
}

impl Operation for OpenUploadOperation {
    /// The target of an accepted upload; `None` when the call was refused.
    type Output = Option<Target>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Option<Target>>, CatalogError> {
        let result = match upload_target(&self.run, &self.address) {
            Ok(target) => {
                self.target = Some(target);
                HostRpcResult::UploadStarted {
                    upload_id: upload_id(self.call_id),
                }
            }
            Err(error) => HostRpcResult::Failed(error),
        };
        Ok(Next::Call(workflow_call(WorkflowEffect::Send {
            session: session_name(self.run.run_id),
            message: HostToCoordinator::RpcResponse {
                call_id: self.call_id,
                result,
            },
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Target>>, CatalogError> {
        if std::mem::replace(&mut self.sent, true) {
            return Err(IoError::Unfinished.into());
        }
        match workflow_answer(result)? {
            WorkflowAnswer::Sent(Ok(())) => Ok(Next::Done(self.target.take())),
            WorkflowAnswer::Sent(Err(error)) => Err(CatalogError::Coordinator(error)),
            _ => Err(crate::workflows::unexpected("WorkflowAnswer::Sent")),
        }
    }
}

/// The state a task change of uwe stands for.
fn task_state(state: TaskState) -> RunState {
    match state {
        TaskState::Requested | TaskState::Retried => RunState::Queued,
        TaskState::Started => RunState::Running,
        TaskState::Finished | TaskState::Reused => RunState::Succeeded,
        TaskState::Failed => RunState::Failed,
        TaskState::Stopped => RunState::Cancelled,
    }
}

fn millis(time_ms: i64) -> Option<u64> {
    u64::try_from(time_ms).ok()
}

/// The task record a task change of uwe gives; the stored attempt keeps its child and files.
pub fn changed_task(run_id: Ulid, change: &TaskChange) -> WorkflowTaskRecord {
    let state = task_state(change.state);
    WorkflowTaskRecord {
        run_id,
        task_id: change.task_id,
        task_key: String::new(),
        step: change.step.clone(),
        name: change.name.clone(),
        state,
        attempt: change.attempt.unwrap_or(1),
        index: None,
        child_job_id: None,
        node_id: None,
        mode: None,
        cached: change.cached || change.state == TaskState::Reused,
        cache_key: None,
        deterministic: None,
        staged_bytes: None,
        cpu_seconds: None,
        max_rss_bytes: None,
        started_at_ms: change.started_ms.and_then(millis),
        finished_at_ms: change.ended_ms.and_then(millis),
        exit_code: change.exit_code,
        error: change.error.clone(),
        inputs: Vec::new(),
        outputs: Vec::new(),
        stdout_tail: Vec::new(),
        stderr_tail: Vec::new(),
    }
}

/// The changes a live event makes to the run; events without a record of their own make none.
pub fn event_changes(run_id: Ulid, event: &LiveWorkflowEvent) -> Vec<RunChange> {
    match event {
        LiveWorkflowEvent::RunStarted { .. } => vec![RunChange::State {
            state: RunState::Running,
            error: None,
        }],
        LiveWorkflowEvent::TaskChanged(change) => {
            vec![RunChange::Task(Box::new(changed_task(run_id, change)))]
        }
        LiveWorkflowEvent::TaskLog { .. }
        | LiveWorkflowEvent::TaskUsage { .. }
        | LiveWorkflowEvent::TaskMarked { .. }
        | LiveWorkflowEvent::Progress(_)
        | LiveWorkflowEvent::Printed { .. }
        | LiveWorkflowEvent::RunEnded { .. } => Vec::new(),
    }
}

/// The state a finished run ends in.
pub fn finished_state(finished: &RunFinishedPayload) -> RunState {
    match (finished.stopped, finished.success) {
        (true, _) => RunState::Cancelled,
        (false, true) => RunState::Succeeded,
        (false, false) => RunState::Failed,
    }
}

#[derive(Debug, PartialEq)]
enum FinishStep {
    Record,
    Close(Box<WorkflowRunRecord>),
}

/// Records how a run ended and closes its coordinator session.
#[derive(Debug, PartialEq)]
pub struct FinishRunOperation {
    run_id: Ulid,
    state: RunState,
    error: Option<String>,
    step: FinishStep,
}

impl FinishRunOperation {
    pub fn new(run_id: Ulid, state: RunState, error: Option<String>) -> Self {
        Self {
            run_id,
            state,
            error,
            step: FinishStep::Record,
        }
    }

    /// The end the coordinator reported.
    pub fn reported(run_id: Ulid, finished: &RunFinishedPayload) -> Self {
        let error = match finished_state(finished) {
            RunState::Succeeded => None,
            _ => finished.error.clone(),
        };
        Self::new(run_id, finished_state(finished), error)
    }
}

impl Operation for FinishRunOperation {
    type Output = WorkflowRunRecord;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let change = RunChange::State {
            state: self.state,
            error: self.error.clone(),
        };
        Ok(Next::Call(change_call(self.run_id, vec![change])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        match std::mem::replace(&mut self.step, FinishStep::Record) {
            FinishStep::Record => {
                let record = changed(result)?;
                self.step = FinishStep::Close(Box::new(record));
                Ok(Next::Call(workflow_call(WorkflowEffect::Close {
                    session: session_name(self.run_id),
                })))
            }
            // A session that is gone already is closed as far as this run is concerned.
            FinishStep::Close(record) => match workflow_answer(result)? {
                WorkflowAnswer::Closed(_) => Ok(Next::Done(*record)),
                _ => Err(crate::workflows::unexpected("WorkflowAnswer::Closed")),
            },
        }
    }
}

/// Applies the changes of one live event, in the order the events came.
#[derive(Debug, PartialEq)]
pub struct ApplyEventOperation {
    run_id: Ulid,
    changes: Vec<RunChange>,
}

impl ApplyEventOperation {
    pub fn new(run_id: Ulid, event: &LiveWorkflowEvent) -> Self {
        Self {
            run_id,
            changes: event_changes(run_id, event),
        }
    }
}

impl Operation for ApplyEventOperation {
    /// The run after the event; `None` when the event changes nothing stored.
    type Output = Option<WorkflowRunRecord>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        if self.changes.is_empty() {
            return Ok(Next::Done(None));
        }
        let changes = std::mem::take(&mut self.changes);
        Ok(Next::Call(change_call(self.run_id, changes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        Ok(Next::Done(Some(changed(result)?)))
    }
}

/// One message to, or the next frame from, a run's coordinator session.
#[derive(Debug, PartialEq)]
pub struct SessionOperation {
    effect: Option<WorkflowEffect>,
}

impl SessionOperation {
    /// Waits for the next frame of the run's session.
    pub fn receive(run_id: Ulid) -> Self {
        Self {
            effect: Some(WorkflowEffect::Receive {
                session: session_name(run_id),
            }),
        }
    }

    /// Sends one message to the run's session.
    pub fn send(run_id: Ulid, message: HostToCoordinator) -> Self {
        Self {
            effect: Some(WorkflowEffect::Send {
                session: session_name(run_id),
                message,
            }),
        }
    }

    /// Asks the coordinator to stop the run's tasks and end it as stopped.
    pub fn cancel(run_id: Ulid, reason: impl Into<String>) -> Self {
        Self::send(
            run_id,
            HostToCoordinator::Cancel {
                reason: reason.into(),
            },
        )
    }

    /// Ends the session and its coordinator attempt.
    pub fn close(run_id: Ulid) -> Self {
        Self {
            effect: Some(WorkflowEffect::Close {
                session: session_name(run_id),
            }),
        }
    }
}

impl Operation for SessionOperation {
    /// The frame a receive got; `None` for every other call and for a channel that ended.
    type Output = Option<CoordinatorToHost>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        let effect = self.effect.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(workflow_call(effect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let answer = match workflow_answer(result)? {
            WorkflowAnswer::Received(frame) => frame,
            WorkflowAnswer::Sent(sent)
            | WorkflowAnswer::Closed(sent)
            | WorkflowAnswer::Opened(sent) => sent.map(|()| None),
            WorkflowAnswer::Preflight(_) => {
                return Err(crate::workflows::unexpected("a session answer"));
            }
        };
        answer.map(Next::Done).map_err(CatalogError::Coordinator)
    }
}

/// The most bytes the host holds for one upload until its last chunk; larger files belong in a
/// bucket the task reads directly.
pub const MAX_UPLOAD_BYTES: usize = 256 << 20;

/// The uploads of one session whose chunks are still arriving.
#[derive(Debug, Default)]
pub struct Uploads {
    open: BTreeMap<String, (Target, Vec<u8>)>,
}

/// What the host does with one upload chunk.
#[derive(Debug, PartialEq)]
pub enum Chunk {
    /// Send this answer now.
    Answer(HostRpcResult),
    /// The upload is complete: store its bytes, then acknowledge chunk `seq`.
    Store {
        target: Target,
        bytes: Vec<u8>,
        seq: u64,
    },
}

impl Uploads {
    /// Starts collecting the chunks of the upload opened by call `call_id`.
    pub fn open(&mut self, call_id: u64, target: Target) {
        self.open.insert(upload_id(call_id), (target, Vec::new()));
    }

    pub fn chunk(&mut self, upload_id: &str, seq: u64, bytes: Vec<u8>, eof: bool) -> Chunk {
        let Some((_, buffer)) = self.open.get_mut(upload_id) else {
            let refused = ContractError::invalid(format!("no open upload {upload_id}"));
            return Chunk::Answer(HostRpcResult::Failed(refused));
        };
        if buffer.len() + bytes.len() > MAX_UPLOAD_BYTES {
            self.open.remove(upload_id);
            let refused = ContractError::unsupported(format!(
                "an upload may hold at most {MAX_UPLOAD_BYTES} bytes"
            ));
            return Chunk::Answer(HostRpcResult::Failed(refused));
        }
        buffer.extend_from_slice(&bytes);
        if !eof {
            return Chunk::Answer(HostRpcResult::UploadAck { seq, receipt: None });
        }
        match self.open.remove(upload_id) {
            Some((target, bytes)) => Chunk::Store { target, bytes, seq },
            None => Chunk::Answer(HostRpcResult::Failed(ContractError::internal(
                "lost upload",
            ))),
        }
    }
}
