//! Copies a run and its log pages onto group record holders so an owner or realm admin can
//! resume an unreachable run on a surviving node; nothing takes over automatically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    RUN_LOG_KEYSPACE, WORKFLOW_KEYSPACE, WORKFLOW_RUN_KEYSPACE, WORKFLOW_SOURCE_KEYSPACE,
    WORKFLOW_TOOL_KEYSPACE, WORKFLOW_VERSION_KEYSPACE,
};
use aruna_core::metadata::protocol::{
    METADATA_IO_TIMEOUT, MetadataReadError, MetadataTransportMessage, encode_message, peer_call,
    peer_reply,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::plan_target_placement;
use aruna_core::placement::resolver::PlacementResolutionContext;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, ToolRecord, WorkflowRecord, WorkflowVersionRecord, tool_key,
    version_key, workflow_key,
};
use aruna_core::structs::execution::workflow_run::{
    LogHead, ReplicatedWorkflow, WorkflowRunRecord, decode_page, encode_page, head_key, page_key,
    run_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::log::{LoadLogOperation, pages_answer};
use super::{read_run, run_answer, run_row};
use crate::workflows::{CatalogError, ScanOperation, scan_answer};

/// Encoded page bytes one store request carries at most, well inside one metadata frame.
const BATCH_BYTES: usize = 8 << 20;

/// What a replica of an active run shows on a node that is not its coordinator.
pub const UNREACHABLE: &str = "coordinator node unreachable";

/// The holders of `group_id`'s records other than `local`, in placement order.
pub fn record_holders(
    config: &RealmConfigDocument,
    group_id: GroupId,
    local: NodeId,
) -> Vec<NodeId> {
    let target = DocumentTarget::Group { group_id };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: None,
    };
    let plan = plan_target_placement(config, &target, context)
        .ok()
        .flatten();
    let holders = plan.map(|plan| plan.holders).unwrap_or_default();
    holders.into_iter().filter(|node| *node != local).collect()
}

/// The note a run gets on `local` when another node coordinates it and it has not ended: this
/// node holds only a copy, which its owner or a realm admin may resume here.
pub fn replica_note(record: &WorkflowRunRecord, local: NodeId) -> Option<&'static str> {
    (record.node_id != local && record.state.is_active()).then_some(UNREACHABLE)
}

#[derive(Debug, PartialEq)]
enum SendStep {
    Run,
    Config,
    Workflow(Vec<NodeId>, bool),
    Tools(Vec<NodeId>, ReplicatedWorkflow, bool),
    Log(Vec<NodeId>),
    Send(NodeId),
}

/// Sends the run and its log pages from `from_page` on to every holder of the run group's
/// records. A replacement holder that lacks earlier pages or workflow metadata receives a full
/// backfill from page 0. Answers the page the next pass starts from.
#[derive(Debug, PartialEq)]
pub struct ReplicateLogOperation {
    run_id: Ulid,
    local: (NodeId, RealmId),
    from_page: u32,
    run: Option<WorkflowRunRecord>,
    log_pages: Vec<RunLogPage>,
    workflow: Option<ReplicatedWorkflow>,
    sends: Vec<(NodeId, Vec<u8>)>,
    backfill: Vec<NodeId>,
    backfilled: BTreeSet<NodeId>,
    /// A holder missed a frame, so the next pass sends from the same page again.
    missed: bool,
    pages: u32,
    step: SendStep,
}

type SendNext = Result<Next<u32>, CatalogError>;

impl ReplicateLogOperation {
    pub fn new(run_id: Ulid, local: (NodeId, RealmId), from_page: u32) -> Self {
        Self {
            run_id,
            local,
            from_page,
            run: None,
            log_pages: Vec::new(),
            workflow: None,
            sends: Vec::new(),
            backfill: Vec::new(),
            backfilled: BTreeSet::new(),
            missed: false,
            pages: 0,
            step: SendStep::Run,
        }
    }

    /// The store requests of the pages from `from_page` on, each inside the batch bound.
    #[cfg(test)]
    fn frames(
        &self,
        run: &WorkflowRunRecord,
        pages: &[RunLogPage],
        workflow: Option<ReplicatedWorkflow>,
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        self.frames_from(self.from_page, run, pages, workflow)
    }

    fn frames_from(
        &self,
        start_page: u32,
        run: &WorkflowRunRecord,
        pages: &[RunLogPage],
        mut workflow: Option<ReplicatedWorkflow>,
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        let mut batches: Vec<Vec<Vec<u8>>> = vec![Vec::new()];
        let mut bytes = 0;
        for page in pages.iter().skip(start_page as usize) {
            let encoded = encode_page(page)?;
            let full = batches.last().is_some_and(|batch| !batch.is_empty());
            if full && bytes + encoded.len() > BATCH_BYTES {
                batches.push(Vec::new());
                bytes = 0;
            }
            bytes += encoded.len();
            if let Some(batch) = batches.last_mut() {
                batch.push(encoded);
            }
        }
        let mut first = start_page;
        let mut frames = Vec::with_capacity(batches.len());
        for batch in batches {
            let count = batch.len() as u32;
            let message = MetadataTransportMessage::StoreRunLog {
                run: Box::new(run.clone()),
                first_page: first,
                pages: batch,
                workflow: workflow.take(),
            };
            let frame = encode_message(&message);
            frames.push(frame.map_err(|error| CatalogError::Storage(error.to_string()))?);
            first += count;
        }
        Ok(frames)
    }

    fn read_workflow(&mut self, holders: Vec<NodeId>, load_log: bool) -> SendNext {
        let Some(run) = &self.run else {
            return Err(CatalogError::NotFound);
        };
        let wf_key = workflow_key(run.workflow_id);
        let ver_key = version_key(run.workflow_id, run.version);
        self.step = SendStep::Workflow(holders, load_log);
        Ok(Next::Call(IoCall::Read(vec![
            (WORKFLOW_KEYSPACE.to_string(), wf_key),
            (WORKFLOW_VERSION_KEYSPACE.to_string(), ver_key.clone()),
            (WORKFLOW_SOURCE_KEYSPACE.to_string(), ver_key),
        ])))
    }

    fn enqueue(
        &mut self,
        start_page: u32,
        holders: Vec<NodeId>,
        include_workflow: bool,
    ) -> SendNext {
        let Some(run) = self.run.clone() else {
            return Err(CatalogError::NotFound);
        };
        let workflow = match include_workflow {
            true => self.workflow.clone(),
            false => None,
        };
        let frames = self.frames_from(start_page, &run, &self.log_pages, workflow)?;
        for node in holders.into_iter().rev() {
            for frame in frames.iter().rev() {
                self.sends.push((node, frame.clone()));
            }
        }
        self.send_next()
    }

    fn send_next(&mut self) -> SendNext {
        if let Some((node, frame)) = self.sends.pop() {
            self.step = SendStep::Send(node);
            return Ok(Next::Call(peer_call(node, frame, METADATA_IO_TIMEOUT * 2)));
        }
        if !self.backfill.is_empty() {
            let peers = std::mem::take(&mut self.backfill);
            if self.workflow.is_some() {
                return self.enqueue(0, peers, true);
            }
            return self.read_workflow(peers, false);
        }
        // Keep the cursor on the last transmitted page because that page remains mutable.
        let next = match self.missed {
            true => self.from_page,
            false => self.pages.saturating_sub(1),
        };
        Ok(Next::Done(next))
    }
}

impl Operation for ReplicateLogOperation {
    type Output = u32;
    type Error = CatalogError;

    fn first(&mut self) -> SendNext {
        Ok(Next::Call(read_run(self.run_id)))
    }

    fn next(&mut self, result: IoResult) -> SendNext {
        match std::mem::replace(&mut self.step, SendStep::Run) {
            SendStep::Run => {
                let run = run_answer(result)?.ok_or(CatalogError::NotFound)?;
                // Only the run's own realm node hands out copies; copies and local device runs stay.
                if run.node_id != self.local.0 || run.local_only {
                    return Ok(Next::Done(self.from_page));
                }
                self.run = Some(run);
                self.step = SendStep::Config;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.local.1,
                ))))
            }
            SendStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| CatalogError::Storage(error.to_string()))?;
                let Some(run) = &self.run else {
                    return Err(CatalogError::NotFound);
                };
                let holders = record_holders(&config, run.group_id, self.local.0);
                if holders.is_empty() {
                    return Ok(Next::Done(self.from_page));
                }
                if self.from_page == 0 {
                    return self.read_workflow(holders, true);
                }
                self.step = SendStep::Log(holders);
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.run_id,
                ))))
            }
            SendStep::Workflow(holders, load_log) => {
                let mut values = result.values()?.into_iter();
                let workflow = match (values.next().flatten(), values.next().flatten()) {
                    (Some(wf), Some(ver)) => Some(ReplicatedWorkflow {
                        workflow: wf.to_vec(),
                        version: ver.to_vec(),
                        source: values.next().flatten().map(|s| s.to_vec()),
                        tools: Vec::new(),
                    }),
                    _ => None,
                };
                let needs_tools = workflow.as_ref().is_some_and(|bundle| {
                    WorkflowVersionRecord::from_bytes(&bundle.version)
                        .is_ok_and(|ver| ver.language == StoredWorkflowLanguage::Uwe)
                });
                if needs_tools
                    && let Some(run) = &self.run
                    && let Some(bundle) = workflow
                {
                    let groups = Some([run.group_id].into());
                    self.step = SendStep::Tools(holders, bundle, load_log);
                    return Ok(Next::Call(IoCall::child(
                        ScanOperation::<ToolRecord>::of_groups(groups),
                    )));
                }
                self.workflow = workflow;
                if load_log {
                    self.step = SendStep::Log(holders);
                    return Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                        self.run_id,
                    ))));
                }
                self.enqueue(0, holders, true)
            }
            SendStep::Tools(holders, mut bundle, load_log) => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                for tool in tools {
                    bundle.tools.push(tool.to_bytes()?);
                }
                self.workflow = Some(bundle);
                if load_log {
                    self.step = SendStep::Log(holders);
                    return Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                        self.run_id,
                    ))));
                }
                self.enqueue(0, holders, true)
            }
            SendStep::Log(holders) => {
                let pages = pages_answer(result)?;
                self.pages = pages.len() as u32;
                self.from_page = self.from_page.min(self.pages.saturating_sub(1));
                self.log_pages = pages;
                self.enqueue(self.from_page, holders, self.from_page == 0)
            }
            SendStep::Send(peer) => {
                match peer_reply(peer, result)? {
                    Ok(MetadataTransportMessage::RunLogStored { result: Ok(_) }) => {}
                    Ok(MetadataTransportMessage::RunLogStored {
                        result: Err(MetadataReadError::NotFound),
                    }) if self.from_page > 0 && self.backfilled.insert(peer) => {
                        // A replacement holder lacks earlier pages or workflow catalog records;
                        // drop its tail frames and send a complete copy from page 0.
                        self.sends.retain(|(node, _)| *node != peer);
                        self.backfill.push(peer);
                    }
                    other => {
                        warn!(run = %self.run_id, %peer, ?other, "a run log copy was not stored");
                        // The other frames for this holder would land after a gap; skip them.
                        self.sends.retain(|(node, _)| *node != peer);
                        self.missed = true;
                    }
                }
                self.send_next()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Config,
    Write,
}

/// Keeps the copy of a run and its log pages that the run's node `peer` sent. It refuses a
/// sender that is not the run's node or not in the realm, a node that holds none of the group's
/// records, and a run this node coordinates itself.
#[derive(Debug, PartialEq)]
pub struct StoreReplicaOperation {
    peer: NodeId,
    local: (NodeId, RealmId),
    run: Box<WorkflowRunRecord>,
    first_page: u32,
    pages: Vec<Vec<u8>>,
    workflow: Option<ReplicatedWorkflow>,
    step: StoreStep,
}

impl StoreReplicaOperation {
    pub fn new(
        peer: NodeId,
        local: (NodeId, RealmId),
        run: Box<WorkflowRunRecord>,
        first_page: u32,
        pages: Vec<Vec<u8>>,
        workflow: Option<ReplicatedWorkflow>,
    ) -> Self {
        Self {
            peer,
            local,
            run,
            first_page,
            pages,
            workflow,
            step: StoreStep::Config,
        }
    }
}

impl Operation for StoreReplicaOperation {
    type Output = u32;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<u32>, MetadataReadError> {
        if self.run.node_id != self.peer || self.run.realm_id != self.local.1 || self.run.local_only
        {
            return Err(MetadataReadError::Forbidden);
        }
        let read = GetConfigOperation::new(self.local.1);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u32>, MetadataReadError> {
        match std::mem::replace(&mut self.step, StoreStep::Write) {
            StoreStep::Config => {
                let config = (result.child::<RealmConfigDocument, GetConfigError>())
                    .map_err(|_| MetadataReadError::Unavailable)?
                    .map_err(|_| MetadataReadError::Unavailable)?;
                let member = config
                    .node_kind(&self.peer)
                    .is_some_and(|kind| kind.is_sync_eligible());
                let holders = record_holders(&config, self.run.group_id, self.peer);
                if !member || !holders.contains(&self.local.0) {
                    return Err(MetadataReadError::Forbidden);
                }
                let write = WriteReplicaOperation {
                    local: self.local.0,
                    run: self.run.clone(),
                    first_page: self.first_page,
                    pages: std::mem::take(&mut self.pages),
                    workflow: self.workflow.take(),
                    reading: true,
                    kept: 0,
                };
                Ok(Next::Call(IoCall::retry(
                    write,
                    &self.run.run_id.to_bytes(),
                )))
            }
            StoreStep::Write => {
                let kept = result
                    .child::<u32, CatalogError>()
                    .map_err(|_| MetadataReadError::Unavailable)?;
                kept.map_err(|error| match error {
                    CatalogError::Forbidden => MetadataReadError::Forbidden,
                    CatalogError::NotFound => MetadataReadError::NotFound,
                    _ => MetadataReadError::Unavailable,
                })
                .map(Next::Done)
            }
        }
    }

    fn expected_error(error: &MetadataReadError) -> bool {
        matches!(error, MetadataReadError::Forbidden)
    }
}

/// Writes a run copy with its pages and the log head they make, in one transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteReplicaOperation {
    local: NodeId,
    run: Box<WorkflowRunRecord>,
    first_page: u32,
    pages: Vec<Vec<u8>>,
    workflow: Option<ReplicatedWorkflow>,
    reading: bool,
    kept: u32,
}

impl Operation for WriteReplicaOperation {
    type Output = u32;
    type Error = CatalogError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<u32>, CatalogError> {
        let wf_key = workflow_key(self.run.workflow_id);
        let ver_key = version_key(self.run.workflow_id, self.run.version);
        let mut reads = vec![
            (WORKFLOW_RUN_KEYSPACE.to_string(), run_key(self.run.run_id)),
            (RUN_LOG_KEYSPACE.to_string(), head_key(self.run.run_id)),
            (WORKFLOW_KEYSPACE.to_string(), wf_key),
            (WORKFLOW_VERSION_KEYSPACE.to_string(), ver_key.clone()),
            (WORKFLOW_SOURCE_KEYSPACE.to_string(), ver_key),
        ];
        for index in 0..self.pages.len() {
            let number = self.first_page + index as u32;
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                page_key(self.run.run_id, number),
            ));
        }
        if let Some(replicated) = &self.workflow {
            for tool_bytes in &replicated.tools {
                let tool =
                    ToolRecord::from_bytes(tool_bytes).map_err(|_| CatalogError::Forbidden)?;
                if tool.group_id != self.run.group_id {
                    return Err(CatalogError::Forbidden);
                }
                reads.push((
                    WORKFLOW_TOOL_KEYSPACE.to_string(),
                    tool_key(&tool.tool_id, &tool.version, tool.group_id),
                ));
            }
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u32>, CatalogError> {
        if !std::mem::replace(&mut self.reading, false) {
            result.done()?;
            return Ok(Next::Done(self.kept));
        }
        let mut values = result.values()?.into_iter();
        let stored = (values.next().flatten())
            .map(|value| WorkflowRunRecord::from_bytes(&value))
            .transpose()?;
        if stored.is_some_and(|stored| stored.node_id == self.local) {
            return Err(CatalogError::Forbidden);
        }
        let mut head = (values.next().flatten())
            .map(|value| LogHead::from_bytes(&value))
            .transpose()?
            .unwrap_or_default();
        let wf_row = values.next().flatten();
        let ver_row = values.next().flatten();
        let src_row = values.next().flatten();
        // Reject gaps or missing catalog records so a replacement holder triggers full backfill.
        if self.first_page > head.pages
            || (self.workflow.is_none() && (wf_row.is_none() || ver_row.is_none()))
        {
            return Err(CatalogError::NotFound);
        }
        let mut writes = vec![run_row(&self.run)?];
        let mut replicated_tools = Vec::new();
        if let Some(replicated) = self.workflow.take() {
            let mut wf = WorkflowRecord::from_bytes(&replicated.workflow)
                .map_err(|_| CatalogError::Forbidden)?;
            let ver = WorkflowVersionRecord::from_bytes(&replicated.version)
                .map_err(|_| CatalogError::Forbidden)?;
            if wf.workflow_id != self.run.workflow_id
                || wf.group_id != self.run.group_id
                || ver.workflow_id != self.run.workflow_id
                || ver.version != self.run.version
            {
                return Err(CatalogError::Forbidden);
            }
            let min_count = wf.version_count.max(ver.version);
            let wf_key = workflow_key(self.run.workflow_id);
            let ver_key = version_key(self.run.workflow_id, self.run.version);
            match wf_row {
                Some(existing_bytes) => {
                    let mut existing = WorkflowRecord::from_bytes(&existing_bytes)?;
                    if existing.version_count < min_count {
                        existing.version_count = min_count;
                        writes.push((
                            WORKFLOW_KEYSPACE.to_string(),
                            wf_key,
                            ByteView::from(existing.to_bytes()?),
                        ));
                    }
                }
                None => {
                    wf.version_count = min_count;
                    writes.push((
                        WORKFLOW_KEYSPACE.to_string(),
                        wf_key,
                        ByteView::from(wf.to_bytes()?),
                    ));
                }
            }
            if ver_row.is_none() {
                writes.push((
                    WORKFLOW_VERSION_KEYSPACE.to_string(),
                    ver_key.clone(),
                    ByteView::from(replicated.version),
                ));
            }
            if src_row.is_none()
                && let Some(source) = replicated.source
            {
                writes.push((
                    WORKFLOW_SOURCE_KEYSPACE.to_string(),
                    ver_key,
                    ByteView::from(source),
                ));
            }
            replicated_tools = replicated.tools;
        }
        let space = RUN_LOG_KEYSPACE.to_string();
        let run_id = self.run.run_id;
        for (index, bytes) in self.pages.iter().enumerate() {
            let existing = values.next().flatten();
            let page = decode_page(bytes).map_err(|_| CatalogError::Forbidden)?;
            let shorter = existing
                .as_deref()
                .and_then(|row| decode_page(row).ok())
                .is_some_and(|old| old.entries.len() > page.entries.len());
            if shorter {
                continue;
            }
            let number = self.first_page + index as u32;
            if number + 1 > head.pages {
                head.pages = number + 1;
                head.entries = page.entries.len() as u32;
            } else if number + 1 == head.pages {
                head.entries = head.entries.max(page.entries.len() as u32);
            }
            for entry in &page.entries {
                match entry {
                    RunLogEntry::Begin { .. } => head.begun = true,
                    RunLogEntry::Change { seq, .. } => {
                        head.last_seq = Some(head.last_seq.map_or(*seq, |last| last.max(*seq)));
                    }
                }
            }
            writes.push((
                space.clone(),
                page_key(run_id, number),
                ByteView::from(bytes.clone()),
            ));
        }
        for tool_bytes in replicated_tools {
            let existing = values.next().flatten();
            if existing.is_some() {
                continue;
            }
            let tool = ToolRecord::from_bytes(&tool_bytes).map_err(|_| CatalogError::Forbidden)?;
            if tool.group_id != self.run.group_id {
                return Err(CatalogError::Forbidden);
            }
            writes.push((
                WORKFLOW_TOOL_KEYSPACE.to_string(),
                tool_key(&tool.tool_id, &tool.version, tool.group_id),
                ByteView::from(tool_bytes),
            ));
        }
        writes.push((space, head_key(run_id), ByteView::from(head.to_bytes()?)));
        self.kept = head.pages;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

#[derive(Debug, PartialEq)]
enum ServeRunStep {
    Config,
    Read(Box<RealmConfigDocument>),
}

/// Serves an authoritative workflow run record to a sync-eligible realm executor verifying vault
/// secret bindings before release.
#[derive(Debug, PartialEq)]
pub struct ServeRunOperation {
    peer: NodeId,
    local: (NodeId, RealmId),
    run_id: Ulid,
    group_id: GroupId,
    step: ServeRunStep,
}

impl ServeRunOperation {
    pub fn new(peer: NodeId, local: (NodeId, RealmId), run_id: Ulid, group_id: GroupId) -> Self {
        Self {
            peer,
            local,
            run_id,
            group_id,
            step: ServeRunStep::Config,
        }
    }
}

impl Operation for ServeRunOperation {
    type Output = Option<Box<WorkflowRunRecord>>;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, MetadataReadError> {
        let read = GetConfigOperation::new(self.local.1);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, MetadataReadError> {
        match std::mem::replace(&mut self.step, ServeRunStep::Config) {
            ServeRunStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()
                    .map_err(|_| MetadataReadError::Unavailable)?
                    .map_err(|_| MetadataReadError::Unavailable)?;
                if !config
                    .node_kind(&self.peer)
                    .is_some_and(|kind| kind.is_sync_eligible())
                {
                    return Err(MetadataReadError::Forbidden);
                }
                self.step = ServeRunStep::Read(Box::new(config));
                Ok(Next::Call(read_run(self.run_id)))
            }
            ServeRunStep::Read(config) => {
                let Some(record) =
                    run_answer(result).map_err(|_| MetadataReadError::Unavailable)?
                else {
                    return Ok(Next::Done(None));
                };
                let authoritative = record.node_id == self.local.0
                    || record_holders(&config, self.group_id, record.node_id)
                        .contains(&self.local.0);
                if record.realm_id != self.local.1
                    || record.group_id != self.group_id
                    || record.local_only
                    || !authoritative
                {
                    return Ok(Next::Done(None));
                }
                Ok(Next::Done(Some(Box::new(record))))
            }
        }
    }

    fn expected_error(error: &MetadataReadError) -> bool {
        matches!(error, MetadataReadError::Forbidden)
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
