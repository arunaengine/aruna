//! Copies a run and its log pages onto group record holders so an owner or realm admin can
//! resume an unreachable run on a surviving node; nothing takes over automatically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    RUN_LOG_KEYSPACE, WORKFLOW_KEYSPACE, WORKFLOW_RUN_KEYSPACE, WORKFLOW_SOURCE_KEYSPACE,
    WORKFLOW_TOOL_KEYSPACE, WORKFLOW_VERSION_KEYSPACE,
};
use aruna_core::metadata::protocol::{
    METADATA_IO_TIMEOUT, MetadataReadError, MetadataTransportMessage, encode_message, message_size,
    peer_call, peer_reply,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::plan_target_placement;
use aruna_core::placement::resolver::PlacementResolutionContext;
use aruna_core::structs::execution::job::input::TaskSecretBinding;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, ToolRecord, WorkflowFiles, WorkflowRecord, WorkflowVersionRecord,
    tool_key, version_key, workflow_key,
};
use aruna_core::structs::execution::workflow_run::{
    ChunkKind, LogHead, ReplicatedChunk, ReplicatedWorkflow, WorkflowRunAuth, WorkflowRunRecord,
    chunk_key, decode_page, encode_page, head_key, page_key, run_key, stage_prefix,
    transfer_prefix,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key};
use aruna_core::workflow::source_digest;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_workflow_contract::preflight::{NodeKind, RunLogEntry, RunLogPage};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::log::{LoadLogOperation, pages_answer};
use super::{read_run, run_answer, run_row};
use crate::workflows::{CatalogError, GraphView, ScanOperation, scan_answer};

/// Encoded store request size target, well inside one 16 MiB metadata frame.
const BATCH_BYTES: usize = 8 << 20;
/// Maximum chunk count allowed for one replicated source or tool blob.
const MAX_CHUNKS: u32 = 64;
/// Age after which an unfinished staged transfer from a peer is reclaimed.
const STALE_STAGE_MS: u64 = 5 * 60 * 1000;
/// Non-expired prior transfers retained per `(run_id, peer)` before evicting oldest.
const MAX_PRIOR_TRANSFERS: usize = 1;
/// Rows scanned per page when reclaiming staged replica rows.
const CLEANUP_ROWS: usize = 256;

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

fn chunk_target(kind: &ChunkKind) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    match kind {
        ChunkKind::Source => {
            hasher.update(b"source");
        }
        ChunkKind::Tool(id, version) => {
            hasher.update(b"tool\0");
            hasher.update(id.as_bytes());
            hasher.update(b"\0");
            hasher.update(version.as_bytes());
        }
        ChunkKind::Page(number) => {
            hasher.update(b"page\0");
            hasher.update(&number.to_be_bytes());
        }
        ChunkKind::Run => {
            hasher.update(b"run");
        }
        ChunkKind::Workflow => {
            hasher.update(b"workflow");
        }
        ChunkKind::Version => {
            hasher.update(b"version");
        }
    }
    *hasher.finalize().as_bytes()
}

fn stage_meta_key(run_id: Ulid, peer: NodeId, transfer: u64) -> Key {
    let mut key = transfer_prefix(run_id, peer, transfer).to_vec();
    key.push(b'm');
    Key::from(key)
}

fn staged_key(run_id: Ulid, peer: NodeId, transfer: u64, kind: ChunkKind) -> Key {
    chunk_key(run_id, peer, transfer, chunk_target(&kind), u32::MAX)
}

fn staged_tools_key(run_id: Ulid, peer: NodeId, transfer: u64) -> Key {
    let mut key = transfer_prefix(run_id, peer, transfer).to_vec();
    key.push(b't');
    Key::from(key)
}

fn parse_stage_key(run_id: Ulid, peer: NodeId, key: &[u8]) -> Option<(u64, u8)> {
    let prefix = stage_prefix(run_id, peer);
    if key.len() < 58 || !key.starts_with(prefix.as_ref()) {
        return None;
    }
    let transfer = u64::from_be_bytes(key[49..57].try_into().ok()?);
    Some((transfer, key[57]))
}

fn stub_run(run: &WorkflowRunRecord) -> WorkflowRunRecord {
    WorkflowRunRecord {
        run_id: run.run_id,
        realm_id: run.realm_id,
        group_id: run.group_id,
        workflow_id: run.workflow_id,
        workflow_name: String::new(),
        version: run.version,
        lineage: run.lineage,
        state: run.state,
        node_id: run.node_id,
        created_by: run.created_by,
        created_at_ms: run.created_at_ms,
        started_at_ms: run.started_at_ms,
        finished_at_ms: run.finished_at_ms,
        error: None,
        values_json: String::new(),
        inputs_json: None,
        output: None,
        outputs: Vec::new(),
        provenance_crate: run.provenance_crate,
        provenance_document_id: run.provenance_document_id,
        provenance_status: None,
        last_event_seq: run.last_event_seq,
        local_only: run.local_only,
        origin: None,
        parent_job_id: run.parent_job_id,
        claim_token_digest: None,
        attempts: run.attempts,
        steps: Vec::new(),
        resumes_run_id: run.resumes_run_id,
        repeats_run_id: run.repeats_run_id,
        cache_scope: run.cache_scope,
        placement_site: None,
        protected: run.protected,
        secrets: Default::default(),
        path_restrictions: None,
        captured_remote: Vec::new(),
        used_tools: Vec::new(),
    }
}

fn chunk_blob(
    data: &[u8],
    kind: ChunkKind,
    chunk_size: usize,
) -> Result<Vec<ReplicatedChunk>, CatalogError> {
    let step = chunk_size.max(1);
    let parts: Vec<&[u8]> = data.chunks(step).collect();
    let total = u32::try_from(parts.len())
        .map_err(|_| CatalogError::Storage("replicated payload exceeds chunk count".into()))?;
    if total > MAX_CHUNKS {
        return Err(CatalogError::Storage(
            "replicated payload exceeds maximum chunk count".into(),
        ));
    }
    Ok(parts
        .into_iter()
        .enumerate()
        .map(|(index, slice)| ReplicatedChunk {
            kind: kind.clone(),
            index: index as u32,
            total,
            data: slice.to_vec(),
        })
        .collect())
}

fn push_chunks(
    frames: &mut Vec<Vec<u8>>,
    envelope: &WorkflowRunRecord,
    start_page: u32,
    kind: ChunkKind,
    data: &[u8],
    (chunk_size, limit, stub, transfer): (usize, usize, bool, u64),
) -> Result<(), CatalogError> {
    for chunk in chunk_blob(data, kind, chunk_size)? {
        let part = ReplicatedWorkflow {
            chunks: vec![chunk],
            more: true,
            stub_run: stub,
            transfer_id: transfer,
            ..ReplicatedWorkflow::default()
        };
        frames.push(pack_frame(
            envelope,
            start_page,
            Vec::new(),
            Some(part),
            limit,
        )?);
    }
    Ok(())
}

fn measure_frame(
    run: &WorkflowRunRecord,
    first_page: u32,
    pages: &[Vec<u8>],
    workflow: Option<&ReplicatedWorkflow>,
) -> Result<usize, CatalogError> {
    let message = MetadataTransportMessage::StoreRunLog {
        run: Box::new(run.clone()),
        first_page,
        pages: pages.to_vec(),
        workflow: workflow.cloned(),
    };
    message_size(&message).map_err(|error| CatalogError::Storage(error.to_string()))
}

fn pack_frame(
    run: &WorkflowRunRecord,
    first_page: u32,
    pages: Vec<Vec<u8>>,
    workflow: Option<ReplicatedWorkflow>,
    limit: usize,
) -> Result<Vec<u8>, CatalogError> {
    let message = MetadataTransportMessage::StoreRunLog {
        run: Box::new(run.clone()),
        first_page,
        pages,
        workflow,
    };
    let bytes =
        encode_message(&message).map_err(|error| CatalogError::Storage(error.to_string()))?;
    if bytes.len() > limit {
        return Err(CatalogError::Storage(
            "replicated workflow frame exceeds batch bound".into(),
        ));
    }
    Ok(bytes)
}

fn keep_stored(stored: &WorkflowRunRecord, incoming: &WorkflowRunRecord) -> bool {
    if stored.state.is_terminal() != incoming.state.is_terminal() {
        return stored.state.is_terminal();
    }
    (stored.attempts, stored.last_event_seq) > (incoming.attempts, incoming.last_event_seq)
}

fn tool_update(
    existing: Option<&[u8]>,
    incoming: &[u8],
    group: GroupId,
) -> Result<Option<(Key, ByteView)>, CatalogError> {
    let tool = ToolRecord::from_bytes(incoming).map_err(|_| CatalogError::Forbidden)?;
    if tool.group_id != group {
        return Err(CatalogError::Forbidden);
    }
    if let Some(old_bytes) = existing
        && let Ok(old) = ToolRecord::from_bytes(old_bytes)
    {
        if tool.created_at_ms < old.created_at_ms {
            return Ok(None);
        }
        if tool.created_at_ms == old.created_at_ms && incoming <= old_bytes {
            return Ok(None);
        }
    }
    let key = tool_key(&tool.tool_id, &tool.version, tool.group_id);
    Ok(Some((key, ByteView::from(incoming.to_vec()))))
}

/// Selects the group tools a composed workflow uses from the coordinator's resolved tool list or
/// parsed step graph, following `ToolLibrary::get` (exact version, else last in scan order).
fn used_tools(
    run_tools: &[(String, String)],
    graph_json: Option<&str>,
    tools: Vec<ToolRecord>,
) -> Vec<ToolRecord> {
    let mut wanted: BTreeSet<(String, String)> = run_tools.iter().cloned().collect();
    if wanted.is_empty()
        && let Some(view) = graph_json.and_then(|json| serde_json::from_str::<GraphView>(json).ok())
    {
        for node in view
            .graph
            .nodes
            .into_iter()
            .filter(|n| n.kind == NodeKind::Step)
        {
            let Some(id) = node.tool else {
                continue;
            };
            match node.version {
                Some(version) => {
                    wanted.insert((id, version));
                }
                None => {
                    if let Some(last) = tools.iter().rfind(|tool| tool.tool_id == id) {
                        wanted.insert((id, last.version.clone()));
                    }
                }
            }
        }
    }
    if wanted.is_empty() {
        return tools;
    }
    tools
        .into_iter()
        .filter(|tool| wanted.contains(&(tool.tool_id.clone(), tool.version.clone())))
        .collect()
}

#[derive(Debug, PartialEq)]
enum SendStep {
    Run,
    Config,
    Workflow(Vec<NodeId>, bool),
    Tools(Vec<NodeId>, ReplicatedWorkflow, Option<String>, bool),
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
    transfer: u64,
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
            transfer: 0,
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
        self.frames_bounded(self.from_page, run, pages, workflow, BATCH_BYTES)
    }

    fn frames_from(
        &self,
        start_page: u32,
        run: &WorkflowRunRecord,
        pages: &[RunLogPage],
        workflow: Option<ReplicatedWorkflow>,
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        self.frames_bounded(start_page, run, pages, workflow, BATCH_BYTES)
    }

    fn frames_bounded(
        &self,
        start_page: u32,
        run: &WorkflowRunRecord,
        pages: &[RunLogPage],
        workflow: Option<ReplicatedWorkflow>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        let mut frames = Vec::new();
        let mut final_meta = None;
        let transfer = self.transfer.max(1);
        let probe_more = ReplicatedWorkflow {
            more: true,
            transfer_id: transfer,
            ..ReplicatedWorkflow::default()
        };
        let use_stub = measure_frame(run, start_page, &[], Some(&probe_more))? > limit / 2;
        let stub = stub_run(run);
        let envelope = if use_stub { &stub } else { run };
        let empty_part = ReplicatedWorkflow {
            more: true,
            stub_run: use_stub,
            transfer_id: transfer,
            ..ReplicatedWorkflow::default()
        };
        let overhead = measure_frame(envelope, start_page, &[], Some(&empty_part))?;
        let chunk_size = limit.saturating_sub(overhead + 256).min(limit / 2).max(1);
        let bounds = (chunk_size, limit, use_stub, transfer);
        if let Some(bundle) = workflow {
            let mut small: Vec<Vec<u8>> = Vec::new();
            for tool_bytes in bundle.tools {
                let solo = ReplicatedWorkflow {
                    tools: vec![tool_bytes.clone()],
                    more: true,
                    stub_run: use_stub,
                    transfer_id: transfer,
                    ..ReplicatedWorkflow::default()
                };
                if measure_frame(envelope, start_page, &[], Some(&solo))? <= limit {
                    let mut candidate = small.clone();
                    candidate.push(tool_bytes.clone());
                    let test = ReplicatedWorkflow {
                        tools: candidate,
                        more: true,
                        stub_run: use_stub,
                        transfer_id: transfer,
                        ..ReplicatedWorkflow::default()
                    };
                    if !small.is_empty()
                        && measure_frame(envelope, start_page, &[], Some(&test))? > limit
                    {
                        let flush = ReplicatedWorkflow {
                            tools: std::mem::take(&mut small),
                            more: true,
                            stub_run: use_stub,
                            transfer_id: transfer,
                            ..ReplicatedWorkflow::default()
                        };
                        frames.push(pack_frame(
                            envelope,
                            start_page,
                            Vec::new(),
                            Some(flush),
                            limit,
                        )?);
                    }
                    small.push(tool_bytes);
                } else {
                    if !small.is_empty() {
                        let flush = ReplicatedWorkflow {
                            tools: std::mem::take(&mut small),
                            more: true,
                            stub_run: use_stub,
                            transfer_id: transfer,
                            ..ReplicatedWorkflow::default()
                        };
                        frames.push(pack_frame(
                            envelope,
                            start_page,
                            Vec::new(),
                            Some(flush),
                            limit,
                        )?);
                    }
                    let tool = ToolRecord::from_bytes(&tool_bytes)?;
                    let kind = ChunkKind::Tool(tool.tool_id, tool.version);
                    push_chunks(&mut frames, envelope, start_page, kind, &tool_bytes, bounds)?;
                }
            }
            let mut commit_wf = if bundle.workflow.len() > chunk_size {
                push_chunks(
                    &mut frames,
                    envelope,
                    start_page,
                    ChunkKind::Workflow,
                    &bundle.workflow,
                    bounds,
                )?;
                Vec::new()
            } else {
                bundle.workflow
            };
            let mut commit_ver = if bundle.version.len() > chunk_size {
                push_chunks(
                    &mut frames,
                    envelope,
                    start_page,
                    ChunkKind::Version,
                    &bundle.version,
                    bounds,
                )?;
                Vec::new()
            } else {
                bundle.version
            };
            let mut commit_src = match bundle.source {
                Some(bytes) if bytes.len() > chunk_size => {
                    push_chunks(
                        &mut frames,
                        envelope,
                        start_page,
                        ChunkKind::Source,
                        &bytes,
                        bounds,
                    )?;
                    None
                }
                other => other,
            };
            let mut commit = ReplicatedWorkflow {
                workflow: std::mem::take(&mut commit_wf),
                version: std::mem::take(&mut commit_ver),
                source: commit_src.take(),
                tools: Vec::new(),
                chunks: Vec::new(),
                more: false,
                stub_run: use_stub,
                transfer_id: transfer,
            };
            if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                && let Some(bytes) = commit.source.take()
            {
                push_chunks(
                    &mut frames,
                    envelope,
                    start_page,
                    ChunkKind::Source,
                    &bytes,
                    bounds,
                )?;
            }
            if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                && !commit.version.is_empty()
            {
                let bytes = std::mem::take(&mut commit.version);
                push_chunks(
                    &mut frames,
                    envelope,
                    start_page,
                    ChunkKind::Version,
                    &bytes,
                    bounds,
                )?;
            }
            if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                && !commit.workflow.is_empty()
            {
                let bytes = std::mem::take(&mut commit.workflow);
                push_chunks(
                    &mut frames,
                    envelope,
                    start_page,
                    ChunkKind::Workflow,
                    &bytes,
                    bounds,
                )?;
            }
            if !small.is_empty() {
                let mut probe = commit.clone();
                probe.tools = small.clone();
                if measure_frame(envelope, start_page, &[], Some(&probe))? <= limit {
                    commit.tools = small;
                } else {
                    let flush = ReplicatedWorkflow {
                        tools: small,
                        more: true,
                        stub_run: use_stub,
                        transfer_id: transfer,
                        ..ReplicatedWorkflow::default()
                    };
                    frames.push(pack_frame(
                        envelope,
                        start_page,
                        Vec::new(),
                        Some(flush),
                        limit,
                    )?);
                }
            }
            final_meta = Some(commit);
        }
        if use_stub {
            let run_bytes = run.to_bytes()?;
            let mut run_parts = chunk_blob(&run_bytes, ChunkKind::Run, chunk_size)?;
            let last_run = run_parts.pop();
            for chunk in run_parts {
                let part = ReplicatedWorkflow {
                    chunks: vec![chunk],
                    more: true,
                    stub_run: true,
                    transfer_id: transfer,
                    ..ReplicatedWorkflow::default()
                };
                frames.push(pack_frame(
                    envelope,
                    start_page,
                    Vec::new(),
                    Some(part),
                    limit,
                )?);
            }
            if let Some(last) = last_run {
                let mut commit = final_meta.take().unwrap_or_else(|| ReplicatedWorkflow {
                    more: false,
                    stub_run: true,
                    transfer_id: transfer,
                    ..ReplicatedWorkflow::default()
                });
                commit.chunks.push(last);
                if measure_frame(envelope, start_page, &[], Some(&commit))? > limit {
                    if !commit.tools.is_empty() {
                        let flush = ReplicatedWorkflow {
                            tools: std::mem::take(&mut commit.tools),
                            more: true,
                            stub_run: true,
                            transfer_id: transfer,
                            ..ReplicatedWorkflow::default()
                        };
                        frames.push(pack_frame(
                            envelope,
                            start_page,
                            Vec::new(),
                            Some(flush),
                            limit,
                        )?);
                    }
                    if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                        && let Some(bytes) = commit.source.take()
                    {
                        push_chunks(
                            &mut frames,
                            envelope,
                            start_page,
                            ChunkKind::Source,
                            &bytes,
                            bounds,
                        )?;
                    }
                    if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                        && !commit.version.is_empty()
                    {
                        let bytes = std::mem::take(&mut commit.version);
                        push_chunks(
                            &mut frames,
                            envelope,
                            start_page,
                            ChunkKind::Version,
                            &bytes,
                            bounds,
                        )?;
                    }
                    if measure_frame(envelope, start_page, &[], Some(&commit))? > limit
                        && !commit.workflow.is_empty()
                    {
                        let bytes = std::mem::take(&mut commit.workflow);
                        push_chunks(
                            &mut frames,
                            envelope,
                            start_page,
                            ChunkKind::Workflow,
                            &bytes,
                            bounds,
                        )?;
                    }
                }
                final_meta = Some(commit);
            }
        }
        let encoded: Vec<Vec<u8>> = pages
            .iter()
            .skip(start_page as usize)
            .map(encode_page)
            .collect::<Result<_, _>>()?;
        let page_meta = |meta: &Option<ReplicatedWorkflow>| {
            meta.clone().or_else(|| {
                use_stub.then(|| ReplicatedWorkflow {
                    more: false,
                    stub_run: true,
                    transfer_id: transfer,
                    ..ReplicatedWorkflow::default()
                })
            })
        };
        let mut first = start_page;
        let mut batch: Vec<Vec<u8>> = Vec::new();
        for (offset, page) in encoded.into_iter().enumerate() {
            let number = start_page + offset as u32;
            let solo_meta = page_meta(&None);
            if measure_frame(
                envelope,
                number,
                std::slice::from_ref(&page),
                solo_meta.as_ref(),
            )? > limit
            {
                if !batch.is_empty() || final_meta.is_some() {
                    let meta = page_meta(&final_meta);
                    final_meta = None;
                    frames.push(pack_frame(
                        envelope,
                        first,
                        std::mem::take(&mut batch),
                        meta,
                        limit,
                    )?);
                }
                let mut parts = chunk_blob(&page, ChunkKind::Page(number), chunk_size)?;
                let last = parts.pop();
                for chunk in parts {
                    let part = ReplicatedWorkflow {
                        chunks: vec![chunk],
                        more: true,
                        stub_run: use_stub,
                        transfer_id: transfer,
                        ..ReplicatedWorkflow::default()
                    };
                    frames.push(pack_frame(envelope, number, Vec::new(), Some(part), limit)?);
                }
                if let Some(chunk) = last {
                    let done = ReplicatedWorkflow {
                        chunks: vec![chunk],
                        more: false,
                        stub_run: use_stub,
                        transfer_id: transfer,
                        ..ReplicatedWorkflow::default()
                    };
                    frames.push(pack_frame(envelope, number, Vec::new(), Some(done), limit)?);
                }
                first = number + 1;
                continue;
            }
            batch.push(page);
            let cur_meta = page_meta(&final_meta);
            if measure_frame(envelope, first, &batch, cur_meta.as_ref())? > limit {
                let overflow = batch.pop();
                if !batch.is_empty() || final_meta.is_some() {
                    let count = batch.len() as u32;
                    let meta = page_meta(&final_meta);
                    final_meta = None;
                    frames.push(pack_frame(
                        envelope,
                        first,
                        std::mem::take(&mut batch),
                        meta,
                        limit,
                    )?);
                    first += count;
                }
                if let Some(item) = overflow {
                    batch.push(item);
                }
            }
        }
        if !batch.is_empty() || final_meta.is_some() || frames.is_empty() {
            let meta = page_meta(&final_meta);
            frames.push(pack_frame(envelope, first, batch, meta, limit)?);
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
        self.transfer = self.transfer.wrapping_add(1).max(1);
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

    fn seed(&mut self, entropy: u128) {
        self.transfer = entropy as u64;
    }

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
                        chunks: Vec::new(),
                        more: false,
                        stub_run: false,
                        transfer_id: 0,
                    }),
                    _ => None,
                };
                let parsed_ver = workflow
                    .as_ref()
                    .and_then(|bundle| WorkflowVersionRecord::from_bytes(&bundle.version).ok());
                let is_uwe = parsed_ver
                    .as_ref()
                    .is_some_and(|ver| ver.language == StoredWorkflowLanguage::Uwe);
                let graph_json = parsed_ver.and_then(|ver| ver.inspection.graph);
                if is_uwe
                    && let Some(run) = &self.run
                    && let Some(bundle) = workflow
                {
                    let groups = Some([run.group_id].into());
                    self.step = SendStep::Tools(holders, bundle, graph_json, load_log);
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
            SendStep::Tools(holders, mut bundle, graph_json, load_log) => {
                let tools: Vec<ToolRecord> = scan_answer(result)?
                    .into_iter()
                    .map(|(_, tool)| tool)
                    .collect();
                let run_tools = self
                    .run
                    .as_ref()
                    .map(|run| run.used_tools.as_slice())
                    .unwrap_or(&[]);
                for tool in used_tools(run_tools, graph_json.as_deref(), tools) {
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
    now_ms: u64,
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
            now_ms: 0,
            step: StoreStep::Config,
        }
    }
}

impl Operation for StoreReplicaOperation {
    type Output = u32;
    type Error = MetadataReadError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

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
                    peer: self.peer,
                    local: self.local.0,
                    run: self.run.clone(),
                    first_page: self.first_page,
                    pages: std::mem::take(&mut self.pages),
                    workflow: self.workflow.take(),
                    now_ms: self.now_ms,
                    step: WriteStep::Read,
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

#[derive(Clone, Debug, PartialEq)]
struct CommitState {
    transfer: u64,
    tools: Vec<(String, String)>,
    writes: Vec<(String, Key, ByteView)>,
    deletes: Vec<(String, Key)>,
}

#[derive(Clone, Debug, PartialEq)]
struct StageScan {
    transfer: u64,
    clear_all: bool,
    writes: Vec<(String, Key, ByteView)>,
    deletes: Vec<(String, Key)>,
    seen: Vec<(u64, u8, Key, Option<u64>)>,
}

#[derive(Clone, Debug, PartialEq)]
enum WriteStep {
    Read,
    ReadTools(Box<CommitState>),
    ScanStage(Box<StageScan>),
    Write(Vec<(String, Key)>),
    Delete,
}

/// Writes a run copy with its pages and the log head they make, in one transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteReplicaOperation {
    peer: NodeId,
    local: NodeId,
    run: Box<WorkflowRunRecord>,
    first_page: u32,
    pages: Vec<Vec<u8>>,
    workflow: Option<ReplicatedWorkflow>,
    now_ms: u64,
    step: WriteStep,
    kept: u32,
}

impl WriteReplicaOperation {
    fn scan_stage(
        &mut self,
        scan: StageScan,
        after: Option<Key>,
    ) -> Result<Next<u32>, CatalogError> {
        let prefix = stage_prefix(self.run.run_id, self.peer);
        self.step = WriteStep::ScanStage(Box::new(scan));
        Ok(Next::Call(IoCall::Page {
            space: RUN_LOG_KEYSPACE.to_string(),
            prefix,
            after,
            limit: CLEANUP_ROWS,
        }))
    }

    fn emit_writes(
        &mut self,
        writes: Vec<(String, Key, ByteView)>,
        mut deletes: Vec<(String, Key)>,
    ) -> Result<Next<u32>, CatalogError> {
        deletes.sort();
        deletes.dedup();
        if writes.is_empty() {
            if !deletes.is_empty() {
                self.step = WriteStep::Delete;
                return Ok(Next::Call(IoCall::Delete(deletes)));
            }
            return Ok(Next::Done(self.kept));
        }
        self.step = WriteStep::Write(deletes);
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn finish_stage(&mut self, mut scan: StageScan) -> Result<Next<u32>, CatalogError> {
        let space = RUN_LOG_KEYSPACE.to_string();
        if !scan.clear_all {
            let mut groups: BTreeMap<u64, (u64, Vec<Key>)> = BTreeMap::new();
            for (transfer, tag, key, ts) in scan.seen {
                let entry = groups.entry(transfer).or_insert_with(|| (0, Vec::new()));
                if tag == b'm'
                    && let Some(stamp) = ts
                {
                    entry.0 = stamp;
                }
                entry.1.push(key);
            }
            let mut active = Vec::new();
            for (transfer, (stamp, keys)) in groups {
                let expired = stamp == 0 || self.now_ms.saturating_sub(stamp) >= STALE_STAGE_MS;
                if expired {
                    for key in keys {
                        scan.deletes.push((space.clone(), key));
                    }
                } else {
                    active.push((stamp, transfer, keys));
                }
            }
            if active.len() > MAX_PRIOR_TRANSFERS {
                active.sort_by_key(|(stamp, transfer, _)| (*stamp, *transfer));
                let drop_count = active.len() - MAX_PRIOR_TRANSFERS;
                for (_, _, keys) in active.into_iter().take(drop_count) {
                    for key in keys {
                        scan.deletes.push((space.clone(), key));
                    }
                }
            }
        }
        self.emit_writes(scan.writes, scan.deletes)
    }
}

impl Operation for WriteReplicaOperation {
    type Output = u32;
    type Error = CatalogError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<u32>, CatalogError> {
        let wf_key = workflow_key(self.run.workflow_id);
        let ver_key = version_key(self.run.workflow_id, self.run.version);
        let run_id = self.run.run_id;
        let transfer = self.workflow.as_ref().map_or(0, |wf| wf.transfer_id);
        if self
            .workflow
            .as_ref()
            .is_some_and(|wf| transfer == 0 && (wf.more || !wf.chunks.is_empty()))
        {
            return Err(CatalogError::Forbidden);
        }
        let mut reads = vec![
            (WORKFLOW_RUN_KEYSPACE.to_string(), run_key(run_id)),
            (RUN_LOG_KEYSPACE.to_string(), head_key(run_id)),
            (WORKFLOW_KEYSPACE.to_string(), wf_key),
            (WORKFLOW_VERSION_KEYSPACE.to_string(), ver_key.clone()),
            (WORKFLOW_SOURCE_KEYSPACE.to_string(), ver_key),
        ];
        if transfer != 0 {
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                staged_key(run_id, self.peer, transfer, ChunkKind::Workflow),
            ));
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                staged_key(run_id, self.peer, transfer, ChunkKind::Version),
            ));
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                staged_key(run_id, self.peer, transfer, ChunkKind::Source),
            ));
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                staged_tools_key(run_id, self.peer, transfer),
            ));
            reads.push((
                RUN_LOG_KEYSPACE.to_string(),
                stage_meta_key(run_id, self.peer, transfer),
            ));
        }
        for index in 0..self.pages.len() {
            let number = self.first_page + index as u32;
            reads.push((RUN_LOG_KEYSPACE.to_string(), page_key(run_id, number)));
        }
        if let Some(replicated) = &self.workflow {
            for tool_bytes in &replicated.tools {
                let tool =
                    ToolRecord::from_bytes(tool_bytes).map_err(|_| CatalogError::Forbidden)?;
                if tool.group_id != self.run.group_id {
                    return Err(CatalogError::Forbidden);
                }
                if replicated.more {
                    reads.push((
                        RUN_LOG_KEYSPACE.to_string(),
                        staged_key(
                            run_id,
                            self.peer,
                            transfer,
                            ChunkKind::Tool(tool.tool_id, tool.version),
                        ),
                    ));
                } else {
                    reads.push((
                        WORKFLOW_TOOL_KEYSPACE.to_string(),
                        tool_key(&tool.tool_id, &tool.version, tool.group_id),
                    ));
                }
            }
            let mut seen_chunks = BTreeSet::new();
            let mut seen_done = BTreeSet::new();
            for chunk in &replicated.chunks {
                if chunk.total == 0 || chunk.index >= chunk.total || chunk.total > MAX_CHUNKS {
                    return Err(CatalogError::Forbidden);
                }
                let target = chunk_target(&chunk.kind);
                if !seen_chunks.insert((target, chunk.index)) {
                    return Err(CatalogError::Forbidden);
                }
                if chunk.index + 1 == chunk.total {
                    if !seen_done.insert(target) {
                        return Err(CatalogError::Forbidden);
                    }
                    for prior in 0..chunk.index {
                        reads.push((
                            RUN_LOG_KEYSPACE.to_string(),
                            chunk_key(run_id, self.peer, transfer, target, prior),
                        ));
                    }
                    match &chunk.kind {
                        ChunkKind::Tool(id, version) => {
                            if replicated.more {
                                reads.push((
                                    RUN_LOG_KEYSPACE.to_string(),
                                    staged_key(
                                        run_id,
                                        self.peer,
                                        transfer,
                                        ChunkKind::Tool(id.clone(), version.clone()),
                                    ),
                                ));
                            } else {
                                reads.push((
                                    WORKFLOW_TOOL_KEYSPACE.to_string(),
                                    tool_key(id, version, self.run.group_id),
                                ));
                            }
                        }
                        ChunkKind::Page(number) => {
                            reads.push((RUN_LOG_KEYSPACE.to_string(), page_key(run_id, *number)));
                        }
                        ChunkKind::Source
                        | ChunkKind::Run
                        | ChunkKind::Workflow
                        | ChunkKind::Version => {}
                    }
                }
            }
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u32>, CatalogError> {
        match std::mem::replace(&mut self.step, WriteStep::Delete) {
            WriteStep::ReadTools(mut commit) => {
                let mut values = result.values()?.into_iter();
                for (id, version) in commit.tools {
                    let staged = values.next().flatten();
                    let existing = values.next().flatten();
                    let Some(bytes) = staged else {
                        continue;
                    };
                    let expected = tool_key(&id, &version, self.run.group_id);
                    let prior = commit
                        .writes
                        .iter()
                        .position(|(ks, k, _)| ks == WORKFLOW_TOOL_KEYSPACE && *k == expected);
                    let base = prior
                        .map(|idx| commit.writes[idx].2.as_ref())
                        .or(existing.as_deref());
                    if let Some((key, value)) = tool_update(base, &bytes, self.run.group_id)? {
                        if key != expected {
                            return Err(CatalogError::Forbidden);
                        }
                        match prior {
                            Some(idx) => commit.writes[idx].2 = value,
                            None => {
                                commit.writes.push((
                                    WORKFLOW_TOOL_KEYSPACE.to_string(),
                                    key,
                                    value,
                                ));
                            }
                        }
                    }
                }
                return self.scan_stage(
                    StageScan {
                        transfer: commit.transfer,
                        clear_all: true,
                        writes: commit.writes,
                        deletes: commit.deletes,
                        seen: Vec::new(),
                    },
                    None,
                );
            }
            WriteStep::ScanStage(mut scan) => {
                let (rows, next) = result.page()?;
                let space = RUN_LOG_KEYSPACE.to_string();
                for (key, value) in rows {
                    if scan.clear_all {
                        scan.deletes.push((space.clone(), key));
                        continue;
                    }
                    let Some((row_transfer, tag)) =
                        parse_stage_key(self.run.run_id, self.peer, key.as_ref())
                    else {
                        continue;
                    };
                    if row_transfer == scan.transfer {
                        continue;
                    }
                    let stamp = (tag == b'm' && value.len() == 8)
                        .then(|| u64::from_be_bytes(value.as_ref().try_into().unwrap()));
                    scan.seen.push((row_transfer, tag, key, stamp));
                }
                if let Some(after) = next {
                    return self.scan_stage(*scan, Some(after));
                }
                return self.finish_stage(*scan);
            }
            WriteStep::Write(deletes) => {
                result.done()?;
                if !deletes.is_empty() {
                    self.step = WriteStep::Delete;
                    return Ok(Next::Call(IoCall::Delete(deletes)));
                }
                return Ok(Next::Done(self.kept));
            }
            WriteStep::Delete => {
                result.done()?;
                return Ok(Next::Done(self.kept));
            }
            WriteStep::Read => {}
        }
        let mut values = result.values()?.into_iter();
        let stored = (values.next().flatten())
            .map(|value| WorkflowRunRecord::from_bytes(&value))
            .transpose()?;
        if stored.as_ref().is_some_and(|old| old.node_id == self.local) {
            return Err(CatalogError::Forbidden);
        }
        let mut head = (values.next().flatten())
            .map(|value| LogHead::from_bytes(&value))
            .transpose()?
            .unwrap_or_default();
        let wf_row = values.next().flatten();
        let ver_row = values.next().flatten();
        let src_row = values.next().flatten();
        let transfer = self.workflow.as_ref().map_or(0, |wf| wf.transfer_id);
        let (staged_wf, staged_ver, staged_src, staged_index_row, meta_row) = match transfer != 0 {
            true => (
                values.next().flatten(),
                values.next().flatten(),
                values.next().flatten(),
                values.next().flatten(),
                values.next().flatten(),
            ),
            false => (None, None, None, None, None),
        };
        let mut staged_index: Vec<(String, String)> = staged_index_row
            .as_deref()
            .and_then(|bytes| postcard::from_bytes(bytes).ok())
            .unwrap_or_default();
        let mut index_dirty = false;
        let mut page_rows = Vec::with_capacity(self.pages.len());
        for _ in 0..self.pages.len() {
            page_rows.push(values.next().flatten());
        }
        let more = self.workflow.as_ref().is_some_and(|wf| wf.more);
        let stub = self.workflow.as_ref().is_some_and(|wf| wf.stub_run);
        if !more && self.first_page > head.pages {
            return Err(CatalogError::NotFound);
        }
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        let mut pending_wf: Option<Vec<u8>> = None;
        let mut pending_ver: Option<Vec<u8>> = None;
        let mut pending_src: Option<Vec<u8>> = None;
        let mut assembled_run: Option<WorkflowRunRecord> = None;
        let run_id = self.run.run_id;
        let peer = self.peer;
        let space = RUN_LOG_KEYSPACE.to_string();
        let wf_key = workflow_key(self.run.workflow_id);
        let ver_key = version_key(self.run.workflow_id, self.run.version);
        let store_workflow = |bytes: &[u8],
                              wf_row: Option<&[u8]>,
                              run: &WorkflowRunRecord,
                              writes: &mut Vec<(String, Key, ByteView)>|
         -> Result<(), CatalogError> {
            let mut wf = WorkflowRecord::from_bytes(bytes).map_err(|_| CatalogError::Forbidden)?;
            if wf.workflow_id != run.workflow_id || wf.group_id != run.group_id {
                return Err(CatalogError::Forbidden);
            }
            let min_count = wf.version_count.max(run.version);
            match wf_row {
                Some(existing_bytes) => {
                    let mut existing = WorkflowRecord::from_bytes(existing_bytes)?;
                    if existing.version_count < min_count {
                        existing.version_count = min_count;
                        writes.push((
                            WORKFLOW_KEYSPACE.to_string(),
                            wf_key.clone(),
                            ByteView::from(existing.to_bytes()?),
                        ));
                    }
                }
                None => {
                    wf.version_count = min_count;
                    writes.push((
                        WORKFLOW_KEYSPACE.to_string(),
                        wf_key.clone(),
                        ByteView::from(wf.to_bytes()?),
                    ));
                }
            }
            Ok(())
        };
        if let Some(replicated) = self.workflow.take() {
            if more
                && (!replicated.workflow.is_empty()
                    || !replicated.version.is_empty()
                    || replicated.source.is_some())
            {
                return Err(CatalogError::Forbidden);
            }
            for tool_bytes in replicated.tools {
                let existing = values.next().flatten();
                if let Some((key, value)) =
                    tool_update(existing.as_deref(), &tool_bytes, self.run.group_id)?
                {
                    if more {
                        let tool = ToolRecord::from_bytes(&tool_bytes)
                            .map_err(|_| CatalogError::Forbidden)?;
                        let pair = (tool.tool_id, tool.version);
                        writes.push((
                            space.clone(),
                            staged_key(
                                run_id,
                                peer,
                                transfer,
                                ChunkKind::Tool(pair.0.clone(), pair.1.clone()),
                            ),
                            value,
                        ));
                        if !staged_index.contains(&pair) {
                            staged_index.push(pair);
                            index_dirty = true;
                        }
                    } else {
                        writes.push((WORKFLOW_TOOL_KEYSPACE.to_string(), key, value));
                    }
                }
            }
            for chunk in replicated.chunks {
                let target = chunk_target(&chunk.kind);
                if chunk.index + 1 < chunk.total {
                    writes.push((
                        space.clone(),
                        chunk_key(run_id, peer, transfer, target, chunk.index),
                        ByteView::from(chunk.data),
                    ));
                    continue;
                }
                let mut assembled = Vec::new();
                for prior in 0..chunk.index {
                    let part = values.next().flatten().ok_or(CatalogError::NotFound)?;
                    assembled.extend_from_slice(&part);
                    deletes.push((
                        space.clone(),
                        chunk_key(run_id, peer, transfer, target, prior),
                    ));
                }
                assembled.extend_from_slice(&chunk.data);
                match chunk.kind {
                    ChunkKind::Source => {
                        WorkflowFiles::from_bytes(&assembled)
                            .map_err(|_| CatalogError::Forbidden)?;
                        if more {
                            if staged_src
                                .as_deref()
                                .is_some_and(|old| old != assembled.as_slice())
                            {
                                return Err(CatalogError::Forbidden);
                            }
                            writes.push((
                                space.clone(),
                                staged_key(run_id, peer, transfer, ChunkKind::Source),
                                ByteView::from(assembled),
                            ));
                        } else {
                            if pending_src.is_some() {
                                return Err(CatalogError::Forbidden);
                            }
                            pending_src = Some(assembled);
                        }
                    }
                    ChunkKind::Workflow => {
                        let wf = WorkflowRecord::from_bytes(&assembled)
                            .map_err(|_| CatalogError::Forbidden)?;
                        if wf.workflow_id != self.run.workflow_id
                            || wf.group_id != self.run.group_id
                        {
                            return Err(CatalogError::Forbidden);
                        }
                        if more {
                            if staged_wf
                                .as_deref()
                                .is_some_and(|old| old != assembled.as_slice())
                            {
                                return Err(CatalogError::Forbidden);
                            }
                            writes.push((
                                space.clone(),
                                staged_key(run_id, peer, transfer, ChunkKind::Workflow),
                                ByteView::from(assembled),
                            ));
                        } else {
                            if pending_wf.is_some() {
                                return Err(CatalogError::Forbidden);
                            }
                            pending_wf = Some(assembled);
                        }
                    }
                    ChunkKind::Version => {
                        let ver = WorkflowVersionRecord::from_bytes(&assembled)
                            .map_err(|_| CatalogError::Forbidden)?;
                        if ver.workflow_id != self.run.workflow_id
                            || ver.version != self.run.version
                        {
                            return Err(CatalogError::Forbidden);
                        }
                        if more {
                            if staged_ver
                                .as_deref()
                                .is_some_and(|old| old != assembled.as_slice())
                            {
                                return Err(CatalogError::Forbidden);
                            }
                            writes.push((
                                space.clone(),
                                staged_key(run_id, peer, transfer, ChunkKind::Version),
                                ByteView::from(assembled),
                            ));
                        } else {
                            if pending_ver.is_some() {
                                return Err(CatalogError::Forbidden);
                            }
                            pending_ver = Some(assembled);
                        }
                    }
                    ChunkKind::Run => {
                        if more || assembled_run.is_some() {
                            return Err(CatalogError::Forbidden);
                        }
                        let full = WorkflowRunRecord::from_bytes(&assembled)
                            .map_err(|_| CatalogError::Forbidden)?;
                        if full.run_id != self.run.run_id
                            || full.realm_id != self.run.realm_id
                            || full.group_id != self.run.group_id
                            || full.workflow_id != self.run.workflow_id
                            || full.version != self.run.version
                            || full.node_id != self.run.node_id
                            || full.local_only
                        {
                            return Err(CatalogError::Forbidden);
                        }
                        assembled_run = Some(full);
                    }
                    ChunkKind::Tool(id, version) => {
                        let existing = values.next().flatten();
                        let expected = tool_key(&id, &version, self.run.group_id);
                        if let Some((key, value)) =
                            tool_update(existing.as_deref(), &assembled, self.run.group_id)?
                        {
                            if key != expected {
                                return Err(CatalogError::Forbidden);
                            }
                            if more {
                                let pair = (id, version);
                                writes.push((
                                    space.clone(),
                                    staged_key(
                                        run_id,
                                        peer,
                                        transfer,
                                        ChunkKind::Tool(pair.0.clone(), pair.1.clone()),
                                    ),
                                    value,
                                ));
                                if !staged_index.contains(&pair) {
                                    staged_index.push(pair);
                                    index_dirty = true;
                                }
                            } else {
                                writes.push((WORKFLOW_TOOL_KEYSPACE.to_string(), key, value));
                            }
                        }
                    }
                    ChunkKind::Page(number) => {
                        if more {
                            return Err(CatalogError::Forbidden);
                        }
                        let existing = values.next().flatten();
                        if number > head.pages {
                            return Err(CatalogError::NotFound);
                        }
                        let page = decode_page(&assembled).map_err(|_| CatalogError::Forbidden)?;
                        let shorter = existing
                            .as_deref()
                            .and_then(|row| decode_page(row).ok())
                            .is_some_and(|old| old.entries.len() > page.entries.len());
                        if !shorter {
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
                                        head.last_seq =
                                            Some(head.last_seq.map_or(*seq, |last| last.max(*seq)));
                                    }
                                }
                            }
                            writes.push((
                                space.clone(),
                                page_key(run_id, number),
                                ByteView::from(assembled),
                            ));
                        }
                    }
                }
            }
            if !more {
                if !replicated.workflow.is_empty() {
                    if pending_wf.is_some() {
                        return Err(CatalogError::Forbidden);
                    }
                    pending_wf = Some(replicated.workflow);
                }
                if !replicated.version.is_empty() {
                    if pending_ver.is_some() {
                        return Err(CatalogError::Forbidden);
                    }
                    pending_ver = Some(replicated.version);
                }
                if let Some(source) = replicated.source {
                    if pending_src.is_some() {
                        return Err(CatalogError::Forbidden);
                    }
                    pending_src = Some(source);
                }
            }
        }
        if more {
            let stamp = self.now_ms.max(1);
            writes.push((
                space.clone(),
                stage_meta_key(run_id, peer, transfer),
                ByteView::from(stamp.to_be_bytes().to_vec()),
            ));
            if index_dirty {
                let encoded = postcard::to_allocvec(&staged_index)
                    .map_err(|error| CatalogError::Storage(error.to_string()))?;
                writes.push((
                    space.clone(),
                    staged_tools_key(run_id, peer, transfer),
                    ByteView::from(encoded),
                ));
            }
            self.kept = head.pages;
            if meta_row.is_none() {
                return self.scan_stage(
                    StageScan {
                        transfer,
                        clear_all: false,
                        writes,
                        deletes,
                        seen: Vec::new(),
                    },
                    None,
                );
            }
            return self.emit_writes(writes, deletes);
        }
        if !more {
            if let Some(bytes) = staged_wf {
                if pending_wf.is_some() {
                    return Err(CatalogError::Forbidden);
                }
                pending_wf = Some(bytes.to_vec());
            }
            if let Some(bytes) = staged_ver {
                if pending_ver.is_some() {
                    return Err(CatalogError::Forbidden);
                }
                pending_ver = Some(bytes.to_vec());
            }
            if let Some(bytes) = staged_src {
                if pending_src.is_some() {
                    return Err(CatalogError::Forbidden);
                }
                pending_src = Some(bytes.to_vec());
            }
            // Publish catalog rows and the run together only when every required row is available.
            if (wf_row.is_none() && pending_wf.is_none())
                || (ver_row.is_none() && pending_ver.is_none())
                || (src_row.is_none() && pending_src.is_none())
            {
                return Err(CatalogError::NotFound);
            }
            let effective_ver = match ver_row.as_deref() {
                Some(bytes) => WorkflowVersionRecord::from_bytes(bytes)?,
                None => {
                    let bytes = pending_ver.as_deref().ok_or(CatalogError::NotFound)?;
                    let ver = WorkflowVersionRecord::from_bytes(bytes)
                        .map_err(|_| CatalogError::Forbidden)?;
                    if ver.workflow_id != self.run.workflow_id || ver.version != self.run.version {
                        return Err(CatalogError::Forbidden);
                    }
                    ver
                }
            };
            if let Some(bytes) = pending_wf {
                store_workflow(&bytes, wf_row.as_deref(), &self.run, &mut writes)?;
            }
            if let Some(bytes) = pending_ver {
                let ver = WorkflowVersionRecord::from_bytes(&bytes)
                    .map_err(|_| CatalogError::Forbidden)?;
                if ver.workflow_id != self.run.workflow_id || ver.version != self.run.version {
                    return Err(CatalogError::Forbidden);
                }
                if ver_row.is_none() {
                    writes.push((
                        WORKFLOW_VERSION_KEYSPACE.to_string(),
                        ver_key.clone(),
                        ByteView::from(bytes),
                    ));
                }
            }
            if let Some(bytes) = pending_src {
                let files =
                    WorkflowFiles::from_bytes(&bytes).map_err(|_| CatalogError::Forbidden)?;
                if source_digest(&files.source()) != effective_ver.digest {
                    return Err(CatalogError::Forbidden);
                }
                if src_row.is_none() {
                    writes.push((
                        WORKFLOW_SOURCE_KEYSPACE.to_string(),
                        ver_key,
                        ByteView::from(bytes),
                    ));
                }
            }
            if let Some(full) = assembled_run {
                if stored.as_ref().is_none_or(|old| !keep_stored(old, &full)) {
                    writes.push(run_row(&full)?);
                }
            } else if stub {
                if stored.is_none() {
                    return Err(CatalogError::NotFound);
                }
            } else if stored
                .as_ref()
                .is_none_or(|old| !keep_stored(old, &self.run))
            {
                writes.push(run_row(&self.run)?);
            }
            for (index, (bytes, existing)) in self.pages.iter().zip(page_rows).enumerate() {
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
            writes.push((
                space.clone(),
                head_key(run_id),
                ByteView::from(head.to_bytes()?),
            ));
            self.kept = head.pages;
            if !staged_index.is_empty() {
                let mut tool_reads = Vec::with_capacity(staged_index.len() * 2);
                for (id, version) in &staged_index {
                    let key = staged_key(
                        run_id,
                        peer,
                        transfer,
                        ChunkKind::Tool(id.clone(), version.clone()),
                    );
                    tool_reads.push((space.clone(), key));
                    tool_reads.push((
                        WORKFLOW_TOOL_KEYSPACE.to_string(),
                        tool_key(id, version, self.run.group_id),
                    ));
                }
                self.step = WriteStep::ReadTools(Box::new(CommitState {
                    transfer,
                    tools: staged_index,
                    writes,
                    deletes,
                }));
                return Ok(Next::Call(IoCall::Read(tool_reads)));
            }
        }
        self.scan_stage(
            StageScan {
                transfer,
                clear_all: true,
                writes,
                deletes,
                seen: Vec::new(),
            },
            None,
        )
    }
}

#[derive(Debug, PartialEq)]
enum ServeRunStep {
    Config,
    Read(Box<RealmConfigDocument>),
}

/// Serves a constant-size authoritative workflow run verdict to a sync-eligible realm executor
/// verifying vault secret bindings before release.
#[derive(Debug, PartialEq)]
pub struct ServeRunOperation {
    peer: NodeId,
    local: (NodeId, RealmId),
    run_id: Ulid,
    group_id: GroupId,
    bindings: Vec<TaskSecretBinding>,
    step: ServeRunStep,
}

impl ServeRunOperation {
    pub fn new(
        peer: NodeId,
        local: (NodeId, RealmId),
        run_id: Ulid,
        group_id: GroupId,
        bindings: Vec<TaskSecretBinding>,
    ) -> Self {
        Self {
            peer,
            local,
            run_id,
            group_id,
            bindings,
            step: ServeRunStep::Config,
        }
    }
}

impl Operation for ServeRunOperation {
    type Output = Option<WorkflowRunAuth>;
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
                let bindings_match = self.bindings.iter().all(|binding| {
                    binding.workflow_id == record.workflow_id
                        && record.secrets.values().any(|bound| {
                            bound.scope == binding.scope
                                && bound.name == binding.name
                                && bound.field == binding.field
                        })
                });
                Ok(Next::Done(Some(WorkflowRunAuth {
                    realm_id: record.realm_id,
                    group_id: record.group_id,
                    workflow_id: record.workflow_id,
                    created_by: record.created_by,
                    local_only: record.local_only,
                    bindings_match,
                })))
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
