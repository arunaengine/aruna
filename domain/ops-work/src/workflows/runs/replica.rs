//! Copies a run and its log pages onto group record holders so an owner or realm admin can
//! resume an unreachable run on a surviving node; nothing takes over automatically.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{RUN_LOG_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::metadata::protocol::{
    METADATA_IO_TIMEOUT, MetadataReadError, MetadataTransportMessage, encode_message, peer_call,
    peer_reply,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::plan_target_placement;
use aruna_core::placement::resolver::PlacementResolutionContext;
use aruna_core::structs::execution::workflow_run::{
    LogHead, WorkflowRunRecord, decode_page, encode_page, head_key, page_key, run_key,
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
use crate::workflows::CatalogError;

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
    Config(Box<WorkflowRunRecord>),
    Log(Box<WorkflowRunRecord>, Vec<NodeId>),
    Send(NodeId),
}

/// Sends the run and its log pages from `from_page` on to every holder of the run group's
/// records. A holder that does not answer is skipped for this pass. Answers the page the next
/// pass starts from: the last one sent, or the same as now when a holder missed a frame.
#[derive(Debug, PartialEq)]
pub struct ReplicateLogOperation {
    run_id: Ulid,
    local: (NodeId, RealmId),
    from_page: u32,
    sends: Vec<(NodeId, Vec<u8>)>,
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
            sends: Vec::new(),
            missed: false,
            pages: 0,
            step: SendStep::Run,
        }
    }

    /// The store requests of the pages from `from_page` on, each inside the batch bound.
    fn frames(
        &self,
        run: &WorkflowRunRecord,
        pages: &[RunLogPage],
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        let mut batches: Vec<Vec<Vec<u8>>> = vec![Vec::new()];
        let mut bytes = 0;
        for page in pages.iter().skip(self.from_page as usize) {
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
        let mut first = self.from_page;
        let mut frames = Vec::with_capacity(batches.len());
        for batch in batches {
            let count = batch.len() as u32;
            let message = MetadataTransportMessage::StoreRunLog {
                run: Box::new(run.clone()),
                first_page: first,
                pages: batch,
            };
            let frame = encode_message(&message);
            frames.push(frame.map_err(|error| CatalogError::Storage(error.to_string()))?);
            first += count;
        }
        Ok(frames)
    }

    fn send_next(&mut self) -> SendNext {
        let Some((node, frame)) = self.sends.pop() else {
            let next = match self.missed {
                true => self.from_page,
                false => self.pages,
            };
            return Ok(Next::Done(next));
        };
        self.step = SendStep::Send(node);
        Ok(Next::Call(peer_call(node, frame, METADATA_IO_TIMEOUT * 2)))
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
                self.step = SendStep::Config(Box::new(run));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.local.1,
                ))))
            }
            SendStep::Config(run) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| CatalogError::Storage(error.to_string()))?;
                let holders = record_holders(&config, run.group_id, self.local.0);
                if holders.is_empty() {
                    return Ok(Next::Done(self.from_page));
                }
                self.step = SendStep::Log(run, holders);
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    self.run_id,
                ))))
            }
            SendStep::Log(run, holders) => {
                let pages = pages_answer(result)?;
                self.pages = pages.len() as u32;
                self.from_page = self.from_page.min(self.pages.saturating_sub(1));
                let frames = self.frames(&run, &pages)?;
                for node in holders.into_iter().rev() {
                    for frame in frames.iter().rev() {
                        self.sends.push((node, frame.clone()));
                    }
                }
                self.send_next()
            }
            SendStep::Send(peer) => {
                match peer_reply(peer, result)? {
                    Ok(MetadataTransportMessage::RunLogStored { result: Ok(_) }) => {}
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
    step: StoreStep,
}

impl StoreReplicaOperation {
    pub fn new(
        peer: NodeId,
        local: (NodeId, RealmId),
        run: Box<WorkflowRunRecord>,
        first_page: u32,
        pages: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            peer,
            local,
            run,
            first_page,
            pages,
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
        Ok(Next::Call(IoCall::Read(vec![
            (WORKFLOW_RUN_KEYSPACE.to_string(), run_key(self.run.run_id)),
            (RUN_LOG_KEYSPACE.to_string(), head_key(self.run.run_id)),
        ])))
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
        let mut writes = vec![run_row(&self.run)?];
        let space = RUN_LOG_KEYSPACE.to_string();
        let run_id = self.run.run_id;
        for (index, bytes) in self.pages.iter().enumerate() {
            let page = decode_page(bytes).map_err(|_| CatalogError::Forbidden)?;
            let number = self.first_page + index as u32;
            if number + 1 >= head.pages {
                head.pages = number + 1;
                head.entries = page.entries.len() as u32;
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
        writes.push((space, head_key(run_id), ByteView::from(head.to_bytes()?)));
        self.kept = head.pages;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
