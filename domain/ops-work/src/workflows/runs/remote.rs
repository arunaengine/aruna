//! Reads of a run's objects that another node holds, like task outputs in its run area there:
//! a stat or a chunk of the exact version, read through the holder, which authorizes the creator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{BaoReadRequest, BaoReadTarget};
use aruna_core::structs::checksum::HASH_SHA256;
use aruna_core::structs::execution::workflow_run::WorkflowRunRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_ops_data::replication::bao_read::{BaoReadError, BaoReadOutput};
use aruna_ops_data::replication::managed_read::ManagedReadOperation;
use aruna_workflow_contract::{ContractError, DATA_CHUNK_BYTES, HostRpcResult, host::StorageEntry};

use super::creator_auth;
use super::data::{DataCall, Target};
use crate::workflows::CatalogError;

/// The read of an exact version `node` holds, as the run's creator; `None` without a version.
pub fn read_call(
    run: &WorkflowRunRecord,
    target: &Target,
    node: NodeId,
    (metadata_only, range): (bool, Option<Range<u64>>),
) -> Option<IoCall> {
    let request = BaoReadRequest {
        auth_context: creator_auth(run),
        realm_id: run.realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id: run.realm_id,
            node_id: node,
            bucket: target.bucket.clone(),
            key: target.key.clone(),
            version: target.version?,
        }),
        expected_blake3: None,
        metadata_only,
        destination: None,
        known_refs: Vec::new(),
        range,
    };
    Some(IoCall::child(ManagedReadOperation::new(node, request)))
}

/// The answer of a [`read_call`], or the refusal the coordinator reads.
pub fn read_answer(result: IoResult) -> Result<Result<BaoReadOutput, ContractError>, CatalogError> {
    Ok(match result.child::<BaoReadOutput, BaoReadError>()? {
        Ok(output) => Ok(output),
        Err(BaoReadError::Refused(refusal)) => Err(ContractError::Denied {
            message: format!("the holder refused the read: {refusal:?}"),
        }),
        Err(error) => Err(ContractError::internal(error.to_string())),
    })
}

#[derive(Debug, PartialEq)]
enum RemoteStep {
    Head,
    Read,
    Collect,
}

/// Answers a stat or a chunk read of an object another node holds; a listing there is refused.
#[derive(Debug, PartialEq)]
pub struct RemoteReadOperation {
    run: WorkflowRunRecord,
    target: Target,
    node: NodeId,
    call: DataCall,
    size: u64,
    step: RemoteStep,
}

type RemoteNext = Result<Next<HostRpcResult>, CatalogError>;

impl RemoteReadOperation {
    pub fn new(run: WorkflowRunRecord, target: Target, node: NodeId, call: DataCall) -> Self {
        Self {
            run,
            target,
            node,
            call,
            size: 0,
            step: RemoteStep::Head,
        }
    }

    fn failed(error: ContractError) -> RemoteNext {
        Ok(Next::Done(HostRpcResult::Failed(error)))
    }

    fn version(&self) -> Option<String> {
        self.target.version.map(|version| version.to_string())
    }

    fn headed(
        &mut self,
        size: u64,
        hashes: &std::collections::HashMap<String, Vec<u8>>,
    ) -> RemoteNext {
        self.size = size;
        let offset = match self.call {
            DataCall::Stat { .. } => {
                let entry = StorageEntry {
                    directory: false,
                    size,
                    modified_ms: None,
                    version: self.version(),
                    sha256: hashes.get(HASH_SHA256).map(hex::encode),
                    aruna_id: None,
                    endpoints: Vec::new(),
                };
                return Ok(Next::Done(HostRpcResult::StorageStat {
                    entry: Some(entry),
                }));
            }
            DataCall::Download { offset, .. } => offset,
            DataCall::List { .. } => return Err(IoError::Unfinished.into()),
        };
        if offset >= size {
            return Ok(Next::Done(HostRpcResult::StorageChunk {
                bytes: Vec::new(),
                eof: true,
                version: self.version(),
            }));
        }
        let end = size.min(offset.saturating_add(DATA_CHUNK_BYTES as u64));
        let read = read_call(
            &self.run,
            &self.target,
            self.node,
            (false, Some(offset..end)),
        );
        self.step = RemoteStep::Read;
        Ok(Next::Call(read.ok_or(IoError::Unfinished)?))
    }
}

impl Operation for RemoteReadOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn first(&mut self) -> RemoteNext {
        if matches!(self.call, DataCall::List { .. }) {
            return Self::failed(ContractError::unsupported(format!(
                "a folder on node {} is not listed from here",
                self.node
            )));
        }
        let Some(head) = read_call(&self.run, &self.target, self.node, (true, None)) else {
            return Self::failed(ContractError::invalid(
                "an object on another node is read by its exact version",
            ));
        };
        self.step = RemoteStep::Head;
        Ok(Next::Call(head))
    }

    fn next(&mut self, result: IoResult) -> RemoteNext {
        match self.step {
            RemoteStep::Head => match read_answer(result)? {
                Ok(BaoReadOutput::Metadata { size, hashes, .. })
                | Ok(BaoReadOutput::Stream { size, hashes, .. }) => self.headed(size, &hashes),
                Err(error) => Self::failed(error),
            },
            RemoteStep::Read => match read_answer(result)? {
                Ok(BaoReadOutput::Stream { blob, .. }) => {
                    let collect = JobEffect::CollectUpTo {
                        stream: blob,
                        limit: DATA_CHUNK_BYTES as u64,
                    };
                    self.step = RemoteStep::Collect;
                    Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(collect)))))
                }
                Ok(BaoReadOutput::Metadata { .. }) => Err(IoError::Unfinished.into()),
                Err(error) => Self::failed(error),
            },
            RemoteStep::Collect => {
                let DataCall::Download { offset, .. } = self.call else {
                    return Err(IoError::Unfinished.into());
                };
                let bytes = match result.event()? {
                    Event::Job(JobEvent::CollectedUpTo(Ok(Some(bytes)))) => bytes,
                    Event::Job(JobEvent::CollectedUpTo(other)) => {
                        let message = format!("a remote chunk was not read: {other:?}");
                        return Err(CatalogError::Storage(message));
                    }
                    _ => return Err(crate::workflows::unexpected("JobEvent::CollectedUpTo")),
                };
                let eof = offset + bytes.len() as u64 >= self.size;
                Ok(Next::Done(HostRpcResult::StorageChunk {
                    bytes,
                    eof,
                    version: self.version(),
                }))
            }
        }
    }
}
