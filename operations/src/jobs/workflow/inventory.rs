//! Attributes a finished run's workspace objects to the versions it reserved before writing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{AttemptControl, ExecutionSpec, JobError, OutputObject};
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::workspace::{insert_output, reserved_version};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};

#[derive(Debug, PartialEq)]
enum InventoryStep {
    List,
    Head { key: String, version_id: Ulid },
}

/// Attributes this execution's outputs under the declared prefixes. A listed key counts only
/// when this execution durably reserved its version before writing; the current head may
/// otherwise belong to a duplicate or unrelated write.
#[derive(Debug, PartialEq)]
pub struct CollectOutputsOperation {
    prefixes: VecDeque<String>,
    declares_files: bool,
    group_id: GroupId,
    bucket: String,
    control: AttemptControl,
    node_id: Option<NodeId>,
    continuation: Option<ListContinuationToken>,
    listed: VecDeque<String>,
    outputs: Vec<OutputObject>,
    keys: HashSet<(NodeId, String, String)>,
    foreign: usize,
    step: InventoryStep,
}

impl CollectOutputsOperation {
    /// `node_id` is this node's identity, which an inventory cannot run without.
    pub fn new(
        spec: &ExecutionSpec,
        bucket: &str,
        control: AttemptControl,
        node_id: Option<NodeId>,
    ) -> Self {
        Self {
            prefixes: spec.output_prefixes.iter().cloned().collect(),
            declares_files: !spec.file_outputs.is_empty(),
            group_id: spec.group_id,
            bucket: bucket.to_string(),
            control,
            node_id,
            continuation: None,
            listed: VecDeque::new(),
            outputs: Vec::new(),
            keys: HashSet::new(),
            foreign: 0,
            step: InventoryStep::List,
        }
    }

    fn list(&mut self) -> Result<Next<Vec<OutputObject>>, JobError> {
        let Some(prefix) = self.prefixes.front() else {
            return self.finish();
        };
        let list = ListBucketOperation::new(ListBucketInput {
            bucket: self.bucket.clone(),
            group_id: self.group_id,
            continuation_token: self.continuation.clone(),
            max_keys: None,
            prefix: Some(prefix.clone()),
            delimiter: None,
            start_after: None,
        });
        self.step = InventoryStep::List;
        Ok(Next::Call(IoCall::child(list)))
    }

    fn finish(&mut self) -> Result<Next<Vec<OutputObject>>, JobError> {
        if self.foreign > 0 {
            return Err(JobError::permanent(format!(
                "output prefix inventory contains {} unreserved objects",
                self.foreign
            )));
        }
        Ok(Next::Done(std::mem::take(&mut self.outputs)))
    }

    /// Heads the next listed key this execution reserved, or goes on with the listing.
    fn advance(&mut self, node_id: NodeId) -> Result<Next<Vec<OutputObject>>, JobError> {
        while let Some(key) = self.listed.pop_front() {
            let Some(version_id) = reserved_version(&self.control, node_id, &self.bucket, &key)
            else {
                self.foreign += 1;
                continue;
            };
            let head = HeadObjectOperation::new(HeadObjectInput {
                bucket: self.bucket.clone(),
                key: key.clone(),
                version_id: Some(version_id),
            });
            self.step = InventoryStep::Head { key, version_id };
            return Ok(Next::Call(IoCall::child(head)));
        }
        if self.continuation.is_none() {
            self.prefixes.pop_front();
        }
        self.list()
    }

    /// Stored location of one reserved version, read by version id and never by current head,
    /// so a concurrent write cannot substitute its own bytes here.
    fn record_head(
        &mut self,
        node_id: NodeId,
        key: String,
        version_id: Ulid,
        head: Result<HeadObjectResult, HeadObjectError>,
    ) -> Result<(), JobError> {
        let bucket = &self.bucket;
        let location = match head {
            Ok(head) if head.version_id == Some(version_id) => head.location,
            Ok(_) => {
                return Err(JobError::permanent(format!(
                    "output {bucket}/{key} does not carry reserved version {version_id}"
                )));
            }
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => {
                return Err(JobError::permanent(format!(
                    "reserved output {bucket}/{key} version {version_id} is absent"
                )));
            }
            Err(error) => {
                return Err(JobError::retryable(format!(
                    "output version lookup failed: {error}"
                )));
            }
        };
        let (size, digest) = match location {
            Some(location) => (location.blob_size, location.get_blake3().map(hex::encode)),
            None => (0, None),
        };
        let output = OutputObject {
            node_id,
            bucket: bucket.clone(),
            key,
            version_id,
            execution_id: self.control.execution_id,
            container_path: String::new(),
            size,
            digest,
        };
        insert_output(&mut self.outputs, &mut self.keys, output)
    }
}

impl Operation for CollectOutputsOperation {
    type Output = Vec<OutputObject>;
    type Error = JobError;

    fn first(&mut self) -> Result<Next<Vec<OutputObject>>, JobError> {
        if !self.declares_files && !self.prefixes.is_empty() {
            return Err(JobError::permanent(
                "output prefixes require declared file outputs",
            ));
        }
        if self.prefixes.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        if self.node_id.is_none() {
            return Err(JobError::retryable(
                "output inventory needs a node identity",
            ));
        }
        self.list()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<OutputObject>>, JobError> {
        let node_id = self
            .node_id
            .ok_or_else(|| JobError::retryable("output inventory needs a node identity"))?;
        match std::mem::replace(&mut self.step, InventoryStep::List) {
            InventoryStep::List => {
                let page = result
                    .child::<ListBucketResult, ListBucketError>()?
                    .map_err(|error| {
                        JobError::retryable(format!("output inventory failed: {error}"))
                    })?;
                self.listed = page
                    .objects
                    .into_iter()
                    .map(|object| object.head.key)
                    .collect();
                self.continuation = page.continuation_token;
            }
            InventoryStep::Head { key, version_id } => {
                let head = result.child::<HeadObjectResult, HeadObjectError>()?;
                self.record_head(node_id, key, version_id, head)?;
            }
        }
        self.advance(node_id)
    }
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
