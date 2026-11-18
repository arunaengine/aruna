//! Reads the placement policies of every bucket a run writes into.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{ExecutionSpec, OutputDestination, WorkspaceMode};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::GroupId;

use super::ids;
use crate::jobs::submit::SubmitJobError;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// The placement refs of every bucket the run writes into; all live on the ingress node and in
/// the execution's group.
#[derive(Debug, PartialEq)]
pub(super) struct OutputPoliciesOperation {
    group_id: GroupId,
    /// Buckets still to read, the next one last.
    buckets: Vec<String>,
    asked: String,
    policies: Vec<PlacementPolicyRef>,
    misplaced: bool,
}

type OutputsNext = Result<Next<Vec<PlacementPolicyRef>>, SubmitJobError>;

impl OutputPoliciesOperation {
    pub(super) fn new(spec: &ExecutionSpec, local: NodeId) -> Self {
        let misplaced = spec
            .file_outputs
            .iter()
            .any(|output| output.destination_node_id != Some(local));
        let mut buckets: BTreeSet<String> = spec
            .file_outputs
            .iter()
            .map(|output| {
                let OutputDestination::S3 { bucket, .. } = &output.destination;
                bucket.clone()
            })
            .collect();
        let (mode, workspace) = ids::workspace_of(spec);
        if mode == WorkspaceMode::Existing
            && let Some(bucket) = workspace
        {
            buckets.insert(bucket);
        }
        Self {
            group_id: spec.group_id,
            buckets: buckets.into_iter().rev().collect(),
            asked: String::new(),
            policies: Vec::new(),
            misplaced,
        }
    }

    fn ask(&mut self) -> OutputsNext {
        let Some(bucket) = self.buckets.pop() else {
            let policies = PlacementPolicyRef::canonical_set(&self.policies)
                .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
            return Ok(Next::Done(policies));
        };
        self.asked = bucket.clone();
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))))
    }

    fn refused(&self, reason: impl std::fmt::Display) -> SubmitJobError {
        SubmitJobError::InvalidWorkspace(format!("s3://{}: {reason}", self.asked))
    }
}

impl Operation for OutputPoliciesOperation {
    type Output = Vec<PlacementPolicyRef>;
    type Error = SubmitJobError;

    fn first(&mut self) -> OutputsNext {
        if self.misplaced {
            return Err(SubmitJobError::InvalidWorkspace(
                "output destination endpoint is invalid".to_string(),
            ));
        }
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> OutputsNext {
        let info = match result.child::<BucketInfo, GetBucketError>()? {
            Ok(info) => info,
            Err(GetBucketError::NotFound) => return Err(self.refused("output bucket not found")),
            Err(
                error @ (GetBucketError::ConversionError(_)
                | GetBucketError::InvalidStateEvent { .. }),
            ) => return Err(self.refused(error)),
            Err(error) => {
                return Err(SubmitJobError::PlacementUnavailable(format!(
                    "s3://{}: {error}",
                    self.asked
                )));
            }
        };
        if info.group_id != self.group_id {
            return Err(self.refused("output bucket is outside the execution group"));
        }
        self.policies.extend(info.placement_policies);
        self.ask()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{node, payload};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::OutputSelection;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    fn spec(buckets: &[&str], destination: NodeId) -> ExecutionSpec {
        let mut spec = payload();
        spec.file_outputs = buckets
            .iter()
            .map(|bucket| OutputSelection {
                container_path: "/out".to_string(),
                path_prefix: None,
                destination_node_id: Some(destination),
                destination: OutputDestination::S3 {
                    bucket: bucket.to_string(),
                    key: "key".to_string(),
                },
                name: None,
                description: None,
            })
            .collect();
        spec
    }

    fn bucket(group_id: GroupId) -> Result<BucketInfo, GetBucketError> {
        Ok(BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: crate::tests::records::user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        })
    }

    fn answer(result: Result<BucketInfo, GetBucketError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // Each distinct bucket is read once; an output on another node is refused before any read.
    #[test]
    fn reads_each_bucket() {
        let spec = spec(&["b", "a", "b"], node(1));
        let mut operation = OutputPoliciesOperation::new(&spec, node(1));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.asked, "a");
        let second = operation.next(answer(bucket(spec.group_id)));
        assert!(matches!(second, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.asked, "b");
        assert_eq!(
            operation.next(answer(bucket(spec.group_id))),
            Ok(Next::Done(Vec::new()))
        );

        let mut elsewhere = OutputPoliciesOperation::new(&spec, node(2));
        assert!(matches!(
            elsewhere.first(),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    // A missing bucket or one of another group is the submitter's error; a store failure is not.
    #[test]
    fn refuses_foreign_bucket() {
        let spec = spec(&["a"], node(1));
        let started = || {
            let mut operation = OutputPoliciesOperation::new(&spec, node(1));
            operation.first().expect("bucket read");
            operation
        };
        let foreign = bucket(Ulid::from_bytes([9u8; 16]));
        assert!(matches!(
            started().next(answer(foreign)),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
        assert!(matches!(
            started().next(answer(Err(GetBucketError::NotFound))),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
        assert!(matches!(
            started().next(answer(Err(GetBucketError::Incomplete))),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }
}
