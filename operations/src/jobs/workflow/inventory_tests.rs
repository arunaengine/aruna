//! Tests the output inventory by its calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::calls::ChildOutcome;
use aruna_core::errors::StorageError;
use aruna_core::structs::execution::job::{JobErrorKind, OutputCommitIntent};
use aruna_core::structs::storage::blob::BlobHeadKey;

use super::*;
use crate::s3::object::list::ListedObject;

const BUCKET: &str = "work";

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn version() -> Ulid {
    Ulid::from_bytes([7; 16])
}

fn spec(prefixes: &[&str]) -> ExecutionSpec {
    let output = aruna_core::structs::execution::job::OutputSelection {
        container_path: "/out/a".to_string(),
        path_prefix: None,
        destination_node_id: Some(node()),
        destination: aruna_core::structs::execution::job::OutputDestination::S3 {
            bucket: BUCKET.to_string(),
            key: "reports/a".to_string(),
        },
        name: None,
        description: None,
    };
    ExecutionSpec {
        group_id: Ulid::from_bytes([2; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: vec![output],
        workspace_outputs: Vec::new(),
        output_prefixes: prefixes.iter().map(|prefix| prefix.to_string()).collect(),
        collision_policy: Default::default(),
    }
}

fn control(keys: &[&str]) -> AttemptControl {
    AttemptControl {
        attempt_epoch: 1,
        execution_id: Ulid::from_bytes([9; 16]),
        controller_generation: 1,
        bound_token: None,
        tombstone_ref: None,
        output_commits: keys
            .iter()
            .map(|key| OutputCommitIntent {
                node_id: node(),
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                version_id: version(),
            })
            .collect(),
        output_record: None,
    }
}

fn collect(prefixes: &[&str], reserved: &[&str]) -> CollectOutputsOperation {
    CollectOutputsOperation::new(&spec(prefixes), BUCKET, control(reserved), Some(node()))
}

fn listed(key: &str) -> ListedObject {
    ListedObject {
        head: BlobHeadKey {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
        },
        version_id: version(),
        location: None,
        source_metadata: None,
        referenced: false,
        kind: None,
        source_path: None,
        connector_id: None,
        origin_node_id: None,
        last_refresh: None,
        version_created_at: None,
        link: None,
    }
}

fn page(keys: &[&str], token: Option<ListContinuationToken>) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, ListBucketError>(
        ListBucketResult {
            objects: keys.iter().map(|key| listed(key)).collect(),
            common_prefixes: Vec::new(),
            continuation_token: token,
        },
    )))
}

fn head(version_id: Option<Ulid>) -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, HeadObjectError>(
        HeadObjectResult {
            location: None,
            metadata: HashMap::new(),
            source_metadata: None,
            last_refresh: None,
            version_created_at: None,
            version_id,
            resolved_version_id: version_id,
            checksum_type: Default::default(),
            composite_hashes: HashMap::new(),
            part_count: None,
        source_policies: Vec::new(),
        source_binding: None,
        },
    )))
}

fn head_failure(error: HeadObjectError) -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<HeadObjectResult, _>(error)))
}

fn is_child(next: &Result<Next<Vec<OutputObject>>, JobError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

// A reserved key is read by its version and becomes one output of this execution.
#[test]
fn collects_reserved_key() {
    let mut inventory = collect(&["reports/"], &["reports/a"]);
    assert!(is_child(&inventory.first()));
    assert!(is_child(&inventory.next(page(&["reports/a"], None))));
    let Ok(Next::Done(outputs)) = inventory.next(head(Some(version()))) else {
        panic!("the inventory ends after its last key");
    };
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].key, "reports/a");
    assert_eq!(outputs[0].version_id, version());
    assert_eq!(outputs[0].execution_id, control(&[]).execution_id);
}

// A listed key without a reservation belongs to another writer and fails the inventory.
#[test]
fn refuses_unreserved_key() {
    let mut inventory = collect(&["reports/"], &[]);
    inventory.first().unwrap();
    let error = inventory.next(page(&["reports/x"], None)).unwrap_err();
    assert_eq!(
        error,
        JobError::permanent("output prefix inventory contains 1 unreserved objects")
    );
}

// A later page of the same prefix is asked for before the next prefix.
#[test]
fn follows_pages_and_prefixes() {
    let token = ListContinuationToken {
        last_key: b"reports/a".to_vec(),
        last_common_prefix: None,
    };
    let mut inventory = collect(&["reports/", "logs/"], &[]);
    inventory.first().unwrap();
    assert!(is_child(&inventory.next(page(&[], Some(token.clone())))));
    assert_eq!(inventory.continuation, Some(token));
    assert_eq!(inventory.prefixes.len(), 2);
    assert!(is_child(&inventory.next(page(&[], None))));
    assert_eq!(inventory.prefixes.len(), 1);
    assert_eq!(inventory.next(page(&[], None)), Ok(Next::Done(Vec::new())));
}

// A head naming another version, or none, is no proof of the reserved one.
#[test]
fn refuses_other_version() {
    let mut inventory = collect(&["reports/"], &["reports/a"]);
    inventory.first().unwrap();
    inventory.next(page(&["reports/a"], None)).unwrap();
    let error = inventory.next(head(Some(Ulid::nil()))).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Permanent);
    assert!(error.message.contains("does not carry reserved version"));
}

// A reserved version that is gone is permanent; a failed read may pass on a retry.
#[test]
fn head_failures_classified() {
    let answer = |result| {
        let mut inventory = collect(&["reports/"], &["reports/a"]);
        inventory.first().unwrap();
        inventory.next(page(&["reports/a"], None)).unwrap();
        inventory.next(result).unwrap_err()
    };
    let absent = answer(head_failure(HeadObjectError::NoSuchVersion));
    assert_eq!(absent.kind, JobErrorKind::Permanent);
    assert!(absent.message.contains("is absent"));
    let failed = HeadObjectError::StorageError(StorageError::Timeout);
    assert_eq!(answer(head_failure(failed)).kind, JobErrorKind::Retryable);
}

// Prefixes without an identity for this node cannot be attributed.
#[test]
fn needs_node_identity() {
    let spec = spec(&["reports/"]);
    let mut inventory = CollectOutputsOperation::new(&spec, BUCKET, control(&[]), None);
    assert_eq!(
        inventory.first(),
        Err(JobError::retryable(
            "output inventory needs a node identity"
        ))
    );
}
