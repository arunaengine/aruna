//! Plans an import through the validation calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{bucket, entry, run};
use super::*;
use crate::jobs::repository::test_support::child;
use crate::jobs::store::JobMutationError;

fn validating(step: Step) -> ValidateSourceOperation {
    let mut validate = ValidateSourceOperation::new(run(), ImportCheckpoint::default());
    validate.step = step;
    validate
}

fn allowed(answer: bool) -> IoResult {
    child(Ok::<_, aruna_core::errors::AuthorizationError>(answer))
}

// The planned version id depends only on the run's seed, the time and the entry.
#[test]
fn versions_are_stable() {
    let first = planned_version(&run(), 7, 1, None);
    assert_eq!(first, planned_version(&run(), 7, 1, None));
    assert_ne!(first, planned_version(&run(), 7, 2, None));
}

// Validation starts at the target bucket.
#[test]
fn validate_reads_bucket() {
    let mut validate = ValidateSourceOperation::new(run(), ImportCheckpoint::default());
    assert_eq!(
        validate.first(),
        Ok(Next::Call(bucket_call(&run().spec.target.bucket)))
    );
}

// Without WRITE on the bucket nothing is planned.
#[test]
fn denied_bucket_fails() {
    let mut validate = validating(Step::BucketAllowed(bucket()));
    assert_eq!(
        validate.next(allowed(false)),
        Err(ImportFailure::Permanent("permission denied".to_string()))
    );
}

// A checkpoint without the inspected archive cannot be planned.
#[test]
fn plan_needs_inspection() {
    let mut validate = validating(Step::Federated(bucket()));
    let answer = child(Ok::<_, ImportError>(()));
    assert_eq!(
        validate.next(answer),
        Err(ImportFailure::Permanent(
            "archive inspection is missing".to_string()
        ))
    );
}

// The stored plan moves the import on and drops what the plan now holds.
#[test]
fn stored_plan_advances() {
    let plan = ImportPlan {
        metadata_json: "{}".to_string(),
        entries: vec![entry("a")],
    };
    let mut validate = validating(Step::Plan);
    validate.plan = Some(plan.clone());
    validate.checkpoint.metadata_json = Some("{}".to_string());
    let Ok(Next::Done(done)) = validate.next(child(Ok::<_, JobMutationError>(()))) else {
        panic!("expected the validated import");
    };
    assert_eq!(done.plan, plan);
    assert_eq!(done.checkpoint.phase, ImportPhase::Write);
    assert_eq!(done.checkpoint.entries_total, 1);
    assert_eq!(done.checkpoint.metadata_json, None);
}
