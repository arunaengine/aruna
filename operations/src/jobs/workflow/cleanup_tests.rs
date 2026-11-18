//! Tests the terminal cleanup by its calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::compute::{AttemptRef, TombstoneEvidence};
use aruna_core::errors::StorageError;

use super::*;

fn job() -> JobId {
    JobId::from_bytes([5; 16])
}

fn attempt() -> AttemptRef {
    AttemptRef::new(job().to_string().to_lowercase(), 1)
}

fn intent() -> AttemptIntent {
    AttemptIntent {
        attempt_no: 1,
        external_name: attempt().external_name(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine".to_string(),
        attempt_epoch: 3,
    }
}

fn fence() -> FenceContext {
    FenceContext {
        attempt: attempt(),
        attempt_epoch: 3,
        controller_generation: 2,
    }
}

fn operation(intent: Option<AttemptIntent>) -> TerminalCleanupOperation {
    TerminalCleanupOperation::new(job(), Ulid::from_bytes([1; 16]), intent, "key")
}

fn answer(answer: ComputeAnswer) -> IoResult {
    IoResult::Event(Box::new(Event::Compute(Box::new(answer))))
}

fn authorized() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, JobMutationError>(fence())))
}

fn compute_call(request: ComputeRequest) -> Next<()> {
    Next::Call(IoCall::Effect(Box::new(Effect::Compute(Box::new(
        ComputeEffect {
            kind: ExecutorKind::Docker,
            fence: fence(),
            request,
        },
    )))))
}

fn tombstone_spec() -> ComputeRequest {
    ComputeRequest::Tombstone(TombstoneSpec { terminal_ref: None })
}

fn access() -> UserAccess {
    UserAccess {
        access_key: "key".to_string(),
        user_identity: Default::default(),
        group_id: Ulid::nil(),
        secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
        expiry: std::time::SystemTime::UNIX_EPOCH,
        path_restrictions: None,
        issued_by: [0; 32],
        revoked_at: None,
    }
}

fn revoked() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, RevokeUserError>(access())))
}

fn missing_key() -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<UserAccess, _>(
        RevokeUserError::NotFound,
    )))
}

fn is_child(next: &Result<Next<()>, JobError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

/// A cleanup that got its fence and waits for the answer to the backend fence.
fn fenced() -> TerminalCleanupOperation {
    let mut cleanup = operation(Some(intent()));
    assert!(is_child(&cleanup.first()));
    assert_eq!(
        cleanup.next(authorized()),
        Ok(compute_call(ComputeRequest::Fence))
    );
    cleanup
}

// The whole path: fence, cleanup, tombstone, its record, and the revoke.
#[test]
fn cleans_then_revokes() {
    let mut cleanup = fenced();
    assert_eq!(
        cleanup.next(answer(ComputeAnswer::Done(Ok(())))),
        Ok(compute_call(ComputeRequest::Cleanup))
    );
    assert_eq!(
        cleanup.next(answer(ComputeAnswer::Done(Ok(())))),
        Ok(compute_call(tombstone_spec()))
    );
    let tombstone = TombstoneEvidence {
        backend_ref: "gone".to_string(),
        attempt_epoch: 3,
    };
    let recorded = cleanup.next(answer(ComputeAnswer::Tombstone(Ok(tombstone))));
    assert!(is_child(&recorded));
    let control = AttemptControl {
        attempt_epoch: 3,
        execution_id: Ulid::nil(),
        controller_generation: 2,
        bound_token: None,
        tombstone_ref: None,
        output_commits: Vec::new(),
        output_record: None,
    };
    let stored = IoResult::Child(ChildOutcome::new(Ok::<_, JobMutationError>(control)));
    assert!(is_child(&cleanup.next(stored)));
    assert_eq!(cleanup.next(revoked()), Ok(Next::Done(())));
}

// Without an attempt only the credential is revoked; a key already gone is fine.
#[test]
fn attemptless_only_revokes() {
    let mut cleanup = operation(None);
    assert!(is_child(&cleanup.first()));
    assert_eq!(cleanup.next(missing_key()), Ok(Next::Done(())));
}

// An object the backend no longer holds still gets its tombstone.
#[test]
fn missing_object_tombstones() {
    let mut cleanup = fenced();
    cleanup.next(answer(ComputeAnswer::Done(Ok(())))).unwrap();
    let gone = BackendError::NotFound("attempt".to_string());
    assert_eq!(
        cleanup.next(answer(ComputeAnswer::Done(Err(gone)))),
        Ok(compute_call(tombstone_spec()))
    );
}

// A failed backend step still revokes, and the job fails with that step's error.
#[test]
fn backend_failure_revokes() {
    let mut cleanup = fenced();
    cleanup.next(answer(ComputeAnswer::Done(Ok(())))).unwrap();
    let down = BackendError::Unavailable("down".to_string());
    assert!(is_child(
        &cleanup.next(answer(ComputeAnswer::Done(Err(down))))
    ));
    let error = cleanup.next(missing_key()).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Retryable);
    assert!(error.message.starts_with("backend cleanup failed"));
}

// A failure no retry can change stays permanent.
#[test]
fn permanent_cleanup_failure() {
    let mut cleanup = fenced();
    cleanup.next(answer(ComputeAnswer::Done(Ok(())))).unwrap();
    let invalid = BackendError::InvalidSpec("bad".to_string());
    cleanup
        .next(answer(ComputeAnswer::Done(Err(invalid))))
        .unwrap();
    let error = cleanup.next(revoked()).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

// A node without compute, or without this backend, retries the cleanup later.
#[test]
fn missing_backend_retries() {
    for (reply, message) in [
        (ComputeAnswer::NoCompute, "cleanup needs a compute backend"),
        (
            ComputeAnswer::NoBackend,
            "cleanup backend docker is unavailable",
        ),
    ] {
        let mut cleanup = fenced();
        assert!(is_child(&cleanup.next(answer(reply))));
        assert_eq!(
            cleanup.next(missing_key()),
            Err(JobError::retryable(message))
        );
    }
}

// A parent that is gone is final; another store failure is not.
#[test]
fn fence_claim_failures() {
    let cases = [
        (JobMutationError::NotFound, JobErrorKind::Permanent),
        (
            JobMutationError::Storage(StorageError::Timeout.to_string()),
            JobErrorKind::Retryable,
        ),
    ];
    for (failure, kind) in cases {
        let mut cleanup = operation(Some(intent()));
        cleanup.first().unwrap();
        let claim = IoResult::Child(ChildOutcome::new(Err::<FenceContext, _>(failure)));
        assert!(is_child(&cleanup.next(claim)));
        assert_eq!(cleanup.next(missing_key()).unwrap_err().kind, kind);
    }
}

// A fence naming another attempt never reaches the backend.
#[test]
fn identity_mismatch_stops() {
    let mut other = intent();
    other.external_name = "aruna-other-a1".to_string();
    let mut cleanup = operation(Some(other));
    cleanup.first().unwrap();
    assert!(is_child(&cleanup.next(authorized())));
    assert_eq!(
        cleanup.next(missing_key()),
        Err(JobError::permanent("cleanup attempt identity mismatch"))
    );
}

// Both failures are reported, and one retryable failure makes the job retry.
#[test]
fn revoke_failure_joins() {
    let mut cleanup = fenced();
    let down = BackendError::Unavailable("down".to_string());
    cleanup
        .next(answer(ComputeAnswer::Done(Err(down))))
        .unwrap();
    let failed = RevokeUserError::StorageError(StorageError::Timeout);
    let reply = IoResult::Child(ChildOutcome::new(Err::<UserAccess, _>(failed)));
    let error = cleanup.next(reply).unwrap_err();
    assert_eq!(error.kind, JobErrorKind::Retryable);
    assert!(
        error
            .message
            .starts_with("workspace credential revoke failed")
    );
    assert!(error.message.contains("; backend fence failed"));
}
