//! Opens export candidates through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{remote_candidate, selected_scope, source_scope, stored_candidate};
use super::*;
use crate::jobs::repository::test_support::child;

fn opening(candidate: ExportCandidate) -> OpenCandidateOperation {
    OpenCandidateOperation::new(source_scope(), candidate, true)
}

fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

fn reading() -> OpenCandidateOperation {
    let mut open = opening(stored_candidate());
    open.step = OpenStep::Read {
        blake3: [1; 32],
        held: None,
    };
    open
}

fn blob_error(error: BlobError) -> IoResult {
    event(Event::Blob(BlobEvent::Error(error)))
}

// An export into a realm never reads from another node.
#[test]
fn selection_denies_remote() {
    let mut open = OpenCandidateOperation::new(selected_scope(), remote_candidate(), true);
    assert_eq!(open.first(), Ok(status(OpenStatus::Denied)));
}

// A local candidate starts with the caller's access.
#[test]
fn local_checks_access() {
    let mut open = opening(stored_candidate());
    assert!(matches!(open.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(open.step, OpenStep::Access(_)));
}

// A local candidate needs its hash, and without a version it is missing.
#[test]
fn local_needs_identity() {
    let mut candidate = stored_candidate();
    candidate.resolved_version = None;
    assert_eq!(opening(candidate).first(), Ok(status(OpenStatus::Missing)));
    let mut candidate = stored_candidate();
    candidate.expected_blake3 = None;
    assert!(matches!(
        opening(candidate).first(),
        Err(ExportFailure::Permanent(_))
    ));
}

// A remote candidate is read from its holder.
#[test]
fn remote_reads_holder() {
    let mut open = opening(remote_candidate());
    assert!(matches!(open.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(open.step, OpenStep::Remote { .. }));
}

// Refused access ends the open with its status.
#[test]
fn access_refusals_map() {
    for (answer, expected) in [
        (LocalOpen::Missing, OpenStatus::Missing),
        (LocalOpen::Denied, OpenStatus::Denied),
        (LocalOpen::NotHolder, OpenStatus::NotHolder),
    ] {
        let mut open = opening(stored_candidate());
        open.first().unwrap();
        let answer = child(Ok::<_, ResolveError>(answer));
        assert_eq!(open.next(answer), Ok(status(expected)));
    }
}

// An admitted plain copy is read from its backend.
#[test]
fn admitted_reads_blob() {
    let mut open = opening(stored_candidate());
    open.first().unwrap();
    let answer = child(Ok::<_, ResolveError>(LocalOpen::Admitted(None)));
    assert!(matches!(
        open.next(answer),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(open.step, OpenStep::Read { .. }));
}

// Damaged bytes are corrupt, a backend failure is offline.
#[test]
fn read_errors_map() {
    let damaged = BlobError::IntegrityCheckFailed("damaged".to_string());
    assert_eq!(
        reading().next(blob_error(damaged)),
        Ok(status(OpenStatus::Corrupt))
    );
    assert_eq!(
        reading().next(blob_error(BlobError::ChannelClosed)),
        Ok(status(OpenStatus::Offline))
    );
}

// A node without blob storage retries instead of leaving the file out.
#[test]
fn missing_handle_retries() {
    assert_eq!(
        reading().next(blob_error(BlobError::HandleMissing)),
        Err(ExportFailure::Retryable(
            "blob handle unavailable".to_string()
        ))
    );
}

// A holder's refusal is the candidate's status.
#[test]
fn remote_refusals_map() {
    let auth = source_scope().spec.auth_context.clone();
    let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let target = WaitTarget::Content { blake3: [1; 32] };
    let answer = |refusal| {
        let read = Err(BaoReadError::Refused(refusal));
        remote_read(read, node, target.clone(), &auth)
    };
    assert_eq!(
        answer(BaoReadRefusal::ReadDenied),
        Ok(status(OpenStatus::Denied))
    );
    assert_eq!(
        answer(BaoReadRefusal::NotFound),
        Ok(status(OpenStatus::Missing))
    );
    assert_eq!(
        answer(BaoReadRefusal::HashMismatch),
        Ok(status(OpenStatus::Corrupt))
    );
    assert_eq!(
        answer(BaoReadRefusal::BackendFailure),
        Ok(status(OpenStatus::Offline))
    );
}
