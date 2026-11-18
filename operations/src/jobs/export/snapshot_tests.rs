//! Reads export crates and takes snapshots through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{op_spec, realm_user};
use super::*;
use crate::jobs::repository::test_support::child;

fn read() -> ReadCrateOperation {
    ReadCrateOperation::new(None, op_spec().auth_context, Ulid::from_parts(3, 3), 64)
}

fn refused(error: MetadataApiError) -> IoResult {
    child(Err::<ExportMetadataResult, _>(error))
}

// A dataset without a raw revision is read as its rendered crate.
#[test]
fn raw_reads_full() {
    let mut read = read();
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let next = read.next(refused(MetadataApiError::NotFound));
    assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    assert!(read.full);
}

// A rendered crate that is missing too ends the read.
#[test]
fn full_missing_fails() {
    let mut read = read();
    read.full = true;
    assert_eq!(
        read.next(refused(MetadataApiError::NotFound)),
        Err(ExportFailure::Permanent(
            "metadata document has no raw RO-Crate revision".to_string()
        ))
    );
}

// Missing access is permanent and an outage is not.
#[test]
fn read_failures_classified() {
    assert_eq!(
        snapshot_read_failure(MetadataApiError::Forbidden),
        ExportFailure::Permanent("metadata document READ permission denied".to_string())
    );
    assert!(matches!(
        snapshot_read_failure(MetadataApiError::ServiceUnavailable),
        ExportFailure::Retryable(_)
    ));
}

// The crate read keeps permanent failures and hides what a caller cannot act on.
#[test]
fn crate_maps_failures() {
    let mut json = CrateJsonOperation::new(None, realm_user(), Ulid::from_parts(3, 3), 64);
    assert!(matches!(json.first(), Ok(Next::Call(IoCall::Child(_)))));
    let invalid = child(Err::<CrateRead, _>(ExportFailure::Validation(Vec::new())));
    assert_eq!(
        json.next(invalid),
        Err(TransferError::Permanent(
            "the dataset crate is invalid".to_string()
        ))
    );
    let busy = child(Err::<CrateRead, _>(ExportFailure::Interrupted));
    assert!(matches!(json.next(busy), Err(TransferError::Retryable(_))));
}

// A crate above the metadata limit is refused before it is parsed.
#[test]
fn snapshot_checks_size() {
    let mut spec = op_spec();
    spec.limits.metadata_bytes = 1;
    let mut snapshot = SnapshotOperation::new(Arc::new(spec), None, ExportCheckpoint::default());
    let read: CrateRead = ("{}".to_string(), Ulid::from_parts(1, 1), [0; 32]);
    let answer = child(Ok::<_, ExportFailure>(read));
    assert_eq!(
        snapshot.next(answer),
        Err(ExportFailure::Permanent(
            "RO-Crate metadata exceeds the 1 byte limit".to_string()
        ))
    );
}

// No mapping means no identifiers.
#[test]
fn empty_identity() {
    assert_eq!(identity_of(None), ExportIdentity::default());
}
