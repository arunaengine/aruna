//! Acquires import sources through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{hidden_location, run, stops};
use super::*;
use aruna_core::stream::BackendStream;
use futures_util::stream;

fn empty() -> ByteStream {
    BackendStream::new(stream::empty::<Result<bytes::Bytes, std::io::Error>>())
}

fn spool() -> SpoolSourceOperation {
    SpoolSourceOperation {
        run: run(),
        stream: Some(empty()),
        expected_size: None,
        eln: true,
        spooled: None,
        step: SpoolStep::Guard,
    }
}

fn spooled() -> IoResult {
    let location = hidden_location();
    let event = BlobEvent::HiddenSpooled {
        location,
        blake3: [1; 32],
        size: 4,
    };
    IoResult::Event(Box::new(Event::Blob(event)))
}

// An upload source starts with its claim.
#[test]
fn upload_claims_first() {
    let mut acquire = AcquireSourceOperation::new(run());
    acquire.stamp(7);
    assert!(matches!(acquire.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(acquire.step, Step::Claim);
}

// A spooled input is acquired without identifiers or a pull.
#[test]
fn spool_answer_acquires() {
    let mut acquire = AcquireSourceOperation::new(run());
    acquire.step = Step::Spool;
    let input = ImportInput {
        location: hidden_location(),
        size: 4,
        blake3: [1; 32],
        upload_id: None,
        eln: false,
    };
    let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(
        Ok::<_, ImportFailure>(input.clone()),
    ));
    let expected = AcquiredSource {
        input,
        identifiers: Vec::new(),
        pull: None,
    };
    assert_eq!(acquire.next(answer), Ok(Next::Done(expected)));
}

// A source above the limit is refused before its stream is touched.
#[test]
fn spool_checks_size() {
    let mut spool = spool();
    spool.expected_size = Some(u64::MAX);
    assert!(matches!(spool.first(), Err(ImportFailure::Permanent(_))));
    assert!(spool.stream.is_some());
}

// A blob spooled while the job runs on is the input.
#[test]
fn spool_keeps_input() {
    let mut spool = spool();
    spool.step = SpoolStep::Spool;
    assert!(matches!(
        spool.next(spooled()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let Ok(Next::Done(input)) = spool.next(stops(false, false)) else {
        panic!("expected the spooled input");
    };
    assert_eq!((input.size, input.eln), (4, true));
}

// A blob that landed while the job was cancelled is deleted, and the stop decides the end.
#[test]
fn spool_discards_stopped() {
    let mut spool = spool();
    spool.step = SpoolStep::Spool;
    spool.next(spooled()).unwrap();
    assert!(matches!(
        spool.next(stops(true, false)),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let failed = IoResult::Event(Box::new(Event::Blob(BlobEvent::HiddenDeleted)));
    assert_eq!(spool.next(failed), Err(ImportFailure::Cancelled));
}
